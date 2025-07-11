package com.example.flink;

// Core Flink APIs
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

// Kafka and Redis connectors
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import org.apache.flink.util.Collector;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;

/**
 * Main Flink pipeline for processing taxi data.
 * Reads from Kafka, calculates speed, distance, and writes to Redis.
 */
public class Main {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    // Constants for monitoring
    private static final double SPEED_LIMIT_KMH = 50.0;
    private static final double FORBIDDEN_CITY_LAT = 39.916668;
    private static final double FORBIDDEN_CITY_LON = 116.383331;
    private static final double AREA_RADIUS_10KM = 10.0; // km
    private static final double AREA_RADIUS_15KM = 15.0; // km

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = null;
        String inputTopic = null;
        String redisHost = null;

        // Parse command-line arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--kafka.bootstrap.servers":
                    kafkaBootstrap = args[++i];
                    break;
                case "--kafka.topic":
                    inputTopic = args[++i];
                    break;
                case "--redis.host":
                    redisHost = args[++i];
                    break;
            }
        }

        if (kafkaBootstrap == null || inputTopic == null || redisHost == null) {
            System.err.println("Usage: --kafka.bootstrap.servers <host:port> --kafka.topic <topic> --redis.host <host>");
            System.exit(1);
        }

        // Set up Flink streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure Kafka consumer
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", kafkaBootstrap);
        kafkaProps.setProperty("group.id", "taxi-consumer");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic, new SimpleStringSchema(), kafkaProps);
        kafkaConsumer.setStartFromEarliest();

        // Read from Kafka topic
        DataStream<String> rawStream = env.addSource(kafkaConsumer);

        // Parse raw data into TaxiData objects
        DataStream<TaxiData> taxiData = rawStream
                .map(line -> {
                    String[] parts = line.split(",");
                    if (parts.length == 4) {
                        return parseTaxiData(parts);
                    } else {
                        System.err.println("Invalid line: " + line);
                        return null;
                    }
                })
                .filter(data -> data != null)
                .name("Parse Taxi Data");

            DataStream<TaxiData> filteredTaxiData = taxiData
            .keyBy(TaxiData::getTaxiId)
            .process(new FilterTaxisByArea())
            .name("Filter Taxis by Area");

        // Store information operator
        DataStream<String> storeInfo = filteredTaxiData
                .keyBy(TaxiData::getTaxiId)
                .process(new StoreInformationOperator())
                .name("Store Information");

        // Propagate to dashboard operator
        DataStream<String> propagateDashboard = filteredTaxiData
                .keyBy(td -> "global_key")
                .process(new PropagateToDashboard())
                .name("Propagate Information to Dashboard");

        // Calculate speed per taxi
        DataStream<TaxiSpeed> enriched = filteredTaxiData
                .keyBy(TaxiData::getTaxiId)
                .process(new CalculateSpeed())
                .name("Calculate Speed");

        // Calculate distance per taxi
        DataStream<TaxiDistance> dists = filteredTaxiData
                .keyBy(TaxiData::getTaxiId)
                .process(new CalculateDistance())
                .name("Calculate Distance");

        // Calculate average speed per taxi
        DataStream<TaxiAverageSpeed> avgSpeeds = enriched
                .keyBy(TaxiSpeed::getTaxiId)
                .process(new CalculateAverageSpeed())
                .name("Calculate Average Speed");

        // Propagate location to dashboard (every 5 seconds)
        DataStream<String> locationDashboard = filteredTaxiData
                .keyBy(TaxiData::getTaxiId)
                .process(new PropagateLocationToDashboard())
                .name("Propagate Location Information to Dashboard");

        // Notify dashboard operators (speeding and area violations)
        DataStream<String> notifications = filteredTaxiData
                .keyBy(TaxiData::getTaxiId)
                .process(new NotifyDashboardOperators())
                .name("Notify Dashboard Once");
        
        // Configure Redis connection
        FlinkJedisPoolConfig redisCfg = new FlinkJedisPoolConfig.Builder()
                .setHost(redisHost)
                .setPort(6379)
                .build();

        // Sink for taxi location data
        filteredTaxiData.addSink(new RedisSink<>(redisCfg, new RedisMapper<TaxiData>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "taxi_location");
            }

            @Override
            public String getKeyFromData(TaxiData data) {
                return "taxi_" + data.getTaxiId();
            }

            @Override
            public String getValueFromData(TaxiData data) {
                return String.format("{\"timestamp\":%d,\"lat\":%.6f,\"lon\":%.6f}",
                        data.getTimestamp(), data.getLatitude(), data.getLongitude());
            }
    }));

        // Sink for speed data
        enriched.addSink(new RedisSink<>(redisCfg, new RedisMapper<TaxiSpeed>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "taxi_speed");
            }

            @Override
            public String getKeyFromData(TaxiSpeed data) {
                return "taxi_" + data.getTaxiId();
            }

            @Override
            public String getValueFromData(TaxiSpeed data) {
                return String.format("%.2f", data.getSpeed());
            }
        }));

        // Sink for average speed data
        avgSpeeds.addSink(new RedisSink<>(redisCfg, new RedisMapper<TaxiAverageSpeed>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "average_speed");
            }

            @Override
            public String getKeyFromData(TaxiAverageSpeed data) {
                return "taxi_" + data.getTaxiId();
            }

            @Override
            public String getValueFromData(TaxiAverageSpeed data) {
                return String.format("%.2f", data.getAverageSpeed());
            }
        }));

        // Sink for distance data
        dists.addSink(new RedisSink<>(redisCfg, new RedisMapper<TaxiDistance>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "taxi_distance");
            }

            @Override
            public String getKeyFromData(TaxiDistance data) {
                return "taxi_" + data.getTaxiId();
            }

            @Override
            public String getValueFromData(TaxiDistance data) {
                return String.format("%.2f", data.getDistance());
            }
        }));

        // Sink for dashboard data
        propagateDashboard.addSink(new RedisSink<>(redisCfg, new RedisMapper<String>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.SET);
            }

            @Override
            public String getKeyFromData(String data) {
                return "dashboard_stats";
            }

            @Override
            public String getValueFromData(String data) {
                return data;
            }
        }));

        // Sink for location updates
        locationDashboard.addSink(new RedisSink<>(redisCfg, new RedisMapper<String>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.LPUSH, "location_updates");
            }

            @Override
            public String getKeyFromData(String data) {
                return "location_updates";
            }

            @Override
            public String getValueFromData(String data) {
                return data;
            }
        }));

        // Sink for notifications
        notifications.addSink(new RedisSink<>(redisCfg, new RedisMapper<String>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.LPUSH, "notifications");
            }

            @Override
            public String getKeyFromData(String data) {
                return "notifications";
            }

            @Override
            public String getValueFromData(String data) {
                return data;
            }
        }));

        env.execute("Taxi Stream Processing");
    }

    // Parse TaxiData
    private static TaxiData parseTaxiData(String[] fields) {
        try {
            String id = fields[0].trim();
            long timestamp = parseTimestamp(fields[1].trim());
            double lon = Double.parseDouble(fields[2].trim());
            double lat = Double.parseDouble(fields[3].trim());
            return new TaxiData(id, timestamp, lon, lat);
        } catch (Exception e) {
            return null;
        }
    }

    private static long parseTimestamp(String input) {
        if (input.matches("\\d+")) {
            return Long.parseLong(input);
        }
        try {
            return DATE_FORMAT.parse(input).getTime();
        } catch (ParseException e) {
            return -1;
        }
    }

    // Calculate haversine distance between two points
    private static double haversine(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371; // Earth radius in km
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                        * Math.sin(dLon / 2) * Math.sin(dLon / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c; // Distance in km
    }

    // Data classes
    static class TaxiData {
        private final String taxiId;
        private final long timestamp;
        private final double latitude;
        private final double longitude;

        public TaxiData(String taxiId, long timestamp, double longitude, double latitude ) {
            this.taxiId = taxiId;
            this.timestamp = timestamp;
            this.longitude = longitude;
            this.latitude = latitude; 
        }

        public String getTaxiId() { return taxiId; }
        public long getTimestamp() { return timestamp; }
        public double getLatitude() { return latitude; }
        public double getLongitude() { return longitude; }
    }

    static class TaxiSpeed extends TaxiData {
        private final double speed;

        public TaxiSpeed(String taxiId, long timestamp, double latitude, double longitude, double speed) {
            super(taxiId, timestamp, latitude, longitude);
            this.speed = speed;
        }

        public double getSpeed() { return speed; }
    }

    static class TaxiDistance {
        private final String taxiId;
        private final double distance;

        public TaxiDistance(String taxiId, double distance) {
            this.taxiId = taxiId;
            this.distance = distance;
        }

        public String getTaxiId() { return taxiId; }
        public double getDistance() { return distance; }
    }

    static class TaxiAverageSpeed {
        private final String taxiId;
        private final double averageSpeed;

        public TaxiAverageSpeed(String taxiId, double averageSpeed) {
            this.taxiId = taxiId;
            this.averageSpeed = averageSpeed;
        }

        public String getTaxiId() { return taxiId; }
        public double getAverageSpeed() { return averageSpeed; }
    }

    // Process Functions
    static class CalculateSpeed extends KeyedProcessFunction<String, TaxiData, TaxiSpeed> {
        private transient ValueState<TaxiData> lastState;

        @Override
        public void open(Configuration parameters) {
            lastState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastState", TaxiData.class));
        }

        @Override
        public void processElement(TaxiData current, Context ctx, Collector<TaxiSpeed> out) throws Exception {
            TaxiData prev = lastState.value();
            if (prev != null) {
                double distance = haversine(prev.getLatitude(), prev.getLongitude(),
                        current.getLatitude(), current.getLongitude());
                double timeDiffSec = (current.getTimestamp() - prev.getTimestamp()) / 1000.0;

                if (timeDiffSec > 0) {
                    double speed = (distance / timeDiffSec) * 3600; // km/h
                    out.collect(new TaxiSpeed(current.getTaxiId(), current.getTimestamp(), 
                            current.getLatitude(), current.getLongitude(), speed));
                }
            } else {
                // First record, speed is 0
                out.collect(new TaxiSpeed(current.getTaxiId(), current.getTimestamp(), 
                        current.getLatitude(), current.getLongitude(), 0.0));
            }
            lastState.update(current);
        }
    }

    static class CalculateDistance extends KeyedProcessFunction<String, TaxiData, TaxiDistance> {
        private transient ValueState<TaxiData> lastPoint;
        private transient ValueState<Double> totalDistance;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastPoint = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastPoint", TaxiData.class));
            totalDistance = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("totalDistance", Double.class));
        }

        @Override
        public void processElement(TaxiData current, Context ctx, Collector<TaxiDistance> out) throws Exception {
            TaxiData prev = lastPoint.value();
            double totalDist = totalDistance.value() != null ? totalDistance.value() : 0.0;

            if (prev != null) {
                double dist = haversine(prev.getLatitude(), prev.getLongitude(), 
                        current.getLatitude(), current.getLongitude());
                totalDist += dist;
            }

            lastPoint.update(current);
            totalDistance.update(totalDist);

            out.collect(new TaxiDistance(current.getTaxiId(), totalDist));
        }
    }

    static class CalculateAverageSpeed extends KeyedProcessFunction<String, TaxiSpeed, TaxiAverageSpeed> {
        private transient ValueState<Tuple2<Double, Integer>> speedSumAndCount;

        @Override
        public void open(Configuration parameters) throws Exception {
            speedSumAndCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("speedSumAndCount",
                            TypeInformation.of(new TypeHint<Tuple2<Double, Integer>>() {})));
        }

        @Override
        public void processElement(TaxiSpeed current, Context ctx, Collector<TaxiAverageSpeed> out) throws Exception {
            Tuple2<Double, Integer> sumAndCount = speedSumAndCount.value();
            if (sumAndCount == null) {
                sumAndCount = Tuple2.of(0.0, 0);
            }

            double newSum = sumAndCount.f0 + current.getSpeed();
            int newCount = sumAndCount.f1 + 1;

            speedSumAndCount.update(Tuple2.of(newSum, newCount));

            double avgSpeed = newSum / newCount;
            out.collect(new TaxiAverageSpeed(current.getTaxiId(), avgSpeed));
        }
    }

    static class StoreInformationOperator extends KeyedProcessFunction<String, TaxiData, String> {
        @Override
        public void processElement(TaxiData value, Context ctx, Collector<String> out) {
            out.collect(String.format("Stored: taxi_%s at %.6f,%.6f", 
                    value.getTaxiId(), value.getLatitude(), value.getLongitude()));
        }
    }
    


    static class PropagateLocationToDashboard extends KeyedProcessFunction<String, TaxiData, String> {
        private transient ValueState<Long> lastEmitTime;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastEmitTime = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastEmitTime", Long.class));
        }

        @Override
        public void processElement(TaxiData value, Context ctx, Collector<String> out) throws Exception {
            Long lastTime = lastEmitTime.value();
            long currentTime = value.getTimestamp();

            if (lastTime == null || currentTime - lastTime >= 5000) { // 5 seconds
                String locationUpdate = String.format(
                        "{\"taxiId\":\"%s\",\"lat\":%.6f,\"lon\":%.6f,\"timestamp\":%d}",
                        value.getTaxiId(), value.getLatitude(), value.getLongitude(), currentTime);
                out.collect(locationUpdate);
                lastEmitTime.update(currentTime);
            }
        }
    }

    static class NotifyDashboardOperators extends KeyedProcessFunction<String, TaxiData, String> {
    private transient ValueState<TaxiData> lastLocationState;
    private transient ValueState<Boolean> hasNotifiedSpeedState;
    private transient ValueState<Boolean> hasNotifiedAreaState;
    private transient ValueState<Long> lastSpeedNotificationTime;
    private transient ValueState<Long> lastAreaNotificationTime;
    
    // Notification cooldown period (5 seconds)
    private static final long NOTIFICATION_COOLDOWN_MS = 5000;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastLocationState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastLocation", TaxiData.class));
        hasNotifiedSpeedState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("hasNotifiedSpeed", Boolean.class));
        hasNotifiedAreaState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("hasNotifiedArea", Boolean.class));
        lastSpeedNotificationTime = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastSpeedNotificationTime", Long.class));
        lastAreaNotificationTime = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastAreaNotificationTime", Long.class));
    }

    @Override
    public void processElement(TaxiData current, Context ctx, Collector<String> out) throws Exception {
        TaxiData prev = lastLocationState.value();
        
        // Calculate distance from Forbidden City
        double distFromFC = haversine(current.getLatitude(), current.getLongitude(),
                FORBIDDEN_CITY_LAT, FORBIDDEN_CITY_LON);

        // Check if taxi is outside 15km area - if so, clear all state and stop processing
        if (distFromFC > AREA_RADIUS_15KM) {
            // Clear all state for this taxi
            lastLocationState.clear();
            hasNotifiedSpeedState.clear();
            hasNotifiedAreaState.clear();
            lastSpeedNotificationTime.clear();
            lastAreaNotificationTime.clear();
            
            // Send removal notification to dashboard
            String removalNotification = String.format(
                    "{\"type\":\"TAXI_REMOVED\",\"taxiId\":\"%s\",\"message\":\"Taxi left monitoring area\",\"distance\":%.2f,\"timestamp\":%d}",
                    current.getTaxiId(), distFromFC, current.getTimestamp());
            out.collect(removalNotification);
            return; // Don't process further
        }

        // Check if leaving 10km area
        Boolean hasNotifiedArea = hasNotifiedAreaState.value();
        Long lastAreaNotifyTime = lastAreaNotificationTime.value();
        
        if (distFromFC > AREA_RADIUS_10KM && (hasNotifiedArea == null || !hasNotifiedArea)) {
            // Only notify if we haven't notified before or if cooldown period has passed
            if (lastAreaNotifyTime == null || 
                (current.getTimestamp() - lastAreaNotifyTime) > NOTIFICATION_COOLDOWN_MS) {
                
                String notification = String.format(
                        "{\"type\":\"AREA_VIOLATION\",\"taxiId\":\"%s\",\"message\":\"Taxi leaving 10km area\",\"distance\":%.2f,\"lat\":%.6f,\"lon\":%.6f,\"timestamp\":%d}",
                        current.getTaxiId(), distFromFC, current.getLatitude(), current.getLongitude(), current.getTimestamp());
                out.collect(notification);
                
                hasNotifiedAreaState.update(true);
                lastAreaNotificationTime.update(current.getTimestamp());
                
                System.out.println("AREA ALERT: Taxi " + current.getTaxiId() + " leaving 10km area. Distance: " + distFromFC);
            }
        } else if (distFromFC <= AREA_RADIUS_10KM) {
            // Reset area notification flag when taxi comes back within 10km
            hasNotifiedAreaState.update(false);
        }

        // Check speed if we have previous location
        if (prev != null) {
            double distance = haversine(prev.getLatitude(), prev.getLongitude(),
                    current.getLatitude(), current.getLongitude());
            
            // Convert milliseconds to seconds for more accurate calculation
            double timeDiffSeconds = (current.getTimestamp() - prev.getTimestamp()) / 1000.0;

            if (timeDiffSeconds > 0 && timeDiffSeconds < 3600) { // Reasonable time diff (less than 1 hour)
                // Calculate speed in km/h
                double speedKmh = (distance / timeDiffSeconds) * 3600;
                
                Boolean hasNotifiedSpeed = hasNotifiedSpeedState.value();
                Long lastSpeedNotifyTime = lastSpeedNotificationTime.value();

                if (speedKmh > SPEED_LIMIT_KMH) {
                    // Only notify if we haven't notified recently (cooldown period)
                    if (lastSpeedNotifyTime == null || 
                        (current.getTimestamp() - lastSpeedNotifyTime) > NOTIFICATION_COOLDOWN_MS) {
                        
                        String notification = String.format(
                                "{\"type\":\"SPEED_VIOLATION\",\"taxiId\":\"%s\",\"speed\":%.2f,\"limit\":%.2f,\"lat\":%.6f,\"lon\":%.6f,\"timestamp\":%d}",
                                current.getTaxiId(), speedKmh, SPEED_LIMIT_KMH, 
                                current.getLatitude(), current.getLongitude(), current.getTimestamp());
                        out.collect(notification);
                        
                        hasNotifiedSpeedState.update(true);
                        lastSpeedNotificationTime.update(current.getTimestamp());
                        
                        System.out.println("SPEED ALERT: Taxi " + current.getTaxiId() + " speeding at " + speedKmh + " km/h");
                    }
                } else {
                    // Reset speed notification flag when taxi slows down
                    hasNotifiedSpeedState.update(false);
                }
            }
        }

        // Update last location
        lastLocationState.update(current);
    }
}

    // Updated PropagateToDashboard to filter out taxis outside 15km area
    static class PropagateToDashboard extends KeyedProcessFunction<String, TaxiData, String> {
        private transient MapState<String, TaxiData> activeTaxisState;
        private transient MapState<String, TaxiData> lastLocationState;
        private transient ValueState<Double> cumulativeTotalDistanceState;
        private static final long TAXI_TIMEOUT_MS = 300000; // 5 minutes

        @Override
        public void open(Configuration parameters) throws Exception {
            activeTaxisState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("activeTaxis", Types.STRING, TypeInformation.of(TaxiData.class)));
            
            lastLocationState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("lastLocation", Types.STRING, TypeInformation.of(TaxiData.class)));
            
            cumulativeTotalDistanceState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("cumulativeTotalDistance", Double.class));
        }

        @Override
        public void processElement(TaxiData value, Context ctx, Collector<String> out) throws Exception {
            long currentTime = value.getTimestamp();
            String taxiId = value.getTaxiId();
            
            // Check if taxi is within 15km of Forbidden City
            double distFromFC = haversine(value.getLatitude(), value.getLongitude(),
                    FORBIDDEN_CITY_LAT, FORBIDDEN_CITY_LON);
            
            // If taxi is outside 15km area, remove it from tracking and don't process
            if (distFromFC > AREA_RADIUS_15KM) {
                activeTaxisState.remove(taxiId);
                // Don't remove from lastLocationState as we still need it for distance calculation
                // when the taxi returns
                
                // Continue with statistics calculation without this taxi
            } else {
                // Taxi is within area, process normally
                TaxiData previousLocation = lastLocationState.get(taxiId);
                double cumulativeTotal = cumulativeTotalDistanceState.value() != null ? 
                    cumulativeTotalDistanceState.value() : 0.0;
                
                // Calculate additional distance if we have a previous location
                if (previousLocation != null) {
                    double additionalDistance = haversine(
                        previousLocation.getLatitude(), previousLocation.getLongitude(),
                        value.getLatitude(), value.getLongitude()
                    );
                    cumulativeTotal += additionalDistance;
                    cumulativeTotalDistanceState.update(cumulativeTotal);
                }
                
                // Update states for active taxi
                activeTaxisState.put(taxiId, value);
                lastLocationState.put(taxiId, value);
            }
            
            // Clean up inactive taxis (haven't sent data in 5 minutes)
            Set<String> taxisToRemove = new HashSet<>();
            for (Map.Entry<String, TaxiData> entry : activeTaxisState.entries()) {
                if (currentTime - entry.getValue().getTimestamp() > TAXI_TIMEOUT_MS) {
                    taxisToRemove.add(entry.getKey());
                }
            }
            
            // Remove inactive taxis from activeTaxisState
            for (String inactiveTaxiId : taxisToRemove) {
                activeTaxisState.remove(inactiveTaxiId);
            }
            
            // Calculate statistics for active taxis only (within 15km area)
            int activeTaxiCount = 0;
            for (Map.Entry<String, TaxiData> entry : activeTaxisState.entries()) {
                activeTaxiCount++;
            }
            
            double cumulativeTotal = cumulativeTotalDistanceState.value() != null ? 
                cumulativeTotalDistanceState.value() : 0.0;
            
            // Create dashboard statistics JSON
            String stats = String.format(
                "{\"activeTaxis\":%d,\"totalDistance\":%.2f,\"timestamp\":%d}", 
                activeTaxiCount, cumulativeTotal, currentTime
            );
            
            out.collect(stats);
        }
    }

    // Add a filter operator to remove taxis outside the forbidden area from location updates
    static class FilterTaxisByArea extends KeyedProcessFunction<String, TaxiData, TaxiData> {
        
        @Override
        public void processElement(TaxiData value, Context ctx, Collector<TaxiData> out) throws Exception {
            // Calculate distance from Forbidden City
            double distFromFC = haversine(value.getLatitude(), value.getLongitude(),
                    FORBIDDEN_CITY_LAT, FORBIDDEN_CITY_LON);
            System.out.println("Processing taxi outside zone " + value.getTaxiId() + " at distance " + distFromFC + "lat: " + value.getLatitude() + "lon: " + value.getLongitude() );
            // Only emit taxis that are within 15km of the Forbidden City
            if (distFromFC <= AREA_RADIUS_15KM) {
                System.out.println("Processing taxi " + value.getTaxiId() + " at distance " + distFromFC);
                out.collect(value);
            }
            // Taxis outside 15km area are simply not emitted (filtered out)
        }
    }
}