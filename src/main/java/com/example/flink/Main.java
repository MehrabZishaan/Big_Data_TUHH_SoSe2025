package com.example.flink;

// Core Flink APIs
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
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

/**
 * Main Flink pipeline for processing taxi data.
 * Reads from Kafka, calculates speed, distance, and writes to Redis.
 */
public class Main {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = null;
        String inputTopic = null;
        String redisHost = null;

        // Parse command line arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--kafka.bootstrap.servers":
                    if (i + 1 < args.length) {
                        kafkaBootstrap = args[++i];
                    } else {
                        System.err.println("Missing value for --kafka.bootstrap.servers");
                        System.exit(1);
                    }
                    break;
                case "--kafka.topic":
                    if (i + 1 < args.length) {
                        inputTopic = args[++i];
                    } else {
                        System.err.println("Missing value for --kafka.topic");
                        System.exit(1);
                    }
                    break;
                case "--redis.host":
                    if (i + 1 < args.length) {
                        redisHost = args[++i];
                    } else {
                        System.err.println("Missing value for --redis.host");
                        System.exit(1);
                    }
                    break;
                default:
                    break;
            }
        }

        if (kafkaBootstrap == null || inputTopic == null || redisHost == null) {
            System.err.println("Usage: Main "
                    + "--kafka.bootstrap.servers <host:port> "
                    + "--kafka.topic <topicName> "
                    + "--redis.host <host>");
            System.exit(1);
        }

        // Set up Flink streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure Kafka consumer
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", kafkaBootstrap);
        consumerProps.setProperty("group.id", "taxi-data-consumer");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                consumerProps);
        kafkaConsumer.setStartFromEarliest();

        // Create the data stream from Kafka
        DataStream<String> rawStream = env.addSource(kafkaConsumer);

        // Parse the raw data into TaxiData objects
        DataStream<TaxiData> taxiData = rawStream
                .map(line -> {
                    if ("END".equals(line.trim())) {
                        return null;
                    }
                    String[] f = line.split(",");
                    if (f.length == 4) {
                        return parseTaxiData(line);
                    } else {
                        System.err.println("Invalid line: " + line);
                        return null;
                    }
                })
                .filter(x -> x != null);

        // Configure Redis connection
        FlinkJedisPoolConfig redisCfg = new FlinkJedisPoolConfig.Builder()
                .setHost(redisHost)
                .setPort(6379)
                .build();

        // Calculate speed between consecutive points
        DataStream<TaxiSpeed> speedData = taxiData
                .keyBy(TaxiData::getTaxiId)
                .process(new CalculateSpeed());

        // Calculate total distance traveled
        DataStream<TaxiDistance> distanceData = taxiData
                .keyBy(TaxiData::getTaxiId)
                .process(new CalculateDistance());

        // Calculate average speed
        DataStream<TaxiAverageSpeed> avgSpeedData = taxiData
                .keyBy(TaxiData::getTaxiId)
                .process(new CalculateAverageSpeed());

        // Detect speeding violations
        DataStream<String> speedingAlerts = speedData
                .filter(speed -> speed.getSpeed() > 50.0)
                .map(speed -> String.format("SPEEDING: Taxi %s at %.1f km/h",
                        speed.getTaxiId(), speed.getSpeed()));

        // Detect geofence violations
        DataStream<String> geofenceAlerts = taxiData
                .keyBy(TaxiData::getTaxiId)
                .process(new GeofenceMonitor())
                .filter(alert -> alert != null);

        // Combine all alerts
        DataStream<String> allAlerts = speedingAlerts.union(geofenceAlerts);

        // Store data in Redis
        taxiData.addSink(new RedisSink<>(redisCfg, new RedisTaxiLocationMapper()));
        speedData.addSink(new RedisSink<>(redisCfg, new RedisSpeedMapper()));
        avgSpeedData.addSink(new RedisSink<>(redisCfg, new RedisAverageSpeedMapper()));
        distanceData.addSink(new RedisSink<>(redisCfg, new RedisDistanceMapper()));
        allAlerts.addSink(new RedisSink<>(redisCfg, new RedisAlertMapper()));

        // Execute the Flink job
        env.execute("Taxi Fleet Monitoring Pipeline");
    }

    private static TaxiData parseTaxiData(String line) {
        String[] f = line.split(",");
        if (f.length == 4) {
            String id = f[0].trim();
            long ts = parseTimestamp(f[1].trim());
            double lat = Double.parseDouble(f[3].trim());
            double lng = Double.parseDouble(f[2].trim());
            return new TaxiData(id, ts, lat, lng);
        } else {
            throw new IllegalArgumentException("Invalid line: " + line);
        }
    }

    private static long parseTimestamp(String s) {
        if (s.matches("\\d+")) {
            return Long.parseLong(s);
        }
        try {
            Date d = DATE_FORMAT.parse(s);
            return d.getTime();
        } catch (ParseException e) {
            throw new IllegalArgumentException("Bad timestamp: " + s, e);
        }
    }

    // Redis mappers
    public static class RedisTaxiLocationMapper implements RedisMapper<TaxiData> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "taxi_locations");
        }

        @Override
        public String getKeyFromData(TaxiData data) {
            return data.getTaxiId();
        }

        @Override
        public String getValueFromData(TaxiData data) {
            return String.format("%.6f,%.6f,%d", data.getLatitude(), data.getLongitude(), data.getTimestamp());
        }
    }

    public static class RedisSpeedMapper implements RedisMapper<TaxiSpeed> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "speed");
        }

        @Override
        public String getKeyFromData(TaxiSpeed data) {
            return data.getTaxiId();
        }

        @Override
        public String getValueFromData(TaxiSpeed data) {
            return String.format("%.2f", data.getSpeed());
        }
    }

    public static class RedisAverageSpeedMapper implements RedisMapper<TaxiAverageSpeed> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "average_speed");
        }

        @Override
        public String getKeyFromData(TaxiAverageSpeed data) {
            return data.getTaxiId();
        }

        @Override
        public String getValueFromData(TaxiAverageSpeed data) {
            return String.format("%.2f", data.getAverageSpeed());
        }
    }

    public static class RedisDistanceMapper implements RedisMapper<TaxiDistance> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "taxi_distance");
        }

        @Override
        public String getKeyFromData(TaxiDistance data) {
            return data.getTaxiId();
        }

        @Override
        public String getValueFromData(TaxiDistance data) {
            return String.format("%.2f", data.getDistance());
        }
    }

    public static class RedisAlertMapper implements RedisMapper<String> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH, "alerts");
        }

        @Override
        public String getKeyFromData(String data) {
            return "alerts";
        }

        @Override
        public String getValueFromData(String data) {
            return data;
        }
    }

    // Data classes
    public static class TaxiData {
        private final String taxiId;
        private final long timestamp;
        private final double latitude;
        private final double longitude;

        public TaxiData(String taxiId, long timestamp, double latitude, double longitude) {
            this.taxiId = taxiId;
            this.timestamp = timestamp;
            this.latitude = latitude;
            this.longitude = longitude;
        }

        public String getTaxiId() {
            return taxiId;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public double getLatitude() {
            return latitude;
        }

        public double getLongitude() {
            return longitude;
        }
    }

    public static class TaxiSpeed {
        private final String taxiId;
        private final double speed;

        public TaxiSpeed(String taxiId, double speed) {
            this.taxiId = taxiId;
            this.speed = speed;
        }

        public String getTaxiId() {
            return taxiId;
        }

        public double getSpeed() {
            return speed;
        }
    }

    public static class TaxiDistance {
        private final String taxiId;
        private final double distance;

        public TaxiDistance(String taxiId, double distance) {
            this.taxiId = taxiId;
            this.distance = distance;
        }

        public String getTaxiId() {
            return taxiId;
        }

        public double getDistance() {
            return distance;
        }
    }

    public static class TaxiAverageSpeed {
        private final String taxiId;
        private final double averageSpeed;

        public TaxiAverageSpeed(String taxiId, double averageSpeed) {
            this.taxiId = taxiId;
            this.averageSpeed = averageSpeed;
        }

        public String getTaxiId() {
            return taxiId;
        }

        public double getAverageSpeed() {
            return averageSpeed;
        }
    }

    // Operator implementations
    public static class CalculateSpeed extends KeyedProcessFunction<String, TaxiData, TaxiSpeed> {
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
                    double speed = (distance / timeDiffSec) * 3600;
                    out.collect(new TaxiSpeed(current.getTaxiId(), speed));
                }
            }
            lastState.update(current);
        }

        private double haversine(double lat1, double lon1, double lat2, double lon2) {
            final int R = 6371;
            double dLat = Math.toRadians(lat2 - lat1);
            double dLon = Math.toRadians(lon2 - lon1);
            double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                    + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                            * Math.sin(dLon / 2) * Math.sin(dLon / 2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            return R * c;
        }
    }

    public static class CalculateDistance extends KeyedProcessFunction<String, TaxiData, TaxiDistance> {
        private transient ValueState<TaxiData> lastPoint;
        private transient ValueState<Double> totalDistance;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastPoint = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastPoint", TaxiData.class));
            totalDistance = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("totalDistance", Double.class, 0.0));
        }

        @Override
        public void processElement(TaxiData current, Context ctx, Collector<TaxiDistance> out) throws Exception {
            TaxiData prev = lastPoint.value();
            Double totalDist = totalDistance.value();

            if (prev != null) {
                double dist = haversine(prev.getLatitude(), prev.getLongitude(),
                        current.getLatitude(), current.getLongitude());
                totalDist += dist;
            }

            lastPoint.update(current);
            totalDistance.update(totalDist);
            out.collect(new TaxiDistance(current.getTaxiId(), totalDist));
        }

        private double haversine(double lat1, double lon1, double lat2, double lon2) {
            final int R = 6371;
            double dLat = Math.toRadians(lat2 - lat1);
            double dLon = Math.toRadians(lon2 - lon1);
            double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                    + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                            * Math.sin(dLon / 2) * Math.sin(dLon / 2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            return R * c;
        }
    }

    public static class CalculateAverageSpeed extends KeyedProcessFunction<String, TaxiData, TaxiAverageSpeed> {
        private transient ValueState<TaxiData> firstPoint;
        private transient ValueState<TaxiData> lastPoint;
        private transient ValueState<Double> totalDistance;

        @Override
        public void open(Configuration parameters) throws Exception {
            firstPoint = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("firstPoint", TaxiData.class));
            lastPoint = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastPoint", TaxiData.class));
            totalDistance = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("totalDistance", Double.class, 0.0));
        }

        @Override
        public void processElement(TaxiData current, Context ctx, Collector<TaxiAverageSpeed> out) throws Exception {
            TaxiData first = firstPoint.value();
            TaxiData last = lastPoint.value();
            Double totalDist = totalDistance.value();

            if (first == null) {
                first = current;
                totalDist = 0.0;
            } else {
                double dist = haversine(last.getLatitude(), last.getLongitude(),
                        current.getLatitude(), current.getLongitude());
                totalDist += dist;
            }

            firstPoint.update(first);
            lastPoint.update(current);
            totalDistance.update(totalDist);

            long timeDiffMs = current.getTimestamp() - first.getTimestamp();
            if (timeDiffMs > 0) {
                double hours = timeDiffMs / 3600000.0;
                double avgSpeed = totalDist / hours;
                out.collect(new TaxiAverageSpeed(current.getTaxiId(), avgSpeed));
            }
        }

        private double haversine(double lat1, double lon1, double lat2, double lon2) {
            final int R = 6371;
            double dLat = Math.toRadians(lat2 - lat1);
            double dLon = Math.toRadians(lon2 - lon1);
            double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                    + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                            * Math.sin(dLon / 2) * Math.sin(dLon / 2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            return R * c;
        }
    }

    public static class GeofenceMonitor extends KeyedProcessFunction<String, TaxiData, String> {
        private static final double CENTER_LAT = 39.916;
        private static final double CENTER_LON = 116.397;
        private static final double WARNING_RADIUS = 10.0;
        private static final double DROP_RADIUS = 15.0;

        @Override
        public void processElement(TaxiData value, Context ctx, Collector<String> out) throws Exception {
            double distance = haversine(CENTER_LAT, CENTER_LON,
                    value.getLatitude(), value.getLongitude());

            if (distance > WARNING_RADIUS && distance <= DROP_RADIUS) {
                out.collect(String.format("GEOFENCE: Taxi %s %.1f km from center",
                        value.getTaxiId(), distance));
            }
        }

        private double haversine(double lat1, double lon1, double lat2, double lon2) {
            final int R = 6371;
            double dLat = Math.toRadians(lat2 - lat1);
            double dLon = Math.toRadians(lon2 - lon1);
            double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                    + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                            * Math.sin(dLon / 2) * Math.sin(dLon / 2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            return R * c;
        }
    }
}