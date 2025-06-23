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
 * Main entry point for the Flink application.
 * Sets up Kafka consumer, processes taxi data, and writes results to Redis.
 */

public class Main {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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

        // Parse raw CSV lines into TaxiData objects
        DataStream<TaxiData> taxiStream = rawStream
                .map(line -> {
                    String[] parts = line.split(",");
                    if (parts.length != 4) return null;
                    return parseTaxiData(parts);
                })
                .filter(data -> data != null);

        // Calculate speed per taxi
        DataStream<TaxiSpeed> speedStream = taxiStream
                .keyBy(TaxiData::getTaxiId)
                .process(new CalculateSpeed());

        // Calculate distance per taxi
        DataStream<TaxiDistance> distanceStream = taxiStream
                .keyBy(TaxiData::getTaxiId)
                .process(new CalculateDistance());

        // Calculate average speed per taxi
        DataStream<TaxiAverageSpeed> avgSpeedStream = taxiStream
                .keyBy(TaxiData::getTaxiId)
                .process(new CalculateAverageSpeed());

        // Configure Redis connection
        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost(redisHost)
                .setPort(6379)
                .build();

        taxiData.addSink(new RedisSink<>(redisCfg, new RedisMapper<TaxiData>() {
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
                return String.format("{\"timestamp\":%d,\"latitude\":%.6f,\"longitude\":%.6f}",
                        data.getTimestamp(), data.getLatitude(), data.getLongitude());
            }
        })).name("Redis Taxi Data");

        enriched.addSink(new RedisSink<>(redisCfg, new RedisExampleMapper()))
                .name("Redis Speed Data");

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
        })).name("Redis Avg. Speed Data");

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
        })).name("Redis Taxi Distance");

        env.execute("Enrich Taxi Data with Speed Calculation");
    }
    
    // Parse TaxiData from String array
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

    private static String parseTaxiId(String line) {
        return line.split(",")[0].trim();
    }

    public static class RedisExampleMapper implements RedisMapper<TaxiSpeed> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "speed");
        }

        @Override
        public String getKeyFromData(TaxiSpeed data) {
            return "taxi_" + data.getTaxiId();
        }

        @Override
        public String getValueFromData(TaxiSpeed data) {
            return String.format("%.2f", data.getSpeed());
        }
    }
}

class TaxiData {
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

class TaxiSpeed {
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

class TaxiDistance {
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

class TaxiAverageSpeed {
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

class CalculateSpeed extends KeyedProcessFunction<String, TaxiData, TaxiSpeed> {
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
        final int R = 6371; // Earth radius in km
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                        * Math.sin(dLon / 2) * Math.sin(dLon / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c; // Distance in km
    }
}

class CalculateDistance extends KeyedProcessFunction<String, TaxiData, TaxiDistance> {

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
            double dist = haversine(prev.getLatitude(), prev.getLongitude(), current.getLatitude(),
                    current.getLongitude());
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

class CalculateAverageSpeed extends KeyedProcessFunction<String, TaxiData, TaxiAverageSpeed> {

    private transient ValueState<TaxiData> firstPoint;
    private transient ValueState<TaxiData> lastPoint;
    private transient ValueState<Double> totalDistance;

    @Override
    public void open(Configuration parameters) throws Exception {
        firstPoint = getRuntimeContext().getState(new ValueStateDescriptor<>("firstPoint", TaxiData.class));
        lastPoint = getRuntimeContext().getState(new ValueStateDescriptor<>("lastPoint", TaxiData.class));
        totalDistance = getRuntimeContext().getState(new ValueStateDescriptor<>("totalDistance", Double.class));
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
            double dist = haversine(last.getLatitude(), last.getLongitude(), current.getLatitude(),
                    current.getLongitude());
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

class StoreInformationOperator extends KeyedProcessFunction<String, TaxiData, String> {
    @Override
    public void processElement(
            TaxiData value,
            Context ctx,
            Collector<String> out) {
        out.collect(value.getTaxiId() + ",stored");
    }
}

class PropagateToDashboard extends KeyedProcessFunction<String, TaxiData, String> {
    @Override
    public void processElement(
            TaxiData value,
            Context ctx,
            Collector<String> out) {
        out.collect(value.getTaxiId() + ",dashboard");
    }
}

class PropagateLocationToDashboard extends KeyedProcessFunction<String, String, String> {
    @Override
    public void processElement(
            String value,
            Context ctx,
            Collector<String> out) {
        out.collect(value);
    }
}

class NotifyDashboardOperators extends KeyedProcessFunction<String, String, String> {
    @Override
    public void processElement(
            String value,
            Context ctx,
            Collector<String> out) {
        out.collect(value + ",notify");
    }
}
