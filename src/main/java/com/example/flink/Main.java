package com.example.flink;

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

public class Main {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = null;
        String inputTopic = null;
        String redisHost = null;

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

        if (kafkaBootstrap == null || inputTopic == null) {
            System.err.println("Usage: Main "
                    + "--kafka.bootstrap.servers <host:port> "
                    + "--kafka.topic <topicName>");
            System.exit(1);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", kafkaBootstrap);
        consumerProps.setProperty("group.id", "taxi-data-consumer");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                consumerProps);
        kafkaConsumer.setStartFromEarliest();

        DataStream<String> rawStream = env.addSource(kafkaConsumer).name("Kafka Source");

        DataStream<TaxiData> taxiData = rawStream
                .map(line -> {
                    String[] f = line.split(",");
                    if (f.length == 4) {
                        return parseTaxiData(line);
                    } else {
                        System.err.println("Invalid line: " + line);
                        return null;
                    }
                }).name("Parse Taxi Data")
                .filter(x -> x != null).name("Filter Null Taxi Data");

        DataStream<String> storeOp = taxiData
                .keyBy(TaxiData::getTaxiId)
                .process(new StoreInformationOperator()).name("Store Information");

        DataStream<String> dashOp = taxiData
                .keyBy(TaxiData::getTaxiId)
                .process(new PropagateToDashboard()).name("Dashboard Propagation");

        DataStream<TaxiSpeed> enriched = taxiData
                .keyBy(TaxiData::getTaxiId)
                .map(new CalculateSpeed()).name("Calculate Speed");

        DataStream<TaxiDistance> dists = taxiData
                .keyBy(TaxiData::getTaxiId)
                .map(new CalculateDistance()).name("Calculate Distance");

        DataStream<TaxiAverageSpeed> avgSpeeds = enriched
                .keyBy(TaxiSpeed::getTaxiId)
                .map(new CalculateAverageSpeed()).name("Calculate Average Speed");

        FlinkJedisPoolConfig redisCfg = new FlinkJedisPoolConfig.Builder()
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
                return String.valueOf(data.getDistance());
            }
        })).name("Redis Taxi Distance");

        env.execute("Enrich Taxi Data with Speed Calculation");
    }

    private static TaxiData parseTaxiData(String line) {
        String[] f = line.split(",");
        if (f.length == 4) {
            String id = f[0].trim();
            long ts = parseTimestamp(f[1].trim());
            double lat = Double.parseDouble(f[2].trim());
            double lng = Double.parseDouble(f[3].trim());
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

class CalculateSpeed implements MapFunction<TaxiData, TaxiSpeed> {
    TaxiData lastState = null;

    @Override
    public TaxiSpeed map(TaxiData current) {
        if (lastState != null) {
            double distance = haversine(lastState.getLatitude(), lastState.getLongitude(),
                    current.getLatitude(), current.getLongitude());
            double timeDiffSec = (current.getTimestamp() - lastState.getTimestamp()) / 1000.0;

            if (timeDiffSec > 0) {
                double speed = (distance / timeDiffSec) * 3600;
                return new TaxiSpeed(current.getTaxiId(), speed);
            }
        }
        lastState = current;
        return new TaxiSpeed(current.getTaxiId(), 0.0);
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

class CalculateDistance implements MapFunction<TaxiData, TaxiDistance> {
    @Override
    public TaxiDistance map(TaxiData td) {

        return new TaxiDistance(td.getTaxiId(), 0.0);
    }
}

class CalculateAverageSpeed extends RichMapFunction<TaxiSpeed, TaxiAverageSpeed> {

    private transient ValueState<Tuple2<Double, Integer>> sumAndCount;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Tuple2<Double, Integer>> descriptor = new ValueStateDescriptor<>("sumAndCount",
                TypeInformation.of(new TypeHint<Tuple2<Double, Integer>>() {
                }));
        sumAndCount = getRuntimeContext().getState(descriptor);
    }

    @Override
    public TaxiAverageSpeed map(TaxiSpeed value) throws IOException {
        Tuple2<Double, Integer> current = sumAndCount.value();
        if (current == null) {
            current = Tuple2.of(0.0, 0);
        }

        double newSum = current.f0 + value.getSpeed();
        int newCount = current.f1 + 1;
        double avg = newSum / newCount;

        sumAndCount.update(Tuple2.of(newSum, newCount));

        return new TaxiAverageSpeed(value.getTaxiId(), avg);
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
