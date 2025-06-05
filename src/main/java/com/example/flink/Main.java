package com.example.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;


public class Main {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = null;
        String inputTopic     = null;
        // String redisHost      = null;

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
            consumerProps
        );
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

        DataStream<String> enriched = rawStream
            .keyBy(Main::parseTaxiId)
            .map(new CalculateSpeed()).name("Calculate Speed");

        DataStream<TaxiDistance> dists = taxiData
            .keyBy(TaxiData::getTaxiId)
            .map(new CalculateDistance()).name("Calculate Distance");

        DataStream<TaxiSpeed> speeds = enriched
            .flatMap(new FlatMapFunction<String, TaxiSpeed>() {
                @Override
                public void flatMap(String line, Collector<TaxiSpeed> out) {
                    String[] f = line.split(",");
                    if (f.length >= 2) {
                        String id = f[0].trim();
                        double sp = 0;
                        try { sp = Double.parseDouble(f[1].trim()); }
                        catch (Exception e) {}
                        out.collect(new TaxiSpeed(id, sp));
                    }
                }
            }).name("Extract Taxi Speed");

        DataStream<TaxiAverageSpeed> avgSpeeds = speeds
            .keyBy(TaxiSpeed::getTaxiId)
            .map(new CalculateAverageSpeed()).name("Calculate Average Speed");

        DataStream<Tuple2<String, String>> redisFeed = enriched
            .flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                @Override
                public void flatMap(String data, Collector<Tuple2<String, String>> out) {
                    String[] f = data.split(",");
                    if (f.length >= 4) {
                        String id = f[0].trim();
                        String lng = f[2].trim();
                        String lat = f[3].trim();
                        String sp = f[1].trim();
                        out.collect(new Tuple2<>("longitude_" + id, lng));
                        out.collect(new Tuple2<>("latitude_" + id, lat));
                        out.collect(new Tuple2<>("speed_" + id, sp));
                    }
                }
            }).name("Extract Redis Feed");

        // FlinkJedisPoolConfig redisCfg = new FlinkJedisPoolConfig.Builder()
        //     .setHost(redisHost)
        //     .setPort(6379)
        //     .build();

        // redisFeed.addSink(new RedisSink<>(redisCfg, new RedisExampleMapper()));

        // avgSpeeds.addSink(new RedisSink<>(redisCfg, new RedisMapper<TaxiAverageSpeed>() {
        //     @Override
        //     public RedisCommandDescription getCommandDescription() {
        //         return new RedisCommandDescription(RedisCommand.HSET, "average_speed");
        //     }
        //     @Override
        //     public String getKeyFromData(TaxiAverageSpeed data) {
        //         return "taxi_" + data.getTaxiId();
        //     }
        //     @Override
        //     public String getValueFromData(TaxiAverageSpeed data) {
        //         return String.valueOf(data.getAverageSpeed());
        //     }
        // }));

        // dists.addSink(new RedisSink<>(redisCfg, new RedisMapper<TaxiDistance>() {
        //     @Override
        //     public RedisCommandDescription getCommandDescription() {
        //         return new RedisCommandDescription(RedisCommand.HSET, "taxi_distance");
        //     }
        //     @Override
        //     public String getKeyFromData(TaxiDistance data) {
        //         return "taxi_" + data.getTaxiId();
        //     }
        //     @Override
        //     public String getValueFromData(TaxiDistance data) {
        //         return String.valueOf(data.getDistance());
        //     }
        // }));

        env.execute("Enrich Taxi Data with Speed Calculation");
    }

    private static TaxiData parseTaxiData(String line) {
        String[] f = line.split(",");
        if (f.length == 4) {
            String id  = f[0].trim();
            long ts    = parseTimestamp(f[1].trim());
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

    public static class RedisExampleMapper implements RedisMapper<Tuple2<String, String>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "speed");
        }
        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }
        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
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
    public String getTaxiId()  { return taxiId; }
    public long   getTimestamp() { return timestamp; }
    public double getLatitude()  { return latitude; }
    public double getLongitude() { return longitude; }
}

class TaxiSpeed {
    private final String taxiId;
    private final double speed;

    public TaxiSpeed(String taxiId, double speed) {
        this.taxiId = taxiId;
        this.speed = speed;
    }
    public String getTaxiId() { return taxiId; }
    public double getSpeed()  { return speed; }
}

class TaxiDistance {
    private final String taxiId;
    private final double distance;

    public TaxiDistance(String taxiId, double distance) {
        this.taxiId = taxiId;
        this.distance = distance;
    }
    public String getTaxiId()   { return taxiId; }
    public double getDistance() { return distance; }
}

class TaxiAverageSpeed {
    private final String taxiId;
    private final double averageSpeed;

    public TaxiAverageSpeed(String taxiId, double averageSpeed) {
        this.taxiId = taxiId;
        this.averageSpeed = averageSpeed;
    }
    public String getTaxiId()       { return taxiId; }
    public double getAverageSpeed() { return averageSpeed; }
}

class CalculateSpeed implements MapFunction<String, String> {
    @Override
    public String map(String line) {
        String[] f = line.split(",");
        if (f.length != 4) {
            return f[0].trim() + ",0.0";
        }
        String id = f[0].trim();
        double sp = 0.0; 
        return id + "," + sp + "," + f[2].trim() + "," + f[3].trim();
    }
}

class CalculateDistance implements MapFunction<TaxiData, TaxiDistance> {
    @Override
    public TaxiDistance map(TaxiData td) {

        return new TaxiDistance(td.getTaxiId(), 0.0);
    }
}

class CalculateAverageSpeed implements MapFunction<TaxiSpeed, TaxiAverageSpeed> {
    @Override
    public TaxiAverageSpeed map(TaxiSpeed ts) {
        return new TaxiAverageSpeed(ts.getTaxiId(), ts.getSpeed());
    }
}

class StoreInformationOperator extends KeyedProcessFunction<String, TaxiData, String> {
    @Override
    public void processElement(
        TaxiData value,
        Context ctx,
        Collector<String> out
    ) {
        out.collect(value.getTaxiId() + ",stored");
    }
}

class PropagateToDashboard extends KeyedProcessFunction<String, TaxiData, String> {
    @Override
    public void processElement(
        TaxiData value,
        Context ctx,
        Collector<String> out
    ) {
        out.collect(value.getTaxiId() + ",dashboard");
    }
}

class PropagateLocationToDashboard extends KeyedProcessFunction<String, String, String> {
    @Override
    public void processElement(
        String value,
        Context ctx,
        Collector<String> out
    ) {
        out.collect(value);
    }
}

class NotifyDashboardOperators extends KeyedProcessFunction<String, String, String> {
    @Override
    public void processElement(
        String value,
        Context ctx,
        Collector<String> out
    ) {
        out.collect(value + ",notify");
    }
}
