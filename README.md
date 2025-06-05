docker-compose up --build
docker exec -it kafka bash

//kafka bash
kafka-topics.sh --create --topic taxi_data --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

//kafka bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic taxi_data --from-beginning --max-messages 5

mvn clean package


//kafka bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic taxi_data

//kafka bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic taxi_data --from-beginning

docker exec -it flink-jobmanager bash

//flink bash
mkdir -p /opt/flink/usrlib


docker cp target/taxi-flink-job-1.0-SNAPSHOT.jar flink-jobmanager:/opt/flink/usrlib/

//flink bash
./bin/flink run -c com.example.flink.Main /opt/flink/usrlib/taxi-flink-job-1.0-SNAPSHOT.jar --kafka.bootstrap.servers kafka:9092 --kafka.topic taxi_data --redis.host redis


docker exec -it redis redis-cli

//flink bash
HGETALL speed