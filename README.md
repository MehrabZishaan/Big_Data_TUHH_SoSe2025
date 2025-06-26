# 🚕 Real-Time Traffic Monitoring


---

## 🧰 Technologies Used

- Apache Kafka (for message streaming)
- Apache Flink (for stream processing)
- Redis (for storing and querying processed data)
- Docker & Docker Compose (for environment setup)

---

## 🚀 Getting Started

### Step 1: Start All Services

```bash
docker-compose up --build

### Step 2: Create Kafka Topic
docker exec -it kafka bash

//kafka bash
kafka-topics.sh --create --topic taxi_data --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

//kafka bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic taxi_data --from-beginning --max-messages 5

### Step 3: Build the Flink Job
mvn clean package


//kafka bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic taxi_data

//kafka bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic taxi_data --from-beginning

### Step 4: Deploy the Flink Job
docker exec -it flink-jobmanager bash

//flink bash
mkdir -p /opt/flink/usrlib


docker cp target/taxi-flink-job-1.0-SNAPSHOT.jar flink-jobmanager:/opt/flink/usrlib/

//flink bash
./bin/flink run -c com.example.flink.Main /opt/flink/usrlib/taxi-flink-job-1.0-SNAPSHOT.jar --kafka.bootstrap.servers kafka:9092 --kafka.topic taxi_data --redis.host redis


### Step 5: Verify Data in Redis
docker exec -it redis redis-cli

//flink bash
HGETALL speed