# 🚕 Real-Time Traffic Monitoring

This project sets up a real-time data processing pipeline using **Apache Kafka**, **Apache Flink**, and **Redis** to track and analyze live taxi movement data.

## 🧰 Technologies Used

- **Apache Kafka** – for message streaming and ingestion  
- **Apache Flink** – for real-time stream processing  
- **Redis** – for storing processed results like speed, location, and distance  
- **Docker & Docker Compose** – for containerized deployment

## 🚀 Getting Started

### Step 1: Start All Services

```bash
docker-compose up --build
```

This will build and launch Kafka, Flink (JobManager + TaskManager), Redis, and your Kafka producer.

### Step 2: Create Kafka Topic

Open a new terminal and run:

```bash
docker exec -it kafka bash
```

Then inside the Kafka container:

```bash
kafka-topics.sh --create --topic taxi_data --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```

To check the first few messages (optional):

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic taxi_data --from-beginning --max-messages 5
```

### Step 3: Build the Flink Job

From your **host machine** (not inside Docker):

```bash
mvn clean package
```

This will generate the job JAR:  
`target/taxi-flink-job-1.0-SNAPSHOT.jar`

### Step 4: Deploy the Flink Job

1. Enter the Flink JobManager container:

```bash
docker exec -it flink-jobmanager bash
```

2. Inside the container, create a directory:

```bash
mkdir -p /opt/flink/usrlib
```

3. Exit the container and run this on your host:

```bash
docker cp target/taxi-flink-job-1.0-SNAPSHOT.jar flink-jobmanager:/opt/flink/usrlib/
```

4. Go back into the Flink container:

```bash
docker exec -it flink-jobmanager bash
```

5. Start the Flink job:

```bash
./bin/flink run -c com.example.flink.Main /opt/flink/usrlib/taxi-flink-job-1.0-SNAPSHOT.jar \
  --kafka.bootstrap.servers kafka:9092 \
  --kafka.topic taxi_data \
  --redis.host redis
```

### Step 5: Verify Output in Redis

Enter the Redis CLI:

```bash
docker exec -it redis redis-cli
```

Then run:

```bash
HGETALL taxi_location
HGETALL taxi_speed
HGETALL taxi_distance
HGETALL average_speed
```

## 🧪 Optional: Monitor Kafka in Real-Time

To watch all messages from the beginning:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic taxi_data \
  --from-beginning
```

To see only new incoming messages:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic taxi_data
```

## ✅ Final Result

- Kafka receives streaming taxi GPS data  
- Flink calculates speed, distance, and stores it  
- Redis holds the final values for querying or dashboard use

Everything is now ready for real-time taxi tracking!



## Regular Commands
### Start everything and build all images:
```bash
docker-compose up --build
```
### If you changed anything inside flink-job/
```bash
docker-compose build flink-jobmanager
docker-compose up -d flink-jobmanager
```

### OR rebuild all services cleanly:
```bash
docker-compose up --build -d
```

### Common Errors
Frequent error on Windows
`Ports are not available: listen tcp 0.0.0.0/50070: bind: An attempt was made to access a socket in a way forbidden by its access permissions`

```bash
net stop winnat
net start winnat
```
