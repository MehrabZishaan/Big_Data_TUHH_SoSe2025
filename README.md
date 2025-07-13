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
### Run Flink Job
```bash
docker exec -it flink-jobmanager ./bin/flink run -c com.example.flink.Main /opt/flink/usrlib/taxi-flink-job-1.0-SNAPSHOT.jar --kafka.bootstrap.servers kafka:9092 --kafka.topic taxi_data --redis.host redis
```




# Real-Time Traffic Monitoring System

This project implements a real-time taxi fleet monitoring dashboard using Apache Kafka, Apache Flink, Redis, and a web-based frontend.

## Architecture

The system consists of:
- **Kafka**: Message streaming platform for taxi location data
- **Flink**: Stream processing engine for real-time analytics
- **Redis**: In-memory data store for processed results
- **Node.js Backend**: API server for dashboard data
- **Frontend**: Web dashboard for visualization

## Quick Start with DockerHub Images

### Prerequisites
- Docker and Docker Compose installed
- Internet connection to pull images from DockerHub

### Deployment Steps

1. **Clone the repository** (if not already done):
   ```bash
   git clone <your-repository-url>
   cd <your-repository-name>
   ```

2. **Run the deployment script**:
   ```bash
   chmod +x deploy.sh
   ./deploy.sh
   ```

   Or manually run:
   ```bash
   docker-compose -f docker-compose-dockerhub.yml up -d
   ```

3. **Access the application**:
   - **Dashboard**: http://localhost:5173
   - **Flink Web UI**: http://localhost:8081
   - **Backend API**: http://localhost:5000

### DockerHub Images

The following public Docker images are available:

- `your-dockerhub-username/flink-job:latest` - Flink job with stream processing topology
- `your-dockerhub-username/kafka-producer:latest` - Kafka producer for taxi data
- `your-dockerhub-username/node-backend:latest` - Node.js backend API
- `your-dockerhub-username/frontend:latest` - Web frontend dashboard

### Data Setup

Ensure your taxi data files are placed in the `./kafka-producer/taxi_data/` directory before starting the system.

### Environment Variables

The system uses the following key environment variables:
- `KAFKA_BOOTSTRAP`: Kafka broker address
- `KAFKA_TOPIC`: Topic name for taxi data
- `SPEED_FACTOR`: Data replay speed multiplier
- `REDIS_HOST`: Redis server host
- `REDIS_PORT`: Redis server port

### Monitoring

- **Flink Jobs**: Monitor at http://localhost:8081
- **Kafka Topics**: Use Kafka CLI tools in the kafka container
- **Redis Data**: Use Redis CLI at localhost:6379
- **Application Logs**: `docker-compose logs -f <service-name>`

### Stopping the System

```bash
docker-compose -f docker-compose-dockerhub.yml down
```

To remove all data:
```bash
docker-compose -f docker-compose-dockerhub.yml down -v
```

### Troubleshooting

1. **Services not starting**: Check logs with `docker-compose logs <service-name>`
2. **Port conflicts**: Ensure ports 2181, 9092, 8081, 5000, 5173, 6379 are available
3. **Memory issues**: Increase Docker memory allocation if needed
4. **Data not flowing**: Verify taxi data files are in the correct directory

### Performance Testing

To test maximum throughput:
1. Adjust the `SPEED_FACTOR` environment variable
2. Monitor Flink metrics at http://localhost:8081
3. Check Redis memory usage
4. Observe dashboard update rates

### Development

For local development without DockerHub:
```bash
docker-compose up -d
```

This will build images locally instead of pulling from DockerHub.


🔧 1. Build the Docker Image
If you're using docker-compose.yml with a custom Dockerfile, build the image:

bash
```bash
docker-compose build
```
This builds the image but does not tag it for Docker Hub yet.

🪪 2. Log in to Docker Hub
Make sure you're logged in to Docker Hub:

```bash
docker login
```
Enter your Docker Hub username and password (or personal access token).

🏷️ 3. Tag the Image for Docker Hub
You need to tag the image with your Docker Hub username and desired repository name:

```bash
docker tag <local-image-name> <dockerhub-username>/<repository-name>:<tag>
```
To find the <local-image-name>, run:

```bash
docker images
```
Example:
If you built an image called my-project_web:

```bash
docker tag my-project_web mydockerhubuser/myapp:latest
```


📤 4. Push the Image to Docker Hub
```bash
docker push mydockerhubuser/myapp:latest
```



## 🌐 Final Project Deployment

Our final project is deployed and accessible via the following URL:

🔗 **Project URL:** http://48.209.9.235:5173/

🕒 **Availability for Grading:**  
Please access the project during our assigned session time:  
**July 25 : (9:00 – 17:00)**
