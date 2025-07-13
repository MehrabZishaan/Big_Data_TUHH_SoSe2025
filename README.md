# 🚕 Real-Time Traffic Monitoring System

A comprehensive real-time taxi fleet monitoring dashboard that processes streaming GPS data using Apache Kafka, Apache Flink, and Redis to provide live analytics and alerts for taxi operations in Beijing.

## 📋 Project Overview

This system implements a complete streaming data pipeline that:
- **Ingests** taxi GPS coordinates from historical data files
- **Processes** location data in real-time to calculate speed, distance, and detect violations
- **Monitors** geofence boundaries around Beijing's Forbidden City
- **Visualizes** live taxi movements and fleet statistics on a web dashboard
- **Alerts** operators about speeding violations and boundary breaches

### Core Components

- **📡 Kafka Producer** - Replays taxi GPS data with configurable speed
- **⚡ Apache Flink** - Stream processing for real-time calculations
- **🔴 Redis** - In-memory storage for processed results
- **🖥️ Node.js Backend** - REST API and WebSocket server
- **🌐 Web Dashboard** - Real-time visualization interface

## 🧰 Technologies Used

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Message Streaming** | Apache Kafka | Data ingestion and event streaming |
| **Stream Processing** | Apache Flink | Real-time data processing and analytics |
| **Data Storage** | Redis | Fast in-memory data store |
| **Backend API** | Node.js + Express | REST API and WebSocket communication |
| **Frontend** | React.js | Interactive web dashboard |
| **Containerization** | Docker & Docker Compose | Service orchestration |
| **Data Source** | T-Drive Dataset | Beijing taxi trajectory data |

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose installed
- At least 8GB RAM available for containers
- Ports 2181, 9092, 8081, 5000, 5173, 6379 available

### Option 1: Deploy with DockerHub Images (Recommended)

```bash
# Clone the repository
git clone https://collaborating.tuhh.de/e-19/teaching/bd25_project_f2_b.git
cd BD25_Project_F2_B

# Start all services using pre-built images
docker-compose -f docker-compose-dockerhub.yml up -d

# Run Flink job
docker-compose -f docker-compose-dockerhub.yml exec flink-jobmanager flink run -c com.example.flink.Main /opt/flink/usrlib/taxi-flink-job-1.0-SNAPSHOT.jar --kafka.bootstrap.servers kafka:9092 --kafka.topic taxi_data --redis.host redis

# Access the dashboard; open in your browser
http://localhost:5173
```

### Option 2: Build from Source for Development

```bash
# Build and start all services
docker-compose up --build -d

# Run Flink job
docker-compose exec flink-jobmanager flink run -c com.example.flink.Main /opt/flink/usrlib/taxi-flink-job-1.0-SNAPSHOT.jar --kafka.bootstrap.servers kafka:9092 --kafka.topic taxi_data --redis.host redis

# Monitor startup logs
docker-compose logs -f

# Access the dashboard; open in your browser
http://localhost:5173
```
## Simulation Speed
To adjust the speed of data replay, modify the `SPEED_FACTOR` environment variable in the `docker-compose.yml` file under ` kafka-producer` container or set it directly in the terminal:

```bash
export SPEED_FACTOR=0.1  # Default is 0.1, increase for faster replay
```

## Dockerhub Images
We have pre-built Docker images available on DockerHub for easy deployment:
- **flink-job**: ` hasebsiddiqui/flink-job:latest`
- **kafka-producer**: `hasebsiddiqui/kafka-producer:latest`
- **Node.js Backend**: `hasebsiddiqui/node-backend:latest`
- **Web Dashboard**: `hasebsiddiqui/web-dashboard:latest`


## 📈 Performance Optimizations

### Kafka Producer Optimizations
#### Memory Management

- Chunk-based Processing: Configurable `CHUNK_SIZE` (default: 1000) prevents memory exhaustion with large datasets
- Lazy Loading: Files loaded in chunks only when needed, maintaining O(k × chunk_size) memory usage
- Buffer Management: Intelligent buffering reduces I/O operations

#### Real-time Synchronization

- Min-heap Event Scheduling: Ensures events with identical timestamps across multiple files are processed simultaneously
- Temporal Ordering: Maintains accurate chronological sequence across all taxi trajectories
- Configurable Replay Speed: SPEED_FACTOR parameter allows accelerated testing

#### Producer Performance

- Message Batching: Groups messages and flushes every 100 messages for optimal throughput
- Buffer Overflow Handling: Automatic flush and retry mechanism prevents data loss
- Asynchronous Delivery: Non-blocking message production with delivery callbacks

### Flink Job Optimizations
- `Stream Processing` - Early filtering of data to reduce load
- `Throughput` - Parallel processing and configurable replay speeds

## 📊 Dashboard Features

### Real-Time Map View
- **Live taxi positions** with 5-second updates
- **Speed indicators** with color-coded markers
- **Geofence visualization** around Forbidden City
- **Interactive controls** for zoom and pan

### Fleet Statistics
- **Active Taxis Count** - Currently driving vehicles
- **Total Distance** - Cumulative distance traveled by all taxis
- **Speed Violations** - Real-time speeding incidents
- **Boundary Alerts** - Geofence violation warnings

### Alert System
- **Speeding Alerts** - Taxis exceeding 50 km/h limit
- **Geofence Warnings** - Vehicles leaving 10KM warning zone
- **Drop Zone Notifications** - Taxis outside 15KM drop zone
- **Real-time Notifications** - Live alert feed with timestamps

## ⚙️ Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `taxi_data` | Topic name for GPS data |
| `SPEED_FACTOR` | `0.1` | Data replay speed multiplier |
| `REDIS_HOST` | `redis` | Redis server hostname |
| `REDIS_PORT` | `6379` | Redis server port |

### Geofence Settings

- **Warning Zone**: 10km radius from Forbidden City (39.916°N, 116.397°E)
- **Drop Zone**: 15km radius - taxis beyond this are excluded
- **Speed Limit**: 50 km/h for violation detection

## 🐛 Troubleshooting

### Common Issues

**Services won't start**
```bash
# Check port conflicts
netstat -tulpn | grep :9092
# Stop conflicting services
sudo systemctl stop kafka

# Clear Docker volumes
docker-compose down -v
docker system prune -f
```

**No data flowing**
```bash
# Verify Kafka topic exists
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Check producer logs
docker-compose logs kafka-producer

# Verify Flink job is running
curl http://localhost:8081/jobs
```
**High memory usage**
```bash
# Reduce data replay speed
export SPEED_FACTOR=0.05
```

### Windows-Specific Issues

```bash
# Port binding errors
net stop winnat
net start winnat
```