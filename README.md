# Big Data TUHH

## About

This repository contains a **group project** developed for the **Big
Data course at Hamburg University of Technology (TUHH)** during the
**Summer Semester 2025**.

The project implements a real-time taxi monitoring system using
technologies such as **Apache Kafka, Apache Flink, Redis, Docker, React,
and Leaflet**. Taxi GPS data is streamed through Kafka, processed using
Flink, stored in Redis, and visualized through a web-based monitoring
dashboard.

The system processes continuously generated taxi location data,
calculates mobility-related metrics, detects abnormal behaviour and
geographical boundary violations, and makes the processed information
available through a real-time dashboard.

The project was developed collaboratively with other students. The
original university GitHub repository is no longer accessible to our
team, so this repository is maintained as a personal copy for
**portfolio, learning, and academic documentation purposes**.

> **Important:** This is a group project and should not be considered an
> individual project. The contribution section below describes the areas
> in which I personally participated.

------------------------------------------------------------------------

## My Contribution

As a member of the project team, I contributed to several parts of the
system, including **stream processing, data storage integration,
deployment, frontend development, testing, and documentation**.

My main contributions included:

-   Worked on the **Apache Flink stream-processing pipeline**, including
    processing taxi GPS data and calculating metrics such as speed,
    average speed, and travelled distance.
-   Worked on the integration between **Kafka, Flink, and Redis** and
    tested the end-to-end flow of streaming and processed taxi data.
-   Worked with **Redis sinks** for storing processed taxi information
    such as location, speed, average speed, and distance.
-   Contributed to **Docker and Docker Compose configuration**,
    including building, publishing, and testing Docker images.
-   Published and tested the **Kafka producer Docker image** through
    Docker Hub.
-   Contributed to the **React/Vite frontend dashboard**, including the
    live taxi map, taxi markers, geofence visualization, fleet
    statistics, incidents, alerts, and real-time updates.
-   Worked with **Leaflet and React Leaflet** for displaying taxi
    locations and operational geofence areas.
-   Worked on frontend deployment configuration using **Docker and
    Nginx**.
-   Debugged integration and deployment issues across **Kafka, Flink,
    Redis, Docker, and the frontend**.
-   Improved project documentation, including restructuring the
    **README** and documenting setup and execution steps.
-   Contributed to the preparation of the final project presentation and
    demonstration.

The project was developed collaboratively, and the contributions listed
above represent the areas in which I personally participated as part of
the team.

------------------------------------------------------------------------

## Project Architecture

The system follows a real-time data processing pipeline:

``` text
Taxi GPS Data
      ↓
    Kafka
      ↓
Apache Flink
      ↓
    Redis
      ↓
Node.js Backend
      ↓
Web Dashboard
```

### Main Components

-   **Kafka Producer**\
    Replays taxi GPS data and publishes the events to Kafka.

-   **Apache Kafka**\
    Provides the message-streaming layer for taxi GPS data.

-   **Apache Flink**\
    Processes the streaming data and performs real-time calculations.

-   **Redis**\
    Stores processed taxi information for fast access.

-   **Node.js Backend**\
    Provides REST API and WebSocket communication between the processed
    data and frontend.

-   **React Web Dashboard**\
    Provides an interactive interface for monitoring taxi movements,
    statistics, and alerts.

------------------------------------------------------------------------

## Project Overview

The system implements a complete streaming data pipeline that:

-   Ingests taxi GPS coordinates from historical trajectory data
-   Streams taxi location events through Apache Kafka
-   Processes location data in real time using Apache Flink
-   Calculates speed and travelled distance
-   Monitors geographical boundaries around Beijing's Forbidden City
-   Detects speeding and geofence violations
-   Stores processed information in Redis
-   Provides real-time data through a backend service
-   Visualizes taxi movements and fleet statistics through a web
    dashboard
-   Provides real-time alerts for operational events

The project uses the **T-Drive taxi trajectory dataset**, which contains
Beijing taxi GPS trajectory data.

------------------------------------------------------------------------

## Technologies Used

  -----------------------------------------------------------------------
  Component                Technology                 Purpose
  ------------------------ -------------------------- -------------------
  Message Streaming        Apache Kafka               Data ingestion and
                                                      event streaming

  Stream Processing        Apache Flink               Real-time data
                                                      processing and
                                                      analytics

  Data Storage             Redis                      Fast in-memory data
                                                      storage

  Backend                  Node.js + Express          REST API and
                                                      WebSocket
                                                      communication

  Frontend                 React.js                   Interactive web
                                                      dashboard

  Mapping                  Leaflet / React Leaflet    Interactive taxi
                                                      and geofence maps

  Containerization         Docker & Docker Compose    Service
                                                      orchestration

  Web Server               Nginx                      Frontend deployment

  Build Tool               Maven                      Java/Flink project
                                                      management

  Data Source              T-Drive Dataset            Beijing taxi
                                                      trajectory data
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## Final Project Deployment

The original team project was deployed as a web-based real-time
monitoring dashboard.

The deployment used the project's Docker-based service architecture,
including the Kafka producer, Flink processing job, backend service,
Redis, and frontend dashboard.

The original deployment URL was used during the university project and
grading period. It may no longer be available after the course ended.

------------------------------------------------------------------------

## Quick Start

### Prerequisites

Make sure the following are installed:

-   Docker
-   Docker Compose
-   Git
-   At least **8 GB RAM** available for containers
-   Available ports:
    -   `2181`
    -   `9092`
    -   `8081`
    -   `5000`
    -   `5173`
    -   `6379`

------------------------------------------------------------------------

### Option 1: Deploy Using DockerHub Images

Pre-built Docker images were created for several project components.

Clone this repository:

``` bash
git clone https://github.com/MehrabZishaan/Big_Data_TUHH_SoSe2025.git
cd Big_Data_TUHH_SoSe2025
```

Start the services:

``` bash
docker-compose -f docker-compose-dockerhub.yml up -d
```

Run the Flink job:

``` bash
docker-compose -f docker-compose-dockerhub.yml exec flink-jobmanager flink run -c com.example.flink.Main /opt/flink/usrlib/taxi-flink-job-1.0-SNAPSHOT.jar --kafka.bootstrap.servers kafka:9092 --kafka.topic taxi_data --redis.host redis
```

Open the dashboard:

``` text
http://localhost:5173
```

------------------------------------------------------------------------

### Option 2: Build from Source

Build and start all services:

``` bash
docker-compose up --build -d
```

Run the Flink job:

``` bash
docker-compose exec flink-jobmanager flink run -c com.example.flink.Main /opt/flink/usrlib/taxi-flink-job-1.0-SNAPSHOT.jar --kafka.bootstrap.servers kafka:9092 --kafka.topic taxi_data --redis.host redis
```

Monitor the services:

``` bash
docker-compose logs -f
```

Open the dashboard:

``` text
http://localhost:5173
```

------------------------------------------------------------------------

## Simulation Speed

The speed of taxi data replay can be adjusted using the `SPEED_FACTOR`
environment variable.

The default value is:

``` text
SPEED_FACTOR=0.1
```

For example:

``` bash
export SPEED_FACTOR=0.1
```

A larger value can be used to replay the data faster.

The `SPEED_FACTOR` configuration is applied to the Kafka producer.

------------------------------------------------------------------------

## DockerHub Images

The project includes pre-built Docker images for easier deployment.

The original team deployment used:

-   **Flink Job**\
    `hasebsiddiqui/flink-job:latest`

-   **Kafka Producer**\
    `hasebsiddiqui/kafka-producer:latest`

-   **Node.js Backend**\
    `hasebsiddiqui/node-backend:latest`

-   **Web Dashboard**\
    `hasebsiddiqui/frontend:latest`

These images are referenced by the Docker Compose configuration used for
deployment.

------------------------------------------------------------------------

## Performance Optimizations

### Kafka Producer Optimizations

#### Memory Management

-   **Chunk-based processing**\
    Configurable `CHUNK_SIZE` helps prevent memory exhaustion when
    processing large datasets.

-   **Lazy loading**\
    Data files are loaded in chunks rather than loading the complete
    dataset into memory.

-   **Buffer management**\
    Buffering reduces unnecessary I/O operations.

#### Real-Time Synchronization

-   **Min-heap event scheduling**\
    Events with identical timestamps across multiple files can be
    processed in the correct temporal order.

-   **Temporal ordering**\
    Maintains chronological ordering across taxi trajectories.

-   **Configurable replay speed**\
    `SPEED_FACTOR` allows accelerated testing and demonstrations.

#### Producer Performance

-   **Message batching**\
    Messages are grouped before being flushed to improve throughput.

-   **Buffer overflow handling**\
    Automatic flushing and retry mechanisms help reduce the risk of data
    loss.

-   **Asynchronous delivery**\
    Non-blocking message production improves producer performance.

### Flink Job Optimizations

-   Early filtering of streaming data to reduce unnecessary processing
-   Parallel processing for improved throughput
-   Configurable replay speed for testing and demonstrations

------------------------------------------------------------------------

## Dashboard Features

### Real-Time Map View

The dashboard provides:

-   Live taxi positions
-   Periodic location updates
-   Speed indicators
-   Interactive taxi markers
-   Geofence visualization
-   Interactive map zoom and pan controls

### Fleet Statistics

The dashboard displays information such as:

-   Active taxi count
-   Total travelled distance
-   Speed violations
-   Boundary violations
-   Other real-time operational statistics

### Alert System

The system can generate alerts for:

-   Speeding violations
-   Geofence warnings
-   Drop-zone violations
-   Other real-time operational events

The original project configuration used:

-   **50 km/h** as the speeding threshold
-   **10 km** warning zone around the Forbidden City
-   **15 km** drop-zone boundary

------------------------------------------------------------------------

## Configuration

### Environment Variables

  Variable            Default        Description
  ------------------- -------------- ------------------------------
  `KAFKA_BOOTSTRAP`   `kafka:9092`   Kafka broker address
  `KAFKA_TOPIC`       `taxi_data`    Topic name for GPS data
  `SPEED_FACTOR`      `0.1`          Data replay speed multiplier
  `REDIS_HOST`        `redis`        Redis server hostname
  `REDIS_PORT`        `6379`         Redis server port

### Geofence Settings

The original project used the following geofence configuration:

-   **Warning Zone:** 10 km radius from the Forbidden City
-   **Drop Zone:** 15 km radius
-   **Speed Limit:** 50 km/h

------------------------------------------------------------------------

## Troubleshooting

### Services Will Not Start

Check whether required ports are already being used.

For example:

``` bash
netstat -tulpn | grep :9092
```

Stop conflicting services if necessary.

You can also restart the Docker environment:

``` bash
docker-compose down -v
docker system prune -f
```

> `docker system prune -f` removes unused Docker resources. Use it
> carefully if you have other Docker projects on your machine.

------------------------------------------------------------------------

### No Data Flowing

Check whether the Kafka topic exists:

``` bash
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

Check Kafka producer logs:

``` bash
docker-compose logs kafka-producer
```

Check whether the Flink job is running:

``` bash
curl http://localhost:8081/jobs
```

------------------------------------------------------------------------

### High Memory Usage

Reduce the replay speed:

``` bash
export SPEED_FACTOR=0.05
```

You can also reduce the configured chunk size if required.

------------------------------------------------------------------------

## Original Repository

The project was originally developed in the university Git environment
as a collaborative group project.

After the university repository was no longer accessible to the team, a
copy of the project was maintained by a team member.

A copy of the group project is currently available at:

https://github.com/hasebsiddiqui/big-data-tuhh

This repository was created from the latest available version of that
project for my personal portfolio and academic documentation.

------------------------------------------------------------------------

## Team Project

This was a **group project** completed as part of the **Big Data course
at Hamburg University of Technology (TUHH)** during the **Summer
Semester 2025**.

The project was developed collaboratively by multiple students.

This repository should therefore **not be interpreted as an individual
project**. The `My Contribution` section identifies the areas in which I
personally participated.

------------------------------------------------------------------------

## Academic Context

**Course:** Big Data\
**Institution:** Hamburg University of Technology (TUHH)\
**Semester:** Summer Semester 2025\
**Project Type:** Group Project

------------------------------------------------------------------------

<p align="center">
<i>Developed collaboratively as part of the Big Data course at Hamburg University of Technology (TUHH) - Summer Semester 2025</i>
</p>