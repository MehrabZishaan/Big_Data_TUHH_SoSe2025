#!/bin/bash

# Real-Time Traffic Monitoring Deployment Script

echo "Starting Real-Time Traffic Monitoring System..."

# Pull the latest images from DockerHub
echo "Pulling latest images from DockerHub..."
docker pull hasebsiddiqui/flink-job:latest
docker pull hasebsiddiqui/kafka-producer:latest
docker pull hasebsiddiqui/node-backend:latest
docker pull hasebsiddiqui/frontend:latest

# Start the services
echo "Starting services with Docker Compose..."
docker-compose -f docker-compose-dockerhub.yml up -d

# Wait for services to start
echo "Waiting for services to start..."
sleep 30

# Check service health
echo "Checking service health..."
docker-compose -f docker-compose-dockerhub.yml ps

echo "Deployment complete!"
echo "Access the dashboard at: http://localhost:5173"
echo "Flink Web UI at: http://localhost:8081"
echo "Redis at: localhost:6379"
echo "Node.js Backend at: http://localhost:5000"

# Show logs
echo "Showing recent logs..."
docker-compose -f docker-compose-dockerhub.yml logs --tail=50