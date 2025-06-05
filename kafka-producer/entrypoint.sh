#!/bin/bash
set -e

# Start Zookeeper
echo "Starting Zookeeper..."
bin/zookeeper-server-start.sh config/zookeeper.properties &
ZOOKEEPER_PID=$!
sleep 10 # Wait for Zookeeper to start

# Start Kafka
echo "Starting Kafka..."
bin/kafka-server-start.sh config/server.properties &
KAFKA_PID=$!
sleep 10 # Wait for Kafka to start

# Run the Python script
echo "Running data provider..."
python3 /producer.py

# Wait for Kafka and Zookeeper processes to finish
wait $ZOOKEEPER_PID
wait $KAFKA_PID
