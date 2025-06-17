import time
import pandas as pd
import os
from confluent_kafka import Producer

# Wait for Kafka to be ready (especially when using Docker)
time.sleep(30)

# Kafka producer configuration
conf = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': 'taxi-data-producer'
}
producer = Producer(conf)

# Kafka delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Build path to taxi_data directory
script_dir = os.path.dirname(__file__)           
data_dir   = os.path.join(script_dir, 'taxi_data')

# Check that the data directory exists
if not os.path.isdir(data_dir):
    raise RuntimeError(f"Directory not found: {data_dir!r}")

topic = 'taxi_data'
speed_factor = float(os.getenv("SPEED_FACTOR", "1.0"))

# Loop through each .txt file in the data directory
for filename in os.listdir(data_dir):
    if not filename.endswith(".txt"):
        continue

    data_file = os.path.join(data_dir, filename)

    # Read and sort the data file by timestamp
    df = pd.read_csv(
        data_file,
        header=None,
        names=['taxiId', 'timestamp', 'longitude', 'latitude'],
        parse_dates=['timestamp'],
        date_parser=lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M:%S")
    )
    df['ts_ms'] = df['timestamp'].astype('int64') // 10**6  # Convert to milliseconds
    df.sort_values(by='ts_ms', inplace=True)

    prev_ts = None

    # Send each row as a Kafka message
    for _, row in df.iterrows():
        current_ts = int(row['ts_ms'])

        # Sleep based on timestamp difference and speed factor
        if prev_ts is not None:
            wait_seconds = (current_ts - prev_ts) / 1000.0 / speed_factor
            if wait_seconds > 0:
                time.sleep(wait_seconds)
        prev_ts = current_ts

        message_value = f"{row['taxiId']},{current_ts},{row['longitude']},{row['latitude']}"
        producer.produce(
            topic,
            key=str(row['taxiId']),
            value=message_value,
            callback=delivery_report
        )

    # Send 'END' marker for each unique taxi ID
    for taxi_id in df['taxiId'].unique():
        producer.produce(
            topic,
            key=str(taxi_id),
            value='END',
            callback=delivery_report
        )

    # Ensure all messages are delivered before moving to the next file
    producer.flush()

print("✅ Data submission completed.")
