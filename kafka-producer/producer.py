import time
import pandas as pd
import os
from confluent_kafka import Producer

# Wait for Kafka to become available (useful in Docker setups)
time.sleep(30)

# Kafka producer configuration
conf = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': 'taxi-data-producer'
}
producer = Producer(conf)

# Read environment variables
speed_factor = float(os.getenv("SPEED_FACTOR", "1000.0"))
debug = os.getenv("DEBUG", "false").lower() == "true"
dry_run = os.getenv("DRY_RUN", "false").lower() == "true"

def log(message: str):
    """Log messages if debug mode is enabled."""
    if debug:
        print(f"[DEBUG] {message}")

def delivery_report(err, msg):
    """Kafka delivery report callback."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        log(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Define the data directory path
script_dir = os.path.dirname(__file__)
data_dir = os.path.join(script_dir, 'taxi_data')

# Validate that data directory exists
if not os.path.isdir(data_dir):
    raise RuntimeError(f"Directory not found: {data_dir!r}")

topic = 'taxi_data'

# Process each .txt file in the taxi_data directory
for file_name in os.listdir(data_dir):
    if not file_name.endswith(".txt"):
        continue

    file_path = os.path.join(data_dir, file_name)
    start_time = time.time()

    try:
        df = pd.read_csv(
            file_path,
            header=None,
            names=['taxiId', 'timestamp', 'longitude', 'latitude'],
            parse_dates=['timestamp'],
            date_parser=lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M:%S"),
            on_bad_lines='skip'
        )
    except Exception as e:
        print(f"Failed to read file '{file_name}': {e}")
        continue

    df['ts_ms'] = df['timestamp'].astype('int64') // 10**6
    df.sort_values(by='ts_ms', inplace=True)

    prev_ts = None

    for _, row in df.iterrows():
        current_ts = int(row['ts_ms'])

        # Wait according to timestamp difference and speed factor
        if prev_ts is not None:
            wait_seconds = (current_ts - prev_ts) / 1000.0 / speed_factor
            if wait_seconds > 0:
                time.sleep(wait_seconds)
        prev_ts = current_ts

        message_value = f"{row['taxiId']},{current_ts},{row['longitude']},{row['latitude']}"
        log(f"Producing message: {message_value}")

        if not dry_run:
            producer.produce(
                topic,
                key=str(row['taxiId']),
                value=message_value,
                callback=delivery_report
            )

    # Send 'END' signal for each unique taxiId
    for taxi_id in df['taxiId'].unique():
        if not dry_run:
            producer.produce(
                topic,
                key=str(taxi_id),
                value='END',
                callback=delivery_report
            )

    if not dry_run:
        producer.flush()

    elapsed_time = round(time.time() - start_time, 2)
    print(f"Finished processing '{file_name}' in {elapsed_time} seconds.")

print("All data files have been processed.")
