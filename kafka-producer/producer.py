import time
import pandas as pd
import os
from confluent_kafka import Producer

# Read config from env
TOPIC = os.getenv('KAFKA_TOPIC', 'taxi_data')
DATA_DIR = os.getenv('DATA_DIR', 'taxi_data')
SPEED_FACTOR = float(os.getenv('SPEED_FACTOR', '1.0'))
DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'
DEBUG = os.getenv('DEBUG', 'false').lower() == 'true'


conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092'),
    'client.id': 'taxi-data-producer'
}
producer = Producer(conf)

def log(message: str):
    """Log messages if debug mode is enabled."""
    if DEBUG:
        print(f"[DEBUG] {message}")

def delivery_report(err, msg):
    """Kafka delivery report callback."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        log(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_file(file_path):
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
        print(f"Failed to read file '{file_path}': {e}")
        return

    df['ts_ms'] = df['timestamp'].astype('int64') // 10**6
    df.sort_values(by='ts_ms', inplace=True)

    prev_ts = None
    msg_count = 0

    for _, row in df.iterrows():
        current_ts = int(row['ts_ms'])

        # Wait according to timestamp difference and speed factor
        if prev_ts is not None:
            wait_seconds = (current_ts - prev_ts) / 1000.0 / SPEED_FACTOR
            if wait_seconds > 0:
                time.sleep(wait_seconds)
        prev_ts = current_ts

        message_value = f"{row['taxiId']},{current_ts},{row['longitude']},{row['latitude']}"
        log(f"Producing message: {message_value}")
        if not DRY_RUN:
            try:
                producer.produce(TOPIC, key=str(row['taxiId']), value=message_value, callback=delivery_report)
                producer.poll(0)
                msg_count += 1
                if msg_count % 100 == 0:
                    producer.flush()
            except BufferError:
                producer.flush()
                producer.produce(TOPIC, key=str(row['taxiId']), value=message_value, callback=delivery_report)

    # Send 'END' signal for each unique taxiId
    for taxi_id in df['taxiId'].unique():
        if not DRY_RUN:
            producer.produce(TOPIC, key=str(taxi_id), value='END', callback=delivery_report)

    if not DRY_RUN:
        producer.flush()
    
    elapsed_time = round(time.time() - start_time, 2)
    print(f"Finished processing '{file_path}' in {elapsed_time} seconds.")

def main():
    if not os.path.isdir(DATA_DIR):
        raise RuntimeError(f"Data dir not found: {DATA_DIR}")

    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".txt"):
            produce_file(os.path.join(DATA_DIR, filename))
            print(f"Finished {filename}")

    print("All data files processed.")

if __name__ == "__main__":
    main()