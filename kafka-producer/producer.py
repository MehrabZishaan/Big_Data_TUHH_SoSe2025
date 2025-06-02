import time
import pandas as pd
import os
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': 'taxi-data-producer'
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

script_dir = os.path.dirname(__file__)           
data_dir   = os.path.join(script_dir, 'taxi_data')



if not os.path.isdir(data_dir):
    raise RuntimeError(f"Directory not found: {data_dir!r}")

topic = 'taxi_data'
speed_factor = float(os.getenv("SPEED_FACTOR", "1.0"))

for filename in os.listdir(data_dir):
    if not filename.endswith(".txt"):
        continue

    data_file = os.path.join(data_dir, filename)
    df = pd.read_csv(
        data_file,
        header=None,
        names=['taxiId', 'timestamp', 'longitude', 'latitude'],
        parse_dates=['timestamp'],
        date_parser=lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M:%S")
    )

    df['ts_ms'] = df['timestamp'].astype('int64') // 10**6

    df.sort_values(by='ts_ms', inplace=True)

    prev_ts = None
    for _, row in df.iterrows():
        current_ts = int(row['ts_ms'])
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

    producer.flush()

    unique_taxis = df['taxiId'].unique()
    for taxi_id in unique_taxis:
        producer.produce(
            topic,
            key=str(taxi_id),
            value='END',
            callback=delivery_report
        )
    producer.flush()

print("Data submission completed.")
