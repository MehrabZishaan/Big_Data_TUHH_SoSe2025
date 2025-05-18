import time
import os
import pandas as pd
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': '172.28.112.1:9092',
    'client.id': 'taxi-data-producer'
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

data_dir = '/data'

topic = 'taxi_data'

for filename in os.listdir(data_dir):
    if filename.endswith(".txt"):
        file_path = os.path.join(data_dir, filename)
        df = pd.read_csv(file_path, header=None, names=['taxiId', 'timestamp', 'longitude', 'latitude'])
        df.sort_values('timestamp', inplace=True)

        for _, row in df.iterrows():
            message = f"{row['taxiId']},{row['timestamp']},{row['longitude']},{row['latitude']}"
            producer.produce(topic, key=str(row['taxiId']), value=message, callback=delivery_report)
            producer.flush()
            time.sleep(0.1)

        for taxi_id in df['taxiId'].unique():
            producer.produce(topic, key=str(taxi_id), value='END')
            producer.flush()

print("Data submission completed.")
