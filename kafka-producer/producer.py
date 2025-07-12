import time
import pandas as pd
import os
import heapq
from confluent_kafka import Producer

# Read config from env
TOPIC = os.getenv('KAFKA_TOPIC', 'taxi_data')
DATA_DIR = os.getenv('DATA_DIR', 'taxi_data')
SPEED_FACTOR = float(os.getenv('SPEED_FACTOR', '1.0'))
DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'
DEBUG = os.getenv('DEBUG', 'false').lower() == 'true'
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', '1000'))  # Process files in chunks to optimize memory

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

def load_file_chunk(file_path, chunk_start=0, chunk_size=CHUNK_SIZE):
    """Load a chunk of data from a file for memory optimization."""
    try:
        df = pd.read_csv(
            file_path,
            header=None,
            names=['taxiId', 'timestamp', 'longitude', 'latitude'],
            parse_dates=['timestamp'],
            date_parser=lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M:%S"),
            on_bad_lines='skip',
            skiprows=chunk_start,
            nrows=chunk_size
        )
        if df.empty:
            return None, True  # End of file reached
        
        df['ts_ms'] = df['timestamp'].astype('int64') // 10**6
        return df.to_dict('records'), False
    except Exception as e:
        print(f"Failed to read chunk from file '{file_path}': {e}")
        return None, True

def get_file_iterators():
    """Initialize file iterators for all taxi data files."""
    file_iterators = []
    
    if not os.path.isdir(DATA_DIR):
        raise RuntimeError(f"Data dir not found: {DATA_DIR}")
    
    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".txt"):
            file_path = os.path.join(DATA_DIR, filename)
            # Load first chunk
            chunk_data, is_eof = load_file_chunk(file_path, 0)
            if chunk_data:
                file_iterators.append({
                    'file_path': file_path,
                    'filename': filename,
                    'chunk_data': chunk_data,
                    'chunk_index': 0,
                    'chunk_start': 0,
                    'is_eof': is_eof,
                    'taxi_ids': set()  # Track taxi IDs in this file for END signals
                })
    
    return file_iterators

def get_next_events(file_iterators):
    """Get all events with the minimum timestamp across all files."""
    min_heap = []
    
    # Build heap with next event from each file iterator
    for i, file_iter in enumerate(file_iterators):
        if file_iter['chunk_data'] and file_iter['chunk_index'] < len(file_iter['chunk_data']):
            row = file_iter['chunk_data'][file_iter['chunk_index']]
            heapq.heappush(min_heap, (row['ts_ms'], i, row))
    
    if not min_heap:
        return []
    
    # Get minimum timestamp
    min_timestamp = min_heap[0][0]
    events = []
    
    # Collect all events with the minimum timestamp
    while min_heap and min_heap[0][0] == min_timestamp:
        timestamp, file_idx, row = heapq.heappop(min_heap)
        events.append((file_idx, row))
        
        # Track taxi ID for END signal
        file_iterators[file_idx]['taxi_ids'].add(row['taxiId'])
        
        # Advance this file's iterator
        file_iterators[file_idx]['chunk_index'] += 1
        
        # Check if we need to load next chunk
        if (file_iterators[file_idx]['chunk_index'] >= len(file_iterators[file_idx]['chunk_data']) 
            and not file_iterators[file_idx]['is_eof']):
            
            # Load next chunk
            file_iterators[file_idx]['chunk_start'] += CHUNK_SIZE
            chunk_data, is_eof = load_file_chunk(
                file_iterators[file_idx]['file_path'], 
                file_iterators[file_idx]['chunk_start']
            )
            
            if chunk_data:
                file_iterators[file_idx]['chunk_data'] = chunk_data
                file_iterators[file_idx]['chunk_index'] = 0
                file_iterators[file_idx]['is_eof'] = is_eof
            else:
                file_iterators[file_idx]['is_eof'] = True
        
        # Add next event from this file to heap if available
        if (file_iterators[file_idx]['chunk_data'] and 
            file_iterators[file_idx]['chunk_index'] < len(file_iterators[file_idx]['chunk_data'])):
            
            next_row = file_iterators[file_idx]['chunk_data'][file_iterators[file_idx]['chunk_index']]
            heapq.heappush(min_heap, (next_row['ts_ms'], file_idx, next_row))
    
    return events

def produce_realtime():
    """Produce taxi data in real-time based on timestamps across all files."""
    start_time = time.time()
    file_iterators = get_file_iterators()
    
    if not file_iterators:
        print("No taxi data files found.")
        return
    
    prev_ts = None
    msg_count = 0
    
    print(f"Starting real-time production from {len(file_iterators)} files...")
    
    while True:
        events = get_next_events(file_iterators)
        
        if not events:
            break  # No more events
        
        # Get timestamp from first event (all events have same timestamp)
        current_ts = events[0][1]['ts_ms']
        
        # Wait according to timestamp difference and speed factor
        if prev_ts is not None:
            wait_seconds = (current_ts - prev_ts) / 1000.0 / SPEED_FACTOR
            if wait_seconds > 0:
                time.sleep(wait_seconds)
        prev_ts = current_ts
        
        # Process all events with the same timestamp
        for file_idx, row in events:
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
    
    # Send 'END' signals for each unique taxiId from each file
    for file_iter in file_iterators:
        for taxi_id in file_iter['taxi_ids']:
            if not DRY_RUN:
                producer.produce(TOPIC, key=str(taxi_id), value='END', callback=delivery_report)
        print(f"Finished processing '{file_iter['filename']}'")
    
    if not DRY_RUN:
        producer.flush()
    
    elapsed_time = round(time.time() - start_time, 2)
    print(f"Finished real-time production in {elapsed_time} seconds.")

def main():
    produce_realtime()
    print("All data files processed in real-time.")

if __name__ == "__main__":
    main()