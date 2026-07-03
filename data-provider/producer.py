import io
import os
import time

import orjson
import zstandard as zstd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

FIELDS = {'id', 'author', 'created_utc', 'body', 'score', 'subreddit', 'controversiality'}
TS_START, TS_END = 1554076800, 1555472130
SPEED_FACTOR = int(os.environ.get('SPEED_FACTOR', 100))
ZST_FILE = 'RC_2019-04.zst'
KAFKA_TOPIC = 'reddit-comments'
KAFKA_BROKER = 'kafka:9092'

def create_producer(retries=10, delay=5):
    for attempt in range(retries):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: orjson.dumps(v)
            )
        except NoBrokersAvailable:
            print(f"Kafka not ready, retrying in {delay}s... ({attempt+1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after retries")

producer = create_producer()
print(f"Connected to Kafka. Streaming at SPEED_FACTOR={SPEED_FACTOR}")

def stream_comments(filepath):
    """Stream comments from .zst file one by one."""
    with open(filepath, 'rb') as f:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(f) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')
            for line in text_stream:
                try:
                    obj = orjson.loads(line)
                    ts = obj.get('created_utc', 0)
                    if TS_START <= ts <= TS_END:
                        yield ts, {k: obj[k] for k in FIELDS if k in obj}
                except Exception:
                    continue

# Stream and send — keep only current bucket in memory
current_ts = None
current_batch = []
prev_ts = None
sent_count = 0

print("Reading and streaming .zst file...")
for ts, comment in stream_comments(ZST_FILE):
    if ts != current_ts:
        # Send previous batch
        if current_batch:
            if prev_ts is not None:
                sleep_time = (current_ts - prev_ts) / SPEED_FACTOR
                if sleep_time > 0:
                    time.sleep(sleep_time)
            for c in current_batch:
                producer.send(KAFKA_TOPIC, key=str(current_ts).encode(), value=c)
            producer.flush()
            sent_count += len(current_batch)
            if sent_count % 10000 == 0:
                print(f"Sent {sent_count} comments... (ts={current_ts})")
            prev_ts = current_ts

        current_ts = ts
        current_batch = [comment]
    else:
        current_batch.append(comment)

# Send final batch
if current_batch:
    for c in current_batch:
        producer.send(KAFKA_TOPIC, key=str(current_ts).encode(), value=c)
    producer.flush()
    sent_count += len(current_batch)

print(f"Done. Total sent: {sent_count} comments.")