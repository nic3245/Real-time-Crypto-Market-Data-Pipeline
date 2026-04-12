from confluent_kafka import DeserializingConsumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import psycopg2
from datetime import datetime, timedelta

# --- SCHEMA REGISTRY SETUP ---
schema_registry_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
avro_deserializer = AvroDeserializer(schema_registry_client)

# --- KAFKA SETUP ---
conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "bitcoin_consumer_group_d",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "key.deserializer": lambda k, ctx: k.decode('utf-8') if k else None,
    "value.deserializer": avro_deserializer,
}

consumer = DeserializingConsumer(conf)
topic = "coingecko"
consumer.subscribe([topic])

# --- POSTGRES SETUP ---
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="crypto_db",
    user="user",
    password="password",
)
conn.autocommit = True
cursor = conn.cursor()

def write_to_postgres(cursor, value, average):
    cursor.execute(
        "INSERT INTO raw_price_events (timestamp, symbol, price, source, average) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
        (value["timestamp"], value["symbol"], value["price"], value["source"], average),
    )
    print(f"Stored event: {value['timestamp']}")

# --- WINDOWING & LATE DATA LOGIC ---
windows = {} # Format: { window_start_time: [prices] }
WATERMARK_DELAY = timedelta(minutes=1) # Allow events up to 1 min late
current_watermark = None  # Set on first event to avoid spurious drops at startup

try:
    print("Consumer started. Waiting for messages...")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
            
        value = msg.value()
        event_time = datetime.strptime(value["timestamp"], "%Y-%m-%d %H:%M:%S")

        # 1. Late-Data Handling (Drop events older than the watermark)
        if current_watermark is not None and event_time < current_watermark:
            print(f"Dropped late event: {value['timestamp']}")
            continue

        # 2. Advance Watermark (The highest timestamp seen minus the allowed delay)
        if current_watermark is None or event_time - WATERMARK_DELAY > current_watermark:
            current_watermark = event_time - WATERMARK_DELAY

        # 3. Windowed Aggregations (1-minute tumbling window)
        window_start = event_time.replace(second=0, microsecond=0)
        if window_start not in windows:
            windows[window_start] = []
            
        windows[window_start].append(value["price"])
        
        # Calculate current average for the active window
        average = sum(windows[window_start]) / len(windows[window_start])
        
        write_to_postgres(cursor, value, average)
        consumer.commit(message=msg)

        # 4. State Cleanup (Remove old windows that are behind the watermark)
        old_windows = [w for w in windows.keys() if current_watermark is not None and w < current_watermark]
        for w in old_windows:
            del windows[w]

except KeyboardInterrupt:
    pass
finally:
    cursor.close()
    conn.close()
    consumer.close()
