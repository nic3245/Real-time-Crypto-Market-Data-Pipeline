from confluent_kafka import Consumer, KafkaException
import psycopg2
import json
from collections import deque

# Kafka setup
conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "bitcoin_consumer_group_d",
    "auto.offset.reset": "earliest",  # Start consuming from the beginning if no offset is committed
}

consumer = Consumer(conf)

topic = "coingecko"
consumer.subscribe([topic])

# Postgres setup
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
    print("Wrote to Postgres")


price_window = deque(maxlen=5)

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        value = json.loads(msg.value())
        print("received message:", value)
        price_window.append(value["price"])
        if len(price_window) < 5:
            average = None
        else:
            average = sum(price_window) / 5
        write_to_postgres(cursor, value, average)
except KeyboardInterrupt:
    pass
finally:
    cursor.close()
    conn.close()
    consumer.close()
