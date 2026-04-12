import requests
import os
from dotenv import load_dotenv
from confluent_kafka import SerializingProducer # Changed
from confluent_kafka.schema_registry import SchemaRegistryClient # New
from confluent_kafka.schema_registry.avro import AvroSerializer # New
import socket
from datetime import datetime
import json
import random
import time

load_dotenv()

# --- CONFIGURATION ---
SIMULATE_MODE = True
API_KEY = os.getenv("API_KEY")
API_URL = "https://api.coingecko.com/api/v3/simple/price?vs_currencies=usd&ids=bitcoin"
TOPIC = "coingecko"

# --- SCHEMA REGISTRY SETUP ---
schema_str = """
{
  "namespace": "crypto",
  "name": "PriceEvent",
  "type": "record",
  "fields": [
    {"name": "timestamp", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "source", "type": "string"},
    {"name": "symbol", "type": "string"}
  ]
}
"""
schema_registry_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# --- PRODUCER SETUP ---
conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": socket.gethostname(),
    "linger.ms": 10,
    "batch.size": 16384,
    "enable.idempotence": True, # Enforces idempotent writes at the producer level
    "key.serializer": lambda k, ctx: k.encode('utf-8') if k else None,
    "value.serializer": avro_serializer
}

producer = SerializingProducer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        if not SIMULATE_MODE:
            print(f"Message delivered to topic '{msg.topic()}' [{msg.partition()}]")

def get_bitcoin_price():
    try:
        headers = {"x-cg-demo-api-key": API_KEY} if API_KEY else {}
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        price = response.json()["bitcoin"]["usd"]
        return price
    except Exception as e:
        print(f"API Error: {e}")
        return None

def generate_simulated_price(last_price):
    change_percent = random.gauss(0, 0.0005)
    new_price = last_price * (1 + change_percent)
    return round(new_price, 2)

def publish_to_kafka(value):
    producer.produce(
        TOPIC,
        key=value["symbol"],
        value=value, # Removed json.dumps, serializer handles it
        on_delivery=delivery_report # Changed from callback to on_delivery
    )
    
    if not SIMULATE_MODE:
        producer.flush()

def main():
    print(f"Starting Producer in {'SIMULATION' if SIMULATE_MODE else 'LIVE'} mode...")
    current_price = 95000.00
    messages_sent = 0
    start_time = time.time()

    try:
        while True:
            if SIMULATE_MODE:
                price = generate_simulated_price(current_price)
                current_price = price
            else:
                price = get_bitcoin_price()
                if price is None:
                    time.sleep(5)
                    continue

            price_event = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "price": price,
                "source": "simulated" if SIMULATE_MODE else "coingecko",
                "symbol": "BTC",
            }

            publish_to_kafka(price_event)
            
            if SIMULATE_MODE:
                producer.poll(0)
                messages_sent += 1
                
                if messages_sent % 1000 == 0:
                    elapsed = time.time() - start_time
                    throughput = 1000 / elapsed
                    print(f"Throughput: {throughput:.0f} events/sec | Price: {price}")
                    start_time = time.time()
                    messages_sent = 0
            else:
                print(f"Sent live price: {price}")
                time.sleep(10) 

    except KeyboardInterrupt:
        print("\nGoodbye!")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
