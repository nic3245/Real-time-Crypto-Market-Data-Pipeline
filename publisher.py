import requests
import os
from dotenv import load_dotenv
from confluent_kafka import Producer
import socket
from datetime import datetime
import json

load_dotenv()

API_KEY = os.getenv("API_KEY")
API_URL = "https://api.coingecko.com/api/v3/simple/price?vs_currencies=usd&ids=bitcoin"


conf = {
    "bootstrap.servers": "localhost:9092",  # Replace with your broker's address and port
    "client.id": socket.gethostname(),
}

producer = Producer(conf)


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic '{msg.topic()}' [{msg.partition()}]")


def get_bitcoin_price():
    response = requests.get(API_URL, headers={"x-cg-demo-api-key": API_KEY})
    price = response.json()["bitcoin"]["usd"]
    return_json = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "price": price,
        "source": "coingecko",
        "symbol": "BTC",
    }
    return return_json


def publish_to_kafka(value):
    producer.produce(
        "coingecko",
        key=value["symbol"],
        value=json.dumps(value).encode("utf-8"),
        callback=delivery_report,
    )
    producer.flush()


def main():
    price_json = get_bitcoin_price()
    publish_to_kafka(price_json)
    print("Goodbye!")


if __name__ == "__main__":
    main()
