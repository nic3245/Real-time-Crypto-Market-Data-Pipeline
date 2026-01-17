from confluent_kafka import Consumer, KafkaException
from minio import Minio
from minio import S3Error
import json
import pandas as pd
import io
from datetime import datetime
from datetime import timedelta

# Kafka setup
conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "bitcoin_consumer_group_s3909000009",
    "auto.offset.reset": "earliest",  # Start consuming from the beginning if no offset is committed
}

consumer = Consumer(conf)

topic = "coingecko"
consumer.subscribe([topic])

# Postgres setup
client = Minio(
    endpoint="localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False,
)


def write_to_s3(client, values, offset):
    try:
        bucket_name = f"coingecko-bucket"
        timestamp = datetime.fromisoformat(values[0]["timestamp"])
        object_name = f"/year={timestamp.year}/month={timestamp.month}/day={timestamp.day}/{offset}.parquet"
        print("Writing to S3...")
        # Make the bucket if it does not exist
        try:
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
                print(f"Bucket '{bucket_name}' created successfully.")
            else:
                print(f"Bucket '{bucket_name}' already exists.")
        except S3Error as e:
            print(f"An S3 error occurred: {e}")

        try:
            # Convert to parquet with pandas
            parquet_buffer = io.BytesIO()
            pd.DataFrame(values).to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            length = parquet_buffer.getbuffer().nbytes

            # Upload the file
            client.put_object(
                bucket_name,
                object_name,
                parquet_buffer,
                length=length
            )
            print(
                f"Successfully uploaded '{object_name}' to bucket '{bucket_name}'."
            )

        except S3Error as e:
            print(f"An S3 error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
        raise e



buffer = []
offset = None
timestamp = datetime.now()
try:
    while True:
        if len(buffer) >= 1000 or (datetime.now() - timestamp > timedelta(minutes=5) and len(buffer) > 0):
            write_to_s3(client, buffer, offset)
            buffer.clear()
            consumer.commit()
            offset = None
            timestamp = datetime.now()
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        if offset is None:
            offset = msg.offset()
        value = json.loads(msg.value())
        print("received message:", value)
        buffer.append(value)

except KeyboardInterrupt:
    pass
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
