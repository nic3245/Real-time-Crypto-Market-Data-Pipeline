# Real-Time Crypto Market Data Pipeline

A production-style streaming data pipeline that ingests live Bitcoin prices, processes them through Apache Kafka, and persists analytics-ready aggregations to Postgres and object storage (MinIO/S3).

---

## Architecture

```
CoinGecko API / Simulator
        │
        ▼
  [ Publisher ]
  Avro + Schema Registry
  Idempotent Producer
        │
        ▼
  [ Kafka Topic: coingecko ]
  3 Partitions | Replication Factor: 1
        │
   ┌────┴────┐
   ▼         ▼
[ Postgres  [ MinIO / S3
 Consumer ]   Consumer ]
Windowed     Parquet files
Aggregations  (partitioned
Late-data      by date)
Handling
```

---

## Features

- **High-throughput ingestion** — simulated producer sustains ~1,000 events/sec with batching (`linger.ms`, `batch.size`) and throughput logging
- **Schema enforcement** — all messages serialized with Avro via Confluent Schema Registry; consumers deserialize with the same registry client
- **Idempotent writes** — `enable.idempotence: True` on the producer prevents duplicate messages under retries
- **Windowed aggregations** — 1-minute tumbling windows computed in-stream; running average is persisted alongside each raw event
- **Late-data handling** — watermark-based approach drops events that arrive more than 1 minute behind the current stream frontier; old window state is cleaned up automatically
- **Dual storage sinks** — Postgres for queryable analytics; MinIO (S3-compatible) for Parquet data lake with date-partitioned paths

---

## Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka (Confluent 7.4) |
| Schema registry | Confluent Schema Registry |
| Stream processing | Python (confluent-kafka) |
| Analytical store | PostgreSQL 15 |
| Object storage | MinIO (S3-compatible) |
| Serialization | Apache Avro (fastavro) |
| Containerization | Docker Compose |

---

## Getting Started

### Prerequisites

- Docker + Docker Compose
- Conda (for the Python environment)

### 1. Start the infrastructure

```bash
docker-compose up -d
```

This starts Zookeeper, Kafka, Schema Registry, Postgres, MinIO, and Redpanda Console. The `kafka-setup` service automatically creates the `coingecko` topic with **3 partitions**.

Verify everything is healthy by opening the Kafka UI at [http://localhost:8080](http://localhost:8080).

### 2. Set up the Python environment

```bash
conda env create -f environment.yml
conda activate crypto-pipeline
```

### 3. Create the Postgres table

Connect to Postgres and run:

```bash
psql -h localhost -U user -d crypto_db -f raw_price.sql
```

Or with Docker:

```bash
docker exec -i postgres psql -U user -d crypto_db < raw_price.sql
```

### 4. Configure environment variables

Create a `.env` and fill in your CoinGecko API key (optional — the pipeline runs in simulation mode without one):

```
API_KEY=your_coingecko_key_here
```

### 5. Run the pipeline

Start the producer in one terminal:

```bash
python publisher.py
```

Start the Postgres consumer in another:

```bash
python postgres_consumer.py
```

Start the S3/MinIO consumer in a third terminal:

```bash
python s3_consumer.py
```

Make sure you activate the conda environment in each terminal.

---

## Running Concurrent Consumers (Partition-Level Parallelism)

The `coingecko` topic has 3 partitions. To exploit this, run multiple instances of the Postgres consumer in the **same consumer group**. Kafka will automatically assign one partition per instance:

```bash
# Terminal 1
python postgres_consumer.py

# Terminal 2
python postgres_consumer.py

# Terminal 3
python postgres_consumer.py
```

All three share `group.id = "bitcoin_consumer_group_d"`, so Kafka distributes the 3 partitions across them — each instance processes a disjoint subset of the event stream in parallel.

---

## Producer Modes

Set `SIMULATE_MODE` in `publisher.py`:

| Mode | Behaviour |
|---|---|
| `True` (default) | Generates synthetic BTC price walk using Gaussian noise; no API calls; logs throughput every 1,000 events |
| `False` | Polls the CoinGecko REST API every 10 seconds for live prices; requires API key for higher rate limits |

---

## Schema

All Kafka messages conform to this Avro schema (registered automatically on first produce):

```json
{
  "namespace": "crypto",
  "name": "PriceEvent",
  "type": "record",
  "fields": [
    { "name": "timestamp", "type": "string" },
    { "name": "price",     "type": "double" },
    { "name": "source",    "type": "string" },
    { "name": "symbol",    "type": "string" }
  ]
}
```

---

## Storage

### Postgres — `raw_price_events`

| Column | Type | Notes |
|---|---|---|
| `timestamp` | TIMESTAMPTZ | Event time |
| `symbol` | VARCHAR(10) | e.g. `BTC` |
| `price` | NUMERIC(18,8) | Raw tick price |
| `source` | VARCHAR(50) | `simulated` or `coingecko` |
| `average` | NUMERIC(18,8) | 1-min tumbling window average |

Primary key on `(symbol, timestamp)` enforces idempotency at the database layer.

### MinIO / S3 — Parquet

Files are written in batches of 1,000 events (or every 5 minutes, whichever comes first), partitioned by date:

```
coingecko-bucket/
  year=2025/
    month=12/
      day=15/
        {offset}.parquet
```

---

## Design Decisions

**Why Kafka over a simple queue?** Kafka's durable, replayable log means the pipeline can be re-run from any point in history — useful for backfilling the Postgres table or reprocessing with updated aggregation logic without re-ingesting from the source.

**Why Python over Flink/Spark?** The goal was correctness and clarity, not distributed scale. Python with `confluent-kafka` is the fastest path to a working prototype with features like windowing, watermarks, and idempotency without the operational overhead of a cluster.

**Why both Postgres and MinIO?** Postgres serves low-latency analytical queries. MinIO provides a raw, immutable record of every event in a columnar format (Parquet), decoupled from any schema changes downstream. This mirrors a common lakehouse pattern.

**Why Avro?** Schema Registry with Avro enforces a contract between producer and consumers. Adding a field without updating the schema will be caught at serialization time, not silently written as a malformed record.

---

## Failure Handling

| Failure | Handling |
|---|---|
| API downtime | Producer falls back to simulation mode; live mode retries with 5s sleep |
| Duplicate events | `enable.idempotence` on producer + `ON CONFLICT DO NOTHING` in Postgres INSERT |
| Late-arriving events | Watermark check drops events older than 1 minute behind the stream frontier |
| Consumer crash | Kafka offsets are committed after successful writes; restart replays from last committed offset |
| S3 write failure | Exception is raised and logged; consumer does not commit offset, so the batch is retried on restart |

---

## Future Improvements

- Add a `aggregated_price_metrics` table for pre-computed 5m/1h OHLCV windows
- Upgrade the S3 consumer to use Avro deserialization (currently uses raw JSON)
- Add a Streamlit dashboard for live price and rolling average visualization
- Parameterize window size and watermark delay via environment variables
- Add dead-letter topic for events that fail schema validation