# Real-Time Data Streaming Pipeline

Real-time streaming pipeline that ingests user data from RandomUser API,
streams through Apache Kafka, processes with Spark Structured Streaming,
and stores in Cassandra — with built-in fault tolerance via Dead Letter Queue
and exponential backoff retry.

## Architecture

![System Architecture](Data%20engineering%20architecture.png)

**Data Flow:**

```
RandomUser API → Airflow (daily schedule)
                    ↓
              Kafka Producer → user_data topic → Spark Structured Streaming → Cassandra
                    ↓ (on failure)
              user_data_dlq topic (Dead Letter Queue)
```

---

## Fault Tolerance Mechanisms

| Mechanism | Implementation | Problem Solved |
|-----------|---------------|----------------|
| **Dead Letter Queue** | Failed messages routed to `user_data_dlq` Kafka topic with error metadata | Prevents data loss from non-recoverable errors |
| **Exponential Backoff with Jitter** | `delay = min(base * 2^attempt, max_delay) * (1 + random(0, 0.5))` | Prevents thundering herd on retries |
| **Retryable vs Non-retryable** | `is_retryable()` checks error type; connection errors → retry, validation errors → DLQ | Avoids wasting resources on permanent failures |
| **Spark Checkpoint** | Offsets persisted to checkpoint directory, recovered on restart | No data loss on Spark failure recovery |
| **Cassandra Upsert** | Primary key-based INSERT acts as natural upsert | Idempotent writes prevent duplicates |
| **Pydantic Validation** | Schema validation before Kafka send; rejects malformed data to DLQ | Catches bad data before it enters the pipeline |

---

## Key Design Decisions

1. **DLQ over retry-forever** — Non-recoverable errors (schema validation, serialization) route to DLQ instead of retrying indefinitely
2. **Backoff with jitter** — Adds random 0-50% delay variance to prevent multiple clients from retrying at the same time
3. **Custom exception hierarchy** — 4 categories (API / Kafka / Cassandra / Data Validation) with `is_retryable()` for automatic retry-or-DLQ routing
4. **Pydantic before Kafka** — Validates data schema before sending to Kafka, so downstream consumers never receive malformed data

---

## Quick Start

### 1. Clone and configure

```bash
git clone https://github.com/TonyTang222/Realtime-Data-Streaming.git
cd Realtime-Data-Streaming
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env
```

### 2. Start all services

```bash
docker compose up -d
```

### 3. Access the services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | airflow / airflow |
| Kafka Control Center | http://localhost:9021 | - |

### 4. Query Cassandra

```bash
# Connect to Cassandra shell
docker exec -it cassandra cqlsh

# Query stored data
SELECT * FROM spark_streams.created_users LIMIT 10;
```

### 5. Run tests

```bash
pytest tests/ -v
```

### Stopping Services

```bash
docker compose down

# Remove all data volumes
docker compose down -v
```

---

## Project Structure

```
├── dags/
│   └── kafka_stream.py              # Airflow DAG: API → validate → transform → Kafka
├── src/
│   ├── config/
│   │   ├── settings.py              # Environment variable configuration (12-factor)
│   │   └── schemas.py               # Pydantic models + Spark schema + Cassandra DDL
│   ├── producers/
│   │   ├── api_client.py            # RandomUser API client with retry
│   │   └── kafka_producer.py        # Resilient Kafka producer with DLQ fallback
│   ├── consumers/
│   │   ├── spark_consumer.py        # Spark Structured Streaming → Cassandra
│   │   └── dlq_consumer.py          # DLQ message analysis, processing, and retry
│   ├── transformers/
│   │   └── user_transformer.py      # API response → flat user record
│   ├── utils/
│   │   ├── retry.py                 # Exponential backoff with jitter decorator
│   │   ├── logging_config.py        # JSON structured logging
│   │   └── validators.py            # Input sanitization utilities
│   └── exceptions/
│       └── custom_exceptions.py     # Exception hierarchy with is_retryable()
├── tests/
│   ├── unit/                        # Unit tests (retry, schemas, transformers, etc.)
│   └── integration/                 # Pipeline integration tests
├── docker-compose.yml               # 11 services with health checks
├── Dockerfile                       # Spark application image
└── spark_stream.py                  # Spark streaming entry point
```

---

## Tech Stack

- **Apache Airflow 2.6** — DAG scheduling and orchestration
- **Apache Kafka** (Confluent 7.4) — Message broker with Dead Letter Queue
- **Spark Structured Streaming 3.5** — Real-time stream processing from Kafka to Cassandra
- **Apache Cassandra** — Distributed NoSQL storage with natural upsert
- **Pydantic v1** — Schema validation for API responses and transformed data
- **Docker Compose** — 12-service orchestration with health checks and startup ordering
- **pytest** — Unit and integration tests with mock-based external service isolation
