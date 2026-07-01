# Real-Time Data Streaming Pipeline

Production-grade streaming pipeline that ingests user data from a REST API,
streams through Apache Kafka, processes with Spark Structured Streaming,
and stores in Cassandra — observable end-to-end via Prometheus and Grafana.

[![CI](https://github.com/TonyTang222/Realtime-Data-Streaming/actions/workflows/ci.yml/badge.svg)](https://github.com/TonyTang222/Realtime-Data-Streaming/actions/workflows/ci.yml)
![Kafka](https://img.shields.io/badge/Kafka-Confluent_7.4-231F20?logo=apachekafka&logoColor=white)
![Spark](https://img.shields.io/badge/Spark_Streaming-3.5-E25A1C?logo=apachespark&logoColor=white)
![Cassandra](https://img.shields.io/badge/Cassandra-latest-1287B1?logo=apachecassandra&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.6-017CEE?logo=apacheairflow&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-2.51-E6522C?logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-10.4-F46800?logo=grafana&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.9-3776AB?logo=python&logoColor=white)

---

## What This Project Delivers

Five production concerns, each backed by a concrete implementation:

| Layer | Implementation | Problem Solved |
|---|---|---|
| **Ingestion** | Airflow DAG with exponential backoff retry + Pydantic validation before Kafka send | "Bad data silently enters the pipeline" |
| **Messaging** | Kafka `user_data` topic (3 partitions) + `user_data_dlq` Dead Letter Queue | "Failed messages are lost with no trace" |
| **Processing** | Spark Structured Streaming with `foreachBatch` + Spark Checkpoint for offset persistence | "Pipeline restart causes data loss or duplicates" |
| **Storage** | Cassandra UUID primary key upsert — idempotent by design | "Duplicate writes corrupt the dataset" |
| **Observability** | Prometheus metrics on producer + consumer; Grafana dashboard with throughput, DLQ rate, p99 latency | "We find out the pipeline is broken from end-users" |

---

## Architecture

```
RandomUser API → Airflow DAG (daily)
                      │
                      ▼
              Kafka Producer ──────────────────────► user_data topic (3 partitions)
              (retry + DLQ)                                    │
                      │                                        ▼
                      │ (validation failure)     Spark Structured Streaming
                      ▼                          (foreachBatch + Checkpoint)
              user_data_dlq topic                              │
                                                               ▼
                                                          Cassandra
                                                     (UUID upsert — idempotent)

Prometheus ◄── /metrics (producer :8000)
Prometheus ◄── /metrics (consumer  :8001)
               │
               ▼
           Grafana Dashboard
```

---

## Fault Tolerance Design

| Mechanism | Implementation | Guarantee |
|---|---|---|
| **Custom Exception Hierarchy** | 4 categories: `APIError` / `KafkaError` / `CassandraError` / `DataValidationError`, each with `is_retryable()` | Permanent failures skip retry and go straight to DLQ |
| **Exponential Backoff + Jitter** | `delay = min(base × 2ⁿ, max) × (1 + rand(0, 0.5))` | Prevents thundering herd when broker recovers |
| **Dead Letter Queue** | Failed messages routed to `user_data_dlq` with error type, message, and retry count | Zero silent data loss — every failure is traceable |
| **Spark Checkpoint** | Kafka offsets persisted to checkpoint directory | At-least-once delivery; idempotent Cassandra writes make it effectively exactly-once |
| **Cassandra UUID Upsert** | `INSERT ... IF NOT EXISTS` on UUID primary key | Duplicate messages from replay produce identical rows, not corruption |
| **Pydantic Pre-validation** | Schema validated before Kafka send; rejects malformed data to DLQ | Downstream consumers never receive invalid data |

---

## Observability

Prometheus scrapes two endpoints every 15 seconds:

| Metric | Type | What It Tells You |
|---|---|---|
| `kafka_messages_sent_total` | Counter | Producer throughput |
| `kafka_dlq_messages_total` | Counter | Failure rate — should stay at 0 |
| `kafka_send_retries_total` | Counter | Broker instability indicator |
| `kafka_send_duration_seconds` | Histogram | Producer latency distribution |
| `spark_batches_processed_total` | Counter | Spark micro-batch throughput |
| `spark_records_written_total` | Counter | Cassandra write throughput |
| `spark_batch_duration_seconds` | Histogram | p50 / p99 processing latency |

Grafana dashboard auto-provisions on startup — no manual setup required.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.6 |
| Messaging | Apache Kafka (Confluent 7.4) + Dead Letter Queue |
| Stream Processing | Spark Structured Streaming 3.5 |
| Storage | Apache Cassandra |
| Observability | Prometheus 2.51 + Grafana 10.4 |
| Validation | Pydantic v1 |
| Infra | Docker Compose (14 services), GitHub Actions CI |
| Testing | pytest — unit + integration |

---

## Quick Start

### 1. Clone and configure

```bash
git clone https://github.com/TonyTang222/Realtime-Data-Streaming.git
cd Realtime-Data-Streaming
cp .env.example .env
```

### 2. Start all services

```bash
docker compose up -d
```

First run pulls ~4 GB of images. Subsequent starts take ~30 seconds.

### 3. Trigger the pipeline

```bash
# Via Airflow UI — navigate to DAGs → user_auto_loading → trigger
# Or via CLI:
docker compose exec webserver airflow dags trigger user_auto_loading
```

### 4. Access services

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8081 | airflow / airflow |
| Kafka Control Center | http://localhost:9021 | — |
| Spark Master UI | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9093 | — |

### 5. Verify data end-to-end

```bash
# Check Kafka messages
docker exec broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic user_data --from-beginning --max-messages 5

# Query Cassandra
docker exec -it cassandra cqlsh -e \
  "SELECT id, first_name, last_name, email FROM spark_streams.created_users LIMIT 10;"
```

### 6. Run tests

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest tests/ -v
```

### Stop services

```bash
docker compose down        # stop containers
docker compose down -v     # stop + remove volumes
```

---

## Project Structure

```
├── dags/
│   └── kafka_stream.py              # Airflow DAG: fetch → validate → transform → Kafka
├── src/
│   ├── config/
│   │   ├── settings.py              # 12-factor config via environment variables
│   │   └── schemas.py               # Pydantic models + Spark schema + Cassandra DDL
│   ├── producers/
│   │   ├── api_client.py            # RandomUser API client with retry
│   │   └── kafka_producer.py        # Resilient producer: retry → DLQ + Prometheus metrics
│   ├── consumers/
│   │   ├── spark_consumer.py        # Spark Structured Streaming → Cassandra + Prometheus metrics
│   │   └── dlq_consumer.py          # DLQ analysis, retry, and sampling
│   ├── transformers/
│   │   └── user_transformer.py      # API response → flat user record
│   ├── utils/
│   │   ├── retry.py                 # Exponential backoff + jitter decorator
│   │   ├── logging_config.py        # JSON structured logging
│   │   └── validators.py            # Input sanitization utilities
│   └── exceptions/
│       └── custom_exceptions.py     # Exception hierarchy with is_retryable()
├── monitoring/
│   ├── prometheus.yml               # Scrape config (producer :8000, consumer :8001)
│   └── grafana/provisioning/        # Auto-provisioned datasource + dashboard
├── tests/
│   ├── unit/                        # Retry, schemas, transformers, validators
│   └── integration/                 # End-to-end pipeline tests
├── docker-compose.yml               # 14 services with health checks + startup ordering
├── Dockerfile                       # Spark application image
└── spark_stream.py                  # Spark streaming entry point
```

---

## Key Design Decisions

**Why Cassandra over PostgreSQL?**
Cassandra's wide-row model handles high-throughput append workloads without write contention. UUID primary keys provide natural idempotency for Spark's at-least-once delivery.

**Why Spark Structured Streaming over Flink?**
Kafka + Spark + Cassandra share a mature connector ecosystem. `foreachBatch` exposes each micro-batch as a DataFrame, enabling both Cassandra writes and Prometheus instrumentation in one hook.

**Why DLQ as a Kafka topic rather than a database table?**
Keeping failed messages in Kafka preserves the ability to replay them through the same consumer infrastructure after fixing the root cause. A separate database would require a separate consumer.

**Why exponential backoff with jitter?**
Pure exponential backoff causes synchronized retry storms when multiple producers recover simultaneously. Jitter (±50% random variance) desynchronizes retries and spreads broker load.
