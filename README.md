# Real-Time Ad Click Fraud Detection Pipeline

A production-grade streaming data pipeline that detects ad click fraud in real time using **Apache Kafka**, **PySpark Structured Streaming**, and **Delta Lake** (Bronze → Silver → Gold medallion architecture).

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  SYNTHETIC DATA PRODUCER                │
│  Python · Faker · kafka-python                          │
│                                                         │
│  ad-impressions ──┐                                     │
│  ad-clicks     ───┼──► 3 Kafka Topics (200 events/sec) │
│  ad-conversions ──┘                                     │
└─────────────────────────────┬───────────────────────────┘
                              │ Kafka
                              ▼
┌─────────────────────────────────────────────────────────┐
│              PYSPARK STRUCTURED STREAMING               │
│                                                         │
│  BRONZE  Raw events, append-only, no transformation     │
│    ▼                                                     │
│  SILVER  Parsed + deduped + fraud flags applied         │
│    • Rule 1: Click Flooding  (>50 clicks/user/10min)    │
│    • Rule 2: Bot IP Traffic  (>20 campaigns/IP/10min)   │
│    • Rule 3: Instant Click   (<200ms after impression)  │
│    • Rule 4: Orphan Conv.    (stream-stream join)       │
│    ▼                                                     │
│  GOLD    Windowed fraud_alerts per campaign (1-min)     │
└─────────────────────────────┬───────────────────────────┘
                              │ Delta Lake
                              ▼
                    /tmp/ad-fraud-detection/
                    ├── delta/bronze/
                    ├── delta/silver/
                    └── delta/gold/
```

---

## Fraud Detection Rules

| Rule | Layer | Signal | Threshold |
|------|-------|--------|-----------|
| Click Flooding | Silver | clicks per `user_id` per 10-min window | > 50 |
| Bot IP Traffic | Silver | distinct `campaign_id` per `ip_address` per 10-min window | > 20 |
| Instant Click | Silver | `time_since_impression_ms` | < 200ms |
| Orphan Conversion | Silver | conversion with no matching click (stream-stream join) | no match |

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Message broker | Apache Kafka 7.5 |
| Stream processing | PySpark Structured Streaming 3.5 |
| Storage format | Delta Lake 3.0 |
| Data generation | Python, Faker |
| Orchestration | Local (extensible to Databricks Workflows / Airflow) |
| Infrastructure | Docker Compose |

---

## Quick Start

### 1. Start Kafka
```bash
cd docker
docker-compose up -d

# Verify Kafka is running
docker ps

# Optional: open Kafka UI at http://localhost:8080
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Start the streaming consumer (Terminal 1)
```bash
python consumer/fraud_detector.py
```

### 4. Start the event producer (Terminal 2)
```bash
python producer/event_producer.py
```

### 5. Query results after ~2 minutes (Terminal 3)
```bash
python notebooks/query_results.py
```

---

## Project Structure

```
ad-fraud-detection/
├── docker/
│   └── docker-compose.yml       # Kafka + Zookeeper + Kafka UI
├── producer/
│   └── event_producer.py        # Synthetic ad event generator
├── consumer/
│   └── fraud_detector.py        # PySpark streaming pipeline
├── notebooks/
│   └── query_results.py         # Query Bronze/Silver/Gold tables
├── requirements.txt
└── README.md
```

---

## Key Engineering Concepts Demonstrated

- **Medallion Architecture** — Bronze (raw) → Silver (cleaned + flagged) → Gold (aggregated alerts)
- **Watermarking** — handles late-arriving events up to 10 minutes late
- **Stateful Streaming** — windowed aggregations maintain state across micro-batches
- **Stream-Stream Join** — detects orphan conversions by joining click and conversion streams within a time window
- **Exactly-Once Semantics** — Delta Lake + checkpoint-based processing guarantees no duplicate writes
- **Multi-Topic Kafka Consumption** — 3 independent topics consumed by a single Spark application

---

## Resume Bullet

> *"Built a real-time ad click fraud detection pipeline using Apache Kafka and PySpark Structured Streaming, processing 200+ events/sec across impression, click, and conversion topics into a Bronze-Silver-Gold Delta Lake architecture — applying 4 fraud detection rules including stateful windowed aggregations and stream-stream joins, flagging 20% simulated fraud traffic with sub-30-second latency."*
