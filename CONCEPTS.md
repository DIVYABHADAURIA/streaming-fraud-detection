# Streaming Fraud Detection — Concepts Reference

A reference guide for Kafka, PySpark Structured Streaming, and the medallion architecture as used in this project.

---

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Kafka Concepts](#2-kafka-concepts)
3. [PySpark Structured Streaming Concepts](#3-pyspark-structured-streaming-concepts)
4. [Watermarks — Deep Dive](#4-watermarks--deep-dive)
5. [Medallion Architecture (Bronze / Silver / Gold)](#5-medallion-architecture-bronze--silver--gold)
6. [Fraud Rules & How They Are Detected](#6-fraud-rules--how-they-are-detected)
7. [Key Patterns Used in This Codebase](#7-key-patterns-used-in-this-codebase)

---

## 1. Project Overview

```
Kafka topics  →  PySpark Structured Streaming  →  Delta Lake (Bronze / Silver / Gold)
```

Three Python files:

| File | Role |
|---|---|
| `event_producer.py` | Simulates a live ad platform; sends 200 events/sec to Kafka |
| `fraud_detector.py` | PySpark Structured Streaming consumer; applies fraud rules; writes Delta tables |
| `query_results.py` | Reads finished Delta tables as batch DataFrames for inspection |

---

## 2. Kafka Concepts

### Topics
Named channels, like a queue. Producers write to a topic; consumers read from it.

This project uses three topics:

| Topic | Event | Key fields |
|---|---|---|
| `ad-impressions` | Ad was shown to a user | `event_id`, `user_id`, `campaign_id` |
| `ad-clicks` | User clicked the ad | `impression_id` links back to impression |
| `ad-conversions` | User completed a purchase/signup | `click_id` links back to click |

### Offsets
The position of a message inside a topic. Kafka never deletes messages immediately — it keeps them for a retention period. Consumers track their own offset so they know where to resume.

`startingOffsets: "earliest"` — replay all messages from the very beginning of the topic.

Spark saves its offset progress in **checkpoints** on disk, so after a crash it resumes from exactly where it stopped.

### SASL_SSL
Authentication + encryption used by Confluent Cloud (the managed Kafka in this project). The `docker-compose.yml` provides a local alternative that needs no auth.

### Producer guarantees
```python
acks="all"    # wait for all replicas to confirm before marking message as sent
retries=3     # retry up to 3 times on transient failures
```

---

## 3. PySpark Structured Streaming Concepts

### readStream / writeStream
The entry and exit points of every streaming pipeline.

```python
# Read from Kafka as a continuous stream
df = spark.readStream.format("kafka").option(...).load()

# Write to Delta Lake as a continuous stream
df.writeStream.format("delta").option(...).start()
```

### Trigger
How often Spark wakes up to process new data (a "micro-batch").

```python
.trigger(processingTime="15 seconds")   # process every 15s
.trigger(processingTime="30 seconds")   # process every 30s
```

Between triggers, Kafka messages accumulate. On each trigger, Spark reads the new batch, processes it, and writes output.

### Output Modes

| Mode | Meaning | Used when |
|---|---|---|
| `append` | Write only new rows | Raw events, non-aggregated streams |
| `complete` | Rewrite the entire result table | Windowed aggregations (totals change as new data arrives) |
| `update` | Write only rows that changed | Stateful aggregations without windows |

### Checkpoints
A directory on disk where Spark saves:
- Current Kafka offsets (so it knows what it already read)
- Intermediate aggregation state

Without a checkpoint, a restart means reprocessing everything from scratch (or losing state).

```python
.option("checkpointLocation", "/tmp/checkpoints/silver/clicks")
```

### foreachBatch
An escape hatch that hands you each micro-batch as a regular (static) DataFrame. Useful when streaming API restrictions get in the way.

```python
def _process(batch_df, batch_id):
    # batch_df is a normal DataFrame — use any batch operation here
    counts = batch_df.groupBy("user_id").count()
    result = batch_df.join(counts, "user_id")
    result.write.format("delta").mode("append").save(path)

stream.writeStream.foreachBatch(_process).start()
```

Why it's needed here: streaming aggregations with `append` output mode are not allowed in Spark. `foreachBatch` sidesteps this by making the aggregation happen inside a batch context.

---

## 4. Watermarks — Deep Dive

### The Problem
Events don't always arrive in order. A click that happened at `10:05` might reach Spark at `10:15` due to network delays or slow producers. Without a boundary, Spark would hold state for every open window forever, waiting for arbitrarily late events.

### What a Watermark Does
A watermark tells Spark: **"Wait this long for late events — then close the window and free the memory."**

```python
.withWatermark("timestamp", "10 minutes")
```

### The Formula

```
Watermark threshold = max(event_timestamp seen so far) - delay_tolerance
```

An event is **accepted** if:  `event.timestamp >= watermark_threshold`
An event is **dropped** if:   `event.timestamp <  watermark_threshold`

### Critical point — watermark is NOT anchored to window boundaries

The watermark advances based on the **latest event timestamp Spark has seen across ALL incoming data**, not based on window start/end times.

---

### Worked Example

```
Events arriving at Spark (shown by their event timestamp):

10:00  click U001  → max_seen=10:00, watermark=09:50
10:03  click U002  → max_seen=10:03, watermark=09:53
10:11  click U003  → max_seen=10:11, watermark=10:01
10:14  click U004  → max_seen=10:14, watermark=10:04

A LATE event arrives now:
10:02  click U005  → event time 10:02 < watermark 10:04  ❌ DROPPED

10:20  click U006  → max_seen=10:20, watermark=10:10
                     → Window 10:00–10:10 is now CLOSED
                     → Results for that window are emitted
                     → State for that window is freed from memory
```

---

### Common misconception — "can I accept events up to 10:20 for the 10:00–10:10 window?"

Not quite. The watermark doesn't know about window boundaries. Here's the precise rule:

```
Latest event Spark has seen: 10:25
Watermark threshold:          10:15   (10:25 - 10 min)

✅ Event timestamped 10:16 → accepted  (10:16 >= 10:15, only 9 min behind)
❌ Event timestamped 10:14 → dropped   (10:14 < 10:15, 11 min behind)
```

The question is always: **"Is this event's timestamp within 10 minutes of the latest timestamp Spark has seen?"** — not "is it within 10 minutes of the window end?"

---

### Tradeoff: latency vs. correctness

| Watermark delay | Late event tolerance | Memory usage | Result latency |
|---|---|---|---|
| Short (1 min) | Low — drops many late events | Low | Fast |
| Long (30 min) | High — catches most late events | High | Slow |

This project uses **10 minutes** — a reasonable default for a real-time fraud system where events rarely arrive more than a few minutes late.

---

### Where watermarks appear in this project

| Location | Purpose |
|---|---|
| `read_kafka()` — `.withWatermark("timestamp", "10 minutes")` | Raw stream: tolerate events arriving up to 10 min late |
| `detect_orphan_conversions()` — both `clicks_df` and `conversions_df` | Stream-stream join: Spark holds state for both sides within the watermark window |
| `silver_for_gold` readStream — `.withWatermark("timestamp", "10 minutes")` | Gold aggregation: 1-min tumbling windows close once watermark passes their end |

---

## 5. Medallion Architecture (Bronze / Silver / Gold)

A layered data storage pattern where each layer adds more refinement.

```
Kafka  →  Bronze  →  Silver  →  Gold
           raw       cleaned     aggregated
```

### Bronze — Raw, append-only
Exactly what arrived from Kafka. No transformations. Schema-on-read.
Think of it as an immutable audit log.

```
/tmp/ad-fraud-detection/delta/bronze/
  ├── impressions/
  ├── clicks/
  └── conversions/
```

### Silver — Parsed, typed, fraud-flagged
Each event is parsed into typed columns, deduplicated, and tagged with fraud flags.

For clicks:
- `is_click_flood` — user clicked >50 times in this micro-batch
- `is_bot_ip` — IP hit >20 distinct campaigns in this micro-batch
- `is_instant_click` — `time_since_impression_ms < 200`
- `is_fraud` — any of the above is true

For conversions:
- `is_orphan_conversion` — no matching click found in the ±30 min window

### Gold — Aggregated fraud alerts
1-minute tumbling windows per campaign. Output columns:

| Column | Meaning |
|---|---|
| `campaign_id` | Which campaign |
| `window_start / window_end` | The 1-min bucket |
| `total_clicks` | All clicks in that window |
| `fraud_clicks` | Clicks flagged as fraud |
| `fraud_rate` | `fraud_clicks / total_clicks` |
| `flood_clicks` | Breakdown: click flooding |
| `bot_ip_clicks` | Breakdown: bot IP |
| `instant_clicks` | Breakdown: instant click |

---

## 6. Fraud Rules & How They Are Detected

| Rule | Pattern | Detection method | Threshold |
|---|---|---|---|
| Click Flooding | User `U99999` spams clicks | Count clicks per `user_id` per micro-batch | >50 clicks |
| Bot IP | IPs `10.0.0.x` hit many campaigns | Count distinct `campaign_id` per `ip_address` | >20 campaigns |
| Instant Click | Click arrives <200ms after impression | Check `time_since_impression_ms` field | <200ms |
| Orphan Conversion | Conversion with no prior click | Stream-stream LEFT OUTER join on `click_id` | no match in ±30 min |

---

## 7. Key Patterns Used in This Codebase

### Pattern 1 — Kafka binary → parsed DataFrame
Kafka messages arrive as raw bytes. Cast to string, then parse JSON:

```python
spark.readStream.format("kafka").load()
  .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
  .select("data.*")
```

### Pattern 2 — foreachBatch for batch-style aggregations inside a stream
Used in `write_silver_clicks()` to apply Rules 1 & 2 without hitting streaming mode restrictions:

```python
def _process(batch_df, batch_id):
    counts = batch_df.groupBy("user_id").agg(F.count(...))
    result = batch_df.join(counts, "user_id").withColumn(...)
    result.write.format("delta").mode("append").save(path)

stream.writeStream.foreachBatch(_process).start()
```

### Pattern 3 — Stream-stream LEFT OUTER join (orphan detection)
Both streams must have a watermark. A time-range condition is required by Spark:

```python
conversions_df.join(
    clicks_keyed,
    on=(
        (conversions_df.click_id == clicks_keyed.click_id) &
        (conversions_df.timestamp >= clicks_keyed.timestamp - expr("INTERVAL 30 MINUTES")) &
        (conversions_df.timestamp <= clicks_keyed.timestamp + expr("INTERVAL 30 MINUTES"))
    ),
    how="left",
)
.withColumn("is_orphan_conversion", F.col("click_found").isNull())
```

### Pattern 4 — Delta table as an intermediate stream source
The Gold layer can't read from the Silver streaming query directly (one stream, one sink). Instead, Silver writes to a Delta table, and Gold opens a new `readStream` on that Delta table:

```python
# Silver writes here
result.write.format("delta").mode("append").save(silver_path)

# Gold reads from the same path as a new stream
spark.readStream.format("delta").load(silver_path)
```

### Pattern 5 — Pre-create Delta table to avoid race conditions
The Gold stream starts immediately but Silver may not have written any data yet. Pre-creating the table prevents Gold from failing on "path does not exist":

```python
DeltaTable.createIfNotExists(spark) \
    .location(silver_clicks_path) \
    .addColumns(SILVER_CLICK_SCHEMA) \
    .execute()
```

---

## How to run this locally

```bash
# 1. Start local Kafka (no auth needed)
docker-compose up -d

# 2. In terminal 1 — produce events
export CONFLUENT_API_KEY=...  # or swap bootstrap to localhost:9092
python event_producer.py

# 3. In terminal 2 — run the detector
python fraud_detector.py

# 4. After ~1 minute, inspect results
python query_results.py
```

kafka-ui is available at `http://localhost:8080` to browse topics and messages visually.
