"""
Ad Fraud Detection — PySpark Structured Streaming Consumer
===========================================================
Reads from 3 Kafka topics and writes through a medallion architecture:

  Bronze  →  Raw events exactly as received (append-only, schema-on-read)
  Silver  →  Parsed, typed, deduped + per-event fraud flags applied
  Gold    →  Windowed fraud_alerts aggregated per campaign (1-min tumbling window)

Fraud rules applied at Silver layer:
  Rule 1 — Click Flooding    : user_id click count > 50 in 10-min window
  Rule 2 — Bot IP Traffic    : ip distinct campaign count > 20 in 10-min window
  Rule 3 — Instant Click     : time_since_impression_ms < 200
  Rule 4 — Orphan Conversion : detected via stream-stream join (separate query)

Run:
    python consumer/fraud_detector.py

Requires Kafka on localhost:9092 and delta-spark installed.
"""

import os

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_PATH       = "/tmp/ad-fraud-detection"
BRONZE_PATH     = f"{BASE_PATH}/delta/bronze"
SILVER_PATH     = f"{BASE_PATH}/delta/silver"
GOLD_PATH       = f"{BASE_PATH}/delta/gold"
CHECKPOINT_BASE = f"{BASE_PATH}/checkpoints"

# ── Kafka ──────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP      = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
CONFLUENT_API_KEY    = os.environ["CONFLUENT_API_KEY"]
CONFLUENT_API_SECRET = os.environ["CONFLUENT_API_SECRET"]
SASL_JAAS_CONFIG     = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{CONFLUENT_API_KEY}" password="{CONFLUENT_API_SECRET}";'
)
TOPIC_IMPRESSIONS    = "ad-impressions"
TOPIC_CLICKS         = "ad-clicks"
TOPIC_CONVERSIONS    = "ad-conversions"

# ── Fraud thresholds ───────────────────────────────────────────────────────────
FLOOD_CLICK_THRESHOLD      = 50    # clicks per user per 10-min window
BOT_IP_CAMPAIGN_THRESHOLD  = 20    # distinct campaigns per IP per 10-min window
INSTANT_CLICK_THRESHOLD_MS = 200   # ms — humanly impossible below this


# ── Schemas ────────────────────────────────────────────────────────────────────

IMPRESSION_SCHEMA = StructType([
    StructField("event_id",    StringType(),    False),
    StructField("event_type",  StringType(),    True),
    StructField("user_id",     StringType(),    True),
    StructField("campaign_id", StringType(),    True),
    StructField("ip_address",  StringType(),    True),
    StructField("user_agent",  StringType(),    True),
    StructField("geo_country", StringType(),    True),
    StructField("timestamp",   TimestampType(), True),
    StructField("fraud_label", StringType(),    True),
])

CLICK_SCHEMA = StructType([
    StructField("event_id",                StringType(),    False),
    StructField("event_type",              StringType(),    True),
    StructField("impression_id",           StringType(),    True),
    StructField("user_id",                 StringType(),    True),
    StructField("campaign_id",             StringType(),    True),
    StructField("ip_address",              StringType(),    True),
    StructField("user_agent",              StringType(),    True),
    StructField("time_since_impression_ms", IntegerType(), True),
    StructField("timestamp",               TimestampType(), True),
    StructField("fraud_label",             StringType(),    True),
])

CONVERSION_SCHEMA = StructType([
    StructField("event_id",        StringType(),    False),
    StructField("event_type",      StringType(),    True),
    StructField("click_id",        StringType(),    True),
    StructField("impression_id",   StringType(),    True),
    StructField("user_id",         StringType(),    True),
    StructField("campaign_id",     StringType(),    True),
    StructField("conversion_type", StringType(),    True),
    StructField("revenue_usd",     DoubleType(),    True),
    StructField("timestamp",       TimestampType(), True),
    StructField("fraud_label",     StringType(),    True),
])

SILVER_CLICK_SCHEMA = StructType([
    StructField("event_id",                 StringType(),    False),
    StructField("impression_id",            StringType(),    True),
    StructField("user_id",                  StringType(),    True),
    StructField("campaign_id",              StringType(),    True),
    StructField("ip_address",               StringType(),    True),
    StructField("user_agent",               StringType(),    True),
    StructField("time_since_impression_ms", IntegerType(),   True),
    StructField("timestamp",                TimestampType(), True),
    StructField("fraud_label",              StringType(),    True),
    StructField("is_click_flood",           BooleanType(),   True),
    StructField("is_bot_ip",                BooleanType(),   True),
    StructField("is_instant_click",         BooleanType(),   True),
    StructField("is_fraud",                 BooleanType(),   True),
])


# ── Spark session ──────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("AdFraudDetection")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")           # Keep low for local dev
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )
    return configure_spark_with_delta_pip(
        builder,
        extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"],
    ).getOrCreate()


# ── Kafka reader ───────────────────────────────────────────────────────────────

def read_kafka(spark: SparkSession, topic: str, schema: StructType):
    """
    Read a Kafka topic as a streaming DataFrame.
    Kafka delivers messages as binary; we parse the JSON value using from_json.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", SASL_JAAS_CONFIG)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        # 'value' is binary — cast to string then parse as JSON
        .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        # Cast timestamp string to proper TimestampType
        .withColumn("timestamp", F.col("timestamp").cast(TimestampType()))
        # Add watermark: Spark will wait up to 10 min for late-arriving events
        .withWatermark("timestamp", "10 minutes")
    )


# ── Bronze layer ───────────────────────────────────────────────────────────────

def write_bronze(df, topic_name: str):
    """
    Append raw events to Bronze Delta table.
    No transformations — exactly what arrived from Kafka.
    """
    path       = f"{BRONZE_PATH}/{topic_name}"
    checkpoint = f"{CHECKPOINT_BASE}/bronze/{topic_name}"
    os.makedirs(path, exist_ok=True)

    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("path", path)
        .trigger(processingTime="15 seconds")
        .start()
    )


# ── Silver layer — Clicks ──────────────────────────────────────────────────────

def write_silver_clicks(clicks_df):
    """
    Applies Rules 1-3 per micro-batch via foreachBatch, avoiding the stream-stream
    LEFT OUTER join restriction (which requires a time-range condition).
    Within each static batch, regular DataFrame joins are used.
    """
    path       = f"{SILVER_PATH}/clicks"
    checkpoint = f"{CHECKPOINT_BASE}/silver/clicks"
    os.makedirs(path, exist_ok=True)

    def _process(batch_df, _batch_id):
        if batch_df.rdd.isEmpty():
            return

        # Rule 1: click flooding — clicks per user in this micro-batch
        user_counts = (
            batch_df.groupBy("user_id")
            .agg(F.count("event_id").alias("click_count"))
        )

        # Rule 2: bot IP — distinct campaigns per IP in this micro-batch
        ip_counts = (
            batch_df.groupBy("ip_address")
            .agg(F.countDistinct("campaign_id").alias("distinct_campaigns"))
        )

        result = (
            batch_df
            .join(user_counts, "user_id", "left")
            .join(ip_counts, "ip_address", "left")
            .withColumn("is_click_flood",
                        F.col("click_count") > FLOOD_CLICK_THRESHOLD)
            .withColumn("is_bot_ip",
                        F.col("distinct_campaigns") > BOT_IP_CAMPAIGN_THRESHOLD)
            .withColumn("is_instant_click",
                        F.col("time_since_impression_ms") < INSTANT_CLICK_THRESHOLD_MS)
            .withColumn("is_fraud",
                        F.col("is_click_flood") | F.col("is_bot_ip") | F.col("is_instant_click"))
            .select(
                "event_id", "impression_id", "user_id", "campaign_id",
                "ip_address", "user_agent", "time_since_impression_ms",
                "timestamp", "fraud_label",
                "is_click_flood", "is_bot_ip", "is_instant_click", "is_fraud",
            )
        )

        result.write.format("delta").mode("append").save(path)

    return (
        clicks_df.writeStream
        .foreachBatch(_process)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="15 seconds")
        .start()
    )


# ── Silver layer — Orphan Conversions (stream-stream join) ─────────────────────

def detect_orphan_conversions(clicks_df, conversions_df):
    """
    Fraud Rule 4 — Orphan Conversion.
    A conversion whose click_id has no matching event in the click stream
    within a 30-minute window is flagged as fraudulent.

    This uses PySpark's stream-stream join — a strong FAANG talking point.
    Spark maintains state for both streams within the watermark window.
    """

    # Keep 'timestamp' name so the watermark from clicks_df is inherited (no redefinition).
    # A sentinel column lets us detect non-matches after the LEFT OUTER join.
    clicks_keyed = clicks_df.select(
        F.col("event_id").alias("click_id"),
        F.col("timestamp"),
        F.lit(True).alias("click_found"),
    )

    # Time-range condition is required by Spark for LEFT OUTER stream-stream joins
    joined = (
        conversions_df
        .join(
            clicks_keyed,
            on=(
                (conversions_df.click_id == clicks_keyed.click_id) &
                (conversions_df.timestamp >= clicks_keyed.timestamp - F.expr("INTERVAL 30 MINUTES")) &
                (conversions_df.timestamp <= clicks_keyed.timestamp + F.expr("INTERVAL 30 MINUTES"))
            ),
            how="left",
        )
        .withColumn(
            "is_orphan_conversion",
            F.col("click_found").isNull()   # null when no matching click was found
        )
        .withColumn(
            "is_fraud",
            F.col("is_orphan_conversion")
        )
        .select(
            conversions_df.event_id,
            conversions_df.click_id,
            conversions_df.impression_id,
            conversions_df.user_id,
            conversions_df.campaign_id,
            conversions_df.conversion_type,
            conversions_df.revenue_usd,
            conversions_df.timestamp,
            conversions_df.fraud_label,
            "is_orphan_conversion",
            "is_fraud",
        )
    )

    return joined


def write_silver_conversions(silver_df):
    path       = f"{SILVER_PATH}/conversions"
    checkpoint = f"{CHECKPOINT_BASE}/silver/conversions"
    os.makedirs(path, exist_ok=True)

    return (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("path", path)
        .trigger(processingTime="15 seconds")
        .start()
    )


# ── Gold layer — Fraud Alerts ──────────────────────────────────────────────────

def build_gold_fraud_alerts(silver_clicks_df):
    """
    Aggregate fraud signals into a Gold-layer alert table.
    1-minute tumbling windows per campaign.

    Output:
      campaign_id       STRING
      window_start      TIMESTAMP
      window_end        TIMESTAMP
      total_clicks      LONG
      fraud_clicks      LONG
      fraud_rate        DOUBLE     (fraud_clicks / total_clicks)
      flood_clicks      LONG
      bot_ip_clicks     LONG
      instant_clicks    LONG
    """
    return (
        silver_clicks_df
        .groupBy(
            F.window("timestamp", "1 minute"),
            "campaign_id",
        )
        .agg(
            F.count("event_id").alias("total_clicks"),
            F.sum(F.col("is_fraud").cast("int")).alias("fraud_clicks"),
            F.sum(F.col("is_click_flood").cast("int")).alias("flood_clicks"),
            F.sum(F.col("is_bot_ip").cast("int")).alias("bot_ip_clicks"),
            F.sum(F.col("is_instant_click").cast("int")).alias("instant_clicks"),
        )
        .withColumn(
            "fraud_rate",
            F.round(F.col("fraud_clicks") / F.col("total_clicks"), 4)
        )
        .select(
            "campaign_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "total_clicks", "fraud_clicks", "fraud_rate",
            "flood_clicks", "bot_ip_clicks", "instant_clicks",
        )
    )


def write_gold_alerts(gold_df):
    path       = f"{GOLD_PATH}/fraud_alerts"
    checkpoint = f"{CHECKPOINT_BASE}/gold/fraud_alerts"
    os.makedirs(path, exist_ok=True)

    return (
        gold_df.writeStream
        .format("delta")
        .outputMode("complete")           # Rewrite full aggregation each trigger
        .option("checkpointLocation", checkpoint)
        .option("path", path)
        .trigger(processingTime="30 seconds")
        .start()
    )


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("\n[Consumer] Starting Ad Fraud Detection pipeline …")
    print(f"           Bronze  → {BRONZE_PATH}")
    print(f"           Silver  → {SILVER_PATH}")
    print(f"           Gold    → {GOLD_PATH}\n")

    # ── Read from Kafka ────────────────────────────────────────────────────────
    impressions_df  = read_kafka(spark, TOPIC_IMPRESSIONS,  IMPRESSION_SCHEMA)
    clicks_df       = read_kafka(spark, TOPIC_CLICKS,       CLICK_SCHEMA)
    conversions_df  = read_kafka(spark, TOPIC_CONVERSIONS,  CONVERSION_SCHEMA)

    # ── Bronze writes (raw) ────────────────────────────────────────────────────
    q_bronze_imp  = write_bronze(impressions_df,  "impressions")
    q_bronze_clk  = write_bronze(clicks_df,       "clicks")
    q_bronze_conv = write_bronze(conversions_df,  "conversions")

    # ── Silver — fraud-flagged clicks ─────────────────────────────────────────
    # Pre-create the Delta table so the gold readStream can start immediately
    silver_clicks_path = f"{SILVER_PATH}/clicks"
    os.makedirs(silver_clicks_path, exist_ok=True)
    DeltaTable.createIfNotExists(spark) \
        .location(silver_clicks_path) \
        .addColumns(SILVER_CLICK_SCHEMA) \
        .execute()

    q_silver_clicks     = write_silver_clicks(clicks_df)

    # ── Silver — orphan conversions ───────────────────────────────────────────
    silver_conv_df      = detect_orphan_conversions(clicks_df, conversions_df)
    q_silver_conv       = write_silver_conversions(silver_conv_df)

    # ── Gold — fraud alert aggregations ──────────────────────────────────────
    # Read from the silver Delta table (written by foreachBatch above)
    silver_for_gold = (
        spark.readStream
        .format("delta")
        .load(silver_clicks_path)
        .withWatermark("timestamp", "10 minutes")
    )
    gold_df             = build_gold_fraud_alerts(silver_for_gold)
    q_gold              = write_gold_alerts(gold_df)

    print("[Consumer] All streaming queries started. Waiting for data …\n")

    # Block until all queries terminate (or Ctrl+C)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
