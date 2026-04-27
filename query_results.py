"""
Query Delta Tables — Inspect Pipeline Output
=============================================
Run this after fraud_detector.py has been running for a few minutes
to query Bronze, Silver, and Gold tables with Spark SQL.

Run:
    python notebooks/query_results.py
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

BASE_PATH  = "/tmp/ad-fraud-detection/delta"

def build_spark():
    builder = (
        SparkSession.builder
        .appName("QueryFraudTables")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n" + "="*60)
    print("  BRONZE — Raw clicks (sample 5 rows)")
    print("="*60)
    spark.read.format("delta").load(f"{BASE_PATH}/bronze/clicks").show(5, truncate=False)

    print("\n" + "="*60)
    print("  SILVER — Fraud-flagged clicks")
    print("="*60)
    silver = spark.read.format("delta").load(f"{BASE_PATH}/silver/clicks")
    total  = silver.count()
    fraud  = silver.filter("is_fraud = true").count()
    print(f"\n  Total clicks : {total:,}")
    print(f"  Fraud clicks : {fraud:,}  ({fraud/total*100:.1f}%)\n")
    silver.filter("is_fraud = true").show(10, truncate=False)

    print("\n" + "="*60)
    print("  GOLD — Fraud Alerts per Campaign (top 10 by fraud rate)")
    print("="*60)
    (
        spark.read.format("delta").load(f"{BASE_PATH}/gold/fraud_alerts")
        .orderBy("fraud_rate", ascending=False)
        .show(10, truncate=False)
    )

    print("\n" + "="*60)
    print("  SILVER — Orphan Conversions")
    print("="*60)
    (
        spark.read.format("delta").load(f"{BASE_PATH}/silver/conversions")
        .filter("is_orphan_conversion = true")
        .select("event_id", "click_id", "campaign_id", "revenue_usd", "is_orphan_conversion")
        .show(10, truncate=False)
    )


if __name__ == "__main__":
    main()
