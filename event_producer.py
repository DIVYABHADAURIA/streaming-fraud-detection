"""
Ad Event Producer — Synthetic Data Generator
=============================================
Generates 3 realistic ad event streams into Kafka:

  Topic: ad-impressions   →  ad is shown to a user
  Topic: ad-clicks        →  user clicks the ad
  Topic: ad-conversions   →  user completes a purchase/signup

Fraud patterns injected:
  1. Click Flooding    — U99999 clicks 100s of times per minute
  2. Bot IP Traffic    — 10.0.0.x hits dozens of campaigns rapidly
  3. Orphan Conversion — conversion with no prior click (fake conversion)
  4. Instant Click     — click arrives < 200ms after impression (bot speed)

Run:
    pip install -r requirements.txt
    python producer/event_producer.py
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP      = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
CONFLUENT_API_KEY    = os.environ["CONFLUENT_API_KEY"]
CONFLUENT_API_SECRET = os.environ["CONFLUENT_API_SECRET"]
TOPIC_IMPRESSIONS    = "ad-impressions"
TOPIC_CLICKS         = "ad-clicks"
TOPIC_CONVERSIONS    = "ad-conversions"

EVENTS_PER_SECOND    = 200        # Total throughput across all topics
FRAUD_RATIO          = 0.20       # 20% of traffic is fraudulent

NUM_CAMPAIGNS        = 50
NUM_NORMAL_USERS     = 5_000
NUM_BOT_IPS          = 10         # Pool of bot IPs: 10.0.0.1 → 10.0.0.10

CLICK_THROUGH_RATE   = 0.03       # 3% of impressions lead to a click
CONVERSION_RATE      = 0.05       # 5% of clicks lead to a conversion

CONVERSION_TYPES     = ["purchase", "signup", "download", "lead_form"]
USER_AGENTS          = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X)",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7)",
]
BOT_USER_AGENTS      = [
    "Googlebot/2.1 (+http://www.google.com/bot.html)",
    "python-requests/2.28.0",
    "curl/7.85.0",
]
# ──────────────────────────────────────────────────────────────────────────────

fake = Faker()

# In-memory store: impression_id → impression event (for linking clicks)
#                  click_id      → click event     (for linking conversions)
recent_impressions: dict = {}
recent_clicks: dict      = {}


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=CONFLUENT_API_KEY,
        sasl_plain_password=CONFLUENT_API_SECRET,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Normal event factories ─────────────────────────────────────────────────────

def make_impression() -> dict:
    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  "impression",
        "user_id":     f"U{random.randint(1, NUM_NORMAL_USERS):05d}",
        "campaign_id": f"C{random.randint(1, NUM_CAMPAIGNS):03d}",
        "ip_address":  fake.ipv4_public(),
        "user_agent":  random.choice(USER_AGENTS),
        "geo_country": random.choices(
            ["US", "GB", "CA", "AU", "DE"],
            weights=[60, 10, 10, 10, 10]
        )[0],
        "timestamp":   now_iso(),
        "fraud_label": "normal",
    }


def make_click(impression: dict, delay_ms: int = None) -> dict:
    """Create a click linked to a prior impression."""
    return {
        "event_id":                str(uuid.uuid4()),
        "event_type":              "click",
        "impression_id":           impression["event_id"],   # ← ancestry chain
        "user_id":                 impression["user_id"],
        "campaign_id":             impression["campaign_id"],
        "ip_address":              impression["ip_address"],
        "user_agent":              impression["user_agent"],
        "time_since_impression_ms": delay_ms or random.randint(800, 30_000),
        "timestamp":               now_iso(),
        "fraud_label":             "normal",
    }


def make_conversion(click: dict) -> dict:
    """Create a conversion linked to a prior click."""
    return {
        "event_id":        str(uuid.uuid4()),
        "event_type":      "conversion",
        "click_id":        click["event_id"],                # ← ancestry chain
        "impression_id":   click["impression_id"],
        "user_id":         click["user_id"],
        "campaign_id":     click["campaign_id"],
        "conversion_type": random.choice(CONVERSION_TYPES),
        "revenue_usd":     round(random.uniform(5.0, 500.0), 2),
        "timestamp":       now_iso(),
        "fraud_label":     "normal",
    }


# ── Fraud event factories ──────────────────────────────────────────────────────

def make_click_flood_event() -> dict:
    """
    Fraud Rule 1 — Click Flooding.
    One user (U99999) generates hundreds of clicks with no linked impression.
    Detection: user click count > 50 in a 10-min window.
    """
    return {
        "event_id":                str(uuid.uuid4()),
        "event_type":              "click",
        "impression_id":           str(uuid.uuid4()),        # Random — no real impression
        "user_id":                 "U99999",                 # Always the same bad actor
        "campaign_id":             f"C{random.randint(1, NUM_CAMPAIGNS):03d}",
        "ip_address":              fake.ipv4_public(),
        "user_agent":              random.choice(USER_AGENTS),
        "time_since_impression_ms": random.randint(50, 300),
        "timestamp":               now_iso(),
        "fraud_label":             "click_flood",
    }


def make_bot_ip_event() -> dict:
    """
    Fraud Rule 2 — Bot IP Traffic.
    A small pool of IPs (10.0.0.x) hits many different campaigns rapidly.
    Detection: distinct campaign count per IP > 20 in a 10-min window.
    """
    bot_ip = f"10.0.0.{random.randint(1, NUM_BOT_IPS)}"
    return {
        "event_id":                str(uuid.uuid4()),
        "event_type":              "click",
        "impression_id":           str(uuid.uuid4()),
        "user_id":                 f"U{random.randint(1, NUM_NORMAL_USERS):05d}",
        "campaign_id":             f"C{random.randint(1, NUM_CAMPAIGNS):03d}",
        "ip_address":              bot_ip,
        "user_agent":              random.choice(BOT_USER_AGENTS),
        "time_since_impression_ms": random.randint(10, 100),
        "timestamp":               now_iso(),
        "fraud_label":             "bot_ip",
    }


def make_orphan_conversion() -> dict:
    """
    Fraud Rule 3 — Orphan Conversion.
    A conversion with a random click_id that was never seen upstream.
    Detection: stream-stream join — conversion with no matching click in window.
    """
    return {
        "event_id":        str(uuid.uuid4()),
        "event_type":      "conversion",
        "click_id":        str(uuid.uuid4()),                # Random — will never match
        "impression_id":   str(uuid.uuid4()),
        "user_id":         f"U{random.randint(1, NUM_NORMAL_USERS):05d}",
        "campaign_id":     f"C{random.randint(1, NUM_CAMPAIGNS):03d}",
        "conversion_type": random.choice(CONVERSION_TYPES),
        "revenue_usd":     round(random.uniform(100.0, 999.0), 2),  # Suspiciously high
        "timestamp":       now_iso(),
        "fraud_label":     "orphan_conversion",
    }


def make_instant_click(impression: dict) -> dict:
    """
    Fraud Rule 4 — Instant Click.
    Click arrives < 200ms after impression — humanly impossible.
    Detection: time_since_impression_ms < 200 at Silver layer.
    """
    click = make_click(impression, delay_ms=random.randint(10, 199))
    click["fraud_label"] = "instant_click"
    return click


# ── Main loop ──────────────────────────────────────────────────────────────────

def run(producer: KafkaProducer):
    total = 0
    batch_size = EVENTS_PER_SECOND

    print(f"\n[Producer] Streaming to Kafka at ~{EVENTS_PER_SECOND} events/sec")
    print(f"           Fraud ratio : {FRAUD_RATIO * 100:.0f}%")
    print(f"           Topics      : {TOPIC_IMPRESSIONS}, {TOPIC_CLICKS}, {TOPIC_CONVERSIONS}")
    print("           Ctrl+C to stop.\n")

    while True:
        batch_start = time.time()

        for _ in range(batch_size):
            is_fraud = random.random() < FRAUD_RATIO

            if is_fraud:
                # ── Inject a fraud event ──────────────────────────────────────
                fraud_type = random.choices(
                    ["click_flood", "bot_ip", "orphan_conversion"],
                    weights=[50, 30, 20],
                )[0]

                if fraud_type == "click_flood":
                    event = make_click_flood_event()
                    producer.send(TOPIC_CLICKS, value=event)

                elif fraud_type == "bot_ip":
                    event = make_bot_ip_event()
                    producer.send(TOPIC_CLICKS, value=event)

                else:  # orphan_conversion
                    event = make_orphan_conversion()
                    producer.send(TOPIC_CONVERSIONS, value=event)

            else:
                # ── Normal impression → optional click → optional conversion ──
                impression = make_impression()
                recent_impressions[impression["event_id"]] = impression
                producer.send(TOPIC_IMPRESSIONS, value=impression)

                # 3% of impressions generate a click
                if random.random() < CLICK_THROUGH_RATE:
                    # 5% chance of instant (fraudulent) click
                    if random.random() < 0.05:
                        click = make_instant_click(impression)
                    else:
                        click = make_click(impression)

                    recent_clicks[click["event_id"]] = click
                    producer.send(TOPIC_CLICKS, value=click)

                    # 5% of clicks generate a conversion
                    if random.random() < CONVERSION_RATE:
                        conversion = make_conversion(click)
                        producer.send(TOPIC_CONVERSIONS, value=conversion)

            total += 1

        # Keep memory bounded — drop events older than last 10K
        if len(recent_impressions) > 10_000:
            keys = list(recent_impressions.keys())[:5_000]
            for k in keys:
                del recent_impressions[k]
        if len(recent_clicks) > 10_000:
            keys = list(recent_clicks.keys())[:5_000]
            for k in keys:
                del recent_clicks[k]

        producer.flush()

        elapsed   = time.time() - batch_start
        sleep_for = max(0.0, 1.0 - elapsed)
        time.sleep(sleep_for)

        print(
            f"[Producer] {total:>10,} events sent  |  "
            f"impressions={len(recent_impressions):,}  "
            f"clicks={len(recent_clicks):,}  "
            f"batch_time={elapsed:.2f}s"
        )


def main():
    producer = make_producer()
    try:
        run(producer)
    except KeyboardInterrupt:
        print("\n[Producer] Shutting down.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
