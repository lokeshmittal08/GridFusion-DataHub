"""
=============================================================================
SOURCE 1: SMART METER READINGS – Kafka Stream Simulator
=============================================================================
PURPOSE:  Simulates a Kafka producer pushing real-time smart meter data
TOPIC:    energy.smart_meter.readings
FORMAT:   JSON (each message = 1 meter reading)
FREQUENCY: ~1 message every 0.2-2 seconds

HOW TO USE IN DATABRICKS:
─────────────────────────
  # Read from Kafka
  df_smart_meter = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "<broker>:9092")
      .option("subscribe", "energy.smart_meter.readings")
      .option("startingOffsets", "latest")
      .option("maxOffsetsPerTrigger", 10000)
      .load()
      .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp")
      .select(from_json("json_value", smart_meter_schema).alias("data"), "kafka_timestamp")
      .select("data.*", "kafka_timestamp")
  )

SCHEMA FOR DATABRICKS:
─────────────────────
  from pyspark.sql.types import *
  smart_meter_schema = StructType([
      StructField("event_id", StringType()),
      StructField("meter_id", StringType()),
      StructField("customer_id", StringType()),
      StructField("timestamp", TimestampType()),
      StructField("reading_kwh", DoubleType()),
      StructField("voltage_v", DoubleType()),
      StructField("current_a", DoubleType()),
      StructField("power_factor", DoubleType()),
      StructField("frequency_hz", DoubleType()),
      StructField("meter_status", StringType()),
      StructField("region", StringType()),
      StructField("state", StringType()),
      StructField("grid_zone", StringType()),
      StructField("is_anomaly_flag", BooleanType()),
  ])
=============================================================================
"""

import json
import random
import uuid
import time
import sys
from datetime import datetime, timedelta

# ── Configuration ──
TOPIC = "energy.smart_meter.readings"
REGIONS = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
METER_IDS = [f"SM-{r[0]}-{str(i).zfill(5)}" for r in REGIONS for i in range(1, 201)]
CUSTOMER_IDS = [f"CUST-{str(i).zfill(6)}" for i in range(1, 1001)]


def generate_meter_reading():
    """Generate a single smart meter reading event."""
    region = random.choice(REGIONS)
    now = datetime.utcnow()

    consumption = round(random.uniform(0.5, 15.0), 3)
    status = "ACTIVE"

    # 5% anomaly: suspected theft
    if random.random() < 0.05:
        consumption = round(random.uniform(0.001, 0.05), 4)
        status = "TAMPERED"
    # 3% anomaly: spike
    elif random.random() < 0.03:
        consumption = round(random.uniform(50.0, 120.0), 2)

    return {
        "event_id": str(uuid.uuid4()),
        "meter_id": random.choice([m for m in METER_IDS if m[3] == region[0]]),
        "customer_id": random.choice(CUSTOMER_IDS),
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "reading_kwh": consumption,
        "voltage_v": round(random.uniform(210, 250), 1),
        "current_a": round(random.uniform(0.5, 30.0), 2),
        "power_factor": round(random.uniform(0.75, 1.0), 3),
        "frequency_hz": round(random.uniform(49.5, 50.5), 2),
        "meter_status": status,
        "region": region,
        "state": f"{region}_State",
        "grid_zone": f"ZONE-{random.randint(1, 10)}",
        "is_anomaly_flag": status == "TAMPERED" or consumption > 50,
    }


def simulate_kafka_producer(messages_per_second=2, duration_seconds=60):
    """
    Simulate Kafka producer output to stdout.
    Pipe this to `kafka-console-producer` or use as reference.
    """
    print(f"🔴 KAFKA PRODUCER SIMULATOR – Topic: {TOPIC}")
    print(f"   Rate: ~{messages_per_second} msg/sec | Duration: {duration_seconds}s")
    print("-" * 60)

    total = 0
    start = time.time()

    while (time.time() - start) < duration_seconds:
        msg = generate_meter_reading()
        # In real Kafka: producer.send(TOPIC, value=json.dumps(msg).encode('utf-8'))
        print(json.dumps(msg))
        total += 1
        time.sleep(1.0 / messages_per_second + random.uniform(-0.1, 0.3))

    print(f"\n✅ Produced {total} messages in {duration_seconds}s")


if __name__ == "__main__":
    duration = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    simulate_kafka_producer(messages_per_second=3, duration_seconds=duration)
