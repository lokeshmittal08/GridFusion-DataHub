"""
=============================================================================
SOURCE 2: SUBSTATION TELEMETRY – IoT Hub Stream Simulator
=============================================================================
PURPOSE:  Simulates IoT sensor data from substations/transformers
SOURCE:   Azure IoT Hub / AWS IoT Core / Event Hub
FORMAT:   JSON (each message = 1 sensor reading)
FREQUENCY: ~1 message every 1-5 seconds per substation

HOW TO USE IN DATABRICKS:
─────────────────────────
  # Read from Event Hub (IoT Hub compatible endpoint)
  conn_str = "Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
  
  df_telemetry = (spark.readStream
      .format("eventhubs")
      .option("eventhubs.connectionString", sc._jvm.org.apache.spark.eventhubs
             .EventHubsUtils.encrypt(conn_str))
      .option("eventhubs.consumerGroup", "databricks-consumer")
      .option("eventhubs.startingPosition", 
              json.dumps({"offset": "-1", "enqueuedTime": None, "isInclusive": True}))
      .load()
      .selectExpr("CAST(body AS STRING) as json_body", "enqueuedTime", "sequenceNumber")
      .select(from_json("json_body", telemetry_schema).alias("data"), "enqueuedTime")
      .select("data.*", "enqueuedTime")
  )

  # OR read from JSON files (for local simulation):
  df_telemetry = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "/checkpoints/telemetry_schema")
      .load("/mnt/landing/substation_iot/")
  )

SCHEMA FOR DATABRICKS:
─────────────────────
  telemetry_schema = StructType([
      StructField("telemetry_id", StringType()),
      StructField("substation_id", StringType()),
      StructField("transformer_id", StringType()),
      StructField("timestamp", TimestampType()),
      StructField("load_mw", DoubleType()),
      StructField("capacity_mw", IntegerType()),
      StructField("load_percentage", DoubleType()),
      StructField("voltage_kv", DoubleType()),
      StructField("oil_temperature_c", DoubleType()),
      StructField("ambient_temperature_c", DoubleType()),
      StructField("dissolved_gas_ppm", DoubleType()),
      StructField("humidity_percent", DoubleType()),
      StructField("breaker_status", StringType()),
      StructField("alarm_code", StringType()),
      StructField("region", StringType()),
      StructField("state", StringType()),
      StructField("iot_device_id", StringType()),
      StructField("iot_hub_enqueued_time", TimestampType()),
      StructField("sequence_number", LongType()),
  ])
=============================================================================
"""

import json
import random
import uuid
import time
import sys
from datetime import datetime


REGIONS = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
SUBSTATION_IDS = [f"SUB-{r[0]}-{str(i).zfill(3)}" for r in REGIONS for i in range(1, 51)]
TRANSFORMER_IDS = [f"TRF-{r[0]}-{str(i).zfill(4)}" for r in REGIONS for i in range(1, 101)]


def generate_telemetry():
    """Generate a single substation telemetry reading."""
    region = random.choice(REGIONS)
    now = datetime.utcnow()
    sub_id = random.choice([s for s in SUBSTATION_IDS if s[4] == region[0]])

    ambient = round(random.uniform(25, 45), 1)
    oil_temp = round(ambient + random.uniform(10, 35), 1)

    # 4% chance of overheating anomaly
    alarm = None
    if random.random() < 0.04:
        oil_temp = round(ambient + random.uniform(60, 90), 1)
        alarm = "OVERHEAT"
    elif random.random() < 0.03:
        alarm = random.choice(["OVERLOAD", "GAS_ALARM", "BREAKER_TRIP"])

    load = round(random.uniform(5, 150), 2)
    capacity = random.choice([50, 100, 150, 200, 250])

    return {
        "telemetry_id": str(uuid.uuid4()),
        "substation_id": sub_id,
        "transformer_id": random.choice([t for t in TRANSFORMER_IDS if t[4] == region[0]]),
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "load_mw": load,
        "capacity_mw": capacity,
        "load_percentage": round((load / capacity) * 100, 1),
        "voltage_kv": round(random.uniform(10.5, 33.5), 2),
        "oil_temperature_c": oil_temp,
        "ambient_temperature_c": ambient,
        "dissolved_gas_ppm": round(random.uniform(10, 500), 1),
        "humidity_percent": round(random.uniform(30, 90), 1),
        "breaker_status": "TRIPPED" if alarm == "BREAKER_TRIP" else random.choice(["CLOSED", "CLOSED", "OPEN"]),
        "alarm_code": alarm,
        "region": region,
        "state": f"{region}_State",
        "iot_device_id": f"IOT-{sub_id}-{random.randint(1,5)}",
        "iot_hub_enqueued_time": now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "sequence_number": random.randint(500000, 999999),
    }


def simulate_iot_stream(messages_per_second=1, duration_seconds=30):
    """Simulate IoT Hub telemetry stream."""
    print(f"📡 IOT TELEMETRY SIMULATOR – Substation Sensors")
    print(f"   Rate: ~{messages_per_second} msg/sec | Duration: {duration_seconds}s")
    print("-" * 60)

    total = 0
    start = time.time()

    while (time.time() - start) < duration_seconds:
        msg = generate_telemetry()
        print(json.dumps(msg))
        total += 1
        time.sleep(1.0 / messages_per_second + random.uniform(0, 0.5))

    print(f"\n✅ Produced {total} telemetry messages in {duration_seconds}s")


if __name__ == "__main__":
    duration = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    simulate_iot_stream(messages_per_second=2, duration_seconds=duration)
