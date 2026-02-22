# SparkWars 4.0 – Data Sources Reference Guide

## Quick Overview

| # | Data Source | Format | Ingestion Method | Simulated By | Databricks Connector |
|---|-------------|--------|------------------|--------------|---------------------|
| 1 | Smart Meter Readings | JSON (line-delimited) | **Kafka Streaming** | `kafka_producer_simulator.py` | `spark.readStream.format("kafka")` |
| 2 | Substation Telemetry | JSON (line-delimited) | **IoT Hub / Event Hub Streaming** | `iot_stream_simulator.py` | `spark.readStream.format("eventhubs")` |
| 3 | Billing Transactions | JSON (CDC format) | **SQL Server CDC** (via Debezium / Spark CDC) | `billing_cdc_feed.json` | `spark.readStream.format("cloudFiles")` with CDC parsing |
| 4 | Maintenance Logs | CSV | **Batch (ADLS landing zone)** | `maintenance_logs_YYYY_MM.csv` | `spark.read.format("csv")` or Auto Loader |
| 5 | Transformer Master | Parquet | **Batch (ADLS)** – SCD Type 2 | `transformer_master.parquet` | `spark.read.format("parquet")` |
| 6 | Customer Master | Parquet | **Batch (ADLS)** – SCD Type 2 | `customer_master.parquet` | `spark.read.format("parquet")` |
| 7 | Renewable Production | JSON (multi-file) | **Auto Loader (ADLS)** | `renewable_production_*.json` | `spark.readStream.format("cloudFiles")` |

---

## Directory Structure

```
data_sources/
├── smart_meter_kafka/           ← SOURCE 1: Kafka stream
│   ├── kafka_producer_simulator.py    (run to simulate live stream)
│   ├── smart_meter_stream.json        (500 pre-generated records)
│   └── sample_message.json            (single message example)
│
├── substation_iot/              ← SOURCE 2: IoT Hub stream
│   ├── iot_stream_simulator.py        (run to simulate live stream)
│   ├── substation_telemetry_stream.json  (300 pre-generated records)
│   └── sample_telemetry.json          (single message example)
│
├── billing_cdc/                 ← SOURCE 3: SQL Server CDC
│   ├── billing_cdc_feed.json          (400 CDC records with __$operation)
│   └── cdc_update_pair_example.json   (before/after image example)
│
├── maintenance_csv/             ← SOURCE 4: CSV Batch
│   ├── maintenance_logs_2025_04.csv   (monthly batch file)
│   ├── maintenance_logs_2025_05.csv
│   └── maintenance_logs_2025_06.csv
│
├── transformer_parquet/         ← SOURCE 5 & 6: Parquet Master Data
│   ├── transformer_master.parquet              (500 transformer records)
│   ├── transformer_master_delta_20250601.parquet (30 changed records for SCD2)
│   └── customer_master.parquet                 (1000 customer records)
│
└── renewable_autoloader/        ← SOURCE 7: Auto Loader JSON
    ├── renewable_production_20250615_0600.json
    ├── renewable_production_20250615_0700.json
    ├── renewable_production_20250615_0800.json  ← schema evolution starts here
    ├── renewable_production_20250615_0900.json     (new fields added)
    └── renewable_production_20250615_1000.json
```

---

## SOURCE 1: Smart Meter Readings (Kafka Streaming)

**What it simulates:** Real-time electricity consumption readings from 1,000 smart meters across 5 regions.

**Kafka Topic:** `energy.smart_meter.readings`

**Sample Message:**
```json
{
  "event_id": "a3f2e1d4-...",
  "meter_id": "SM-N-00042",
  "customer_id": "CUST-000123",
  "timestamp": "2025-06-15T14:23:45.123Z",
  "reading_kwh": 7.234,
  "voltage_v": 232.5,
  "current_a": 12.45,
  "power_factor": 0.923,
  "frequency_hz": 50.02,
  "meter_status": "ACTIVE",
  "region": "NORTH",
  "state": "Delhi",
  "grid_zone": "ZONE-3",
  "is_anomaly_flag": false
}
```

**Built-in Anomalies (for fraud/anomaly detection):**
- ~5% records: `reading_kwh` between 0.001–0.05 (suspected **power theft**)
- ~3% records: `reading_kwh` between 50–120 (suspected **spike/meter malfunction**)
- `meter_status` = "TAMPERED" / "DISCONNECTED" on anomalous readings

**Databricks Ingestion (Bronze):**
```python
df_raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", "energy.smart_meter.readings")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 10000)
    .load()
)

# Parse JSON + add watermark for deduplication
df_parsed = (df_raw
    .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
    .select(from_json("json_str", smart_meter_schema).alias("d"), "kafka_ts")
    .select("d.*", "kafka_ts")
    .withWatermark("timestamp", "10 minutes")
    .dropDuplicates(["event_id", "timestamp"])
)
```

**How to Run Simulator:**
```bash
python kafka_producer_simulator.py 30   # produces ~90 messages over 30 seconds
```

---

## SOURCE 2: Substation Telemetry (IoT Hub Streaming)

**What it simulates:** Sensor data from 250 substations – temperature, load, voltage, dissolved gas analysis (DGA).

**IoT Hub Endpoint / Event Hub Topic:** `energy.substation.telemetry`

**Sample Message:**
```json
{
  "telemetry_id": "b4d3c2a1-...",
  "substation_id": "SUB-S-023",
  "transformer_id": "TRF-S-0045",
  "timestamp": "2025-06-15T10:15:30.456Z",
  "load_mw": 87.34,
  "capacity_mw": 150,
  "load_percentage": 58.2,
  "voltage_kv": 22.45,
  "oil_temperature_c": 65.3,
  "ambient_temperature_c": 38.2,
  "dissolved_gas_ppm": 245.7,
  "humidity_percent": 67.4,
  "breaker_status": "CLOSED",
  "alarm_code": null,
  "region": "SOUTH",
  "state": "Karnataka",
  "iot_device_id": "IOT-SUB-S-023-2",
  "iot_hub_enqueued_time": "2025-06-15T10:15:31.100Z",
  "sequence_number": 678234
}
```

**Built-in Anomalies (for predictive maintenance):**
- ~4% records: `oil_temperature_c` is 60–90°C above ambient (transformer **overheating**)
- ~3% records: `alarm_code` values = OVERLOAD, GAS_ALARM, BREAKER_TRIP
- High `dissolved_gas_ppm` correlates with transformer degradation

**Databricks Ingestion (Bronze):**
```python
# Option A: From Event Hub
df_telemetry = (spark.readStream
    .format("eventhubs")
    .option("eventhubs.connectionString", encrypted_conn_str)
    .load()
    .selectExpr("CAST(body AS STRING)", "enqueuedTime")
    .select(from_json("body", telemetry_schema).alias("d"), "enqueuedTime")
    .select("d.*", "enqueuedTime")
    .withWatermark("timestamp", "5 minutes")
    .dropDuplicates(["telemetry_id"])
)

# Option B: From JSON files (local simulation)
df_telemetry = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/telemetry_schema")
    .load("abfss://landing@storage.dfs.core.windows.net/substation_iot/")
)
```

---

## SOURCE 3: Billing Transactions (SQL Server CDC)

**What it simulates:** Change Data Capture from the billing database. Includes INSERT, UPDATE, DELETE operations.

**CDC Source Table:** `dbo.billing_transactions`

**CDC Operation Codes:**
| Code | Operation |
|------|-----------|
| 1 | DELETE |
| 2 | INSERT |
| 3 | UPDATE (before image) |
| 4 | UPDATE (after image) |

**Sample CDC Record:**
```json
{
  "__$operation": 2,
  "__$start_lsn": "0x0A3B5C7D8E",
  "__$seqval": "0x0B4C6D9E0F",
  "__$update_mask": "0x00",
  "__$command_id": 1,
  "transaction_id": "TXN-00000042",
  "bill_id": "BILL-00012345",
  "customer_id": "CUST-000789",
  "meter_id": "SM-W-00134",
  "billing_period_start": "2025-06-01",
  "billing_period_end": "2025-06-29",
  "units_consumed_kwh": 456.78,
  "rate_per_kwh": 6.50,
  "base_amount": 2968.07,
  "tax_amount": 534.25,
  "total_amount": 3502.32,
  "payment_status": "PAID",
  "payment_method": "UPI",
  "due_date": "2025-07-01",
  "region": "WEST",
  "state": "Maharashtra",
  "fraud_flag": false,
  "cdc_timestamp": "2025-06-15T11:23:45.000Z"
}
```

**Built-in Anomalies (for billing fraud detection):**
- ~4% records: `base_amount` between ₹0.01–₹50 (suspected **billing fraud**)
- `fraud_flag: true` for these records
- Mismatched `units_consumed_kwh` vs `base_amount` ratios

**Databricks CDC Ingestion:**
```python
# Option A: Using Databricks CDC connector (Debezium format)
df_cdc = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/billing_cdc_schema")
    .load("abfss://landing@storage.dfs.core.windows.net/billing_cdc/")
)

# Parse CDC operations
df_changes = (df_cdc
    .filter(col("__$operation").isin(2, 4))  # INSERTs and UPDATE-after
    .withColumn("cdc_operation", 
        when(col("__$operation") == 2, "INSERT")
        .when(col("__$operation") == 4, "UPDATE")
        .when(col("__$operation") == 1, "DELETE"))
)

# Option B: Read from Delta table with CDF enabled
df_cdf = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("energy_raw.billing_transactions")
)
```

---

## SOURCE 4: Maintenance Logs (CSV Batch)

**What it simulates:** Monthly CSV exports from field maintenance management system.

**Landing Zone:** `abfss://landing@storage.dfs.core.windows.net/maintenance/`

**Files Generated:**
- `maintenance_logs_2025_04.csv` (April batch)
- `maintenance_logs_2025_05.csv` (May batch)
- `maintenance_logs_2025_06.csv` (June batch)

**Sample Row:**
```
log_id,transformer_id,substation_id,region,state,maintenance_type,failure_type,severity,...
ML-2025_04-0001,TRF-N-0034,SUB-N-012,NORTH,Delhi,CORRECTIVE,OIL_LEAK,HIGH,2025-04-05,...
```

**Columns (21 fields):**
`log_id` | `transformer_id` | `substation_id` | `region` | `state` | `maintenance_type` (PREVENTIVE/CORRECTIVE/EMERGENCY/INSPECTION) | `failure_type` | `severity` (LOW/MEDIUM/HIGH/CRITICAL) | `reported_date` | `scheduled_date` | `completed_date` | `technician_id` | `technician_name` | `hours_spent` | `parts_replaced` | `cost_inr` | `downtime_hours` | `root_cause` | `resolution_notes` | `follow_up_required` | `next_inspection_date`

**Databricks Ingestion (Bronze):**
```python
# Batch read
df_maintenance = (spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load("abfss://landing@storage.dfs.core.windows.net/maintenance/*.csv")
    .withColumn("_source_file", input_file_name())
    .withColumn("_ingestion_ts", current_timestamp())
)

# OR Auto Loader for incremental
df_maintenance = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/checkpoints/maintenance_schema")
    .option("header", True)
    .load("abfss://landing@storage.dfs.core.windows.net/maintenance/")
)
```

---

## SOURCE 5: Transformer Master (Parquet – SCD Type 2)

**What it simulates:** Master dimension table for 500 transformers. Includes a delta file with 30 changed records for SCD Type 2 processing.

**Files:**
- `transformer_master.parquet` — Full snapshot (500 records, version 1)
- `transformer_master_delta_20250601.parquet` — 30 updated records (version 2)

**Key Fields for SCD Type 2:**
```
transformer_id        – Business key (natural key)
health_index_score    – Changes over time
status                – Changes (ACTIVE → UNDER_MAINTENANCE → DECOMMISSIONED)
last_maintenance_date – Changes
record_effective_date – SCD2 effective date
record_end_date       – SCD2 end date ("9999-12-31" = current)
is_current            – True/False
scd_version           – Version counter
```

**Databricks SCD2 Implementation:**
```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "/mnt/silver/dim_transformer")
source = spark.read.parquet("/mnt/landing/transformer_master_delta_*.parquet")

# MERGE for SCD Type 2
(target.alias("t")
    .merge(source.alias("s"), "t.transformer_id = s.transformer_id AND t.is_current = true")
    .whenMatchedUpdate(
        condition="t.health_index_score <> s.health_index_score OR t.status <> s.status",
        set={
            "record_end_date": "s.record_effective_date",
            "is_current": lit(False)
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)
```

---

## SOURCE 6: Customer Master (Parquet – SCD Type 2)

**What it simulates:** 1,000 customer records linked to meters and transformers.

**Key Fields:** `customer_id` | `customer_name` | `meter_id` | `transformer_id` | `region` | `state` | `tariff_category` | `connection_type` | `sanctioned_load_kw` | `account_status` | `outstanding_balance` | `record_effective_date` | `record_end_date` | `is_current`

---

## SOURCE 7: Renewable Production (Auto Loader with Schema Evolution)

**What it simulates:** Hourly JSON files from solar, wind, and hydro plants. Files 1–2 have the base schema; files 3–5 include **new fields** (`curtailment_mw`, `battery_storage_mwh`) to test schema evolution.

**Landing Zone:** `abfss://landing@storage.dfs.core.windows.net/renewable/`

**Sample Record (base schema — files 1–2):**
```json
{
  "reading_id": "c5e4d3b2-...",
  "plant_id": "REN-SOLAR-015",
  "plant_type": "SOLAR",
  "timestamp": "2025-06-15T08:34:12.789Z",
  "production_mw": 6.234,
  "capacity_mw": 25,
  "capacity_factor": 0.412,
  "weather_condition": "CLEAR",
  "irradiance_w_m2": 745.3,
  "wind_speed_mps": null,
  "water_flow_m3s": null,
  "grid_injection_mw": 5.892,
  "auxiliary_consumption_mw": 0.342,
  "region": "SOUTH",
  "state": "Karnataka",
  "inverter_status": "ONLINE"
}
```

**Evolved Schema (files 3–5 add):**
```json
{
  "...all base fields...",
  "curtailment_mw": 0.45,
  "battery_storage_mwh": 3.78
}
```

**Databricks Auto Loader with Schema Evolution:**
```python
df_renewable = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", True)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaLocation", "/checkpoints/renewable_schema")
    .option("cloudFiles.schemaHints", 
            "production_mw double, curtailment_mw double, battery_storage_mwh double")
    .load("abfss://landing@storage.dfs.core.windows.net/renewable/")
    .withColumn("_source_file", input_file_name())
    .withColumn("_ingestion_ts", current_timestamp())
)
```

---

## Cross-Reference Keys (for joins & referential integrity)

```
CUSTOMER_IDS:     CUST-000001 to CUST-001000   (1,000 customers)
METER_IDS:        SM-{R}-00001 to SM-{R}-00200  (200 per region × 5 = 1,000 meters)
SUBSTATION_IDS:   SUB-{R}-001 to SUB-{R}-050    (50 per region × 5 = 250 substations)
TRANSFORMER_IDS:  TRF-{R}-0001 to TRF-{R}-0100  (100 per region × 5 = 500 transformers)
PLANT_IDS:        REN-{TYPE}-001 to REN-{TYPE}-030  (30 per type × 3 = 90 plants)

Where {R} = N(NORTH), S(SOUTH), E(EAST), W(WEST), C(CENTRAL)
Where {TYPE} = SOLAR, WIND, HYDRO
```

**Join Relationships:**
```
smart_meter.meter_id      → customer_master.meter_id
smart_meter.customer_id   → customer_master.customer_id
billing.customer_id       → customer_master.customer_id
billing.meter_id          → smart_meter.meter_id
telemetry.substation_id   → transformer_master.substation_id
telemetry.transformer_id  → transformer_master.transformer_id
maintenance.transformer_id→ transformer_master.transformer_id
customer.transformer_id   → transformer_master.transformer_id
```

---

## Running the Generator

```bash
# Generate all static data files
python generate_all_data.py

# Simulate live Kafka stream (smart meters)
python data_sources/smart_meter_kafka/kafka_producer_simulator.py 60

# Simulate live IoT stream (substations)
python data_sources/substation_iot/iot_stream_simulator.py 60
```
