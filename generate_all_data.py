"""
=============================================================================
SparkWars 4.0 – COMPLETE DATA SOURCE GENERATOR
=============================================================================
Generates realistic sample data for all 6 data sources:
  1. Smart Meter Readings      → Kafka-style JSON stream
  2. Substation Telemetry      → IoT JSON stream
  3. Billing Transactions      → SQL Server CDC-format JSON
  4. Maintenance Logs          → CSV batch files
  5. Transformer Master        → Parquet batch files
  6. Renewable Production      → ADLS JSON (Auto Loader landing zone)
=============================================================================
"""

import json
import csv
import random
import uuid
import os
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ── Seed for reproducibility ──
random.seed(42)

BASE = Path("data_sources")
REGIONS = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
STATES = {
    "NORTH": ["Delhi", "Haryana", "Punjab", "UP"],
    "SOUTH": ["TamilNadu", "Karnataka", "Kerala", "Telangana"],
    "EAST": ["WestBengal", "Odisha", "Bihar", "Jharkhand"],
    "WEST": ["Maharashtra", "Gujarat", "Rajasthan", "Goa"],
    "CENTRAL": ["MP", "Chhattisgarh", "Uttarakhand", "HP"],
}

# ── Shared reference IDs ──
METER_IDS = [f"SM-{region[:1]}-{str(i).zfill(5)}" for region in REGIONS for i in range(1, 201)]
SUBSTATION_IDS = [f"SUB-{region[:1]}-{str(i).zfill(3)}" for region in REGIONS for i in range(1, 51)]
TRANSFORMER_IDS = [f"TRF-{region[:1]}-{str(i).zfill(4)}" for region in REGIONS for i in range(1, 101)]
CUSTOMER_IDS = [f"CUST-{str(i).zfill(6)}" for i in range(1, 1001)]
RENEWABLE_PLANT_IDS = [f"REN-{t}-{str(i).zfill(3)}" for t in ["SOLAR", "WIND", "HYDRO"] for i in range(1, 31)]


def ts(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# =============================================================================
# 1. SMART METER READINGS – Kafka Streaming JSON
# =============================================================================
def generate_smart_meter_stream(num_records=500):
    """
    Simulates Kafka topic: energy.smart_meter.readings
    Each message = one JSON record (as Kafka value payload)
    """
    out_dir = BASE / "smart_meter_kafka"
    out_dir.mkdir(parents=True, exist_ok=True)

    records = []
    base_time = datetime(2025, 6, 15, 0, 0, 0)

    for i in range(num_records):
        region = random.choice(REGIONS)
        meter_id = random.choice([m for m in METER_IDS if m.startswith(f"SM-{region[0]}")])
        event_time = base_time + timedelta(seconds=random.randint(0, 86400))

        # Simulate anomalies: ~5% power theft (unusually low), ~3% spike
        consumption = round(random.uniform(0.5, 15.0), 3)
        is_anomaly = False
        if random.random() < 0.05:
            consumption = round(random.uniform(0.001, 0.05), 4)  # Suspected theft
            is_anomaly = True
        elif random.random() < 0.03:
            consumption = round(random.uniform(50.0, 120.0), 2)  # Spike
            is_anomaly = True

        record = {
            "event_id": str(uuid.uuid4()),
            "meter_id": meter_id,
            "customer_id": random.choice(CUSTOMER_IDS),
            "timestamp": ts(event_time),
            "reading_kwh": consumption,
            "voltage_v": round(random.uniform(210, 250), 1),
            "current_a": round(random.uniform(0.5, 30.0), 2),
            "power_factor": round(random.uniform(0.75, 1.0), 3),
            "frequency_hz": round(random.uniform(49.5, 50.5), 2),
            "meter_status": random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "TAMPERED", "DISCONNECTED"]),
            "region": region,
            "state": random.choice(STATES[region]),
            "grid_zone": f"ZONE-{random.randint(1, 10)}",
            "is_anomaly_flag": is_anomaly,
            "kafka_partition": random.randint(0, 7),
            "kafka_offset": 1000000 + i,
            "kafka_timestamp": ts(event_time + timedelta(milliseconds=random.randint(50, 500))),
        }
        records.append(record)

    # Write as line-delimited JSON (simulates Kafka topic dump)
    with open(out_dir / "smart_meter_stream.json", "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    # Also write sample single messages for unit testing
    with open(out_dir / "sample_message.json", "w") as f:
        json.dump(records[0], f, indent=2)

    print(f"✅ Smart Meter: {len(records)} records → {out_dir}")
    return records


# =============================================================================
# 2. SUBSTATION TELEMETRY – IoT JSON Streaming
# =============================================================================
def generate_substation_telemetry(num_records=300):
    """
    Simulates IoT Hub / Event Hub: energy.substation.telemetry
    Higher-frequency sensor data from substations
    """
    out_dir = BASE / "substation_iot"
    out_dir.mkdir(parents=True, exist_ok=True)

    records = []
    base_time = datetime(2025, 6, 15, 0, 0, 0)

    for i in range(num_records):
        region = random.choice(REGIONS)
        sub_id = random.choice([s for s in SUBSTATION_IDS if s.startswith(f"SUB-{region[0]}")])
        event_time = base_time + timedelta(seconds=random.randint(0, 86400))

        # Transformer temperature anomaly simulation
        ambient_temp = round(random.uniform(25, 45), 1)
        oil_temp = round(ambient_temp + random.uniform(10, 35), 1)
        if random.random() < 0.04:  # 4% overheating
            oil_temp = round(ambient_temp + random.uniform(60, 90), 1)

        record = {
            "telemetry_id": str(uuid.uuid4()),
            "substation_id": sub_id,
            "transformer_id": random.choice([t for t in TRANSFORMER_IDS if t.startswith(f"TRF-{region[0]}")]),
            "timestamp": ts(event_time),
            "load_mw": round(random.uniform(5, 150), 2),
            "capacity_mw": random.choice([50, 100, 150, 200, 250]),
            "load_percentage": round(random.uniform(20, 95), 1),
            "voltage_kv": round(random.uniform(10.5, 33.5), 2),
            "oil_temperature_c": oil_temp,
            "ambient_temperature_c": ambient_temp,
            "dissolved_gas_ppm": round(random.uniform(10, 500), 1),
            "humidity_percent": round(random.uniform(30, 90), 1),
            "breaker_status": random.choice(["CLOSED", "CLOSED", "CLOSED", "OPEN", "TRIPPED"]),
            "alarm_code": random.choice([None, None, None, None, "OVERLOAD", "OVERHEAT", "GAS_ALARM", "BREAKER_TRIP"]),
            "region": region,
            "state": random.choice(STATES[region]),
            "iot_device_id": f"IOT-{sub_id}-{random.randint(1,5)}",
            "iot_hub_enqueued_time": ts(event_time + timedelta(milliseconds=random.randint(100, 2000))),
            "sequence_number": 500000 + i,
        }
        records.append(record)

    with open(out_dir / "substation_telemetry_stream.json", "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    with open(out_dir / "sample_telemetry.json", "w") as f:
        json.dump(records[0], f, indent=2)

    print(f"✅ Substation Telemetry: {len(records)} records → {out_dir}")
    return records


# =============================================================================
# 3. BILLING TRANSACTIONS – SQL Server CDC Format
# =============================================================================
def generate_billing_cdc(num_records=400):
    """
    Simulates SQL Server CDC output: dbo_billing_transactions_CT
    __$operation: 1=delete, 2=insert, 3=update(before), 4=update(after)
    """
    out_dir = BASE / "billing_cdc"
    out_dir.mkdir(parents=True, exist_ok=True)

    records = []
    base_time = datetime(2025, 6, 1, 0, 0, 0)

    for i in range(num_records):
        region = random.choice(REGIONS)
        bill_date = base_time + timedelta(days=random.randint(0, 30))
        due_date = bill_date + timedelta(days=30)

        amount = round(random.uniform(500, 15000), 2)
        # Fraud simulation: ~4% have suspicious patterns
        is_fraud = False
        if random.random() < 0.04:
            amount = round(random.uniform(0.01, 50.0), 2)  # Abnormally low
            is_fraud = True

        operation = random.choices([2, 2, 2, 4, 1], weights=[60, 10, 10, 15, 5])[0]

        record = {
            "__$operation": operation,
            "__$start_lsn": f"0x{random.randint(10**10, 10**11):012X}",
            "__$seqval": f"0x{random.randint(10**10, 10**11):012X}",
            "__$update_mask": "0xFF" if operation == 4 else "0x00",
            "__$command_id": random.randint(1, 5),
            "transaction_id": f"TXN-{str(i+1).zfill(8)}",
            "bill_id": f"BILL-{str(random.randint(1,50000)).zfill(8)}",
            "customer_id": random.choice(CUSTOMER_IDS),
            "meter_id": random.choice(METER_IDS),
            "billing_period_start": bill_date.strftime("%Y-%m-%d"),
            "billing_period_end": (bill_date + timedelta(days=29)).strftime("%Y-%m-%d"),
            "units_consumed_kwh": round(random.uniform(50, 2000), 2),
            "rate_per_kwh": round(random.uniform(3.5, 9.0), 2),
            "base_amount": amount,
            "tax_amount": round(amount * 0.18, 2),
            "total_amount": round(amount * 1.18, 2),
            "payment_status": random.choice(["PAID", "PAID", "PENDING", "OVERDUE", "DISPUTED"]),
            "payment_method": random.choice(["UPI", "CARD", "NETBANKING", "CASH", "AUTO_DEBIT"]),
            "due_date": due_date.strftime("%Y-%m-%d"),
            "region": region,
            "state": random.choice(STATES[region]),
            "fraud_flag": is_fraud,
            "cdc_timestamp": ts(bill_date + timedelta(hours=random.randint(0, 23))),
        }
        records.append(record)

    with open(out_dir / "billing_cdc_feed.json", "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    # Also create a "before/after" CDC pair example
    update_before = records[0].copy()
    update_before["__$operation"] = 3
    update_before["payment_status"] = "PENDING"

    update_after = records[0].copy()
    update_after["__$operation"] = 4
    update_after["payment_status"] = "PAID"

    with open(out_dir / "cdc_update_pair_example.json", "w") as f:
        json.dump({"before_image": update_before, "after_image": update_after}, f, indent=2)

    print(f"✅ Billing CDC: {len(records)} records → {out_dir}")
    return records


# =============================================================================
# 4. MAINTENANCE LOGS – CSV Batch Files
# =============================================================================
def generate_maintenance_logs(num_records=200):
    """
    Simulates batch CSV files dropped into ADLS/S3 landing zone.
    Typically uploaded daily/weekly by field maintenance teams.
    """
    out_dir = BASE / "maintenance_csv"
    out_dir.mkdir(parents=True, exist_ok=True)

    failure_types = [
        "OIL_LEAK", "WINDING_FAILURE", "BUSHING_CRACK", "COOLING_FAN_FAIL",
        "OVERHEATING", "INSULATION_BREAKDOWN", "TAP_CHANGER_FAULT",
        "MOISTURE_INGRESS", "CORROSION", "LIGHTNING_DAMAGE"
    ]
    severity_levels = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    maintenance_types = ["PREVENTIVE", "CORRECTIVE", "EMERGENCY", "INSPECTION"]

    headers = [
        "log_id", "transformer_id", "substation_id", "region", "state",
        "maintenance_type", "failure_type", "severity", "reported_date",
        "scheduled_date", "completed_date", "technician_id", "technician_name",
        "hours_spent", "parts_replaced", "cost_inr", "downtime_hours",
        "root_cause", "resolution_notes", "follow_up_required", "next_inspection_date"
    ]

    # Generate 3 monthly batch files
    for month_offset in range(3):
        month_base = datetime(2025, 4 + month_offset, 1)
        month_name = month_base.strftime("%Y_%m")
        batch_records = []

        count = num_records // 3
        for i in range(count):
            region = random.choice(REGIONS)
            reported = month_base + timedelta(days=random.randint(0, 28))
            scheduled = reported + timedelta(days=random.randint(1, 14))
            completed = scheduled + timedelta(days=random.randint(0, 7)) if random.random() > 0.1 else None

            row = {
                "log_id": f"ML-{month_name}-{str(i+1).zfill(4)}",
                "transformer_id": random.choice([t for t in TRANSFORMER_IDS if t.startswith(f"TRF-{region[0]}")]),
                "substation_id": random.choice([s for s in SUBSTATION_IDS if s.startswith(f"SUB-{region[0]}")]),
                "region": region,
                "state": random.choice(STATES[region]),
                "maintenance_type": random.choice(maintenance_types),
                "failure_type": random.choice(failure_types) if random.random() > 0.3 else "NONE",
                "severity": random.choice(severity_levels),
                "reported_date": reported.strftime("%Y-%m-%d"),
                "scheduled_date": scheduled.strftime("%Y-%m-%d"),
                "completed_date": completed.strftime("%Y-%m-%d") if completed else "",
                "technician_id": f"TECH-{random.randint(1001, 1200)}",
                "technician_name": random.choice([
                    "Rajesh Kumar", "Priya Sharma", "Amit Patel", "Sanjay Verma",
                    "Deepak Singh", "Anil Gupta", "Suresh Reddy", "Mahesh Joshi"
                ]),
                "hours_spent": round(random.uniform(1, 48), 1) if completed else "",
                "parts_replaced": random.choice([
                    "BUSHING", "OIL_FILTER", "COOLING_FAN", "GASKET", "TAP_CHANGER",
                    "WINDING_COIL", "NONE", "NONE", "NONE"
                ]),
                "cost_inr": round(random.uniform(5000, 500000), 2) if completed else "",
                "downtime_hours": round(random.uniform(0, 72), 1),
                "root_cause": random.choice([
                    "Age deterioration", "Manufacturing defect", "Overloading",
                    "Weather damage", "Rodent damage", "Voltage surge",
                    "Insufficient maintenance", "Unknown"
                ]),
                "resolution_notes": random.choice([
                    "Replaced faulty component", "Performed oil filtration",
                    "Tightened connections", "Full overhaul completed",
                    "Temporary fix applied", "Awaiting parts", "Monitoring ongoing"
                ]),
                "follow_up_required": random.choice(["YES", "NO", "NO", "NO"]),
                "next_inspection_date": (scheduled + timedelta(days=90)).strftime("%Y-%m-%d"),
            }
            batch_records.append(row)

        filepath = out_dir / f"maintenance_logs_{month_name}.csv"
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(batch_records)

    print(f"✅ Maintenance CSV: 3 monthly files → {out_dir}")


# =============================================================================
# 5. TRANSFORMER MASTER – Parquet Batch Files
# =============================================================================
def generate_transformer_master():
    """
    Simulates master/dimension data stored as Parquet in ADLS.
    Used for SCD Type 2 slowly changing dimensions.
    """
    out_dir = BASE / "transformer_parquet"
    out_dir.mkdir(parents=True, exist_ok=True)

    manufacturers = ["ABB", "Siemens", "Schneider", "Crompton", "BHEL", "Toshiba", "Hitachi"]
    capacities = [25, 50, 63, 100, 160, 200, 250, 315, 500, 1000]
    cooling_types = ["ONAN", "ONAF", "OFAF", "OFWF"]
    voltage_classes = ["11kV", "22kV", "33kV", "66kV", "110kV", "220kV"]

    records = []
    for trf_id in TRANSFORMER_IDS:
        region = [r for r in REGIONS if r[0] == trf_id.split("-")[1]][0]
        install_date = datetime(2000, 1, 1) + timedelta(days=random.randint(0, 9000))
        age_years = (datetime(2025, 6, 1) - install_date).days / 365.25

        records.append({
            "transformer_id": trf_id,
            "substation_id": random.choice([s for s in SUBSTATION_IDS if s.startswith(f"SUB-{region[0]}")]),
            "region": region,
            "state": random.choice(STATES[region]),
            "district": f"District-{random.randint(1, 50)}",
            "latitude": round(random.uniform(8.0, 35.0), 6),
            "longitude": round(random.uniform(68.0, 97.0), 6),
            "manufacturer": random.choice(manufacturers),
            "model_number": f"MDL-{random.randint(1000, 9999)}",
            "capacity_kva": random.choice(capacities),
            "voltage_class": random.choice(voltage_classes),
            "cooling_type": random.choice(cooling_types),
            "installation_date": install_date.strftime("%Y-%m-%d"),
            "commissioning_date": (install_date + timedelta(days=random.randint(7, 60))).strftime("%Y-%m-%d"),
            "warranty_expiry": (install_date + timedelta(days=365*5)).strftime("%Y-%m-%d"),
            "age_years": round(age_years, 1),
            "expected_life_years": random.choice([25, 30, 35, 40]),
            "health_index_score": round(random.uniform(1.0, 10.0), 2),
            "last_maintenance_date": (datetime(2025, 6, 1) - timedelta(days=random.randint(10, 365))).strftime("%Y-%m-%d"),
            "maintenance_frequency_days": random.choice([90, 180, 365]),
            "total_failures": random.randint(0, 15),
            "status": random.choices(
                ["ACTIVE", "ACTIVE", "ACTIVE", "UNDER_MAINTENANCE", "DECOMMISSIONED"],
                weights=[70, 10, 5, 10, 5]
            )[0],
            "criticality_tier": random.choice(["TIER_1", "TIER_2", "TIER_3"]),
            "connected_feeders": random.randint(1, 8),
            "customers_served": random.randint(50, 5000),
            "record_effective_date": "2025-01-01",
            "record_end_date": "9999-12-31",
            "is_current": True,
            "scd_version": 1,
        })

    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, out_dir / "transformer_master.parquet")

    # Also write a "change" file (simulating updates for SCD2)
    changes = random.sample(records, 30)
    for c in changes:
        c["scd_version"] = 2
        c["record_effective_date"] = "2025-06-01"
        c["health_index_score"] = round(random.uniform(1.0, 10.0), 2)
        c["status"] = random.choice(["ACTIVE", "UNDER_MAINTENANCE"])
        c["last_maintenance_date"] = "2025-05-28"

    df_changes = pd.DataFrame(changes)
    table_changes = pa.Table.from_pandas(df_changes)
    pq.write_table(table_changes, out_dir / "transformer_master_delta_20250601.parquet")

    print(f"✅ Transformer Master: {len(records)} records + 30 changes → {out_dir}")


# =============================================================================
# 6. CUSTOMER MASTER – Parquet (for SCD Type 2)
# =============================================================================
def generate_customer_master():
    """
    Customer dimension for SCD Type 2 implementation.
    """
    out_dir = BASE / "transformer_parquet"  # same dir for simplicity

    tariff_categories = ["DOMESTIC", "COMMERCIAL", "INDUSTRIAL", "AGRICULTURAL", "INSTITUTIONAL"]
    connection_types = ["SINGLE_PHASE", "THREE_PHASE"]

    records = []
    for cust_id in CUSTOMER_IDS:
        region = random.choice(REGIONS)
        records.append({
            "customer_id": cust_id,
            "customer_name": f"Customer_{cust_id.split('-')[1]}",
            "meter_id": random.choice([m for m in METER_IDS if m.startswith(f"SM-{region[0]}")]),
            "transformer_id": random.choice([t for t in TRANSFORMER_IDS if t.startswith(f"TRF-{region[0]}")]),
            "region": region,
            "state": random.choice(STATES[region]),
            "district": f"District-{random.randint(1, 50)}",
            "address": f"{random.randint(1,999)}, Block-{random.choice('ABCDEFGH')}, Sector-{random.randint(1,50)}",
            "tariff_category": random.choice(tariff_categories),
            "connection_type": random.choice(connection_types),
            "sanctioned_load_kw": random.choice([1, 2, 3, 5, 10, 25, 50, 100]),
            "connection_date": (datetime(2010, 1, 1) + timedelta(days=random.randint(0, 5000))).strftime("%Y-%m-%d"),
            "account_status": random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "SUSPENDED", "CLOSED"]),
            "outstanding_balance": round(random.uniform(0, 50000), 2),
            "last_payment_date": (datetime(2025, 6, 1) - timedelta(days=random.randint(1, 120))).strftime("%Y-%m-%d"),
            "record_effective_date": "2025-01-01",
            "record_end_date": "9999-12-31",
            "is_current": True,
        })

    df = pd.DataFrame(records)
    pq.write_table(pa.Table.from_pandas(df), out_dir / "customer_master.parquet")
    print(f"✅ Customer Master: {len(records)} records → {out_dir}")


# =============================================================================
# 7. RENEWABLE PRODUCTION – ADLS JSON (Auto Loader)
# =============================================================================
def generate_renewable_production(num_files=5, records_per_file=60):
    """
    Simulates JSON files landing in ADLS for Auto Loader ingestion.
    Each file = one hour of production data from renewable plants.
    Schema evolution: later files include a new 'curtailment_mw' field.
    """
    out_dir = BASE / "renewable_autoloader"
    out_dir.mkdir(parents=True, exist_ok=True)

    plant_types = {"SOLAR": (6, 18), "WIND": (0, 23), "HYDRO": (0, 23)}
    base_time = datetime(2025, 6, 15, 6, 0, 0)

    for file_idx in range(num_files):
        hour_start = base_time + timedelta(hours=file_idx)
        records = []

        for i in range(records_per_file):
            plant_id = random.choice(RENEWABLE_PLANT_IDS)
            plant_type = plant_id.split("-")[1]
            region = random.choice(REGIONS)

            # Production depends on type and time
            hour = (hour_start + timedelta(minutes=random.randint(0, 59))).hour
            if plant_type == "SOLAR":
                base_production = max(0, 10 * (1 - abs(hour - 12) / 6)) * random.uniform(0.5, 1.2)
            elif plant_type == "WIND":
                base_production = random.uniform(2, 25)
            else:  # HYDRO
                base_production = random.uniform(5, 40)

            record = {
                "reading_id": str(uuid.uuid4()),
                "plant_id": plant_id,
                "plant_type": plant_type,
                "timestamp": ts(hour_start + timedelta(minutes=random.randint(0, 59))),
                "production_mw": round(base_production, 3),
                "capacity_mw": random.choice([10, 25, 50, 100]),
                "capacity_factor": round(random.uniform(0.15, 0.85), 3),
                "weather_condition": random.choice([
                    "CLEAR", "CLOUDY", "PARTLY_CLOUDY", "RAINY", "WINDY", "OVERCAST"
                ]),
                "irradiance_w_m2": round(random.uniform(100, 1000), 1) if plant_type == "SOLAR" else None,
                "wind_speed_mps": round(random.uniform(2, 25), 1) if plant_type == "WIND" else None,
                "water_flow_m3s": round(random.uniform(10, 500), 1) if plant_type == "HYDRO" else None,
                "grid_injection_mw": round(base_production * random.uniform(0.85, 0.98), 3),
                "auxiliary_consumption_mw": round(base_production * random.uniform(0.02, 0.08), 3),
                "region": region,
                "state": random.choice(STATES[region]),
                "inverter_status": random.choice(["ONLINE", "ONLINE", "ONLINE", "FAULT", "STANDBY"]) if plant_type == "SOLAR" else None,
            }

            # SCHEMA EVOLUTION: files 3+ include new field
            if file_idx >= 3:
                record["curtailment_mw"] = round(random.uniform(0, 2), 3) if random.random() < 0.2 else 0.0
                record["battery_storage_mwh"] = round(random.uniform(0, 10), 2)

            records.append(record)

        filename = f"renewable_production_{hour_start.strftime('%Y%m%d_%H%M')}.json"
        with open(out_dir / filename, "w") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")

    print(f"✅ Renewable Production: {num_files} files → {out_dir}")


# =============================================================================
# MAIN EXECUTION
# =============================================================================
if __name__ == "__main__":
    print("=" * 70)
    print("SparkWars 4.0 – Generating All Data Sources")
    print("=" * 70)

    generate_smart_meter_stream(500)
    generate_substation_telemetry(300)
    generate_billing_cdc(400)
    generate_maintenance_logs(200)
    generate_transformer_master()
    generate_customer_master()
    generate_renewable_production(5, 60)

    print("\n" + "=" * 70)
    print("✅ ALL DATA SOURCES GENERATED SUCCESSFULLY!")
    print("=" * 70)
