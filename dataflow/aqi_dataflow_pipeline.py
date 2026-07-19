import os
import json
from dotenv import load_dotenv
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

# ------------------- Load Environment Variables -------------------
load_dotenv()
PROJECT_ID = os.getenv("GCP_PROJECT")
PUBSUB_SUBSCRIPTION = os.getenv("PUBSUB_SUBSCRIPTION")
BRONZE_PATH = os.getenv("BRONZE_PATH")
SILVER_PATH = os.getenv("SILVER_PATH")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")
TEMP_LOCATION = os.getenv("TEMP_LOCATION")
STAGING_LOCATION = os.getenv("STAGING_LOCATION")
REGION = os.getenv("REGION")

# ------------------- Beam Pipeline Options -------------------
pipeline_options = PipelineOptions(
    project=PROJECT_ID,
    streaming=True,
    temp_location=TEMP_LOCATION,
    staging_location=STAGING_LOCATION,
    region=REGION,
)
pipeline_options.view_as(StandardOptions).streaming = True

# ------------------- Helper Functions -------------------
def parse_json(message):
    try:
        return json.loads(message)
    except Exception:
        return None

def is_valid_aqi(record):
    try:
        if record is None:
            return False
        sensor_id = record.get("sensor_id")
        pm25 = record.get("pm25_level")
        co2 = record.get("co2_level")
        humidity = record.get("humidity")

        # Validasi kualitas udara
        if not sensor_id or pm25 is None or co2 is None or humidity is None:
            return False
        if not (0 <= humidity <= 100) or pm25 < 0 or co2 < 0:
            return False
        return True
    except Exception:
        return False

def extract_for_aggregation(record):
    return (record["sensor_id"], record)

def aggregate_aqi_records(key_values):
    sensor_id, records = key_values
    count = len(records)
    
    avg_pm25 = sum(r["pm25_level"] for r in records) / count
    avg_co2 = sum(r["co2_level"] for r in records) / count
    avg_temp = sum(r["ambient_temperature"] for r in records) / count
    avg_humidity = sum(r["humidity"] for r in records) / count
    latest_timestamp = max(r["timestamp"] for r in records)

    return {
        "sensor_id": sensor_id,
        "timestamp": latest_timestamp,
        "pm25_level": float(round(avg_pm25, 2)),
        "co2_level": float(round(avg_co2, 2)),
        "ambient_temperature": float(round(avg_temp, 2)),
        "humidity": float(round(avg_humidity, 2))
    }

# ------------------- The Medallion Pipeline Core -------------------
with beam.Pipeline(options=pipeline_options) as p:

    # =================== BRONZE LAYER ===================
    # Membaca data streaming asli dari PubSub
    bronze_data = (
        p
        | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription=PUBSUB_SUBSCRIPTION)
        | "Decode to string" >> beam.Map(lambda x: x.decode("utf-8"))
        | "Window Bronze Data" >> beam.WindowInto(FixedWindows(60))  # Jendela 1 menit
    )

    bronze_data | "Write Bronze to GCS" >> beam.io.WriteToText(
        BRONZE_PATH + "raw_data",
        file_name_suffix=".json"
    )

    # =================== SILVER LAYER ===================
    # Proses pembersihan data dalam memori
    silver_data = (
        bronze_data
        | "Parse JSON" >> beam.Map(parse_json)
        | "Filter Invalid Records" >> beam.Filter(is_valid_aqi)
        | "Window Silver Data" >> beam.WindowInto(FixedWindows(60))
    )
    
    silver_data | "Format JSON to String" >> beam.Map(json.dumps) | "Write Silver to GCS" >> beam.io.WriteToText(
        SILVER_PATH + "cleaned_data",
        file_name_suffix=".json"
    )

    # =================== GOLD LAYER ===================
    # Proses perangkuman (Agregasi rata-rata per menit)
    gold_data = (
        silver_data
        | "Key by sensor_id" >> beam.Map(extract_for_aggregation)
        | "Group by sensor_id" >> beam.GroupByKey()
        | "Aggregate per sensor" >> beam.Map(aggregate_aqi_records)
    )

    gold_data | "Write Gold to BigQuery" >> beam.io.WriteToBigQuery(
        BIGQUERY_TABLE,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED 
    )