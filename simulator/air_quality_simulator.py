import time
import json
import random
import os
from google.cloud import pubsub_v1
from dotenv import load_dotenv

# Load variabel dari file .env
load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "default-project")
TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID", "smart-city-aqi-topic")
SENSOR_COUNT = int(os.getenv("SENSOR_COUNT", 20))
STREAM_INTERVAL = float(os.getenv("STREAM_INTERVAL", 2.0))
ERROR_RATE = float(os.getenv("ERROR_RATE", 0.1))

# Pub/Sub Publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# List of unique Sensor IDs
sensor_ids = [f"SNS-{str(i).zfill(3)}" for i in range(1, SENSOR_COUNT + 1)]

def generate_sensor_data():
    """Menghasilkan satu data sensor IoT lingkungan secara acak."""
    sensor_id = random.choice(sensor_ids)
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

    # Generate sensor values within realistic thresholds
    pm25 = round(random.uniform(10.0, 150.0), 1)      # Particulate matter (ug/m3)
    co2 = round(random.uniform(350.0, 1000.0), 1)     # CO2 levels (ppm)
    temperature = round(random.uniform(24.0, 36.0), 1) # Air temperature (Celsius)
    humidity = round(random.uniform(40.0, 90.0), 1)    # Humidity (%)

    record = {
        "sensor_id": sensor_id,
        "timestamp": timestamp,
        "pm25_level": pm25,
        "co2_level": co2,
        "ambient_temperature": temperature,
        "humidity": humidity
    }

    # Error Injection (10% probability) - For Silver Layer
    if random.random() < ERROR_RATE:
        error_type = random.choice(["missing_field", "negative_value", "out_of_range"])
        if error_type == "missing_field":
            field_to_remove = random.choice(["pm25_level", "co2_level", "ambient_temperature", "humidity"])
            record[field_to_remove] = None
        elif error_type == "negative_value":
            record["pm25_level"] = -99.0
        elif error_type == "out_of_range":
            record["humidity"] = 250.0

    return record

if __name__ == "__main__":
    print(f"🚀 Memulai IoT Air Quality Simulator untuk {SENSOR_COUNT} stasiun...")
    print(f"📡 Mengirim data ke Topic: {topic_path} setiap {STREAM_INTERVAL} detik.")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    while True:
        try:
            data = generate_sensor_data()
            message_json = json.dumps(data)
            message_bytes = message_json.encode("utf-8")
            future = publisher.publish(topic_path, message_bytes)
            print(f" [PUBLISHED] ➔ {message_json}")
            time.sleep(STREAM_INTERVAL)
        except KeyboardInterrupt:
            print("\n🛑 Simulator dihentikan oleh pengguna.")
            break
        except Exception as e:
            print(f"⚠️ Gagal mengirim data: {e}")
            time.sleep(5)
