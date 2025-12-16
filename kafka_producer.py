"""
Kafka Producer – Sensor Data Simulator

This script simulates IoT sensors and sends one JSON message per second
to a Kafka topic named `sensor_data`.

The producer runs locally and connects to Kafka via localhost:9092.
The topic must already exist (auto topic creation is disabled on Kafka).
"""

import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "sensor_data"


def build_producer() -> KafkaProducer:
    """
    Build and configure a KafkaProducer instance.
    """
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=20,
    )


def generate_sensor_event(device_id: str) -> dict:
    """
    Generate a random IoT sensor event.
    """
    sensor_type = random.choice(["temperature", "humidity", "co2"])

    if sensor_type == "temperature":
        value, unit = round(random.uniform(18, 32), 2), "°C"
    elif sensor_type == "humidity":
        value, unit = round(random.uniform(20, 80), 2), "%"
    else:
        value, unit = round(random.uniform(350, 2000), 2), "ppm"

    return {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "device_id": device_id,
        "building": random.choice(["A", "B", "C"]),
        "floor": random.choice([0, 1, 2, 3, 4]),
        "type": sensor_type,
        "value": float(value),
        "unit": unit,
    }


def main() -> None:
    """
    Continuously send one message per second to Kafka.
    """
    producer = build_producer()
    devices = [f"sensor-{i:03d}" for i in range(1, 21)]

    print(f"Producing 1 message/second to Kafka topic '{TOPIC}'. Press Ctrl+C to stop.")

    try:
        while True:
            device_id = random.choice(devices)
            event = generate_sensor_event(device_id)
            producer.send(TOPIC, key=device_id, value=event)
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()
        print("Kafka producer stopped.")


if __name__ == "__main__":
    main()
