"""
(1) Sensor simulado → Kafka (topic: temperature-raw)
Publica leituras somente quando há variação significativa de temperatura.
"""

import json
import random
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from const import BOOTSTRAP_SERVERS, SENSOR_ID, SIGNIFICANT_VARIATION, TOPIC_RAW


def create_producer() -> KafkaProducer:
    for attempt in range(10):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except NoBrokersAvailable:
            print(f"Kafka não disponível, tentativa {attempt + 1}/10...")
            time.sleep(3)
    raise RuntimeError("Não foi possível conectar ao Kafka.")


def run():
    producer = create_producer()
    temperature = 22.0
    last_sent = temperature

    print(f"Sensor '{SENSOR_ID}' iniciado. Publicando em '{TOPIC_RAW}'")

    try:
        while True:
            temperature += random.gauss(0, 0.4)
            temperature = max(15.0, min(35.0, temperature))

            if abs(temperature - last_sent) >= SIGNIFICANT_VARIATION:
                event = {
                    "sensor_id": SENSOR_ID,
                    "temperature": round(temperature, 2),
                    "timestamp": int(time.time()),
                }
                producer.send(TOPIC_RAW, value=event)
                producer.flush()
                last_sent = temperature
                print(f"[SENSOR] Publicado: {event}")

            time.sleep(1)

    except KeyboardInterrupt:
        print("Sensor encerrado.")
    finally:
        producer.close()


if __name__ == "__main__":
    run()
