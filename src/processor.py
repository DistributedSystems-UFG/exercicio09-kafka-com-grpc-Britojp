"""
(2) Processador: consumidor/produtor Kafka
  - Consome de 'temperature-raw'
  - Mantém janela deslizante de 2 horas
  - Calcula temperatura média da janela
  - Publica resultado em 'temperature-processed'
"""

import json
import time
from collections import deque

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from const import BOOTSTRAP_SERVERS, TOPIC_PROCESSED, TOPIC_RAW, WINDOW_SECONDS


def create_consumer() -> KafkaConsumer:
    for attempt in range(10):
        try:
            return KafkaConsumer(
                TOPIC_RAW,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="processor-group",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
        except NoBrokersAvailable:
            print(f"Kafka não disponível, tentativa {attempt + 1}/10...")
            time.sleep(3)
    raise RuntimeError("Não foi possível conectar ao Kafka.")


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def run():
    consumer = create_consumer()
    producer = create_producer()
    # janela: deque de (timestamp, temperature)
    window: deque = deque()

    print(f"Processador iniciado. '{TOPIC_RAW}' → '{TOPIC_PROCESSED}'")

    try:
        for message in consumer:
            event = message.value
            now = int(time.time())
            ts = event.get("timestamp", now)
            temp = float(event["temperature"])

            window.append((ts, temp))

            # Remove leituras fora da janela de 2 horas
            cutoff = now - WINDOW_SECONDS
            while window and window[0][0] < cutoff:
                window.popleft()

            avg = sum(t for _, t in window) / len(window)

            result = {
                "average_celsius": round(avg, 2),
                "timestamp": now,
                "sample_count": len(window),
            }

            producer.send(TOPIC_PROCESSED, value=result)
            producer.flush()
            print(
                f"[PROCESSOR] avg={result['average_celsius']:.2f}°C "
                f"({result['sample_count']} amostras na janela)"
            )

    except KeyboardInterrupt:
        print("Processador encerrado.")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    run()
