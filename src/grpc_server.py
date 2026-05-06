import json
import sqlite3
import threading
import time
from concurrent import futures

import grpc
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

import temperature_pb2
import temperature_pb2_grpc

from const import BOOTSTRAP_SERVERS, DB_PATH, GRPC_PORT, TOPIC_PROCESSED


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS temperature_averages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            average_celsius REAL NOT NULL,
            timestamp INTEGER NOT NULL,
            sample_count INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_timestamp ON temperature_averages(timestamp)"
    )
    conn.commit()
    conn.close()


def kafka_consumer_loop():
    for attempt in range(10):
        try:
            consumer = KafkaConsumer(
                TOPIC_PROCESSED,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="grpc-server-group",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            break
        except NoBrokersAvailable:
            print(f"Kafka não disponível, tentativa {attempt + 1}/10...")
            time.sleep(3)
    else:
        raise RuntimeError("Não foi possível conectar ao Kafka.")

    print(f"[SERVER] Consumindo de '{TOPIC_PROCESSED}'")

    for message in consumer:
        event = message.value
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO temperature_averages (average_celsius, timestamp, sample_count) VALUES (?, ?, ?)",
            (event["average_celsius"], event["timestamp"], event["sample_count"]),
        )
        conn.commit()
        conn.close()
        print(
            f"[SERVER] Armazenado: avg={event['average_celsius']:.2f}°C "
            f"({event['sample_count']} amostras)"
        )


class TemperatureServiceServicer(temperature_pb2_grpc.TemperatureServiceServicer):

    def GetLatest(self, request, context):
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT average_celsius, timestamp, sample_count "
            "FROM temperature_averages ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
        conn.close()

        if row is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Nenhum dado disponível.")
            return temperature_pb2.TemperatureAverage()

        return temperature_pb2.TemperatureAverage(
            average_celsius=row[0],
            timestamp=row[1],
            sample_count=row[2],
        )

    def GetHistory(self, request, context):
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT average_celsius, timestamp, sample_count "
            "FROM temperature_averages "
            "WHERE timestamp BETWEEN ? AND ? ORDER BY timestamp",
            (request.start_timestamp, request.end_timestamp),
        ).fetchall()
        conn.close()

        for row in rows:
            yield temperature_pb2.TemperatureAverage(
                average_celsius=row[0],
                timestamp=row[1],
                sample_count=row[2],
            )


def serve():
    init_db()

    consumer_thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    consumer_thread.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    temperature_pb2_grpc.add_TemperatureServiceServicer_to_server(
        TemperatureServiceServicer(), server
    )
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    print(f"[SERVER] gRPC escutando na porta {GRPC_PORT}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)
        print("[SERVER] Encerrado.")


if __name__ == "__main__":
    serve()
