import os

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = os.getenv("TOPIC_RAW", "temperature-raw")
TOPIC_PROCESSED = os.getenv("TOPIC_PROCESSED", "temperature-processed")


SENSOR_ID = os.getenv("SENSOR_ID", "sensor-01")
SIGNIFICANT_VARIATION = float(os.getenv("SIGNIFICANT_VARIATION", "0.5"))

WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", str(2 * 60 * 60)))

DB_PATH = os.getenv(
    "DB_PATH",
    os.path.join(os.path.dirname(__file__), "..", "temperature.db"),
)

GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))
GRPC_SERVER = os.getenv("GRPC_SERVER", f"localhost:{GRPC_PORT}")
DEADLINE_SECONDS = int(os.getenv("DEADLINE_SECONDS", "5"))
