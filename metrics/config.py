import os

from dotenv import load_dotenv


load_dotenv()


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:19092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "telemetry-events",
)

KAFKA_CONSUMER_GROUP = os.getenv(
    "KAFKA_CONSUMER_GROUP",
    "adaptive-processors",
)

# By default, Kafka is observed every 5 seconds.
METRICS_SAMPLE_INTERVAL = float(
    os.getenv(
        "METRICS_SAMPLE_INTERVAL",
        "5.0",
    )
)

KAFKA_REQUEST_TIMEOUT = float(
    os.getenv(
        "KAFKA_REQUEST_TIMEOUT",
        "5.0",
    )
)

# Planner configuration

WORKER_CAPACITY = float(
    os.getenv("WORKER_CAPACITY", "100.0")
)

TARGET_UTILIZATION = float(
    os.getenv("TARGET_UTILIZATION", "0.8")
)