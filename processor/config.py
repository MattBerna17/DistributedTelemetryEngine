import os
from dotenv import load_dotenv

# This module centralizes the Kafka configuration parameters used by the Processor

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

KAFKA_AUTO_OFFSET_RESET = os.getenv(
    "KAFKA_AUTO_OFFSET_RESET",
    "earliest",
)

KAFKA_DLQ_TOPIC = os.getenv(
    "KAFKA_DLQ_TOPIC",
    "telemetry-events-dlq",
)