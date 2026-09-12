import json
import time
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, Producer

from processor.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_DLQ_TOPIC,
    KAFKA_TOPIC,
)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.kafka,
]


def test_invalid_event_is_forwarded_to_dlq():
    marker = f"invalid-test-{uuid4()}"
    invalid_event = {
        "source_id": marker,
        "value": "not-a-number",
    }

    producer = Producer(
        {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
    )
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"dlq-pytest-{uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([KAFKA_DLQ_TOPIC])

    try:
        producer.produce(
            topic=KAFKA_TOPIC,
            key=marker,
            value=json.dumps(invalid_event),
        )
        assert producer.flush(5.0) == 0

        deadline = time.monotonic() + 15.0
        matching_message = None

        while time.monotonic() < deadline:
            message = consumer.poll(1.0)

            if message is None:
                continue
            if message.error():
                pytest.fail(f"Kafka consumer error: {message.error()}")

            value = json.loads(message.value().decode("utf-8"))
            if value.get("source_id") == marker:
                matching_message = message
                break

        assert matching_message is not None, "Invalid event was not found in DLQ"
        assert matching_message.key().decode("utf-8") == marker

        headers = {
            key: value.decode("utf-8") if value is not None else None
            for key, value in (matching_message.headers() or [])
        }
        assert headers["original-topic"] == KAFKA_TOPIC
        assert "original-partition" in headers
        assert "original-offset" in headers
    finally:
        consumer.close()
