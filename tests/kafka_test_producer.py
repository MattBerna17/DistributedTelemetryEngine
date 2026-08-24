import json
import time
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer

from processor.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
)


def delivery_report(error, message):
    """
    Called by Kafka when message delivery succeeds or fails.
    """

    if error is not None:
        print(f"Message delivery failed: {error}")
        return

    print(
        "Message delivered: "
        f"topic={message.topic()}, "
        f"partition={message.partition()}, "
        f"offset={message.offset()}"
    )


def create_producer() -> Producer:
    """
    Creates a Kafka producer used only for manual testing.
    """

    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        }
    )


def main():
    print("\n===================================")
    print("KAFKA TEST PRODUCER")
    print("===================================")

    producer = create_producer()

    event_id = str(uuid4())

    event = {
        "event_id": event_id,
        "source_id": "test-sensor",
        "event_time": datetime.now(
            timezone.utc
        ).isoformat(),
        "metric_name": "temperature",
        "value": 23.5,
        "schema_version": 1,
    }

    payload = json.dumps(event)

    print("\nSending event:")
    print(payload)

    producer.produce(
        topic=KAFKA_TOPIC,

        # source_id is used as the Kafka key so that
        # events from the same source are assigned
        # consistently to the same partition.
        key=event["source_id"],

        value=payload,

        callback=delivery_report,
    )

    producer.flush()

    print("\nFirst event sent successfully.")

    # Send exactly the same event again to test deduplication.
    print("\nSending the same event again...")

    producer.produce(
        topic=KAFKA_TOPIC,
        key=event["source_id"],
        value=payload,
        callback=delivery_report,
    )

    producer.flush()

    print("\nDuplicate event sent.")

    print("\nEvent ID used for the test:")
    print(event_id)

    # Small delay only to make console output easier to read.
    time.sleep(1)


if __name__ == "__main__":
    main()