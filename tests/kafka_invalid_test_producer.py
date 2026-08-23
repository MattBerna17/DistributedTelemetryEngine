import json

from confluent_kafka import Producer

from processor.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
)


def main():
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        }
    )

    invalid_event = {
        "source_id": "invalid-test-sensor",
        "value": "not-a-number",
    }

    print("Sending invalid event:")
    print(json.dumps(invalid_event))

    producer.produce(
        topic=KAFKA_TOPIC,
        key="invalid-test-sensor",
        value=json.dumps(invalid_event),
    )

    producer.flush()

    print("Invalid event sent.")


if __name__ == "__main__":
    main()