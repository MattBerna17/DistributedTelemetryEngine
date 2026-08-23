from confluent_kafka import Consumer

from processor.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_DLQ_TOPIC,
)


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "dlq-test-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([KAFKA_DLQ_TOPIC])

    print(f"Reading messages from '{KAFKA_DLQ_TOPIC}'...")

    try:
        empty_polls = 0

        while empty_polls < 5:
            message = consumer.poll(1.0)

            if message is None:
                empty_polls += 1
                continue

            if message.error():
                print(f"Kafka error: {message.error()}")
                continue

            empty_polls = 0

            print("\n--- DLQ MESSAGE ---")

            print(
                "Value:",
                message.value().decode("utf-8")
                if message.value()
                else None,
            )

            print(
                "Key:",
                message.key().decode("utf-8")
                if message.key()
                else None,
            )

            print("Headers:")

            if message.headers():
                for key, value in message.headers():
                    print(
                        f"  {key}: "
                        f"{value.decode('utf-8') if value else None}"
                    )

    finally:
        consumer.close()


if __name__ == "__main__":
    main()