# This module implements the Kafka consumer used by the Processor.

from confluent_kafka import Consumer, KafkaError
from pydantic import ValidationError
from processor.dlq import send_to_dlq
from database.repository import process_event
from common.models import TelemetryEvent
from processor.config import (
    KAFKA_AUTO_OFFSET_RESET,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC,
)
from processor.windowing import get_window_start


def create_consumer() -> Consumer:
    """
    Creates and configures the Kafka consumer.
    """

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_CONSUMER_GROUP,
            "auto.offset.reset": KAFKA_AUTO_OFFSET_RESET,
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([KAFKA_TOPIC])

    return consumer


def process_message(message) -> bool:
    """
    Validates and processes a single Kafka message.
    """

    try:
        # message.value() contains the raw JSON as bytes.
        event = TelemetryEvent.model_validate_json(
            message.value()
        )

    except ValidationError as exc:
        print(
            f"Invalid event received "
            f"(partition={message.partition()}, "
            f"offset={message.offset()}): {exc}"
        )

        send_to_dlq(message)
        return True

    # Determine the 1-minute window containing the event.
    window_start = get_window_start(
        event.event_time
    )

    # Store the event processing result in PostgreSQL.
    is_new_event = process_event(
        event_id=event.event_id,
        source_id=event.source_id,
        metric_name=event.metric_name,
        window_start=window_start,
        value=event.value,
        kafka_partition=message.partition(),
        kafka_offset=message.offset(),
    )

    if is_new_event:
        print(
            f"Event processed: "
            f"event_id={event.event_id}, "
            f"source_id={event.source_id}, "
            f"metric={event.metric_name}, "
            f"partition={message.partition()}, "
            f"offset={message.offset()}"
        )
    else:
        print(
            f"Duplicate event ignored: "
            f"event_id={event.event_id}"
        )
    return True


def run_consumer() -> None:
    """
    Starts the Processor and continuously consumes
    messages from Kafka.
    """

    consumer = create_consumer()

    print(
        f"Processor started. Listening on topic "
        f"'{KAFKA_TOPIC}'..."
    )

    try:
        while True:

            # Wait up to one second for a Kafka message.
            message = consumer.poll(1.0)

            # No message currently available.
            if message is None:
                continue

            # Kafka returned an error instead of a normal message.
            if message.error():

                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                print(
                    f"Kafka consumer error: "
                    f"{message.error()}"
                )
                continue

            processed = process_message(message)

            if processed:
                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

    except KeyboardInterrupt:
        print("\nProcessor stopped by user.")

    finally:
        consumer.close()
        print("Kafka consumer closed.")


if __name__ == "__main__":
    run_consumer()