# This module manages invalid Kafka messages by sending them to the DLQ.

from confluent_kafka import Producer
from processor.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_DLQ_TOPIC,
)


def create_dlq_producer() -> Producer:
    """
    Creates the Kafka producer used to publish invalid messages to the Dead Letter Queue
    """
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        }
    )


# reuse the same producer for all DLQ messages
_dlq_producer = create_dlq_producer()


def send_to_dlq(message) -> None:
    """
    Sends an invalid Kafka message to the Dead Letter Queue

    The original message value and key are preserved. information about the original topic, partition and offset is stored in Kafka headers

    Raises:
        RuntimeError: if the message cannot be delivered to the DLQ.
    """
    delivery_error = None
    def delivery_report(error, delivered_message):
        nonlocal delivery_error
        if error is not None:
            delivery_error = error
            return
        print(
            f"Invalid event sent to DLQ: "
            f"topic={delivered_message.topic()}, "
            f"partition={delivered_message.partition()}, "
            f"offset={delivered_message.offset()}"
        )

    _dlq_producer.produce(
        topic=KAFKA_DLQ_TOPIC,
        # preserve the original Kafka key
        key=message.key(),
        # preserve the original raw payload
        value=message.value(),

        # store information about where the invalid message originally came from
        headers=[
            (
                "original-topic",
                message.topic().encode("utf-8"),
            ),
            (
                "original-partition",
                str(message.partition()).encode("utf-8"),
            ),
            (
                "original-offset",
                str(message.offset()).encode("utf-8"),
            ),
        ],
        callback=delivery_report,
    )

    # wait until Kafka has finished attempting delivery
    remaining_messages = _dlq_producer.flush()

    if remaining_messages > 0:
        raise RuntimeError("Some messages could not be delivered to the DLQ.")
    if delivery_error is not None:
        raise RuntimeError(f"Failed to deliver message to DLQ: {delivery_error}")