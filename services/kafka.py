import json
import logging

from confluent_kafka import Producer
from common.models import TelemetryEvent


logger = logging.getLogger(__name__)


class KafkaProducerService:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic

        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all", # wait for ack from all the replicas
            "retries": 5 # retry 5 times without blocking the API
        })

    def _delivery_report(self, err, msg):
        # async callback so the HTTP ingestion path does not have to wait for the kafka ack
        if err is not None:
            logger.error(
                "Kafka delivery failed: topic=%s partition=%s error=%s",
                msg.topic(),
                msg.partition(),
                err
            )
        else:
            logger.debug(
                "Kafka delivery successful: topic=%s partition=%s offset=%s",
                msg.topic(),
                msg.partition(),
                msg.offset()
            )

    def publish_event(self, event: TelemetryEvent) -> bool:
        try:
            payload = json.dumps(event.model_dump(mode="json"))

            self.producer.produce(
                topic=self.topic,
                key=str(event.source_id),
                value=payload,
                callback=self._delivery_report,
            )

            # serve eventual delivery callbacks without blocking until the message is acknowledged by Kafka
            self.producer.poll(0)

            return True

        except Exception as exc:
            logger.error("Kafka producer error: %s", exc)
            return False