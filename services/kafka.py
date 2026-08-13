import json
from confluent_kafka import Producer
from common.models import TelemetryEvent


class KafkaProducerService:
    """
    Service to manage the Kafka producer instance
    """
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic # producer only produces for one topic
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})

    def publish_event(self, event: TelemetryEvent) -> None:
        payload = json.dumps(event.model_dump(mode="json"))
        self.producer.produce(topic=self.topic, key=str(event.source_id), value=payload)