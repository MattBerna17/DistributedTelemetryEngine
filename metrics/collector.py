import asyncio

from confluent_kafka import (
    ConsumerGroupTopicPartitions,
    TopicPartition,
)
from confluent_kafka.admin import (
    AdminClient,
    OffsetSpec,
)

from common.models import SystemMetricsSnapshot
from metrics.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_REQUEST_TIMEOUT,
    KAFKA_TOPIC,
    METRICS_SAMPLE_INTERVAL,
)


class MetricsCollector:
    """
    Collects system metrics from Kafka without joining
    the Processor consumer group.
    """


    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
        consumer_group: str = KAFKA_CONSUMER_GROUP,
        sample_interval: float = METRICS_SAMPLE_INTERVAL,
    ):
        if sample_interval <= 0:
            raise ValueError("sample_interval must be greater than 0")

        self.topic = topic
        self.consumer_group = consumer_group
        self.sample_interval = sample_interval

        self.admin = AdminClient(
            {
                "bootstrap.servers": bootstrap_servers,
            }
        )

    def _get_partition_ids(self) -> list[int]:
        """
        Returns the partition IDs of the monitored topic.
        """

        metadata = self.admin.list_topics(
            topic=self.topic,
            timeout=KAFKA_REQUEST_TIMEOUT,
        )

        topic_metadata = metadata.topics.get(self.topic)

        if topic_metadata is None:
            raise RuntimeError(f"Topic '{self.topic}' was not found")

        if topic_metadata.error is not None:
            raise RuntimeError(
                f"Cannot read topic metadata: " f"{topic_metadata.error}"
            )

        return sorted(topic_metadata.partitions.keys())

    def _get_latest_offsets(
        self,
        partition_ids: list[int],
    ) -> dict[int, int]:
        """
        Returns the latest offset for every topic partition.
        """

        requests = {
            TopicPartition(
                self.topic,
                partition,
            ): OffsetSpec.latest()
            for partition in partition_ids
        }

        # Returns a structure which contains Futures.
        futures = self.admin.list_offsets(
            requests,
            request_timeout=KAFKA_REQUEST_TIMEOUT,
        )

        offsets = {}

        # Iterates on TopicPartition -> Future.
        for topic_partition, future in futures.items():
            result = future.result()  # Takes the result.

            offsets[topic_partition.partition] = result.offset

        return offsets

    # Kafka may delete old messages due to retention, so the earliest offset is not always 0.
    def _get_earliest_offsets(
        self,
        partition_ids: list[int],
    ) -> dict[int, int]:
        """
        Returns the earliest available offset for every partition.
        """

        requests = {
            TopicPartition(
                self.topic,
                partition,
            ): OffsetSpec.earliest()
            for partition in partition_ids
        }

        futures = self.admin.list_offsets(
            requests,
            request_timeout=KAFKA_REQUEST_TIMEOUT,
        )

        offsets = {}

        for topic_partition, future in futures.items():
            result = future.result()

            offsets[topic_partition.partition] = result.offset

        return offsets

    def _get_committed_offsets(
        self,
    ) -> dict[int, int]:
        """
        Returns the offsets committed by the Processor
        consumer group for the monitored topic.
        """

        # Request related to the ConsumerGroup.
        request = ConsumerGroupTopicPartitions(self.consumer_group)

        # Sends the request using AdminClient
        futures = self.admin.list_consumer_group_offsets(
            [request],
            request_timeout=KAFKA_REQUEST_TIMEOUT,
        )

        # Takes the result
        result = futures[self.consumer_group].result()

        offsets = {}

        # Iterates on topic partitions
        for topic_partition in (
            result.topic_partitions or []
        ):  # Use an empty list if no topic partitions are returned.
            if (
                topic_partition.topic == self.topic
            ):  # Filters only the topic of interest to the Collector.
                offsets[topic_partition.partition] = topic_partition.offset

        return offsets

    def _get_current_workers(self) -> int:
        """
        Returns the number of active members of the
        Processor Kafka consumer group.
        """

        futures = self.admin.describe_consumer_groups(
            [self.consumer_group],
            request_timeout=KAFKA_REQUEST_TIMEOUT,
        )

        description = futures[self.consumer_group].result()

        return len(description.members)

    @staticmethod
    def _calculate_partition_rates(
        start_offsets: dict[int, int],
        end_offsets: dict[int, int],
        interval: float,
    ) -> dict[int, float]:
        """
        Calculates the arrival rate of every partition.
        """

        rates = {}

        for partition, end_offset in end_offsets.items():
            start_offset = start_offsets.get(
                partition,
                end_offset,
            )

            new_messages = max(
                0,
                end_offset - start_offset,
            )

            # A partition rate is the number of events per second arriving at a Kafka partition.
            rates[partition] = new_messages / interval

        return rates

    @staticmethod
    def _calculate_consumer_lag(
        partition_ids: list[int],
        latest_offsets: dict[int, int],
        earliest_offsets: dict[int, int],
        committed_offsets: dict[int, int],
    ) -> int:
        """
        Calculates the total consumer lag of the
        Processor consumer group.
        """

        total_lag = 0

        for partition in partition_ids:

            latest = latest_offsets[
                partition
            ]  # Takes the latest offset for each partition.

            committed = committed_offsets.get(
                partition,
                -1,
            )

            # If the group has never committed an offset
            # for this partition, consider the earliest
            # available offset as the starting point.
            if committed < 0:
                committed = earliest_offsets[partition]

            partition_lag = max(
                0,
                latest - committed,
            )

            total_lag += partition_lag

        return total_lag

    async def collect(
        self,
    ) -> SystemMetricsSnapshot:
        """
        Collects a complete snapshot of the system metrics.
        """

        partition_ids = self._get_partition_ids()

        # First sample.
        start_offsets = self._get_latest_offsets(partition_ids)

        # Observe Kafka traffic for the configured interval.
        await asyncio.sleep(self.sample_interval)

        # Second sample.
        end_offsets = self._get_latest_offsets(partition_ids)

        partition_rates = self._calculate_partition_rates(
            start_offsets,
            end_offsets,
            self.sample_interval,
        )

        arrival_rate = sum(partition_rates.values())

        earliest_offsets = self._get_earliest_offsets(partition_ids)

        committed_offsets = self._get_committed_offsets()

        consumer_lag = self._calculate_consumer_lag(
            partition_ids,
            end_offsets,
            earliest_offsets,
            committed_offsets,
        )

        current_workers = self._get_current_workers()

        return SystemMetricsSnapshot(
            arrival_rate=arrival_rate,
            consumer_lag=consumer_lag,
            partition_rates=partition_rates,
            current_workers=current_workers,
        )
