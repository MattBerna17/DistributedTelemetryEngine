import json
import time
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from confluent_kafka import Producer

from database.repository import (
    get_aggregates_by_source_and_metric,
    get_connection,
)
from processor.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


pytestmark = [
    pytest.mark.integration,
    pytest.mark.kafka,
    pytest.mark.postgres,
]


def test_duplicate_kafka_event_has_single_logical_effect():
    event_id = uuid4()
    source_id = f"pytest-dedup-{uuid4()}"
    metric_name = "temperature"
    event = {
        "event_id": str(event_id),
        "source_id": source_id,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "metric_name": metric_name,
        "value": 23.5,
        "schema_version": 1,
    }
    payload = json.dumps(event)

    producer = Producer(
        {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
    )

    try:
        producer.produce(
            topic=KAFKA_TOPIC,
            key=source_id,
            value=payload,
        )
        producer.produce(
            topic=KAFKA_TOPIC,
            key=source_id,
            value=payload,
        )
        assert producer.flush(5.0) == 0

        deadline = time.monotonic() + 15.0
        aggregate = None

        while time.monotonic() < deadline:
            aggregates = get_aggregates_by_source_and_metric(
                source_id,
                metric_name,
            )
            if aggregates:
                aggregate = aggregates[0]
                break
            time.sleep(0.25)

        assert aggregate is not None, "Processor did not persist the test event"

        # Give the Processor enough time to consume the duplicate as well.
        time.sleep(1.0)
        aggregate = get_aggregates_by_source_and_metric(
            source_id,
            metric_name,
        )[0]

        assert aggregate["count"] == 1
        assert aggregate["minimum"] == 23.5
        assert aggregate["maximum"] == 23.5
        assert aggregate["average"] == 23.5

        # value_sum is stored in PostgreSQL but is not exposed by the
        # current repository response dictionaries.
        assert "value_sum" not in aggregate
    finally:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM processed_events WHERE event_id = %s",
                    (event_id,),
                )
                cur.execute(
                    "DELETE FROM aggregates WHERE source_id = %s",
                    (source_id,),
                )
