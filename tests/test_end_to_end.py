import os
import time
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import httpx
import pytest

from database.repository import get_connection


pytestmark = [
    pytest.mark.integration,
    pytest.mark.e2e,
    pytest.mark.kafka,
    pytest.mark.postgres,
]

API_BASE_URL = os.getenv(
    "E2E_API_URL",
    "http://localhost:8000",
).rstrip("/")

PROCESSING_TIMEOUT = float(
    os.getenv("E2E_PROCESSING_TIMEOUT", "20.0")
)


def _cleanup_test_data(source_id: str, event_ids: list[UUID]) -> None:
    """Remove data created by an end-to-end test run."""
    conn = get_connection()

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM processed_events
                    WHERE event_id IN (%s, %s, %s)
                    """,
                    tuple(event_ids),
                )
                cur.execute(
                    """
                    DELETE FROM aggregates
                    WHERE source_id = %s
                    """,
                    (source_id,),
                )
    finally:
        conn.close()


def _wait_for_expected_aggregate(
    client: httpx.Client,
    *,
    source_id: str,
    metric_name: str,
    window_start: datetime,
    window_end: datetime,
    expected_count: int,
) -> dict:
    """Poll the public API until the Processor has persisted the aggregate."""
    deadline = time.monotonic() + PROCESSING_TIMEOUT
    last_response = None

    while time.monotonic() < deadline:
        response = client.get(
            "/aggregates",
            params={
                "source_id": source_id,
                "metric_name": metric_name,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
            },
        )
        assert response.status_code == 200, response.text

        aggregates = response.json()
        last_response = aggregates

        if aggregates and aggregates[0]["count"] == expected_count:
            return aggregates[0]

        time.sleep(0.25)

    pytest.fail(
        "Timed out waiting for the expected aggregate. "
        f"Last API response: {last_response}"
    )


def test_full_pipeline_http_kafka_processor_postgres_api():
    """
    Verify the complete data path using only the public HTTP API for input/output:

        HTTP API -> Kafka -> Processor -> PostgreSQL -> HTTP API

    The same event is submitted twice to verify end-to-end idempotence as well.
    """
    source_id = f"pytest-e2e-{uuid4()}"
    metric_name = "temperature"

    # All events belong to the same deterministic one-minute aggregation window.
    window_start = datetime.now(timezone.utc).replace(
        second=0,
        microsecond=0,
    )
    event_time = window_start + timedelta(seconds=10)
    window_end = window_start + timedelta(minutes=1)

    event_ids = [uuid4(), uuid4(), uuid4()]

    events = [
        {
            "event_id": str(event_ids[0]),
            "source_id": source_id,
            "event_time": event_time.isoformat(),
            "metric_name": metric_name,
            "value": 20.0,
            "schema_version": 1,
        },
        {
            "event_id": str(event_ids[1]),
            "source_id": source_id,
            "event_time": event_time.isoformat(),
            "metric_name": metric_name,
            "value": 24.0,
            "schema_version": 1,
        },
        {
            "event_id": str(event_ids[2]),
            "source_id": source_id,
            "event_time": event_time.isoformat(),
            "metric_name": metric_name,
            "value": 22.0,
            "schema_version": 1,
        },
    ]

    try:
        with httpx.Client(
            base_url=API_BASE_URL,
            timeout=10.0,
        ) as client:
            health_response = client.get("/health")
            assert health_response.status_code == 200
            assert health_response.json() == {"status": "ok"}

            # Submit two unique events, one duplicate, and one final unique event.
            # Because all events use the same source_id, Kafka preserves their order
            # within the same partition.
            submitted_events = [
                events[0],
                events[1],
                events[0],  # exact duplicate: same event_id
                events[2],
            ]

            for event in submitted_events:
                response = client.post("/events", json=event)

                # The current implementation returns 200. 202 is also accepted here
                # so the E2E test remains valid if the endpoint is later made
                # semantically explicit as "202 Accepted".
                assert response.status_code in {200, 202}, response.text
                assert response.json() == {
                    "event_id": event["event_id"],
                    "status": "accepted",
                }

            aggregate = _wait_for_expected_aggregate(
                client,
                source_id=source_id,
                metric_name=metric_name,
                window_start=window_start,
                window_end=window_end,
                expected_count=3,
            )

            # Four HTTP submissions produced only three logical effects because
            # one event_id was duplicated.
            assert aggregate["source_id"] == source_id
            assert aggregate["metric_name"] == metric_name
            assert aggregate["count"] == 3
            assert aggregate["minimum"] == pytest.approx(20.0)
            assert aggregate["maximum"] == pytest.approx(24.0)
            assert aggregate["average"] == pytest.approx(22.0)

    finally:
        _cleanup_test_data(source_id, event_ids)


def test_live_metrics_observe_both_processors_and_all_partitions():
    """Verify that the live deployment exposes the expected Processor group."""
    with httpx.Client(
        base_url=API_BASE_URL,
        timeout=20.0,
    ) as client:
        response = client.get("/metrics")

    assert response.status_code == 200, response.text

    metrics = response.json()

    # The provided Docker Compose deployment starts two Processor instances.
    assert metrics["current_workers"] == 2

    # telemetry-events is created with four partitions by kafka-init.
    assert set(metrics["partition_rates"].keys()) == {
        "0",
        "1",
        "2",
        "3",
    }

    assert metrics["arrival_rate"] >= 0
    assert metrics["consumer_lag"] >= 0
