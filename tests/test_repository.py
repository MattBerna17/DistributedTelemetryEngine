from datetime import datetime, timezone
from uuid import UUID

import pytest

from database.repository import (
    check_database_connection,
    get_aggregates_by_source,
    get_aggregates_by_source_and_metric,
    get_connection,
    process_event,
)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
]

SOURCE_ID = "pytest-repository-sensor"
TEMPERATURE = "temperature"
HUMIDITY = "humidity"
WINDOW_START = datetime(2026, 8, 16, 10, 15, tzinfo=timezone.utc)
EVENT_ID_1 = UUID("11111111-1111-1111-1111-111111111111")
EVENT_ID_2 = UUID("22222222-2222-2222-2222-222222222222")
EVENT_ID_3 = UUID("33333333-3333-3333-3333-333333333333")


def cleanup_test_data():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM processed_events WHERE event_id IN (%s, %s, %s)",
                (EVENT_ID_1, EVENT_ID_2, EVENT_ID_3),
            )
            cur.execute(
                "DELETE FROM aggregates WHERE source_id = %s",
                (SOURCE_ID,),
            )


@pytest.fixture(autouse=True)
def clean_repository_test_data():
    cleanup_test_data()
    yield
    cleanup_test_data()


def test_database_connection():
    assert check_database_connection() is True


def test_process_new_event_creates_aggregate():
    result = process_event(
        event_id=EVENT_ID_1,
        source_id=SOURCE_ID,
        metric_name=TEMPERATURE,
        window_start=WINDOW_START,
        value=20.0,
        kafka_partition=0,
        kafka_offset=100,
    )

    assert result is True

    aggregates = get_aggregates_by_source_and_metric(
        SOURCE_ID,
        TEMPERATURE,
    )

    assert len(aggregates) == 1
    aggregate = aggregates[0]
    assert aggregate["count"] == 1
    assert aggregate["minimum"] == 20.0
    assert aggregate["maximum"] == 20.0
    assert aggregate["average"] == 20.0

    # The current repository API intentionally does not expose value_sum.
    assert "value_sum" not in aggregate


def test_duplicate_event_does_not_change_aggregate():
    process_event(
        event_id=EVENT_ID_1,
        source_id=SOURCE_ID,
        metric_name=TEMPERATURE,
        window_start=WINDOW_START,
        value=20.0,
        kafka_partition=0,
        kafka_offset=100,
    )

    duplicate_result = process_event(
        event_id=EVENT_ID_1,
        source_id=SOURCE_ID,
        metric_name=TEMPERATURE,
        window_start=WINDOW_START,
        value=999.0,
        kafka_partition=0,
        kafka_offset=101,
    )

    assert duplicate_result is False

    aggregate = get_aggregates_by_source_and_metric(
        SOURCE_ID,
        TEMPERATURE,
    )[0]

    assert aggregate["count"] == 1
    assert aggregate["minimum"] == 20.0
    assert aggregate["maximum"] == 20.0
    assert aggregate["average"] == 20.0


def test_second_event_in_same_window_updates_aggregate():
    process_event(
        event_id=EVENT_ID_1,
        source_id=SOURCE_ID,
        metric_name=TEMPERATURE,
        window_start=WINDOW_START,
        value=20.0,
        kafka_partition=0,
        kafka_offset=100,
    )
    process_event(
        event_id=EVENT_ID_2,
        source_id=SOURCE_ID,
        metric_name=TEMPERATURE,
        window_start=WINDOW_START,
        value=24.0,
        kafka_partition=0,
        kafka_offset=102,
    )

    aggregate = get_aggregates_by_source_and_metric(
        SOURCE_ID,
        TEMPERATURE,
    )[0]

    assert aggregate["count"] == 2
    assert aggregate["minimum"] == 20.0
    assert aggregate["maximum"] == 24.0
    assert aggregate["average"] == 22.0


def test_different_metrics_are_aggregated_separately():
    process_event(
        event_id=EVENT_ID_1,
        source_id=SOURCE_ID,
        metric_name=TEMPERATURE,
        window_start=WINDOW_START,
        value=20.0,
        kafka_partition=0,
        kafka_offset=100,
    )
    process_event(
        event_id=EVENT_ID_3,
        source_id=SOURCE_ID,
        metric_name=HUMIDITY,
        window_start=WINDOW_START,
        value=60.0,
        kafka_partition=0,
        kafka_offset=103,
    )

    aggregates = get_aggregates_by_source(SOURCE_ID)
    metrics = {aggregate["metric_name"] for aggregate in aggregates}

    assert len(aggregates) == 2
    assert metrics == {TEMPERATURE, HUMIDITY}


def test_nonexistent_metric_returns_empty_list():
    assert get_aggregates_by_source_and_metric(
        SOURCE_ID,
        "pressure",
    ) == []
