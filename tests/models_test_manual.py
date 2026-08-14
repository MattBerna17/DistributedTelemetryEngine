from datetime import datetime
from uuid import UUID

from pydantic import ValidationError

from common.models import (
    TelemetryEvent,
    ReconfigurationRequest,
    ReconfigurationResponse,
    ScaleWorkersAction,
    InvestigateHotPartitionAction,
)


def test_valid_event():
    event = TelemetryEvent(
        event_id="8f55e942-78c3-49ee-8f55-c72bf728a214",
        source_id="sensor-17",
        event_time="2026-08-01T10:15:22.521Z",
        metric_name="temperature",
        value=41.7,
        schema_version=1,
    )

    assert event.source_id == "sensor-17"
    assert event.metric_name == "temperature"
    assert event.value == 41.7
    assert isinstance(event.event_time, datetime)

    print("PASS - valid event")


def test_invalid_uuid():
    try:
        TelemetryEvent(
            event_id="invalid-uuid",
            source_id="sensor-17",
            event_time="2026-08-01T10:15:22.521Z",
            metric_name="temperature",
            value=41.7,
            schema_version=1,
        )
    except ValidationError:
        print("PASS - invalid UUID rejected")
        return

    raise AssertionError("Invalid UUID was accepted")


def test_invalid_schema_version():
    try:
        TelemetryEvent(
            event_id="8f55e942-78c3-49ee-8f55-c72bf728a214",
            source_id="sensor-17",
            event_time="2026-08-01T10:15:22.521Z",
            metric_name="temperature",
            value=41.7,
            schema_version=2,
        )
    except ValidationError:
        print("PASS - unsupported schema version rejected")
        return

    raise AssertionError("Unsupported schema version was accepted")


def test_invalid_reconfiguration_request():
    try:
        ReconfigurationRequest(
            arrival_rate=420,
            worker_capacity=250,
            target_utilization=1.5,
            current_workers=2,
            consumer_lag=1800,
            partition_rates={
                0: 70,
                1: 65,
                2: 240,
                3: 45,
            },
        )
    except ValidationError:
        print("PASS - invalid reconfiguration request rejected")
        return

    raise AssertionError("Invalid reconfiguration request was accepted")


def test_reconfiguration_response():
    response = ReconfigurationResponse(
        reason=[
            "target_capacity_exceeded",
            "partition_skew_detected",
        ],
        current_workers=2,
        target_workers=3,
        actions=[
            ScaleWorkersAction(
                **{
                    "type": "SCALE_WORKERS",
                    "from": 2,
                    "to": 3,
                }
            ),
            InvestigateHotPartitionAction(
                type="INVESTIGATE_HOT_PARTITION",
                partition=2,
            ),
        ],
    )

    assert response.current_workers == 2
    assert response.target_workers == 3
    assert len(response.actions) == 2

    print("PASS - reconfiguration response")


if __name__ == "__main__":
    test_valid_event()
    test_invalid_uuid()
    test_invalid_schema_version()
    test_invalid_reconfiguration_request()
    test_reconfiguration_response()

    print("\nAll tests passed.")