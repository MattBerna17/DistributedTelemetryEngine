from datetime import datetime

import pytest
from pydantic import ValidationError

from common.models import (
    InvestigateHotPartitionAction,
    ReconfigurationRequest,
    ReconfigurationResponse,
    ScaleWorkersAction,
    TelemetryEvent,
)


def test_valid_telemetry_event():
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


def test_invalid_uuid_is_rejected():
    with pytest.raises(ValidationError):
        TelemetryEvent(
            event_id="invalid-uuid",
            source_id="sensor-17",
            event_time="2026-08-01T10:15:22.521Z",
            metric_name="temperature",
            value=41.7,
            schema_version=1,
        )


def test_invalid_schema_version_is_rejected():
    with pytest.raises(ValidationError):
        TelemetryEvent(
            event_id="8f55e942-78c3-49ee-8f55-c72bf728a214",
            source_id="sensor-17",
            event_time="2026-08-01T10:15:22.521Z",
            metric_name="temperature",
            value=41.7,
            schema_version=2,
        )


def test_invalid_reconfiguration_request_is_rejected():
    with pytest.raises(ValidationError):
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


def test_reconfiguration_response_accepts_actions():
    response = ReconfigurationResponse(
        reason=[
            "target_capacity_exceeded",
            "partition_skew_detected",
        ],
        current_workers=2,
        target_workers=3,
        actions=[
            ScaleWorkersAction.model_validate(
                {
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
