from datetime import datetime, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

import main
from common.models import SystemMetricsSnapshot


class FakeKafkaProducer:
    """Minimal Kafka producer fake used by API unit tests."""

    def __init__(self):
        self.events = []
        self.producer = self

    def publish_event(self, event):
        self.events.append(event)
        return True

    def flush(self):
        pass


class FakeMetricsCollector:
    """Returns a deterministic system-metrics snapshot."""

    async def collect(self):
        return SystemMetricsSnapshot(
            arrival_rate=200.0,
            consumer_lag=25,
            current_workers=2,
            partition_rates={
                0: 50.0,
                1: 50.0,
                2: 60.0,
                3: 40.0,
            },
        )


@pytest.fixture
def fake_kafka_producer(monkeypatch):
    # The lifespan reads these variables when TestClient starts.
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "unused:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "telemetry-events")

    producer = FakeKafkaProducer()
    monkeypatch.setattr(
        main,
        "KafkaProducerService",
        lambda **kwargs: producer,
    )
    return producer


@pytest.fixture
def client(fake_kafka_producer):
    with TestClient(main.app) as test_client:
        yield test_client


def test_health(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_post_event(client, fake_kafka_producer):
    event_id = str(uuid4())
    payload = {
        "event_id": event_id,
        "source_id": "1",
        "event_time": "2026-08-23T12:00:00Z",
        "metric_name": "temperature",
        "value": 23.0,
        "schema_version": 1,
    }

    response = client.post("/events", json=payload)

    # The current FastAPI endpoint does not explicitly set 202,
    # therefore FastAPI returns the default 200 status code.
    assert response.status_code == 200
    assert response.json() == {
        "event_id": event_id,
        "status": "accepted",
    }
    assert len(fake_kafka_producer.events) == 1
    assert str(fake_kafka_producer.events[0].event_id) == event_id


def test_invalid_event_is_rejected(client):
    response = client.post(
        "/events",
        json={
            "source_id": "sensor-invalid",
            "value": "not-a-number",
        },
    )

    assert response.status_code == 422
    assert response.json() == {
        "event_id": None,
        "status": "rejected",
    }


def test_get_aggregates(client, monkeypatch):
    fake_aggregates = [
        {
            "source_id": "1",
            "metric_name": "temperature",
            "window_start": datetime(
                2026, 8, 23, 12, 0, tzinfo=timezone.utc
            ),
            "count": 10,
            "average": 23.5,
            "minimum": 20.0,
            "maximum": 27.0,
        },
        {
            "source_id": "1",
            "metric_name": "temperature",
            "window_start": datetime(
                2026, 8, 23, 12, 1, tzinfo=timezone.utc
            ),
            "count": 15,
            "average": 24.0,
            "minimum": 21.0,
            "maximum": 28.0,
        },
    ]

    monkeypatch.setattr(
        main,
        "get_aggregates_by_source_metric_and_window",
        lambda *args: fake_aggregates,
    )

    response = client.get(
        "/aggregates",
        params={
            "source_id": "1",
            "metric_name": "temperature",
            "window_start": "2026-08-23T12:00:00Z",
            "window_end": "2026-08-23T12:02:00Z",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["source_id"] == "1"
    assert data[0]["metric_name"] == "temperature"
    assert data[0]["count"] == 10
    assert data[0]["average"] == 23.5
    assert data[1]["count"] == 15
    assert data[1]["maximum"] == 28.0


def test_manual_reconfiguration(client):
    payload = {
        "arrival_rate": 2000,
        "worker_capacity": 1000,
        "target_utilization": 0.8,
        "current_workers": 2,
        "consumer_lag": 500,
        "partition_rates": {
            "0": 100,
            "1": 100,
            "2": 600,
            "3": 100,
        },
    }

    response = client.post("/reconfiguration", json=payload)

    assert response.status_code == 200
    data = response.json()
    assert data["current_workers"] == 2
    assert data["target_workers"] == 3
    assert len(data["actions"]) == 2
    assert any(
        action["type"] == "SCALE_WORKERS"
        and action["from"] == 2
        and action["to"] == 3
        for action in data["actions"]
    )
    assert any(
        action["type"] == "INVESTIGATE_HOT_PARTITION"
        and action["partition"] == 2
        for action in data["actions"]
    )


def test_metrics_endpoint(client, monkeypatch):
    monkeypatch.setattr(
        main,
        "metrics_collector",
        FakeMetricsCollector(),
    )

    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.json() == {
        "arrival_rate": 200.0,
        "consumer_lag": 25,
        "partition_rates": {
            "0": 50.0,
            "1": 50.0,
            "2": 60.0,
            "3": 40.0,
        },
        "current_workers": 2,
    }


def test_automatic_reconfiguration_endpoint(client, monkeypatch):
    monkeypatch.setattr(
        main,
        "metrics_collector",
        FakeMetricsCollector(),
    )

    response = client.get("/reconfiguration/auto")

    assert response.status_code == 200
    data = response.json()
    assert data["current_workers"] == 2
    assert "target_workers" in data
    assert "actions" in data
    assert "reason" in data
