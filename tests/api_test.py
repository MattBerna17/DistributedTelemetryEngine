from datetime import datetime, timezone
from uuid import uuid4
from fastapi.testclient import TestClient
import main
from services.reconfiguration import ReconfigurationPlanner

# monkeypatch, provided by pytest, is used to set some attributes of the main (e.g. kafka and postgresql) to decouple the api with the services and other dependencies


class FakeKafkaProducer:
    def __init__(self):
        self.events = []
        self.producer = self

    def publish_event(self, event):
        self.events.append(event)

    def flush(self):
        pass


def test_health():
    with TestClient(main.app) as client:
        response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_post_event(monkeypatch):
    fake_producer = FakeKafkaProducer()
    monkeypatch.setattr(main, "KafkaProducerService", lambda **kwargs: fake_producer)
    event_id = str(uuid4())
    payload = {
        "event_id": event_id,
        "source_id": "1",
        "event_time": "2026-08-23T12:00:00Z",
        "metric_name": "temperature",
        "value": 23.0,
        "schema_version": 1,
    }

    with TestClient(main.app) as client:
        response = client.post("/events", json=payload)

    assert response.status_code == 202
    assert response.json() == {
        "event_id": event_id,
        "status": "accepted",
    }
    assert len(fake_producer.events) == 1
    assert str(fake_producer.events[0].event_id) == event_id


def test_get_aggregates(monkeypatch):
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

    monkeypatch.setattr(main, "get_aggregates_by_source_metric_and_window", lambda *args: fake_aggregates)

    with TestClient(main.app) as client:
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


def test_reconfiguration():
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

    with TestClient(main.app) as client:
        response = client.post("/reconfiguration",json=payload)
    
    assert response.status_code == 200
    data = response.json()
    assert data["current_workers"] == 2
    assert data["target_workers"] == 3
    assert len(data["actions"]) == 2
    assert any(action["type"] == "SCALE_WORKERS" and action["from"] == 2 and action["to"] == 3 for action in data["actions"])
    assert any(action["type"] == "INVESTIGATE_HOT_PARTITION" and action["partition"] == 2 for action in data["actions"])