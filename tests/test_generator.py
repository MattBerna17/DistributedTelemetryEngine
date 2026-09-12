from datetime import datetime
from uuid import UUID

import pytest

from generator.config import METRIC_RANGES, METRICS
from generator.event_factory import (
    build_source_ids,
    choose_metric,
    choose_source,
    create_event,
    generate_value,
)


def test_build_source_ids():
    assert build_source_ids(3) == [
        "sensor-1",
        "sensor-2",
        "sensor-3",
    ]


def test_build_source_ids_rejects_non_positive_count():
    with pytest.raises(ValueError):
        build_source_ids(0)


def test_choose_source_uniform_returns_known_source():
    sources = build_source_ids(5)

    for _ in range(100):
        assert choose_source(sources, "uniform") in sources


def test_choose_source_skewed_returns_known_source():
    sources = build_source_ids(5)

    for _ in range(100):
        assert choose_source(sources, "skewed") in sources


def test_choose_source_rejects_invalid_distribution():
    with pytest.raises(ValueError):
        choose_source(build_source_ids(3), "invalid")


def test_choose_source_rejects_empty_source_list():
    with pytest.raises(ValueError):
        choose_source([], "uniform")


def test_choose_metric_returns_configured_metric():
    for _ in range(100):
        assert choose_metric(METRICS) in METRICS


def test_choose_metric_rejects_empty_list():
    with pytest.raises(ValueError):
        choose_metric([])


def test_generate_value_respects_metric_ranges():
    for metric_name, (minimum, maximum) in METRIC_RANGES.items():
        for _ in range(100):
            value = generate_value(metric_name)
            assert isinstance(value, float)
            assert minimum <= value <= maximum


def test_generate_value_rejects_unknown_metric():
    with pytest.raises(ValueError):
        generate_value("unknown_metric")


def test_create_event_builds_valid_event():
    source_id = "sensor-1"
    metric_name = "temperature"

    event = create_event(source_id, metric_name)

    assert set(event) == {
        "event_id",
        "source_id",
        "event_time",
        "metric_name",
        "value",
        "schema_version",
    }
    assert event["source_id"] == source_id
    assert event["metric_name"] == metric_name

    UUID(event["event_id"])

    parsed_time = datetime.fromisoformat(event["event_time"])
    assert parsed_time.tzinfo is not None

    minimum, maximum = METRIC_RANGES[metric_name]
    assert minimum <= event["value"] <= maximum
    assert event["schema_version"] == 1


def test_create_event_generates_unique_ids():
    event_1 = create_event("sensor-1", "temperature")
    event_2 = create_event("sensor-1", "temperature")

    assert event_1["event_id"] != event_2["event_id"]
