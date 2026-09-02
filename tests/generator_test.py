from datetime import datetime
from uuid import UUID

from generator.config import (
    METRIC_RANGES,
    METRICS,
)
from generator.event_factory import (
    build_source_ids,
    choose_metric,
    choose_source,
    create_event,
    generate_value,
)


def test_build_source_ids():
    print("\n--- TEST 1: build_source_ids ---")

    sources = build_source_ids(3)

    assert sources == [
        "sensor-1",
        "sensor-2",
        "sensor-3",
    ]

    print("TEST 1 PASSED")


def test_build_source_ids_invalid():
    print("\n--- TEST 2: invalid number of sources ---")

    try:
        build_source_ids(0)

        # If execution reaches this point,
        # the expected exception was not raised.
        assert False

    except ValueError:
        pass

    print("TEST 2 PASSED")


def test_choose_source_uniform():
    print("\n--- TEST 3: uniform source selection ---")

    sources = build_source_ids(5)

    for _ in range(100):
        source = choose_source(
            sources,
            "uniform",
        )

        assert source in sources

    print("TEST 3 PASSED")


def test_choose_source_skewed():
    print("\n--- TEST 4: skewed source selection ---")

    sources = build_source_ids(5)

    for _ in range(100):
        source = choose_source(
            sources,
            "skewed",
        )

        assert source in sources

    print("TEST 4 PASSED")


def test_choose_source_invalid_distribution():
    print("\n--- TEST 5: invalid distribution ---")

    sources = build_source_ids(3)

    try:
        choose_source(
            sources,
            "invalid",
        )

        assert False

    except ValueError:
        pass

    print("TEST 5 PASSED")


def test_choose_source_empty_list():
    print("\n--- TEST 6: empty source list ---")

    try:
        choose_source(
            [],
            "uniform",
        )

        assert False

    except ValueError:
        pass

    print("TEST 6 PASSED")


def test_choose_metric():
    print("\n--- TEST 7: choose_metric ---")

    for _ in range(100):
        metric = choose_metric(METRICS)

        assert metric in METRICS

    print("TEST 7 PASSED")


def test_choose_metric_empty_list():
    print("\n--- TEST 8: empty metric list ---")

    try:
        choose_metric([])

        assert False

    except ValueError:
        pass

    print("TEST 8 PASSED")


def test_generate_value():
    print("\n--- TEST 9: generate_value ---")

    for metric_name, value_range in METRIC_RANGES.items():
        minimum, maximum = value_range

        for _ in range(100):
            value = generate_value(
                metric_name
            )

            assert isinstance(value, float)
            assert minimum <= value <= maximum

    print("TEST 9 PASSED")


def test_generate_value_invalid_metric():
    print("\n--- TEST 10: unsupported metric ---")

    try:
        generate_value(
            "unknown_metric"
        )

        assert False

    except ValueError:
        pass

    print("TEST 10 PASSED")


def test_create_event():
    print("\n--- TEST 11: create_event ---")

    source_id = "sensor-1"
    metric_name = "temperature"

    event = create_event(
        source_id,
        metric_name,
    )

    # Verify required fields.
    assert "event_id" in event
    assert "source_id" in event
    assert "event_time" in event
    assert "metric_name" in event
    assert "value" in event
    assert "schema_version" in event

    # Verify source.
    assert event["source_id"] == source_id

    # Verify metric.
    assert event["metric_name"] == metric_name

    # Verify event_id is a valid UUID.
    UUID(event["event_id"])

    # Verify event_time is a valid ISO datetime.
    parsed_time = datetime.fromisoformat(
        event["event_time"]
    )

    assert parsed_time.tzinfo is not None

    # Verify generated value respects
    # the configured metric range.
    minimum, maximum = METRIC_RANGES[
        metric_name
    ]

    assert minimum <= event["value"] <= maximum

    # Current schema version.
    assert event["schema_version"] == 1

    print("TEST 11 PASSED")


def test_event_ids_are_unique():
    print("\n--- TEST 12: unique event IDs ---")

    event_1 = create_event(
        "sensor-1",
        "temperature",
    )

    event_2 = create_event(
        "sensor-1",
        "temperature",
    )

    assert (
        event_1["event_id"]
        != event_2["event_id"]
    )

    print("TEST 12 PASSED")


def main():
    print("\n===================================")
    print("EVENT GENERATOR MANUAL TEST")
    print("===================================")

    test_build_source_ids()
    test_build_source_ids_invalid()

    test_choose_source_uniform()
    test_choose_source_skewed()
    test_choose_source_invalid_distribution()
    test_choose_source_empty_list()

    test_choose_metric()
    test_choose_metric_empty_list()

    test_generate_value()
    test_generate_value_invalid_metric()

    test_create_event()
    test_event_ids_are_unique()

    print("\n===================================")
    print("ALL EVENT GENERATOR TESTS PASSED")
    print("===================================")


if __name__ == "__main__":
    main()