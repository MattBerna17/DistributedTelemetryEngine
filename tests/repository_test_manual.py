from datetime import datetime, timezone
from uuid import UUID

from database.repository import (
    check_database_connection,
    get_aggregates_by_source,
    get_aggregates_by_source_and_metric,
    get_connection,
    process_event,
)


# =========================================================
# Test data
# =========================================================

SOURCE_ID = "manual-test-sensor"

TEMPERATURE = "temperature"
HUMIDITY = "humidity"

WINDOW_START = datetime(
    2026,
    8,
    16,
    10,
    15,
    tzinfo=timezone.utc,
)

EVENT_ID_1 = UUID(
    "11111111-1111-1111-1111-111111111111"
)

EVENT_ID_2 = UUID(
    "22222222-2222-2222-2222-222222222222"
)

EVENT_ID_3 = UUID(
    "33333333-3333-3333-3333-333333333333"
)


def cleanup_test_data():
    """
    Removes all data created by this test.
    """

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                DELETE FROM processed_events
                WHERE event_id IN (%s, %s, %s)
                """,
                (
                    EVENT_ID_1,
                    EVENT_ID_2,
                    EVENT_ID_3,
                ),
            )

            cur.execute(
                """
                DELETE FROM aggregates
                WHERE source_id = %s
                """,
                (SOURCE_ID,),
            )


def main():

    print("\n===================================")
    print("REPOSITORY MANUAL TEST")
    print("===================================")

    # Always start from a clean state.
    cleanup_test_data()

    try:

        # =========================================================
        # TEST 1: database connection
        # =========================================================

        print("\n--- TEST 1: database connection ---")

        assert check_database_connection() is True

        print("TEST 1 PASSED")


        # =========================================================
        # TEST 2: process a new event
        # =========================================================

        print("\n--- TEST 2: process a new event ---")

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
        assert aggregate["value_sum"] == 20.0
        assert aggregate["minimum"] == 20.0
        assert aggregate["maximum"] == 20.0
        assert aggregate["average"] == 20.0

        print("TEST 2 PASSED")


        # =========================================================
        # TEST 3: duplicate event
        # =========================================================

        print("\n--- TEST 3: duplicate event ---")

        result = process_event(
            event_id=EVENT_ID_1,
            source_id=SOURCE_ID,
            metric_name=TEMPERATURE,
            window_start=WINDOW_START,

            # Different value on purpose:
            # it must NOT affect the aggregate.
            value=999.0,

            kafka_partition=0,
            kafka_offset=101,
        )

        assert result is False

        aggregates = get_aggregates_by_source_and_metric(
            SOURCE_ID,
            TEMPERATURE,
        )

        assert len(aggregates) == 1

        aggregate = aggregates[0]

        # Aggregate must still contain only the first event.
        assert aggregate["count"] == 1
        assert aggregate["value_sum"] == 20.0
        assert aggregate["minimum"] == 20.0
        assert aggregate["maximum"] == 20.0
        assert aggregate["average"] == 20.0

        print("TEST 3 PASSED")


        # =========================================================
        # TEST 4: second event in the same window
        # =========================================================

        print(
            "\n--- TEST 4: second event in the same window ---"
        )

        result = process_event(
            event_id=EVENT_ID_2,
            source_id=SOURCE_ID,
            metric_name=TEMPERATURE,
            window_start=WINDOW_START,
            value=24.0,
            kafka_partition=0,
            kafka_offset=102,
        )

        assert result is True

        aggregates = get_aggregates_by_source_and_metric(
            SOURCE_ID,
            TEMPERATURE,
        )

        assert len(aggregates) == 1

        aggregate = aggregates[0]

        assert aggregate["count"] == 2
        assert aggregate["value_sum"] == 44.0
        assert aggregate["minimum"] == 20.0
        assert aggregate["maximum"] == 24.0
        assert aggregate["average"] == 22.0

        print("TEST 4 PASSED")


        # =========================================================
        # TEST 5: different metric for the same source
        # =========================================================

        print(
            "\n--- TEST 5: different metric for the same source ---"
        )

        result = process_event(
            event_id=EVENT_ID_3,
            source_id=SOURCE_ID,
            metric_name=HUMIDITY,
            window_start=WINDOW_START,
            value=60.0,
            kafka_partition=0,
            kafka_offset=103,
        )

        assert result is True

        humidity_aggregates = (
            get_aggregates_by_source_and_metric(
                SOURCE_ID,
                HUMIDITY,
            )
        )

        assert len(humidity_aggregates) == 1

        aggregate = humidity_aggregates[0]

        assert aggregate["count"] == 1
        assert aggregate["value_sum"] == 60.0
        assert aggregate["minimum"] == 60.0
        assert aggregate["maximum"] == 60.0
        assert aggregate["average"] == 60.0

        print("TEST 5 PASSED")


        # =========================================================
        # TEST 6: get all aggregates for the source
        # =========================================================

        print(
            "\n--- TEST 6: retrieve all aggregates for source ---"
        )

        aggregates = get_aggregates_by_source(
            SOURCE_ID
        )

        # Temperature + humidity
        assert len(aggregates) == 2

        metrics = {
            aggregate["metric_name"]
            for aggregate in aggregates
        }

        assert TEMPERATURE in metrics
        assert HUMIDITY in metrics

        print("TEST 6 PASSED")


        # =========================================================
        # TEST 7: nonexistent metric
        # =========================================================

        print(
            "\n--- TEST 7: nonexistent metric returns empty list ---"
        )

        aggregates = get_aggregates_by_source_and_metric(
            SOURCE_ID,
            "pressure",
        )

        assert aggregates == []

        print("TEST 7 PASSED")


        # =========================================================
        # Final result
        # =========================================================

        print("\n===================================")
        print("ALL REPOSITORY TESTS PASSED")
        print("===================================")

    finally:

        print("\n--- Cleaning test data ---")

        cleanup_test_data()

        print("Test data removed.")


if __name__ == "__main__":
    main()