import os

import psycopg2
from dotenv import load_dotenv


# Load environment variables from .env, if available
load_dotenv()


EVENT_ID = "11111111-1111-1111-1111-111111111111"
SOURCE_ID = "test-sensor"
METRIC_NAME = "temperature"
WINDOW_START = "2026-08-15 10:15:00+00"


def get_connection():
    """
    Create a connection to the PostgreSQL database.
    """

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "adaptive_metrics"),
        user=os.getenv("POSTGRES_USER", "adaptive"),
        password=os.environ["POSTGRES_PASSWORD"],
    )


def main():
    print("\n--- Connecting to PostgreSQL ---")

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            # =========================================================
            # Initial cleanup
            # =========================================================

            print("\n--- Cleaning previous test data ---")

            cur.execute(
                """
                DELETE FROM processed_events
                WHERE event_id = %s
                """,
                (EVENT_ID,),
            )

            cur.execute(
                """
                DELETE FROM aggregates
                WHERE source_id = %s
                  AND metric_name = %s
                  AND window_start = %s
                """,
                (
                    SOURCE_ID,
                    METRIC_NAME,
                    WINDOW_START,
                ),
            )

            # =========================================================
            # TEST 1: processed_events insertion
            # =========================================================

            print("\n--- TEST 1: inserting a processed event ---")

            cur.execute(
                """
                INSERT INTO processed_events (
                    event_id,
                    processed_at,
                    kafka_partition,
                    kafka_offset
                )
                VALUES (%s, NOW(), %s, %s)
                """,
                (
                    EVENT_ID,
                    0,
                    15,
                ),
            )

            cur.execute(
                """
                SELECT COUNT(*)
                FROM processed_events
                WHERE event_id = %s
                """,
                (EVENT_ID,),
            )

            count = cur.fetchone()[0]

            print("Number of rows:", count)
            assert count == 1

            print("TEST 1 PASSED")

            # =========================================================
            # TEST 2: duplicate event
            # =========================================================

            print("\n--- TEST 2: inserting the same event_id again ---")

            cur.execute(
                """
                INSERT INTO processed_events (
                    event_id,
                    processed_at,
                    kafka_partition,
                    kafka_offset
                )
                VALUES (%s, NOW(), %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    EVENT_ID,
                    0,
                    16,
                ),
            )

            cur.execute(
                """
                SELECT COUNT(*)
                FROM processed_events
                WHERE event_id = %s
                """,
                (EVENT_ID,),
            )

            count = cur.fetchone()[0]

            print("Number of rows:", count)
            assert count == 1

            print("TEST 2 PASSED")

            # =========================================================
            # TEST 3: create the first aggregate
            # =========================================================

            print("\n--- TEST 3: creating the first aggregate ---")

            cur.execute(
                """
                INSERT INTO aggregates (
                    source_id,
                    metric_name,
                    window_start,
                    event_count,
                    value_sum,
                    minimum_value,
                    maximum_value
                )
                VALUES (%s, %s, %s, 1, %s, %s, %s)
                """,
                (
                    SOURCE_ID,
                    METRIC_NAME,
                    WINDOW_START,
                    20.0,
                    20.0,
                    20.0,
                ),
            )

            cur.execute(
                """
                SELECT
                    event_count,
                    value_sum,
                    minimum_value,
                    maximum_value
                FROM aggregates
                WHERE source_id = %s
                  AND metric_name = %s
                  AND window_start = %s
                """,
                (
                    SOURCE_ID,
                    METRIC_NAME,
                    WINDOW_START,
                ),
            )

            row = cur.fetchone()

            print("Aggregate:", row)

            assert row[0] == 1
            assert row[1] == 20.0
            assert row[2] == 20.0
            assert row[3] == 20.0

            print("TEST 3 PASSED")

            # =========================================================
            # TEST 4: aggregate UPSERT
            # =========================================================

            print("\n--- TEST 4: updating the aggregate with value 24 ---")

            cur.execute(
                """
                INSERT INTO aggregates (
                    source_id,
                    metric_name,
                    window_start,
                    event_count,
                    value_sum,
                    minimum_value,
                    maximum_value
                )
                VALUES (%s, %s, %s, 1, %s, %s, %s)

                ON CONFLICT (
                    source_id,
                    metric_name,
                    window_start
                )
                DO UPDATE SET
                    event_count =
                        aggregates.event_count + 1,

                    value_sum =
                        aggregates.value_sum
                        + EXCLUDED.value_sum,

                    minimum_value =
                        LEAST(
                            aggregates.minimum_value,
                            EXCLUDED.minimum_value
                        ),

                    maximum_value =
                        GREATEST(
                            aggregates.maximum_value,
                            EXCLUDED.maximum_value
                        )
                """,
                (
                    SOURCE_ID,
                    METRIC_NAME,
                    WINDOW_START,
                    24.0,
                    24.0,
                    24.0,
                ),
            )

            cur.execute(
                """
                SELECT
                    event_count,
                    value_sum,
                    minimum_value,
                    maximum_value,
                    value_sum / event_count AS average
                FROM aggregates
                WHERE source_id = %s
                  AND metric_name = %s
                  AND window_start = %s
                """,
                (
                    SOURCE_ID,
                    METRIC_NAME,
                    WINDOW_START,
                ),
            )

            row = cur.fetchone()

            event_count = row[0]
            value_sum = row[1]
            minimum = row[2]
            maximum = row[3]
            average = row[4]

            print("Count   =", event_count)
            print("Sum     =", value_sum)
            print("Minimum =", minimum)
            print("Maximum =", maximum)
            print("Average =", average)

            assert event_count == 2
            assert value_sum == 44.0
            assert minimum == 20.0
            assert maximum == 24.0
            assert average == 22.0

            print("TEST 4 PASSED")

        print("\n==============================")
        print("ALL DATABASE TESTS PASSED")
        print("==============================")

    finally:
        # Roll back the whole transaction so that
        # no test data remains in the database.
        conn.rollback()
        conn.close()

        print("\nTest transaction rolled back.")
        print("Database connection closed.")


if __name__ == "__main__":
    main()