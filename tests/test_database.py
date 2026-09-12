import os
from uuid import uuid4

import psycopg2
import pytest
from dotenv import load_dotenv


load_dotenv()

pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
]

METRIC_NAME = "temperature"
WINDOW_START = "2026-08-15 10:15:00+00"


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "adaptive_metrics"),
        user=os.getenv("POSTGRES_USER", "adaptive"),
        password=os.environ["POSTGRES_PASSWORD"],
    )


@pytest.fixture
def db_connection():
    conn = get_connection()

    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()


def test_processed_events_primary_key_prevents_duplicates(db_connection):
    event_id = str(uuid4())

    with db_connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO processed_events (
                event_id, processed_at, kafka_partition, kafka_offset
            )
            VALUES (%s, NOW(), %s, %s)
            """,
            (event_id, 0, 15),
        )

        cur.execute(
            """
            INSERT INTO processed_events (
                event_id, processed_at, kafka_partition, kafka_offset
            )
            VALUES (%s, NOW(), %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (event_id, 0, 16),
        )

        cur.execute(
            "SELECT COUNT(*) FROM processed_events WHERE event_id = %s",
            (event_id,),
        )

        assert cur.fetchone()[0] == 1


def test_aggregate_upsert_updates_statistics(db_connection):
    source_id = f"pytest-db-{uuid4()}"

    with db_connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO aggregates (
                source_id, metric_name, window_start,
                event_count, value_sum, minimum_value, maximum_value
            )
            VALUES (%s, %s, %s, 1, %s, %s, %s)
            """,
            (
                source_id,
                METRIC_NAME,
                WINDOW_START,
                20.0,
                20.0,
                20.0,
            ),
        )

        cur.execute(
            """
            INSERT INTO aggregates (
                source_id, metric_name, window_start,
                event_count, value_sum, minimum_value, maximum_value
            )
            VALUES (%s, %s, %s, 1, %s, %s, %s)
            ON CONFLICT (source_id, metric_name, window_start)
            DO UPDATE SET
                event_count = aggregates.event_count + 1,
                value_sum = aggregates.value_sum + EXCLUDED.value_sum,
                minimum_value = LEAST(
                    aggregates.minimum_value,
                    EXCLUDED.minimum_value
                ),
                maximum_value = GREATEST(
                    aggregates.maximum_value,
                    EXCLUDED.maximum_value
                )
            """,
            (
                source_id,
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
            (source_id, METRIC_NAME, WINDOW_START),
        )

        row = cur.fetchone()
        assert row == (2, 44.0, 20.0, 24.0, 22.0)
