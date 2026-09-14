from datetime import datetime
import os
from uuid import UUID
import psycopg2
from psycopg2.extras import register_uuid
from dotenv import load_dotenv


load_dotenv()

register_uuid()


def get_connection():
    """
    Creates a connection to PostgreSQL
    """
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "adaptive_metrics"),
        user=os.getenv("POSTGRES_USER", "adaptive"),
        password=os.environ["POSTGRES_PASSWORD"],
    )


def check_database_connection() -> bool:
    """
    Executes a simple SELECT 1 query to verify PostgreSQL availability
    """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
        return result == (1,)
    except psycopg2.Error:
        return False
    finally:
        if conn is not None:
            conn.close()


def process_event(
    event_id: UUID,
    source_id: str,
    metric_name: str,
    window_start: datetime,
    value: float,
    kafka_partition: int,
    kafka_offset: int,
) -> bool:
    """
    Processes one event transactionally

    Returns:
        True -> new event, aggregate updated
        False -> duplicate event, aggregate unchanged
    """
    conn = get_connection()
    try:
        # the connection context manager handles commit on success and rollback on error
        with conn:
            with conn.cursor() as cur:
                # try to register the event as processed. if event_id already exists, the event is a duplicate
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
                    RETURNING event_id
                    """,
                    (
                        event_id,
                        kafka_partition,
                        kafka_offset,
                    ),
                )

                inserted_event = cur.fetchone()
                # duplicate event: do not update the aggregate
                if inserted_event is None:
                    return False

                # new event: update the corresponding aggregate in the same database transaction
                _upsert_aggregate(
                    cur,
                    source_id,
                    metric_name,
                    window_start,
                    value,
                )
        return True
    finally:
        conn.close()


def _upsert_aggregate(
    cursor,
    source_id: str,
    metric_name: str,
    window_start: datetime,
    value: float,
) -> None:
    """
    Inserts a new aggregate or updates an existing one
    """
    cursor.execute(
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
            metric_name,
            window_start,
            value,
            value,
            value,
        ),
    )


def get_aggregates_by_source(
    source_id: str,
) -> list[dict]:
    """
    Returns all aggregates for a source
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        source_id,
                        metric_name,
                        window_start,
                        event_count,
                        value_sum,
                        minimum_value,
                        maximum_value,
                        value_sum / event_count AS average
                    FROM aggregates
                    WHERE source_id = %s
                    ORDER BY metric_name, window_start
                    """,
                    (source_id,),
                )

                rows = cur.fetchall()
    finally:
        conn.close()
    return [
        {
            "source_id": row[0],
            "metric_name": row[1],
            "window_start": row[2],
            "count": row[3],
            "minimum": row[5],
            "maximum": row[6],
            "average": row[7],
        }
        for row in rows
    ]


def get_aggregates_by_source_and_metric(
    source_id: str,
    metric_name: str,
) -> list[dict]:
    """
    Returns aggregates for one source and one metric
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        source_id,
                        metric_name,
                        window_start,
                        event_count,
                        value_sum,
                        minimum_value,
                        maximum_value,
                        value_sum / event_count AS average
                    FROM aggregates
                    WHERE source_id = %s
                      AND metric_name = %s
                    ORDER BY window_start
                    """,
                    (
                        source_id,
                        metric_name,
                    ),
                )

                rows = cur.fetchall()
    finally:
        conn.close()
    return [
        {
            "source_id": row[0],
            "metric_name": row[1],
            "window_start": row[2],
            "count": row[3],
            "minimum": row[5],
            "maximum": row[6],
            "average": row[7],
        }
        for row in rows
    ]


def get_aggregates_by_source_metric_and_window(
    source_id: str,
    metric_name: str,
    window_start: datetime,
    window_end: datetime,
) -> list[dict]:
    """
    Returns aggregates for one source and one metric within the specified time window
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        source_id,
                        metric_name,
                        window_start,
                        event_count,
                        value_sum,
                        minimum_value,
                        maximum_value,
                        value_sum / event_count AS average
                    FROM aggregates
                    WHERE source_id = %s
                      AND metric_name = %s
                      AND window_start >= %s
                      AND window_start < %s
                    ORDER BY window_start
                    """,
                    (
                        source_id,
                        metric_name,
                        window_start,
                        window_end,
                    ),
                )
                rows = cur.fetchall()
    finally:
        conn.close()
    return [
        {
            "source_id": row[0],
            "metric_name": row[1],
            "window_start": row[2],
            "count": row[3],
            "minimum": row[5],
            "maximum": row[6],
            "average": row[7],
        }
        for row in rows
    ]