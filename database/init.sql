CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS aggregates (
    source_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    event_count BIGINT NOT NULL,
    value_sum DOUBLE PRECISION NOT NULL,
    minimum_value DOUBLE PRECISION NOT NULL,
    maximum_value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (source_id, metric_name, window_start)
);
