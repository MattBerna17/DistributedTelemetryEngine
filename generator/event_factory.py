import random
from datetime import datetime, timezone
from uuid import uuid4

from generator.config import (
    HOT_SOURCE_PROBABILITY,
    METRIC_RANGES,
    SCHEMA_VERSION,
    SUPPORTED_DISTRIBUTIONS,
)


def build_source_ids(num_sources: int) -> list[str]:
    """
    Creates a list of simulated source identifiers.
    """

    if num_sources <= 0:
        raise ValueError("num_sources must be greater than 0")

    sources = []

    for i in range(num_sources):
        sources.append(f"sensor-{i + 1}")

    return sources


def choose_source(
    source_ids: list[str],
    distribution: str,
) -> str:
    """
    Selects the source that will generate the next event.

    Supported distributions:
        uniform: all sources have the same probability
        skewed: the first source is the hot source
    """

    if not source_ids:
        raise ValueError("source_ids cannot be empty")

    if distribution not in SUPPORTED_DISTRIBUTIONS:
        raise ValueError(
            f"'distribution' must be one of: "
            f"{', '.join(SUPPORTED_DISTRIBUTIONS)}"
        )

    if distribution == "uniform":
        return random.choice(source_ids)

    # With only one source, it is necessarily selected.
    if len(source_ids) == 1:
        return source_ids[0]

    # By convention, the first source is the hot source.
    remaining_probability = (
        1.0 - HOT_SOURCE_PROBABILITY
    ) / (len(source_ids) - 1)

    weights = [HOT_SOURCE_PROBABILITY]

    for _ in range(1, len(source_ids)):
        weights.append(remaining_probability)

    return random.choices(
        source_ids,
        weights=weights,
        k=1,
    )[0]


def choose_metric(
    metrics: list[str],
) -> str:
    """
    Randomly selects the metric for the next event.
    """

    if not metrics:
        raise ValueError("metrics cannot be empty")

    return random.choice(metrics)


def generate_value(
    metric_name: str,
) -> float:
    """
    Generates a random value within the configured range
    for the given metric.
    """

    if metric_name not in METRIC_RANGES:
        raise ValueError(
            f"Unsupported metric: {metric_name}"
        )

    minimum, maximum = METRIC_RANGES[metric_name]

    value = random.uniform(
        minimum,
        maximum,
    )

    return round(value, 2)


def create_event(
    source_id: str,
    metric_name: str,
) -> dict:
    """
    Creates a new telemetry event.
    """

    return {
        "event_id": str(uuid4()),
        "source_id": source_id,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "metric_name": metric_name,
        "value": generate_value(metric_name),
        "schema_version": SCHEMA_VERSION,
    }