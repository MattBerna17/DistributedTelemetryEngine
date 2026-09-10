from common.models import (
    ReconfigurationRequest,
    SystemMetricsSnapshot,
)
from metrics.config import (
    TARGET_UTILIZATION,
    WORKER_CAPACITY,
)


def build_reconfiguration_request(
    metrics: SystemMetricsSnapshot,
) -> ReconfigurationRequest:
    """
    Builds a ReconfigurationRequest using the metrics
    collected from the running system and the configured
    Planner parameters.
    """

    return ReconfigurationRequest(
        arrival_rate=metrics.arrival_rate,
        worker_capacity=WORKER_CAPACITY,
        target_utilization=TARGET_UTILIZATION,
        current_workers=metrics.current_workers,
        consumer_lag=metrics.consumer_lag,
        partition_rates=metrics.partition_rates,
    )