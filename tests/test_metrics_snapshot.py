from common.models import SystemMetricsSnapshot
from metrics.config import TARGET_UTILIZATION, WORKER_CAPACITY
from services.reconfiguration_service import build_reconfiguration_request


def test_build_reconfiguration_request_from_metrics_snapshot():
    metrics = SystemMetricsSnapshot(
        arrival_rate=200.0,
        consumer_lag=50,
        current_workers=2,
        partition_rates={
            0: 50.0,
            1: 50.0,
            2: 60.0,
            3: 40.0,
        },
    )

    request = build_reconfiguration_request(metrics)

    assert request.arrival_rate == metrics.arrival_rate
    assert request.consumer_lag == metrics.consumer_lag
    assert request.current_workers == metrics.current_workers
    assert request.partition_rates == metrics.partition_rates

    # These two values come from the current metrics/config.py.
    assert request.worker_capacity == WORKER_CAPACITY
    assert request.target_utilization == TARGET_UTILIZATION
