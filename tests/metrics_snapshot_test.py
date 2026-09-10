from common.models import SystemMetricsSnapshot
from services.reconfiguration_service import (
    build_reconfiguration_request,
)


def test_build_reconfiguration_request():
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

    request = build_reconfiguration_request(
        metrics
    )

    assert request.arrival_rate == 200.0
    assert request.consumer_lag == 50
    assert request.current_workers == 2
    assert request.partition_rates == {
        0: 50.0,
        1: 50.0,
        2: 60.0,
        3: 40.0,
    }

    assert request.worker_capacity > 0
    assert 0 < request.target_utilization <= 1