import pytest

from metrics.collector import MetricsCollector


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.anyio
async def test_collect_system_metrics_from_kafka():
    collector = MetricsCollector()

    metrics = await collector.collect()

    assert metrics.arrival_rate >= 0
    assert metrics.consumer_lag >= 0
    assert metrics.current_workers >= 0
    assert metrics.partition_rates
    assert all(rate >= 0 for rate in metrics.partition_rates.values())
    assert sum(metrics.partition_rates.values()) == pytest.approx(
        metrics.arrival_rate
    )
