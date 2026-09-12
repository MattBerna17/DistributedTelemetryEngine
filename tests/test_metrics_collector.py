import pytest

from metrics.collector import MetricsCollector


def test_calculate_partition_rates():
    rates = MetricsCollector._calculate_partition_rates(
        start_offsets={
            0: 100,
            1: 200,
            2: 300,
            3: 400,
        },
        end_offsets={
            0: 150,
            1: 300,
            2: 500,
            3: 450,
        },
        interval=5.0,
    )

    assert rates == {
        0: 10.0,
        1: 20.0,
        2: 40.0,
        3: 10.0,
    }


def test_calculate_consumer_lag():
    lag = MetricsCollector._calculate_consumer_lag(
        partition_ids=[0, 1, 2, 3],
        latest_offsets={
            0: 1000,
            1: 800,
            2: 1200,
            3: 600,
        },
        earliest_offsets={
            0: 0,
            1: 0,
            2: 0,
            3: 0,
        },
        committed_offsets={
            0: 900,
            1: 750,
            2: 900,
            3: 550,
        },
    )

    assert lag == 500


def test_consumer_lag_uses_earliest_when_no_commit_exists():
    lag = MetricsCollector._calculate_consumer_lag(
        partition_ids=[0],
        latest_offsets={0: 1000},
        earliest_offsets={0: 300},
        committed_offsets={},
    )

    assert lag == 700


def test_invalid_sample_interval_is_rejected():
    with pytest.raises(ValueError):
        MetricsCollector(sample_interval=0)
