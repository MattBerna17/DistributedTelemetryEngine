import pytest

from common.models import (
    InvestigateHotPartitionAction,
    ReconfigurationRequest,
    ScaleWorkersAction,
)
from services.reconfiguration import ReconfigurationPlanner


@pytest.fixture
def planner():
    return ReconfigurationPlanner()


def test_no_reconfiguration_needed(planner):
    request = ReconfigurationRequest(
        arrival_rate=500,
        worker_capacity=1000,
        target_utilization=0.8,
        current_workers=2,
        consumer_lag=0,
        partition_rates={0: 120, 1: 130, 2: 125, 3: 125},
    )

    response = planner.analyze(request)

    assert response.current_workers == 2
    assert response.target_workers == 2
    assert response.actions == []


def test_scale_up_workers(planner):
    request = ReconfigurationRequest(
        arrival_rate=2000,
        worker_capacity=1000,
        target_utilization=0.8,
        current_workers=2,
        consumer_lag=0,
        partition_rates={0: 500, 1: 500, 2: 500, 3: 500},
    )

    response = planner.analyze(request)

    assert response.current_workers == 2
    assert response.target_workers == 3
    assert len(response.actions) == 1

    action = response.actions[0]
    assert isinstance(action, ScaleWorkersAction)
    assert action.type == "SCALE_WORKERS"
    assert action.from_workers == 2
    assert action.to_workers == 3


def test_scale_up_with_consumer_lag(planner):
    request = ReconfigurationRequest(
        arrival_rate=2000,
        worker_capacity=1000,
        target_utilization=0.8,
        current_workers=2,
        consumer_lag=500,
        partition_rates={0: 500, 1: 500, 2: 500, 3: 500},
    )

    response = planner.analyze(request)

    assert response.target_workers == 3
    assert len(response.actions) == 1
    assert "consumer lag" in response.reason[0].lower()


def test_hot_partition(planner):
    request = ReconfigurationRequest(
        arrival_rate=900,
        worker_capacity=1000,
        target_utilization=0.8,
        current_workers=2,
        consumer_lag=0,
        partition_rates={0: 100, 1: 100, 2: 600, 3: 100},
    )

    response = planner.analyze(request)

    assert response.target_workers == 2
    assert len(response.actions) == 1

    action = response.actions[0]
    assert isinstance(action, InvestigateHotPartitionAction)
    assert action.type == "INVESTIGATE_HOT_PARTITION"
    assert action.partition == 2


def test_scale_up_and_hot_partition(planner):
    request = ReconfigurationRequest(
        arrival_rate=2000,
        worker_capacity=1000,
        target_utilization=0.8,
        current_workers=2,
        consumer_lag=500,
        partition_rates={0: 100, 1: 100, 2: 600, 3: 100},
    )

    response = planner.analyze(request)

    assert response.current_workers == 2
    assert response.target_workers == 3
    assert len(response.actions) == 2
    assert any(
        isinstance(action, ScaleWorkersAction)
        for action in response.actions
    )
    assert any(
        isinstance(action, InvestigateHotPartitionAction)
        and action.partition == 2
        for action in response.actions
    )

def test_capacity_exceeds_partition_limit(planner):
    request = ReconfigurationRequest(
        arrival_rate=1000,
        worker_capacity=100,
        target_utilization=0.8,
        current_workers=4,
        consumer_lag=100,
        partition_rates={
            0: 250,
            1: 250,
            2: 250,
            3: 250,
        },
    )

    response = planner.analyze(request)

    assert response.target_workers == 4
    assert response.actions == []

    assert any(
        "exceeds the parallelism" in reason
        for reason in response.reason
    )

    assert not any(
        "capacity is sufficient" in reason
        for reason in response.reason
    )