import math
from common.models import (
    ReconfigurationRequest,
    ReconfigurationResponse,
    ScaleWorkersAction,
    InvestigateHotPartitionAction,
)

class ReconfigurationPlanner:
    def __init__(self, hot_partition_factor=1.5):
        self.HOT_PARTITION_FACTOR = hot_partition_factor

    def analyze(self, request: ReconfigurationRequest) -> ReconfigurationResponse:
        """
        Function to analyze a reconfiguration request 
        """
        reasons = []
        actions = []

        effective_capacity = (
            request.worker_capacity
            * request.target_utilization
        )

        required_workers = math.ceil(
            request.arrival_rate / effective_capacity
        )

        partition_count = len(request.partition_rates)

        if partition_count > 0:
            useful_required_workers = min(
                required_workers,
                partition_count,
            )
        else:
            useful_required_workers = required_workers

        target_workers = max(
            request.current_workers,
            useful_required_workers,
        )

        if (
            partition_count > 0
            and required_workers > partition_count
        ):
            reasons.append(
                "The required processing capacity exceeds the "
                "parallelism available with the current Kafka partitions"
            )

        # in case more workers are needed than the ones currently assigned...
        if target_workers > request.current_workers:
            # need to scale workers
            actions.append(
                ScaleWorkersAction.model_validate({
                    "type": "SCALE_WORKERS",
                    "from": request.current_workers,
                    "to": target_workers
                }) # model_validate is necessary since there are aliases for from_worker and to_worker
            )
            # add consumer lag in case it's present
            if request.consumer_lag > 0:
                reasons.append("The current processing capacity is insufficient and consumer lag is present")
            else:
                reasons.append("The current processing capacity is insufficient for the observed arrival rate")

        # check for possible hot partitions
        if request.partition_rates:
            average_rate = sum(request.partition_rates.values()) / len(request.partition_rates) # compute mean rate of requests for each partition
            hot_partition_threshold = average_rate * self.HOT_PARTITION_FACTOR # threshold indicating whether a partition is overloaded or not

            for partition, rate in request.partition_rates.items():
                # in case a partition has a rate of incoming traffic bigger than the threshold, add the investigation of the partitions to the actions to perform
                if rate > hot_partition_threshold:
                    actions.append(
                        InvestigateHotPartitionAction(
                            type="INVESTIGATE_HOT_PARTITION",
                            partition=partition
                        )
                    )
                    reasons.append(f"Partition {partition} has an unusually high arrival rate")

        # in case no action is needed and no problems occurred (current capacity is ok and no hot partition present)
        if not actions and not reasons:
            reasons.append("The current worker capacity is sufficient and no hot partition was detected")

        return ReconfigurationResponse(
            reason=reasons,
            current_workers=request.current_workers,
            target_workers=target_workers,
            actions=actions
        )