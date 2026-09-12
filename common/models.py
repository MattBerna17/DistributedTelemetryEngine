from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, ConfigDict

class TelemetryEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: UUID
    source_id: str = Field(min_length=1)
    event_time: datetime
    metric_name: str = Field(min_length=1)
    value: float
    schema_version: Literal[1]



class EventResponse(BaseModel):
    event_id: UUID
    status: Literal["accepted", "rejected"]

class ErrorResponse(BaseModel):
    error_code: str
    message: str
    retryable: bool


class AggregateResponse(BaseModel):
    source_id: str
    metric_name: str
    window_start: datetime
    count: int
    average: float
    minimum: float
    maximum: float


class ReconfigurationRequest(BaseModel):
    arrival_rate: float = Field(ge=0)
    worker_capacity: float = Field(gt=0)
    target_utilization: float = Field(gt=0, le=1)

    # Can be 0 if all Processor instances are temporarily down.
    current_workers: int = Field(ge=0)

    consumer_lag: int = Field(ge=0)
    partition_rates: dict[int, float]


class ScaleWorkersAction(BaseModel):
    type: Literal["SCALE_WORKERS"]

    # Scaling may start from 0 active workers.
    from_workers: int = Field(ge=0, alias="from")

    # A scale-up target must contain at least one worker.
    to_workers: int = Field(gt=0, alias="to")


class InvestigateHotPartitionAction(BaseModel):
    type: Literal["INVESTIGATE_HOT_PARTITION"]
    partition: int = Field(ge=0)


class ReconfigurationResponse(BaseModel):
    reason: list[str]
    current_workers: int
    target_workers: int
    actions: list[
        ScaleWorkersAction | InvestigateHotPartitionAction
    ]


class SystemMetricsSnapshot(BaseModel):
    """
    Runtime metrics collected from the distributed system.
    """

    arrival_rate: float = Field(ge=0)
    consumer_lag: int = Field(ge=0)
    partition_rates: dict[int, float]

    # There may temporarily be no active Processor.
    current_workers: int = Field(ge=0)