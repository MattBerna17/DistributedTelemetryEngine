from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

class TelemetryEvent(BaseModel):
    event_id: UUID
    source_id: str = Field(min_length=1)
    event_time: datetime
    metric_name: str = Field(min_length=1)
    value: float
    schema_version: Literal[1]


class EventAcceptedResponse(BaseModel):
    event_id: UUID
    status: Literal["accepted"]


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
    current_workers: int = Field(gt=0)
    consumer_lag: int = Field(ge=0)
    partition_rates: dict[int, float]


class ScaleWorkersAction(BaseModel):
    type: Literal["SCALE_WORKERS"]
    from_workers: int = Field(gt=0, alias="from")
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