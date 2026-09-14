from datetime import datetime
from typing import Literal
from uuid import UUID
from pydantic import BaseModel, Field, ConfigDict


class TelemetryEvent(BaseModel):
    """
    Model of a single telemetry event in the system
    """
    model_config = ConfigDict(extra="forbid")
    event_id: UUID
    source_id: str = Field(min_length=1)
    event_time: datetime
    metric_name: str = Field(min_length=1)
    value: float
    schema_version: Literal[1]



class EventResponse(BaseModel):
    """
    Model to define the response the API returns when an event is sent
    """
    event_id: UUID
    status: Literal["accepted", "rejected"]

class ErrorResponse(BaseModel):
    """
    Model to define an error response
    """
    error_code: str
    message: str
    retryable: bool


class AggregateResponse(BaseModel):
    """
    Model to define the response returned from the system when requesting the aggregates
    """
    source_id: str
    metric_name: str
    window_start: datetime
    count: int
    average: float
    minimum: float
    maximum: float


class ReconfigurationRequest(BaseModel):
    """
    Model to define the request of a reconfiguration
    """
    arrival_rate: float = Field(ge=0)
    worker_capacity: float = Field(gt=0)
    target_utilization: float = Field(gt=0, le=1)
    current_workers: int = Field(ge=0) # can be 0 if all processor instances are temporarily down
    consumer_lag: int = Field(ge=0)
    partition_rates: dict[int, float]


class ScaleWorkersAction(BaseModel):
    """
    Model to define the action of scaling the workers in the architecture
    """
    type: Literal["SCALE_WORKERS"]
    from_workers: int = Field(ge=0, alias="from") # scaling may start from 0 active workers
    to_workers: int = Field(gt=0, alias="to") # a scale-up target must contain at least one worker


class InvestigateHotPartitionAction(BaseModel):
    """
    Model to define the hot partition investigation action for the reconfiguration
    """
    type: Literal["INVESTIGATE_HOT_PARTITION"]
    partition: int = Field(ge=0)


class ReconfigurationResponse(BaseModel):
    """
    Model to define the response of the Reconfiguration Planner
    """
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
    current_workers: int = Field(ge=0) # there may temporarily be no active processor