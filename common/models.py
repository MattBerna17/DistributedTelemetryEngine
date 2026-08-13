from datetime import datetime
from uuid import UUID

from pydantic import BaseModel
from typing import Literal # Used for ["accepted"], in order to avoid the use of an arbitrary string

class TelemetryEvent(BaseModel):
    event_id: UUID
    source_id: str
    event_time: datetime
    metric_name: str
    value: float
    schema_version: int

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
    arrival_rate: float
    worker_capacity: float
    target_utilization: float
    current_workers: int
    consumer_lag: int
    partition_rates: dict[int, float]

