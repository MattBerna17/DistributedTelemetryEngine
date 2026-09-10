# File with the API endpoints defined in it

from datetime import datetime
from fastapi import (
    FastAPI,
    Depends,
    status,
    Request,
    HTTPException,
)

from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from database.repository import (
    get_aggregates_by_source_metric_and_window
)

from common.models import (
    TelemetryEvent,
    EventResponse,
    AggregateResponse,
    ReconfigurationResponse,
    ReconfigurationRequest,
    SystemMetricsSnapshot,
)

from services.kafka import KafkaProducerService
from services.reconfiguration import ReconfigurationPlanner
from services.reconfiguration_service import (
    build_reconfiguration_request,
)

from metrics.collector import MetricsCollector

from contextlib import asynccontextmanager
import os

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Initialize Kafka producer
    """
    app.state.kafka_producer = KafkaProducerService(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"], topic=os.environ["KAFKA_TOPIC"]) # take bootstrap server and topic name from environment
    yield
    app.state.kafka_producer.producer.flush()

app = FastAPI(lifespan=lifespan)
planner = ReconfigurationPlanner()
metrics_collector = MetricsCollector()

def get_kafka_producer(request: Request):
    """
    Function to get the kafka producer from the request
    """
    return request.app.state.kafka_producer

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request,
    exc: RequestValidationError
):
    """
    Function to handle the validation error when inserting new non-valid jsons
    """
    return JSONResponse(
        status_code=422,
        content={
            "event_id": None,
            "status": "rejected"
        }
    )

@app.post(
    "/events",
    response_model=EventResponse,
)
async def publish_event(event: TelemetryEvent, producer: KafkaProducerService = Depends(get_kafka_producer)):
    """
    Function to publish event to the Kafka queue
    """
    accepted = producer.publish_event(event)
    if accepted:
        return EventResponse(event_id=event.event_id, status="accepted")
    else:
        return EventResponse(event_id=event.event_id, status="rejected")

@app.get(
    "/aggregates",
    response_model=list[AggregateResponse]
)
async def get_aggregates(
    source_id: str,
    metric_name: str,
    window_start: datetime,
    window_end: datetime
):
    """
    Return aggregates for a source and metric within a time window
    """
    aggregates = get_aggregates_by_source_metric_and_window(source_id, metric_name, window_start, window_end)
    return aggregates

@app.post(
    "/reconfiguration",
    response_model=ReconfigurationResponse
)
async def reconfigurate(req: ReconfigurationRequest):
    return planner.analyze(req)

@app.get("/health")
async def get_health():
    return {"status": "ok"}


@app.get("/")
async def root():
    print("hello")
    return {"status": "ok"}

# Exposes the current system metrics collected from Kafka.
@app.get(
    "/metrics",
    response_model=SystemMetricsSnapshot,
)
async def get_system_metrics():
    """
    Collect and return the current system metrics.
    """

    try:
        return await metrics_collector.collect()

    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Unable to collect system metrics: {exc}",
        )

# Endpoint to connect Collector and Planner.
@app.get(
    "/reconfiguration/auto",
    response_model=ReconfigurationResponse,
)
async def automatic_reconfiguration():
    """
    Collect system metrics and automatically generate
    a reconfiguration recommendation.
    """

    try:
        metrics = await metrics_collector.collect()

        request = build_reconfiguration_request(
            metrics
        )

        return planner.analyze(
            request
        )

    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Unable to analyze system metrics: {exc}",
        )