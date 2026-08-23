<<<<<<< HEAD
"""
File with the API endpoints defined in it
"""

from datetime import datetime
from fastapi import FastAPI, Depends, status, Request
from database.repository import get_aggregates_by_source_metric_and_window
from common.models import TelemetryEvent, EventAcceptedResponse, AggregateResponse, ReconfigurationResponse, ReconfigurationRequest
from services.kafka import KafkaProducerService
from services.reconfiguration import ReconfigurationPlanner
from contextlib import asynccontextmanager
import os

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Initialize Kafka producer
=======
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends, status, Request
from common.models import TelemetryEvent, EventAcceptedResponse, AggregateResponse, ReconfigurationResponse, ReconfigurationRequest
from services.kafka import KafkaProducerService
from contextlib import asynccontextmanager
import os

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Define Kafka producer for the topic
>>>>>>> b9876c21813baf38b27063572ed7540cba0fa9d8
    """
    app.state.kafka_producer = KafkaProducerService(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"], topic=os.environ["KAFKA_TOPIC"]) # take bootstrap server and topic name from environment
    yield
    app.state.kafka_producer.producer.flush()

app = FastAPI(lifespan=lifespan)
<<<<<<< HEAD
planner = ReconfigurationPlanner()
=======
>>>>>>> b9876c21813baf38b27063572ed7540cba0fa9d8

def get_kafka_producer(request: Request):
    """
    Function to get the kafka producer from the request
    """
    return request.app.state.kafka_producer

@app.post(
    "/events",
    response_model=EventAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED
)
async def publish_event(event: TelemetryEvent, producer: KafkaProducerService = Depends(get_kafka_producer)):
    """
    Function to publish event to the Kafka queue
    """
    producer.publish_event(event)
    return EventAcceptedResponse(event_id=event.event_id, status="accepted")
<<<<<<< HEAD

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

=======

@app.get(
    "/aggregates",
    response_model=AggregateResponse
)
async def get_aggregates(
    source_id: str,
    metric_name: str,
    window_start: datetime,
    window_end: datetime
):
    # implement retrival of data from the postgresql data
    return AggregateResponse(
        source_id=source_id,
        metric_name=metric_name,
        window_start=window_start,
        count=10,
        average=23.5,
        minimum=20.0,
        maximum=27.0
    )
>>>>>>> b9876c21813baf38b27063572ed7540cba0fa9d8

@app.post(
    "/reconfiguration",
    response_model=ReconfigurationResponse
)
async def reconfigurate(req: ReconfigurationRequest):
    return ReconfigurationResponse()

@app.get("/health")
async def get_health():
    return {"Kafka reachable": True, "PosgreSQL reachable": True}


@app.get("/")
async def root():
    print("hello")
    return {"status": "ok"}