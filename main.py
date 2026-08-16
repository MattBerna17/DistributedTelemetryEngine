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
    """
    app.state.kafka_producer = KafkaProducerService(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"], topic=os.environ["KAFKA_TOPIC"]) # take bootstrap server and topic name from environment
    yield
    app.state.kafka_producer.producer.flush()

app = FastAPI(lifespan=lifespan)

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