from fastapi import FastAPI, HTTPException, status
from common.models import TelemetryEvent, EventAcceptedResponse
from services.kafka import KafkaProducerService

app = FastAPI()

@app.post(
    "/events",
    response_model=EventAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED
)
async def get_event(event: TelemetryEvent):
    print(event)

    return EventAcceptedResponse(
        event_id=event.event_id,
        status="accepted"
    )

@app.get("/")
async def root():
    print("hello")
    return {"status": "ok"}

@app.get("/home")
def home():
    print("try")
    return "It works."