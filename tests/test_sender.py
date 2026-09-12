import json

import httpx
import pytest

from generator.sender import SendResult, send_event


TEST_EVENT = {
    "event_id": "11111111-1111-1111-1111-111111111111",
    "source_id": "sensor-1",
    "event_time": "2026-08-26T10:00:00+00:00",
    "metric_name": "temperature",
    "value": 23.5,
    "schema_version": 1,
}


@pytest.mark.anyio
async def test_success_response():
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            status_code=202,
            json={"status": "accepted"},
        )
    )

    async with httpx.AsyncClient(transport=transport) as client:
        result = await send_event(client, TEST_EVENT)

    assert isinstance(result, SendResult)
    assert result.success is True
    assert result.status_code == 202
    assert result.error is None
    assert result.latency_ms >= 0


@pytest.mark.anyio
async def test_client_error_response():
    transport = httpx.MockTransport(
        lambda request: httpx.Response(status_code=400)
    )

    async with httpx.AsyncClient(transport=transport) as client:
        result = await send_event(client, TEST_EVENT)

    assert result.success is False
    assert result.status_code == 400
    assert result.error == "HTTP 400"
    assert result.latency_ms >= 0


@pytest.mark.anyio
async def test_server_error_response():
    transport = httpx.MockTransport(
        lambda request: httpx.Response(status_code=500)
    )

    async with httpx.AsyncClient(transport=transport) as client:
        result = await send_event(client, TEST_EVENT)

    assert result.success is False
    assert result.status_code == 500
    assert result.error == "HTTP 500"
    assert result.latency_ms >= 0


@pytest.mark.anyio
async def test_timeout():
    def handler(request):
        raise httpx.ReadTimeout(
            "Simulated timeout",
            request=request,
        )

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(transport=transport) as client:
        result = await send_event(client, TEST_EVENT)

    assert result.success is False
    assert result.status_code is None
    assert result.error is not None
    assert result.error.startswith("Timeout:")
    assert result.latency_ms >= 0


@pytest.mark.anyio
async def test_request_error():
    def handler(request):
        raise httpx.ConnectError(
            "Simulated connection error",
            request=request,
        )

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(transport=transport) as client:
        result = await send_event(client, TEST_EVENT)

    assert result.success is False
    assert result.status_code is None
    assert result.error is not None
    assert result.error.startswith("Request error:")
    assert result.latency_ms >= 0


@pytest.mark.anyio
async def test_event_is_sent_as_json():
    received = {}

    def handler(request):
        received["body"] = request.read()
        return httpx.Response(status_code=200)

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(transport=transport) as client:
        result = await send_event(client, TEST_EVENT)

    assert result.success is True
    assert json.loads(received["body"]) == TEST_EVENT
