import asyncio

import httpx

from generator.sender import (
    SendResult,
    send_event,
)


TEST_EVENT = {
    "event_id": "11111111-1111-1111-1111-111111111111",
    "source_id": "sensor-1",
    "event_time": "2026-08-26T10:00:00+00:00",
    "metric_name": "temperature",
    "value": 23.5,
    "schema_version": 1,
}


async def test_success_response():
    print("\n--- TEST 1: successful HTTP response ---")

    def handler(request):
        return httpx.Response(
            status_code=202,
            json={"status": "accepted"},
        )

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(
        transport=transport,
    ) as client:

        result = await send_event(
            client,
            TEST_EVENT,
        )

    assert isinstance(result, SendResult)
    assert result.success is True
    assert result.status_code == 202
    assert result.error is None
    assert result.latency_ms >= 0

    print("TEST 1 PASSED")


async def test_client_error_response():
    print("\n--- TEST 2: HTTP 400 response ---")

    def handler(request):
        return httpx.Response(
            status_code=400,
        )

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(
        transport=transport,
    ) as client:

        result = await send_event(
            client,
            TEST_EVENT,
        )

    assert result.success is False
    assert result.status_code == 400
    assert result.error == "HTTP 400"
    assert result.latency_ms >= 0

    print("TEST 2 PASSED")


async def test_server_error_response():
    print("\n--- TEST 3: HTTP 500 response ---")

    def handler(request):
        return httpx.Response(
            status_code=500,
        )

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(
        transport=transport,
    ) as client:

        result = await send_event(
            client,
            TEST_EVENT,
        )

    assert result.success is False
    assert result.status_code == 500
    assert result.error == "HTTP 500"
    assert result.latency_ms >= 0

    print("TEST 3 PASSED")


async def test_timeout():
    print("\n--- TEST 4: timeout ---")

    def handler(request):
        raise httpx.ReadTimeout(
            "Simulated timeout",
            request=request,
        )

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(
        transport=transport,
    ) as client:

        result = await send_event(
            client,
            TEST_EVENT,
        )

    assert result.success is False
    assert result.status_code is None
    assert result.error is not None
    assert result.error.startswith("Timeout:")
    assert result.latency_ms >= 0

    print("TEST 4 PASSED")


async def test_request_error():
    print("\n--- TEST 5: request error ---")

    def handler(request):
        raise httpx.ConnectError(
            "Simulated connection error",
            request=request,
        )

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(
        transport=transport,
    ) as client:

        result = await send_event(
            client,
            TEST_EVENT,
        )

    assert result.success is False
    assert result.status_code is None
    assert result.error is not None
    assert result.error.startswith("Request error:")
    assert result.latency_ms >= 0

    print("TEST 5 PASSED")


async def test_event_is_sent_as_json():
    print("\n--- TEST 6: event is sent as JSON ---")

    received_body = None

    def handler(request):
        nonlocal received_body

        received_body = request.read()

        return httpx.Response(
            status_code=200,
        )

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(
        transport=transport,
    ) as client:

        result = await send_event(
            client,
            TEST_EVENT,
        )

    assert result.success is True

    assert received_body is not None

    body_text = received_body.decode("utf-8")

    assert "sensor-1" in body_text
    assert "temperature" in body_text
    assert "23.5" in body_text

    print("TEST 6 PASSED")


async def main():
    print("\n===================================")
    print("SENDER MANUAL TEST")
    print("===================================")

    await test_success_response()
    await test_client_error_response()
    await test_server_error_response()
    await test_timeout()
    await test_request_error()
    await test_event_is_sent_as_json()

    print("\n===================================")
    print("ALL SENDER TESTS PASSED")
    print("===================================")


if __name__ == "__main__":
    asyncio.run(main())