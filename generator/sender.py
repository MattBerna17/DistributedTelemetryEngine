# This module handles HTTP communication between the Event Generator and the ingestion API.

from dataclasses import dataclass
from time import perf_counter

import httpx

from config import (
    API_URL,
    HTTP_TIMEOUT,
)


@dataclass
class SendResult:
    """
    Represents the result of a single HTTP event submission.
    """

    success: bool
    status_code: int | None
    latency_ms: float
    error: str | None


def create_http_client() -> httpx.AsyncClient:
    """
    Creates the asynchronous HTTP client used by the Event Generator.
    """

    return httpx.AsyncClient(
        timeout=HTTP_TIMEOUT,
    )


async def send_event(
    client: httpx.AsyncClient,
    event: dict,
) -> SendResult:
    """
    Sends a telemetry event to the ingestion API.

    Returns information about:
        - whether the request succeeded
        - the HTTP status code
        - the request latency
        - any error that occurred
    """

    start_time = perf_counter()

    try:
        # Send the event to the API.
        response = await client.post(
            API_URL,
            json=event,
        )

        # Calculate the latency of the response
        latency_ms = (
            perf_counter() - start_time
        ) * 1000

        # Check the code returned by the API.
        success = 200 <= response.status_code < 300

        if success:
            return SendResult(
                success=True,
                status_code=response.status_code,
                latency_ms=latency_ms,
                error=None,
            )

        return SendResult(
            success=False,
            status_code=response.status_code,
            latency_ms=latency_ms,
            error=f"HTTP {response.status_code}",
        )

    # If the API does not respond within HTTP_TIMEOUT.
    except httpx.TimeoutException as exc:
        latency_ms = (
            perf_counter() - start_time
        ) * 1000

        return SendResult(
            success=False,
            status_code=None,
            latency_ms=latency_ms,
            error=f"Timeout: {exc}",
        )

    # If there is another type of problem.
    except httpx.RequestError as exc:
        latency_ms = (
            perf_counter() - start_time
        ) * 1000

        return SendResult(
            success=False,
            status_code=None,
            latency_ms=latency_ms,
            error=f"Request error: {exc}",
        )