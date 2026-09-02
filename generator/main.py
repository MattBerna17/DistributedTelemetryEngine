# This module orchestrates the Event Generator.

import argparse
import asyncio
import math
import random
from dataclasses import dataclass, field
from time import perf_counter

from config import (
    DEFAULT_DISTRIBUTION,
    DEFAULT_DUPLICATE_RATE,
    DEFAULT_DURATION,
    DEFAULT_NUM_SOURCES,
    DEFAULT_RATE,
    METRICS,
    SUPPORTED_DISTRIBUTIONS,
)
from event_factory import (
    build_source_ids,
    choose_metric,
    choose_source,
    create_event,
)
from sender import (
    SendResult,
    create_http_client,
    send_event,
)


@dataclass
class GeneratorStats:
    """
    Stores statistics collected during a generator run.
    """

    requests_sent: int = 0
    accepted: int = 0
    failed: int = 0
    duplicates_generated: int = 0
    latencies_ms: list[float] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments used to configure
    the Event Generator.
    """

    parser = argparse.ArgumentParser(
        description="AdaptiveMetrics telemetry event generator"
    )

    parser.add_argument(
        "--sources",
        type=int,
        default=DEFAULT_NUM_SOURCES,
        help="Number of simulated telemetry sources",
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=DEFAULT_RATE,
        help="Target number of HTTP requests per second",
    )

    parser.add_argument(
        "--duration",
        type=float,
        default=DEFAULT_DURATION,
        help="Generator duration in seconds",
    )

    parser.add_argument(
        "--duplicate-rate",
        type=float,
        default=DEFAULT_DUPLICATE_RATE,
        help="Probability of generating a duplicate event (0.0 - 1.0)",
    )

    parser.add_argument(
        "--distribution",
        choices=SUPPORTED_DISTRIBUTIONS,
        default=DEFAULT_DISTRIBUTION,
        help="Source selection distribution",
    )

    parser.add_argument(
        "--metrics",
        nargs="+",
        choices=METRICS,
        default=METRICS,
        help="Telemetry metrics to generate",
    )

    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    """
    Validates generator parameters.
    """

    if args.sources <= 0:
        raise ValueError("'sources' must be greater than 0")

    if args.rate <= 0:
        raise ValueError("'rate' must be greater than 0")

    if args.duration <= 0:
        raise ValueError("'duration' must be greater than 0")

    if not 0.0 <= args.duplicate_rate <= 1.0:
        raise ValueError("'duplicate-rate' must be between 0.0 and 1.0")


def calculate_p95(
    latencies_ms: list[float],
) -> float:
    """
    Calculates the 95th percentile latency.
    """

    if not latencies_ms:
        return 0.0

    ordered = sorted(latencies_ms)

    index = math.ceil(0.95 * len(ordered)) - 1

    return ordered[index]


async def send_and_record(
    client,
    event: dict,
    stats: GeneratorStats,
) -> None:
    """
    Sends one event and updates generator statistics.
    """

    result: SendResult = await send_event(
        client,
        event,
    )

    stats.requests_sent += 1
    stats.latencies_ms.append(result.latency_ms)

    if result.success:
        stats.accepted += 1
    else:
        stats.failed += 1

        print(
            f"Request failed: " f"status={result.status_code}, " f"error={result.error}"
        )


def select_event(
    source_ids: list[str],
    metrics: list[str],
    distribution: str,
    duplicate_rate: float,
    event_history: list[dict],
    stats: GeneratorStats,
) -> dict:
    """
    Creates a new telemetry event or selects an existing
    one to simulate a duplicate/retry.
    """

    # event_history must contain at least one event,
    # otherwise there is nothing to duplicate.

    # Generate a duplicate if the random value
    # is lower than duplicate_rate.
    should_duplicate = event_history and random.random() < duplicate_rate

    if should_duplicate:
        stats.duplicates_generated += 1

        return random.choice(event_history)

    source_id = choose_source(
        source_ids,
        distribution,
    )

    metric_name = choose_metric(
        metrics,
    )

    event = create_event(
        source_id,
        metric_name,
    )

    event_history.append(event)

    return event


async def run_generator(
    args: argparse.Namespace,
) -> None:
    """
    Runs the Event Generator according to the requested
    rate, duration, source distribution and duplicate rate.
    """

    source_ids = build_source_ids(args.sources)

    stats = GeneratorStats()

    # Stores original events so that the generator can
    # resend them to simulate retries and duplicates.
    event_history: list[dict] = []

    # Contains the asynchronous HTTP requests that have
    # been initiated but have not yet completed.
    pending_tasks: set[asyncio.Task] = set()

    # Calculates the time that must elapse between the
    # generation of two events.
    interval = 1.0 / args.rate

    # Takes the asyncio event loop, which is executing
    # the generator.
    loop = asyncio.get_running_loop()

    start_time = perf_counter()
    end_time = start_time + args.duration

    # Represents the moment when the next event must
    # be generated.
    next_event_time = loop.time()

    print("\n===================================")
    print("EVENT GENERATOR STARTED")
    print("===================================")

    print(f"Sources:        {args.sources}")
    print(f"Rate:           {args.rate} req/s")
    print(f"Duration:       {args.duration} s")
    print(
        f"Duplicate rate: " f"{args.duplicate_rate:.2%}"
    )  # .2% is needed to format the value as a percentage.
    print(f"Distribution:   {args.distribution}")
    print(f"Metrics:        " f"{', '.join(args.metrics)}")

    # Creates the asynchronous HTTP client and keeps it open for the duration of the test.
    async with create_http_client() as client:

        while perf_counter() < end_time:

            now = loop.time()

            if now < next_event_time:
                await asyncio.sleep(next_event_time - now)

            # Duration may have expired while sleeping.
            if perf_counter() >= end_time:
                break

            event = select_event(
                source_ids=source_ids,
                metrics=args.metrics,
                distribution=args.distribution,
                duplicate_rate=args.duplicate_rate,
                event_history=event_history,
                stats=stats,
            )

            # The HTTP request runs concurrently, allowing
            # the generator to sustain higher event rates.
            task = asyncio.create_task(
                send_and_record(
                    client,
                    event,
                    stats,
                )
            )

            pending_tasks.add(task)

            # When the task finishes, it automatically removes it from pending_tasks.
            task.add_done_callback(pending_tasks.discard)

            # Calculate when the next event is due to start.
            next_event_time += interval

        # Wait for all HTTP requests already started
        # before closing the HTTP client.
        if pending_tasks:
            await asyncio.gather(*pending_tasks)

    actual_duration = perf_counter() - start_time

    print_summary(
        stats,
        actual_duration,
        args.rate,
    )


def print_summary(
    stats: GeneratorStats,
    duration: float,
    target_rate: float,
) -> None:
    """
    Prints a summary of the generator execution.
    """

    if stats.latencies_ms:
        average_latency = sum(stats.latencies_ms) / len(stats.latencies_ms)
    else:
        average_latency = 0.0

    p95_latency = calculate_p95(stats.latencies_ms)

    if duration > 0:
        actual_rate = stats.requests_sent / duration
    else:
        actual_rate = 0.0

    print("\n===================================")
    print("GENERATOR SUMMARY")
    print("===================================")

    print(f"Duration:             " f"{duration:.2f} s") # .2f is needed to display two decimals.

    print(f"Requests sent:        " f"{stats.requests_sent}")

    print(f"Accepted:             " f"{stats.accepted}")

    print(f"Failed:               " f"{stats.failed}")

    print(f"Duplicates generated: " f"{stats.duplicates_generated}")

    print(f"Target rate:          " f"{target_rate:.2f} req/s")

    print(f"Actual rate:          " f"{actual_rate:.2f} req/s")

    print(f"Average latency:      " f"{average_latency:.2f} ms")

    print(f"p95 latency:          " f"{p95_latency:.2f} ms")


async def main() -> None:
    args = parse_args()

    try:
        validate_args(args)

        await run_generator(args)

    except ValueError as exc:
        print(f"Invalid generator configuration: " f"{exc}")


if __name__ == "__main__":
    asyncio.run(main())
