import asyncio

from metrics.collector import MetricsCollector


async def main():
    collector = MetricsCollector()

    print(
        "Collecting system metrics..."
    )

    metrics = await collector.collect()

    print("\nSystem Metrics")
    print("------------------------------")
    print(
        f"Arrival rate: "
        f"{metrics.arrival_rate:.2f} events/s"
    )
    print(
        f"Consumer lag: "
        f"{metrics.consumer_lag}"
    )
    print(
        f"Current workers: "
        f"{metrics.current_workers}"
    )
    print(
        f"Partition rates: "
        f"{metrics.partition_rates}"
    )


if __name__ == "__main__":
    asyncio.run(main())