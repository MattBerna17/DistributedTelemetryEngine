# Pytest suite - latest merge

This suite is aligned with the code in `DistributedTelemetryEngine-main-2.zip`.

## Main differences from the previous cleaned suite

- `POST /events` currently returns HTTP `200`, because the endpoint in `main.py` does not set an explicit `status_code=202`.
- Repository aggregate dictionaries currently expose `count`, `minimum`, `maximum` and `average`, but **not** `value_sum`. Tests of the repository and Kafka deduplication therefore no longer expect `value_sum` in the returned dictionary.
- `value_sum` is still tested directly at the PostgreSQL schema/SQL level in `test_database.py`.
- `test_metrics_snapshot.py` now checks the exact `WORKER_CAPACITY` and `TARGET_UTILIZATION` values imported from the current `metrics/config.py`.
- The API tests include the current `/metrics` and `/reconfiguration/auto` endpoints.
- Manual scripts have been converted to normal pytest tests and `main()`/`print()`-driven test execution has been removed.

## Recommended execution

The most reliable way to run the suite is from the API container, because it already has all project dependencies and the Docker-internal Kafka/PostgreSQL addresses.

Start the environment from the project root:

```bash
docker compose up -d --build
```

Run unit tests only:

```bash
docker compose exec api pytest -m "not integration" -v
```

Run the complete suite:

```bash
docker compose exec api pytest -v
```

Run only integration tests:

```bash
docker compose exec api pytest -m integration -v
```

Run only Kafka integration tests:

```bash
docker compose exec api pytest -m kafka -v
```

Run only PostgreSQL integration tests:

```bash
docker compose exec api pytest -m postgres -v
```

## Integration-test requirements

For the full suite, Kafka, PostgreSQL and the Processor containers must be running. In particular:

- `test_kafka_dedup.py` requires Kafka, PostgreSQL and at least one running Processor;
- `test_kafka_dlq.py` requires Kafka and a running Processor;
- `test_metrics_integration.py` requires Kafka and reads the current Processor consumer group;
- `test_database.py` and `test_repository.py` require PostgreSQL.

## Note about HTTP 202

The old API test expected `202 Accepted` for `POST /events`, but the latest `main.py` currently returns FastAPI's default `200 OK`. The updated test follows the current implementation. If the API is later changed to explicitly return `202`, update that single assertion accordingly.
