from unittest.mock import patch

from processor.consumer import process_message


class FakeKafkaMessage:
    """
    Minimal fake Kafka message used to test process_message()
    without a real Kafka broker.
    """

    def __init__(
        self,
        value: bytes,
        partition: int = 0,
        offset: int = 0,
    ):
        self._value = value
        self._partition = partition
        self._offset = offset

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


def test_valid_event():
    print("\n--- TEST 1: valid event ---")

    message = FakeKafkaMessage(
        value=b"""
        {
            "event_id": "11111111-1111-1111-1111-111111111111",
            "source_id": "sensor-17",
            "event_time": "2026-08-23T10:15:37.521+00:00",
            "metric_name": "temperature",
            "value": 21.7,
            "schema_version": 1
        }
        """,
        partition=2,
        offset=100,
    )

    with patch(
        "processor.consumer.process_event",
        return_value=True,
    ) as mocked_process_event:

        process_message(message)

        # The repository must be called exactly once.
        mocked_process_event.assert_called_once()

        arguments = mocked_process_event.call_args.kwargs

        assert arguments["source_id"] == "sensor-17"
        assert arguments["metric_name"] == "temperature"
        assert arguments["value"] == 21.7

        assert arguments["kafka_partition"] == 2
        assert arguments["kafka_offset"] == 100

        # Verify the 1-minute window.
        assert arguments["window_start"].second == 0
        assert arguments["window_start"].microsecond == 0
        assert arguments["window_start"].minute == 15

    print("TEST 1 PASSED")


def test_duplicate_event():
    print("\n--- TEST 2: duplicate event ---")

    message = FakeKafkaMessage(
        value=b"""
        {
            "event_id": "22222222-2222-2222-2222-222222222222",
            "source_id": "sensor-20",
            "event_time": "2026-08-23T11:05:42+00:00",
            "metric_name": "humidity",
            "value": 60.0,
            "schema_version": 1
        }
        """,
        partition=1,
        offset=200,
    )

    # Returning False simulates repository.process_event()
    # detecting an already processed event.
    with patch(
        "processor.consumer.process_event",
        return_value=False,
    ) as mocked_process_event:

        process_message(message)

        mocked_process_event.assert_called_once()

    print("TEST 2 PASSED")


def test_invalid_event():
    print("\n--- TEST 3: invalid event ---")

    message = FakeKafkaMessage(
        value=b"""
        {
            "source_id": "sensor-invalid",
            "value": "not-a-number"
        }
        """,
        partition=0,
        offset=300,
    )

    with patch(
        "processor.consumer.process_event"
    ) as mocked_process_event, patch(
        "processor.consumer.send_to_dlq"
    ) as mocked_send_to_dlq:

        result = process_message(message)

        # Invalid messages must not reach PostgreSQL.
        mocked_process_event.assert_not_called()

        # The invalid message must be sent to the DLQ.
        mocked_send_to_dlq.assert_called_once_with(message)

        # The message is considered handled because
        # it was successfully sent to the DLQ.
        assert result is True

    print("TEST 3 PASSED")


def main():
    print("\n===================================")
    print("PROCESSOR MANUAL TEST")
    print("===================================")

    test_valid_event()
    test_duplicate_event()
    test_invalid_event()

    print("\n===================================")
    print("ALL PROCESSOR TESTS PASSED")
    print("===================================")


if __name__ == "__main__":
    main()