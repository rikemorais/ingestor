"""Kafka source: consumes messages and yields them as records."""

from collections.abc import Iterator

from confluent_kafka import Consumer, KafkaError, KafkaException

from ingestor.ports.record import Record


class KafkaSource:
    """A streaming Source backed by a confluent-kafka Consumer."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topics: list[str] | None = None,
        group: str = "ingestor",
        from_beginning: bool = True,
        max_empty_polls: int = 10,
        timeout: float = 1.0,
    ):
        """Create the consumer.

        Args:
            bootstrap_servers: Comma-separated broker addresses.
            topics: Topics to subscribe to.
            group: Consumer group id (controls committed offsets).
            from_beginning: Read from the start of the topic when the group has
                no saved offset; otherwise read only new messages.
            max_empty_polls: Stop after this many consecutive empty polls, so
                demos and batch-style runs terminate.
            timeout: Seconds to wait on each poll.
        """
        self._topics = topics or []
        self._max_empty_polls = max_empty_polls
        self._timeout = timeout
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group,
                "auto.offset.reset": "earliest" if from_beginning else "latest",
            }
        )

    def read(self) -> Iterator[Record]:
        """Subscribe and yield records until the topic goes quiet.

        Yields:
            Each consumed message translated into a Record.

        Raises:
            KafkaException: If the broker returns a non-EOF error.
        """
        self._consumer.subscribe(self._topics)
        empty = 0
        while empty < self._max_empty_polls:
            message = self._consumer.poll(self._timeout)
            if message is None:
                empty += 1
                continue

            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    empty += 1
                    continue
                raise KafkaException(error)

            empty = 0
            value = message.value()
            key = message.key()
            yield Record(
                value=value.decode("utf-8") if value is not None else "",
                key=key.decode("utf-8") if key is not None else None,
                origin=message.topic(),
            )

    def close(self) -> None:
        """Close the underlying consumer."""
        self._consumer.close()
