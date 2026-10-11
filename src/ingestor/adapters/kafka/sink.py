"""Kafka sink: writes records by producing to a topic."""

from confluent_kafka import Producer

from ingestor.ports.record import Record


class KafkaSink:
    """An output Sink backed by a confluent-kafka Producer."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        default_topic: str | None = None,
    ):
        """Create the producer.

        Args:
            bootstrap_servers: Comma-separated broker addresses.
            default_topic: Topic used when a record has no ``origin``.
        """
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})
        self._default_topic = default_topic

    def write(self, record: Record) -> None:
        """Produce a record to Kafka.

        Args:
            record: The record to produce. Its ``origin`` is used as the topic,
                falling back to ``default_topic``.

        Raises:
            ValueError: If no destination topic can be determined.
        """
        topic = record.origin or self._default_topic
        if topic is None:
            raise ValueError("No destination topic: set record.origin or default_topic.")
        self._producer.produce(topic, value=record.value, key=record.key)
        self._producer.poll(0)

    def close(self, timeout: float = 10.0) -> None:
        """Flush pending messages to the broker.

        Args:
            timeout: Maximum seconds to wait for delivery before giving up, so a
                dead broker cannot block forever.
        """
        self._producer.flush(timeout)
