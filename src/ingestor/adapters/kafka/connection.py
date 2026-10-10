"""Kafka connection: builds KafkaSource and KafkaSink.

The only place that joins the Kafka config, the concrete adapters, the
AdminClient and the metadata translation. The rest of the system sees only
the Connection interface.
"""

from dataclasses import dataclass, field

from confluent_kafka.admin import AdminClient

from ingestor.adapters.kafka.info import KafkaInfoCluster
from ingestor.adapters.kafka.sink import KafkaSink
from ingestor.adapters.kafka.source import KafkaSource
from ingestor.ports.connection import Connection
from ingestor.ports.sink import Sink
from ingestor.ports.source import Source


@dataclass(frozen=True)
class KafkaConnectionInfo:
    """Kafka-specific connection config.

    Attributes:
        brokers: Broker addresses to connect to.
        topics: Topics the source subscribes to; the first is the sink default.
        group: Consumer group id.
        from_beginning: Whether the source reads from the start of the topic.
    """

    brokers: list[str]
    topics: list[str] = field(default_factory=list)
    group: str = "ingestor"
    from_beginning: bool = True


class KafkaConnection(Connection):
    """Builds Kafka adapters from a KafkaConnectionInfo."""

    def __init__(self, info: KafkaConnectionInfo) -> None:
        """Store the config and derive the broker string.

        Args:
            info: The Kafka connection config.
        """
        self._info = info
        self._servers = ",".join(info.brokers)

    def connect(self, timeout: float = 10.0) -> KafkaInfoCluster:
        """Fetch cluster metadata (a Kafka-only extra, not part of the port).

        Args:
            timeout: Seconds to wait for the metadata response.

        Returns:
            A summary of the cluster.
        """
        cluster = AdminClient({"bootstrap.servers": self._servers}).list_topics(timeout=timeout)
        return KafkaInfoCluster.from_cluster(cluster)

    def source(self) -> Source:
        """Build a KafkaSource for the configured topics.

        Returns:
            A streaming Source.
        """
        return KafkaSource(
            bootstrap_servers=self._servers,
            topics=self._info.topics,
            group=self._info.group,
            from_beginning=self._info.from_beginning,
        )

    def sink(self) -> Sink:
        """Build a KafkaSink targeting the first configured topic.

        Returns:
            An output Sink.
        """
        topic = self._info.topics[0] if self._info.topics else None
        return KafkaSink(bootstrap_servers=self._servers, default_topic=topic)
