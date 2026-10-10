"""Kafka-specific cluster summary (metadata)."""

from dataclasses import dataclass
from typing import Any, Self


@dataclass(frozen=True)
class KafkaInfoCluster:
    """Cluster metadata translated from confluent's ClusterMetadata.

    Attributes:
        brokers: Broker addresses discovered in the cluster.
        topics: Topic names discovered in the cluster, sorted.
    """

    brokers: list[str]
    topics: list[str]

    @property
    def broker_count(self) -> int:
        """Number of brokers in the cluster."""
        return len(self.brokers)

    @classmethod
    def from_cluster(cls, cluster: Any) -> Self:
        """Build a summary from confluent_kafka's ClusterMetadata.

        Args:
            cluster: The ClusterMetadata returned by ``list_topics``.

        Returns:
            A domain-level summary of the cluster.
        """
        return cls(
            brokers=[str(broker) for broker in cluster.brokers.values()],
            topics=sorted(cluster.topics.keys()),
        )
