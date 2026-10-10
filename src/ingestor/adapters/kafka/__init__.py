"""Kafka adapter: all Kafka-specific code lives here."""

from ingestor.adapters.kafka.connection import KafkaConnection, KafkaConnectionInfo
from ingestor.adapters.kafka.info import KafkaInfoCluster
from ingestor.adapters.kafka.sink import KafkaSink
from ingestor.adapters.kafka.source import KafkaSource

__all__ = [
    "KafkaConnection",
    "KafkaConnectionInfo",
    "KafkaSource",
    "KafkaSink",
    "KafkaInfoCluster",
]
