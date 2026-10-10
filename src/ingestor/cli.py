"""Composition Root: picks the concrete tools and injects them into the logic.

Demo: seeds a few records into Kafka and then ingests from Kafka to the console.
To change source/destination, just build another Source/Sink here - app.py stays.
"""

import os
import sys

from confluent_kafka import KafkaException

from ingestor.adapters.console import ConsoleSink
from ingestor.adapters.kafka import KafkaConnection, KafkaConnectionInfo
from ingestor.app import ingest
from ingestor.ports import Record

TOPIC = "demo-messages"


def main() -> int:
    """Run the Kafka-to-console demo.

    Seeds a few records into the topic, then ingests them to the console.

    Returns:
        Process exit code: ``0`` on success, ``1`` if Kafka cannot be reached.
    """
    server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    connection = KafkaConnection(
        KafkaConnectionInfo(brokers=server.split(","), topics=[TOPIC])
    )
    try:
        info = connection.connect()
        if info.broker_count == 0:
            print(
                "No Kafka brokers reachable. Is Kafka running? "
                "Start it with: docker compose up -d",
                file=sys.stderr,
            )
            return 1
        print(f"Connected to Kafka: {info.broker_count} broker(s).")

        # (demo) seed a few records into the topic
        kafka_sink = connection.sink()
        for i in range(1, 6):
            kafka_sink.write(
                Record(value=f"Hello Kafka #{i}", key=str(i), origin=TOPIC)
            )
        kafka_sink.close()

        # ingest from Kafka to the console
        total = ingest(connection.source(), ConsoleSink(), limit=5)
        print(f"Ingested {total} records.")
        return 0
    except KafkaException as error:
        print(f"Failed to connect to Kafka: {error}", file=sys.stderr)
        return 1
