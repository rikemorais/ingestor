"""Consume the BTC ticker topic and print each record to the console.

Run this in a second terminal while ``make bridge`` is publishing ticks.
"""

import os

from ingestor.adapters.console import ConsoleSink
from ingestor.adapters.kafka import KafkaConnection, KafkaConnectionInfo
from ingestor.app import ingest

TOPIC = "btc-ticks"


def main() -> None:
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    connection = KafkaConnection(
        KafkaConnectionInfo(
            brokers=brokers.split(","),
            topics=[TOPIC],
            # Keep this group independent from other consumers.
            group="btc-console",
            from_beginning=True,
        )
    )

    print(f"Consuming Kafka topic '{TOPIC}'... Ctrl+C to stop")
    ingest(connection.source(), ConsoleSink())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
