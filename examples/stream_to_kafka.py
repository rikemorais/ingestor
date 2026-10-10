"""Bridge: pipe a live external stream into a Kafka topic.

Reads the Coinbase BTC-USD stream and produces each message to Kafka. This is
the real use for Kafka here: capture a live stream durably so other consumers
can read it (and replay it) later.

Runs continuously (reconnects on drops) until Ctrl+C. Pass a number to stop
after N messages instead.

Needs Kafka running and the `ws` extra:

    docker compose up -d
    make bridge            # continuous, Ctrl+C to stop
    make bridge N=100      # stop after 100 messages
    # or: PYTHONPATH=src uv run --extra ws python examples/stream_to_kafka.py [N]
"""

import sys

from ingestor.adapters.console import JsonFieldsConsoleSink
from ingestor.adapters.kafka import KafkaConnection, KafkaConnectionInfo
from ingestor.adapters.tee import TeeSink
from ingestor.adapters.ws import WebSocketSource
from ingestor.app import ingest

URL = "wss://ws-feed.exchange.coinbase.com"
SUBSCRIBE = {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker"]}
TOPIC = "btc-ticks"


def run(limit: int | None = None) -> int:
    connection = KafkaConnection(
        KafkaConnectionInfo(brokers=["localhost:9092"], topics=[TOPIC])
    )
    source = WebSocketSource(URL, subscribe=SUBSCRIBE, origin=TOPIC)
    sink = TeeSink(
        [
            connection.sink(),
            JsonFieldsConsoleSink(fields=("price", "side", "time")),
        ]
    )
    return ingest(source, sink, limit=limit)


def main() -> None:
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None

    if limit is None:
        print(f"Bridging live BTC ticks into Kafka topic '{TOPIC}'... Ctrl+C to stop")
    else:
        print(f"Bridging {limit} BTC ticks into Kafka topic '{TOPIC}'...")

    try:
        total = run(limit)
        print(f"\nDone! Produced {total} messages to '{TOPIC}'!")
    except KeyboardInterrupt:
        print("\n---Stopped!!!---")


if __name__ == "__main__":
    main()
