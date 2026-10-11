"""Stream live Bitcoin prices (Coinbase WebSocket) to the console.

Runs continuously and reconnects on drops. Press Ctrl+C to stop.
Requires the `ws` extra (websocket-client):

    make btc
    # or: PYTHONPATH=src uv run --extra ws python examples/bitcoin_stream.py
"""

import json

from ingestor.adapters.ws import WebSocketSource
from ingestor.app import ingest
from ingestor.ports.record import Record

URL = "wss://ws-feed.exchange.coinbase.com"
SUBSCRIBE = {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker"]}


class PricePrinter:
    """A Sink that prints the live BTC-USD price from each ticker message."""

    def write(self, record: Record) -> None:
        try:
            event = json.loads(record.value)
        except json.JSONDecodeError:
            return
        if event.get("type") != "ticker":
            return  # skip the subscription ack and heartbeats
        price = event.get("price")
        size = event.get("last_size")
        side = event.get("side", "")
        print(f"BTC-USD  ${price}  (last trade {size} {side})")

    def close(self) -> None:
        pass


def run() -> int:
    source = WebSocketSource(URL, subscribe=SUBSCRIBE, origin="coinbase")
    return ingest(source, PricePrinter())


def main() -> None:
    print("Streaming Live BTC-USD Price (Ctrl+C to Stop)\n")
    try:
        run()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
