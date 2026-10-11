# Streaming examples

Beyond Kafka and files, Ingestor ships adapters for two public, real-time streams
you can run right now. Each one is just another `Source`, so the same `ingest`
function consumes it without any change.

Their HTTP clients are optional extras, so the base install stays small:

| Source | Extra | Installs |
|---|---|---|
| `SSESource` (Server-Sent Events) | `sse` | httpx |
| `WebSocketSource` | `ws` | websocket-client |

## Live Wikipedia edits (SSE, no auth)

[Wikimedia EventStreams](https://stream.wikimedia.org/v2/stream/recentchange)
pushes every recent change across all Wikimedia projects as Server-Sent Events.

```bash
make wiki
```

The raw equivalent, if you prefer: `PYTHONPATH=src uv run --extra sse python examples/wikimedia_stream.py`.

!!! tip "Why `PYTHONPATH=src`"
    The examples use `PYTHONPATH=src` so the package resolves straight from the
    source tree. This avoids a recurring quirk where `uv` rebuilds the editable
    install when you switch extras, which can leave `ingestor` temporarily
    unimportable. `make wiki` and `make btc` already handle this for you.

Output (one line per event, stops after 15):

```text
[frwiki] (edit) Etampes-sur-Marne - by JCBotCommunes
[wikidatawiki] (edit) Q11388908 - by Pudox
[enwiktionary] (new) Template:R:alg:Goddard:1973c - by Victar
```

Each event is a JSON object. Useful fields: `type` (edit/new/log/categorize),
`title`, `user`, `wiki`, `comment`, `timestamp`, `bot`, `length`, `revision`.

### Using it in code

```python
from ingestor.adapters.sse import SSESource
from ingestor.adapters.console import ConsoleSink
from ingestor.app import ingest

source = SSESource(
    "https://stream.wikimedia.org/v2/stream/recentchange",
    origin="wikimedia",
)
ingest(source, ConsoleSink(), limit=10)
```

!!! note "User-Agent"
    Wikimedia rejects requests without a descriptive `User-Agent` (HTTP 403).
    `SSESource` sends a default one; override it with the `user_agent` argument.

## Live Bitcoin price (WebSocket, no auth)

The Coinbase market-data feed pushes the BTC-USD price on every trade over a
WebSocket connection.

```bash
make btc
```

The raw equivalent: `PYTHONPATH=src uv run --extra ws python examples/bitcoin_stream.py`.

Output:

```text
BTC-USD  $82559.93  (last trade 0.00045952 buy)
BTC-USD  $82560.54  (last trade 0.02339576 buy)
```

Each message is a JSON `ticker`. Useful fields: `price`, `last_size`, `side`
(buy/sell), `best_bid`/`best_ask`, `high_24h`/`low_24h`/`volume_24h`, `time`.

### Using it in code

```python
from ingestor.adapters.ws import WebSocketSource
from ingestor.adapters.console import ConsoleSink
from ingestor.app import ingest

source = WebSocketSource(
    "wss://ws-feed.exchange.coinbase.com",
    subscribe={
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": ["ticker"],
    },
    origin="coinbase",
)
ingest(source, ConsoleSink(), limit=10)
```

## Send a live stream into Kafka

`SSESource` and `WebSocketSource` are generic: any SSE or WebSocket endpoint
works by changing the URL (and the subscribe message). And `ingest(source, sink)`
never changes, so you can route these streams to any sink, including Kafka:

```python
from ingestor.adapters.kafka import KafkaConnection, KafkaConnectionInfo
from ingestor.adapters.ws import WebSocketSource
from ingestor.app import ingest

connection = KafkaConnection(KafkaConnectionInfo(brokers=["localhost:9092"], topics=["btc"]))
source = WebSocketSource(
    "wss://ws-feed.exchange.coinbase.com",
    subscribe={"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker"]},
    origin="btc",
)
ingest(source, connection.sink(), limit=50)  # live stream -> Kafka topic
```
