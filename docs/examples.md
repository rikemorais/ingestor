# Examples

Every feature has a runnable script in `examples/`. Each one uses the same
`ingest(source, sink)` function; only the adapters change.

| Command | What it shows | Needs |
|---|---|---|
| `make batch` | Batch: read a file line by line | - |
| `make custom` | Write your own Source and Sink | - |
| `make micro-batch` | Process a stream in fixed-size windows | - |
| `make run` | Streaming with Kafka (produce + consume) | Kafka |
| `make wiki` | Live Wikipedia edits (SSE) | `sse` extra |
| `make btc` | Live Bitcoin price (WebSocket) | `ws` extra |
| `make bridge` | Live stream into a Kafka topic | Kafka + `ws` extra |

The live streams (`wiki`, `btc`) run continuously and reconnect on drops; stop
them with Ctrl+C. The others are finite and stop on their own.

## Batch (`make batch`)

`FileSource` reads a text file, one `Record` per line, into `ConsoleSink`.

## Write your own (`make custom`)

A `Source` only needs `read()` and `close()`; a `Sink` needs `write()` and
`close()`. There is no base class to inherit from, structural typing is enough.
This is how you plug a new tool into your own project:

```python
from collections.abc import Iterator
from ingestor.ports.record import Record

class RangeSource:
    def __init__(self, count: int) -> None:
        self._count = count

    def read(self) -> Iterator[Record]:
        for i in range(1, self._count + 1):
            yield Record(value=f"item {i}", key=str(i), origin="range")

    def close(self) -> None:
        pass
```

## Micro-batch (`make micro-batch`)

A `BatchingSink` buffers records and flushes them in windows of N, so you process
the stream in groups instead of one by one. The last partial window is flushed on
`close()`.

## Live streams (`make wiki` / `make btc`)

See [Live streams](streaming.md) for the SSE and WebSocket sources, how to run
them, and the data each one returns.

## Bridge (`make bridge`)

Reads the live Coinbase BTC-USD stream and produces each tick into a Kafka topic.
This is the real use for a broker: capture a live stream durably so other
consumers can read it (and replay it) later. Round trip:

```text
Coinbase WebSocket --(WebSocketSource)--> ingest --(KafkaSink)--> Kafka topic
                                                                      |
                                               KafkaSource <----------+  (consume back)
```
