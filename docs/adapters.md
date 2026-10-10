# Adapters

An adapter is a concrete implementation of a port. To plug a new tool in, write a
class that satisfies the port - the use case (`ingest`) never changes.

## Add a new Source

A `Source` yields records. Create a folder under `adapters/` for the tool and
implement `read()` and `close()`:

```python
# src/ingestor/adapters/mytool/source.py
from collections.abc import Iterator

from ingestor.ports.record import Record


class MyToolSource:
    def __init__(self, ...) -> None:
        ...

    def read(self) -> Iterator[Record]:
        for raw in self._pull():
            yield Record(value=raw.text, key=raw.id, origin="mytool")

    def close(self) -> None:
        ...
```

That's it - it already satisfies the `Source` port (structural typing), so it can
be passed straight to `ingest`:

```python
from ingestor.app import ingest
from ingestor.adapters.console import ConsoleSink

ingest(MyToolSource(...), ConsoleSink())
```

## Add a new Sink

A `Sink` receives records. Implement `write(record)` and `close()`:

```python
# src/ingestor/adapters/mytool/sink.py
from ingestor.ports.record import Record


class MyToolSink:
    def write(self, record: Record) -> None:
        self._client.send(record.value, key=record.key)

    def close(self) -> None:
        self._client.flush()
```

## Group creation behind a Connection (optional)

When a tool needs shared config to build both its source and sink, implement a
`Connection` so the Composition Root stays tidy:

```python
class MyToolConnection:
    def __init__(self, config) -> None:
        self._config = config

    def source(self) -> MyToolSource:
        return MyToolSource(self._config)

    def sink(self) -> MyToolSink:
        return MyToolSink(self._config)
```

## Wire it in the Composition Root

Only `cli.py` knows the concrete classes. Swapping tools is a one-line change
there - `app.py` is never touched.

## Built-in adapters

| Tool | Source | Sink |
|---|---|---|
| Kafka (`adapters/kafka`) | `KafkaSource` | `KafkaSink` |
| File (`adapters/batch`) | `FileSource` | - |
| Console (`adapters/console`) | - | `ConsoleSink` |
| SSE (`adapters/sse`) | `SSESource` | - |
| WebSocket (`adapters/ws`) | `WebSocketSource` | - |

`SSESource` and `WebSocketSource` are generic streaming sources (any SSE or
WebSocket endpoint). See [Streaming examples](streaming.md) for ready-to-run
demos (live Wikipedia edits and Bitcoin price). They need the `sse` / `ws`
optional extras.

See the [API reference](reference.md) for the full signatures.
