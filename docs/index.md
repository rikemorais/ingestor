# Ingestor

A Python **data ingestion** app that reads records from a source (**Source**) and
writes them to a destination (**Sink**). The same logic serves several modes:
**streaming** (Kafka), **batch** (file) and **micro-batch** (by limit/window).

The architecture is **ports and adapters**: the logic depends only on interfaces
(`ingestor.ports`), and each tool lives isolated in its own adapter
(`ingestor.adapters.*`). Swapping source or destination - or using in-memory
fakes in tests - does not touch the logic.

## Highlights

- **One use case, many modes.** `ingest(source, sink)` is identical for streaming,
  batch and micro-batch - only the injected `Source` changes.
- **Tool-agnostic core.** No specific library (like `confluent_kafka`) leaks into
  the logic or the tests.
- **Easy to extend.** A new tool is a new adapter that implements a port; nothing
  else changes.

## The three modes, the same logic

| Mode | Source | Does `ingest` change? |
|---|---|---|
| Streaming | `KafkaSource` | no |
| Batch | `FileSource` | no |
| Micro-batch | any Source + `limit` | no |

## Where to go next

- [Getting started](getting-started.md) - install, run and test.
- [Architecture](architecture.md) - the ports & adapters design and the diagram.
- [Adapters](adapters.md) - how to add a new Source or Sink.
- [API reference](reference.md) - generated from the code's docstrings.
