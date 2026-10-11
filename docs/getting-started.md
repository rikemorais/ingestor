# Getting started

## Requirements

- Python 3.14 or newer.
- [`uv`](https://docs.astral.sh/uv/) to manage dependencies.
- Docker with Docker Compose running (only for the Kafka mode).
- Port `9092` free for local Kafka.

## Install

```bash
git clone https://github.com/rikemorais/ingestor.git
cd ingestor
uv sync
```

## Run (streaming demo)

Start Kafka, then run the app:

```bash
docker compose up -d
uv run ingestor
```

The demo seeds a few records into the topic and then ingests from Kafka to the
console. Expected output:

```text
Connected to Kafka: 1 broker(s).
Ingested: origin=demo-messages key=1 value=Hello Kafka #1
...
Ingested 5 records.
```

!!! note "Consumer group and offsets"
    `KafkaSource` consumes in the `ingestor` group and stops after a few
    consecutive empty polls (`max_empty_polls`), so the demo terminates instead
    of waiting forever. Running again with the same group won't re-read messages
    (the offset is saved). To re-consume everything, change the group in
    `src/ingestor/adapters/kafka/connection.py` or recreate the topic.

### Use another address

```bash
KAFKA_BOOTSTRAP_SERVERS=server:9092 uv run ingestor
```

## Run the tests (no Kafka)

The tests use in-memory fakes and a file source, so they need neither Docker nor
a broker:

```bash
uv run python -m unittest discover -v
```

They exercise the same `ingest` function with a `FakeConnection` and with a
`FileSource` (batch) - proving the logic does not depend on the Kafka library.

!!! tip
    When adding or moving files under `src/`, if `uv run` complains about a
    module not found, run once: `uv sync --reinstall-package data-ingestor`.

## Stop Kafka

```bash
docker compose stop
```
