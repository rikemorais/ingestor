# Ingestor

A Python **data ingestion** app that reads records from a source (**Source**) and writes them to a destination (**Sink**). The same logic serves several modes: **streaming** (Kafka), **batch** (file) and **micro-batch** (by limit/window).

The architecture is **ports and adapters**: the logic depends only on interfaces (`ingestor.ports`), and each tool lives isolated in its own adapter (`ingestor.adapters.*`). Swapping source or destination - or using in-memory fakes in tests - does not touch the logic.

## Requirements

- Python 3.14 or newer.
- `uv` installed to manage dependencies.
- Docker with Docker Compose and Docker Desktop running (only for the Kafka mode).
- Port `9092` free for local Kafka.

## How to run

### 1. Clone the project

```bash
git clone https://github.com/rikemorais/ingestor.git
cd ingestor
```

### 2. Install dependencies

```bash
uv sync
```

### 3. Start Kafka

```bash
docker compose up -d
```

Wait for the broker to start. To follow the logs:

```bash
docker compose logs -f kafka
```

Press `Ctrl+C` to leave the logs. Kafka keeps running.

### 4. Run the app

```bash
uv run ingestor
```

> `ingestor` is the command defined in `pyproject.toml` (`[project.scripts]`). Equivalent alternative: `uv run python -m ingestor`.

The demo seeds a few records into the topic and then **ingests from Kafka to the console**. With local Kafka available, the expected output is:

```text
Connected to Kafka: 1 broker(s).
Ingested: origin=demo-messages key=1 value=Hello Kafka #1
Ingested: origin=demo-messages key=2 value=Hello Kafka #2
Ingested: origin=demo-messages key=3 value=Hello Kafka #3
Ingested: origin=demo-messages key=4 value=Hello Kafka #4
Ingested: origin=demo-messages key=5 value=Hello Kafka #5
Ingested 5 records.
```

The connection uses `localhost:9092` by default and waits up to 10 seconds for a response.

> Note: `KafkaSource` consumes in the `ingestor` group and, after a few consecutive empty polls (`max_empty_polls`), stops - that's why the demo terminates instead of waiting forever. Running again with the same group won't re-read already-read messages (the offset is saved). To re-consume everything, change the group in `src/ingestor/adapters/kafka/connection.py` or recreate the topic.

## Use another address

Set `KAFKA_BOOTSTRAP_SERVERS` when running:

```bash
KAFKA_BOOTSTRAP_SERVERS=server:9092 uv run ingestor
```

Replace `server:9092` with your broker's address. The current setup uses an unauthenticated connection; SASL/TLS clusters need adjustments in the adapter.

## Run the tests (no Kafka)

The tests use in-memory implementations (fakes) and a file source, so they **need neither Docker nor a broker**:

```bash
uv run python -m unittest discover -v
```

They exercise the same `ingest` function with a `FakeConnection` and with a `FileSource` (batch) - proving in practice that the logic does not depend on the Kafka lib.

> Tip: when adding/moving files under `src/`, if `uv run` complains about a module not found, run once `uv sync --reinstall-package ingestor`.

## Stop Kafka

```bash
docker compose stop
```

To start the same container again:

```bash
docker compose start
```

## If the connection fails

Check that Docker is running and inspect the broker's state and logs:

```bash
docker compose ps
docker compose logs --tail=50 kafka
```

If Kafka is still starting up, wait and run again. Also check the address and that port `9092` is free. On failure, the app prints the error and exits with code `1`.

## Structure

`src/` layout, with the code in an `ingestor` package organized as ports × adapters:

```text
ingestor/
├── src/
│   └── ingestor/
│       ├── __init__.py
│       ├── app.py              # use case: ingest(source, sink, limit)
│       ├── cli.py              # Composition Root: picks and injects the adapters
│       ├── __main__.py         # enables `python -m ingestor`
│       ├── ports/              # interfaces + domain (tool-agnostic)
│       │   ├── record.py       # Record (value, key, origin)
│       │   ├── source.py       # Source: read() -> Iterator[Record]
│       │   ├── sink.py         # Sink: write(record)
│       │   └── connection.py   # Connection: source() / sink()
│       └── adapters/           # concrete implementations (one folder per tool)
│           ├── kafka/          # streaming: KafkaSource, KafkaSink, KafkaConnection, KafkaInfoCluster
│           ├── console/        # generic destination: ConsoleSink
│           └── batch/          # batch: FileSource (reads a file line by line)
├── tests/
│   ├── __init__.py
│   ├── fakes.py                # ListSource, MemorySink, FakeConnection
│   └── test_ingestor.py        # unittest tests
├── docker-compose.yml          # local Kafka
├── pyproject.toml              # dependencies, build and the `ingestor` command
└── uv.lock
```

**How the layers relate:** `app.py` depends only on `ingestor.ports` (Source, Sink, Connection); the adapters in `adapters/` implement those ports; and only `cli.py` knows the concrete classes, wiring everything together. This way no specific lib (like `confluent_kafka`) "leaks" into the logic or the tests.

**The three modes, the same logic:**

| Mode | Source | Does `ingest` change? |
|---|---|---|
| Streaming | `KafkaSource` | no |
| Batch | `FileSource` | no |
| Micro-batch | any Source + `limit` | no |
