# API reference

Generated automatically from the code's docstrings.

## Domain

::: ingestor.ports.record.Record

## Ports

::: ingestor.ports.source.Source

::: ingestor.ports.sink.Sink

::: ingestor.ports.connection.Connection

::: ingestor.ports.connection.ConnectionInfo

## Use case

::: ingestor.app.ingest

## Kafka adapter

::: ingestor.adapters.kafka.connection.KafkaConnection

::: ingestor.adapters.kafka.connection.KafkaConnectionInfo

::: ingestor.adapters.kafka.source.KafkaSource

::: ingestor.adapters.kafka.sink.KafkaSink

::: ingestor.adapters.kafka.info.KafkaInfoCluster

## Batch adapter

::: ingestor.adapters.batch.file_source.FileSource

## Console adapter

::: ingestor.adapters.console.sink.ConsoleSink

## Streaming adapters

::: ingestor.adapters.sse.source.SSESource

::: ingestor.adapters.ws.source.WebSocketSource

## Testing utilities

In-memory doubles for testing a pipeline without Kafka or Docker, usable in your
own project (`from ingestor.testing import ...`).

::: ingestor.testing.ListSource

::: ingestor.testing.MemorySink

::: ingestor.testing.FakeConnection
