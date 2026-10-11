"""Domain record that flows through ingestion.

Tool-agnostic: it knows nothing about Kafka, files or any other source.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Record:
    """A single unit of data read from a source or written to a sink.

    Attributes:
        value: The record payload.
        key: Optional partitioning/identity key.
        origin: Where the record came from (topic, file name, table...).
    """

    value: str
    key: str | None = None
    origin: str | None = None
