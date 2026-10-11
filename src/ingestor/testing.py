"""In-memory test doubles of the ports.

These satisfy the Source, Sink and Connection ports without any real tool, so you
can test an ingestion pipeline without Kafka or Docker, in this project or in your
own. Import them as ``from ingestor.testing import ListSource, MemorySink``.
"""

from collections.abc import Iterator

from ingestor.ports import Record


class ListSource:
    """Finite source: yields a ready list of records (batch-like)."""

    def __init__(self, records: list[Record]) -> None:
        """Store the records to yield."""
        self._records = records
        self.closed = False

    def read(self) -> Iterator[Record]:
        """Yield each stored record."""
        yield from self._records

    def close(self) -> None:
        """Mark the source as closed."""
        self.closed = True


class MemorySink:
    """Sink that collects records in memory, for inspection."""

    def __init__(self) -> None:
        """Start with an empty record buffer."""
        self.records: list[Record] = []
        self.closed = False

    def write(self, record: Record) -> None:
        """Collect the record for later inspection."""
        self.records.append(record)

    def close(self) -> None:
        """Mark the sink as closed."""
        self.closed = True


class FakeConnection:
    """Connection without any Kafka - proves tool swappability."""

    def __init__(self, records: list[Record]) -> None:
        """Store the records and a reusable sink to inspect."""
        self._records = records
        self.used_sink = MemorySink()

    def source(self) -> ListSource:
        """Build the in-memory source."""
        return ListSource(self._records)

    def sink(self) -> MemorySink:
        """Return the in-memory sink."""
        return self.used_sink
