"""Sink adapter that writes each record to multiple sinks."""

from collections.abc import Iterable

from ingestor.ports.record import Record
from ingestor.ports.sink import Sink


class TeeSink:
    """Forward every record to all configured sinks."""

    def __init__(self, sinks: Iterable[Sink]) -> None:
        """Store the downstream sinks to fan out to."""
        self._sinks = tuple(sinks)

    def write(self, record: Record) -> None:
        """Write the record to every sink."""
        for sink in self._sinks:
            sink.write(record)

    def close(self) -> None:
        """Close every sink."""
        for sink in self._sinks:
            sink.close()
