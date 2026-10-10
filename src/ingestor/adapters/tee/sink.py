"""Sink adapter that writes each record to multiple sinks."""

from collections.abc import Iterable

from ingestor.ports.record import Record
from ingestor.ports.sink import Sink


class TeeSink:
    """Forward every record to all configured sinks."""

    def __init__(self, sinks: Iterable[Sink]) -> None:
        self._sinks = tuple(sinks)

    def write(self, record: Record) -> None:
        for sink in self._sinks:
            sink.write(record)

    def close(self) -> None:
        for sink in self._sinks:
            sink.close()
