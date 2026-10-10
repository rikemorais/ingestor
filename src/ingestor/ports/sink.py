"""Output port: a destination that receives records (data lake, Kafka, console...)."""

from typing import Protocol

from ingestor.ports.record import Record


class Sink(Protocol):
    """Contract for anything that consumes records."""

    def write(self, record: Record) -> None:
        """Write a single record to the destination.

        Args:
            record: The record to persist.
        """
        ...

    def close(self) -> None:
        """Flush and release resources.

        Make sure everything was written before closing.
        """
        ...
