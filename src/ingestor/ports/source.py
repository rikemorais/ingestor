"""Input port: a source that yields records.

Streaming, batch and micro-batch are all Sources - only how ``read`` iterates
changes: continuous for streaming, finite for batch, windowed for micro-batch.
"""

from collections.abc import Iterator
from typing import Protocol

from ingestor.ports.record import Record


class Source(Protocol):
    """Contract for anything that produces records."""

    def read(self) -> Iterator[Record]:
        """Yield records one by one.

        Yields:
            The next record read from the underlying system.
        """
        ...

    def close(self) -> None:
        """Release any resources held by the source."""
        ...
