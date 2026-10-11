"""Micro-batch: process the stream in fixed-size windows instead of one by one.

The use case stays the same (`ingest`); the batching lives in a Sink that
buffers records and flushes them in groups. This works the same over a finite
source (here) or a live stream.

Run: make micro-batch
     (or: PYTHONPATH=src uv run python examples/micro_batch.py)
"""

from collections.abc import Iterator

from ingestor.app import ingest
from ingestor.ports.record import Record


class CountSource:
    """A source that yields `count` numbered records."""

    def __init__(self, count: int) -> None:
        """Store the example configuration."""
        self._count = count

    def read(self) -> Iterator[Record]:
        """Yield the example records."""
        for i in range(1, self._count + 1):
            yield Record(value=str(i), origin="count")

    def close(self) -> None:
        """Release any resources."""
        pass


class BatchingSink:
    """Buffers records and flushes them as a batch every `size` records."""

    def __init__(self, size: int) -> None:
        """Store the example configuration."""
        self._size = size
        self._buffer: list[Record] = []
        self._batch = 0

    def write(self, record: Record) -> None:
        """Handle the record."""
        self._buffer.append(record)
        if len(self._buffer) >= self._size:
            self._flush()

    def close(self) -> None:
        """Release any resources."""
        if self._buffer:
            self._flush()  # flush the last partial window

    def _flush(self) -> None:
        """Run the _flush step of the example."""
        self._batch += 1
        values = [r.value for r in self._buffer]
        print(f"batch {self._batch}: {values}")
        self._buffer = []


def run() -> int:
    """Build the pipeline and run the ingestion."""
    return ingest(CountSource(23), BatchingSink(size=5))


def main() -> None:
    """Run the example from the command line."""
    run()


if __name__ == "__main__":
    main()
