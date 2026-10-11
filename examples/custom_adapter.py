"""Write your own Source and Sink.

This is the whole point of the library: to ingest from or to a new tool, you
just write a class that satisfies a port. There is no base class to inherit from
(structural typing is enough), and `ingest` never changes.

A Source needs `read()` and `close()`; a Sink needs `write()` and `close()`.

Run: make custom
     (or: PYTHONPATH=src uv run python examples/custom_adapter.py)
"""

from collections.abc import Iterator

from ingestor.app import ingest
from ingestor.ports.record import Record


class RangeSource:
    """A tiny custom Source that yields a few numbered records."""

    def __init__(self, count: int) -> None:
        """Store the example configuration."""
        self._count = count

    def read(self) -> Iterator[Record]:
        """Yield the example records."""
        for i in range(1, self._count + 1):
            yield Record(value=f"item {i}", key=str(i), origin="range")

    def close(self) -> None:
        """Release any resources."""
        pass


class UpperCaseSink:
    """A tiny custom Sink that prints each value in upper case."""

    def __init__(self) -> None:
        """Store the example configuration."""
        self.written = 0

    def write(self, record: Record) -> None:
        """Handle the record."""
        print(record.value.upper())
        self.written += 1

    def close(self) -> None:
        """Release any resources."""
        print(f"(sink closed after {self.written} records)")


def run() -> int:
    """Build the pipeline and run the ingestion."""
    return ingest(RangeSource(5), UpperCaseSink())


def main() -> None:
    """Run the example from the command line."""
    run()


if __name__ == "__main__":
    main()
