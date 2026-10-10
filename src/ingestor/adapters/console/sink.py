"""A simple sink that prints records - handy for demos and debugging."""

from ingestor.ports.record import Record


class ConsoleSink:
    """A Sink that writes each record to standard output.

    Depends on no tool, showing that the Sink port is truly agnostic.
    """

    def __init__(self, include_metadata: bool = True) -> None:
        self._include_metadata = include_metadata

    def write(self, record: Record) -> None:
        """Print a record to stdout.

        Args:
            record: The record to print.
        """
        if self._include_metadata:
            print(
                f"Ingested: Origin({record.origin}) "
                f"Key({record.key}) Value: {record.value}"
            )
        else:
            print(record.value)

    def close(self) -> None:
        """No-op: there is nothing to flush."""
        pass
