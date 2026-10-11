"""Batch source: reads a text file, one record per line.

Shows batch entering the system without touching ``ingest``: just another Source.
"""

from collections.abc import Iterator
from pathlib import Path
from typing import TextIO

from ingestor.ports.record import Record


class FileSource:
    """A finite Source that reads a text file line by line."""

    def __init__(self, path: str | Path, encoding: str = "utf-8") -> None:
        """Store the file path.

        Args:
            path: Path to the text file to read.
            encoding: Text encoding used to open the file.
        """
        self._path = Path(path)
        self._encoding = encoding
        self._file: TextIO | None = None

    def read(self) -> Iterator[Record]:
        """Open the file and yield one record per line.

        Yields:
            A record whose ``value`` is the line (newline stripped) and whose
            ``origin`` is the file name.
        """
        self._file = self._path.open("r", encoding=self._encoding)
        origin = self._path.name
        for line in self._file:
            yield Record(value=line.rstrip("\n"), origin=origin)

    def close(self) -> None:
        """Close the file if it is open."""
        if self._file is not None:
            self._file.close()
            self._file = None
