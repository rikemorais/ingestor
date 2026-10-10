"""Connection port: builds a tool's Source and Sink.

Generic - it knows nothing about Kafka. Each tool implements this interface by
returning its own Source/Sink, which satisfy the abstract ports.
"""

from typing import Protocol

from ingestor.ports.sink import Sink
from ingestor.ports.source import Source


class ConnectionInfo(Protocol):
    """Marker for a tool's connection config (Kafka, Rabbit, file, ...)."""

    ...


class Connection(Protocol):
    """Factory that builds a Source and a Sink for one tool."""

    def source(self) -> Source:
        """Build the input adapter.

        Returns:
            A Source that yields records from the tool.
        """
        ...

    def sink(self) -> Sink:
        """Build the output adapter.

        Returns:
            A Sink that writes records to the tool.
        """
        ...
