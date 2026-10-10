"""The ports (interfaces) and the domain model, all in one place."""

from ingestor.ports.connection import Connection, ConnectionInfo
from ingestor.ports.record import Record
from ingestor.ports.sink import Sink
from ingestor.ports.source import Source

__all__ = ["Record", "Source", "Sink", "Connection", "ConnectionInfo"]
