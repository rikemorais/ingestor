"""Console sinks: print records as text or as selected JSON fields."""

from ingestor.adapters.console.json_sink import JsonFieldsConsoleSink
from ingestor.adapters.console.sink import ConsoleSink

__all__ = ["ConsoleSink", "JsonFieldsConsoleSink"]
