"""Console sink for displaying selected fields from JSON records."""

import json
from collections.abc import Iterable

from ingestor.ports.record import Record


class JsonFieldsConsoleSink:
    """Print selected JSON fields from each record."""

    def __init__(self, fields: Iterable[str]) -> None:
        self._fields = tuple(fields)

    def write(self, record: Record) -> None:
        try:
            payload = json.loads(record.value)
            values = " ".join(f"{field}={payload.get(field, '-')}" for field in self._fields)
            print(values)
        except (TypeError, json.JSONDecodeError):
            print(record.value)

    def close(self) -> None:
        pass
