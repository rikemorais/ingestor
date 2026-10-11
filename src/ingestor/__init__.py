"""Ingestor: read records from sources (streaming, batch, micro-batch) and write them to sinks.

Organized as ports & adapters:
- `ports`: Source, Sink, Connection and the Record domain model.
- `adapters`: concrete implementations (kafka, console, ...).
- `app`: the `ingest` use case (Source -> Sink).
- `cli`: the Composition Root.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("data-ingestor")
except PackageNotFoundError:  # pragma: no cover - only when running uninstalled
    __version__ = "0.0.0"
