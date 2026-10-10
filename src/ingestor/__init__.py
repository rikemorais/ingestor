"""Ingestor: reads data from many sources (streaming, batch, micro-batch) and writes it to destinations.

Organized as ports & adapters:
- `ports`: Source, Sink, Connection and the Record domain model.
- `adapters`: concrete implementations (kafka, console, ...).
- `app`: the `ingest` use case (Source -> Sink).
- `cli`: the Composition Root.
"""

__version__ = "0.1.0"
