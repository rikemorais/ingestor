"""Core ingestion use case: read from a Source and write to a Sink.

Talks only to the ports. The same ``ingest`` serves streaming, batch and
micro-batch - only the injected Source changes (and ``limit``, for micro-batch).
"""

from ingestor.ports import Sink, Source


def ingest(source: Source, sink: Sink, limit: int | None = None) -> int:
    """Read every record from ``source`` and write it to ``sink``.

    Args:
        source: Where records are read from.
        sink: Where records are written to.
        limit: Stop after this many records; ``None`` reads until the source is
            exhausted.

    Returns:
        The number of records ingested.
    """
    total = 0
    try:
        for record in source.read():
            sink.write(record)
            total += 1
            if limit is not None and total >= limit:
                break
    finally:
        source.close()
        sink.close()
    return total
