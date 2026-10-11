"""Batch ingestion: read a text file line by line and print each record.

A batch source is finite: `ingest` runs until the file ends, then stops.

Run:
    make batch FILE=/path/to/your.txt
    (or: PYTHONPATH=src uv run python examples/batch_file.py /path/to/your.txt)
"""

import sys
from pathlib import Path

from ingestor.adapters.batch import FileSource
from ingestor.adapters.console import ConsoleSink
from ingestor.app import ingest


def run(path: Path) -> int:
    """Build the pipeline and run the ingestion."""
    return ingest(FileSource(path), ConsoleSink())


def main() -> None:
    """Run the example from the command line."""
    if len(sys.argv) < 2:
        print("Error: No Input File Given!!!", file=sys.stderr)
        print("Usage: make batch FILE=<path>", file=sys.stderr)
        print(
            "   or: PYTHONPATH=src uv run python examples/batch_file.py <path>",
            file=sys.stderr,
        )
        raise SystemExit(2)

    path = Path(sys.argv[1]).expanduser()
    if not path.is_file():
        print(f"Error: File Not Found: {path}", file=sys.stderr)
        raise SystemExit(1)

    total = run(path)
    print(f"\nBatch Done: {total} record(s)!")


if __name__ == "__main__":
    main()
