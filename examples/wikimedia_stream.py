"""Stream live Wikimedia edits (Server-Sent Events) to the console.

Runs continuously and reconnects on drops. Press Ctrl+C to stop.
No auth needed. Requires the `sse` extra (httpx):

    make wiki
    # or: PYTHONPATH=src uv run --extra sse python examples/wikimedia_stream.py
"""

import json

from ingestor.adapters.sse import SSESource
from ingestor.app import ingest
from ingestor.ports.record import Record

STREAM = "https://stream.wikimedia.org/v2/stream/recentchange"


class EditPrinter:
    """A Sink that prints a readable one-line summary per Wikimedia event."""

    def write(self, record: Record) -> None:
        """Handle the record."""
        try:
            event = json.loads(record.value)
        except json.JSONDecodeError:
            return
        wiki = event.get("wiki", "?")
        kind = event.get("type", "?")
        title = event.get("title", "?")
        user = event.get("user", "?")
        print(f"[{wiki}] ({kind}) {title} - by {user}")

    def close(self) -> None:
        """Release any resources."""
        pass


def run() -> int:
    """Build the pipeline and run the ingestion."""
    return ingest(SSESource(STREAM, origin="wikimedia"), EditPrinter())


def main() -> None:
    """Run the example from the command line."""
    print("Streaming live Wikimedia edits... Ctrl+C to stop\n")
    try:
        run()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
