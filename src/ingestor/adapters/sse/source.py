"""Server-Sent Events (SSE) source: consumes any text/event-stream endpoint.

True streaming: it holds the HTTP connection open and yields a Record for each
event the server pushes, as it arrives. Reconnects automatically on drops so it
can run continuously. Example: Wikimedia EventStreams.
"""

import sys
import time
from collections.abc import Iterator

import httpx

from ingestor.ports.record import Record


class SSESource:
    """A streaming Source for any SSE (text/event-stream) endpoint."""

    def __init__(
        self,
        url: str,
        origin: str | None = None,
        connect_timeout: float = 10.0,
        user_agent: str = "ingestor/0.1 (https://github.com/rikemorais/ingestor)",
        reconnect: bool = True,
        reconnect_delay: float = 3.0,
    ):
        """Configure the stream.

        Args:
            url: The SSE endpoint to connect to.
            origin: Value used as each record's ``origin``; defaults to the URL.
            connect_timeout: Seconds to wait for the connection to open (reads
                have no timeout, since the stream stays open between events).
            user_agent: Sent as the ``User-Agent`` header. Some providers (such
                as Wikimedia) reject requests without a descriptive one.
            reconnect: Reconnect automatically if the connection drops or ends.
            reconnect_delay: Seconds to wait before each reconnection attempt.
        """
        self._url = url
        self._origin = origin or url
        self._connect_timeout = connect_timeout
        self._user_agent = user_agent
        self._reconnect = reconnect
        self._reconnect_delay = reconnect_delay
        self._running = True
        self._client: httpx.Client | None = None

    def read(self) -> Iterator[Record]:
        """Yield one Record per SSE event, reconnecting on drops.

        Yields:
            A record whose ``value`` is the event's ``data`` payload, ``key`` is
            the event id (if sent) and ``origin`` is the configured origin.
        """
        while self._running:
            try:
                yield from self._stream_once()
            except httpx.HTTPError as error:
                if not self._reconnect or not self._running:
                    raise
                print(
                    f"[SSESource] connection lost ({error}); "
                    f"reconnecting in {self._reconnect_delay}s",
                    file=sys.stderr,
                )
            if not self._reconnect:
                break
            if self._running:
                time.sleep(self._reconnect_delay)

    def _stream_once(self) -> Iterator[Record]:
        headers = {
            "Accept": "text/event-stream",
            "User-Agent": self._user_agent,
        }
        with httpx.Client(
            timeout=httpx.Timeout(self._connect_timeout, read=None)
        ) as client:
            self._client = client
            with client.stream("GET", self._url, headers=headers) as response:
                response.raise_for_status()
                data: list[str] = []
                event_id: str | None = None
                for line in response.iter_lines():
                    if line == "":
                        if data:
                            yield Record(
                                value="\n".join(data),
                                key=event_id,
                                origin=self._origin,
                            )
                            data = []
                            event_id = None
                    elif line.startswith("data:"):
                        data.append(line[len("data:"):].lstrip())
                    elif line.startswith("id:"):
                        event_id = line[len("id:"):].strip()
                    # comment (":...") and other fields (event:, retry:) fall through
        self._client = None

    def close(self) -> None:
        """Stop reconnecting and close the HTTP connection."""
        self._running = False
        if self._client is not None:
            self._client.close()
            self._client = None
