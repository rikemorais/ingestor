"""WebSocket source: consumes any WebSocket endpoint as a stream of records.

True streaming: it keeps the socket open and yields a Record for each message
the server pushes. Reconnects automatically on drops so it can run continuously.
Example: the Coinbase market-data feed.
"""

import json
import sys
import time
from collections.abc import Iterator

from websocket import WebSocket, WebSocketException, create_connection

from ingestor.ports.record import Record


class WebSocketSource:
    """A streaming Source for any WebSocket endpoint."""

    def __init__(
        self,
        url: str,
        subscribe: dict | str | None = None,
        origin: str | None = None,
        open_timeout: float = 10.0,
        reconnect: bool = True,
        reconnect_delay: float = 3.0,
    ):
        """Configure the stream.

        Args:
            url: The ``wss://`` endpoint to connect to.
            subscribe: Message sent right after connecting (a dict is JSON
                encoded). Used by feeds that need a subscription, like Coinbase.
            origin: Value used as each record's ``origin``; defaults to the URL.
            open_timeout: Seconds to wait for the connection to open.
            reconnect: Reconnect automatically if the connection drops.
            reconnect_delay: Seconds to wait before each reconnection attempt.
        """
        self._url = url
        self._subscribe = subscribe
        self._origin = origin or url
        self._open_timeout = open_timeout
        self._reconnect = reconnect
        self._reconnect_delay = reconnect_delay
        self._running = True
        self._ws: WebSocket | None = None

    def read(self) -> Iterator[Record]:
        """Yield one Record per message, reconnecting on drops.

        Yields:
            A record whose ``value`` is the raw message text and ``origin`` is
            the configured origin.
        """
        while self._running:
            try:
                yield from self._stream_once()
            except (WebSocketException, OSError) as error:
                if not self._reconnect or not self._running:
                    raise
                print(
                    f"[WebSocketSource] connection lost ({error}); "
                    f"reconnecting in {self._reconnect_delay}s",
                    file=sys.stderr,
                )
            if not self._reconnect:
                break  # pragma: no cover - _stream_once only returns by raising
            if self._running:
                time.sleep(self._reconnect_delay)

    def _stream_once(self) -> Iterator[Record]:
        self._ws = create_connection(self._url, timeout=self._open_timeout)
        try:
            if self._subscribe is not None:
                message = (
                    self._subscribe
                    if isinstance(self._subscribe, str)
                    else json.dumps(self._subscribe)
                )
                self._ws.send(message)
            self._ws.settimeout(None)  # block waiting for pushed messages
            while True:
                frame = self._ws.recv()
                if frame:
                    text = frame if isinstance(frame, str) else frame.decode("utf-8", "replace")
                    yield Record(value=text, origin=self._origin)
        finally:
            if self._ws is not None:
                self._ws.close()
                self._ws = None

    def close(self) -> None:
        """Stop reconnecting and close the WebSocket connection."""
        self._running = False
        if self._ws is not None:
            self._ws.close()
            self._ws = None
