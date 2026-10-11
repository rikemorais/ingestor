"""WebSocket source, driven by a fake socket - no network."""

import json

import pytest
from websocket import WebSocketException

from ingestor.adapters.ws.source import WebSocketSource


class FakeWS:
    def __init__(self, script):
        # script: list of ("yield", value) or ("raise", exception)
        self._script = list(script)
        self.sent = []
        self.closed = False

    def send(self, message):
        self.sent.append(message)

    def settimeout(self, timeout):
        self._timeout = timeout

    def recv(self):
        action, payload = self._script.pop(0)
        if action == "yield":
            return payload
        raise payload

    def close(self):
        self.closed = True


def patch_connect(monkeypatch, sockets):
    created = []

    def factory(url, timeout=None):
        ws = sockets.pop(0)
        created.append(ws)
        return ws

    monkeypatch.setattr("ingestor.adapters.ws.source.create_connection", factory)
    return created


def test_subscribe_dict_is_json_encoded_and_yields(monkeypatch):
    ws = FakeWS([("yield", "m1"), ("yield", "m2"), ("raise", WebSocketException())])
    patch_connect(monkeypatch, [ws])
    source = WebSocketSource("wss://x", subscribe={"type": "sub"}, reconnect=False, origin="feed")

    gen = source.read()
    assert next(gen).value == "m1"
    assert next(gen).value == "m2"
    with pytest.raises(WebSocketException):
        next(gen)

    assert ws.sent == [json.dumps({"type": "sub"})]
    assert ws.closed
    assert source._ws is None


def test_subscribe_string_sent_raw(monkeypatch):
    ws = FakeWS([("raise", OSError("down"))])
    patch_connect(monkeypatch, [ws])
    source = WebSocketSource("wss://x", subscribe="raw-sub", reconnect=False)
    with pytest.raises(OSError):
        list(source.read())
    assert ws.sent == ["raw-sub"]


def test_reconnects_then_stops(monkeypatch):
    monkeypatch.setattr("ingestor.adapters.ws.source.time.sleep", lambda s: None)
    first = FakeWS([("raise", WebSocketException("drop"))])
    second = FakeWS([])
    source = WebSocketSource("wss://x", reconnect=True, reconnect_delay=0)

    def recv_stop():
        source._running = False
        raise OSError("closed")

    second.recv = recv_stop
    patch_connect(monkeypatch, [first, second])

    with pytest.raises(OSError):
        list(source.read())
    assert first.closed and second.closed


def test_close_stops_and_closes_socket():
    source = WebSocketSource("wss://x")
    ws = FakeWS([])
    source._ws = ws
    source.close()
    assert source._running is False
    assert source._ws is None
    assert ws.closed

    idle = WebSocketSource("wss://x")
    idle.close()
    assert idle._running is False
