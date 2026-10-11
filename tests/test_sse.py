"""SSE source, driven by a fake httpx client - no network."""

import httpx
import pytest

from ingestor.adapters.sse.source import SSESource


class FakeStream:
    def __init__(self, lines):
        self._lines = lines

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        return None

    def iter_lines(self):
        yield from self._lines


class FakeClient:
    def __init__(self, lines):
        self._lines = lines
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def stream(self, method, url, headers=None):
        return FakeStream(self._lines)

    def close(self):
        self.closed = True


def patch_client(monkeypatch, lines):
    monkeypatch.setattr(
        "ingestor.adapters.sse.source.httpx.Client", lambda **kwargs: FakeClient(lines)
    )


def test_stream_once_parses_events(monkeypatch):
    lines = ["", "data: hello", "id: 5", "", "data: a", "data: b", "", ":comment"]
    patch_client(monkeypatch, lines)
    source = SSESource("http://x", origin="feed")
    records = list(source._stream_once())
    assert [(r.value, r.key, r.origin) for r in records] == [
        ("hello", "5", "feed"),
        ("a\nb", None, "feed"),
    ]
    assert source._client is None


def test_read_without_reconnect_stops_after_stream(monkeypatch):
    patch_client(monkeypatch, ["data: one", ""])
    source = SSESource("http://x", reconnect=False)
    assert [r.value for r in source.read()] == ["one"]


def test_read_reconnects_on_http_error(monkeypatch):
    monkeypatch.setattr("ingestor.adapters.sse.source.time.sleep", lambda s: None)
    source = SSESource("http://x", reconnect=True, reconnect_delay=0)
    calls = {"n": 0}

    def fake_stream_once():
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.HTTPError("dropped")
        source._running = False
        return iter(())

    source._stream_once = fake_stream_once
    assert list(source.read()) == []
    assert calls["n"] == 2


def test_read_raises_when_reconnect_disabled(monkeypatch):
    source = SSESource("http://x", reconnect=False)

    def boom():
        raise httpx.HTTPError("dropped")

    source._stream_once = boom
    with pytest.raises(httpx.HTTPError):
        list(source.read())


def test_close_stops_and_closes_client():
    source = SSESource("http://x")
    fake = FakeClient([])
    source._client = fake
    source.close()
    assert source._running is False
    assert source._client is None
    assert fake.closed

    idle = SSESource("http://x")
    idle.close()
    assert idle._running is False
