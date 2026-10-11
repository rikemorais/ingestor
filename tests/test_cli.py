"""The composition root (cli.main), with Kafka replaced by fakes."""

import runpy
from unittest import mock

import pytest
from confluent_kafka import KafkaException

from ingestor import cli
from ingestor.ports import Record


class FakeInfo:
    def __init__(self, broker_count):
        self.broker_count = broker_count


class FakeSink:
    def __init__(self):
        self.writes = []
        self.closed = False

    def write(self, record):
        self.writes.append(record)

    def close(self):
        self.closed = True


class FakeSource:
    def read(self):
        yield Record(value="x")
        yield Record(value="y")

    def close(self):
        return None


class FakeConnection:
    def __init__(self, broker_count=1, raises=False):
        self._broker_count = broker_count
        self._raises = raises
        self.seed_sink = FakeSink()

    def connect(self):
        if self._raises:
            raise KafkaException("boom")
        return FakeInfo(self._broker_count)

    def sink(self):
        return self.seed_sink

    def source(self):
        return FakeSource()


def patch_connection(monkeypatch, connection):
    monkeypatch.setattr(cli, "KafkaConnection", lambda info: connection)


def test_main_happy_path(monkeypatch, capsys):
    connection = FakeConnection(broker_count=1)
    patch_connection(monkeypatch, connection)
    assert cli.main() == 0
    out = capsys.readouterr().out
    assert "Connected to Kafka: 1 broker(s)." in out
    assert "Ingested 2 records." in out
    # the demo seeded five records into the sink before ingesting
    assert len(connection.seed_sink.writes) == 5
    assert connection.seed_sink.closed


def test_main_returns_one_without_brokers(monkeypatch, capsys):
    patch_connection(monkeypatch, FakeConnection(broker_count=0))
    assert cli.main() == 1
    assert "No Kafka brokers reachable" in capsys.readouterr().err


def test_main_returns_one_on_kafka_exception(monkeypatch, capsys):
    patch_connection(monkeypatch, FakeConnection(raises=True))
    assert cli.main() == 1
    assert "Failed to connect to Kafka" in capsys.readouterr().err


def test_main_reads_bootstrap_servers_from_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "host-a:9092,host-b:9092")
    captured = {}

    def capture(info):
        captured["brokers"] = info.brokers
        return FakeConnection(broker_count=1)

    monkeypatch.setattr(cli, "KafkaConnection", capture)
    assert cli.main() == 0
    assert captured["brokers"] == ["host-a:9092", "host-b:9092"]


def test_module_entrypoint_runs_main():
    with mock.patch("ingestor.cli.main", return_value=0) as main_mock:
        with pytest.raises(SystemExit) as exc:
            runpy.run_module("ingestor.__main__", run_name="__main__")
    assert exc.value.code == 0
    main_mock.assert_called_once()
