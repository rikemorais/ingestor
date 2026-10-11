"""Kafka adapters, driven by fakes - no broker, no network."""

import pytest
from confluent_kafka import KafkaError, KafkaException

from ingestor.adapters.kafka import (
    KafkaConnection,
    KafkaConnectionInfo,
    KafkaSink,
    KafkaSource,
)
from ingestor.ports import Record


class FakeError:
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code


class FakeMessage:
    def __init__(self, value=None, key=None, topic=None, error=None):
        self._value, self._key, self._topic, self._error = value, key, topic, error

    def error(self):
        return self._error

    def value(self):
        return self._value

    def key(self):
        return self._key

    def topic(self):
        return self._topic


class FakeConsumer:
    def __init__(self, config, polls=()):
        self.config = config
        self.subscribed = None
        self.closed = False
        self._polls = list(polls)

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout):
        return self._polls.pop(0) if self._polls else None

    def close(self):
        self.closed = True


def consumer_factory(polls):
    return lambda config: FakeConsumer(config, polls)


# --------------------------- KafkaSource ---------------------------

def test_source_yields_decoded_records(monkeypatch):
    polls = [FakeMessage(value=b"hello", key=b"1", topic="t")]
    monkeypatch.setattr(
        "ingestor.adapters.kafka.source.Consumer", consumer_factory(polls)
    )
    source = KafkaSource(topics=["t"], max_empty_polls=1)
    records = list(source.read())
    assert records == [Record(value="hello", key="1", origin="t")]
    assert source._consumer.subscribed == ["t"]


def test_source_handles_none_value_and_key(monkeypatch):
    polls = [FakeMessage(value=None, key=None, topic="t")]
    monkeypatch.setattr(
        "ingestor.adapters.kafka.source.Consumer", consumer_factory(polls)
    )
    source = KafkaSource(topics=["t"], max_empty_polls=1)
    (record,) = list(source.read())
    assert record.value == "" and record.key is None


def test_source_stops_after_empty_polls(monkeypatch):
    monkeypatch.setattr(
        "ingestor.adapters.kafka.source.Consumer", consumer_factory([])
    )
    source = KafkaSource(topics=["t"], max_empty_polls=2)
    assert list(source.read()) == []


def test_source_treats_partition_eof_as_quiet(monkeypatch):
    polls = [FakeMessage(error=FakeError(KafkaError._PARTITION_EOF))]
    monkeypatch.setattr(
        "ingestor.adapters.kafka.source.Consumer", consumer_factory(polls)
    )
    source = KafkaSource(max_empty_polls=1)
    assert list(source.read()) == []


def test_source_raises_on_real_error(monkeypatch):
    polls = [FakeMessage(error=FakeError(KafkaError.UNKNOWN))]
    monkeypatch.setattr(
        "ingestor.adapters.kafka.source.Consumer", consumer_factory(polls)
    )
    source = KafkaSource(max_empty_polls=3)
    with pytest.raises(KafkaException):
        list(source.read())


def test_source_offset_reset_and_close(monkeypatch):
    monkeypatch.setattr(
        "ingestor.adapters.kafka.source.Consumer", consumer_factory([])
    )
    earliest = KafkaSource(from_beginning=True)
    latest = KafkaSource(from_beginning=False)
    assert earliest._consumer.config["auto.offset.reset"] == "earliest"
    assert latest._consumer.config["auto.offset.reset"] == "latest"
    earliest.close()
    assert earliest._consumer.closed


# --------------------------- KafkaSink -----------------------------

class FakeProducer:
    def __init__(self, config):
        self.config = config
        self.produced = []
        self.polled = []
        self.flushed = None

    def produce(self, topic, value=None, key=None):
        self.produced.append((topic, value, key))

    def poll(self, timeout):
        self.polled.append(timeout)

    def flush(self, timeout):
        self.flushed = timeout


def test_sink_uses_record_origin_as_topic(monkeypatch):
    monkeypatch.setattr("ingestor.adapters.kafka.sink.Producer", FakeProducer)
    sink = KafkaSink()
    sink.write(Record(value="v", key="k", origin="dest"))
    assert sink._producer.produced == [("dest", "v", "k")]
    assert sink._producer.polled == [0]


def test_sink_falls_back_to_default_topic(monkeypatch):
    monkeypatch.setattr("ingestor.adapters.kafka.sink.Producer", FakeProducer)
    sink = KafkaSink(default_topic="fallback")
    sink.write(Record(value="v"))
    assert sink._producer.produced == [("fallback", "v", None)]


def test_sink_raises_without_any_topic(monkeypatch):
    monkeypatch.setattr("ingestor.adapters.kafka.sink.Producer", FakeProducer)
    sink = KafkaSink()
    with pytest.raises(ValueError):
        sink.write(Record(value="v"))


def test_sink_close_flushes(monkeypatch):
    monkeypatch.setattr("ingestor.adapters.kafka.sink.Producer", FakeProducer)
    sink = KafkaSink()
    sink.close(timeout=5.0)
    assert sink._producer.flushed == 5.0


# ------------------------- KafkaConnection -------------------------

class FakeCluster:
    def __init__(self):
        self.brokers = {1: "b1:9092"}
        self.topics = {"t2": object(), "t1": object()}


class FakeAdmin:
    def __init__(self, config):
        self.config = config

    def list_topics(self, timeout):
        return FakeCluster()


def test_connection_connect_summarizes_cluster(monkeypatch):
    monkeypatch.setattr(
        "ingestor.adapters.kafka.connection.AdminClient", FakeAdmin
    )
    conn = KafkaConnection(KafkaConnectionInfo(brokers=["b1:9092", "b2:9092"]))
    info = conn.connect()
    assert info.broker_count == 1
    assert info.topics == ["t1", "t2"]
    assert conn._servers == "b1:9092,b2:9092"


def test_connection_builds_source_and_sink(monkeypatch):
    monkeypatch.setattr(
        "ingestor.adapters.kafka.source.Consumer", consumer_factory([])
    )
    monkeypatch.setattr("ingestor.adapters.kafka.sink.Producer", FakeProducer)
    conn = KafkaConnection(
        KafkaConnectionInfo(brokers=["b1:9092"], topics=["t1", "t2"])
    )
    source = conn.source()
    sink = conn.sink()
    assert isinstance(source, KafkaSource)
    assert source._topics == ["t1", "t2"]
    assert isinstance(sink, KafkaSink)
    assert sink._default_topic == "t1"


def test_connection_sink_without_topics_has_no_default(monkeypatch):
    monkeypatch.setattr("ingestor.adapters.kafka.sink.Producer", FakeProducer)
    conn = KafkaConnection(KafkaConnectionInfo(brokers=["b1:9092"]))
    assert conn.sink()._default_topic is None
