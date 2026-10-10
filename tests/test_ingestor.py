"""Ingestion tests - run without Kafka and without Docker.

Run: uv run python -m unittest discover
"""

import tempfile
import unittest
from pathlib import Path

from ingestor.adapters.batch import FileSource
from ingestor.adapters.kafka import KafkaInfoCluster
from ingestor.app import ingest
from ingestor.ports import Record

from ingestor.testing import FakeConnection, ListSource, MemorySink


class TestIngest(unittest.TestCase):
    def test_ingests_from_source_to_sink(self) -> None:
        source = ListSource([Record("a"), Record("b"), Record("c")])
        sink = MemorySink()

        total = ingest(source, sink)

        self.assertEqual(total, 3)
        self.assertEqual([r.value for r in sink.records], ["a", "b", "c"])
        self.assertTrue(source.closed)
        self.assertTrue(sink.closed)

    def test_respects_limit(self) -> None:
        source = ListSource([Record(str(i)) for i in range(10)])
        sink = MemorySink()

        total = ingest(source, sink, limit=4)

        self.assertEqual(total, 4)
        self.assertEqual(len(sink.records), 4)

    def test_empty_source(self) -> None:
        sink = MemorySink()
        total = ingest(ListSource([]), sink)
        self.assertEqual(total, 0)
        self.assertEqual(sink.records, [])


class TestSwappableConnection(unittest.TestCase):
    def test_ingest_with_connection_without_kafka(self) -> None:
        # A Connection that is NOT Kafka provides source/sink,
        # and the SAME `ingest` runs just the same.
        connection = FakeConnection([Record("x"), Record("y")])

        total = ingest(connection.source(), connection.sink())

        self.assertEqual(total, 2)
        self.assertEqual([r.value for r in connection.used_sink.records], ["x", "y"])
        self.assertTrue(connection.used_sink.closed)


class TestFileSourceBatch(unittest.TestCase):
    def test_ingests_file_line_by_line(self) -> None:
        # The SAME `ingest`, now with a batch source (file).
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "data.txt"
            path.write_text("line 1\nline 2\nline 3\n", encoding="utf-8")

            sink = MemorySink()
            total = ingest(FileSource(path), sink)

            self.assertEqual(total, 3)
            self.assertEqual(
                [r.value for r in sink.records],
                ["line 1", "line 2", "line 3"],
            )
            self.assertEqual(sink.records[0].origin, "data.txt")
            self.assertTrue(sink.closed)

    def test_respects_limit_on_file(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "data.txt"
            path.write_text("a\nb\nc\nd\ne\n", encoding="utf-8")

            sink = MemorySink()
            total = ingest(FileSource(path), sink, limit=2)

            self.assertEqual(total, 2)
            self.assertEqual([r.value for r in sink.records], ["a", "b"])


class _FakeClusterMetadata:
    """Mimics confluent's ClusterMetadata (only what from_cluster uses)."""

    def __init__(self) -> None:
        self.brokers = {1: "b1:9092", 2: "b2:9092"}
        self.topics = {"t2": object(), "t1": object()}


class TestKafkaInfoCluster(unittest.TestCase):
    def test_from_cluster_translates_metadata(self) -> None:
        info = KafkaInfoCluster.from_cluster(_FakeClusterMetadata())
        self.assertEqual(info.broker_count, 2)
        self.assertEqual(info.topics, ["t1", "t2"])  # sorted


if __name__ == "__main__":
    unittest.main()
