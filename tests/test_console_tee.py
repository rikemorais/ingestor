"""Console sinks and the Tee fan-out sink."""

from ingestor.adapters.console import ConsoleSink, JsonFieldsConsoleSink
from ingestor.adapters.tee import TeeSink
from ingestor.ports import Record
from ingestor.testing import MemorySink


def test_console_sink_prints_metadata(capsys):
    sink = ConsoleSink()
    sink.write(Record(value="hello", key="k1", origin="topic"))
    sink.close()
    out = capsys.readouterr().out
    assert "Ingested: Origin(topic) Key(k1) Value: hello" in out


def test_console_sink_prints_value_only(capsys):
    sink = ConsoleSink(include_metadata=False)
    sink.write(Record(value="just the value"))
    out = capsys.readouterr().out
    assert out.strip() == "just the value"


def test_json_sink_prints_selected_fields(capsys):
    sink = JsonFieldsConsoleSink(["name", "age"])
    sink.write(Record(value='{"name": "Ana", "age": 30, "extra": "x"}'))
    out = capsys.readouterr().out
    assert out.strip() == "name=Ana age=30"


def test_json_sink_missing_field_shows_dash(capsys):
    sink = JsonFieldsConsoleSink(["name", "missing"])
    sink.write(Record(value='{"name": "Ana"}'))
    assert capsys.readouterr().out.strip() == "name=Ana missing=-"


def test_json_sink_falls_back_on_invalid_json(capsys):
    sink = JsonFieldsConsoleSink(["name"])
    sink.write(Record(value="not json at all"))
    sink.close()
    assert capsys.readouterr().out.strip() == "not json at all"


def test_tee_forwards_write_and_close_to_all_sinks():
    a, b = MemorySink(), MemorySink()
    tee = TeeSink([a, b])

    tee.write(Record(value="r1"))
    tee.write(Record(value="r2"))
    tee.close()

    assert [r.value for r in a.records] == ["r1", "r2"]
    assert [r.value for r in b.records] == ["r1", "r2"]
    assert a.closed and b.closed
