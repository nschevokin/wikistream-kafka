from __future__ import annotations

from typing import Any

from wikistream.events import parse_event
from wikistream.sink import DuckDBSink, Record


def test_writing_the_same_events_twice_stores_them_once(valid_raw: dict[str, Any]) -> None:
    sink = DuckDBSink(":memory:")
    record = Record(parse_event(valid_raw), partition=0, offset=0)
    assert sink.write([record]) == 1
    assert sink.write([record]) == 0  # redelivered: no-op
    assert sink.count() == 1


def test_per_minute_view(valid_raw: dict[str, Any]) -> None:
    sink = DuckDBSink(":memory:")
    sink.write([Record(parse_event(valid_raw), partition=0, offset=0)])
    ((wiki, edits),) = sink.query("SELECT wiki, edits FROM edits_per_minute")
    assert (wiki, edits) == ("enwiki", 1)


def test_file_database_survives_reopen(tmp_path: Any, valid_raw: dict[str, Any]) -> None:
    path = str(tmp_path / "nested" / "db.duckdb")
    sink = DuckDBSink(path)
    sink.write([Record(parse_event(valid_raw), partition=0, offset=0)])
    sink.close()
    assert DuckDBSink(path).count() == 1


def test_duplicates_inside_one_batch_are_collapsed(valid_raw: dict[str, Any]) -> None:
    sink = DuckDBSink(":memory:")
    event = parse_event(valid_raw)
    assert sink.write([Record(event, 0, 0), Record(event, 0, 1)]) == 1
    assert sink.count() == 1


def test_timestamps_can_be_read_back(valid_raw: dict[str, Any]) -> None:
    sink = DuckDBSink(":memory:")
    sink.write([Record(parse_event(valid_raw), partition=0, offset=0)])
    ((first, last),) = sink.query("SELECT min(event_ts), max(event_ts) FROM edits")
    assert first == last
