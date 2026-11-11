from __future__ import annotations

import json
from typing import Any

import pytest

from wikistream import consumer as consumer_mod
from wikistream.events import parse_event
from wikistream.sink import DuckDBSink

from .fakes import FakeConsumer, FakeLog


class Crash(Exception):
    pass


def crash() -> None:
    raise Crash


def make_log(valid_raw: dict[str, Any], n: int) -> FakeLog:
    values = []
    for i in range(n):
        raw = json.loads(json.dumps(valid_raw))
        raw["meta"]["id"] = f"event-{i}"
        values.append(parse_event(raw).to_bytes())
    return FakeLog(values)


def test_commits_only_after_writing(valid_raw: dict[str, Any]) -> None:
    log = make_log(valid_raw, 10)
    sink = DuckDBSink(":memory:")
    fake = FakeConsumer(log)
    consumed, inserted = consumer_mod.run(fake, sink, batch_size=4, poll_timeout=0, idle_timeout=0)
    assert (consumed, inserted) == (10, 10)
    assert log.committed == 10
    assert fake.commits == 3  # batches of 4, 4, 2
    assert fake.closed


def test_crash_before_commit_redelivers_without_duplicates(valid_raw: dict[str, Any]) -> None:
    log = make_log(valid_raw, 10)
    sink = DuckDBSink(":memory:")

    # First run: batch 1 is committed, batch 2 is written, then the process "dies".
    with pytest.raises(Crash):
        consumer_mod.run(FakeConsumer(log), sink, batch_size=4, poll_timeout=0,
                         crash_after_batches=2, crash=crash)
    assert sink.count() == 8
    assert log.committed == 4  # batch 2 was never committed

    # Restart: Kafka redelivers from offset 4; batch 2 is already stored.
    consumed, inserted = consumer_mod.run(FakeConsumer(log), sink, batch_size=4, poll_timeout=0, idle_timeout=0)
    assert consumed == 6  # offsets 4..9, including the 4 redelivered ones
    assert inserted == 2  # only offsets 8 and 9 are new
    assert sink.count() == 10
    assert log.committed == 10
