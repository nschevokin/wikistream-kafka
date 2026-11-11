from __future__ import annotations

from wikistream.producer import run

from .conftest import INVALID_IN_SAMPLE, VALID_IN_SAMPLE
from .fakes import FakeProducer


def test_run_counts_deliveries_dead_letters_and_skips(sample_lines: list[str]) -> None:
    producer = FakeProducer()
    stats = run(sample_lines, producer, topic="main", dlq_topic="dlq")  # type: ignore[arg-type]
    assert stats.delivered == VALID_IN_SAMPLE
    assert stats.to_dlq == INVALID_IN_SAMPLE
    assert stats.skipped == 1
    assert stats.failed == 0


def test_limit_stops_early(sample_lines: list[str]) -> None:
    producer = FakeProducer()
    run(sample_lines, producer, limit=3, topic="main", dlq_topic="dlq")  # type: ignore[arg-type]
    assert len(producer.sent) == 3
