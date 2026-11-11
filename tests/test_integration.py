"""End-to-end against a real broker: `make up` then `make e2e`. Runs in CI too."""

from __future__ import annotations

import time
import uuid

import pytest

from wikistream import config
from wikistream import consumer as consumer_mod
from wikistream.producer import build_producer, ensure_topics, run
from wikistream.sink import DuckDBSink

from .conftest import INVALID_IN_SAMPLE, VALID_IN_SAMPLE

pytestmark = pytest.mark.integration


def wait_for_broker(timeout: float = 60.0) -> None:
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": config.BOOTSTRAP_SERVERS})
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            admin.list_topics(timeout=5)
            return
        except Exception:
            time.sleep(1)
    pytest.fail(f"no Kafka broker at {config.BOOTSTRAP_SERVERS}")


def count_messages(topic: str) -> int:
    from confluent_kafka import Consumer, TopicPartition

    probe = Consumer({"bootstrap.servers": config.BOOTSTRAP_SERVERS, "group.id": f"probe-{uuid.uuid4()}"})
    metadata = probe.list_topics(topic, timeout=10)
    total = 0
    for p in metadata.topics[topic].partitions:
        low, high = probe.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        total += high - low
    probe.close()
    return total


def test_sample_events_flow_from_producer_to_duckdb(sample_lines: list[str]) -> None:
    from confluent_kafka import Consumer

    wait_for_broker()
    suffix = uuid.uuid4().hex[:8]
    topic, dlq = f"test.edits.{suffix}", f"test.edits.dlq.{suffix}"
    ensure_topics(config.BOOTSTRAP_SERVERS, [topic, dlq])

    stats = run(sample_lines, build_producer(config.BOOTSTRAP_SERVERS), topic=topic, dlq_topic=dlq)
    assert (stats.delivered, stats.to_dlq, stats.failed) == (VALID_IN_SAMPLE, INVALID_IN_SAMPLE, 0)
    assert count_messages(dlq) == INVALID_IN_SAMPLE

    def new_consumer() -> Consumer:
        c = Consumer({
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "group.id": f"test-group-{suffix}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        })
        c.subscribe([topic])
        return c

    sink = DuckDBSink(":memory:")
    consumed, inserted = consumer_mod.run(new_consumer(), sink, idle_timeout=10)
    assert (consumed, inserted) == (VALID_IN_SAMPLE, VALID_IN_SAMPLE)

    # Same group again: offsets were committed, so nothing is redelivered.
    consumed, inserted = consumer_mod.run(new_consumer(), sink, idle_timeout=5)
    assert (consumed, inserted) == (0, 0)
    assert sink.count() == VALID_IN_SAMPLE
