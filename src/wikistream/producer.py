"""Producer: Wikimedia SSE stream -> Kafka.

Design choices worth knowing:

* Key = wiki. Kafka hashes the key to pick a partition, so every edit to the
  same wiki lands in the same partition and stays in order. Different wikis
  spread across partitions, which is what lets consumers work in parallel.
* Idempotent producer (enable.idempotence + acks=all). If a send is retried
  after a network error, the broker de-duplicates it, so retries never create
  duplicate messages, and nothing counts as sent until all in-sync replicas
  have it.
* Dead-letter topic. Events that fail validation go to a separate topic with
  the reason in a header, instead of crashing the producer or vanishing.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from wikistream import config
from wikistream.events import InvalidEvent, is_canary, parse_event
from wikistream.sse import stream

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Routed:
    topic: str
    key: bytes | None
    value: bytes
    headers: list[tuple[str, str | bytes | None]] = field(default_factory=list)


def route(raw_data: str, topic: str = config.TOPIC, dlq_topic: str = config.DLQ_TOPIC) -> Routed | None:
    """Decide where one raw SSE payload goes. Returns None for events to skip."""
    try:
        raw: Any = json.loads(raw_data)
    except json.JSONDecodeError as exc:
        return Routed(dlq_topic, None, raw_data.encode(), [("error", f"invalid json: {exc}".encode())])
    if not isinstance(raw, dict):
        return Routed(dlq_topic, None, raw_data.encode(), [("error", b"payload is not a JSON object")])
    if is_canary(raw):
        return None
    try:
        event = parse_event(raw)
    except InvalidEvent as exc:
        return Routed(dlq_topic, None, raw_data.encode(), [("error", str(exc).encode())])
    return Routed(topic, event.wiki.encode(), event.to_bytes())


def ensure_topics(bootstrap: str, topics: Iterable[str], partitions: int = config.PARTITIONS) -> None:
    """Create topics explicitly instead of relying on broker auto-creation."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics([NewTopic(t, num_partitions=partitions, replication_factor=1) for t in topics])
    for name, fut in futures.items():
        try:
            fut.result()
            log.info("created topic %s (%d partitions)", name, partitions)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def build_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,  # implies acks=all and safe retries
            "acks": "all",
            "linger.ms": 50,  # wait up to 50 ms to batch messages together
            "compression.type": "lz4",
        }
    )


@dataclass
class Stats:
    delivered: int = 0
    to_dlq: int = 0
    skipped: int = 0
    failed: int = 0


def file_source(path: Path) -> Iterator[str]:
    """Replay raw events from a JSON-lines file (used by tests and offline runs)."""
    with path.open() as fh:
        for line in fh:
            if line.strip():
                yield line.rstrip("\n")


def live_source() -> Iterator[str]:
    for msg in stream(config.STREAM_URL, config.USER_AGENT):
        if msg.event == "message":
            yield msg.data


def run(
    source: Iterable[str],
    producer: Producer,
    limit: int | None = None,
    topic: str = config.TOPIC,
    dlq_topic: str = config.DLQ_TOPIC,
) -> Stats:
    stats = Stats()
    stopping = False

    def on_delivery(err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            stats.failed += 1
            log.error("delivery failed: %s", err)
        elif msg.topic() == dlq_topic:
            stats.to_dlq += 1
        else:
            stats.delivered += 1

    def stop(*_: object) -> None:
        nonlocal stopping
        stopping = True

    previous = signal.signal(signal.SIGINT, stop)
    try:
        sent = 0
        for raw_data in source:
            if stopping or (limit is not None and sent >= limit):
                break
            routed = route(raw_data, topic, dlq_topic)
            if routed is None:
                stats.skipped += 1
                continue
            while True:
                try:
                    producer.produce(
                        routed.topic, value=routed.value, key=routed.key,
                        headers=routed.headers, on_delivery=on_delivery,
                    )
                    break
                except BufferError:  # local queue full: let it drain, then retry
                    producer.poll(1.0)
            producer.poll(0)  # serve delivery callbacks
            sent += 1
            if sent % 1000 == 0:
                log.info("sent %d", sent)
    finally:
        remaining = producer.flush(30)
        signal.signal(signal.SIGINT, previous)
        if remaining:
            log.error("%d messages still undelivered after flush", remaining)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream Wikimedia edits into Kafka.")
    parser.add_argument("--limit", type=int, default=None, help="stop after N events (default: run until Ctrl-C)")
    parser.add_argument("--file", type=Path, default=None, help="replay a JSON-lines file instead of the live stream")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    ensure_topics(config.BOOTSTRAP_SERVERS, [config.TOPIC, config.DLQ_TOPIC])
    source = file_source(args.file) if args.file else live_source()
    stats = run(source, build_producer(config.BOOTSTRAP_SERVERS), limit=args.limit)
    log.info(
        "done: %d delivered, %d to dead-letter topic, %d canary skipped, %d failed",
        stats.delivered, stats.to_dlq, stats.skipped, stats.failed,
    )


if __name__ == "__main__":
    main()
