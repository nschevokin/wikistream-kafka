"""Consumer: Kafka -> DuckDB, at-least-once.

The loop is: poll a batch -> write it to DuckDB -> commit the offsets.

Auto-commit is off. If offsets were committed on a timer, a crash after the
commit but before the write would lose that batch for good. Committing after
the write means a crash can only cause redelivery, and the idempotent sink turns
redelivered messages into no-ops.

Several workers in the same consumer group split the topic's partitions between
them. Start or stop one and Kafka rebalances: the partitions are reassigned and
each new owner resumes from the last committed offset.
"""

from __future__ import annotations

import argparse
import logging
import os
import threading
import time
from typing import Any, Callable, Literal, Protocol, Sequence

from wikistream import config
from wikistream.events import EditEvent
from wikistream.sink import DuckDBSink, Record

log = logging.getLogger(__name__)


class MessageLike(Protocol):
    def error(self) -> Any: ...
    def value(self) -> bytes | None: ...
    def partition(self) -> int | None: ...
    def offset(self) -> int | None: ...


class ConsumerLike(Protocol):
    def consume(self, num_messages: int = ..., timeout: float = ...) -> Sequence[MessageLike]: ...
    def commit(self, *, asynchronous: Literal[False]) -> Any: ...
    def close(self) -> None: ...


def to_records(messages: Sequence[MessageLike]) -> list[Record]:
    records = []
    for msg in messages:
        if msg.error():
            log.warning("skipping message with error: %s", msg.error())
            continue
        value, partition, offset = msg.value(), msg.partition(), msg.offset()
        if value is None or partition is None or offset is None:
            continue
        records.append(Record(EditEvent.from_bytes(value), partition, offset))
    return records


def run(
    consumer: ConsumerLike,
    sink: DuckDBSink,
    name: str = "worker",
    batch_size: int = 500,
    poll_timeout: float = 1.0,
    idle_timeout: float | None = None,
    crash_after_batches: int | None = None,
    crash: Callable[[], None] = lambda: os._exit(1),
) -> tuple[int, int]:
    """Consume until idle for `idle_timeout` seconds (or forever). Returns (consumed, inserted).

    `crash_after_batches` simulates the process dying after writing a batch but
    before committing it, to demonstrate redelivery.
    """
    consumed = inserted = batches = 0
    last_message = time.monotonic()
    try:
        while True:
            messages = consumer.consume(num_messages=batch_size, timeout=poll_timeout)
            if not messages:
                if idle_timeout is not None and time.monotonic() - last_message > idle_timeout:
                    break
                continue
            last_message = time.monotonic()
            records = to_records(messages)
            new = sink.write(records)  # 1. write
            batches += 1
            consumed += len(records)
            inserted += new
            log.info("%s: batch of %d, %d new, %d already stored", name, len(records), new, len(records) - new)
            if crash_after_batches is not None and batches >= crash_after_batches:
                log.warning("%s: simulating a crash BEFORE committing offsets", name)
                crash()
                return consumed, inserted
            consumer.commit(asynchronous=False)  # 2. then commit
    finally:
        consumer.close()
    return consumed, inserted


def build_consumer(name: str) -> Any:
    from confluent_kafka import Consumer

    def on_assign(_: Any, partitions: list[Any]) -> None:
        log.info("%s: assigned partitions %s", name, sorted(p.partition for p in partitions))

    def on_revoke(_: Any, partitions: list[Any]) -> None:
        log.info("%s: revoked partitions %s", name, sorted(p.partition for p in partitions))

    consumer = Consumer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "group.id": config.CONSUMER_GROUP,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",  # a new group starts from the beginning of the topic
            # How long the broker waits without a heartbeat before declaring this consumer dead and
            # reassigning its partitions. The default is 45 s; 6 s makes crash recovery quick to demo.
            "session.timeout.ms": 6000,
            "client.id": name,
        }
    )
    consumer.subscribe([config.TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    return consumer


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume Wikimedia edits from Kafka into DuckDB.")
    parser.add_argument("--workers", type=int, default=1, help="consumers in the same group (threads)")
    parser.add_argument("--idle-timeout", type=float, default=None, help="stop after N seconds without messages")
    parser.add_argument("--crash-after-batches", type=int, default=None, help="simulate a crash before commit")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(threadName)s: %(message)s")

    sink = DuckDBSink(config.DB_PATH)
    totals: list[tuple[int, int]] = []

    def work(i: int) -> None:
        name = f"worker-{i}"
        totals.append(
            run(build_consumer(name), sink, name=name, idle_timeout=args.idle_timeout,
                crash_after_batches=args.crash_after_batches)
        )

    threads = [threading.Thread(target=work, args=(i,), name=f"worker-{i}") for i in range(args.workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    log.info(
        "done: %d messages consumed, %d new rows, %d rows in DuckDB",
        sum(c for c, _ in totals), sum(n for _, n in totals), sink.count(),
    )
    sink.close()


if __name__ == "__main__":
    main()
