"""In-memory stand-ins for a Kafka partition log, consumer and producer.

They model the one behaviour the tests care about: a consumer resumes from the
last *committed* offset, so anything consumed but not committed is redelivered.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class FakeMessage:
    _value: bytes | None
    _partition: int
    _offset: int
    _topic: str = "main"

    def error(self) -> None:
        return None

    def value(self) -> bytes | None:
        return self._value

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def topic(self) -> str:
        return self._topic


@dataclass
class FakeLog:
    """One partition's messages plus the consumer group's committed offset."""

    values: list[bytes]
    committed: int = 0


@dataclass
class FakeConsumer:
    log: FakeLog
    position: int = field(init=False)
    commits: int = 0
    closed: bool = False

    def __post_init__(self) -> None:
        self.position = self.log.committed  # a (re)started consumer resumes here

    def consume(self, num_messages: int = 500, timeout: float = 1.0) -> list[FakeMessage]:
        end = min(self.position + num_messages, len(self.log.values))
        batch = [FakeMessage(self.log.values[i], 0, i) for i in range(self.position, end)]
        self.position = end
        return batch

    def commit(self, *, asynchronous: bool = True) -> None:
        self.log.committed = self.position
        self.commits += 1

    def close(self) -> None:
        self.closed = True


@dataclass
class FakeProducer:
    sent: list[tuple[str, bytes | None, bytes]] = field(default_factory=list)
    _pending: list[Callable[[], None]] = field(default_factory=list)

    def produce(self, topic: str, value: bytes, key: bytes | None, headers: Any, on_delivery: Any) -> None:
        self.sent.append((topic, key, value))
        msg = FakeMessage(value, 0, len(self.sent) - 1, topic)
        self._pending.append(lambda: on_delivery(None, msg))

    def poll(self, timeout: float) -> int:
        served, self._pending = self._pending, []
        for callback in served:
            callback()
        return len(served)

    def flush(self, timeout: float) -> int:
        self.poll(0)
        return 0
