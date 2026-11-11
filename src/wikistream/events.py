"""The event model: parse and validate one Wikimedia recentchange event.

Validation happens once, at the producer. Anything that reaches the main topic
is a valid `EditEvent`; anything that fails goes to the dead-letter topic with
the reason attached, so bad input is visible instead of silently dropped.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Mapping


class InvalidEvent(ValueError):
    """Raised when a raw event cannot be turned into an EditEvent."""


@dataclass(frozen=True)
class EditEvent:
    event_id: str  # meta.id, a UUID unique per event: the deduplication key
    wiki: str  # e.g. "enwiki"; also the Kafka message key
    title: str
    user: str
    is_bot: bool
    change_type: str  # edit, new, log, categorize
    event_ts: datetime  # when the change happened (UTC)
    length_delta: int | None  # bytes added or removed, when the event has lengths

    def to_bytes(self) -> bytes:
        payload = asdict(self)
        payload["event_ts"] = self.event_ts.isoformat()
        return json.dumps(payload, separators=(",", ":")).encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> EditEvent:
        payload = json.loads(data)
        payload["event_ts"] = datetime.fromisoformat(payload["event_ts"])
        return cls(**payload)


def is_canary(raw: Mapping[str, Any]) -> bool:
    """Wikimedia injects synthetic 'canary' events to monitor the stream. Skip them."""
    meta = raw.get("meta")
    return isinstance(meta, Mapping) and meta.get("domain") == "canary"


def _require(raw: Mapping[str, Any], key: str, kind: type) -> Any:
    value = raw.get(key)
    # bool is a subclass of int in Python, so check it explicitly.
    if value is None or not isinstance(value, kind) or (kind is int and isinstance(value, bool)):
        raise InvalidEvent(f"field '{key}' missing or not {kind.__name__}")
    return value


def parse_event(raw: Mapping[str, Any]) -> EditEvent:
    meta = raw.get("meta")
    if not isinstance(meta, Mapping):
        raise InvalidEvent("field 'meta' missing or not an object")
    event_id = _require(meta, "id", str)

    wiki = _require(raw, "wiki", str)
    title = _require(raw, "title", str)
    user = _require(raw, "user", str)
    is_bot = _require(raw, "bot", bool)
    change_type = _require(raw, "type", str)
    timestamp = _require(raw, "timestamp", int)

    length_delta: int | None = None
    length = raw.get("length")
    if isinstance(length, Mapping) and isinstance(length.get("new"), int):
        old = length.get("old")
        length_delta = length["new"] - (old if isinstance(old, int) else 0)

    return EditEvent(
        event_id=event_id,
        wiki=wiki,
        title=title,
        user=user,
        is_bot=is_bot,
        change_type=change_type,
        event_ts=datetime.fromtimestamp(timestamp, tz=timezone.utc),
        length_delta=length_delta,
    )
