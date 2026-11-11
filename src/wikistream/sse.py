"""A minimal Server-Sent Events client for Wikimedia EventStreams.

SSE is a long-lived HTTP response where each event is a block of lines:

    id: <opaque position in the stream>
    data: <json>
    <blank line>

The `id` matters for reliability: after a disconnect we reconnect with the
`Last-Event-ID` header and the server resumes from there, so a network blip
does not lose events.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Iterable, Iterator

import requests

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SSEMessage:
    data: str
    id: str | None
    event: str


def parse_sse(lines: Iterable[str]) -> Iterator[SSEMessage]:
    """Turn raw SSE lines into messages, following the SSE spec's field rules."""
    data: list[str] = []
    event_id: str | None = None
    event_type = "message"
    for line in lines:
        if line == "":
            if data:
                yield SSEMessage(data="\n".join(data), id=event_id, event=event_type)
            data, event_type = [], "message"
            continue
        if line.startswith(":"):  # comment / keep-alive
            continue
        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "data":
            data.append(value)
        elif field == "id":
            event_id = value
        elif field == "event":
            event_type = value
    if data:
        yield SSEMessage(data="\n".join(data), id=event_id, event=event_type)


def stream(url: str, user_agent: str, max_backoff: float = 30.0) -> Iterator[SSEMessage]:
    """Yield messages forever, reconnecting with backoff and resuming from the last id."""
    last_id: str | None = None
    backoff = 1.0
    while True:
        headers = {"Accept": "text/event-stream", "User-Agent": user_agent}
        if last_id:
            headers["Last-Event-ID"] = last_id
        try:
            with requests.get(url, headers=headers, stream=True, timeout=(10, 60)) as resp:
                resp.raise_for_status()
                backoff = 1.0
                resp.encoding = "utf-8"  # SSE is always UTF-8; the header may not say so
                lines = resp.iter_lines(decode_unicode=True)
                for msg in parse_sse(line or "" for line in lines):
                    if msg.id:
                        last_id = msg.id
                    yield msg
        except (requests.RequestException, ValueError) as exc:
            log.warning("stream interrupted (%s); reconnecting in %.0fs", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
