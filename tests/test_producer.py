from __future__ import annotations

from wikistream.events import EditEvent
from wikistream.producer import route

from .conftest import INVALID_IN_SAMPLE, VALID_IN_SAMPLE


def test_valid_event_goes_to_main_topic_keyed_by_wiki(sample_lines: list[str]) -> None:
    routed = route(sample_lines[0], "main", "dlq")
    assert routed is not None
    assert routed.topic == "main"
    assert routed.key == b"enwiki"
    assert EditEvent.from_bytes(routed.value).wiki == "enwiki"


def test_sample_file_routing(sample_lines: list[str]) -> None:
    routed = [route(line, "main", "dlq") for line in sample_lines]
    assert sum(r is not None and r.topic == "main" for r in routed) == VALID_IN_SAMPLE
    assert sum(r is not None and r.topic == "dlq" for r in routed) == INVALID_IN_SAMPLE
    assert sum(r is None for r in routed) == 1  # the canary


def test_dead_letter_keeps_raw_payload_and_reason() -> None:
    routed = route("{not json", "main", "dlq")
    assert routed is not None
    assert routed.topic == "dlq"
    assert routed.value == b"{not json"
    assert routed.headers[0][0] == "error"
