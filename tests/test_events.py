from __future__ import annotations

from datetime import timezone
from typing import Any

import pytest

from wikistream.events import EditEvent, InvalidEvent, is_canary, parse_event


def test_parses_a_valid_event(valid_raw: dict[str, Any]) -> None:
    event = parse_event(valid_raw)
    assert event.wiki == "enwiki"
    assert event.is_bot is False
    assert event.length_delta == 150
    assert event.event_ts.tzinfo == timezone.utc


def test_round_trips_through_bytes(valid_raw: dict[str, Any]) -> None:
    event = parse_event(valid_raw)
    assert EditEvent.from_bytes(event.to_bytes()) == event


def test_missing_field_is_invalid(valid_raw: dict[str, Any]) -> None:
    del valid_raw["wiki"]
    with pytest.raises(InvalidEvent, match="wiki"):
        parse_event(valid_raw)


def test_wrong_type_is_invalid(valid_raw: dict[str, Any]) -> None:
    valid_raw["bot"] = "no"
    with pytest.raises(InvalidEvent, match="bot"):
        parse_event(valid_raw)


def test_bool_is_not_accepted_as_timestamp(valid_raw: dict[str, Any]) -> None:
    valid_raw["timestamp"] = True
    with pytest.raises(InvalidEvent, match="timestamp"):
        parse_event(valid_raw)


def test_new_page_delta_counts_from_zero(valid_raw: dict[str, Any]) -> None:
    valid_raw["length"] = {"new": 2048}
    assert parse_event(valid_raw).length_delta == 2048


def test_canary_detection(valid_raw: dict[str, Any]) -> None:
    assert not is_canary(valid_raw)
    valid_raw["meta"]["domain"] = "canary"
    assert is_canary(valid_raw)
