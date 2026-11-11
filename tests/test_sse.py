from __future__ import annotations

from wikistream.sse import parse_sse


def test_parses_events_ids_and_comments() -> None:
    lines = [
        ":ok",
        "event: message",
        'id: [{"partition":0,"offset":1}]',
        'data: {"a": 1}',
        "",
        ": keep-alive",
        "data: line one",
        "data: line two",
        "",
    ]
    messages = list(parse_sse(lines))
    assert len(messages) == 2
    assert messages[0].data == '{"a": 1}'
    assert messages[0].id == '[{"partition":0,"offset":1}]'
    # the last seen id carries over until a new one arrives (SSE spec)
    assert messages[1].id == messages[0].id
    assert messages[1].data == "line one\nline two"


def test_blank_lines_without_data_yield_nothing() -> None:
    assert list(parse_sse(["", "", ":comment", ""])) == []


def test_trailing_event_without_blank_line_is_kept() -> None:
    assert [m.data for m in parse_sse(["data: x"])] == ["x"]
