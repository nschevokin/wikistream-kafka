from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

SAMPLE = Path(__file__).parent / "data" / "sample_events.jsonl"

# The sample file holds 6 valid events, 1 canary, 1 event missing a field and 1 line of broken JSON.
VALID_IN_SAMPLE = 6
INVALID_IN_SAMPLE = 2


@pytest.fixture
def sample_lines() -> list[str]:
    return [line for line in SAMPLE.read_text().splitlines() if line.strip()]


@pytest.fixture
def valid_raw(sample_lines: list[str]) -> dict[str, Any]:
    raw: dict[str, Any] = json.loads(sample_lines[0])
    return raw
