"""DuckDB sink with idempotent writes.

The consumer commits offsets only after a batch is written (at-least-once), so
after a crash the same messages can be delivered again. The sink makes that
harmless: event_id is the primary key and inserts use ON CONFLICT DO NOTHING,
so writing the same event twice leaves one row. At-least-once delivery plus an
idempotent sink gives effectively-once results.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import duckdb

from wikistream.events import EditEvent

SCHEMA = """
CREATE TABLE IF NOT EXISTS edits (
    event_id        VARCHAR PRIMARY KEY,
    wiki            VARCHAR NOT NULL,
    title           VARCHAR NOT NULL,
    user_name       VARCHAR NOT NULL,
    is_bot          BOOLEAN NOT NULL,
    change_type     VARCHAR NOT NULL,
    event_ts        TIMESTAMPTZ NOT NULL,
    length_delta    INTEGER,
    kafka_partition INTEGER NOT NULL,
    kafka_offset    BIGINT NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE OR REPLACE VIEW edits_per_minute AS
SELECT date_trunc('minute', event_ts) AS minute,
       wiki,
       count(*)                     AS edits,
       round(avg(is_bot::INT), 3)   AS bot_share
FROM edits
GROUP BY ALL;
"""


@dataclass(frozen=True)
class Record:
    event: EditEvent
    partition: int
    offset: int


class DuckDBSink:
    def __init__(self, path: str) -> None:
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(path)
        self._conn.execute(SCHEMA)
        self._lock = threading.Lock()

    def write(self, records: Sequence[Record]) -> int:
        """Insert a batch in one transaction. Returns how many rows were new."""
        if not records:
            return 0
        # The same event can appear twice in one batch (e.g. the SSE stream replays a
        # few events after a reconnect). Keep the first so the INSERT sees unique keys.
        unique: dict[str, Record] = {}
        for r in records:
            unique.setdefault(r.event.event_id, r)
        rows = [
            (
                r.event.event_id, r.event.wiki, r.event.title, r.event.user, r.event.is_bot,
                r.event.change_type, r.event.event_ts, r.event.length_delta, r.partition, r.offset,
            )
            for r in unique.values()
        ]
        # DuckDB allows one writing process per file; within this process the
        # lock serialises batches from several consumer threads.
        with self._lock:
            cur = self._conn.cursor()
            try:
                before = self.count(cur)
                cur.execute("BEGIN")
                # One multi-row INSERT per batch: much faster in DuckDB than executemany.
                placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"] * len(rows))
                cur.execute(
                    f"""INSERT INTO edits (event_id, wiki, title, user_name, is_bot, change_type,
                                           event_ts, length_delta, kafka_partition, kafka_offset)
                        VALUES {placeholders}
                        ON CONFLICT (event_id) DO NOTHING""",
                    [value for row in rows for value in row],
                )
                cur.execute("COMMIT")
                return self.count(cur) - before
            except Exception:
                cur.execute("ROLLBACK")
                raise
            finally:
                cur.close()

    def count(self, cur: duckdb.DuckDBPyConnection | None = None) -> int:
        result = (cur or self._conn).execute("SELECT count(*) FROM edits").fetchone()
        return int(result[0]) if result else 0

    def query(self, sql: str) -> list[tuple[object, ...]]:
        with self._lock:
            return self._conn.execute(sql).fetchall()

    def close(self) -> None:
        self._conn.close()
