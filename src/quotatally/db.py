"""SQLite persistence.

Schema includes `user_id` and `team_id` columns from day one — required by the
Enterprise track (see spec Appendix A). Local mode populates both with "local";
hosted mode overrides with real IDs. No migration needed when Business tier ships.

Idempotency key is the `uuid` column (Claude's per-message uuid, Codex equivalent
when we parse it). `INSERT OR IGNORE` lets ingest be re-run safely.
"""
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterable, Iterator

from .aggregator import Summary, aggregate
from .parser import AssistantEvent
from .pricing import cost_of


SCHEMA_VERSION = 1

_DDL = [
    """CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)""",
    """CREATE TABLE IF NOT EXISTS events (
        uuid TEXT PRIMARY KEY,
        user_id TEXT NOT NULL DEFAULT 'local',
        team_id TEXT NOT NULL DEFAULT 'local',
        source TEXT NOT NULL,
        session_id TEXT NOT NULL,
        timestamp TEXT,
        model TEXT,
        input_tokens INTEGER NOT NULL DEFAULT 0,
        output_tokens INTEGER NOT NULL DEFAULT 0,
        cache_read_tokens INTEGER NOT NULL DEFAULT 0,
        cache_write_tokens INTEGER NOT NULL DEFAULT 0,
        tools TEXT NOT NULL DEFAULT '[]',
        cost_usd REAL NOT NULL DEFAULT 0.0,
        source_file TEXT,
        ingested_at TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_events_user_team_ts ON events(user_id, team_id, timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id)",
    "CREATE INDEX IF NOT EXISTS idx_events_source_file ON events(source_file)",
    """CREATE TABLE IF NOT EXISTS ingest_checkpoints (
        source_file TEXT PRIMARY KEY,
        last_offset INTEGER NOT NULL,
        last_ingested_at TEXT NOT NULL
    )""",
]


DEFAULT_DB_PATH = os.path.expanduser("~/.quotatally/quotatally.db")


class Database:
    def __init__(self, path: str = DEFAULT_DB_PATH):
        self.path = path
        if path != ":memory:":
            os.makedirs(os.path.dirname(path), exist_ok=True)
        self._ensure_schema()

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _ensure_schema(self) -> None:
        with self._conn() as c:
            for stmt in _DDL:
                c.execute(stmt)
            row = c.execute("SELECT version FROM schema_version").fetchone()
            if row is None:
                c.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))

    def ingest(
        self,
        events: Iterable[AssistantEvent],
        user_id: str = "local",
        team_id: str = "local",
    ) -> tuple[int, int]:
        """Insert events. Returns (inserted, skipped_duplicates)."""
        now = datetime.now(timezone.utc).isoformat()
        inserted = 0
        skipped = 0
        with self._conn() as c:
            for ev in events:
                usage = ev.usage
                cost = cost_of(usage, ev.model)
                cur = c.execute(
                    """INSERT OR IGNORE INTO events (
                        uuid, user_id, team_id, source, session_id, timestamp, model,
                        input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
                        tools, cost_usd, source_file, ingested_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        ev.uuid, user_id, team_id, ev.source, ev.session_id,
                        ev.timestamp.isoformat() if ev.timestamp else None,
                        ev.model,
                        usage.get("input_tokens", 0),
                        usage.get("output_tokens", 0),
                        usage.get("cache_read_input_tokens", 0),
                        usage.get("cache_creation_input_tokens", 0),
                        json.dumps(ev.tools),
                        cost,
                        ev.source_file,
                        now,
                    ),
                )
                if cur.rowcount:
                    inserted += 1
                else:
                    skipped += 1
        return inserted, skipped

    def get_checkpoint(self, source_file: str) -> int:
        """Return byte offset last successfully ingested for this file, or 0."""
        with self._conn() as c:
            row = c.execute(
                "SELECT last_offset FROM ingest_checkpoints WHERE source_file=?",
                (source_file,),
            ).fetchone()
            return row[0] if row else 0

    def set_checkpoint(self, source_file: str, offset: int) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as c:
            c.execute(
                """INSERT INTO ingest_checkpoints (source_file, last_offset, last_ingested_at)
                   VALUES (?,?,?)
                   ON CONFLICT(source_file) DO UPDATE SET
                       last_offset=excluded.last_offset,
                       last_ingested_at=excluded.last_ingested_at""",
                (source_file, offset, now),
            )

    def count_events(self, user_id: str = "local", team_id: str = "local") -> int:
        with self._conn() as c:
            row = c.execute(
                "SELECT COUNT(*) FROM events WHERE user_id=? AND team_id=?",
                (user_id, team_id),
            ).fetchone()
            return row[0]

    def summary(
        self,
        days: int,
        user_id: str = "local",
        team_id: str = "local",
    ) -> Summary:
        """Build a Summary from stored events for the last N days."""
        with self._conn() as c:
            rows = c.execute(
                """SELECT uuid, source, session_id, timestamp, model,
                          input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
                          tools, source_file
                     FROM events
                    WHERE user_id=? AND team_id=?
                      AND (timestamp IS NULL
                           OR datetime(timestamp) >= datetime('now', ?))""",
                (user_id, team_id, f"-{int(days)} days"),
            ).fetchall()

        events = [_row_to_event(r) for r in rows]
        return aggregate(events)


def _row_to_event(row: tuple) -> AssistantEvent:
    (uuid_, source, session_id, ts, model,
     inp, out, cr, cc, tools_json, source_file) = row
    from .parser import parse_ts
    return AssistantEvent(
        source=source,
        uuid=uuid_,
        session_id=session_id,
        timestamp=parse_ts(ts),
        model=model or "?",
        usage={
            "input_tokens": inp,
            "output_tokens": out,
            "cache_read_input_tokens": cr,
            "cache_creation_input_tokens": cc,
        },
        tools=json.loads(tools_json) if tools_json else [],
        source_file=source_file or "",
    )
