from datetime import datetime, timezone

from quotatally.db import Database
from quotatally.parser import AssistantEvent


def _ev(uuid, tools=(), tokens=1_000_000, model="claude-sonnet-4-6", session="s1"):
    return AssistantEvent(
        source="claude",
        uuid=uuid,
        session_id=session,
        timestamp=datetime(2026, 4, 18, tzinfo=timezone.utc),
        model=model,
        usage={"input_tokens": tokens, "output_tokens": 0,
               "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
        tools=list(tools),
        source_file="/tmp/x.jsonl",
    )


def test_schema_creates_events_table(tmp_path):
    db = Database(str(tmp_path / "q.db"))
    assert db.count_events() == 0


def test_ingest_inserts_events(tmp_path):
    db = Database(str(tmp_path / "q.db"))
    inserted, skipped = db.ingest([_ev("u1"), _ev("u2", tools=["Bash"])])
    assert inserted == 2
    assert skipped == 0
    assert db.count_events() == 2


def test_ingest_is_idempotent(tmp_path):
    db = Database(str(tmp_path / "q.db"))
    db.ingest([_ev("u1"), _ev("u2")])
    inserted, skipped = db.ingest([_ev("u1"), _ev("u2"), _ev("u3")])
    assert inserted == 1
    assert skipped == 2
    assert db.count_events() == 3


def test_user_team_isolation(tmp_path):
    db = Database(str(tmp_path / "q.db"))
    db.ingest([_ev("a1")], user_id="alice", team_id="acme")
    db.ingest([_ev("b1")], user_id="bob", team_id="acme")
    assert db.count_events(user_id="alice", team_id="acme") == 1
    assert db.count_events(user_id="bob", team_id="acme") == 1
    assert db.count_events(user_id="local", team_id="local") == 0


def test_summary_from_db_matches_aggregate(tmp_path):
    db = Database(str(tmp_path / "q.db"))
    db.ingest([_ev("u1", tools=["Bash"]), _ev("u2", tools=["Read"])])
    s = db.summary(days=30)
    # Two sonnet msgs, 1M input each = $3 * 2 = $6 total
    assert abs(s.totals.cost - 6.0) < 1e-9
    assert s.totals.msg_count == 2
    assert "Bash" in s.per_tool_cost
    assert "Read" in s.per_tool_cost


def test_cost_computed_at_ingest_time(tmp_path):
    db = Database(str(tmp_path / "q.db"))
    db.ingest([_ev("u1", model="claude-opus-4-7")])
    import sqlite3
    conn = sqlite3.connect(str(tmp_path / "q.db"))
    row = conn.execute("SELECT cost_usd, model FROM events WHERE uuid='u1'").fetchone()
    assert abs(row[0] - 15.0) < 1e-9  # opus input 1M = $15
    assert row[1] == "claude-opus-4-7"
