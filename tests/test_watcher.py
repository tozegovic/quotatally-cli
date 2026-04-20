"""Tests for the watch-mode daemon."""
from __future__ import annotations

import json
import os

import pytest

from quotatally.db import Database
from quotatally.watcher import Watcher


def _claude_assistant_line(uuid: str, session: str, ts: str = "2026-04-19T20:00:00Z",
                           model: str = "claude-opus-4-7", input_tokens: int = 100,
                           output_tokens: int = 50) -> str:
    return json.dumps({
        "type": "assistant",
        "uuid": uuid,
        "sessionId": session,
        "timestamp": ts,
        "message": {
            "model": model,
            "usage": {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cache_read_input_tokens": 0,
                "cache_creation_input_tokens": 0,
            },
            "content": [{"type": "text", "text": "ok"}],
        },
    }) + "\n"


def _codex_lines(session: str, model: str = "gpt-5.4-mini") -> str:
    return (
        json.dumps({"type": "turn_context", "payload": {"model": model}}) + "\n"
        + json.dumps({
            "type": "event_msg",
            "timestamp": "2026-04-19T20:00:00Z",
            "payload": {
                "type": "token_count",
                "info": {"last_token_usage": {
                    "input_tokens": 1000,
                    "cached_input_tokens": 200,
                    "output_tokens": 300,
                    "reasoning_output_tokens": 50,
                }},
            },
        }) + "\n"
    )


@pytest.fixture
def dirs(tmp_path):
    claude = tmp_path / "claude_projects" / "p1"
    codex = tmp_path / "codex_sessions"
    claude.mkdir(parents=True)
    codex.mkdir(parents=True)
    return str(claude.parent), str(codex)


def _watcher(tmp_path, claude_dir, codex_dir):
    db = Database(str(tmp_path / "qt.db"))
    return db, Watcher(db, claude_dir=claude_dir, codex_dir=codex_dir)


def test_initial_ingest_and_checkpoint(tmp_path, dirs):
    claude_dir, codex_dir = dirs
    fp = os.path.join(claude_dir, "p1", "session-a.jsonl")
    with open(fp, "w") as f:
        f.write(_claude_assistant_line("u1", "s1"))
        f.write(_claude_assistant_line("u2", "s1"))

    db, w = _watcher(tmp_path, claude_dir, codex_dir)
    stats = w.run_once()
    assert stats.events_inserted == 2
    assert db.get_checkpoint(fp) == os.path.getsize(fp)


def test_append_picked_up_on_next_poll(tmp_path, dirs):
    claude_dir, codex_dir = dirs
    fp = os.path.join(claude_dir, "p1", "session-b.jsonl")
    with open(fp, "w") as f:
        f.write(_claude_assistant_line("u1", "s2"))

    db, w = _watcher(tmp_path, claude_dir, codex_dir)
    w.run_once()
    assert db.count_events() == 1

    with open(fp, "a") as f:
        f.write(_claude_assistant_line("u2", "s2"))
        f.write(_claude_assistant_line("u3", "s2"))

    stats = w.run_once()
    assert stats.events_inserted == 2
    assert db.count_events() == 3


def test_partial_trailing_line_not_advanced(tmp_path, dirs):
    claude_dir, codex_dir = dirs
    fp = os.path.join(claude_dir, "p1", "session-c.jsonl")
    full = _claude_assistant_line("u1", "s3")
    partial = '{"type":"assistant","uuid":"u2","mess'
    with open(fp, "w") as f:
        f.write(full)
        f.write(partial)

    db, w = _watcher(tmp_path, claude_dir, codex_dir)
    stats = w.run_once()
    assert stats.events_inserted == 1
    # Checkpoint stops at the last newline (end of `full`), not at file size.
    assert db.get_checkpoint(fp) == len(full.encode())


def test_idempotent_repoll_no_growth(tmp_path, dirs):
    claude_dir, codex_dir = dirs
    fp = os.path.join(claude_dir, "p1", "session-d.jsonl")
    with open(fp, "w") as f:
        f.write(_claude_assistant_line("u1", "s4"))

    db, w = _watcher(tmp_path, claude_dir, codex_dir)
    w.run_once()
    stats = w.run_once()
    assert stats.files_scanned == 0  # offset already at EOF, file size unchanged
    assert stats.events_inserted == 0


def test_codex_full_reread_dedup(tmp_path, dirs):
    claude_dir, codex_dir = dirs
    fp = os.path.join(codex_dir, "rollout-x.jsonl")
    with open(fp, "w") as f:
        f.write(_codex_lines("rollout-x"))

    db, w = _watcher(tmp_path, claude_dir, codex_dir)
    s1 = w.run_once()
    assert s1.events_inserted == 1

    # Touch to force mtime change but keep content; full re-read, uuid dedup.
    os.utime(fp, (os.path.getmtime(fp) + 1, os.path.getmtime(fp) + 1))
    s2 = w.run_once()
    assert s2.events_inserted == 0
    assert s2.events_skipped == 1


def test_new_file_discovered_on_next_poll(tmp_path, dirs):
    claude_dir, codex_dir = dirs
    fp1 = os.path.join(claude_dir, "p1", "session-e.jsonl")
    with open(fp1, "w") as f:
        f.write(_claude_assistant_line("u1", "s5"))

    db, w = _watcher(tmp_path, claude_dir, codex_dir)
    w.run_once()

    fp2 = os.path.join(claude_dir, "p1", "session-f.jsonl")
    with open(fp2, "w") as f:
        f.write(_claude_assistant_line("u9", "s6"))

    stats = w.run_once()
    assert stats.events_inserted == 1
    assert db.count_events() == 2
