"""Tests for the Codex CLI rollout-JSONL parser."""
import json

from quotatally.parser import iter_codex_events


def _line(obj):
    return json.dumps(obj) + "\n"


def test_codex_parser_extracts_token_counts(tmp_path):
    sess_dir = tmp_path / "2026" / "04" / "18"
    sess_dir.mkdir(parents=True)
    fp = sess_dir / "rollout-2026-04-18T13-31-04-xyz.jsonl"

    lines = []
    lines.append(_line({
        "timestamp": "2026-04-18T13:31:04Z",
        "type": "session_meta",
        "payload": {"id": "abc"},
    }))
    lines.append(_line({
        "timestamp": "2026-04-18T13:31:05Z",
        "type": "turn_context",
        "payload": {"turn_id": "t1", "model": "gpt-5.4"},
    }))
    lines.append(_line({
        "timestamp": "2026-04-18T13:31:20Z",
        "type": "event_msg",
        "payload": {
            "type": "token_count",
            "info": {
                "last_token_usage": {
                    "input_tokens": 1000,
                    "cached_input_tokens": 200,
                    "output_tokens": 50,
                    "reasoning_output_tokens": 10,
                }
            },
        },
    }))
    fp.write_text("".join(lines))

    events = list(iter_codex_events(days_back=365, sessions_dir=str(tmp_path)))
    assert len(events) == 1
    ev = events[0]
    assert ev.source == "codex"
    assert ev.model == "gpt-5.4"
    assert ev.session_id == "rollout-2026-04-18T13-31-04-xyz"
    # Input normalized: total - cached = 1000 - 200 = 800 uncached
    assert ev.usage["input_tokens"] == 800
    assert ev.usage["cache_read_input_tokens"] == 200
    # Reasoning folded into output: 50 + 10 = 60
    assert ev.usage["output_tokens"] == 60
    assert ev.tools == []


def test_codex_parser_skips_empty_info(tmp_path):
    fp = tmp_path / "rollout-empty.jsonl"
    lines = [
        _line({"type": "turn_context", "payload": {"model": "gpt-5.4"}}),
        _line({"type": "event_msg", "payload": {"type": "token_count", "info": None}}),
        _line({"type": "event_msg", "payload": {"type": "token_count"}}),
    ]
    fp.write_text("".join(lines))
    events = list(iter_codex_events(days_back=365, sessions_dir=str(tmp_path)))
    assert events == []


def test_codex_parser_handles_missing_dir(tmp_path):
    """Should return empty, not crash, when sessions dir doesn't exist."""
    events = list(iter_codex_events(days_back=365, sessions_dir=str(tmp_path / "nope")))
    assert events == []


def test_codex_parser_tracks_model_across_turns(tmp_path):
    fp = tmp_path / "rollout-multi.jsonl"
    lines = [
        _line({"type": "turn_context", "payload": {"model": "gpt-5.4"}}),
        _line({"timestamp": "2026-04-18T13:31:20Z", "type": "event_msg",
               "payload": {"type": "token_count",
                           "info": {"last_token_usage": {"input_tokens": 10, "output_tokens": 1}}}}),
        _line({"type": "turn_context", "payload": {"model": "gpt-5.4-mini"}}),
        _line({"timestamp": "2026-04-18T13:32:20Z", "type": "event_msg",
               "payload": {"type": "token_count",
                           "info": {"last_token_usage": {"input_tokens": 20, "output_tokens": 2}}}}),
    ]
    fp.write_text("".join(lines))
    events = list(iter_codex_events(days_back=365, sessions_dir=str(tmp_path)))
    assert len(events) == 2
    assert events[0].model == "gpt-5.4"
    assert events[1].model == "gpt-5.4-mini"
