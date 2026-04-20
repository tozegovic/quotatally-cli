import json
from datetime import timezone

from quotatally.parser import (
    AssistantEvent,
    _extract_tools,
    iter_claude_events,
    parse_ts,
)


def test_parse_ts_handles_z_suffix():
    ts = parse_ts("2026-04-18T12:34:56.000Z")
    assert ts is not None
    assert ts.tzinfo is not None
    assert ts.year == 2026
    assert ts.month == 4


def test_parse_ts_none_and_garbage():
    assert parse_ts(None) is None
    assert parse_ts("") is None
    assert parse_ts("not-a-date") is None


def test_extract_tools_finds_tool_uses():
    content = [
        {"type": "text", "text": "ok"},
        {"type": "tool_use", "name": "Bash", "id": "1"},
        {"type": "tool_use", "name": "Read", "id": "2"},
    ]
    assert _extract_tools(content) == ["Bash", "Read"]


def test_extract_tools_rejects_non_list():
    assert _extract_tools("string") == []
    assert _extract_tools(None) == []


def test_iter_claude_events_reads_jsonl(tmp_path):
    proj = tmp_path / "p1"
    proj.mkdir()
    jsonl = proj / "session-abc.jsonl"
    rec = {
        "type": "assistant",
        "uuid": "u-1",
        "sessionId": "sess-1",
        "timestamp": "2026-04-18T01:02:03Z",
        "message": {
            "model": "claude-opus-4-7",
            "usage": {
                "input_tokens": 10,
                "output_tokens": 20,
                "cache_read_input_tokens": 0,
                "cache_creation_input_tokens": 0,
            },
            "content": [{"type": "tool_use", "name": "Bash", "id": "x"}],
        },
    }
    jsonl.write_text(json.dumps(rec) + "\n")

    events = list(iter_claude_events(days_back=365, projects_dir=str(tmp_path)))
    assert len(events) == 1
    ev: AssistantEvent = events[0]
    assert ev.source == "claude"
    assert ev.uuid == "u-1"
    assert ev.session_id == "sess-1"
    assert ev.model == "claude-opus-4-7"
    assert ev.tools == ["Bash"]
    assert ev.timestamp is not None
    assert ev.timestamp.tzinfo is not None


def test_iter_claude_events_skips_non_assistant(tmp_path):
    jsonl = tmp_path / "x.jsonl"
    jsonl.write_text(json.dumps({"type": "user", "message": {}}) + "\n")
    assert list(iter_claude_events(days_back=365, projects_dir=str(tmp_path))) == []


def test_iter_claude_events_handles_garbage_lines(tmp_path):
    jsonl = tmp_path / "x.jsonl"
    jsonl.write_text("not json\n" + json.dumps({
        "type": "assistant",
        "sessionId": "s",
        "message": {"model": "claude-sonnet-4-6", "usage": {"input_tokens": 1}},
    }) + "\n")
    events = list(iter_claude_events(days_back=365, projects_dir=str(tmp_path)))
    assert len(events) == 1
