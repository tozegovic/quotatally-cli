"""Source-agnostic session-log parsing.

Each source yields AssistantEvent records normalized across providers.
Phase 1 implements the Claude Code JSONL source; Codex CLI source is stubbed
with a TODO marker.
"""
from __future__ import annotations

import glob
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterator


CLAUDE_PROJECTS_DIR = os.path.expanduser("~/.claude/projects")
CODEX_SESSIONS_DIR = os.path.expanduser("~/codex_workspace/.codex/sessions")


@dataclass
class AssistantEvent:
    source: str              # "claude" | "codex"
    uuid: str                # unique id from source (Claude's rec["uuid"], fallback hash)
    session_id: str
    timestamp: datetime | None
    model: str
    usage: dict              # raw usage block
    tools: list[str]         # tool names used in this message
    source_file: str


def parse_ts(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _extract_tools(content) -> list[str]:
    if not isinstance(content, list):
        return []
    return [
        c.get("name", "?")
        for c in content
        if isinstance(c, dict) and c.get("type") == "tool_use"
    ]


def iter_claude_events(days_back: int, projects_dir: str = CLAUDE_PROJECTS_DIR) -> Iterator[AssistantEvent]:
    """Walk Claude Code JSONL session logs and yield normalized events."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    pattern = os.path.join(projects_dir, "**", "*.jsonl")
    for fp in glob.glob(pattern, recursive=True):
        try:
            if datetime.fromtimestamp(os.path.getmtime(fp), tz=timezone.utc) < cutoff:
                continue
        except OSError:
            continue
        try:
            with open(fp) as f:
                for line_no, line in enumerate(f, start=1):
                    rec = _safe_load(line)
                    if not rec or rec.get("type") != "assistant":
                        continue
                    msg = rec.get("message") or {}
                    usage = msg.get("usage") or {}
                    if not usage:
                        continue
                    uid = rec.get("uuid") or f"{fp}:{line_no}"
                    yield AssistantEvent(
                        source="claude",
                        uuid=uid,
                        session_id=rec.get("sessionId") or os.path.basename(fp).replace(".jsonl", ""),
                        timestamp=parse_ts(rec.get("timestamp")),
                        model=msg.get("model", "?"),
                        usage=usage,
                        tools=_extract_tools(msg.get("content", [])),
                        source_file=fp,
                    )
        except OSError:
            continue


def iter_codex_events(days_back: int, sessions_dir: str = CODEX_SESSIONS_DIR) -> Iterator[AssistantEvent]:
    """Walk Codex CLI rollout JSONL files and yield normalized events.

    Codex records per-turn token usage in `event_msg` entries whose
    payload.type == "token_count" and payload.info is populated. The model
    for each turn comes from the most recent `turn_context` event seen in
    the same file. Session ID is the rollout filename stem.

    Unlike Anthropic's JSONL, Codex's `input_tokens` includes cached input;
    we subtract `cached_input_tokens` so the `usage.input_tokens` emitted here
    means "uncached input" to match Anthropic convention. `reasoning_output_tokens`
    is folded into `output_tokens` for cost purposes.
    """
    if not os.path.isdir(sessions_dir):
        return
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    pattern = os.path.join(sessions_dir, "**", "*.jsonl")
    for fp in glob.glob(pattern, recursive=True):
        try:
            if datetime.fromtimestamp(os.path.getmtime(fp), tz=timezone.utc) < cutoff:
                continue
        except OSError:
            continue
        session_id = os.path.basename(fp).replace(".jsonl", "")
        current_model = "?"
        try:
            with open(fp) as f:
                for line_no, line in enumerate(f, start=1):
                    rec = _safe_load(line)
                    if not rec:
                        continue
                    pl = rec.get("payload") or {}
                    if rec.get("type") == "turn_context":
                        current_model = pl.get("model") or current_model
                        continue
                    if rec.get("type") != "event_msg":
                        continue
                    if pl.get("type") != "token_count":
                        continue
                    info = pl.get("info")
                    if not info:
                        continue
                    last = info.get("last_token_usage") or {}
                    if not last:
                        continue
                    total_input = last.get("input_tokens", 0)
                    cached = last.get("cached_input_tokens", 0)
                    uncached_input = max(0, total_input - cached)
                    output = last.get("output_tokens", 0) + last.get("reasoning_output_tokens", 0)
                    yield AssistantEvent(
                        source="codex",
                        uuid=f"{session_id}:{line_no}",
                        session_id=session_id,
                        timestamp=parse_ts(rec.get("timestamp")),
                        model=current_model,
                        usage={
                            "input_tokens": uncached_input,
                            "cache_read_input_tokens": cached,
                            "cache_creation_input_tokens": 0,
                            "output_tokens": output,
                        },
                        tools=[],  # Codex tool-use attribution is Phase 2
                        source_file=fp,
                    )
        except OSError:
            continue


def parse_claude_line(line: str, fp: str, line_no: int) -> AssistantEvent | None:
    rec = _safe_load(line)
    if not rec or rec.get("type") != "assistant":
        return None
    msg = rec.get("message") or {}
    usage = msg.get("usage") or {}
    if not usage:
        return None
    uid = rec.get("uuid") or f"{fp}:{line_no}"
    return AssistantEvent(
        source="claude",
        uuid=uid,
        session_id=rec.get("sessionId") or os.path.basename(fp).replace(".jsonl", ""),
        timestamp=parse_ts(rec.get("timestamp")),
        model=msg.get("model", "?"),
        usage=usage,
        tools=_extract_tools(msg.get("content", [])),
        source_file=fp,
    )


def iter_claude_events_from_file(fp: str, start_offset: int = 0) -> tuple[list[AssistantEvent], int]:
    """Read fp from start_offset, yield complete events, return (events, new_offset).

    new_offset advances only to the last complete newline so a partial trailing
    line is re-read on the next poll.
    """
    events: list[AssistantEvent] = []
    try:
        with open(fp, "rb") as f:
            f.seek(start_offset)
            data = f.read()
    except OSError:
        return events, start_offset

    if not data:
        return events, start_offset

    last_nl = data.rfind(b"\n")
    if last_nl < 0:
        return events, start_offset
    complete = data[: last_nl + 1].decode("utf-8", errors="replace")
    new_offset = start_offset + last_nl + 1

    base_line_no = _approx_line_no(fp, start_offset)
    for i, line in enumerate(complete.splitlines()):
        ev = parse_claude_line(line, fp, base_line_no + i)
        if ev is not None:
            events.append(ev)
    return events, new_offset


def iter_codex_events_from_file(fp: str) -> list[AssistantEvent]:
    """Re-read a single Codex rollout file end-to-end. Caller relies on uuid dedup."""
    events: list[AssistantEvent] = []
    session_id = os.path.basename(fp).replace(".jsonl", "")
    current_model = "?"
    try:
        with open(fp) as f:
            for line_no, line in enumerate(f, start=1):
                rec = _safe_load(line)
                if not rec:
                    continue
                pl = rec.get("payload") or {}
                if rec.get("type") == "turn_context":
                    current_model = pl.get("model") or current_model
                    continue
                if rec.get("type") != "event_msg":
                    continue
                if pl.get("type") != "token_count":
                    continue
                info = pl.get("info")
                if not info:
                    continue
                last = info.get("last_token_usage") or {}
                if not last:
                    continue
                total_input = last.get("input_tokens", 0)
                cached = last.get("cached_input_tokens", 0)
                uncached_input = max(0, total_input - cached)
                output = last.get("output_tokens", 0) + last.get("reasoning_output_tokens", 0)
                events.append(AssistantEvent(
                    source="codex",
                    uuid=f"{session_id}:{line_no}",
                    session_id=session_id,
                    timestamp=parse_ts(rec.get("timestamp")),
                    model=current_model,
                    usage={
                        "input_tokens": uncached_input,
                        "cache_read_input_tokens": cached,
                        "cache_creation_input_tokens": 0,
                        "output_tokens": output,
                    },
                    tools=[],
                    source_file=fp,
                ))
    except OSError:
        return events
    return events


def _approx_line_no(fp: str, start_offset: int) -> int:
    """Count lines in fp up to start_offset (1-indexed for the next line)."""
    if start_offset == 0:
        return 1
    try:
        with open(fp, "rb") as f:
            head = f.read(start_offset)
        return head.count(b"\n") + 1
    except OSError:
        return 1


def iter_events(days_back: int, sources: list[str] | None = None) -> Iterator[AssistantEvent]:
    """Yield events across all configured sources."""
    sources = sources or ["claude", "codex"]
    if "claude" in sources:
        yield from iter_claude_events(days_back)
    if "codex" in sources:
        yield from iter_codex_events(days_back)


def _safe_load(line: str):
    try:
        return json.loads(line)
    except (json.JSONDecodeError, ValueError):
        return None
