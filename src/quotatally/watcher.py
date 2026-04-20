"""Watch-mode daemon: poll JSONL log directories and ingest new events.

Strategy per source:
  - Claude: byte-offset tail. Each JSONL line is self-contained; resume from
    last_offset, advance only past complete newlines.
  - Codex: re-read full rollout file when its mtime advances. Files are small
    and the parser is stateful (model carried across turn_context events), so
    full re-read with uuid dedup is simpler and correct.
"""
from __future__ import annotations

import glob
import os
import time
from dataclasses import dataclass

from .db import Database
from .parser import (
    CLAUDE_PROJECTS_DIR,
    CODEX_SESSIONS_DIR,
    iter_claude_events_from_file,
    iter_codex_events_from_file,
)


@dataclass
class PollStats:
    files_scanned: int = 0
    events_inserted: int = 0
    events_skipped: int = 0


class Watcher:
    def __init__(
        self,
        db: Database,
        claude_dir: str = CLAUDE_PROJECTS_DIR,
        codex_dir: str = CODEX_SESSIONS_DIR,
        sources: tuple[str, ...] = ("claude", "codex"),
        user_id: str = "local",
        team_id: str = "local",
    ):
        self.db = db
        self.claude_dir = claude_dir
        self.codex_dir = codex_dir
        self.sources = sources
        self.user_id = user_id
        self.team_id = team_id
        self._codex_mtime: dict[str, float] = {}

    def run_once(self) -> PollStats:
        stats = PollStats()
        if "claude" in self.sources:
            self._poll_claude(stats)
        if "codex" in self.sources:
            self._poll_codex(stats)
        return stats

    def run_forever(self, interval: float = 5.0) -> None:
        while True:
            self.run_once()
            time.sleep(interval)

    def _poll_claude(self, stats: PollStats) -> None:
        if not os.path.isdir(self.claude_dir):
            return
        for fp in sorted(glob.glob(os.path.join(self.claude_dir, "**", "*.jsonl"), recursive=True)):
            try:
                size = os.path.getsize(fp)
            except OSError:
                continue
            offset = self.db.get_checkpoint(fp)
            if size <= offset:
                continue
            stats.files_scanned += 1
            events, new_offset = iter_claude_events_from_file(fp, start_offset=offset)
            if events:
                inserted, skipped = self.db.ingest(events, user_id=self.user_id, team_id=self.team_id)
                stats.events_inserted += inserted
                stats.events_skipped += skipped
            if new_offset != offset:
                self.db.set_checkpoint(fp, new_offset)

    def _poll_codex(self, stats: PollStats) -> None:
        if not os.path.isdir(self.codex_dir):
            return
        for fp in sorted(glob.glob(os.path.join(self.codex_dir, "**", "*.jsonl"), recursive=True)):
            try:
                mtime = os.path.getmtime(fp)
            except OSError:
                continue
            if self._codex_mtime.get(fp) == mtime:
                continue
            stats.files_scanned += 1
            events = iter_codex_events_from_file(fp)
            if events:
                inserted, skipped = self.db.ingest(events, user_id=self.user_id, team_id=self.team_id)
                stats.events_inserted += inserted
                stats.events_skipped += skipped
            self._codex_mtime[fp] = mtime
