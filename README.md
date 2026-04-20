# QuotaTally

Cross-client AI cost attribution from local session logs.

**Answers the question:** *"Why did my Claude Max quota drop 15% this morning?"*

QuotaTally reads your local Claude Code / Codex CLI / Cursor session logs and
tells you — per day, per tool, per model, per session — where your AI tokens
actually went. No gateway, no proxy, no telemetry back to us.

## Install

```
pipx install quotatally
```

Or with `uv`:

```
uvx quotatally report --days 14
```

## Quick start

```
quotatally report --days 14
```

Prints a markdown report with:

- Total token counts (input, output, cache reads, cache writes)
- Cache hit rate
- Per-tool cost and call counts
- Per-day cost (time series)
- Top sessions and messages by cost
- Per-model cost breakdown

## Status

Phase 1 — OSS daemon scaffold. Follows the [build spec](https://github.com/tozegovic/quotatally/blob/main/SPEC.md).

- Claude Code JSONL parsing
- Codex CLI parsing (stubbed)
- Cursor parsing (Phase 3)
- Local SQLite persistence (next)
- Watch-mode daemon (next)
- Hosted dashboard (Phase 2)

## License

MIT
