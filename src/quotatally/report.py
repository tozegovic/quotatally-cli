"""Render a Summary as a markdown report."""
from __future__ import annotations

from io import StringIO

from .aggregator import Summary


def render(s: Summary, days: int, top: int = 15) -> str:
    out = StringIO()
    w = out.write

    w("# QuotaTally Report\n\n")
    w(f"*Scanned: last {days} days, {s.totals.msg_count:,} assistant messages, "
      f"{len(s.per_session_cost):,} sessions.*\n\n")

    w("## Token totals\n\n")
    w(f"- Input (uncached): **{s.totals.input_tokens:,}**\n")
    w(f"- Cache reads:      **{s.totals.cache_read:,}**\n")
    w(f"- Cache writes:     **{s.totals.cache_write:,}**\n")
    w(f"- Output:           **{s.totals.output_tokens:,}**\n")
    w(f"- **Cache hit rate: {s.cache_hit_rate:.1f}%**\n\n")

    w("## Per-tool cost\n\n")
    w("| Tool | Calls | Messages | Cost ($) | % |\n|---|---:|---:|---:|---:|\n")
    total = s.totals.cost or 1.0
    for tool, c in sorted(s.per_tool_cost.items(), key=lambda x: -x[1])[:top]:
        w(f"| `{tool}` | {s.per_tool_calls.get(tool,0):,} | "
          f"{s.per_tool_messages[tool]:,} | {c:.2f} | {c/total*100:.1f}% |\n")
    w("\n")

    w("## Daily cost\n\n| Date | Cost ($) |\n|---|---:|\n")
    for d in sorted(s.per_day_cost)[-top:]:
        w(f"| {d} | {s.per_day_cost[d]:.2f} |\n")
    w("\n")

    w(f"## Top {top} sessions\n\n| Session | Cost ($) | Distinct tools |\n|---|---:|---|\n")
    for sid, c in sorted(s.per_session_cost.items(), key=lambda x: -x[1])[:top]:
        tools = ", ".join(sorted(s.per_session_tools[sid])[:6])
        w(f"| `{sid[:8]}…` | {c:.2f} | {tools} |\n")
    w("\n")

    w("## Cost by model\n\n| Model | Cost ($) | % |\n|---|---:|---:|\n")
    for m, c in sorted(s.per_model_cost.items(), key=lambda x: -x[1]):
        w(f"| `{m}` | {c:.2f} | {c/total*100:.1f}% |\n")
    w("\n")

    w(f"**Total estimated cost (last {days} days): ${s.totals.cost:.2f}**\n")
    return out.getvalue()
