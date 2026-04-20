"""Aggregate AssistantEvent streams into report-ready summaries."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable

from .parser import AssistantEvent
from .pricing import cost_of


@dataclass
class Totals:
    cost: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read: int = 0
    cache_write: int = 0
    msg_count: int = 0


@dataclass
class Summary:
    totals: Totals = field(default_factory=Totals)
    per_day_cost: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    per_tool_cost: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    per_tool_calls: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    per_tool_messages: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    per_model_cost: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    per_session_cost: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    per_session_tools: dict[str, set] = field(default_factory=lambda: defaultdict(set))
    top_messages: list[tuple] = field(default_factory=list)

    @property
    def cache_hit_rate(self) -> float:
        denom = self.totals.cache_read + self.totals.input_tokens
        return (self.totals.cache_read / denom * 100) if denom else 0.0


def aggregate(events: Iterable[AssistantEvent], top_k_messages: int = 50) -> Summary:
    s = Summary()
    for ev in events:
        cost = cost_of(ev.usage, ev.model)
        s.totals.cost += cost
        s.totals.msg_count += 1
        s.totals.input_tokens += ev.usage.get("input_tokens", 0)
        s.totals.output_tokens += ev.usage.get("output_tokens", 0)
        s.totals.cache_read += ev.usage.get("cache_read_input_tokens", 0)
        s.totals.cache_write += ev.usage.get("cache_creation_input_tokens", 0)

        if ev.timestamp:
            s.per_day_cost[ev.timestamp.date().isoformat()] += cost
        s.per_model_cost[ev.model] += cost
        s.per_session_cost[ev.session_id] += cost

        unique_tools = set(ev.tools) or {"<no-tool>"}
        share = cost / len(unique_tools)
        for t in ev.tools:
            s.per_tool_calls[t] += 1
            s.per_session_tools[ev.session_id].add(t)
        for t in unique_tools:
            s.per_tool_cost[t] += share
            s.per_tool_messages[t] += 1

        s.top_messages.append((cost, ev.model, ev.timestamp, tuple(unique_tools), ev.session_id))

    s.top_messages.sort(key=lambda x: -x[0])
    s.top_messages = s.top_messages[:top_k_messages]
    return s
