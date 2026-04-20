from datetime import datetime, timezone

from quotatally.aggregator import aggregate
from quotatally.parser import AssistantEvent


def _event(tools, cost_tokens=1_000_000, session="s1", model="claude-sonnet-4-6",
           ts=datetime(2026, 4, 18, tzinfo=timezone.utc), uuid="u"):
    return AssistantEvent(
        source="claude",
        uuid=uuid,
        session_id=session,
        timestamp=ts,
        model=model,
        usage={"input_tokens": cost_tokens, "output_tokens": 0,
               "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
        tools=tools,
        source_file="/tmp/x.jsonl",
    )


def test_aggregate_splits_cost_across_tools():
    ev = _event(tools=["Bash", "Read"])  # 1M input tokens at sonnet = $3.00 total
    s = aggregate([ev])
    assert abs(s.totals.cost - 3.0) < 1e-9
    assert abs(s.per_tool_cost["Bash"] - 1.5) < 1e-9
    assert abs(s.per_tool_cost["Read"] - 1.5) < 1e-9


def test_aggregate_attributes_no_tool_when_empty():
    ev = _event(tools=[])
    s = aggregate([ev])
    assert "<no-tool>" in s.per_tool_cost
    assert abs(s.per_tool_cost["<no-tool>"] - 3.0) < 1e-9


def test_aggregate_per_day_and_per_model():
    e1 = _event(tools=["Bash"], ts=datetime(2026, 4, 17, tzinfo=timezone.utc))
    e2 = _event(tools=["Bash"], ts=datetime(2026, 4, 18, tzinfo=timezone.utc),
                model="claude-opus-4-7")
    s = aggregate([e1, e2])
    assert set(s.per_day_cost.keys()) == {"2026-04-17", "2026-04-18"}
    # sonnet input 1M = $3, opus input 1M = $15
    assert abs(s.per_model_cost["claude-sonnet-4-6"] - 3.0) < 1e-9
    assert abs(s.per_model_cost["claude-opus-4-7"] - 15.0) < 1e-9


def test_cache_hit_rate():
    ev = AssistantEvent(
        source="claude", uuid="u", session_id="s", timestamp=None, model="claude-sonnet-4-6",
        usage={"input_tokens": 10, "output_tokens": 0,
               "cache_read_input_tokens": 990, "cache_creation_input_tokens": 0},
        tools=[], source_file="",
    )
    s = aggregate([ev])
    assert abs(s.cache_hit_rate - 99.0) < 1e-9
