from quotatally.pricing import DEFAULT, cost_of, resolve, ANTHROPIC, OPENAI


def test_resolve_known_model():
    assert resolve("claude-opus-4-7") is ANTHROPIC["claude-opus-4-7"]
    assert resolve("claude-sonnet-4-6-20260101") is ANTHROPIC["claude-sonnet-4-6"]


def test_resolve_unknown_falls_back():
    assert resolve("some-future-unknown-model") is DEFAULT
    assert resolve(None) is DEFAULT
    assert resolve("") is DEFAULT


def test_cost_of_opus():
    usage = {
        "input_tokens": 1_000_000,
        "output_tokens": 1_000_000,
        "cache_read_input_tokens": 1_000_000,
        "cache_creation_input_tokens": 1_000_000,
    }
    # Opus 4.7: 15 + 75 + 1.5 + 18.75 = 110.25
    assert cost_of(usage, "claude-opus-4-7") == 110.25


def test_cost_of_empty_usage_is_zero():
    assert cost_of({}, "claude-opus-4-7") == 0.0


def test_openai_resolve_picks_specific_over_prefix():
    # gpt-5.4-mini must NOT fall through to gpt-5.4 pricing.
    assert resolve("gpt-5.4-mini") is OPENAI["gpt-5.4-mini"]
    assert resolve("gpt-5.4") is OPENAI["gpt-5.4"]
    assert resolve("gpt-5.2") is OPENAI["gpt-5.2"]
    assert resolve("gpt-5.2-codex") is OPENAI["gpt-5.2-codex"]


def test_cost_of_gpt_5_4_mini():
    usage = {
        "input_tokens": 1_000_000,
        "output_tokens": 1_000_000,
        "cache_read_input_tokens": 1_000_000,
        "cache_creation_input_tokens": 0,
    }
    # 0.75 + 4.50 + 0.075 + 0 = 5.325
    assert cost_of(usage, "gpt-5.4-mini") == 5.325
