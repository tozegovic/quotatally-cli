"""Model pricing and per-message cost calculation.

Pricing is list-price per 1M tokens (April 2026). The hosted tier will pull
live pricing from the provider API; the OSS core keeps a static table that
users can override.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Pricing:
    input: float
    output: float
    cache_read: float
    cache_write: float


ANTHROPIC = {
    "claude-sonnet-4-6": Pricing(3.00, 15.00, 0.30, 3.75),
    "claude-opus-4-7":   Pricing(15.00, 75.00, 1.50, 18.75),
    "claude-haiku-4-5":  Pricing(0.80, 4.00, 0.08, 1.00),
}

# OpenAI pricing, verified against developers.openai.com/api/docs/pricing
# and platform.openai.com/docs/models/gpt-5.2 (April 2026).
# OpenAI prompt-caching has no separate "write" rate — cache creation is
# billed at the regular input rate, so cache_write=0 here (cache_creation
# tokens are always 0 in our Codex parser output anyway).
# IMPORTANT: order matters — `resolve()` does a substring match, so the more
# specific keys (gpt-5.4-mini, gpt-5.2-codex) must come before less specific
# prefixes (gpt-5.4, gpt-5.2).
OPENAI = {
    "gpt-5.4-mini": Pricing(0.75, 4.50, 0.075, 0.0),
    "gpt-5.4":      Pricing(2.50, 15.00, 0.25, 0.0),
    "gpt-5.2-codex": Pricing(1.75, 14.00, 0.175, 0.0),  # assumed same as gpt-5.2
    "gpt-5.2":       Pricing(1.75, 14.00, 0.175, 0.0),  # scheduled for retirement 2026-06-05
}

ALL = {**ANTHROPIC, **OPENAI}

DEFAULT = Pricing(3.00, 15.00, 0.30, 3.75)


def resolve(model: str | None) -> Pricing:
    if not model:
        return DEFAULT
    for key, p in ALL.items():
        if key in model:
            return p
    return DEFAULT


def cost_of(usage: dict, model: str | None) -> float:
    """Return $ cost for a single assistant message's usage block."""
    p = resolve(model)
    inp = usage.get("input_tokens", 0)
    out = usage.get("output_tokens", 0)
    cr = usage.get("cache_read_input_tokens", 0)
    cc = usage.get("cache_creation_input_tokens", 0)
    return (inp * p.input + out * p.output + cr * p.cache_read + cc * p.cache_write) / 1_000_000
