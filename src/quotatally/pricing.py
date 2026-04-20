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

# OpenAI pricing is a placeholder — populate with verified figures from
# openai.com/pricing before shipping. Zeros are deliberate so users can see
# "this model needs pricing configured" rather than get silently-wrong
# Sonnet-rate numbers via the DEFAULT fallback.
OPENAI = {
    "gpt-5.4": Pricing(0.0, 0.0, 0.0, 0.0),
    "gpt-5.4-mini": Pricing(0.0, 0.0, 0.0, 0.0),
    "gpt-5.2": Pricing(0.0, 0.0, 0.0, 0.0),
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
