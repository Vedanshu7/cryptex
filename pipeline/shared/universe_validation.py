"""Sanity checks for a trading universe beyond what the Universe schema enforces.

The Universe pydantic model (shared/universe.py) only guarantees structural
validity — every region has a non-empty exchange string and symbol list. It
can't catch a typo'd exchange name or a symbol pasted in the wrong exchange's
notation (e.g. a Binance-style "BTCUSDT" accidentally added under the eu/
Deribit region, which expects "BTC-PERPETUAL" style instrument names).

This module adds a second, semantic validation pass on top of that, meant to
be run before a universe.json edit is deployed — see scripts/validate_universe.py.
"""

import re
from dataclasses import dataclass
from typing import Literal

from shared.universe import Universe

Severity = Literal["error", "warning"]

# Notation patterns for the exchanges this platform currently integrates with.
# Deliberately permissive (a false-negative just skips a soft warning) rather
# than a precise instrument-name grammar for each exchange.
_KNOWN_EXCHANGE_PATTERNS: dict[str, re.Pattern[str]] = {
    "binance": re.compile(r"^[A-Z0-9]+$"),
    "crypto_com": re.compile(r"^[A-Z0-9]+_[A-Z0-9]+$"),
    "deribit": re.compile(r"^[A-Z0-9]+-[A-Z0-9-]+$"),
}


@dataclass(frozen=True)
class ValidationIssue:
    """One finding from validating a universe beyond basic schema checks."""

    severity: Severity
    region: str
    message: str


def validate_universe(universe: Universe) -> list[ValidationIssue]:
    """Check each region's exchange name and symbol notation.

    Returns:
        Issues found, most severe first. An unrecognized exchange name is an
        "error" (this platform's exchange clients — see engine/EMS — are
        hardcoded per known exchange, so a typo here has no client to route
        to at all). A symbol that doesn't match its exchange's usual notation
        is a "warning" — the pattern check isn't an exhaustive instrument-name
        grammar, so a real-but-unusual symbol could trip it.
    """
    issues: list[ValidationIssue] = []

    for region_name, region in universe.regions.items():
        pattern = _KNOWN_EXCHANGE_PATTERNS.get(region.exchange)

        if pattern is None:
            issues.append(
                ValidationIssue(
                    severity="error",
                    region=region_name,
                    message=(
                        f"Unrecognized exchange {region.exchange!r} — expected one of "
                        f"{sorted(_KNOWN_EXCHANGE_PATTERNS)}."
                    ),
                )
            )
            continue

        for symbol in region.symbols:
            if not pattern.match(symbol):
                issues.append(
                    ValidationIssue(
                        severity="warning",
                        region=region_name,
                        message=(
                            f"Symbol {symbol!r} doesn't look like a typical "
                            f"{region.exchange} instrument name."
                        ),
                    )
                )

    return sorted(issues, key=lambda i: i.severity != "error")
