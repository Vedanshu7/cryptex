"""Normalizes raw Binance WebSocket trade messages into Tick models."""

from datetime import datetime, timezone

from shared.logger import get_logger
from shared.models import Tick

_logger = get_logger(__name__)


def normalize(raw: dict[str, object]) -> Tick | None:
    """Parse a raw Binance @trade stream message into a normalized Tick.

    Returns None if the message is missing required fields or contains
    invalid values, allowing the caller to skip and continue.
    """
    try:
        return Tick(
            symbol=str(raw["s"]).upper(),
            price=float(str(raw["p"])),
            volume=float(str(raw["q"])),
            timestamp=datetime.fromtimestamp(
                int(str(raw["T"])) / 1000, tz=timezone.utc
            ),
        )
    except (KeyError, ValueError, TypeError) as exc:
        _logger.warning(
            "Failed to normalize tick message.",
            extra={"error": str(exc), "keys": list(raw.keys())},
        )
        return None
