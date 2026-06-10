"""Candle aggregator-specific types.

The aggregator uses the shared Candle model for Kafka messages.
This module holds aggregator-internal state types that are not
appropriate for the shared layer.
"""

from dataclasses import dataclass, field
from datetime import datetime

from shared.models import Tick


@dataclass
class WindowBuffer:
    """Holds the tick buffer and window metadata for a single symbol."""

    symbol: str
    window_start: datetime
    ticks: list[Tick] = field(default_factory=list)

    def is_empty(self) -> bool:
        """Return True when no ticks have been collected for this window."""
        return len(self.ticks) == 0
