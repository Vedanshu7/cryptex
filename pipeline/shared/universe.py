"""Versioned trading universe — per-region exchange and symbol assignment.

Backed by a plain JSON file (path via UNIVERSE_FILE) rather than a database
table, so operators can widen the universe by editing one file and letting
already-running services pick up the change on their own poll cadence — no
redeploy, no schema migration.

Regions are deliberately paired one-to-one with a specific exchange for
latency reasons — each region's OMS/EMS pair is deployed in the AWS zone
closest to that exchange's own infrastructure (see db/migrations/011).
Because of that pairing, a region's tradable symbols are capped by whatever
its one exchange actually lists, in that exchange's own notation (Binance
"BTCUSDT", Crypto.com "BTC_USDT", Deribit "BTC-PERPETUAL" are not
interchangeable strings) — so symbols are declared per-region, not as one
flat list shared across every region.

Each service calls get_universe() wherever it currently needs the active
region/symbol set instead of reading it once into a module-level constant at
import time. get_universe() only re-reads the file when its mtime changes,
so this is cheap to call on every loop iteration.
"""

import os
from pathlib import Path

from pydantic import BaseModel, field_validator

from shared.logger import get_logger

_logger = get_logger(__name__)

_DEFAULT_UNIVERSE_FILE = "universe.json"


class RegionUniverse(BaseModel):
    """The exchange and tradable symbols assigned to one region."""

    exchange: str
    symbols: list[str]

    @field_validator("symbols")
    @classmethod
    def symbols_nonempty(cls, value: list[str]) -> list[str]:
        """Reject a region with no tradable symbols."""
        if not value:
            raise ValueError("Region must declare at least one symbol.")
        return value


class Universe(BaseModel):
    """A versioned snapshot of each region's exchange and tradable symbols."""

    version: int
    regions: dict[str, RegionUniverse]

    @field_validator("regions")
    @classmethod
    def regions_nonempty(cls, value: dict[str, RegionUniverse]) -> dict[str, RegionUniverse]:
        """Reject a universe with no active regions."""
        if not value:
            raise ValueError("Universe must declare at least one region.")
        return value

    @property
    def region_names(self) -> list[str]:
        """All active region names, e.g. ["tokyo", "sgp", "eu"]."""
        return list(self.regions.keys())

    @property
    def all_symbols(self) -> list[str]:
        """Union of every symbol across all regions, deduplicated.

        Used by the signal pipeline, which predicts per-symbol regardless of
        which region(s) later trade that symbol — region filtering happens
        downstream in the signal router.
        """
        seen: dict[str, None] = {}
        for region in self.regions.values():
            for symbol in region.symbols:
                seen[symbol] = None
        return list(seen)

    def symbols_for_exchange(self, exchange: str) -> list[str]:
        """Union of symbols across all regions assigned to a given exchange.

        Used by the exchange connector, which today only ingests market data
        from a single exchange (Binance) regardless of how many regions
        reference it.
        """
        seen: dict[str, None] = {}
        for region in self.regions.values():
            if region.exchange == exchange:
                for symbol in region.symbols:
                    seen[symbol] = None
        return list(seen)

    def has_symbol(self, region: str, symbol: str) -> bool:
        """True if `region`'s exchange lists `symbol` in this universe.

        Used by the signal router to avoid routing a signal to a tenant
        whose region's exchange doesn't actually offer that symbol.
        """
        region_universe = self.regions.get(region)
        return region_universe is not None and symbol in region_universe.symbols


class UniverseStore:
    """Loads a Universe from disk, re-reading only when the file's mtime changes."""

    def __init__(self, path: Path | None = None) -> None:
        self._path = path or Path(os.getenv("UNIVERSE_FILE", _DEFAULT_UNIVERSE_FILE))
        self._mtime: float | None = None
        self._cached: Universe | None = None

    def get(self) -> Universe:
        """Return the current Universe, reloading from disk if it changed.

        Falls back to the last-known-good Universe if the file is missing or
        invalid on a reload attempt — a bad edit shouldn't take a running
        service down. Raises only if no Universe has ever loaded successfully.
        """
        try:
            mtime = self._path.stat().st_mtime
        except OSError as exc:
            if self._cached is not None:
                _logger.warning(
                    "Universe file unreadable — keeping last-known-good universe.",
                    extra={"path": str(self._path), "error": str(exc)},
                )
                return self._cached
            raise FileNotFoundError(
                f"Universe file not found and no prior universe loaded: {self._path}"
            ) from exc

        if mtime == self._mtime and self._cached is not None:
            return self._cached

        try:
            universe = Universe.model_validate_json(self._path.read_text())
        except Exception as exc:
            if self._cached is not None:
                _logger.warning(
                    "Universe file invalid — keeping last-known-good universe.",
                    extra={"path": str(self._path), "error": str(exc)},
                )
                return self._cached
            raise

        if self._cached is None or universe.version != self._cached.version:
            _logger.info(
                "Universe updated.",
                extra={
                    "version": universe.version,
                    "regions": {
                        name: {"exchange": r.exchange, "symbols": r.symbols}
                        for name, r in universe.regions.items()
                    },
                },
            )

        self._mtime = mtime
        self._cached = universe
        return universe


_store = UniverseStore()


def get_universe() -> Universe:
    """Return the current trading universe, reloading from disk if it changed."""
    return _store.get()
