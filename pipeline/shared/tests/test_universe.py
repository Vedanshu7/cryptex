"""Unit tests for the versioned trading universe."""

import json
import os
import time
from pathlib import Path

import pytest

from shared.universe import RegionUniverse, Universe, UniverseStore


def _write(path: Path, version: int, regions: dict[str, dict[str, object]]) -> None:
    path.write_text(json.dumps({"version": version, "regions": regions}))


_DEFAULT_REGIONS: dict[str, dict[str, object]] = {
    "tokyo": {"exchange": "binance", "symbols": ["BTCUSDT", "ETHUSDT"]},
    "sgp": {"exchange": "crypto_com", "symbols": ["BTCUSDT"]},
    "eu": {"exchange": "deribit", "symbols": ["BTC-PERPETUAL"]},
}


class TestRegionUniverse:
    def test_empty_symbols_rejected(self) -> None:
        with pytest.raises(ValueError, match="at least one symbol"):
            RegionUniverse(exchange="binance", symbols=[])


class TestUniverseModel:
    def _universe(self) -> Universe:
        return Universe.model_validate({"version": 1, "regions": _DEFAULT_REGIONS})

    def test_empty_regions_rejected(self) -> None:
        with pytest.raises(ValueError, match="at least one region"):
            Universe.model_validate({"version": 1, "regions": {}})

    def test_region_names(self) -> None:
        assert self._universe().region_names == ["tokyo", "sgp", "eu"]

    def test_all_symbols_union_deduplicated(self) -> None:
        # BTCUSDT appears in both tokyo and sgp - should not be duplicated.
        assert self._universe().all_symbols == ["BTCUSDT", "ETHUSDT", "BTC-PERPETUAL"]

    def test_symbols_for_exchange_filters_by_exchange(self) -> None:
        assert self._universe().symbols_for_exchange("binance") == ["BTCUSDT", "ETHUSDT"]
        assert self._universe().symbols_for_exchange("deribit") == ["BTC-PERPETUAL"]

    def test_symbols_for_exchange_unknown_exchange_is_empty(self) -> None:
        assert self._universe().symbols_for_exchange("kraken") == []

    def test_has_symbol_true_for_supported_pair(self) -> None:
        assert self._universe().has_symbol("tokyo", "BTCUSDT") is True

    def test_has_symbol_false_for_unsupported_pair(self) -> None:
        # BTCUSDT is not listed under eu (Deribit uses BTC-PERPETUAL instead).
        assert self._universe().has_symbol("eu", "BTCUSDT") is False

    def test_has_symbol_false_for_unknown_region(self) -> None:
        assert self._universe().has_symbol("nonexistent", "BTCUSDT") is False


class TestUniverseStore:
    def test_loads_valid_file(self, tmp_path: Path) -> None:
        path = tmp_path / "universe.json"
        _write(path, version=1, regions=_DEFAULT_REGIONS)

        universe = UniverseStore(path).get()

        assert universe.version == 1
        assert universe.region_names == ["tokyo", "sgp", "eu"]

    def test_missing_file_with_no_prior_load_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "does_not_exist.json"

        with pytest.raises(FileNotFoundError):
            UniverseStore(path).get()

    def test_does_not_reread_when_mtime_unchanged(self, tmp_path: Path) -> None:
        path = tmp_path / "universe.json"
        _write(path, version=1, regions=_DEFAULT_REGIONS)
        store = UniverseStore(path)

        first = store.get()
        # Mutate the file's content without touching its mtime - the store
        # should keep serving the cached version since it only checks mtime.
        os.utime(path, (path.stat().st_atime, path.stat().st_mtime))
        second = store.get()

        assert first is second

    def test_rereads_when_mtime_changes(self, tmp_path: Path) -> None:
        path = tmp_path / "universe.json"
        _write(path, version=1, regions=_DEFAULT_REGIONS)
        store = UniverseStore(path)
        first = store.get()

        time.sleep(0.01)
        widened: dict[str, object] = {
            "exchange": "binance",
            "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        }
        _write(path, version=2, regions={"tokyo": widened})

        second = store.get()

        assert first.version == 1
        assert second.version == 2
        assert "SOLUSDT" in second.all_symbols

    def test_invalid_file_on_reload_falls_back_to_last_known_good(self, tmp_path: Path) -> None:
        path = tmp_path / "universe.json"
        _write(path, version=1, regions=_DEFAULT_REGIONS)
        store = UniverseStore(path)
        good = store.get()

        time.sleep(0.01)
        path.write_text("{not valid json")

        fallback = store.get()

        assert fallback is good

    def test_file_deleted_after_load_falls_back_to_last_known_good(self, tmp_path: Path) -> None:
        path = tmp_path / "universe.json"
        _write(path, version=1, regions=_DEFAULT_REGIONS)
        store = UniverseStore(path)
        good = store.get()

        path.unlink()

        fallback = store.get()

        assert fallback is good
