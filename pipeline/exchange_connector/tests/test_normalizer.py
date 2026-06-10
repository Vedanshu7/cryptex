"""Unit tests for the Binance message normalizer."""

import pytest

from exchange_connector.src.normalizer import normalize


class TestNormalize:
    def _valid_raw(self) -> dict[str, object]:
        return {
            "s": "btcusdt",
            "p": "50000.00",
            "q": "0.001",
            "T": 1_700_000_000_000,
        }

    def test_valid_message_returns_tick(self) -> None:
        raw = self._valid_raw()
        tick = normalize(raw)
        assert tick is not None
        assert tick.symbol == "BTCUSDT"
        assert tick.price == 50_000.0
        assert tick.volume == 0.001

    def test_symbol_uppercased(self) -> None:
        raw = self._valid_raw()
        raw["s"] = "ethusdt"
        tick = normalize(raw)
        assert tick is not None
        assert tick.symbol == "ETHUSDT"

    def test_missing_key_returns_none(self) -> None:
        raw = self._valid_raw()
        del raw["p"]
        result = normalize(raw)
        assert result is None

    def test_invalid_price_string_returns_none(self) -> None:
        raw = self._valid_raw()
        raw["p"] = "not_a_number"
        result = normalize(raw)
        assert result is None

    def test_zero_price_returns_none(self) -> None:
        raw = self._valid_raw()
        raw["p"] = "0.0"
        result = normalize(raw)
        assert result is None

    def test_negative_volume_returns_none(self) -> None:
        raw = self._valid_raw()
        raw["q"] = "-1.0"
        result = normalize(raw)
        assert result is None

    @pytest.mark.parametrize("bad_input", [{}, {"s": "BTC"}, None])
    def test_malformed_inputs_do_not_raise(self, bad_input: object) -> None:
        if bad_input is None:
            with pytest.raises((TypeError, AttributeError)):
                normalize(bad_input)  # type: ignore[arg-type]
        else:
            result = normalize(bad_input)  # type: ignore[arg-type]
            assert result is None
