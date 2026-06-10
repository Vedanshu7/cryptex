"""Unit tests for the LightGBM signal generator."""

import pytest

from shared.models import TradeSide, TradeSignal
from signal_pipeline.src.features import build_features
from signal_pipeline.src.model import SignalGenerator

from .test_features import _rising_candles


@pytest.fixture(scope="module")
def generator() -> SignalGenerator:
    """Shared generator — trains once per module (LightGBM is fast but not instant)."""
    return SignalGenerator()


class TestSignalGeneratorInit:
    def test_model_fitted_after_init(self, generator: SignalGenerator) -> None:
        assert generator._model is not None

    def test_starts_in_warm_up_mode(self) -> None:
        gen = SignalGenerator()
        assert not gen.trained_on_real


class TestPredict:
    def test_returns_trade_signal(self, generator: SignalGenerator) -> None:
        features = build_features(_rising_candles(60))
        assert features is not None
        result = generator.predict("BTCUSDT", features)
        assert isinstance(result, TradeSignal)

    def test_side_is_valid_enum_member(self, generator: SignalGenerator) -> None:
        features = build_features(_rising_candles(60))
        assert features is not None
        result = generator.predict("BTCUSDT", features)
        assert result.side in (TradeSide.BUY, TradeSide.SELL, TradeSide.HOLD)

    def test_confidence_in_range(self, generator: SignalGenerator) -> None:
        features = build_features(_rising_candles(60))
        assert features is not None
        result = generator.predict("BTCUSDT", features)
        assert 0.0 <= result.confidence <= 1.0

    def test_expires_after_generated(self, generator: SignalGenerator) -> None:
        features = build_features(_rising_candles(60))
        assert features is not None
        result = generator.predict("BTCUSDT", features)
        assert result.expires_at > result.generated_at

    def test_timestamps_are_utc_aware(self, generator: SignalGenerator) -> None:
        features = build_features(_rising_candles(60))
        assert features is not None
        result = generator.predict("BTCUSDT", features)
        assert result.generated_at.tzinfo is not None
        assert result.expires_at.tzinfo is not None

    def test_unique_signal_ids(self, generator: SignalGenerator) -> None:
        features = build_features(_rising_candles(60))
        assert features is not None
        r1 = generator.predict("BTCUSDT", features)
        r2 = generator.predict("BTCUSDT", features)
        assert r1.id != r2.id

    def test_symbol_preserved(self, generator: SignalGenerator) -> None:
        features = build_features(_rising_candles(60))
        assert features is not None
        result = generator.predict("ETHUSDT", features)
        assert result.symbol == "ETHUSDT"


class TestRetrain:
    def test_retrain_with_sufficient_candles_sets_trained_on_real(self) -> None:
        gen      = SignalGenerator()
        candles  = _rising_candles(600)
        gen.retrain({"BTCUSDT": candles})
        assert gen.trained_on_real

    def test_retrain_with_insufficient_candles_keeps_current_model(self) -> None:
        gen         = SignalGenerator()
        old_model   = gen._model
        gen.retrain({"BTCUSDT": _rising_candles(10)})
        # Model unchanged when not enough data
        assert gen._model is old_model
        assert not gen.trained_on_real

    def test_model_still_predicts_after_retrain(self) -> None:
        gen     = SignalGenerator()
        gen.retrain({"BTCUSDT": _rising_candles(600)})
        features = build_features(_rising_candles(60))
        assert features is not None
        result = gen.predict("BTCUSDT", features)
        assert isinstance(result, TradeSignal)
