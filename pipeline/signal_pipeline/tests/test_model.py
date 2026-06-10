"""Unit tests for the LightGBM ML signal source."""

import pytest

from shared.models import TradeSide, TradeSignal
from signal_pipeline.src.features import build_features
from signal_pipeline.src.model import MLSignalSource, SignalGenerator

from .test_features import _rising_candles


@pytest.fixture(scope="module")
def source() -> MLSignalSource:
    """Shared source — warmed up once per module (LightGBM bootstrap is fast but not instant)."""
    src = MLSignalSource()
    src.warmup()
    return src


class TestMLSignalSourceInit:
    def test_model_none_before_warmup(self) -> None:
        src = MLSignalSource()
        assert src._model is None

    def test_model_fitted_after_warmup(self) -> None:
        src = MLSignalSource()
        src.warmup()
        assert src._model is not None

    def test_starts_in_warm_up_mode(self) -> None:
        src = MLSignalSource()
        assert not src.trained_on_real

    def test_signal_generator_alias(self) -> None:
        assert SignalGenerator is MLSignalSource


class TestPredict:
    def test_returns_trade_signal(self, source: MLSignalSource) -> None:
        candles  = _rising_candles(60)
        features = build_features(candles)
        assert features is not None
        result = source.predict("BTCUSDT", candles, features)
        assert isinstance(result, TradeSignal)

    def test_side_is_valid_enum_member(self, source: MLSignalSource) -> None:
        candles  = _rising_candles(60)
        features = build_features(candles)
        assert features is not None
        result = source.predict("BTCUSDT", candles, features)
        assert result.side in (TradeSide.BUY, TradeSide.SELL, TradeSide.HOLD)

    def test_confidence_in_range(self, source: MLSignalSource) -> None:
        candles  = _rising_candles(60)
        features = build_features(candles)
        assert features is not None
        result = source.predict("BTCUSDT", candles, features)
        assert 0.0 <= result.confidence <= 1.0

    def test_expires_after_generated(self, source: MLSignalSource) -> None:
        candles  = _rising_candles(60)
        features = build_features(candles)
        assert features is not None
        result = source.predict("BTCUSDT", candles, features)
        assert result.expires_at > result.generated_at

    def test_timestamps_are_utc_aware(self, source: MLSignalSource) -> None:
        candles  = _rising_candles(60)
        features = build_features(candles)
        assert features is not None
        result = source.predict("BTCUSDT", candles, features)
        assert result.generated_at.tzinfo is not None
        assert result.expires_at.tzinfo is not None

    def test_unique_signal_ids(self, source: MLSignalSource) -> None:
        candles  = _rising_candles(60)
        features = build_features(candles)
        assert features is not None
        r1 = source.predict("BTCUSDT", candles, features)
        r2 = source.predict("BTCUSDT", candles, features)
        assert r1.id != r2.id

    def test_symbol_preserved(self, source: MLSignalSource) -> None:
        candles  = _rising_candles(60)
        features = build_features(candles)
        assert features is not None
        result = source.predict("ETHUSDT", candles, features)
        assert result.symbol == "ETHUSDT"

    def test_source_field_is_ml(self, source: MLSignalSource) -> None:
        candles  = _rising_candles(60)
        features = build_features(candles)
        assert features is not None
        result = source.predict("BTCUSDT", candles, features)
        assert result.source == "ml"


class TestRetrain:
    def test_retrain_with_sufficient_candles_sets_trained_on_real(self) -> None:
        src = MLSignalSource()
        src.warmup()
        src.retrain({"BTCUSDT": _rising_candles(600)})
        assert src.trained_on_real

    def test_retrain_with_insufficient_candles_keeps_current_model(self) -> None:
        src       = MLSignalSource()
        src.warmup()
        old_model = src._model
        src.retrain({"BTCUSDT": _rising_candles(10)})
        assert src._model is old_model
        assert not src.trained_on_real

    def test_model_still_predicts_after_retrain(self) -> None:
        src = MLSignalSource()
        src.warmup()
        src.retrain({"BTCUSDT": _rising_candles(600)})
        candles  = _rising_candles(60)
        features = build_features(candles)
        assert features is not None
        result = src.predict("BTCUSDT", candles, features)
        assert isinstance(result, TradeSignal)
