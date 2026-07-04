"""Unit tests for universe semantic validation, plus a live check of the
actual committed pipeline/universe.json — so a bad edit fails CI, not just a
manually-run script.
"""

from pathlib import Path

from shared.universe import Universe
from shared.universe_validation import validate_universe

_REPO_UNIVERSE_PATH = Path(__file__).resolve().parents[2] / "universe.json"


class TestValidateUniverse:
    def test_clean_universe_has_no_issues(self) -> None:
        universe = Universe.model_validate(
            {
                "version": 1,
                "regions": {
                    "tokyo": {"exchange": "binance", "symbols": ["BTCUSDT"]},
                    "sgp": {"exchange": "crypto_com", "symbols": ["BTC_USDT"]},
                    "eu": {"exchange": "deribit", "symbols": ["BTC-PERPETUAL"]},
                },
            }
        )

        assert validate_universe(universe) == []

    def test_unknown_exchange_is_an_error(self) -> None:
        universe = Universe.model_validate(
            {"version": 1, "regions": {"tokyo": {"exchange": "krakn", "symbols": ["BTCUSDT"]}}}
        )

        issues = validate_universe(universe)

        assert len(issues) == 1
        assert issues[0].severity == "error"
        assert "krakn" in issues[0].message

    def test_symbol_in_wrong_notation_is_a_warning(self) -> None:
        # Crypto.com notation uses an underscore; this looks like Binance's instead.
        universe = Universe.model_validate(
            {"version": 1, "regions": {"sgp": {"exchange": "crypto_com", "symbols": ["BTCUSDT"]}}}
        )

        issues = validate_universe(universe)

        assert len(issues) == 1
        assert issues[0].severity == "warning"

    def test_errors_sort_before_warnings(self) -> None:
        universe = Universe.model_validate(
            {
                "version": 1,
                "regions": {
                    "sgp": {"exchange": "crypto_com", "symbols": ["BTCUSDT"]},  # warning
                    "tokyo": {"exchange": "unknown_exchange", "symbols": ["X"]},  # error
                },
            }
        )

        issues = validate_universe(universe)

        assert issues[0].severity == "error"


class TestCommittedUniverseFile:
    def test_repo_universe_json_has_no_validation_errors(self) -> None:
        """The actual pipeline/universe.json must never regress to a bad state."""
        universe = Universe.model_validate_json(_REPO_UNIVERSE_PATH.read_text())

        issues = validate_universe(universe)
        errors = [i for i in issues if i.severity == "error"]

        assert errors == []
