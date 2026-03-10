from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import yaml

from quantframe.app.runtime import main, reset_application
from quantframe.core.models import Bar, OrderResult, PortfolioSnapshot, Position
from quantframe.platforms.registry import register_platform


class FakePlatform:
    name = "fake"

    def __init__(self, config) -> None:
        self.config = config
        self.mode = config.runtime.mode
        self.context = None
        self.instruments = {}
        self.submitted = []

    def bind_context(self, context) -> None:
        self.context = context

    def initialize(self) -> None:
        return None

    def subscribe(self, instruments, frequency, history_bars) -> None:
        _ = frequency
        _ = history_bars
        self.instruments = {item.instrument_id: item for item in instruments}

    def fetch_history(self, instrument, frequency, count):
        _ = frequency
        start = datetime(2026, 1, 1)
        rows = []
        for index in range(count):
            close = 100.0 + (index * 0.5)
            rows.append(
                Bar(
                    instrument_id=instrument.instrument_id,
                    symbol=instrument.platform_symbol,
                    frequency="1d",
                    timestamp=start + timedelta(days=index),
                    open=close - 0.1,
                    high=close + 0.5,
                    low=close - 0.5,
                    close=close,
                    volume=1000 + index,
                )
            )
        return rows

    def normalize_bars(self, raw_bars):
        normalized = []
        for raw in raw_bars:
            instrument = self.instruments[raw["instrument_id"]]
            normalized.append(
                Bar(
                    instrument_id=raw["instrument_id"],
                    symbol=instrument.platform_symbol,
                    frequency=raw["frequency"],
                    timestamp=raw["timestamp"],
                    open=raw["open"],
                    high=raw["high"],
                    low=raw["low"],
                    close=raw["close"],
                    volume=raw["volume"],
                )
            )
        return normalized

    def get_portfolio_snapshot(self):
        return PortfolioSnapshot(equity=500000.0, cash=500000.0)

    def get_position(self, instrument):
        return Position(instrument_id=instrument.instrument_id, symbol=instrument.platform_symbol, qty=0, avg_price=0.0)

    def submit_orders(self, orders):
        self.submitted.extend(list(orders))
        return [OrderResult(request_id=str(index), accepted=True, reason="accepted") for index, _ in enumerate(orders, start=1)]

    def run(self, callbacks):
        callbacks.initialize(object())
        callbacks.on_bar(
            object(),
            [
                {
                    "instrument_id": "DCE.P",
                    "frequency": "1d",
                    "timestamp": datetime(2026, 4, 1),
                    "open": 150.0,
                    "high": 151.0,
                    "low": 149.0,
                    "close": 150.5,
                    "volume": 3000.0,
                }
            ],
        )


class RuntimeSmokeTest(unittest.TestCase):
    def tearDown(self) -> None:
        reset_application()

    def test_main_runs_with_fake_platform(self):
        register_platform("fake", lambda config: FakePlatform(config))
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            universe_path = base / "universe.yaml"
            instruments_path = base / "instruments.yaml"
            config_path = base / "config.yaml"
            universe_path.write_text(yaml.safe_dump({"symbols": ["DCE.P"]}, sort_keys=False), encoding="utf-8")
            instruments_path.write_text(
                yaml.safe_dump(
                    {
                        "instruments": {
                            "DCE.P": {
                                "platform_symbol": "DCE.P",
                                "multiplier": 10,
                                "tick_size": 1,
                                "lot_size": 1,
                            }
                        }
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "runtime": {"mode": "BACKTEST", "run_id": "smoke"},
                        "platform": {"name": "fake"},
                        "resources": {
                            "universe": str(universe_path),
                            "instruments": str(instruments_path),
                        },
                        "strategy": {
                            "factory": "strategies.trend.turtle_breakout:create_strategy",
                            "params": {
                                "decision_frequency": "1d",
                                "history_bars": 60,
                                "entry_window": 20,
                                "exit_window": 10,
                                "atr_window": 20,
                                "risk_per_trade_ratio": 0.01,
                                "atr_stop_multiple": 2.0,
                                "max_position_ratio": 0.20,
                                "max_abs_contracts": 5,
                            },
                        },
                        "reporting": {"enabled": True, "output_dir": str(base / "reports")},
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            exit_code = main(["--config", str(config_path)])
            self.assertEqual(exit_code, 0)
            self.assertTrue((base / "reports" / "signals.csv").exists())
            self.assertTrue((base / "reports" / "orders.csv").exists())


if __name__ == "__main__":
    unittest.main()
