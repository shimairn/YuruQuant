from datetime import datetime, timedelta
import unittest

from quantframe.app.config import load_config, load_resources
from quantframe.core.models import Bar, PortfolioSnapshot, Position
from quantframe.trend import DecisionContext
from strategies.trend.turtle_breakout import create_strategy


def _trend_bars(count: int = 120) -> list[Bar]:
    start = datetime(2026, 1, 1)
    rows = []
    for index in range(count):
        close = 100.0 + (index * 0.7)
        rows.append(
            Bar(
                instrument_id="DCE.P",
                symbol="DCE.P",
                frequency="1d",
                timestamp=start + timedelta(days=index),
                open=close - 0.3,
                high=close + 0.8,
                low=close - 0.8,
                close=close,
                volume=1000 + index,
            )
        )
    return rows


def _exit_bars() -> list[Bar]:
    rows = _trend_bars(119)
    rows.append(
        Bar(
            instrument_id="DCE.P",
            symbol="DCE.P",
            frequency="1d",
            timestamp=datetime(2026, 5, 1),
            open=120.0,
            high=121.0,
            low=108.0,
            close=109.0,
            volume=5000.0,
        )
    )
    return rows


class TrendStrategyTest(unittest.TestCase):
    def test_turtle_strategy_generates_long_target(self):
        config = load_config("resources/configs/gm_turtle_breakout.yaml")
        resources = load_resources(config)
        strategy = create_strategy(config, resources)
        instrument = resources.by_id["DCE.P"]
        context = DecisionContext(
            instrument=instrument,
            bars=tuple(_trend_bars()),
            portfolio=PortfolioSnapshot(equity=500000.0, cash=500000.0),
            position=Position(instrument_id="DCE.P", symbol="DCE.P", qty=0, avg_price=0.0),
        )
        decision = strategy.evaluate(context)
        self.assertIsNotNone(decision.signal)
        self.assertEqual(decision.signal.direction, 1)
        self.assertIsNotNone(decision.target)
        self.assertGreater(decision.target.target_qty, 0)
        self.assertEqual(len(decision.orders), 1)

    def test_turtle_strategy_exits_long_when_breaking_exit_channel(self):
        config = load_config("resources/configs/gm_turtle_breakout.yaml")
        resources = load_resources(config)
        strategy = create_strategy(config, resources)
        instrument = resources.by_id["DCE.P"]
        context = DecisionContext(
            instrument=instrument,
            bars=tuple(_exit_bars()),
            portfolio=PortfolioSnapshot(equity=500000.0, cash=500000.0),
            position=Position(instrument_id="DCE.P", symbol="DCE.P", qty=3, avg_price=130.0),
        )
        decision = strategy.evaluate(context)
        self.assertIsNotNone(decision.signal)
        self.assertEqual(decision.signal.direction, 0)
        self.assertEqual(decision.target.target_qty, 0)
        self.assertEqual(len(decision.orders), 1)


if __name__ == "__main__":
    unittest.main()
