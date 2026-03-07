from __future__ import annotations

import unittest
from datetime import datetime
from pathlib import Path

from strategy.config import load_config
from strategy.pipelines.risk import process_risk_pipeline
from strategy.pipelines.risk.signal_builder import build_position_risk_state
from strategy.types import RuntimeContext, SymbolState


class _StubAccount:
    def __init__(self, nav: float) -> None:
        self.cash = {"nav": nav}

    def position(self, symbol: str, side):
        _ = symbol
        _ = side
        return None


class _StubContext:
    def __init__(self) -> None:
        self.now = datetime(2026, 1, 5, 10, 0, 0)
        self._account = _StubAccount(500000.0)

    def account(self):
        return self._account


class RiskPipelineTest(unittest.TestCase):
    def test_hard_stop_generates_close_signal(self):
        cfg = load_config(Path("config/strategy.yaml"))
        runtime = RuntimeContext(cfg=cfg)
        runtime.context = _StubContext()

        state = SymbolState(csymbol="DCE.p", main_symbol="DCE.p2409")
        state.position_risk = build_position_risk_state(
            runtime=runtime,
            state=state,
            direction=1,
            entry_price=100.0,
            atr_val=1.0,
            campaign_id="demo",
            entry_eob=datetime(2026, 1, 5, 9, 0, 0),
        )

        stop_price = state.position_risk.initial_stop_loss
        should_stop, signal = process_risk_pipeline(
            runtime,
            state,
            "DCE.p",
            "DCE.p2409",
            datetime(2026, 1, 5, 10, 5, 0),
            current_price=stop_price - 0.1,
            atr_val=1.0,
            long_qty=3,
            short_qty=0,
        )

        self.assertTrue(should_stop)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "close_long")


if __name__ == "__main__":
    unittest.main()
