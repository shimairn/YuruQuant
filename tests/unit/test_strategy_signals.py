from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

from strategy.config import load_config
from strategy.core.kline_types import KlineFrame
from strategy.pipelines.entry import process_entry_pipeline
from strategy.types import RuntimeContext, SymbolState


def _frame_from_rows(rows: list[dict[str, object]]) -> KlineFrame:
    return KlineFrame(frame=pl.DataFrame(rows), symbol="DCE.p2409", frequency="300s")


class EntryPipelineTest(unittest.TestCase):
    def test_breakout_generates_buy_signal(self):
        cfg = load_config(Path("config/strategy.yaml"))
        cfg.strategy.breakout_lookback_5m = 10
        runtime = RuntimeContext(cfg=cfg)
        runtime.portfolio_risk.current_equity = 500000.0
        runtime.portfolio_risk.effective_risk_mult = 1.0

        state = SymbolState(csymbol="DCE.p")
        state.h1_trend = 1
        state.h1_strength = 0.8
        state.bar_index_5m = 100

        base = datetime(2026, 1, 5, 9, 0, 0)
        rows: list[dict[str, object]] = []
        price = 100.0
        for i in range(20):
            eob = base + timedelta(minutes=5 * i)
            high = price + 1.2
            low = price - 0.8
            close = price + 0.2
            vol = 1000 + (i % 10) * 20
            rows.append({"eob": eob, "open": price, "high": high, "low": low, "close": close, "volume": vol})
            price += 0.05

        # force a breakout on the latest bar
        rows[-1]["close"] = float(rows[-2]["high"]) + 2.0
        rows[-1]["high"] = float(rows[-1]["close"]) + 0.4
        rows[-1]["volume"] = 3000.0

        frame = _frame_from_rows(rows)
        signal = process_entry_pipeline(
            runtime,
            state,
            "DCE.p",
            "DCE.p2409",
            frame,
            rows[-1]["eob"],
            float(rows[-1]["close"]),
            atr_val=1.2,
            long_qty=0,
            short_qty=0,
        )
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "buy")
        self.assertGreater(signal.qty, 0)


if __name__ == "__main__":
    unittest.main()
