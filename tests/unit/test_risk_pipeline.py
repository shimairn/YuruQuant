from __future__ import annotations

from datetime import datetime
import unittest

from yuruquant.core.models import PortfolioRuntime, PortfolioSnapshot
from yuruquant.portfolio.risk import evaluate_portfolio_guard


class RiskPipelineTest(unittest.TestCase):
    def test_daily_loss_halts_portfolio(self):
        state = PortfolioRuntime(initial_equity=500000.0, current_equity=500000.0, equity_peak=500000.0, daily_start_equity=500000.0, current_date="2026-01-05")
        decision = evaluate_portfolio_guard(
            state=state,
            snapshot=PortfolioSnapshot(equity=470000.0, cash=470000.0),
            max_daily_loss_ratio=0.05,
            max_drawdown_halt_ratio=0.15,
            trade_time=datetime(2026, 1, 5, 14, 0, 0),
            fallback_equity=500000.0,
        )
        self.assertFalse(decision.allow_entries)
        self.assertTrue(decision.force_flatten)
        self.assertEqual(state.risk_state, "halt_daily_loss")


if __name__ == "__main__":
    unittest.main()
