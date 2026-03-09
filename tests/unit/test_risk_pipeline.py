from __future__ import annotations

from datetime import datetime
import unittest

from yuruquant.app.config_schema import RiskThrottleStep
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

    def test_drawdown_schedule_throttles_without_hard_halt_when_disabled(self):
        state = PortfolioRuntime(
            initial_equity=500000.0,
            current_equity=500000.0,
            equity_peak=500000.0,
            daily_start_equity=480000.0,
            current_date="2026-01-06",
        )
        decision = evaluate_portfolio_guard(
            state=state,
            snapshot=PortfolioSnapshot(equity=440000.0, cash=440000.0),
            max_daily_loss_ratio=0.20,
            max_drawdown_halt_ratio=0.15,
            trade_time=datetime(2026, 1, 6, 14, 0, 0),
            fallback_equity=500000.0,
            drawdown_halt_mode='disabled',
            drawdown_risk_schedule=(
                RiskThrottleStep(drawdown_ratio=0.08, risk_mult=0.50),
                RiskThrottleStep(drawdown_ratio=0.12, risk_mult=0.25),
            ),
        )
        self.assertTrue(decision.allow_entries)
        self.assertFalse(decision.force_flatten)
        self.assertEqual(state.risk_state, "throttle_drawdown")
        self.assertAlmostEqual(0.25, state.effective_risk_mult, places=6)
        self.assertFalse(state.halt_flag)

    def test_drawdown_hard_halt_remains_absorbing_by_default(self):
        state = PortfolioRuntime(
            initial_equity=500000.0,
            current_equity=500000.0,
            equity_peak=500000.0,
            daily_start_equity=480000.0,
            current_date="2026-01-06",
        )
        decision = evaluate_portfolio_guard(
            state=state,
            snapshot=PortfolioSnapshot(equity=420000.0, cash=420000.0),
            max_daily_loss_ratio=0.20,
            max_drawdown_halt_ratio=0.15,
            trade_time=datetime(2026, 1, 6, 14, 0, 0),
            fallback_equity=500000.0,
            drawdown_halt_mode='hard',
            drawdown_risk_schedule=(
                RiskThrottleStep(drawdown_ratio=0.08, risk_mult=0.50),
                RiskThrottleStep(drawdown_ratio=0.12, risk_mult=0.25),
            ),
        )
        self.assertFalse(decision.allow_entries)
        self.assertTrue(decision.force_flatten)
        self.assertEqual(state.risk_state, "halt_drawdown")
        self.assertTrue(state.halt_flag)


if __name__ == "__main__":
    unittest.main()
