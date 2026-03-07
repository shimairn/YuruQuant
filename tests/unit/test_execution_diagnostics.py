from __future__ import annotations

import unittest
from datetime import datetime

from yuruquant.core.execution_diagnostics import build_execution_diagnostics
from yuruquant.core.models import ExitSignal, InstrumentSpec, ManagedPosition


class ExecutionDiagnosticsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = InstrumentSpec(
            multiplier=10.0,
            min_tick=1.0,
            min_lot=1,
            lot_step=1,
            sessions_day=[('09:00', '11:30'), ('13:30', '15:00')],
            sessions_night=[('21:00', '23:00')],
        )

    def test_session_restart_gap_uses_first_restart_stop_bar(self) -> None:
        position = ManagedPosition(
            entry_price=100.0,
            direction=1,
            qty=1,
            entry_atr=2.0,
            initial_stop_loss=95.6,
            stop_loss=101.0,
            protected_stop_price=101.0,
            phase='protected',
            campaign_id='demo',
            entry_eob=datetime(2026, 1, 5, 12, 0, 0),
            breakout_anchor=100.0,
            highest_price_since_entry=104.0,
            lowest_price_since_entry=100.0,
        )
        signal = ExitSignal(
            action='close_long',
            reason='protected stop',
            direction=1,
            qty=1,
            price=104.0,
            created_at=datetime(2026, 1, 5, 13, 5, 0),
            exit_trigger='protected_stop',
            campaign_id='demo',
            holding_bars=3,
            mfe_r=1.8,
            gross_pnl=40.0,
            net_pnl=35.0,
            phase='protected',
        )

        diagnostics = build_execution_diagnostics(signal, datetime(2026, 1, 5, 13, 10, 0), 104.5, self.spec, position)

        self.assertEqual('session_restart_gap', diagnostics.execution_regime)
        self.assertAlmostEqual(0.5, diagnostics.fill_gap_points)
        self.assertAlmostEqual(0.25, diagnostics.fill_gap_atr)

    def test_portfolio_halt_gap_uses_fill_window_and_fill_gap(self) -> None:
        position = ManagedPosition(
            entry_price=100.0,
            direction=-1,
            qty=1,
            entry_atr=2.0,
            initial_stop_loss=104.4,
            stop_loss=104.4,
            protected_stop_price=99.4,
            phase='armed',
            campaign_id='demo',
            entry_eob=datetime(2026, 1, 5, 14, 55, 0),
            breakout_anchor=100.0,
            highest_price_since_entry=100.0,
            lowest_price_since_entry=97.0,
        )
        signal = ExitSignal(
            action='close_short',
            reason='portfolio halt: drawdown=15.00%',
            direction=-1,
            qty=1,
            price=101.0,
            created_at=datetime(2026, 1, 5, 15, 0, 0),
            exit_trigger='portfolio_halt',
            campaign_id='demo',
            holding_bars=4,
            mfe_r=0.8,
            gross_pnl=-10.0,
            net_pnl=-12.0,
            phase='armed',
        )

        diagnostics = build_execution_diagnostics(signal, datetime(2026, 1, 6, 1, 5, 0), 101.8, self.spec, position)

        self.assertEqual('session_restart_gap', diagnostics.execution_regime)
        self.assertAlmostEqual(0.8, diagnostics.fill_gap_points)
        self.assertAlmostEqual(0.4, diagnostics.fill_gap_atr)


if __name__ == '__main__':
    unittest.main()
