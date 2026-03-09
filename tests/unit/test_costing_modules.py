from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from yuruquant.app.config import load_config
from yuruquant.reporting.cost_reports import build_platform_portfolio_daily_rows, build_summary
from yuruquant.reporting.costing import build_costed_trades, build_min_tick_lookup, build_platform_costed_trades, build_spec_lookup
from yuruquant.reporting.trade_records import TradeRecord


def _build_trade() -> TradeRecord:
    return TradeRecord(
        campaign_id='c1',
        csymbol='DCE.P',
        entry_signal_ts='2026-01-01 09:00:00',
        entry_fill_ts='2026-01-01 09:00:00',
        exit_signal_ts='2026-01-01 10:00:00',
        exit_fill_ts='2026-01-01 10:00:00',
        direction=1,
        qty=2,
        entry_signal_price=100.0,
        entry_fill_price=100.0,
        exit_signal_price=110.0,
        exit_fill_price=110.0,
        initial_stop_loss=95.0,
        protected_stop_price=101.0,
        exit_reason='protected stop',
        exit_trigger='protected_stop',
        phase_at_exit='protected',
        mfe_r=2.5,
        multiplier=10.0,
        pnl_points=10.0,
        gross_pnl=200.0,
        theoretical_stop_price=101.0,
        theoretical_stop_gross_pnl=20.0,
        overshoot_pnl=0.0,
        overshoot_ratio=0.0,
        exit_execution_regime='normal',
        exit_fill_gap_points=0.0,
        exit_fill_gap_atr=0.0,
    )


class CostingModulesTest(unittest.TestCase):
    def test_build_costed_trades_applies_profile_costs(self) -> None:
        config = load_config(Path('config/strategy.yaml'))
        specs = build_spec_lookup(config)
        min_ticks = build_min_tick_lookup(config)
        row = type('Row', (), {'commission_ratio_per_side': 0.001, 'slippage_ticks_per_side': 1.5})()

        costed = build_costed_trades(
            trades=[_build_trade()],
            cost_profile='realistic_top10_v1',
            specs=specs,
            min_ticks=min_ticks,
            frequency=config.universe.entry_frequency,
            profile_rows={'DCE.P': row},
        )

        self.assertEqual(1, len(costed))
        self.assertAlmostEqual(4200.0, costed[0].turnover, places=6)
        self.assertAlmostEqual(4.2, costed[0].commission_cost, places=6)
        self.assertAlmostEqual(60.0, costed[0].slippage_cost, places=6)
        self.assertAlmostEqual(135.8, costed[0].net_pnl, places=6)

    def test_build_summary_uses_platform_daily_rows_without_local_costs(self) -> None:
        config = load_config(Path('config/strategy.yaml'))
        costed = build_platform_costed_trades(
            trades=[_build_trade()],
            cost_profile='gm_builtin_unified',
            specs=build_spec_lookup(config),
            frequency=config.universe.entry_frequency,
        )

        with tempfile.TemporaryDirectory() as tmp:
            portfolio_path = Path(tmp) / 'portfolio_daily.csv'
            with portfolio_path.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow([
                    'run_id', 'mode', 'date', 'snapshot_ts', 'equity_start', 'equity_end', 'equity_peak', 'drawdown_ratio', 'risk_state', 'effective_risk_mult',
                    'trades_count', 'wins', 'losses', 'realized_pnl', 'halt_flag', 'halt_reason'
                ])
                writer.writerow(['r', 'BACKTEST', '2026-01-01', '2026-01-01 15:00:00', '500000', '500200', '500200', '0.0', 'normal', '1.0', '1', '1', '0', '200', '0', ''])

            daily_rows, halt_count = build_platform_portfolio_daily_rows(costed, portfolio_path)

        summary = build_summary(costed, daily_rows, halt_count)
        self.assertAlmostEqual(0.0, summary['total_cost'], places=6)
        self.assertAlmostEqual(200.0, summary['net_pnl'], places=6)
        self.assertEqual(0, summary['portfolio_halt_count_costed'])
        self.assertAlmostEqual(500200.0, summary['end_equity'], places=6)


if __name__ == '__main__':
    unittest.main()
