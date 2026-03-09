from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from yuruquant.app.config import load_config
from yuruquant.reporting.cost_overlay import apply_cost_overlay
from yuruquant.reporting.trade_records import build_trade_records


class CostOverlayTest(unittest.TestCase):
    def test_apply_cost_overlay_with_symbol_profile(self) -> None:
        config = load_config(Path('config/strategy.yaml'))
        config.universe.symbols = ['DCE.P']
        config.universe.instrument_overrides = {}

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            signals_path = root / 'signals.csv'
            executions_path = root / 'executions.csv'
            portfolio_path = root / 'portfolio_daily.csv'

            with signals_path.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow([
                    'run_id', 'mode', 'ts', 'csymbol', 'symbol', 'action', 'reason', 'direction', 'qty', 'price', 'stop_or_trigger',
                    'campaign_id', 'environment_ma', 'macd_histogram', 'protected_stop_price', 'phase', 'mfe_r'
                ])
                writer.writerow(['r', 'BACKTEST', '2026-01-01 09:00:00', 'DCE.P', 'DCE.p2605', 'buy', 'dual_core_breakout', '1', '2', '100', '95', 'c1', '', '', '101', '', ''])
                writer.writerow(['r', 'BACKTEST', '2026-01-01 10:00:00', 'DCE.P', 'DCE.p2605', 'close_long', 'protected stop', '1', '2', '110', 'protected_stop', 'c1', '', '', '', 'protected', '2.5'])

            with executions_path.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow([
                    'run_id', 'mode', 'signal_ts', 'fill_ts', 'csymbol', 'symbol', 'campaign_id', 'signal_action', 'signal_price', 'fill_price', 'execution_regime', 'fill_gap_points', 'fill_gap_atr',
                    'request_id', 'intended_action', 'intended_qty', 'accepted', 'reason', 'event_timestamp'
                ])
                writer.writerow(['r', 'BACKTEST', '2026-01-01 09:00:00', '2026-01-01 09:00:00', 'DCE.P', 'DCE.p2605', 'c1', 'buy', '100', '100', 'normal', '0', '0', 'e1', 'buy:open_long', '2', '1', 'submitted', '2026-01-01T09:00:00'])
                writer.writerow(['r', 'BACKTEST', '2026-01-01 10:00:00', '2026-01-01 10:00:00', 'DCE.P', 'DCE.p2605', 'c1', 'close_long', '110', '110', 'normal', '0', '0', 'e2', 'close_long', '0', '1', 'submitted', '2026-01-01T10:00:00'])

            with portfolio_path.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow([
                    'run_id', 'mode', 'date', 'snapshot_ts', 'equity_start', 'equity_end', 'equity_peak', 'drawdown_ratio', 'risk_state', 'effective_risk_mult',
                    'trades_count', 'wins', 'losses', 'realized_pnl', 'halt_flag', 'halt_reason'
                ])
                writer.writerow(['r', 'BACKTEST', '2026-01-01', '2026-01-01 15:00:00', '500000', '500200', '500200', '0.0', 'normal', '1.0', '1', '1', '0', '200', '0', ''])

            trades = build_trade_records(signals_path, {'DCE.P': 10.0}, executions_path)
            realistic = {
                'DCE.P': type('Row', (), {
                    'commission_ratio_per_side': 0.001,
                    'slippage_ticks_per_side': 1.5,
                })()
            }
            result = apply_cost_overlay(trades, portfolio_path, config, 'realistic_top10_v1', profile_rows=realistic)

        self.assertEqual(1, len(result.trade_diagnostics))
        self.assertAlmostEqual(4200.0, result.trade_diagnostics[0]['turnover'], places=6)
        self.assertAlmostEqual(4.2, result.trade_diagnostics[0]['commission_cost'], places=6)
        self.assertAlmostEqual(60.0, result.trade_diagnostics[0]['slippage_cost'], places=6)
        self.assertAlmostEqual(64.2, result.trade_diagnostics[0]['total_cost'], places=6)
        self.assertAlmostEqual(135.8, result.trade_diagnostics[0]['actual_net_pnl'], places=6)
        self.assertEqual(12, result.trade_diagnostics[0]['holding_bars'])
        self.assertEqual(0, result.trade_diagnostics[0]['multi_session_hold'])
        self.assertEqual(1, result.trade_diagnostics[0]['protected_reach'])
        self.assertEqual(0, result.trade_diagnostics[0]['overnight_hold'])
        self.assertEqual(0, result.trade_diagnostics[0]['trading_day_flat_exit'])
        self.assertAlmostEqual(4200.0, result.summary['turnover'], places=6)
        self.assertAlmostEqual(200.0, result.summary['gross_pnl'], places=6)
        self.assertAlmostEqual(4.2, result.summary['commission_cost'], places=6)
        self.assertAlmostEqual(60.0, result.summary['slippage_cost'], places=6)
        self.assertAlmostEqual(64.2, result.summary['total_cost'], places=6)
        self.assertAlmostEqual(135.8, result.summary['net_pnl'], places=6)
        self.assertAlmostEqual(64.2, result.summary['avg_cost_per_trade'], places=6)
        self.assertAlmostEqual(64.2 / 200.0, result.summary['cost_to_gross_pnl_ratio'], places=9)
        self.assertAlmostEqual(64.2 / 4200.0, result.summary['cost_to_turnover_ratio'], places=9)
        self.assertAlmostEqual(135.8 / 500000.0, result.summary['net_return_ratio'], places=9)
        self.assertEqual(1, result.summary['protected_reach_count'])
        self.assertAlmostEqual(1.0, result.summary['protected_reach_ratio'], places=9)
        self.assertEqual(0, result.summary['overnight_hold_count'])
        self.assertEqual(0, result.summary['session_flat_exit_count'])
        self.assertEqual(0, result.summary['trading_day_flat_exit_count'])
        self.assertAlmostEqual(1.0, result.summary['top_symbol_pnl_share'], places=9)
        self.assertEqual(1, result.summary['positive_symbol_count'])
        self.assertEqual(0, result.summary['portfolio_halt_count_costed'])
        self.assertEqual(1, len(result.portfolio_daily))
        self.assertAlmostEqual(4200.0, result.portfolio_daily[0]['cumulative_turnover'], places=6)
        self.assertAlmostEqual(4.2, result.portfolio_daily[0]['cumulative_commission_cost'], places=6)
        self.assertAlmostEqual(60.0, result.portfolio_daily[0]['cumulative_slippage_cost'], places=6)
        self.assertAlmostEqual(64.2, result.portfolio_daily[0]['cumulative_total_cost'], places=6)
        self.assertAlmostEqual(4200.0, result.portfolio_daily[0]['daily_turnover'], places=6)
        self.assertAlmostEqual(4.2, result.portfolio_daily[0]['daily_commission_cost'], places=6)
        self.assertAlmostEqual(60.0, result.portfolio_daily[0]['daily_slippage_cost'], places=6)
        self.assertAlmostEqual(64.2, result.portfolio_daily[0]['daily_total_cost'], places=6)
        self.assertEqual(1, len(result.symbol_cost_drag))
        self.assertAlmostEqual(4200.0, result.symbol_cost_drag[0]['turnover'], places=6)
        self.assertAlmostEqual(64.2, result.symbol_cost_drag[0]['total_cost'], places=6)
        self.assertAlmostEqual(64.2 / 4200.0, result.symbol_cost_drag[0]['cost_to_turnover_ratio'], places=9)
        self.assertAlmostEqual(64.2, result.symbol_cost_drag[0]['avg_cost_per_trade'], places=6)
        self.assertEqual(1, result.symbol_cost_drag[0]['protected_reach_count'])
        self.assertEqual(0, result.symbol_cost_drag[0]['overnight_hold_count'])
        self.assertEqual(0, result.symbol_cost_drag[0]['trading_day_flat_exit_count'])


if __name__ == '__main__':
    unittest.main()
