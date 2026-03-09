from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from yuruquant.reporting.analysis import build_trade_diagnostics, build_trade_records, summarize_backtest_run, summarize_portfolio_daily


class ReportAnalysisTest(unittest.TestCase):
    def test_collapse_portfolio_daily_uses_last_snapshot_per_day(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            portfolio_path = Path(tmp) / 'portfolio_daily.csv'
            with portfolio_path.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow([
                    'run_id', 'mode', 'date', 'snapshot_ts', 'equity_start', 'equity_end', 'equity_peak', 'drawdown_ratio', 'risk_state', 'effective_risk_mult',
                    'trades_count', 'wins', 'losses', 'realized_pnl', 'halt_flag', 'halt_reason'
                ])
                writer.writerow(['r', 'BACKTEST', '2026-01-01', '2026-01-01 09:05:00', '500000', '499000', '500000', '0.002', 'normal', '1.0', '0', '0', '0', '0', '0', ''])
                writer.writerow(['r', 'BACKTEST', '2026-01-01', '2026-01-01 15:00:00', '500000', '501500', '501500', '0.000', 'normal', '1.0', '1', '1', '0', '1500', '0', ''])
                writer.writerow(['r', 'BACKTEST', '2026-01-02', '2026-01-02 15:00:00', '501500', '500500', '501500', '0.004', 'normal', '1.0', '2', '1', '1', '500', '0', ''])

            summary = summarize_portfolio_daily(portfolio_path)
            self.assertAlmostEqual(summary['start_equity'], 500000.0)
            self.assertAlmostEqual(summary['end_equity'], 500500.0)
            self.assertAlmostEqual(summary['net_profit'], 500.0)
            self.assertEqual(summary['days'], 2)

    def test_summarize_backtest_run_and_trade_diagnostics(self) -> None:
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
                writer.writerow(['r', 'BACKTEST', '2026-01-01 09:00:00', 'DCE.P', 'DCE.p2605', 'buy', 'dual_core_breakout', '1', '1', '100', '95', 'c1', '', '', '100.5', '', ''])
                writer.writerow(['r', 'BACKTEST', '2026-01-01 10:00:00', 'DCE.P', 'DCE.p2605', 'close_long', 'session flat', '1', '1', '115', 'session_flat', 'c1', '', '', '', 'protected', '3.2'])
                writer.writerow(['r', 'BACKTEST', '2026-01-02 09:00:00', 'DCE.P', 'DCE.p2605', 'buy', 'dual_core_breakout', '1', '1', '100', '95', 'c2', '', '', '100.5', '', ''])
                writer.writerow(['r', 'BACKTEST', '2026-01-02 10:00:00', 'DCE.P', 'DCE.p2605', 'close_long', 'hard stop', '1', '1', '92', 'hard_stop', 'c2', '', '', '', 'armed', '0.4'])
                writer.writerow(['r', 'BACKTEST', '2026-01-03 09:00:00', 'DCE.P', 'DCE.p2605', 'buy', 'dual_core_breakout', '1', '1', '200', '190', 'c3', '', '', '201', '', ''])
                writer.writerow(['r', 'BACKTEST', '2026-01-03 10:00:00', 'DCE.P', 'DCE.p2605', 'close_long', 'protected stop', '1', '1', '199', 'protected_stop', 'c3', '', '', '', 'protected', '2.7'])
                writer.writerow(['r', 'BACKTEST', '2026-01-04 09:00:00', 'DCE.P', 'DCE.p2605', 'sell', 'dual_core_breakout', '-1', '1', '300', '305', 'c4', '', '', '299', '', ''])
                writer.writerow(['r', 'BACKTEST', '2026-01-04 10:00:00', 'DCE.P', 'DCE.p2605', 'close_short', 'portfolio halt: drawdown=15.00%', '-1', '1', '310', 'portfolio_halt', 'c4', '', '', '', 'protected', '1.4'])

            with executions_path.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow([
                    'run_id', 'mode', 'signal_ts', 'fill_ts', 'csymbol', 'symbol', 'campaign_id', 'signal_action', 'signal_price', 'fill_price', 'execution_regime', 'fill_gap_points', 'fill_gap_atr',
                    'request_id', 'intended_action', 'intended_qty', 'accepted', 'reason', 'event_timestamp'
                ])
                writer.writerow(['r', 'BACKTEST', '2026-01-01 09:00:00', '2026-01-01 09:05:00', 'DCE.P', 'DCE.p2605', 'c1', 'buy', '100', '101', 'normal', '1', '1.0', 'e1', 'buy:open_long', '1', '1', 'submitted', '2026-01-01T09:05:00'])
                writer.writerow(['r', 'BACKTEST', '2026-01-01 10:00:00', '2026-01-01 10:05:00', 'DCE.P', 'DCE.p2605', 'c1', 'close_long', '115', '114', 'normal', '1', '1.0', 'e2', 'close_long', '0', '1', 'submitted', '2026-01-01T10:05:00'])
                writer.writerow(['r', 'BACKTEST', '2026-01-02 09:00:00', '2026-01-02 09:00:00', 'DCE.P', 'DCE.p2605', 'c2', 'buy', '100', '100', 'normal', '0', '0.0', 'e3', 'buy:open_long', '1', '1', 'submitted', '2026-01-02T09:00:00'])
                writer.writerow(['r', 'BACKTEST', '2026-01-02 10:00:00', '2026-01-02 10:05:00', 'DCE.P', 'DCE.p2605', 'c2', 'close_long', '92', '91', 'session_restart_gap', '1', '1.0', 'e4', 'close_long', '0', '1', 'submitted', '2026-01-02T10:05:00'])
                writer.writerow(['r', 'BACKTEST', '2026-01-03 09:00:00', '2026-01-03 09:00:00', 'DCE.P', 'DCE.p2605', 'c3', 'buy', '200', '200', 'normal', '0', '0.0', 'e5', 'buy:open_long', '1', '1', 'submitted', '2026-01-03T09:00:00'])
                writer.writerow(['r', 'BACKTEST', '2026-01-03 10:00:00', '2026-01-03 10:05:00', 'DCE.P', 'DCE.p2605', 'c3', 'close_long', '199', '199', 'normal', '0', '0.0', 'e6', 'close_long', '0', '1', 'submitted', '2026-01-03T10:05:00'])
                writer.writerow(['r', 'BACKTEST', '2026-01-04 09:00:00', '2026-01-04 09:05:00', 'DCE.P', 'DCE.p2605', 'c4', 'sell', '300', '300', 'normal', '0', '0.0', 'e7', 'sell:open_short', '1', '1', 'submitted', '2026-01-04T09:05:00'])
                writer.writerow(['r', 'BACKTEST', '2026-01-04 10:00:00', '2026-01-04 10:05:00', 'DCE.P', 'DCE.p2605', 'c4', 'close_short', '310', '308', 'session_restart_gap', '2', '0.8', 'e8', 'close_short', '0', '1', 'submitted', '2026-01-04T10:05:00'])

            with portfolio_path.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow([
                    'run_id', 'mode', 'date', 'snapshot_ts', 'equity_start', 'equity_end', 'equity_peak', 'drawdown_ratio', 'risk_state', 'effective_risk_mult',
                    'trades_count', 'wins', 'losses', 'realized_pnl', 'halt_flag', 'halt_reason'
                ])
                writer.writerow(['r', 'BACKTEST', '2026-01-01', '2026-01-01 15:00:00', '500000', '500150', '500150', '0.00', 'normal', '1.0', '1', '1', '0', '150', '0', ''])
                writer.writerow(['r', 'BACKTEST', '2026-01-02', '2026-01-02 15:00:00', '500150', '500070', '500150', '0.00', 'normal', '1.0', '2', '1', '1', '70', '0', ''])
                writer.writerow(['r', 'BACKTEST', '2026-01-03', '2026-01-03 15:00:00', '500070', '500060', '500150', '0.01', 'normal', '1.0', '3', '1', '2', '60', '0', ''])
                writer.writerow(['r', 'BACKTEST', '2026-01-04', '2026-01-04 15:00:00', '500060', '499960', '500150', '0.02', 'halt_drawdown', '0.0', '4', '1', '3', '-40', '1', 'drawdown=15.00%'])

            summary = summarize_backtest_run(signals_path, portfolio_path, {'DCE.P': 10.0}, executions_path)
            trades = build_trade_records(signals_path, {'DCE.P': 10.0}, executions_path)
            diagnostics = build_trade_diagnostics(trades)

            self.assertEqual(summary['trades'], 4)
            self.assertEqual(summary['wins'], 1)
            self.assertEqual(summary['losses'], 3)
            self.assertEqual(summary['hard_stop_count'], 1)
            self.assertEqual(summary['protected_stop_count'], 1)
            self.assertEqual(summary['session_flat_exit_count'], 1)
            self.assertEqual(summary['portfolio_halt_count'], 1)
            self.assertEqual(summary['session_restart_gap_exit_count'], 2)
            self.assertAlmostEqual(summary['session_restart_gap_exit_ratio'], 0.5)
            self.assertEqual(summary['session_restart_gap_portfolio_halt_count'], 1)
            self.assertAlmostEqual(summary['session_restart_gap_portfolio_halt_ratio'], 1.0)
            self.assertEqual(summary['session_restart_gap_stop_count'], 1)
            self.assertAlmostEqual(summary['session_restart_gap_stop_ratio'], 0.5)
            self.assertAlmostEqual(summary['session_restart_gap_overshoot_sum'], 40.0)
            self.assertAlmostEqual(summary['session_restart_gap_overshoot_ratio'], 40.0 / 60.0)
            self.assertAlmostEqual(summary['hard_stop_overshoot_avg'], 40.0)
            self.assertAlmostEqual(summary['hard_stop_overshoot_max'], 40.0)
            self.assertAlmostEqual(summary['protected_stop_overshoot_avg'], 20.0)
            self.assertAlmostEqual(summary['protected_stop_overshoot_max'], 20.0)
            self.assertAlmostEqual(summary['start_equity'], 500000.0)
            self.assertAlmostEqual(summary['end_equity'], 499960.0)
            self.assertAlmostEqual(summary['net_profit'], -40.0)
            self.assertAlmostEqual(summary['max_drawdown'], 0.02)
            self.assertEqual(summary['halt_days'], 1)

            self.assertEqual(len(diagnostics), 4)
            by_campaign = {row['campaign_id']: row for row in diagnostics}
            self.assertIsNone(by_campaign['c1']['theoretical_stop_price'])
            self.assertEqual(by_campaign['c1']['entry_fill_ts'], '2026-01-01 09:05:00')
            self.assertAlmostEqual(by_campaign['c1']['initial_stop_loss'], 96.0)
            self.assertAlmostEqual(by_campaign['c1']['protected_stop_price'], 101.5)
            self.assertAlmostEqual(by_campaign['c2']['overshoot_ratio'], 0.8)
            self.assertAlmostEqual(by_campaign['c3']['theoretical_stop_price'], 201.0)
            self.assertAlmostEqual(by_campaign['c3']['overshoot_pnl'], 20.0)
            self.assertEqual(by_campaign['c2']['exit_execution_regime'], 'session_restart_gap')
            self.assertAlmostEqual(by_campaign['c2']['exit_fill_gap_atr'], 1.0)
            self.assertEqual(by_campaign['c3']['phase_at_exit'], 'protected')
            self.assertEqual(by_campaign['c4']['exit_execution_regime'], 'session_restart_gap')
            self.assertAlmostEqual(by_campaign['c4']['actual_gross_pnl'], -80.0)
            self.assertIsNone(by_campaign['c4']['overshoot_pnl'])


if __name__ == '__main__':
    unittest.main()
