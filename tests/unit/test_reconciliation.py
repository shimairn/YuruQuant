from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from yuruquant.reporting.reconciliation import build_reconciliation_row, reconcile_backtest_run


class ReconciliationTest(unittest.TestCase):
    def _write_headers(self, signals_path: Path, executions_path: Path, portfolio_path: Path) -> None:
        with signals_path.open('w', newline='', encoding='utf-8') as handle:
            writer = csv.writer(handle)
            writer.writerow([
                'run_id', 'mode', 'ts', 'csymbol', 'symbol', 'action', 'reason', 'direction', 'qty', 'price', 'stop_or_trigger',
                'campaign_id', 'environment_ma', 'macd_histogram', 'protected_stop_price', 'phase', 'mfe_r'
            ])
        with executions_path.open('w', newline='', encoding='utf-8') as handle:
            writer = csv.writer(handle)
            writer.writerow([
                'run_id', 'mode', 'signal_ts', 'fill_ts', 'csymbol', 'symbol', 'campaign_id', 'signal_action', 'signal_price', 'fill_price', 'execution_regime', 'fill_gap_points', 'fill_gap_atr',
                'request_id', 'intended_action', 'intended_qty', 'accepted', 'reason', 'event_timestamp'
            ])
        with portfolio_path.open('w', newline='', encoding='utf-8') as handle:
            writer = csv.writer(handle)
            writer.writerow([
                'run_id', 'mode', 'date', 'snapshot_ts', 'equity_start', 'equity_end', 'equity_peak', 'drawdown_ratio', 'risk_state', 'effective_risk_mult',
                'trades_count', 'wins', 'losses', 'realized_pnl', 'halt_flag', 'halt_reason'
            ])

    def test_reconcile_backtest_run_aligned(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            signals_path = root / 'signals.csv'
            executions_path = root / 'executions.csv'
            portfolio_path = root / 'portfolio_daily.csv'
            self._write_headers(signals_path, executions_path, portfolio_path)

            with signals_path.open('a', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow(['r', 'BACKTEST', '2026-01-01 09:00:00', 'DCE.P', 'DCE.p2605', 'buy', 'breakout', '1', '1', '100', '95', 'c1', '', '', '100.5', '', ''])
                writer.writerow(['r', 'BACKTEST', '2026-01-01 10:00:00', 'DCE.P', 'DCE.p2605', 'close_long', 'protected stop', '1', '1', '110', 'protected_stop', 'c1', '', '', '', 'protected', '1.5'])
            with executions_path.open('a', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow(['r', 'BACKTEST', '2026-01-01 09:00:00', '2026-01-01 09:00:00', 'DCE.P', 'DCE.p2605', 'c1', 'buy', '100', '100', 'normal', '0', '0', 'e1', 'buy:open_long', '1', '1', 'submitted', '2026-01-01T09:00:00'])
                writer.writerow(['r', 'BACKTEST', '2026-01-01 10:00:00', '2026-01-01 10:00:00', 'DCE.P', 'DCE.p2605', 'c1', 'close_long', '110', '110', 'normal', '0', '0', 'e2', 'close_long', '0', '1', 'submitted', '2026-01-01T10:00:00'])
            with portfolio_path.open('a', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow(['r', 'BACKTEST', '2026-01-01', '2026-01-01 15:00:00', '500000', '500100', '500100', '0.0', 'normal', '1.0', '1', '1', '0', '100', '0', ''])

            result = reconcile_backtest_run(signals_path, portfolio_path, {'DCE.P': 10.0}, executions_path)

            self.assertEqual('aligned', result.status)
            self.assertAlmostEqual(0.0, result.pnl_gap)
            self.assertEqual(tuple(), result.issues)
            self.assertAlmostEqual(100.0, build_reconciliation_row(result)['portfolio_net_profit'])

    def test_reconcile_backtest_run_flags_session_restart_gap_divergence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            signals_path = root / 'signals.csv'
            executions_path = root / 'executions.csv'
            portfolio_path = root / 'portfolio_daily.csv'
            self._write_headers(signals_path, executions_path, portfolio_path)

            with signals_path.open('a', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow(['r', 'BACKTEST', '2026-01-02 09:00:00', 'DCE.P', 'DCE.p2605', 'buy', 'breakout', '1', '1', '100', '95', 'c1', '', '', '100.5', '', ''])
                writer.writerow(['r', 'BACKTEST', '2026-01-02 10:00:00', 'DCE.P', 'DCE.p2605', 'close_long', 'hard stop', '1', '1', '110', 'hard_stop', 'c1', '', '', '', 'armed', '0.3'])
            with executions_path.open('a', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow(['r', 'BACKTEST', '2026-01-02 09:00:00', '2026-01-02 09:00:00', 'DCE.P', 'DCE.p2605', 'c1', 'buy', '100', '100', 'normal', '0', '0', 'e1', 'buy:open_long', '1', '1', 'submitted', '2026-01-02T09:00:00'])
                writer.writerow(['r', 'BACKTEST', '2026-01-02 10:00:00', '2026-01-02 10:05:00', 'DCE.P', 'DCE.p2605', 'c1', 'close_long', '110', '108', 'session_restart_gap', '2', '1.0', 'e2', 'close_long', '0', '1', 'submitted', '2026-01-02T10:05:00'])
            with portfolio_path.open('a', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow(['r', 'BACKTEST', '2026-01-02', '2026-01-02 15:00:00', '500000', '500100', '500100', '0.0', 'normal', '1.0', '1', '1', '0', '100', '0', ''])

            result = reconcile_backtest_run(signals_path, portfolio_path, {'DCE.P': 10.0}, executions_path)

            issue_codes = {issue.code for issue in result.issues}
            self.assertEqual('diverged', result.status)
            self.assertAlmostEqual(20.0, result.pnl_gap)
            self.assertIn('pnl_mismatch', issue_codes)
            self.assertIn('session_restart_gap_present', issue_codes)

    def test_reconcile_backtest_run_flags_portfolio_halt_divergence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            signals_path = root / 'signals.csv'
            executions_path = root / 'executions.csv'
            portfolio_path = root / 'portfolio_daily.csv'
            self._write_headers(signals_path, executions_path, portfolio_path)

            with signals_path.open('a', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow(['r', 'BACKTEST', '2026-01-03 09:00:00', 'DCE.P', 'DCE.p2605', 'sell', 'breakout', '-1', '1', '300', '305', 'c1', '', '', '299', '', ''])
                writer.writerow(['r', 'BACKTEST', '2026-01-03 10:00:00', 'DCE.P', 'DCE.p2605', 'close_short', 'portfolio halt', '-1', '1', '310', 'portfolio_halt', 'c1', '', '', '', 'protected', '1.0'])
            with executions_path.open('a', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow(['r', 'BACKTEST', '2026-01-03 09:00:00', '2026-01-03 09:00:00', 'DCE.P', 'DCE.p2605', 'c1', 'sell', '300', '300', 'normal', '0', '0', 'e1', 'sell:open_short', '1', '1', 'submitted', '2026-01-03T09:00:00'])
                writer.writerow(['r', 'BACKTEST', '2026-01-03 10:00:00', '2026-01-03 10:05:00', 'DCE.P', 'DCE.p2605', 'c1', 'close_short', '310', '308', 'normal', '2', '0.8', 'e2', 'close_short', '0', '1', 'submitted', '2026-01-03T10:05:00'])
            with portfolio_path.open('a', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow(['r', 'BACKTEST', '2026-01-03', '2026-01-03 15:00:00', '500000', '499700', '500000', '0.0006', 'halt_drawdown', '0.0', '1', '0', '1', '-300', '1', 'drawdown=15.00%'])

            result = reconcile_backtest_run(signals_path, portfolio_path, {'DCE.P': 10.0}, executions_path)

            issue_codes = {issue.code for issue in result.issues}
            self.assertEqual('diverged', result.status)
            self.assertAlmostEqual(-220.0, result.pnl_gap)
            self.assertEqual(1, result.portfolio_truth['halt_days'])
            self.assertIn('pnl_mismatch', issue_codes)
            self.assertIn('portfolio_halt_present', issue_codes)

    def test_reconcile_backtest_run_requires_existing_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(FileNotFoundError):
                reconcile_backtest_run(root / 'signals.csv', root / 'portfolio_daily.csv', {'DCE.P': 10.0}, root / 'executions.csv')


if __name__ == '__main__':
    unittest.main()
