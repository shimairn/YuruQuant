from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from yuruquant.reporting.analysis import summarize_backtest_run


class ReportAnalysisTest(unittest.TestCase):
    def test_summarize_backtest_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            signals_path = root / 'signals.csv'
            portfolio_path = root / 'portfolio_daily.csv'
            with signals_path.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow([
                    'run_id', 'mode', 'ts', 'csymbol', 'symbol', 'action', 'reason', 'direction', 'qty', 'price', 'stop_or_trigger', 'campaign_id', 'environment_ma', 'macd_histogram'
                ])
                writer.writerow(['r', 'BACKTEST', '2026-01-01 09:00:00', 'DCE.P', 'DCE.p2605', 'buy', 'dual_core_breakout', '1', '2', '100', '95', 'c1', '', ''])
                writer.writerow(['r', 'BACKTEST', '2026-01-01 10:00:00', 'DCE.P', 'DCE.p2605', 'close_long', 'trend ma stop', '1', '2', '110', 'trend_ma_stop', 'c1', '', ''])
                writer.writerow(['r', 'BACKTEST', '2026-01-02 09:00:00', 'DCE.P', 'DCE.p2605', 'sell', 'dual_core_breakout', '-1', '1', '200', '205', 'c2', '', ''])
                writer.writerow(['r', 'BACKTEST', '2026-01-02 10:00:00', 'DCE.P', 'DCE.p2605', 'close_short', 'hard stop', '-1', '1', '206', 'hard_stop', 'c2', '', ''])
            with portfolio_path.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow([
                    'run_id', 'mode', 'date', 'equity_start', 'equity_end', 'equity_peak', 'drawdown_ratio', 'risk_state', 'effective_risk_mult', 'trades_count', 'wins', 'losses', 'realized_pnl', 'halt_flag', 'halt_reason'
                ])
                writer.writerow(['r', 'BACKTEST', '2026-01-01', '500000', '502000', '502000', '0.00', 'normal', '1.0', '1', '1', '0', '2000', '0', ''])
                writer.writerow(['r', 'BACKTEST', '2026-01-02', '502000', '501000', '502000', '0.01', 'normal', '1.0', '2', '1', '1', '1000', '0', ''])

            summary = summarize_backtest_run(signals_path, portfolio_path, {'DCE.P': 10.0})

            self.assertEqual(summary['trades'], 2)
            self.assertEqual(summary['wins'], 1)
            self.assertEqual(summary['losses'], 1)
            self.assertAlmostEqual(summary['win_rate'], 0.5)
            self.assertEqual(summary['hard_stop_count'], 1)
            self.assertEqual(summary['trend_ma_stop_count'], 1)
            self.assertAlmostEqual(summary['avg_win_pnl'], 200.0)
            self.assertAlmostEqual(summary['avg_loss_pnl'], -60.0)
            self.assertAlmostEqual(summary['avg_win_loss_ratio'], 3.3333333333, places=6)
            self.assertAlmostEqual(summary['start_equity'], 500000.0)
            self.assertAlmostEqual(summary['end_equity'], 501000.0)
            self.assertAlmostEqual(summary['net_profit'], 1000.0)
            self.assertAlmostEqual(summary['max_drawdown'], 0.01)
            self.assertEqual(summary['halt_days'], 0)


if __name__ == '__main__':
    unittest.main()
