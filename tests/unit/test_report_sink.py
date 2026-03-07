from __future__ import annotations

import csv
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from yuruquant.core.models import EntrySignal, ExecutionDiagnostics, ExecutionResult, ExitSignal, RuntimeState
from yuruquant.reporting.csv_sink import CsvReportSink


class ReportSinkTest(unittest.TestCase):
    def test_ensure_ready_resets_existing_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            signals = output_dir / 'signals.csv'
            executions = output_dir / 'executions.csv'
            portfolio = output_dir / 'portfolio_daily.csv'
            for path in (signals, executions, portfolio):
                path.write_text('stale\nvalue\n', encoding='utf-8')

            sink = CsvReportSink(str(output_dir), 'signals.csv', 'executions.csv', 'portfolio_daily.csv')
            runtime = RuntimeState()
            sink.ensure_ready(runtime=runtime, mode='BACKTEST', run_id='run_001')

            self.assertEqual(
                'run_id,mode,ts,csymbol,symbol,action,reason,direction,qty,price,stop_or_trigger,campaign_id,environment_ma,macd_histogram,protected_stop_price,phase,mfe_r',
                signals.read_text(encoding='utf-8').strip(),
            )
            self.assertEqual(
                'run_id,mode,signal_ts,fill_ts,csymbol,symbol,campaign_id,signal_action,signal_price,fill_price,execution_regime,fill_gap_points,fill_gap_atr,request_id,intended_action,intended_qty,accepted,reason,event_timestamp',
                executions.read_text(encoding='utf-8').strip(),
            )
            self.assertEqual(
                'run_id,mode,date,snapshot_ts,equity_start,equity_end,equity_peak,drawdown_ratio,risk_state,effective_risk_mult,trades_count,wins,losses,realized_pnl,halt_flag,halt_reason',
                portfolio.read_text(encoding='utf-8').strip(),
            )

    def test_record_signal_and_execution_write_diagnostic_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            sink = CsvReportSink(str(output_dir), 'signals.csv', 'executions.csv', 'portfolio_daily.csv')
            runtime = RuntimeState()
            sink.ensure_ready(runtime=runtime, mode='BACKTEST', run_id='run_001')

            entry = EntrySignal(
                action='buy',
                reason='dual_core_breakout',
                direction=1,
                qty=2,
                price=100.0,
                stop_loss=97.8,
                protected_stop_price=100.6,
                created_at=datetime(2026, 1, 5, 9, 0, 0),
                entry_atr=1.0,
                breakout_anchor=100.5,
                campaign_id='c1',
                environment_ma=99.0,
                macd_histogram=0.4,
            )
            exit_signal = ExitSignal(
                action='close_long',
                reason='protected stop',
                direction=1,
                qty=2,
                price=100.2,
                created_at=datetime(2026, 1, 5, 9, 10, 0),
                exit_trigger='protected_stop',
                campaign_id='c1',
                holding_bars=2,
                mfe_r=1.6,
                gross_pnl=4.0,
                net_pnl=2.0,
                phase='protected',
            )

            sink.record_signal(runtime, 'BACKTEST', 'run_001', 'DCE.P', 'DCE.p2605', entry)
            sink.record_signal(runtime, 'BACKTEST', 'run_001', 'DCE.P', 'DCE.p2605', exit_signal)
            sink.record_executions(
                runtime,
                'BACKTEST',
                'run_001',
                'DCE.P',
                'DCE.p2605',
                entry,
                datetime(2026, 1, 5, 9, 5, 0),
                101.0,
                ExecutionDiagnostics(execution_regime='session_restart_gap', fill_gap_points=1.0, fill_gap_atr=1.0),
                [ExecutionResult('req-1', 'buy:open_long', 2, True, 'submitted', '2026-01-05T09:05:00')],
            )

            with (output_dir / 'signals.csv').open('r', encoding='utf-8', newline='') as handle:
                signal_rows = list(csv.DictReader(handle))
            with (output_dir / 'executions.csv').open('r', encoding='utf-8', newline='') as handle:
                execution_rows = list(csv.DictReader(handle))

            self.assertEqual(signal_rows[0]['protected_stop_price'], '100.600000')
            self.assertEqual(signal_rows[0]['phase'], '')
            self.assertEqual(signal_rows[0]['mfe_r'], '')
            self.assertEqual(signal_rows[1]['stop_or_trigger'], 'protected_stop')
            self.assertEqual(signal_rows[1]['phase'], 'protected')
            self.assertEqual(signal_rows[1]['mfe_r'], '1.600000')

            self.assertEqual(execution_rows[0]['campaign_id'], 'c1')
            self.assertEqual(execution_rows[0]['signal_action'], 'buy')
            self.assertEqual(execution_rows[0]['signal_price'], '100.000000')
            self.assertEqual(execution_rows[0]['fill_price'], '101.000000')
            self.assertEqual(execution_rows[0]['execution_regime'], 'session_restart_gap')
            self.assertEqual(execution_rows[0]['fill_gap_points'], '1.000000')
            self.assertEqual(execution_rows[0]['fill_gap_atr'], '1.000000')
            self.assertEqual(execution_rows[0]['fill_ts'], '2026-01-05 09:05:00')


if __name__ == '__main__':
    unittest.main()


