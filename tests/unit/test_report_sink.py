import tempfile
import unittest
from pathlib import Path

from yuruquant.core.models import RuntimeState
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

            self.assertEqual('run_id,mode,ts,csymbol,symbol,action,reason,direction,qty,price,stop_or_trigger,campaign_id,environment_ma,macd_histogram', signals.read_text(encoding='utf-8').strip())
            self.assertEqual('run_id,mode,ts,csymbol,symbol,request_id,intended_action,intended_qty,accepted,reason,event_timestamp', executions.read_text(encoding='utf-8').strip())
            self.assertEqual('run_id,mode,date,equity_start,equity_end,equity_peak,drawdown_ratio,risk_state,effective_risk_mult,trades_count,wins,losses,realized_pnl,halt_flag,halt_reason', portfolio.read_text(encoding='utf-8').strip())


if __name__ == '__main__':
    unittest.main()


