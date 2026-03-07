from __future__ import annotations

import csv
from pathlib import Path

from yuruquant.core.models import EntrySignal, ExecutionDiagnostics, ExecutionResult, ExitSignal, RuntimeState, Signal
from yuruquant.reporting.logging import warn


class CsvReportSink:
    SIGNAL_HEADER = [
        'run_id',
        'mode',
        'ts',
        'csymbol',
        'symbol',
        'action',
        'reason',
        'direction',
        'qty',
        'price',
        'stop_or_trigger',
        'campaign_id',
        'environment_ma',
        'macd_histogram',
        'protected_stop_price',
        'phase',
        'mfe_r',
    ]
    EXECUTION_HEADER = [
        'run_id',
        'mode',
        'signal_ts',
        'fill_ts',
        'csymbol',
        'symbol',
        'campaign_id',
        'signal_action',
        'signal_price',
        'fill_price',
        'execution_regime',
        'fill_gap_points',
        'fill_gap_atr',
        'request_id',
        'intended_action',
        'intended_qty',
        'accepted',
        'reason',
        'event_timestamp',
    ]
    PORTFOLIO_HEADER = [
        'run_id',
        'mode',
        'date',
        'snapshot_ts',
        'equity_start',
        'equity_end',
        'equity_peak',
        'drawdown_ratio',
        'risk_state',
        'effective_risk_mult',
        'trades_count',
        'wins',
        'losses',
        'realized_pnl',
        'halt_flag',
        'halt_reason',
    ]

    def __init__(self, output_dir: str, signals_filename: str, executions_filename: str, portfolio_daily_filename: str) -> None:
        self.output_dir = Path(output_dir)
        self.signals_filename = signals_filename
        self.executions_filename = executions_filename
        self.portfolio_daily_filename = portfolio_daily_filename

    def _reset_file(self, path: Path, header: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('w', newline='', encoding='utf-8') as handle:
            writer = csv.writer(handle)
            writer.writerow(header)

    def ensure_ready(self, runtime: RuntimeState, mode: str, run_id: str) -> None:
        _ = mode
        _ = run_id
        runtime.reports.signals_path = self.output_dir / self.signals_filename
        runtime.reports.executions_path = self.output_dir / self.executions_filename
        runtime.reports.portfolio_daily_path = self.output_dir / self.portfolio_daily_filename
        self._reset_file(runtime.reports.signals_path, self.SIGNAL_HEADER)
        self._reset_file(runtime.reports.executions_path, self.EXECUTION_HEADER)
        self._reset_file(runtime.reports.portfolio_daily_path, self.PORTFOLIO_HEADER)

    def record_signal(self, runtime: RuntimeState, mode: str, run_id: str, csymbol: str, symbol: str, signal: Signal) -> None:
        path = runtime.reports.signals_path
        if path is None:
            warn('report.signals_missing')
            return

        marker = ''
        environment_ma = ''
        macd_histogram = ''
        protected_stop_price = ''
        phase = ''
        mfe_r = ''

        if isinstance(signal, EntrySignal):
            marker = f'{signal.stop_loss:.6f}'
            environment_ma = f'{signal.environment_ma:.6f}'
            macd_histogram = f'{signal.macd_histogram:.6f}'
            protected_stop_price = f'{signal.protected_stop_price:.6f}'
        elif isinstance(signal, ExitSignal):
            marker = signal.exit_trigger
            phase = signal.phase
            mfe_r = f'{signal.mfe_r:.6f}'

        with path.open('a', newline='', encoding='utf-8') as handle:
            writer = csv.writer(handle)
            writer.writerow([
                run_id,
                mode,
                str(signal.created_at),
                csymbol,
                symbol,
                signal.action,
                signal.reason,
                int(signal.direction),
                int(signal.qty),
                f'{float(signal.price):.6f}',
                marker,
                signal.campaign_id,
                environment_ma,
                macd_histogram,
                protected_stop_price,
                phase,
                mfe_r,
            ])

    def record_executions(
        self,
        runtime: RuntimeState,
        mode: str,
        run_id: str,
        csymbol: str,
        symbol: str,
        signal: Signal,
        fill_ts: object,
        fill_price: float,
        diagnostics: ExecutionDiagnostics,
        results: list[ExecutionResult],
    ) -> None:
        path = runtime.reports.executions_path
        if path is None:
            warn('report.executions_missing')
            return
        with path.open('a', newline='', encoding='utf-8') as handle:
            writer = csv.writer(handle)
            for item in results:
                writer.writerow([
                    run_id,
                    mode,
                    str(signal.created_at),
                    str(fill_ts),
                    csymbol,
                    symbol,
                    signal.campaign_id,
                    signal.action,
                    f'{float(signal.price):.6f}',
                    f'{float(fill_price):.6f}',
                    diagnostics.execution_regime,
                    f'{float(diagnostics.fill_gap_points):.6f}',
                    f'{float(diagnostics.fill_gap_atr):.6f}',
                    getattr(item, 'request_id', ''),
                    getattr(item, 'intended_action', ''),
                    int(getattr(item, 'intended_qty', 0) or 0),
                    int(bool(getattr(item, 'accepted', False))),
                    getattr(item, 'reason', ''),
                    getattr(item, 'timestamp', ''),
                ])

    def record_portfolio_day(self, runtime: RuntimeState, mode: str, run_id: str, trade_day: str, snapshot_ts: object) -> None:
        path = runtime.reports.portfolio_daily_path
        if path is None:
            return
        portfolio = runtime.portfolio
        equity_end = portfolio.current_equity if portfolio.current_equity > 0 else max(portfolio.initial_equity, portfolio.daily_start_equity)
        with path.open('a', newline='', encoding='utf-8') as handle:
            writer = csv.writer(handle)
            writer.writerow([
                run_id,
                mode,
                trade_day,
                str(snapshot_ts),
                f'{float(portfolio.daily_start_equity):.6f}',
                f'{float(equity_end):.6f}',
                f'{float(portfolio.equity_peak):.6f}',
                f'{float(portfolio.drawdown_ratio):.6f}',
                portfolio.risk_state,
                f'{float(portfolio.effective_risk_mult):.6f}',
                int(portfolio.trades_count),
                int(portfolio.wins),
                int(portfolio.losses),
                f'{float(portfolio.realized_pnl):.6f}',
                int(bool(portfolio.halt_flag)),
                portfolio.halt_reason,
            ])


