from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from yuruquant.reporting.summary import summarize_portfolio_daily, summarize_trades
from yuruquant.reporting.trade_records import TradeRecord, build_trade_records


@dataclass(frozen=True)
class ReconciliationIssue:
    code: str
    detail: str


@dataclass(frozen=True)
class ReconciliationResult:
    status: str
    portfolio_truth: dict[str, float | int]
    reconstructed: dict[str, float | int]
    pnl_gap: float
    pnl_gap_ratio_to_start_equity: float
    issues: tuple[ReconciliationIssue, ...]


def _require_existing(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(path.as_posix())


def summarize_reconstructed_run(
    signals_path: Path,
    multiplier_by_csymbol: Mapping[str, float],
    executions_path: Path | None = None,
) -> tuple[dict[str, float | int], list[TradeRecord]]:
    _require_existing(signals_path)
    if executions_path is not None:
        _require_existing(executions_path)
    trades = build_trade_records(signals_path, multiplier_by_csymbol, executions_path)
    summary = summarize_trades(trades)
    gross_pnl = sum(float(trade.gross_pnl) for trade in trades)
    summary.update(
        {
            'reconstructed_gross_pnl': gross_pnl,
            'reconstructed_trade_count': len(trades),
            'reconstructed_win_rate': float(summary.get('win_rate', 0.0) or 0.0),
        }
    )
    return summary, trades


def reconcile_backtest_run(
    signals_path: Path,
    portfolio_daily_path: Path,
    multiplier_by_csymbol: Mapping[str, float],
    executions_path: Path | None = None,
    pnl_tolerance: float = 1e-6,
) -> ReconciliationResult:
    _require_existing(portfolio_daily_path)
    reconstructed_summary, trades = summarize_reconstructed_run(signals_path, multiplier_by_csymbol, executions_path)
    portfolio_truth = summarize_portfolio_daily(portfolio_daily_path)
    pnl_gap = float(portfolio_truth.get('net_profit', 0.0) or 0.0) - float(reconstructed_summary.get('reconstructed_gross_pnl', 0.0) or 0.0)
    start_equity = float(portfolio_truth.get('start_equity', 0.0) or 0.0)
    issues: list[ReconciliationIssue] = []

    if abs(pnl_gap) > max(float(pnl_tolerance), 0.0):
        issues.append(ReconciliationIssue('pnl_mismatch', f'GM net profit differs from reconstructed gross PnL by {pnl_gap:.6f}.'))
    if any(trade.exit_execution_regime == 'session_restart_gap' for trade in trades):
        count = sum(1 for trade in trades if trade.exit_execution_regime == 'session_restart_gap')
        issues.append(ReconciliationIssue('session_restart_gap_present', f'{count} trade(s) exited under session restart gap execution.'))
    if int(reconstructed_summary.get('portfolio_halt_count', 0) or 0) > 0 or int(portfolio_truth.get('halt_days', 0) or 0) > 0:
        issues.append(
            ReconciliationIssue(
                'portfolio_halt_present',
                'Portfolio halt activity is present in the run and can create divergence between structural diagnostics and GM equity truth.',
            )
        )

    status = 'aligned' if not issues or (len(issues) == 1 and issues[0].code != 'pnl_mismatch') else 'diverged'
    return ReconciliationResult(
        status=status,
        portfolio_truth=portfolio_truth,
        reconstructed=reconstructed_summary,
        pnl_gap=pnl_gap,
        pnl_gap_ratio_to_start_equity=(pnl_gap / start_equity) if start_equity else 0.0,
        issues=tuple(issues),
    )


def build_reconciliation_row(result: ReconciliationResult) -> dict[str, float | int | str]:
    return {
        'status': result.status,
        'portfolio_start_equity': result.portfolio_truth.get('start_equity', 0.0),
        'portfolio_end_equity': result.portfolio_truth.get('end_equity', 0.0),
        'portfolio_net_profit': result.portfolio_truth.get('net_profit', 0.0),
        'portfolio_return_ratio': result.portfolio_truth.get('return_ratio', 0.0),
        'portfolio_max_drawdown': result.portfolio_truth.get('max_drawdown', 0.0),
        'portfolio_halt_days': result.portfolio_truth.get('halt_days', 0),
        'reconstructed_trade_count': result.reconstructed.get('reconstructed_trade_count', 0),
        'reconstructed_gross_pnl': result.reconstructed.get('reconstructed_gross_pnl', 0.0),
        'reconstructed_win_rate': result.reconstructed.get('reconstructed_win_rate', 0.0),
        'session_restart_gap_exit_count': result.reconstructed.get('session_restart_gap_exit_count', 0),
        'portfolio_halt_exit_count': result.reconstructed.get('portfolio_halt_count', 0),
        'pnl_gap': result.pnl_gap,
        'pnl_gap_ratio_to_start_equity': result.pnl_gap_ratio_to_start_equity,
        'issue_codes': ','.join(issue.code for issue in result.issues),
    }


def format_reconciliation_markdown(result: ReconciliationResult) -> str:
    lines = [
        '# GM Truth Reconciliation',
        '',
        f"- status: `{result.status}`",
        f"- portfolio_net_profit: `{float(result.portfolio_truth.get('net_profit', 0.0) or 0.0):.6f}`",
        f"- reconstructed_gross_pnl: `{float(result.reconstructed.get('reconstructed_gross_pnl', 0.0) or 0.0):.6f}`",
        f"- pnl_gap: `{result.pnl_gap:.6f}`",
        f"- pnl_gap_ratio_to_start_equity: `{result.pnl_gap_ratio_to_start_equity:.6f}`",
        '',
        '## Truth Priority',
        '',
        '1. `portfolio_daily.csv` and the GM equity ledger are the primary PnL truth.',
        '2. `signals.csv` and `executions.csv` describe intent and accepted fills.',
        '3. local trade reconstruction remains a diagnostic layer.',
        '',
        '## Summary',
        '',
        '| layer | metric | value |',
        '| --- | --- | ---: |',
        f"| GM | start_equity | {float(result.portfolio_truth.get('start_equity', 0.0) or 0.0):.6f} |",
        f"| GM | end_equity | {float(result.portfolio_truth.get('end_equity', 0.0) or 0.0):.6f} |",
        f"| GM | net_profit | {float(result.portfolio_truth.get('net_profit', 0.0) or 0.0):.6f} |",
        f"| GM | max_drawdown | {float(result.portfolio_truth.get('max_drawdown', 0.0) or 0.0):.6f} |",
        f"| Local | reconstructed_trade_count | {int(result.reconstructed.get('reconstructed_trade_count', 0) or 0)} |",
        f"| Local | reconstructed_gross_pnl | {float(result.reconstructed.get('reconstructed_gross_pnl', 0.0) or 0.0):.6f} |",
        f"| Local | session_restart_gap_exit_count | {int(result.reconstructed.get('session_restart_gap_exit_count', 0) or 0)} |",
        f"| Local | portfolio_halt_exit_count | {int(result.reconstructed.get('portfolio_halt_count', 0) or 0)} |",
    ]
    if result.issues:
        lines.extend(['', '## Issues', ''])
        for issue in result.issues:
            lines.append(f"- `{issue.code}`: {issue.detail}")
    else:
        lines.extend(['', '## Issues', '', '- none'])
    return '\n'.join(lines) + '\n'


__all__ = [
    'ReconciliationIssue',
    'ReconciliationResult',
    'build_reconciliation_row',
    'format_reconciliation_markdown',
    'reconcile_backtest_run',
    'summarize_reconstructed_run',
]
