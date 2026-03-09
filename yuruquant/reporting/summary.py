from __future__ import annotations

from pathlib import Path
from typing import Mapping

from yuruquant.reporting.csv_utils import load_csv_rows, normalize_optional, to_float
from yuruquant.reporting.trade_records import TradeRecord, build_trade_records


def overshoot_stats(trades: list[TradeRecord], trigger: str) -> tuple[float, float]:
    values = [max(float(trade.overshoot_pnl or 0.0), 0.0) for trade in trades if trade.exit_trigger == trigger and trade.overshoot_pnl is not None]
    if not values:
        return 0.0, 0.0
    return sum(values) / len(values), max(values)


def session_restart_gap_stats(trades: list[TradeRecord]) -> tuple[int, float, float, float]:
    gap_trades = [trade for trade in trades if trade.exit_execution_regime == 'session_restart_gap']
    count = len(gap_trades)
    if not count:
        return 0, 0.0, 0.0, 0.0
    overshoot_sum = sum(max(float(trade.overshoot_pnl or 0.0), 0.0) for trade in gap_trades if trade.overshoot_pnl is not None)
    total_stop_overshoot_sum = sum(
        max(float(trade.overshoot_pnl or 0.0), 0.0)
        for trade in trades
        if trade.exit_trigger in {'hard_stop', 'protected_stop'} and trade.overshoot_pnl is not None
    )
    overshoot_ratio = (overshoot_sum / total_stop_overshoot_sum) if total_stop_overshoot_sum > 0 else 0.0
    return count, count / len(trades), overshoot_sum, overshoot_ratio


def session_restart_gap_exit_stats(trades: list[TradeRecord]) -> tuple[int, float]:
    count = sum(1 for trade in trades if trade.exit_execution_regime == 'session_restart_gap')
    return count, (count / len(trades)) if trades else 0.0


def session_restart_gap_portfolio_halt_stats(trades: list[TradeRecord]) -> tuple[int, float]:
    count = sum(1 for trade in trades if trade.exit_execution_regime == 'session_restart_gap' and trade.exit_trigger == 'portfolio_halt')
    portfolio_halts = sum(1 for trade in trades if trade.exit_trigger == 'portfolio_halt')
    return count, (count / portfolio_halts) if portfolio_halts else 0.0


def summarize_trades(trades: list[TradeRecord]) -> dict[str, float | int]:
    if not trades:
        return {
            'trades': 0,
            'wins': 0,
            'losses': 0,
            'win_rate': 0.0,
            'hard_stop_count': 0,
            'hard_stop_ratio': 0.0,
            'protected_stop_count': 0,
            'protected_stop_ratio': 0.0,
            'armed_flush_count': 0,
            'armed_flush_ratio': 0.0,
            'session_flat_exit_count': 0,
            'session_flat_exit_ratio': 0.0,
            'portfolio_halt_count': 0,
            'session_restart_gap_exit_count': 0,
            'session_restart_gap_exit_ratio': 0.0,
            'session_restart_gap_portfolio_halt_count': 0,
            'session_restart_gap_portfolio_halt_ratio': 0.0,
            'session_restart_gap_stop_count': 0,
            'session_restart_gap_stop_ratio': 0.0,
            'session_restart_gap_overshoot_sum': 0.0,
            'session_restart_gap_overshoot_ratio': 0.0,
            'hard_stop_overshoot_avg': 0.0,
            'hard_stop_overshoot_max': 0.0,
            'protected_stop_overshoot_avg': 0.0,
            'protected_stop_overshoot_max': 0.0,
            'avg_win_pnl': 0.0,
            'avg_loss_pnl': 0.0,
            'avg_win_loss_ratio': 0.0,
            'best_trade_pnl': 0.0,
            'worst_trade_pnl': 0.0,
        }

    trades_count = len(trades)
    wins = [trade.gross_pnl for trade in trades if trade.gross_pnl > 0]
    losses = [trade.gross_pnl for trade in trades if trade.gross_pnl <= 0]
    hard_stop_count = sum(1 for trade in trades if trade.exit_trigger == 'hard_stop')
    protected_stop_count = sum(1 for trade in trades if trade.exit_trigger == 'protected_stop')
    armed_flush_count = sum(1 for trade in trades if trade.exit_trigger == 'armed_flush')
    session_flat_exit_count = sum(1 for trade in trades if trade.exit_trigger == 'session_flat')
    portfolio_halt_count = sum(1 for trade in trades if trade.exit_trigger == 'portfolio_halt')
    session_restart_gap_exit_count, session_restart_gap_exit_ratio = session_restart_gap_exit_stats(trades)
    session_restart_gap_portfolio_halt_count, session_restart_gap_portfolio_halt_ratio = session_restart_gap_portfolio_halt_stats(trades)
    session_restart_gap_stop_count = sum(1 for trade in trades if trade.exit_execution_regime == 'session_restart_gap' and trade.exit_trigger in {'hard_stop', 'protected_stop'})
    session_restart_gap_stop_ratio = (session_restart_gap_stop_count / max(hard_stop_count + protected_stop_count, 1)) if (hard_stop_count + protected_stop_count) else 0.0
    _, _, session_restart_gap_overshoot_sum, session_restart_gap_overshoot_ratio = session_restart_gap_stats(trades)
    hard_stop_overshoot_avg, hard_stop_overshoot_max = overshoot_stats(trades, 'hard_stop')
    protected_stop_overshoot_avg, protected_stop_overshoot_max = overshoot_stats(trades, 'protected_stop')
    avg_win_pnl = sum(wins) / len(wins) if wins else 0.0
    avg_loss_pnl = sum(losses) / len(losses) if losses else 0.0
    avg_win_loss_ratio = abs(avg_win_pnl / avg_loss_pnl) if wins and losses and avg_loss_pnl != 0 else 0.0

    return {
        'trades': trades_count,
        'wins': len(wins),
        'losses': len(losses),
        'win_rate': len(wins) / trades_count,
        'hard_stop_count': hard_stop_count,
        'hard_stop_ratio': hard_stop_count / trades_count,
        'protected_stop_count': protected_stop_count,
        'protected_stop_ratio': protected_stop_count / trades_count,
        'armed_flush_count': armed_flush_count,
        'armed_flush_ratio': armed_flush_count / trades_count,
        'session_flat_exit_count': session_flat_exit_count,
        'session_flat_exit_ratio': session_flat_exit_count / trades_count,
        'portfolio_halt_count': portfolio_halt_count,
        'session_restart_gap_exit_count': session_restart_gap_exit_count,
        'session_restart_gap_exit_ratio': session_restart_gap_exit_ratio,
        'session_restart_gap_portfolio_halt_count': session_restart_gap_portfolio_halt_count,
        'session_restart_gap_portfolio_halt_ratio': session_restart_gap_portfolio_halt_ratio,
        'session_restart_gap_stop_count': session_restart_gap_stop_count,
        'session_restart_gap_stop_ratio': session_restart_gap_stop_ratio,
        'session_restart_gap_overshoot_sum': session_restart_gap_overshoot_sum,
        'session_restart_gap_overshoot_ratio': session_restart_gap_overshoot_ratio,
        'hard_stop_overshoot_avg': hard_stop_overshoot_avg,
        'hard_stop_overshoot_max': hard_stop_overshoot_max,
        'protected_stop_overshoot_avg': protected_stop_overshoot_avg,
        'protected_stop_overshoot_max': protected_stop_overshoot_max,
        'avg_win_pnl': avg_win_pnl,
        'avg_loss_pnl': avg_loss_pnl,
        'avg_win_loss_ratio': avg_win_loss_ratio,
        'best_trade_pnl': max((trade.gross_pnl for trade in trades), default=0.0),
        'worst_trade_pnl': min((trade.gross_pnl for trade in trades), default=0.0),
    }


def collapse_portfolio_daily_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    collapsed: list[dict[str, str]] = []
    for row in rows:
        trade_day = normalize_optional(row.get('date'))
        if not trade_day:
            continue
        if collapsed and normalize_optional(collapsed[-1].get('date')) == trade_day:
            latest = dict(row)
            latest['equity_start'] = collapsed[-1].get('equity_start', row.get('equity_start', ''))
            collapsed[-1] = latest
            continue
        collapsed.append(dict(row))
    return collapsed


def summarize_portfolio_daily(portfolio_daily_path: Path) -> dict[str, float | int]:
    raw_rows = load_csv_rows(portfolio_daily_path)
    if not raw_rows:
        return {
            'days': 0,
            'start_equity': 0.0,
            'end_equity': 0.0,
            'net_profit': 0.0,
            'return_ratio': 0.0,
            'max_drawdown': 0.0,
            'halt_days': 0,
            'final_realized_pnl': 0.0,
        }

    rows = collapse_portfolio_daily_rows(raw_rows)
    start_equity = to_float(rows[0].get('equity_start'))
    end_equity = to_float(rows[-1].get('equity_end'))
    max_drawdown = max(to_float(row.get('drawdown_ratio')) for row in raw_rows)
    halt_days = len({normalize_optional(row.get('date')) for row in raw_rows if normalize_optional(row.get('halt_flag')) == '1'})
    final_realized_pnl = to_float(rows[-1].get('realized_pnl'))
    net_profit = end_equity - start_equity
    return {
        'days': len(rows),
        'start_equity': start_equity,
        'end_equity': end_equity,
        'net_profit': net_profit,
        'return_ratio': (net_profit / start_equity) if start_equity else 0.0,
        'max_drawdown': max_drawdown,
        'halt_days': halt_days,
        'final_realized_pnl': final_realized_pnl,
    }


def summarize_backtest_run(
    signals_path: Path,
    portfolio_daily_path: Path,
    multiplier_by_csymbol: Mapping[str, float],
    executions_path: Path | None = None,
) -> dict[str, float | int]:
    # Portfolio daily stays the GM truth layer. Trade summaries remain structural diagnostics.
    trades = build_trade_records(signals_path, multiplier_by_csymbol, executions_path)
    summary: dict[str, float | int] = {}
    summary.update(summarize_trades(trades))
    summary.update(summarize_portfolio_daily(portfolio_daily_path))
    return summary


__all__ = ['collapse_portfolio_daily_rows', 'summarize_backtest_run', 'summarize_portfolio_daily', 'summarize_trades']
