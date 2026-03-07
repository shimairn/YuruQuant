from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class TradeRecord:
    campaign_id: str
    csymbol: str
    entry_ts: str
    exit_ts: str
    direction: int
    qty: int
    entry_price: float
    exit_price: float
    exit_reason: str
    multiplier: float
    pnl_points: float
    gross_pnl: float


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open('r', newline='', encoding='utf-8-sig') as handle:
        return list(csv.DictReader(handle))


def build_trade_records(signals_path: Path, multiplier_by_csymbol: Mapping[str, float]) -> list[TradeRecord]:
    rows = load_csv_rows(signals_path)
    exits_by_campaign: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        action = str(row.get('action', ''))
        if action.startswith('close_'):
            exits_by_campaign.setdefault(str(row.get('campaign_id', '')), []).append(row)

    trades: list[TradeRecord] = []
    for row in rows:
        action = str(row.get('action', ''))
        if action not in {'buy', 'sell'}:
            continue
        campaign_id = str(row.get('campaign_id', ''))
        exit_rows = exits_by_campaign.get(campaign_id)
        if not exit_rows:
            continue
        exit_row = exit_rows[0]
        direction = int(float(row.get('direction', '0') or 0))
        qty = int(float(row.get('qty', '0') or 0))
        entry_price = float(row.get('price', '0') or 0.0)
        exit_price = float(exit_row.get('price', '0') or 0.0)
        multiplier = float(multiplier_by_csymbol.get(str(row.get('csymbol', '')), 1.0))
        pnl_points = exit_price - entry_price if direction > 0 else entry_price - exit_price
        trades.append(
            TradeRecord(
                campaign_id=campaign_id,
                csymbol=str(row.get('csymbol', '')),
                entry_ts=str(row.get('ts', '')),
                exit_ts=str(exit_row.get('ts', '')),
                direction=direction,
                qty=qty,
                entry_price=entry_price,
                exit_price=exit_price,
                exit_reason=str(exit_row.get('reason', '')),
                multiplier=multiplier,
                pnl_points=pnl_points,
                gross_pnl=pnl_points * qty * multiplier,
            )
        )
    return trades


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
            'trend_ma_stop_count': 0,
            'trend_ma_stop_ratio': 0.0,
            'avg_win_pnl': 0.0,
            'avg_loss_pnl': 0.0,
            'avg_win_loss_ratio': 0.0,
            'best_trade_pnl': 0.0,
            'worst_trade_pnl': 0.0,
        }

    wins = [trade.gross_pnl for trade in trades if trade.gross_pnl > 0]
    losses = [trade.gross_pnl for trade in trades if trade.gross_pnl <= 0]
    hard_stop_count = sum(1 for trade in trades if trade.exit_reason == 'hard stop')
    protected_stop_count = sum(1 for trade in trades if trade.exit_reason == 'protected stop')
    trend_ma_stop_count = sum(1 for trade in trades if trade.exit_reason == 'trend ma stop')
    trades_count = len(trades)
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
        'trend_ma_stop_count': trend_ma_stop_count,
        'trend_ma_stop_ratio': trend_ma_stop_count / trades_count,
        'avg_win_pnl': avg_win_pnl,
        'avg_loss_pnl': avg_loss_pnl,
        'avg_win_loss_ratio': avg_win_loss_ratio,
        'best_trade_pnl': max((trade.gross_pnl for trade in trades), default=0.0),
        'worst_trade_pnl': min((trade.gross_pnl for trade in trades), default=0.0),
    }


def summarize_portfolio_daily(portfolio_daily_path: Path) -> dict[str, float | int]:
    rows = load_csv_rows(portfolio_daily_path)
    if not rows:
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

    start_equity = float(rows[0].get('equity_start', '0') or 0.0)
    end_equity = float(rows[-1].get('equity_end', '0') or 0.0)
    max_drawdown = max(float(row.get('drawdown_ratio', '0') or 0.0) for row in rows)
    halt_days = sum(1 for row in rows if str(row.get('halt_flag', '0')) == '1')
    final_realized_pnl = float(rows[-1].get('realized_pnl', '0') or 0.0)
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


def summarize_backtest_run(signals_path: Path, portfolio_daily_path: Path, multiplier_by_csymbol: Mapping[str, float]) -> dict[str, float | int]:
    trades = build_trade_records(signals_path, multiplier_by_csymbol)
    summary: dict[str, float | int] = {}
    summary.update(summarize_trades(trades))
    summary.update(summarize_portfolio_daily(portfolio_daily_path))
    return summary
