from __future__ import annotations

from collections import defaultdict
from statistics import median
from typing import Any

from yuruquant.reporting.costing import CostedTrade, aggregate_cost_totals, daily_items
from yuruquant.reporting.csv_utils import load_csv_rows, normalize_optional, to_float, to_int
from yuruquant.reporting.diagnostics import build_trade_diagnostics
from yuruquant.reporting.summary import collapse_portfolio_daily_rows


def build_trade_diagnostics_rows(costed_trades: list[CostedTrade]) -> list[dict[str, Any]]:
    raw_rows = build_trade_diagnostics([item.trade for item in costed_trades])
    by_campaign = {row['campaign_id']: row for row in raw_rows}
    rows: list[dict[str, Any]] = []
    for item in costed_trades:
        row = dict(by_campaign.get(item.trade.campaign_id, {}))
        row.update(
            {
                'cost_profile': item.cost_profile,
                'turnover': item.turnover,
                'commission_cost': item.commission_cost,
                'slippage_cost': item.slippage_cost,
                'total_cost': item.total_cost,
                'actual_net_pnl': item.net_pnl,
                'holding_bars': item.holding_bars,
                'multi_session_hold': int(item.multi_session_hold),
                'session_flat_exit': int(item.session_flat_exit),
                'protected_reach': int(item.protected_reach),
                'overnight_hold': int(item.overnight_hold),
                'trading_day_flat_exit': int(item.trading_day_flat_exit),
            }
        )
        rows.append(row)
    return rows


def build_symbol_cost_drag_rows(costed_trades: list[CostedTrade]) -> list[dict[str, Any]]:
    grouped: dict[str, list[CostedTrade]] = defaultdict(list)
    for item in costed_trades:
        grouped[item.trade.csymbol].append(item)

    rows: list[dict[str, Any]] = []
    for csymbol in sorted(grouped):
        items = grouped[csymbol]
        totals = aggregate_cost_totals(items)
        rows.append(
            {
                'cost_profile': items[0].cost_profile,
                'csymbol': csymbol,
                'trades': len(items),
                'turnover': totals['turnover'],
                'gross_pnl': totals['gross_pnl'],
                'commission_cost': totals['commission_cost'],
                'slippage_cost': totals['slippage_cost'],
                'total_cost': totals['total_cost'],
                'net_pnl': totals['net_pnl'],
                'cost_drag_ratio': (totals['total_cost'] / abs(totals['gross_pnl'])) if totals['gross_pnl'] else 0.0,
                'cost_to_turnover_ratio': (totals['total_cost'] / totals['turnover']) if totals['turnover'] else 0.0,
                'avg_cost_per_trade': (totals['total_cost'] / len(items)) if items else 0.0,
                'avg_holding_bars': (sum(item.holding_bars for item in items) / len(items)) if items else 0.0,
                'protected_reach_count': sum(1 for item in items if item.protected_reach),
                'multi_session_hold_count': sum(1 for item in items if item.multi_session_hold),
                'overnight_hold_count': sum(1 for item in items if item.overnight_hold),
                'session_flat_exit_count': sum(1 for item in items if item.session_flat_exit),
                'trading_day_flat_exit_count': sum(1 for item in items if item.trading_day_flat_exit),
            }
        )
    return rows


def build_platform_portfolio_daily_rows(
    costed_trades: list[CostedTrade],
    portfolio_daily_path,
) -> tuple[list[dict[str, Any]], int]:
    raw_rows = collapse_portfolio_daily_rows(load_csv_rows(portfolio_daily_path))
    by_day = {normalize_optional(row.get('date')): dict(row) for row in raw_rows if normalize_optional(row.get('date'))}
    trade_days = daily_items(costed_trades)
    all_days = sorted({*by_day.keys(), *trade_days.keys()})

    rows: list[dict[str, Any]] = []
    cumulative_turnover = 0.0
    previous_end = to_float(raw_rows[0].get('equity_start')) if raw_rows else 0.0
    previous_peak = previous_end
    halt_days: set[str] = set()

    for trade_day in all_days:
        source_row = by_day.get(trade_day, {})
        day_items = trade_days.get(trade_day, [])
        daily_totals = aggregate_cost_totals(day_items)
        cumulative_turnover += daily_totals['turnover']

        equity_start = to_float(source_row.get('equity_start'), previous_end)
        equity_end = to_float(source_row.get('equity_end'), equity_start + daily_totals['net_pnl'])
        equity_peak = to_float(source_row.get('equity_peak'), max(previous_peak, equity_end))
        drawdown_ratio = to_float(source_row.get('drawdown_ratio'), ((equity_peak - equity_end) / equity_peak) if equity_peak else 0.0)
        halt_flag = int(normalize_optional(source_row.get('halt_flag')) == '1')
        if halt_flag:
            halt_days.add(trade_day)

        rows.append(
            {
                'cost_profile': costed_trades[0].cost_profile if costed_trades else '',
                'date': trade_day,
                'snapshot_ts': normalize_optional(source_row.get('snapshot_ts')) or trade_day,
                'equity_start': equity_start,
                'equity_end': equity_end,
                'equity_peak': equity_peak,
                'drawdown_ratio': drawdown_ratio,
                'risk_state': normalize_optional(source_row.get('risk_state')) or ('halt_drawdown' if halt_flag else 'normal'),
                'effective_risk_mult': to_float(source_row.get('effective_risk_mult'), 0.0 if halt_flag else 1.0),
                'trades_count': to_int(source_row.get('trades_count'), len(day_items)),
                'wins': to_int(source_row.get('wins'), sum(1 for item in day_items if item.net_pnl > 0)),
                'losses': to_int(source_row.get('losses'), sum(1 for item in day_items if item.net_pnl <= 0)),
                'gross_realized_pnl': to_float(source_row.get('realized_pnl')),
                'cumulative_turnover': cumulative_turnover,
                'cumulative_commission_cost': 0.0,
                'cumulative_slippage_cost': 0.0,
                'cumulative_total_cost': 0.0,
                'realized_pnl': to_float(source_row.get('realized_pnl')),
                'daily_turnover': daily_totals['turnover'],
                'daily_gross_pnl': daily_totals['gross_pnl'],
                'daily_commission_cost': 0.0,
                'daily_slippage_cost': 0.0,
                'daily_total_cost': 0.0,
                'daily_net_pnl': daily_totals['net_pnl'],
                'halt_flag': halt_flag,
                'halt_reason': normalize_optional(source_row.get('halt_reason')),
            }
        )
        previous_end = equity_end
        previous_peak = max(previous_peak, equity_peak)

    return rows, len(halt_days)


def build_portfolio_daily_rows(
    costed_trades: list[CostedTrade],
    portfolio_daily_path,
    initial_equity: float,
    halt_drawdown_ratio: float,
) -> tuple[list[dict[str, Any]], int]:
    raw_rows = collapse_portfolio_daily_rows(load_csv_rows(portfolio_daily_path))
    by_day = {normalize_optional(row.get('date')): dict(row) for row in raw_rows if normalize_optional(row.get('date'))}
    trade_days = daily_items(costed_trades)
    all_days = sorted({*by_day.keys(), *trade_days.keys()})

    rows: list[dict[str, Any]] = []
    previous_end = float(initial_equity)
    equity_peak = float(initial_equity)
    cumulative_gross = 0.0
    cumulative_net = 0.0
    cumulative_turnover = 0.0
    cumulative_commission_cost = 0.0
    cumulative_slippage_cost = 0.0
    cumulative_total_cost = 0.0
    halt_count = 0

    for trade_day in all_days:
        day_items = trade_days.get(trade_day, [])
        daily_totals = aggregate_cost_totals(day_items)
        daily_gross = daily_totals['gross_pnl']
        daily_net = daily_totals['net_pnl']
        cumulative_gross += daily_gross
        cumulative_net += daily_net
        cumulative_turnover += daily_totals['turnover']
        cumulative_commission_cost += daily_totals['commission_cost']
        cumulative_slippage_cost += daily_totals['slippage_cost']
        cumulative_total_cost += daily_totals['total_cost']
        equity_start = previous_end
        equity_end = equity_start + daily_net
        equity_peak = max(equity_peak, equity_end)
        drawdown_ratio = (equity_peak - equity_end) / equity_peak if equity_peak > 0 else 0.0
        halt_triggered = drawdown_ratio >= max(float(halt_drawdown_ratio), 0.0)
        halt_count += int(halt_triggered)
        rows.append(
            {
                'cost_profile': costed_trades[0].cost_profile if costed_trades else '',
                'date': trade_day,
                'snapshot_ts': normalize_optional(by_day.get(trade_day, {}).get('snapshot_ts')) or trade_day,
                'equity_start': equity_start,
                'equity_end': equity_end,
                'equity_peak': equity_peak,
                'drawdown_ratio': drawdown_ratio,
                'risk_state': 'halt_drawdown' if halt_triggered else 'normal',
                'effective_risk_mult': 0.0 if halt_triggered else 1.0,
                'trades_count': len(day_items),
                'wins': sum(1 for item in day_items if item.net_pnl > 0),
                'losses': sum(1 for item in day_items if item.net_pnl <= 0),
                'gross_realized_pnl': cumulative_gross,
                'cumulative_turnover': cumulative_turnover,
                'cumulative_commission_cost': cumulative_commission_cost,
                'cumulative_slippage_cost': cumulative_slippage_cost,
                'cumulative_total_cost': cumulative_total_cost,
                'realized_pnl': cumulative_net,
                'daily_turnover': daily_totals['turnover'],
                'daily_gross_pnl': daily_gross,
                'daily_commission_cost': daily_totals['commission_cost'],
                'daily_slippage_cost': daily_totals['slippage_cost'],
                'daily_total_cost': daily_totals['total_cost'],
                'daily_net_pnl': daily_net,
                'halt_flag': int(halt_triggered),
                'halt_reason': f'drawdown={drawdown_ratio:.2%}' if halt_triggered else '',
            }
        )
        previous_end = equity_end

    return rows, halt_count


def build_summary(costed_trades: list[CostedTrade], daily_rows: list[dict[str, Any]], halt_count: int) -> dict[str, Any]:
    trades = len(costed_trades)
    wins = sum(1 for item in costed_trades if item.net_pnl > 0)
    losses = trades - wins
    totals = aggregate_cost_totals(costed_trades)
    gross_profit = totals['gross_pnl']
    net_profit = totals['net_pnl']
    commission_cost = totals['commission_cost']
    slippage_cost = totals['slippage_cost']
    total_cost = totals['total_cost']
    turnover = totals['turnover']
    start_equity = to_float(daily_rows[0].get('equity_start')) if daily_rows else 0.0
    end_equity = to_float(daily_rows[-1].get('equity_end')) if daily_rows else start_equity
    max_drawdown = max((to_float(row.get('drawdown_ratio')) for row in daily_rows), default=0.0)
    holding_values = [item.holding_bars for item in costed_trades]
    protected_reach_count = sum(1 for item in costed_trades if item.protected_reach)
    overnight_hold_count = sum(1 for item in costed_trades if item.overnight_hold)
    trading_day_flat_exit_count = sum(1 for item in costed_trades if item.trading_day_flat_exit)
    net_by_symbol: dict[str, float] = defaultdict(float)
    for item in costed_trades:
        net_by_symbol[item.trade.csymbol] += float(item.net_pnl)
    positive_symbol_count = sum(1 for value in net_by_symbol.values() if value > 0)
    top_symbol_pnl_share = (max(net_by_symbol.values()) / net_profit) if net_profit > 0 and net_by_symbol else 0.0
    return {
        'cost_profile': costed_trades[0].cost_profile if costed_trades else '',
        'trades': trades,
        'wins': wins,
        'losses': losses,
        'win_rate': (wins / trades) if trades else 0.0,
        'turnover': turnover,
        'gross_pnl': gross_profit,
        'commission_cost': commission_cost,
        'slippage_cost': slippage_cost,
        'total_cost': total_cost,
        'net_pnl': net_profit,
        'gross_return_ratio': (gross_profit / start_equity) if start_equity else 0.0,
        'net_return_ratio': (net_profit / start_equity) if start_equity else 0.0,
        'cost_drag_ratio': (total_cost / start_equity) if start_equity else 0.0,
        'cost_to_gross_pnl_ratio': (total_cost / abs(gross_profit)) if gross_profit else 0.0,
        'cost_to_turnover_ratio': (total_cost / turnover) if turnover else 0.0,
        'max_drawdown': max_drawdown,
        'end_equity': end_equity,
        'avg_cost_per_trade': (total_cost / trades) if trades else 0.0,
        'avg_holding_bars': (sum(holding_values) / len(holding_values)) if holding_values else 0.0,
        'median_holding_bars': float(median(holding_values)) if holding_values else 0.0,
        'protected_reach_count': protected_reach_count,
        'protected_reach_ratio': (protected_reach_count / trades) if trades else 0.0,
        'multi_session_hold_count': sum(1 for item in costed_trades if item.multi_session_hold),
        'overnight_hold_count': overnight_hold_count,
        'session_flat_exit_count': sum(1 for item in costed_trades if item.session_flat_exit),
        'trading_day_flat_exit_count': trading_day_flat_exit_count,
        'top_symbol_pnl_share': top_symbol_pnl_share,
        'positive_symbol_count': positive_symbol_count,
        'portfolio_halt_count_costed': halt_count,
    }


__all__ = [
    'build_platform_portfolio_daily_rows',
    'build_portfolio_daily_rows',
    'build_summary',
    'build_symbol_cost_drag_rows',
    'build_trade_diagnostics_rows',
]
