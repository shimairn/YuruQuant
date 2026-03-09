from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any, Mapping

from yuruquant.app.config_schema import AppConfig
from yuruquant.core.models import InstrumentSpec
from yuruquant.core.time import exchange_datetime, frequency_to_timedelta, to_exchange_trade_day
from yuruquant.reporting.diagnostics import build_trade_diagnostics
from yuruquant.reporting.summary import collapse_portfolio_daily_rows
from yuruquant.reporting.trade_records import TradeRecord, load_csv_rows, normalize_optional, to_float
from yuruquant.core.session_clock import current_session_snapshot, session_end_crosses_trade_day


COST_PROFILE_COLUMNS = [
    'csymbol',
    'commission_ratio_per_side',
    'slippage_ticks_per_side',
    'source_url',
    'as_of_date',
    'notes',
]
COSTED_TRADE_DIAGNOSTIC_COLUMNS = [
    'cost_profile',
    'campaign_id',
    'csymbol',
    'entry_ts',
    'exit_ts',
    'entry_signal_ts',
    'entry_fill_ts',
    'exit_signal_ts',
    'exit_fill_ts',
    'exit_trigger',
    'phase_at_exit',
    'entry_price',
    'exit_price',
    'entry_signal_price',
    'entry_fill_price',
    'exit_signal_price',
    'exit_fill_price',
    'initial_stop_loss',
    'protected_stop_price',
    'theoretical_stop_price',
    'theoretical_stop_gross_pnl',
    'actual_gross_pnl',
    'turnover',
    'commission_cost',
    'slippage_cost',
    'total_cost',
    'actual_net_pnl',
    'holding_bars',
    'multi_session_hold',
    'session_flat_exit',
    'protected_reach',
    'overnight_hold',
    'trading_day_flat_exit',
    'overshoot_pnl',
    'overshoot_ratio',
    'exit_execution_regime',
    'exit_fill_gap_points',
    'exit_fill_gap_atr',
]
SYMBOL_COST_DRAG_COLUMNS = [
    'cost_profile',
    'csymbol',
    'trades',
    'turnover',
    'gross_pnl',
    'commission_cost',
    'slippage_cost',
    'total_cost',
    'net_pnl',
    'cost_drag_ratio',
    'cost_to_turnover_ratio',
    'avg_cost_per_trade',
    'avg_holding_bars',
    'protected_reach_count',
    'multi_session_hold_count',
    'overnight_hold_count',
    'session_flat_exit_count',
    'trading_day_flat_exit_count',
]
PORTFOLIO_DAILY_COSTED_COLUMNS = [
    'cost_profile',
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
    'gross_realized_pnl',
    'cumulative_turnover',
    'cumulative_commission_cost',
    'cumulative_slippage_cost',
    'cumulative_total_cost',
    'realized_pnl',
    'daily_turnover',
    'daily_gross_pnl',
    'daily_commission_cost',
    'daily_slippage_cost',
    'daily_total_cost',
    'daily_net_pnl',
    'halt_flag',
    'halt_reason',
]
SUMMARY_COSTED_COLUMNS = [
    'cost_profile',
    'trades',
    'wins',
    'losses',
    'win_rate',
    'turnover',
    'gross_pnl',
    'commission_cost',
    'slippage_cost',
    'total_cost',
    'net_pnl',
    'gross_return_ratio',
    'net_return_ratio',
    'cost_drag_ratio',
    'cost_to_gross_pnl_ratio',
    'cost_to_turnover_ratio',
    'max_drawdown',
    'end_equity',
    'avg_cost_per_trade',
    'avg_holding_bars',
    'median_holding_bars',
    'protected_reach_count',
    'protected_reach_ratio',
    'multi_session_hold_count',
    'overnight_hold_count',
    'session_flat_exit_count',
    'trading_day_flat_exit_count',
    'top_symbol_pnl_share',
    'positive_symbol_count',
    'portfolio_halt_count_costed',
]


@dataclass(frozen=True)
class CostProfileRow:
    csymbol: str
    commission_ratio_per_side: float
    slippage_ticks_per_side: float
    source_url: str
    as_of_date: str
    notes: str


@dataclass(frozen=True)
class CostedTrade:
    cost_profile: str
    trade: TradeRecord
    turnover: float
    commission_cost: float
    slippage_cost: float
    total_cost: float
    net_pnl: float
    holding_bars: int
    protected_reach: bool
    multi_session_hold: bool
    overnight_hold: bool
    session_flat_exit: bool
    trading_day_flat_exit: bool


@dataclass(frozen=True)
class CostedOverlayResult:
    summary: dict[str, Any]
    trade_diagnostics: list[dict[str, Any]]
    portfolio_daily: list[dict[str, Any]]
    symbol_cost_drag: list[dict[str, Any]]


def load_cost_profile(path: Path) -> dict[str, CostProfileRow]:
    rows: dict[str, CostProfileRow] = {}
    with path.open('r', encoding='utf-8-sig', newline='') as handle:
        for raw in csv.DictReader(handle):
            csymbol = normalize_optional(raw.get('csymbol'))
            if not csymbol:
                continue
            rows[csymbol] = CostProfileRow(
                csymbol=csymbol,
                commission_ratio_per_side=max(to_float(raw.get('commission_ratio_per_side')), 0.0),
                slippage_ticks_per_side=max(to_float(raw.get('slippage_ticks_per_side')), 0.0),
                source_url=normalize_optional(raw.get('source_url')),
                as_of_date=normalize_optional(raw.get('as_of_date')),
                notes=normalize_optional(raw.get('notes')),
            )
    return rows


def build_spec_lookup(config: AppConfig) -> dict[str, InstrumentSpec]:
    specs = {csymbol: config.universe.instrument_defaults for csymbol in config.universe.symbols}
    specs.update(config.universe.instrument_overrides)
    return specs


def build_min_tick_lookup(config: AppConfig) -> dict[str, float]:
    return {csymbol: float(spec.min_tick) for csymbol, spec in build_spec_lookup(config).items()}


def _trade_turnover(trade: TradeRecord) -> float:
    return (abs(float(trade.entry_fill_price)) + abs(float(trade.exit_fill_price))) * float(trade.multiplier) * int(trade.qty)


def _holding_bars(trade: TradeRecord, frequency: str) -> int:
    delta = frequency_to_timedelta(frequency)
    if delta is None or delta.total_seconds() <= 0:
        return 0
    entry_dt = exchange_datetime(trade.entry_fill_ts or trade.entry_signal_ts)
    exit_dt = exchange_datetime(trade.exit_fill_ts or trade.exit_signal_ts)
    elapsed_seconds = max((exit_dt - entry_dt).total_seconds(), 0.0)
    return max(int(round(elapsed_seconds / delta.total_seconds())), 1)


def _session_end_datetime(spec: InstrumentSpec, entry_ts: object):
    snapshot = current_session_snapshot(spec, entry_ts)
    return snapshot.end_dt if snapshot is not None else None


def _is_multi_session_hold(trade: TradeRecord, spec: InstrumentSpec) -> bool:
    session_end = _session_end_datetime(spec, trade.entry_fill_ts or trade.entry_signal_ts)
    if session_end is None:
        return False
    exit_dt = exchange_datetime(trade.exit_fill_ts or trade.exit_signal_ts)
    return exit_dt > session_end


def _is_overnight_hold(trade: TradeRecord) -> bool:
    entry_trade_day = to_exchange_trade_day(trade.entry_fill_ts or trade.entry_signal_ts)
    exit_trade_day = to_exchange_trade_day(trade.exit_fill_ts or trade.exit_signal_ts)
    return entry_trade_day != exit_trade_day


def _is_trading_day_flat_exit(trade: TradeRecord, spec: InstrumentSpec) -> bool:
    if trade.exit_trigger != 'session_flat':
        return False
    return session_end_crosses_trade_day(spec, trade.exit_signal_ts or trade.exit_fill_ts)


def _profile_value(row: Any, field: str) -> float:
    value = getattr(row, field, 0.0)
    try:
        return float(value)
    except Exception:
        return 0.0


def _cost_trade(
    trade: TradeRecord,
    cost_profile: str,
    spec: InstrumentSpec,
    min_tick: float,
    frequency: str,
    profile_row: Any | None = None,
    current_high_cost: tuple[float, float] | None = None,
) -> CostedTrade:
    turnover = _trade_turnover(trade)
    if profile_row is not None:
        commission_cost = turnover * max(_profile_value(profile_row, 'commission_ratio_per_side'), 0.0)
        slippage_cost = max(_profile_value(profile_row, 'slippage_ticks_per_side'), 0.0) * max(float(min_tick), 0.0) * float(trade.multiplier) * int(trade.qty) * 2.0
    elif current_high_cost is not None:
        commission_ratio, slippage_ratio = current_high_cost
        commission_cost = turnover * max(float(commission_ratio), 0.0)
        slippage_cost = turnover * max(float(slippage_ratio), 0.0)
    else:
        raise ValueError('either profile_row or current_high_cost must be provided')

    total_cost = commission_cost + slippage_cost
    net_pnl = float(trade.gross_pnl) - total_cost
    return CostedTrade(
        cost_profile=cost_profile,
        trade=trade,
        turnover=turnover,
        commission_cost=commission_cost,
        slippage_cost=slippage_cost,
        total_cost=total_cost,
        net_pnl=net_pnl,
        holding_bars=_holding_bars(trade, frequency),
        protected_reach=trade.phase_at_exit == 'protected',
        multi_session_hold=_is_multi_session_hold(trade, spec),
        overnight_hold=_is_overnight_hold(trade),
        session_flat_exit=trade.exit_trigger == 'session_flat',
        trading_day_flat_exit=_is_trading_day_flat_exit(trade, spec),
    )


def _build_costed_trades(
    trades: list[TradeRecord],
    cost_profile: str,
    specs: Mapping[str, InstrumentSpec],
    min_ticks: Mapping[str, float],
    frequency: str,
    profile_rows: Mapping[str, Any] | None = None,
    current_high_cost: tuple[float, float] | None = None,
) -> list[CostedTrade]:
    costed: list[CostedTrade] = []
    for trade in trades:
        spec = specs.get(trade.csymbol)
        if spec is None:
            continue
        if profile_rows is not None:
            profile_row = profile_rows.get(trade.csymbol)
            if profile_row is None:
                raise ValueError(f'missing realistic cost row for {trade.csymbol}')
            costed.append(_cost_trade(trade, cost_profile, spec, float(min_ticks.get(trade.csymbol, spec.min_tick)), frequency, profile_row=profile_row))
        else:
            costed.append(_cost_trade(trade, cost_profile, spec, float(min_ticks.get(trade.csymbol, spec.min_tick)), frequency, current_high_cost=current_high_cost))
    return costed


def _build_trade_diagnostics_rows(costed_trades: list[CostedTrade]) -> list[dict[str, Any]]:
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


def _aggregate_cost_totals(items: list[CostedTrade]) -> dict[str, float]:
    turnover = sum(float(item.turnover) for item in items)
    gross_pnl = sum(float(item.trade.gross_pnl) for item in items)
    commission_cost = sum(float(item.commission_cost) for item in items)
    slippage_cost = sum(float(item.slippage_cost) for item in items)
    total_cost = commission_cost + slippage_cost
    net_pnl = sum(float(item.net_pnl) for item in items)
    return {
        'turnover': turnover,
        'gross_pnl': gross_pnl,
        'commission_cost': commission_cost,
        'slippage_cost': slippage_cost,
        'total_cost': total_cost,
        'net_pnl': net_pnl,
    }


def _build_symbol_cost_drag_rows(costed_trades: list[CostedTrade]) -> list[dict[str, Any]]:
    grouped: dict[str, list[CostedTrade]] = defaultdict(list)
    for item in costed_trades:
        grouped[item.trade.csymbol].append(item)

    rows: list[dict[str, Any]] = []
    for csymbol in sorted(grouped):
        items = grouped[csymbol]
        totals = _aggregate_cost_totals(items)
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


def _daily_items(costed_trades: list[CostedTrade]) -> dict[str, list[CostedTrade]]:
    grouped: dict[str, list[CostedTrade]] = defaultdict(list)
    for item in costed_trades:
        trade_day = to_exchange_trade_day(item.trade.exit_fill_ts or item.trade.exit_signal_ts)
        grouped[trade_day].append(item)
    return grouped


def _build_portfolio_daily_rows(
    costed_trades: list[CostedTrade],
    portfolio_daily_path: Path,
    initial_equity: float,
    halt_drawdown_ratio: float,
) -> tuple[list[dict[str, Any]], int]:
    raw_rows = collapse_portfolio_daily_rows(load_csv_rows(portfolio_daily_path))
    by_day = {normalize_optional(row.get('date')): dict(row) for row in raw_rows if normalize_optional(row.get('date'))}
    trade_days = _daily_items(costed_trades)
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
        daily_totals = _aggregate_cost_totals(day_items)
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


def _build_summary(costed_trades: list[CostedTrade], daily_rows: list[dict[str, Any]], halt_count: int) -> dict[str, Any]:
    trades = len(costed_trades)
    wins = sum(1 for item in costed_trades if item.net_pnl > 0)
    losses = trades - wins
    totals = _aggregate_cost_totals(costed_trades)
    gross_profit = totals['gross_pnl']
    net_profit = totals['net_pnl']
    commission_cost = totals['commission_cost']
    slippage_cost = totals['slippage_cost']
    total_cost = totals['total_cost']
    turnover = totals['turnover']
    start_equity = to_float(daily_rows[0].get('equity_start')) if daily_rows else 0.0
    end_equity = to_float(daily_rows[-1].get('equity_end')) if daily_rows else start_equity
    max_drawdown = max((to_float(row.get('drawdown_ratio')) for row in daily_rows), default=0.0)
    holding_bars = [item.holding_bars for item in costed_trades]
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
        'avg_holding_bars': (sum(holding_bars) / len(holding_bars)) if holding_bars else 0.0,
        'median_holding_bars': float(median(holding_bars)) if holding_bars else 0.0,
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


def apply_cost_overlay(
    trades: list[TradeRecord],
    portfolio_daily_path: Path,
    config: AppConfig,
    cost_profile: str,
    profile_rows: Mapping[str, Any] | None = None,
    current_high_cost: tuple[float, float] | None = None,
) -> CostedOverlayResult:
    specs = build_spec_lookup(config)
    min_ticks = build_min_tick_lookup(config)
    costed_trades = _build_costed_trades(
        trades=trades,
        cost_profile=cost_profile,
        specs=specs,
        min_ticks=min_ticks,
        frequency=config.universe.entry_frequency,
        profile_rows=profile_rows,
        current_high_cost=current_high_cost,
    )
    portfolio_daily, halt_count = _build_portfolio_daily_rows(
        costed_trades=costed_trades,
        portfolio_daily_path=portfolio_daily_path,
        initial_equity=float(config.broker.gm.backtest.initial_cash),
        halt_drawdown_ratio=float(config.portfolio.max_drawdown_halt_ratio),
    )
    return CostedOverlayResult(
        summary=_build_summary(costed_trades, portfolio_daily, halt_count),
        trade_diagnostics=_build_trade_diagnostics_rows(costed_trades),
        portfolio_daily=portfolio_daily,
        symbol_cost_drag=_build_symbol_cost_drag_rows(costed_trades),
    )


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, '') for name in fieldnames})


__all__ = [
    'COST_PROFILE_COLUMNS',
    'COSTED_TRADE_DIAGNOSTIC_COLUMNS',
    'PORTFOLIO_DAILY_COSTED_COLUMNS',
    'SUMMARY_COSTED_COLUMNS',
    'SYMBOL_COST_DRAG_COLUMNS',
    'CostProfileRow',
    'CostedOverlayResult',
    'apply_cost_overlay',
    'build_min_tick_lookup',
    'build_spec_lookup',
    'load_cost_profile',
    'write_csv',
]
