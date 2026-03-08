from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from statistics import median
from typing import Any, Mapping

from yuruquant.app.config_schema import AppConfig
from yuruquant.core.models import InstrumentSpec
from yuruquant.core.time import frequency_to_timedelta, parse_datetime, to_trade_day
from yuruquant.reporting.diagnostics import build_trade_diagnostics
from yuruquant.reporting.summary import collapse_portfolio_daily_rows
from yuruquant.reporting.trade_records import TradeRecord, load_csv_rows, normalize_optional, to_float
from yuruquant.strategy.trend_breakout.session_windows import current_session_window


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
    'commission_cost',
    'slippage_cost',
    'total_cost',
    'actual_net_pnl',
    'holding_bars',
    'multi_session_hold',
    'session_flat_exit',
    'ascended_negative_gross',
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
    'gross_pnl',
    'commission_cost',
    'slippage_cost',
    'total_cost',
    'net_pnl',
    'cost_drag_ratio',
    'avg_holding_bars',
    'multi_session_hold_count',
    'session_flat_exit_count',
    'ascended_exit_count',
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
    'realized_pnl',
    'daily_gross_pnl',
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
    'gross_return_ratio',
    'net_return_ratio',
    'cost_drag_ratio',
    'max_drawdown',
    'end_equity',
    'avg_holding_bars',
    'median_holding_bars',
    'multi_session_hold_count',
    'session_flat_exit_count',
    'ascended_exit_count',
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
    commission_cost: float
    slippage_cost: float
    total_cost: float
    net_pnl: float
    holding_bars: int
    multi_session_hold: bool
    session_flat_exit: bool
    ascended_negative_gross: bool


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
    entry_dt = parse_datetime(trade.entry_fill_ts or trade.entry_signal_ts)
    exit_dt = parse_datetime(trade.exit_fill_ts or trade.exit_signal_ts)
    elapsed_seconds = max((exit_dt - entry_dt).total_seconds(), 0.0)
    return max(int(round(elapsed_seconds / delta.total_seconds())), 1)


def _session_end_datetime(spec: InstrumentSpec, entry_ts: object):
    current = current_session_window(spec, entry_ts)
    if current is None:
        return None
    _, remaining_minutes = current
    return parse_datetime(entry_ts) + timedelta(minutes=remaining_minutes)


def _is_multi_session_hold(trade: TradeRecord, spec: InstrumentSpec) -> bool:
    session_end = _session_end_datetime(spec, trade.entry_fill_ts or trade.entry_signal_ts)
    if session_end is None:
        return False
    exit_dt = parse_datetime(trade.exit_fill_ts or trade.exit_signal_ts)
    return exit_dt > session_end


def _cost_trade(
    trade: TradeRecord,
    cost_profile: str,
    spec: InstrumentSpec,
    min_tick: float,
    frequency: str,
    profile_row: CostProfileRow | None = None,
    current_high_cost: tuple[float, float] | None = None,
) -> CostedTrade:
    turnover = _trade_turnover(trade)
    if profile_row is not None:
        commission_cost = turnover * float(profile_row.commission_ratio_per_side)
        slippage_cost = max(float(profile_row.slippage_ticks_per_side), 0.0) * max(float(min_tick), 0.0) * float(trade.multiplier) * int(trade.qty) * 2.0
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
        commission_cost=commission_cost,
        slippage_cost=slippage_cost,
        total_cost=total_cost,
        net_pnl=net_pnl,
        holding_bars=_holding_bars(trade, frequency),
        multi_session_hold=_is_multi_session_hold(trade, spec),
        session_flat_exit=trade.exit_trigger == 'session_flat',
        ascended_negative_gross=trade.phase_at_exit == 'ascended' and float(trade.gross_pnl) < 0.0,
    )


def _build_costed_trades(
    trades: list[TradeRecord],
    cost_profile: str,
    specs: Mapping[str, InstrumentSpec],
    min_ticks: Mapping[str, float],
    frequency: str,
    profile_rows: Mapping[str, CostProfileRow] | None = None,
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
                'commission_cost': item.commission_cost,
                'slippage_cost': item.slippage_cost,
                'total_cost': item.total_cost,
                'actual_net_pnl': item.net_pnl,
                'holding_bars': item.holding_bars,
                'multi_session_hold': int(item.multi_session_hold),
                'session_flat_exit': int(item.session_flat_exit),
                'ascended_negative_gross': int(item.ascended_negative_gross),
            }
        )
        rows.append(row)
    return rows


def _build_symbol_cost_drag_rows(costed_trades: list[CostedTrade]) -> list[dict[str, Any]]:
    grouped: dict[str, list[CostedTrade]] = defaultdict(list)
    for item in costed_trades:
        grouped[item.trade.csymbol].append(item)

    rows: list[dict[str, Any]] = []
    for csymbol in sorted(grouped):
        items = grouped[csymbol]
        gross = sum(float(item.trade.gross_pnl) for item in items)
        commission = sum(float(item.commission_cost) for item in items)
        slippage = sum(float(item.slippage_cost) for item in items)
        total_cost = commission + slippage
        net = sum(float(item.net_pnl) for item in items)
        rows.append(
            {
                'cost_profile': items[0].cost_profile,
                'csymbol': csymbol,
                'trades': len(items),
                'gross_pnl': gross,
                'commission_cost': commission,
                'slippage_cost': slippage,
                'total_cost': total_cost,
                'net_pnl': net,
                'cost_drag_ratio': (total_cost / abs(gross)) if gross else 0.0,
                'avg_holding_bars': (sum(item.holding_bars for item in items) / len(items)) if items else 0.0,
                'multi_session_hold_count': sum(1 for item in items if item.multi_session_hold),
                'session_flat_exit_count': sum(1 for item in items if item.session_flat_exit),
                'ascended_exit_count': sum(1 for item in items if item.trade.phase_at_exit == 'ascended'),
            }
        )
    return rows


def _build_portfolio_daily_rows(
    costed_trades: list[CostedTrade],
    portfolio_daily_path: Path,
    initial_equity: float,
    halt_drawdown_ratio: float,
) -> tuple[list[dict[str, Any]], int]:
    raw_rows = collapse_portfolio_daily_rows(load_csv_rows(portfolio_daily_path))
    if not raw_rows:
        return [], 0

    ordered_days = [normalize_optional(row.get('date')) for row in raw_rows if normalize_optional(row.get('date'))]
    start_equity = to_float(raw_rows[0].get('equity_start'), float(initial_equity)) if raw_rows else float(initial_equity)
    if start_equity <= 0:
        start_equity = float(initial_equity)

    daily_gross: dict[str, float] = defaultdict(float)
    daily_net: dict[str, float] = defaultdict(float)
    daily_trades: dict[str, list[CostedTrade]] = defaultdict(list)
    for item in costed_trades:
        trade_day = to_trade_day(item.trade.exit_fill_ts or item.trade.exit_signal_ts)
        daily_gross[trade_day] += float(item.trade.gross_pnl)
        daily_net[trade_day] += float(item.net_pnl)
        daily_trades[trade_day].append(item)

    rows: list[dict[str, Any]] = []
    equity_peak = start_equity
    cumulative_gross = 0.0
    cumulative_net = 0.0
    halt_triggered = False
    halt_count = 0
    previous_end = start_equity

    for raw in raw_rows:
        trade_day = normalize_optional(raw.get('date'))
        if not trade_day:
            continue
        day_items = daily_trades.get(trade_day, [])
        equity_start = previous_end if rows else start_equity
        cumulative_gross += daily_gross.get(trade_day, 0.0)
        cumulative_net += daily_net.get(trade_day, 0.0)
        equity_end = start_equity + cumulative_net
        equity_peak = max(equity_peak, equity_end)
        drawdown_ratio = ((equity_peak - equity_end) / equity_peak) if equity_peak > 0 else 0.0
        if (not halt_triggered) and drawdown_ratio >= max(float(halt_drawdown_ratio), 0.0) > 0.0:
            halt_triggered = True
            halt_count += 1
        rows.append(
            {
                'cost_profile': day_items[0].cost_profile if day_items else (costed_trades[0].cost_profile if costed_trades else ''),
                'date': trade_day,
                'snapshot_ts': normalize_optional(raw.get('snapshot_ts')),
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
                'realized_pnl': cumulative_net,
                'daily_gross_pnl': daily_gross.get(trade_day, 0.0),
                'daily_net_pnl': daily_net.get(trade_day, 0.0),
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
    gross_profit = sum(float(item.trade.gross_pnl) for item in costed_trades)
    net_profit = sum(float(item.net_pnl) for item in costed_trades)
    total_cost = gross_profit - net_profit
    start_equity = to_float(daily_rows[0].get('equity_start')) if daily_rows else 0.0
    end_equity = to_float(daily_rows[-1].get('equity_end')) if daily_rows else start_equity
    max_drawdown = max((to_float(row.get('drawdown_ratio')) for row in daily_rows), default=0.0)
    holding_bars = [item.holding_bars for item in costed_trades]
    return {
        'cost_profile': costed_trades[0].cost_profile if costed_trades else '',
        'trades': trades,
        'wins': wins,
        'losses': losses,
        'win_rate': (wins / trades) if trades else 0.0,
        'gross_return_ratio': (gross_profit / start_equity) if start_equity else 0.0,
        'net_return_ratio': (net_profit / start_equity) if start_equity else 0.0,
        'cost_drag_ratio': (total_cost / start_equity) if start_equity else 0.0,
        'max_drawdown': max_drawdown,
        'end_equity': end_equity,
        'avg_holding_bars': (sum(holding_bars) / len(holding_bars)) if holding_bars else 0.0,
        'median_holding_bars': float(median(holding_bars)) if holding_bars else 0.0,
        'multi_session_hold_count': sum(1 for item in costed_trades if item.multi_session_hold),
        'session_flat_exit_count': sum(1 for item in costed_trades if item.session_flat_exit),
        'ascended_exit_count': sum(1 for item in costed_trades if item.trade.phase_at_exit == 'ascended'),
        'portfolio_halt_count_costed': halt_count,
    }


def apply_cost_overlay(
    trades: list[TradeRecord],
    portfolio_daily_path: Path,
    config: AppConfig,
    cost_profile: str,
    profile_rows: Mapping[str, CostProfileRow] | None = None,
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
