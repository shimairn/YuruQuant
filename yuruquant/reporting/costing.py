from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from yuruquant.app.config_schema import AppConfig
from yuruquant.core.models import InstrumentSpec
from yuruquant.core.session_clock import current_session_snapshot, session_end_crosses_trade_day
from yuruquant.core.time import exchange_datetime, frequency_to_timedelta, to_exchange_trade_day
from yuruquant.reporting.csv_utils import normalize_optional, to_float
from yuruquant.reporting.trade_records import TradeRecord


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


def trade_turnover(trade: TradeRecord) -> float:
    return (abs(float(trade.entry_fill_price)) + abs(float(trade.exit_fill_price))) * float(trade.multiplier) * int(trade.qty)


def holding_bars(trade: TradeRecord, frequency: str) -> int:
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


def is_multi_session_hold(trade: TradeRecord, spec: InstrumentSpec) -> bool:
    session_end = _session_end_datetime(spec, trade.entry_fill_ts or trade.entry_signal_ts)
    if session_end is None:
        return False
    exit_dt = exchange_datetime(trade.exit_fill_ts or trade.exit_signal_ts)
    return exit_dt > session_end


def is_overnight_hold(trade: TradeRecord) -> bool:
    entry_trade_day = to_exchange_trade_day(trade.entry_fill_ts or trade.entry_signal_ts)
    exit_trade_day = to_exchange_trade_day(trade.exit_fill_ts or trade.exit_signal_ts)
    return entry_trade_day != exit_trade_day


def is_trading_day_flat_exit(trade: TradeRecord, spec: InstrumentSpec) -> bool:
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
    turnover = trade_turnover(trade)
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
        holding_bars=holding_bars(trade, frequency),
        protected_reach=trade.phase_at_exit == 'protected',
        multi_session_hold=is_multi_session_hold(trade, spec),
        overnight_hold=is_overnight_hold(trade),
        session_flat_exit=trade.exit_trigger == 'session_flat',
        trading_day_flat_exit=is_trading_day_flat_exit(trade, spec),
    )


def _platform_cost_trade(
    trade: TradeRecord,
    cost_profile: str,
    spec: InstrumentSpec,
    frequency: str,
) -> CostedTrade:
    return CostedTrade(
        cost_profile=cost_profile,
        trade=trade,
        turnover=trade_turnover(trade),
        commission_cost=0.0,
        slippage_cost=0.0,
        total_cost=0.0,
        net_pnl=float(trade.gross_pnl),
        holding_bars=holding_bars(trade, frequency),
        protected_reach=trade.phase_at_exit == 'protected',
        multi_session_hold=is_multi_session_hold(trade, spec),
        overnight_hold=is_overnight_hold(trade),
        session_flat_exit=trade.exit_trigger == 'session_flat',
        trading_day_flat_exit=is_trading_day_flat_exit(trade, spec),
    )


def build_costed_trades(
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


def build_platform_costed_trades(
    trades: list[TradeRecord],
    cost_profile: str,
    specs: Mapping[str, InstrumentSpec],
    frequency: str,
) -> list[CostedTrade]:
    costed: list[CostedTrade] = []
    for trade in trades:
        spec = specs.get(trade.csymbol)
        if spec is None:
            continue
        costed.append(_platform_cost_trade(trade, cost_profile, spec, frequency))
    return costed


def aggregate_cost_totals(items: list[CostedTrade]) -> dict[str, float]:
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


def daily_items(costed_trades: list[CostedTrade]) -> dict[str, list[CostedTrade]]:
    grouped: dict[str, list[CostedTrade]] = {}
    for item in costed_trades:
        trade_day = to_exchange_trade_day(item.trade.exit_fill_ts or item.trade.exit_signal_ts)
        grouped.setdefault(trade_day, []).append(item)
    return grouped


__all__ = [
    'CostProfileRow',
    'CostedTrade',
    'aggregate_cost_totals',
    'build_costed_trades',
    'build_min_tick_lookup',
    'build_platform_costed_trades',
    'build_spec_lookup',
    'daily_items',
    'load_cost_profile',
]
