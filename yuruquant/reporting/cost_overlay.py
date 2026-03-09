from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from yuruquant.app.config_schema import AppConfig
from yuruquant.reporting.cost_reports import (
    build_platform_portfolio_daily_rows,
    build_portfolio_daily_rows,
    build_summary,
    build_symbol_cost_drag_rows,
    build_trade_diagnostics_rows,
)
from yuruquant.reporting.costing import (
    CostProfileRow,
    build_costed_trades,
    build_min_tick_lookup,
    build_platform_costed_trades,
    build_spec_lookup,
    load_cost_profile,
)
from yuruquant.reporting.trade_records import TradeRecord


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
class CostedOverlayResult:
    summary: dict[str, Any]
    trade_diagnostics: list[dict[str, Any]]
    portfolio_daily: list[dict[str, Any]]
    symbol_cost_drag: list[dict[str, Any]]


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
    costed_trades = build_costed_trades(
        trades=trades,
        cost_profile=cost_profile,
        specs=specs,
        min_ticks=min_ticks,
        frequency=config.universe.entry_frequency,
        profile_rows=profile_rows,
        current_high_cost=current_high_cost,
    )
    portfolio_daily, halt_count = build_portfolio_daily_rows(
        costed_trades=costed_trades,
        portfolio_daily_path=portfolio_daily_path,
        initial_equity=float(config.broker.gm.backtest.initial_cash),
        halt_drawdown_ratio=float(config.portfolio.max_drawdown_halt_ratio),
    )
    return CostedOverlayResult(
        summary=build_summary(costed_trades, portfolio_daily, halt_count),
        trade_diagnostics=build_trade_diagnostics_rows(costed_trades),
        portfolio_daily=portfolio_daily,
        symbol_cost_drag=build_symbol_cost_drag_rows(costed_trades),
    )


def build_platform_cost_report(
    trades: list[TradeRecord],
    portfolio_daily_path: Path,
    config: AppConfig,
    cost_profile: str,
) -> CostedOverlayResult:
    specs = build_spec_lookup(config)
    costed_trades = build_platform_costed_trades(
        trades=trades,
        cost_profile=cost_profile,
        specs=specs,
        frequency=config.universe.entry_frequency,
    )
    portfolio_daily, halt_count = build_platform_portfolio_daily_rows(
        costed_trades=costed_trades,
        portfolio_daily_path=portfolio_daily_path,
    )
    return CostedOverlayResult(
        summary=build_summary(costed_trades, portfolio_daily, halt_count),
        trade_diagnostics=build_trade_diagnostics_rows(costed_trades),
        portfolio_daily=portfolio_daily,
        symbol_cost_drag=build_symbol_cost_drag_rows(costed_trades),
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
    'build_platform_cost_report',
    'build_spec_lookup',
    'load_cost_profile',
    'write_csv',
]
