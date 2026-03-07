from __future__ import annotations

import math
from typing import Optional

from strategy.core.indicators import atr_series, latest_breakout_channel, latest_volume_ratio
from strategy.core.kline_types import KlineFrame
from strategy.core.time_utils import make_event_id
from strategy.domain.instruments import (
    get_instrument_spec,
    is_in_sessions,
    normalize_lot,
    resolve_session_volume_ratio_min,
)
from strategy.pipelines.common import estimate_turnover_cost
from strategy.types import RuntimeContext, SymbolState, TradingSignal


def _calc_vol_target_multiplier(runtime: RuntimeContext, current_price: float, atr: float) -> float:
    if current_price <= 0 or atr <= 0:
        return 1.0
    bars_per_year = 252.0 * 80.0
    realized_vol = (atr / current_price) * math.sqrt(bars_per_year)
    target = max(runtime.cfg.strategy.target_annual_vol, 1e-6)
    raw = target / max(realized_vol, 1e-6)
    return min(1.0, max(0.25, raw))


def _entry_blocked_by_stopout_limit(runtime: RuntimeContext, state: SymbolState) -> bool:
    limit = max(int(runtime.cfg.risk.max_stopouts_per_day_per_symbol), 0)
    return limit > 0 and state.daily_stopout_count >= limit


def _resolve_qty(
    runtime: RuntimeContext,
    csymbol: str,
    current_price: float,
    trend_strength: float,
    atr: float,
) -> int:
    spec = get_instrument_spec(runtime.cfg, csymbol)
    per_lot_value = abs(float(current_price)) * float(spec.multiplier)
    if per_lot_value <= 0:
        return 0

    portfolio = runtime.portfolio_risk
    equity = portfolio.current_equity if portfolio.current_equity > 0 else max(portfolio.initial_equity, 500000.0)
    if equity <= 0:
        equity = 500000.0

    risk_mult = max(float(portfolio.effective_risk_mult), 0.0)
    if risk_mult <= 0:
        return 0

    vol_target_mult = _calc_vol_target_multiplier(runtime, current_price, atr)
    trend_mult = max(0.25, min(1.0, float(trend_strength) if trend_strength > 0 else 0.25))
    size_mult = risk_mult * vol_target_mult * trend_mult

    fixed_equity_percent = spec.fixed_equity_percent if spec.fixed_equity_percent > 0 else runtime.cfg.risk.fixed_equity_percent
    max_pos_size_percent = spec.max_pos_size_percent if spec.max_pos_size_percent > 0 else runtime.cfg.risk.max_pos_size_percent

    order_value = equity * fixed_equity_percent * size_mult
    qty_raw = int(order_value / per_lot_value)

    max_pos_value = equity * max_pos_size_percent * risk_mult
    if max_pos_value <= 0:
        return 0
    max_qty_by_cap = normalize_lot(int(max_pos_value / per_lot_value), spec.min_lot, spec.lot_step)
    if max_qty_by_cap <= 0:
        return 0

    risk_ratio = max(float(runtime.cfg.risk.risk_per_trade_notional_ratio), 0.0)
    if risk_ratio <= 0:
        return 0
    risk_budget = equity * risk_ratio
    max_qty_by_risk = normalize_lot(int(risk_budget / per_lot_value), spec.min_lot, spec.lot_step)
    if max_qty_by_risk <= 0:
        return 0

    qty = normalize_lot(qty_raw, spec.min_lot, spec.lot_step)
    qty = min(qty, max_qty_by_cap, max_qty_by_risk)
    if qty <= 0:
        fallback = normalize_lot(spec.min_lot, spec.min_lot, spec.lot_step)
        if fallback <= 0:
            return 0
        if fallback > max_qty_by_cap or fallback > max_qty_by_risk:
            return 0
        qty = fallback
    return qty


def _build_signal(
    runtime: RuntimeContext,
    state: SymbolState,
    csymbol: str,
    direction: int,
    current_price: float,
    atr: float,
    current_eob: object,
) -> Optional[TradingSignal]:
    qty = _resolve_qty(runtime, csymbol, current_price, state.h1_strength, atr)
    if qty <= 0:
        return None

    spec = get_instrument_spec(runtime.cfg, csymbol)
    min_tick = max(float(spec.min_tick), 1e-6)
    atr_used = atr if atr > 0 else max(abs(current_price) * 0.005, min_tick)
    stop_dist = max(runtime.cfg.risk.hard_stop_atr * atr_used, min_tick)
    stop_loss = current_price - stop_dist if direction > 0 else current_price + stop_dist

    action = "buy" if direction > 0 else "sell"
    campaign_id = make_event_id(csymbol, current_eob)
    ratio = runtime.cfg.risk.backtest_commission_ratio + runtime.cfg.risk.backtest_slippage_ratio
    est_cost = estimate_turnover_cost(current_price, current_price, qty, spec.multiplier, ratio)

    return TradingSignal(
        action=action,
        reason="multi_tf_breakout",
        direction=direction,
        qty=qty,
        price=float(current_price),
        stop_loss=float(stop_loss),
        take_profit=0.0,
        entry_atr=float(atr_used),
        risk_stage=runtime.portfolio_risk.risk_state,
        campaign_id=campaign_id,
        created_eob=current_eob,
        est_cost=float(est_cost),
        trend_strength=float(state.h1_strength),
        daily_stopout_count=int(state.daily_stopout_count),
    )


def process_entry_pipeline(
    runtime: RuntimeContext,
    state: SymbolState,
    csymbol: str,
    symbol: str,
    frame_5m: KlineFrame,
    current_eob: object,
    current_price: float,
    atr_val: float,
    long_qty: int,
    short_qty: int,
) -> Optional[TradingSignal]:
    _ = symbol
    if frame_5m.empty:
        return None

    if long_qty > 0 or short_qty > 0:
        return None
    if not is_in_sessions(csymbol, runtime.cfg, current_eob):
        return None
    if _entry_blocked_by_stopout_limit(runtime, state):
        return None
    if state.daily_entry_count >= runtime.cfg.strategy.max_entries_per_day:
        return None

    upper, lower, close = latest_breakout_channel(frame_5m, runtime.cfg.strategy.breakout_lookback_5m)
    if upper <= 0 or lower <= 0 or close <= 0:
        return None

    direction = 0
    breakout_distance = 0.0
    if close > upper:
        direction = 1
        breakout_distance = close - upper
    elif close < lower:
        direction = -1
        breakout_distance = lower - close
    if direction == 0:
        return None

    if state.h1_trend != direction:
        return None
    if state.h1_strength < runtime.cfg.strategy.trend_strength_min:
        return None

    bars_since_last = state.bar_index_5m - state.last_entry_bar_index
    if state.last_entry_direction == direction and bars_since_last <= runtime.cfg.strategy.entry_cooldown_bars:
        return None

    atr_used = atr_val
    if atr_used <= 0:
        atr_s = atr_series(frame_5m, runtime.cfg.strategy.atr_period)
        atr_used = float(atr_s.item(-1) or 0.0) if len(atr_s) > 0 else 0.0
    if atr_used <= 0:
        return None

    min_distance = runtime.cfg.strategy.breakout_min_distance_atr * atr_used
    if breakout_distance < min_distance:
        return None

    width = upper - lower
    width_in_atr = width / atr_used if atr_used > 0 else 0.0
    if width_in_atr < runtime.cfg.strategy.breakout_width_min_atr:
        return None
    if width_in_atr > runtime.cfg.strategy.breakout_width_max_atr:
        return None

    volume_ratio = latest_volume_ratio(frame_5m, window=20)
    min_volume_ratio = resolve_session_volume_ratio_min(csymbol, runtime.cfg, current_eob)
    if min_volume_ratio <= 0:
        min_volume_ratio = max(runtime.cfg.strategy.volume_ratio_day_min, runtime.cfg.strategy.volume_ratio_night_min)
    if volume_ratio < min_volume_ratio:
        return None

    signal = _build_signal(runtime, state, csymbol, direction, current_price, atr_used, current_eob)
    if signal is None:
        return None

    state.last_entry_direction = direction
    state.last_entry_bar_index = state.bar_index_5m
    state.daily_entry_count += 1
    return signal
