from __future__ import annotations

import math
from typing import Optional

import pandas as pd

from strategy.domain.instruments import (
    get_instrument_spec,
    get_min_tick,
    get_multiplier,
    is_in_sessions,
    normalize_lot,
)
from strategy.pipelines.chan_5m import (
    build_bi,
    calculate_atr,
    check_breakout_volume,
    check_platform_breakout,
    get_latest_platform,
    identify_fractals,
    identify_zhongshu,
    merge_klines,
    resolve_session_volume_ratio_min,
)
from strategy.pipelines.common import estimate_turnover_cost
from strategy.types import PlatformState, RuntimeContext, SymbolState, TradingSignal


def _to_ts(value: object) -> pd.Timestamp:
    return pd.to_datetime(value)


def signal_due(current_eob: object, signal_eob: object | None) -> bool:
    if signal_eob is None:
        return False
    return _to_ts(current_eob) > _to_ts(signal_eob)


def calc_min_platform_width(runtime: RuntimeContext, csymbol: str, atr: float) -> float:
    min_tick = get_min_tick(runtime.cfg, csymbol)
    min_by_atr = max(float(atr), 0.0) * runtime.cfg.strategy.min_platform_width_atr if atr > 0 else 0.0
    return max(min_by_atr, min_tick * 8.0)


def resolve_h1_size_multiplier(runtime: RuntimeContext, breakout_direction: int, h1_direction: int) -> tuple[float, str]:
    mode = runtime.cfg.strategy.h1_filter_mode.strip().lower()
    if mode not in {"strict", "soft", "off"}:
        mode = "soft"

    direction = 1 if breakout_direction > 0 else -1 if breakout_direction < 0 else 0
    if direction == 0:
        return 0.0, "invalid direction"

    if mode == "off":
        return 1.0, "off"

    if mode == "strict":
        if h1_direction == direction:
            return 1.0, "strict aligned"
        return 0.0, "strict mismatch"

    if h1_direction == direction:
        return 1.0, "soft aligned"
    if h1_direction == 0:
        return max(0.0, runtime.cfg.strategy.h1_neutral_size_mult), "soft neutral"
    return 0.0, "soft mismatch"


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


def _compute_atr_pause_flag(runtime: RuntimeContext, df_5m: pd.DataFrame, atr_current: float) -> int:
    if atr_current <= 0:
        return 0
    threshold = float(runtime.cfg.strategy.atr_pause_ratio)
    if threshold <= 0:
        return 0
    atr_series = calculate_atr(df_5m, runtime.cfg.strategy.atr_period)
    if atr_series.empty:
        return 0
    lookback = max(int(runtime.cfg.strategy.atr_pause_lookback), 1)
    atr_ma = float(atr_series.tail(lookback).mean())
    if atr_ma <= 0:
        return 0
    atr_pause = float(atr_current) / atr_ma
    return int(atr_pause >= threshold)


def _build_entry_signal(
    runtime: RuntimeContext,
    csymbol: str,
    direction: int,
    current_price: float,
    atr: float,
    current_eob: object,
    h1_size_mult: float,
    entry_platform_zg: float,
    entry_platform_zd: float,
    daily_stopout_count: int,
    atr_pause_flag: int,
) -> Optional[TradingSignal]:
    multiplier = get_multiplier(runtime.cfg, csymbol)
    min_tick = get_min_tick(runtime.cfg, csymbol)
    spec = get_instrument_spec(runtime.cfg, csymbol)
    atr_used = atr if atr > 0 else max(current_price * 0.005, 1e-6)
    stop_dist = max(runtime.cfg.risk.hard_stop_atr * atr_used, min_tick)
    stop_loss = current_price - stop_dist if direction > 0 else current_price + stop_dist
    first_target = (
        current_price + stop_dist * runtime.cfg.risk.first_target_r_ratio
        if direction > 0
        else current_price - stop_dist * runtime.cfg.risk.first_target_r_ratio
    )

    portfolio = runtime.portfolio_risk
    equity = (
        portfolio.current_equity
        if portfolio.current_equity > 0
        else (portfolio.equity_peak if portfolio.equity_peak > 0 else portfolio.initial_equity)
    )
    if equity <= 0:
        equity = 500000.0

    h1_mult = max(float(h1_size_mult), 0.0)
    risk_mult = max(float(portfolio.effective_risk_mult), 0.0)
    vol_target_mult = _calc_vol_target_multiplier(runtime, current_price, atr_used)
    size_mult = h1_mult * risk_mult * vol_target_mult
    if size_mult <= 0:
        return None

    # Fixed equity sizing with risk multipliers.
    per_lot_value = abs(current_price) * multiplier
    if per_lot_value <= 0:
        return None
    fixed_equity_percent = spec.fixed_equity_percent if spec.fixed_equity_percent > 0 else runtime.cfg.risk.fixed_equity_percent
    max_pos_size_percent = spec.max_pos_size_percent if spec.max_pos_size_percent > 0 else runtime.cfg.risk.max_pos_size_percent

    order_value = equity * fixed_equity_percent * size_mult
    qty_raw = int(order_value / per_lot_value)

    max_pos_value = equity * max_pos_size_percent * risk_mult
    if max_pos_value <= 0:
        return None
    max_qty_by_cap = normalize_lot(int(max_pos_value / per_lot_value), spec.min_lot, spec.lot_step)
    if max_qty_by_cap <= 0:
        return None

    risk_per_trade = max(float(runtime.cfg.risk.risk_per_trade), 0.0)
    max_qty_by_risk = max_qty_by_cap
    if risk_per_trade > 0:
        # Cap quantity by allowed worst-case loss at initial hard stop.
        risk_budget = equity * risk_per_trade
        per_lot_risk = max(stop_dist * multiplier, 1e-9)
        max_qty_by_risk = normalize_lot(int(risk_budget / per_lot_risk), spec.min_lot, spec.lot_step)
        if max_qty_by_risk <= 0:
            return None

    qty = normalize_lot(qty_raw, spec.min_lot, spec.lot_step)
    qty = min(qty, max_qty_by_cap, max_qty_by_risk)
    if qty <= 0:
        # fixed_equity_percent may underflow to zero lots; allow one min lot
        # if all caps can still hold that lot.
        min_trade_qty = normalize_lot(spec.min_lot, spec.min_lot, spec.lot_step)
        if (
            min_trade_qty <= 0
            or max_qty_by_cap < min_trade_qty
            or max_qty_by_risk < min_trade_qty
        ):
            return None
        qty = min_trade_qty

    current_pos_value = abs(current_price * qty) * multiplier
    if current_pos_value > max_pos_value:
        return None

    action = "buy" if direction > 0 else "sell"
    campaign_id = f"{csymbol}-{_to_ts(current_eob).strftime('%Y%m%d%H%M%S')}"
    ratio = runtime.cfg.risk.backtest_commission_ratio + runtime.cfg.risk.backtest_slippage_ratio
    est_cost = estimate_turnover_cost(current_price, current_price, qty, multiplier, ratio)
    return TradingSignal(
        action=action,
        reason="platform breakout confirmed",
        direction=direction,
        qty=qty,
        price=float(current_price),
        stop_loss=float(stop_loss),
        take_profit=float(first_target),
        entry_atr=float(atr_used),
        risk_stage=runtime.portfolio_risk.risk_state,
        campaign_id=campaign_id,
        created_eob=current_eob,
        est_cost=float(est_cost),
        entry_platform_zg=float(entry_platform_zg),
        entry_platform_zd=float(entry_platform_zd),
        daily_stopout_count=int(daily_stopout_count),
        atr_pause_flag=int(atr_pause_flag),
    )


def _resolve_volume_ratio_min(runtime: RuntimeContext, csymbol: str, current_eob: object) -> float:
    spec = get_instrument_spec(runtime.cfg, csymbol)
    day_min = spec.volume_ratio_min.day
    night_min = spec.volume_ratio_min.night
    fallback = runtime.cfg.strategy.breakout_volume_ratio_min
    if day_min <= 0:
        day_min = fallback
    if night_min <= 0:
        night_min = fallback
    return resolve_session_volume_ratio_min(day_min, night_min, current_eob, spec.sessions.day, spec.sessions.night)


def _try_confirm_pending_platform(
    runtime: RuntimeContext,
    state: SymbolState,
    csymbol: str,
    current_eob: object,
    current_price: float,
    atr_val: float,
    atr_pause_flag: int,
) -> Optional[TradingSignal]:
    pending = state.pending_platform
    if pending is None:
        return None

    confirmed = (current_price > pending.zg) if pending.direction > 0 else (current_price < pending.zd)
    state.pending_platform = None
    if not confirmed:
        return None

    breakout_direction = 1 if pending.direction > 0 else -1
    h1_mult, _ = resolve_h1_size_multiplier(runtime, breakout_direction, state.h1_trend)
    if h1_mult <= 0:
        return None

    if state.h1_trend == breakout_direction and state.h1_strength < runtime.cfg.strategy.h1_strength_min:
        return None

    bars_since_last = state.bar_index_5m - state.last_entry_bar_index
    if state.last_entry_direction == breakout_direction and bars_since_last <= runtime.cfg.strategy.entry_cooldown_bars:
        return None

    if state.daily_entry_count >= runtime.cfg.strategy.max_entries_per_day:
        return None

    signal = _build_entry_signal(
        runtime,
        csymbol=csymbol,
        direction=breakout_direction,
        current_price=current_price,
        atr=pending.atr_at_candidate if pending.atr_at_candidate > 0 else atr_val,
        current_eob=current_eob,
        h1_size_mult=h1_mult,
        entry_platform_zg=pending.zg,
        entry_platform_zd=pending.zd,
        daily_stopout_count=state.daily_stopout_count,
        atr_pause_flag=atr_pause_flag,
    )
    if signal is None:
        return None

    state.last_entry_direction = breakout_direction
    state.last_entry_bar_index = state.bar_index_5m
    state.daily_entry_count += 1
    return signal


def process_entry_pipeline(
    runtime: RuntimeContext,
    state: SymbolState,
    csymbol: str,
    symbol: str,
    df_5m: pd.DataFrame,
    current_eob: object,
    current_price: float,
    atr_val: float,
    long_qty: int,
    short_qty: int,
) -> Optional[TradingSignal]:
    _ = symbol
    if long_qty > 0 or short_qty > 0:
        if state.bar_index_5m % 50 == 0:
            print(
                f"entry skip has_position csymbol={csymbol} symbol={symbol} "
                f"long_qty={long_qty} short_qty={short_qty}"
            )
        return None

    if not is_in_sessions(csymbol, runtime.cfg, current_eob):
        if state.bar_index_5m % 50 == 0:
            print(
                f"entry skip not_in_sessions csymbol={csymbol} symbol={symbol} "
                f"current_eob={current_eob}"
            )
        state.pending_platform = None
        return None

    if _entry_blocked_by_stopout_limit(runtime, state):
        if state.bar_index_5m % 50 == 0:
            print(
                f"entry skip stopout_limit csymbol={csymbol} symbol={symbol} "
                f"daily_stopout_count={state.daily_stopout_count}"
            )
        state.pending_platform = None
        return None

    atr_pause_flag = _compute_atr_pause_flag(runtime, df_5m, atr_val)
    if atr_pause_flag:
        if state.bar_index_5m % 50 == 0:
            print(
                f"entry skip atr_pause csymbol={csymbol} symbol={symbol} "
                f"atr_pause_flag={atr_pause_flag}"
            )
        state.pending_platform = None
        return None

    # Step A: confirm pending candidate
    if state.pending_platform is not None:
        due = signal_due(current_eob, state.pending_platform.candidate_eob)
        if runtime.cfg.strategy.require_next_bar_confirm:
            if due:
                return _try_confirm_pending_platform(
                    runtime,
                    state,
                    csymbol,
                    current_eob,
                    current_price,
                    atr_val,
                    atr_pause_flag,
                )
            return None
        return _try_confirm_pending_platform(
            runtime,
            state,
            csymbol,
            current_eob,
            current_price,
            atr_val,
            atr_pause_flag,
        )

    # Step B: detect new candidate
    merged = merge_klines(df_5m)
    fractals = identify_fractals(merged, runtime.cfg.strategy.fractal_confirm_bars)
    bis = build_bi(
        merged,
        fractals,
        min_tick=get_min_tick(runtime.cfg, csymbol),
        atr_multiplier=runtime.cfg.strategy.atr_multiplier,
        min_move_pct=runtime.cfg.strategy.min_move_pct,
    )
    zhongshus = identify_zhongshu(bis)
    platform = get_latest_platform(zhongshus)
    if platform is None:
        return None

    width = platform.zg - platform.zd
    min_width = calc_min_platform_width(runtime, csymbol, atr_val)
    max_width = max(0.0, atr_val) * runtime.cfg.strategy.max_platform_width_atr
    if max_width > 0 and width > max_width:
        return None
    if width < min_width:
        return None

    breakout = check_platform_breakout(current_price, platform)
    if not breakout.is_breakout:
        return None

    breakout_distance = (current_price - breakout.zg) if breakout.direction > 0 else (breakout.zd - current_price)
    min_distance = max(0.0, atr_val) * runtime.cfg.strategy.breakout_min_distance_atr
    if breakout_distance < min_distance:
        return None

    current_volume = float(df_5m.iloc[-1]["volume"])
    vol_ma = float(df_5m["volume"].tail(20).mean())
    volume_ratio_min = _resolve_volume_ratio_min(runtime, csymbol, current_eob)
    volume_ok, volume_ratio = check_breakout_volume(current_volume, vol_ma, volume_ratio_min)
    if not volume_ok:
        return None

    state.pending_platform = PlatformState(
        direction=breakout.direction,
        zg=breakout.zg,
        zd=breakout.zd,
        candidate_eob=current_eob,
        atr_at_candidate=atr_val,
        volume_ratio=volume_ratio,
    )
    return None
