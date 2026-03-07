from __future__ import annotations

from datetime import timedelta
from typing import Optional

import pandas as pd

from strategy.domain.instruments import get_instrument_spec, get_min_tick, get_multiplier, normalize_lot
from strategy.pipelines.common import estimate_turnover_cost
from strategy.pipelines.trend_1h import calculate_h1_trailing_stop_ema
from strategy.types import PositionRiskState, RuntimeContext, SymbolState, TradingSignal


def _get_account_equity(context) -> float:
    try:
        cash = context.account().cash
        if isinstance(cash, dict):
            for key in ("nav", "balance", "equity", "total", "cash", "available"):
                if key in cash:
                    return float(cash.get(key) or 0.0)
        return float(cash or 0.0)
    except Exception:
        return 0.0


def _estimate_cost(runtime: RuntimeContext, entry_price: float, exit_price: float, qty: int, multiplier: float) -> float:
    ratio = runtime.cfg.risk.backtest_commission_ratio + runtime.cfg.risk.backtest_slippage_ratio
    return estimate_turnover_cost(entry_price, exit_price, qty, multiplier, ratio)


def _to_trade_day(value: object) -> str:
    return pd.to_datetime(value).strftime("%Y-%m-%d")


def ensure_daily_stopout_counter(state: SymbolState, current_eob: object) -> None:
    trade_day = _to_trade_day(current_eob)
    if state.daily_stopout_date != trade_day:
        state.daily_stopout_date = trade_day
        state.daily_stopout_count = 0


def _increment_daily_stopout(state: SymbolState, current_eob: object) -> None:
    ensure_daily_stopout_counter(state, current_eob)
    state.daily_stopout_count += 1


def _build_position_risk_state(
    runtime: RuntimeContext,
    state: SymbolState,
    direction: int,
    entry_price: float,
    atr_val: float,
    campaign_id: str,
    initial_stop_loss: float | None = None,
    first_target_price: float | None = None,
    entry_platform_zg: float = 0.0,
    entry_platform_zd: float = 0.0,
) -> PositionRiskState:
    min_tick = get_min_tick(runtime.cfg, state.csymbol)
    atr_used = atr_val if atr_val > 0 else max(entry_price * 0.005, 1e-6)
    dist = max(runtime.cfg.risk.hard_stop_atr * atr_used, min_tick)
    init_stop = (
        initial_stop_loss
        if initial_stop_loss is not None and float(initial_stop_loss) > 0
        else (entry_price - dist if direction > 0 else entry_price + dist)
    )
    first_target = (
        first_target_price
        if first_target_price is not None and float(first_target_price) > 0
        else (
            entry_price + dist * runtime.cfg.risk.first_target_r_ratio
            if direction > 0
            else entry_price - dist * runtime.cfg.risk.first_target_r_ratio
        )
    )
    return PositionRiskState(
        entry_price=float(entry_price),
        direction=direction,
        entry_atr=float(atr_used),
        initial_stop_loss=float(init_stop),
        stop_loss=float(init_stop),
        first_target_price=float(first_target),
        campaign_id=campaign_id,
        highest_price_since_entry=float(entry_price),
        lowest_price_since_entry=float(entry_price),
        entry_platform_zg=float(entry_platform_zg),
        entry_platform_zd=float(entry_platform_zd),
        initial_risk_r=float(abs(float(entry_price) - float(init_stop))),
        is_half_closed=False,
    )


def seed_position_risk_from_entry_signal(
    runtime: RuntimeContext,
    state: SymbolState,
    signal: TradingSignal,
    current_price: float,
    current_eob: object,
) -> None:
    if signal.action not in {"buy", "sell"}:
        return
    direction = 1 if signal.action == "buy" else -1
    entry_price = float(signal.price) if float(signal.price) > 0 else float(current_price)
    atr_val = float(signal.entry_atr) if float(signal.entry_atr) > 0 else 0.0
    campaign = signal.campaign_id or f"{state.csymbol}-{str(current_eob).replace(' ', '_')}"
    state.position_risk = _build_position_risk_state(
        runtime=runtime,
        state=state,
        direction=direction,
        entry_price=entry_price,
        atr_val=atr_val,
        campaign_id=campaign,
        initial_stop_loss=float(signal.stop_loss) if float(signal.stop_loss) > 0 else None,
        first_target_price=float(signal.take_profit) if float(signal.take_profit) > 0 else None,
        entry_platform_zg=float(getattr(signal, "entry_platform_zg", 0.0)),
        entry_platform_zd=float(getattr(signal, "entry_platform_zd", 0.0)),
    )


def _make_close_signal(
    runtime: RuntimeContext,
    state: SymbolState,
    csymbol: str,
    action: str,
    reason: str,
    trigger: str,
    current_price: float,
    qty: int,
) -> TradingSignal:
    pos = state.position_risk
    entry_price = float(pos.entry_price) if pos is not None else float(current_price)
    direction = int(pos.direction) if pos is not None else (1 if action == "close_long" else -1)
    campaign_id = pos.campaign_id if pos is not None else ""

    multiplier = get_multiplier(runtime.cfg, csymbol)
    gross_pnl = (float(current_price) - entry_price) * direction * multiplier * max(int(qty), 0)
    est_cost = _estimate_cost(runtime, entry_price, current_price, qty, multiplier)
    holding_bars = int(pos.bars_in_trade) if pos is not None else 0
    mfe_r = float(pos.mfe_r) if pos is not None else 0.0
    entry_platform_zg = float(pos.entry_platform_zg) if pos is not None else 0.0
    entry_platform_zd = float(pos.entry_platform_zd) if pos is not None else 0.0

    return TradingSignal(
        action=action,
        reason=reason,
        direction=direction,
        qty=max(int(qty), 0),
        price=float(current_price),
        stop_loss=0.0,
        take_profit=0.0,
        entry_atr=float(pos.entry_atr) if pos is not None else 0.0,
        risk_stage=runtime.portfolio_risk.risk_state,
        campaign_id=campaign_id,
        created_eob=None,
        exit_trigger_type=trigger,
        est_cost=est_cost,
        gross_pnl=gross_pnl,
        net_pnl=gross_pnl - est_cost,
        entry_platform_zg=entry_platform_zg,
        entry_platform_zd=entry_platform_zd,
        holding_bars=holding_bars,
        mfe_r=mfe_r,
        daily_stopout_count=int(state.daily_stopout_count),
    )


def _update_portfolio_risk(runtime: RuntimeContext, context) -> tuple[bool, str]:
    state = runtime.portfolio_risk
    equity = _get_account_equity(context)
    if equity <= 0:
        equity = 500000.0
    state.current_equity = equity

    today = context.now.strftime("%Y-%m-%d")
    if state.initial_equity <= 0:
        state.initial_equity = equity
        state.current_equity = equity
        state.equity_peak = equity
        state.daily_start_equity = equity
        state.current_date = today
        state.risk_state = "normal"
        state.effective_risk_mult = 1.0
        return True, ""

    if state.current_date != today:
        state.daily_start_equity = equity
        state.current_date = today

    state.equity_peak = max(state.equity_peak, equity)
    state.drawdown_ratio = (state.equity_peak - equity) / state.equity_peak if state.equity_peak > 0 else 0.0

    if state.daily_start_equity > 0:
        daily_loss = (state.daily_start_equity - equity) / state.daily_start_equity
        if daily_loss >= runtime.cfg.portfolio.max_daily_loss_ratio:
            state.risk_state = "halt_daily_loss"
            state.effective_risk_mult = 0.0
            return False, f"daily loss exceeded: {daily_loss:.2%}"

    if state.halt_until_date and today < state.halt_until_date:
        state.risk_state = "halt"
        state.effective_risk_mult = 0.0
        return False, f"halt until {state.halt_until_date}"

    if state.drawdown_ratio > runtime.cfg.portfolio.dd_state_3:
        if not state.recovery_mode:
            state.halt_until_date = (context.now + timedelta(days=1)).strftime("%Y-%m-%d")
            state.recovery_mode = True
            state.risk_state = "halt"
            state.effective_risk_mult = 0.0
            return False, f"max drawdown exceeded: {state.drawdown_ratio:.2%}"
        state.risk_state = "halt_recovery"
        state.effective_risk_mult = runtime.cfg.portfolio.dd_risk_mult_3
        return True, ""

    if state.drawdown_ratio > runtime.cfg.portfolio.dd_state_2:
        state.risk_state = "defense"
        state.effective_risk_mult = runtime.cfg.portfolio.dd_risk_mult_2
    elif state.drawdown_ratio > runtime.cfg.portfolio.dd_state_1:
        state.risk_state = "caution"
        state.effective_risk_mult = runtime.cfg.portfolio.dd_risk_mult_1
    else:
        state.risk_state = "normal"
        state.effective_risk_mult = 1.0

    if state.recovery_mode and state.drawdown_ratio <= runtime.cfg.portfolio.dd_state_2:
        state.recovery_mode = False
        state.halt_until_date = ""

    return True, ""


def _ensure_position_risk(
    runtime: RuntimeContext,
    state: SymbolState,
    direction: int,
    current_price: float,
    atr_val: float,
    current_eob: object,
) -> PositionRiskState:
    if state.position_risk is not None and state.position_risk.direction == direction:
        return state.position_risk

    campaign = f"{state.csymbol}-{str(current_eob).replace(' ', '_')}"
    pos = _build_position_risk_state(
        runtime=runtime,
        state=state,
        direction=direction,
        entry_price=float(current_price),
        atr_val=float(atr_val),
        campaign_id=campaign,
    )
    state.position_risk = pos
    return pos


def _update_trade_stats(pos: PositionRiskState, current_price: float) -> None:
    pos.highest_price_since_entry = max(pos.highest_price_since_entry, float(current_price))
    pos.lowest_price_since_entry = min(pos.lowest_price_since_entry, float(current_price))
    pos.bars_in_trade += 1
    risk_r = max(abs(pos.entry_price - pos.initial_stop_loss), 1e-9)

    if pos.direction > 0:
        favorable = pos.highest_price_since_entry - pos.entry_price
    else:
        favorable = pos.entry_price - pos.lowest_price_since_entry

    pos.mfe_r = max(pos.mfe_r, favorable / risk_r)


def _mark_failure_stopout(
    state: SymbolState,
    signal: TradingSignal,
    current_eob: object,
) -> TradingSignal:
    _increment_daily_stopout(state, current_eob)
    signal.daily_stopout_count = int(state.daily_stopout_count)
    return signal


def _check_1r_half_close_and_h1_trail(
    runtime: RuntimeContext,
    state: SymbolState,
    csymbol: str,
    pos: PositionRiskState,
    current_price: float,
    df_1h: pd.DataFrame,
    qty: int,
) -> tuple[bool, Optional[TradingSignal]]:
    """
    DC_Fractal_Sniper: 1:1 减半 + 1H 趋势跟踪止损
    Returns:
        (handled, signal)
    """
    direction = pos.direction
    atr_used = pos.entry_atr

    # 计算初始风险 R
    if pos.initial_risk_r <= 0:
        pos.initial_risk_r = abs(pos.entry_price - pos.initial_stop_loss)

    initial_r = pos.initial_risk_r
    current_profit = (current_price - pos.entry_price) * direction

    # --- 阶段 1: 达到 1:1 盈亏比，平半仓 ---
    if not pos.is_half_closed and current_profit >= initial_r:
        half_qty = max(1, qty // 2)
        spec = get_instrument_spec(runtime.cfg, csymbol)
        half_qty = normalize_lot(half_qty, spec.min_lot, spec.lot_step)

        if half_qty > 0:
            pos.is_half_closed = True
            pos.partial_exited = True
            pos.half_close_price = current_price

            # 剩余仓位的止损移到开仓价（保本）
            new_stop = pos.entry_price
            if direction > 0:
                new_stop = max(new_stop, pos.stop_loss)  # 只能上移
            else:
                new_stop = min(new_stop, pos.stop_loss)  # 只能下移
            pos.stop_loss = new_stop

            action = "close_half_long" if direction > 0 else "close_half_short"
            return True, _make_close_signal(
                runtime, state, csymbol, action, "Target 1:1 (half close)",
                "split_exit_1r", current_price, half_qty
            )

    # --- 阶段 2: 剩余仓位使用 1H 趋势止损 ---
    if pos.is_half_closed:
        h1_stop = calculate_h1_trailing_stop_ema(df_1h, ema_period=20)

        if h1_stop > 0:
            # 只能向有利方向移动止损
            if direction > 0:
                new_stop = max(pos.stop_loss, h1_stop)
                if current_price <= new_stop:
                    return True, _make_close_signal(
                        runtime, state, csymbol, "close_long", "1H trend stop",
                        "trend_following_stop", current_price, qty
                    )
            else:
                new_stop = min(pos.stop_loss, h1_stop)
                if current_price >= new_stop:
                    return True, _make_close_signal(
                        runtime, state, csymbol, "close_short", "1H trend stop",
                        "trend_following_stop", current_price, qty
                    )
            pos.stop_loss = new_stop

    return False, None


def _check_dynamic_stop(
    runtime: RuntimeContext,
    state: SymbolState,
    csymbol: str,
    pos: PositionRiskState,
    current_price: float,
    atr_val: float,
    qty: int,
) -> tuple[bool, Optional[TradingSignal]]:
    cfg = runtime.cfg.risk
    if not cfg.enable_dynamic_stop:
        return False, None
    if atr_val <= 0 or cfg.dynamic_stop_atr <= 0:
        return False, None
    if pos.mfe_r < cfg.dynamic_stop_activate_r:
        return False, None

    min_tick = get_min_tick(runtime.cfg, csymbol)
    stop_dist = max(cfg.dynamic_stop_atr * atr_val, min_tick)

    if pos.direction > 0:
        dyn_stop = pos.highest_price_since_entry - stop_dist
        pos.stop_loss = max(pos.stop_loss, dyn_stop)
        if current_price <= pos.stop_loss:
            sig = _make_close_signal(
                runtime,
                state,
                csymbol,
                "close_long",
                "dynamic stop",
                "dynamic_stop",
                current_price,
                qty,
            )
            return True, sig
    else:
        dyn_stop = pos.lowest_price_since_entry + stop_dist
        pos.stop_loss = min(pos.stop_loss, dyn_stop)
        if current_price >= pos.stop_loss:
            sig = _make_close_signal(
                runtime,
                state,
                csymbol,
                "close_short",
                "dynamic stop",
                "dynamic_stop",
                current_price,
                qty,
            )
            return True, sig

    return False, None


def process_risk_pipeline(
    runtime: RuntimeContext,
    state: SymbolState,
    csymbol: str,
    symbol: str,
    current_eob: object,
    current_price: float,
    atr_val: float,
    long_qty: int,
    short_qty: int,
    df_1h: pd.DataFrame | None = None,
) -> tuple[bool, Optional[TradingSignal]]:
    _ = symbol
    ensure_daily_stopout_counter(state, current_eob)
    allowed, reason = _update_portfolio_risk(runtime, runtime.context)
    if not allowed:
        if long_qty > 0:
            signal = _make_close_signal(
                runtime,
                state,
                csymbol,
                "close_long",
                f"portfolio risk pause: {reason}",
                "portfolio_pause",
                current_price,
                long_qty,
            )
            state.position_risk = None
            return True, signal
        if short_qty > 0:
            signal = _make_close_signal(
                runtime,
                state,
                csymbol,
                "close_short",
                f"portfolio risk pause: {reason}",
                "portfolio_pause",
                current_price,
                short_qty,
            )
            state.position_risk = None
            return True, signal
        signal = TradingSignal(
            action="none",
            reason=f"portfolio risk pause: {reason}",
            direction=0,
            qty=0,
            price=float(current_price),
            stop_loss=0.0,
            take_profit=0.0,
            entry_atr=0.0,
            risk_stage=runtime.portfolio_risk.risk_state,
            campaign_id="",
            created_eob=current_eob,
            exit_trigger_type="portfolio_pause",
            daily_stopout_count=int(state.daily_stopout_count),
        )
        return True, signal

    if long_qty <= 0 and short_qty <= 0:
        state.position_risk = None
        return False, None

    direction = 1 if long_qty > 0 else -1
    qty = long_qty if long_qty > 0 else short_qty
    spec = get_instrument_spec(runtime.cfg, csymbol)
    qty_lot = normalize_lot(qty, spec.min_lot, spec.lot_step)
    if qty_lot <= 0:
        return False, None

    pos = _ensure_position_risk(runtime, state, direction, current_price, atr_val, current_eob)
    _update_trade_stats(pos, current_price)

    if direction > 0 and current_price <= pos.initial_stop_loss:
        sig = _make_close_signal(runtime, state, csymbol, "close_long", "hard stop", "hard_stop", current_price, qty_lot)
        sig = _mark_failure_stopout(state, sig, current_eob)
        state.position_risk = None
        return True, sig
    if direction < 0 and current_price >= pos.initial_stop_loss:
        sig = _make_close_signal(runtime, state, csymbol, "close_short", "hard stop", "hard_stop", current_price, qty_lot)
        sig = _mark_failure_stopout(state, sig, current_eob)
        state.position_risk = None
        return True, sig

    if not pos.is_half_closed and pos.bars_in_trade >= runtime.cfg.risk.time_stop_bars:
        failed_breakout = False
        if direction > 0 and pos.entry_platform_zg > 0 and current_price <= pos.entry_platform_zg:
            failed_breakout = True
        if direction < 0 and pos.entry_platform_zd > 0 and current_price >= pos.entry_platform_zd:
            failed_breakout = True
        if failed_breakout:
            action = "close_long" if direction > 0 else "close_short"
            sig = _make_close_signal(runtime, state, csymbol, action, "time stop breakout failure", "time_stop", current_price, qty_lot)
            sig = _mark_failure_stopout(state, sig, current_eob)
            state.position_risk = None
            return True, sig

    # DC_Fractal_Sniper: 1:1 半仓 + 1H 趋势止损
    if df_1h is not None and not df_1h.empty:
        sig_handled, split_signal = _check_1r_half_close_and_h1_trail(
            runtime, state, csymbol, pos, current_price, df_1h, qty_lot
        )
        if sig_handled:
            if split_signal is not None:
                # 全平仓时清空 position_risk
                if split_signal.action in {"close_long", "close_short"}:
                    state.position_risk = None
            # 半仓后跳过 classic trail_stop 和 dynamic_stop（避免冲突）
            return True, split_signal

    # Dynamic stop: 仅在未启用 1H 半仓模式时生效
    # （半仓后由 1H EMA 接管止损）
    if not pos.is_half_closed:
        dynamic_handled, dynamic_signal = _check_dynamic_stop(
            runtime=runtime,
            state=state,
            csymbol=csymbol,
            pos=pos,
            current_price=current_price,
            atr_val=atr_val,
            qty=qty_lot,
        )
        if dynamic_handled:
            state.position_risk = None
            return True, dynamic_signal

    # 原有的 trail stop 逻辑（仅在未启用 1H 止损且未半仓时使用）
    if not pos.is_half_closed:
        trail_trigger = pos.mfe_r >= runtime.cfg.risk.trail_activate_r
        if trail_trigger and atr_val > 0:
            trail_atr = runtime.cfg.risk.trail_stop_atr
            if direction > 0:
                trail = pos.highest_price_since_entry - trail_atr * atr_val
                pos.stop_loss = max(pos.stop_loss, trail)
                if current_price <= pos.stop_loss:
                    sig = _make_close_signal(runtime, state, csymbol, "close_long", "trail stop", "trail_stop", current_price, qty_lot)
                    state.position_risk = None
                    return True, sig
            else:
                trail = pos.lowest_price_since_entry + trail_atr * atr_val
                pos.stop_loss = min(pos.stop_loss, trail)
                if current_price >= pos.stop_loss:
                    sig = _make_close_signal(runtime, state, csymbol, "close_short", "trail stop", "trail_stop", current_price, qty_lot)
                    state.position_risk = None
                    return True, sig

    return False, None
