from __future__ import annotations

from strategy.domain.instruments import get_instrument_spec, normalize_lot
from strategy.types import TradingSignal

from .exit_rules import check_dynamic_stop, maybe_apply_trailing_stop, maybe_arm_break_even
from .portfolio_guard import update_portfolio_risk
from .position_guard import ensure_daily_stopout_counter, ensure_position_risk, mark_failure_stopout, update_trade_stats
from .signal_builder import make_close_signal, seed_position_risk_from_entry_signal


def process_risk_pipeline(
    runtime,
    state,
    csymbol: str,
    symbol: str,
    current_eob: object,
    current_price: float,
    atr_val: float,
    long_qty: int,
    short_qty: int,
    frame_1h=None,
) -> tuple[bool, TradingSignal | None]:
    _ = symbol
    _ = frame_1h

    ensure_daily_stopout_counter(state, current_eob)

    allowed, reason = update_portfolio_risk(runtime, runtime.context)
    if not allowed:
        if long_qty > 0:
            sig = make_close_signal(runtime, state, csymbol, "close_long", f"portfolio halt: {reason}", "portfolio_halt", current_price, long_qty)
            state.position_risk = None
            return True, sig
        if short_qty > 0:
            sig = make_close_signal(runtime, state, csymbol, "close_short", f"portfolio halt: {reason}", "portfolio_halt", current_price, short_qty)
            state.position_risk = None
            return True, sig
        return True, TradingSignal(
            action="none",
            reason=f"portfolio halt: {reason}",
            direction=0,
            qty=0,
            price=float(current_price),
            stop_loss=0.0,
            take_profit=0.0,
            entry_atr=0.0,
            risk_stage=runtime.portfolio_risk.risk_state,
            campaign_id="",
            created_eob=current_eob,
        )

    if long_qty <= 0 and short_qty <= 0:
        state.position_risk = None
        return False, None

    direction = 1 if long_qty > 0 else -1
    qty = long_qty if long_qty > 0 else short_qty

    spec = get_instrument_spec(runtime.cfg, csymbol)
    qty_lot = normalize_lot(qty, spec.min_lot, spec.lot_step)
    if qty_lot <= 0:
        state.position_risk = None
        return False, None

    pos = ensure_position_risk(runtime, state, direction, current_price, atr_val, current_eob)
    update_trade_stats(pos, current_price)

    hard_stop_hit = (direction > 0 and current_price <= pos.initial_stop_loss) or (direction < 0 and current_price >= pos.initial_stop_loss)
    if hard_stop_hit:
        action = "close_long" if direction > 0 else "close_short"
        sig = make_close_signal(runtime, state, csymbol, action, "hard stop", "hard_stop", current_price, qty_lot)
        sig = mark_failure_stopout(state, sig, current_eob)
        state.position_risk = None
        return True, sig

    maybe_arm_break_even(runtime, pos, current_price)
    maybe_apply_trailing_stop(runtime, pos, current_price, atr_val)

    if not pos.break_even_armed:
        handled, dyn_signal = check_dynamic_stop(runtime, state, csymbol, pos, current_price, atr_val, qty_lot)
        if handled:
            state.position_risk = None
            return True, dyn_signal

    stop_hit = (direction > 0 and current_price <= pos.stop_loss) or (direction < 0 and current_price >= pos.stop_loss)
    if stop_hit:
        action = "close_long" if direction > 0 else "close_short"
        sig = make_close_signal(runtime, state, csymbol, action, "stop loss", "stop_loss", current_price, qty_lot)
        state.position_risk = None
        return True, sig

    if pos.bars_in_trade >= runtime.cfg.risk.time_stop_bars:
        action = "close_long" if direction > 0 else "close_short"
        sig = make_close_signal(runtime, state, csymbol, action, "time stop", "time_stop", current_price, qty_lot)
        state.position_risk = None
        return True, sig

    return False, None


__all__ = [
    "process_risk_pipeline",
    "seed_position_risk_from_entry_signal",
]
