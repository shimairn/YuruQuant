from __future__ import annotations

from .signal_builder import make_close_signal


def maybe_arm_break_even(runtime, pos, current_price: float) -> None:
    if pos.break_even_armed:
        return
    if pos.mfe_r >= runtime.cfg.risk.break_even_activate_r:
        pos.break_even_armed = True
        if pos.direction > 0:
            pos.stop_loss = max(pos.stop_loss, pos.entry_price)
        else:
            pos.stop_loss = min(pos.stop_loss, pos.entry_price)


def maybe_apply_trailing_stop(runtime, pos, current_price: float, atr_val: float) -> None:
    if atr_val <= 0:
        return
    if pos.mfe_r < runtime.cfg.risk.trail_activate_r:
        return

    trail_dist = runtime.cfg.risk.trail_stop_atr * atr_val
    if pos.direction > 0:
        trail = pos.highest_price_since_entry - trail_dist
        pos.stop_loss = max(pos.stop_loss, trail)
    else:
        trail = pos.lowest_price_since_entry + trail_dist
        pos.stop_loss = min(pos.stop_loss, trail)


def check_dynamic_stop(runtime, state, csymbol: str, pos, current_price: float, atr_val: float, qty: int):
    if not runtime.cfg.risk.dynamic_stop_enabled or atr_val <= 0:
        return False, None
    if pos.mfe_r < runtime.cfg.risk.dynamic_stop_activate_r:
        return False, None

    stop_dist = runtime.cfg.risk.dynamic_stop_atr * atr_val
    if pos.direction > 0:
        dyn_stop = pos.highest_price_since_entry - stop_dist
        pos.stop_loss = max(pos.stop_loss, dyn_stop)
        if current_price <= pos.stop_loss:
            return True, make_close_signal(runtime, state, csymbol, "close_long", "dynamic stop", "dynamic_stop", current_price, qty)
    else:
        dyn_stop = pos.lowest_price_since_entry + stop_dist
        pos.stop_loss = min(pos.stop_loss, dyn_stop)
        if current_price >= pos.stop_loss:
            return True, make_close_signal(runtime, state, csymbol, "close_short", "dynamic stop", "dynamic_stop", current_price, qty)
    return False, None
