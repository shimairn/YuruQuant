from __future__ import annotations

from strategy.core.time_utils import make_event_id, to_trade_day
from strategy.types import PositionRiskState, SymbolState

from .signal_builder import build_position_risk_state


def ensure_daily_stopout_counter(state: SymbolState, current_eob: object) -> None:
    trade_day = to_trade_day(current_eob)
    if state.daily_stopout_date != trade_day:
        state.daily_stopout_date = trade_day
        state.daily_stopout_count = 0


def increment_daily_stopout(state: SymbolState, current_eob: object) -> None:
    ensure_daily_stopout_counter(state, current_eob)
    state.daily_stopout_count += 1


def ensure_position_risk(runtime, state: SymbolState, direction: int, current_price: float, atr_val: float, current_eob: object) -> PositionRiskState:
    if state.position_risk is not None and state.position_risk.direction == int(direction):
        return state.position_risk

    campaign = make_event_id(state.csymbol, current_eob)
    pos = build_position_risk_state(
        runtime=runtime,
        state=state,
        direction=int(direction),
        entry_price=float(current_price),
        atr_val=float(atr_val),
        campaign_id=campaign,
        entry_eob=current_eob,
    )
    state.position_risk = pos
    return pos


def update_trade_stats(pos: PositionRiskState, current_price: float) -> None:
    cp = float(current_price)
    pos.highest_price_since_entry = max(pos.highest_price_since_entry, cp)
    pos.lowest_price_since_entry = min(pos.lowest_price_since_entry, cp)
    pos.bars_in_trade += 1

    risk_r = max(abs(pos.entry_price - pos.initial_stop_loss), 1e-9)
    favorable = (pos.highest_price_since_entry - pos.entry_price) if pos.direction > 0 else (pos.entry_price - pos.lowest_price_since_entry)
    pos.mfe_r = max(pos.mfe_r, favorable / risk_r)


def mark_failure_stopout(state: SymbolState, signal, current_eob: object):
    increment_daily_stopout(state, current_eob)
    signal.daily_stopout_count = int(state.daily_stopout_count)
    return signal
