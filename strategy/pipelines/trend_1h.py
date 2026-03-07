from __future__ import annotations

from strategy.core.indicators import compute_trend_1h
from strategy.core.kline_types import KlineFrame
from strategy.core.time_utils import is_after
from strategy.types import StrategySettings, SymbolState


def refresh_h1_trend_state(state: SymbolState, frame_1h: KlineFrame, settings: StrategySettings) -> None:
    if frame_1h.empty:
        return

    latest_eob = frame_1h.latest_eob()
    if latest_eob is None:
        return
    if state.last_h1_eob is not None and not is_after(latest_eob, state.last_h1_eob):
        return

    snap = compute_trend_1h(
        frame_1h,
        ema_fast_period=settings.trend_ema_fast_1h,
        ema_slow_period=settings.trend_ema_slow_1h,
        atr_period=settings.atr_period,
    )
    state.h1_trend = int(snap.direction)
    state.h1_strength = float(snap.strength)
    state.last_h1_eob = latest_eob
