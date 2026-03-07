from __future__ import annotations

from yuruquant.app.config import AppConfig
from yuruquant.core.frames import KlineFrame
from yuruquant.core.indicators import latest_atr, latest_donchian_channel
from yuruquant.core.models import EntrySignal, EnvironmentSnapshot, InstrumentSpec, PortfolioRuntime
from yuruquant.core.time import make_event_id, parse_datetime
from yuruquant.strategy.trend_breakout.risk_model import resolve_order_qty


ATR_PERIOD = 14


def _instrument_spec(config: AppConfig, csymbol: str) -> InstrumentSpec:
    return config.universe.instrument_overrides.get(csymbol, config.universe.instrument_defaults)


def _parse_hhmm(value: str) -> int:
    hh, mm = value.split(':', 1)
    return int(hh) * 60 + int(mm)


def _in_ranges(minute_of_day: int, ranges: list[tuple[str, str]]) -> bool:
    for start, end in ranges:
        start_minute = _parse_hhmm(start)
        end_minute = _parse_hhmm(end)
        if start_minute <= end_minute and start_minute <= minute_of_day <= end_minute:
            return True
        if start_minute > end_minute and (minute_of_day >= start_minute or minute_of_day <= end_minute):
            return True
    return False


def _is_in_session(spec: InstrumentSpec, eob: object) -> bool:
    dt = parse_datetime(eob)
    minute_of_day = dt.hour * 60 + dt.minute
    return _in_ranges(minute_of_day, spec.sessions_day) or _in_ranges(minute_of_day, spec.sessions_night)


def _latest_bar_extremes(frame_5m: KlineFrame) -> tuple[float, float]:
    if frame_5m.empty_frame:
        return 0.0, 0.0
    high = float(frame_5m.frame.get_column('high').item(-1) or 0.0)
    low = float(frame_5m.frame.get_column('low').item(-1) or 0.0)
    return high, low


def _close_position(close: float, low: float, high: float) -> float:
    bar_range = float(high) - float(low)
    if bar_range <= 0:
        return 0.5
    return (float(close) - float(low)) / bar_range


def _build_signal(config: AppConfig, portfolio: PortfolioRuntime, environment: EnvironmentSnapshot, spec: InstrumentSpec, csymbol: str, direction: int, current_price: float, atr_value: float, current_eob: object, breakout_anchor: float) -> EntrySignal | None:
    qty = resolve_order_qty(
        portfolio=portfolio,
        spec=spec,
        risk_per_trade_ratio=config.portfolio.risk_per_trade_ratio,
        current_price=current_price,
        atr_value=atr_value,
        hard_stop_atr=config.strategy.exit.hard_stop_atr,
    )
    if qty <= 0:
        return None
    stop_distance = config.strategy.exit.hard_stop_atr * atr_value
    stop_loss = current_price - stop_distance if direction > 0 else current_price + stop_distance
    cost_ratio = config.execution.backtest_commission_ratio + config.execution.backtest_slippage_ratio
    compensation = max(spec.min_tick, current_price * max(cost_ratio, 0.0) * 2.0)
    protected_stop = current_price + compensation if direction > 0 else current_price - compensation
    action = 'buy' if direction > 0 else 'sell'
    return EntrySignal(
        action=action,
        reason='dual_core_breakout',
        direction=direction,
        qty=qty,
        price=float(current_price),
        stop_loss=float(stop_loss),
        protected_stop_price=float(protected_stop),
        created_at=current_eob,
        entry_atr=float(atr_value),
        breakout_anchor=float(breakout_anchor),
        campaign_id=make_event_id(csymbol, current_eob),
        environment_ma=float(environment.moving_average),
        macd_histogram=float(environment.macd_histogram),
    )


def maybe_generate_entry(config: AppConfig, portfolio: PortfolioRuntime, environment: EnvironmentSnapshot, csymbol: str, frame_5m: KlineFrame, current_eob: object) -> EntrySignal | None:
    if frame_5m.empty_frame or not environment.trend_ok:
        return None
    spec = _instrument_spec(config, csymbol)
    if not _is_in_session(spec, current_eob):
        return None
    atr_value = latest_atr(frame_5m, ATR_PERIOD)
    if atr_value <= 0:
        return None
    upper, lower, close = latest_donchian_channel(frame_5m, config.strategy.entry.donchian_lookback)
    if upper <= 0 or lower <= 0 or close <= 0:
        return None
    channel_width = upper - lower
    if channel_width <= config.strategy.entry.min_channel_width_atr * atr_value:
        return None

    current_high, current_low = _latest_bar_extremes(frame_5m)
    close_position = _close_position(close, current_low, current_high)
    breakout_buffer = config.strategy.entry.breakout_atr_buffer * atr_value
    close_position_min = config.strategy.entry.breakout_close_position_min
    short_close_position_max = 1.0 - close_position_min

    direction = 0
    breakout_anchor = 0.0
    if close > upper + breakout_buffer:
        if close_position < close_position_min:
            return None
        direction = 1
        breakout_anchor = upper
    elif close < lower - breakout_buffer:
        if close_position > short_close_position_max:
            return None
        direction = -1
        breakout_anchor = lower
    if direction == 0 or direction != environment.direction:
        return None
    return _build_signal(
        config=config,
        portfolio=portfolio,
        environment=environment,
        spec=spec,
        csymbol=csymbol,
        direction=direction,
        current_price=close,
        atr_value=atr_value,
        current_eob=current_eob,
        breakout_anchor=breakout_anchor,
    )
