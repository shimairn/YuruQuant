from __future__ import annotations

from yuruquant.app.config import AppConfig
from yuruquant.core.frames import KlineFrame
from yuruquant.core.indicators import latest_atr, latest_donchian_channel
from yuruquant.core.models import EntrySignal, EnvironmentSnapshot, InstrumentSpec, PortfolioRuntime
from yuruquant.core.time import make_event_id
from yuruquant.strategy.trend_breakout.risk_model import resolve_order_qty
from yuruquant.strategy.trend_breakout.session_windows import blocked_by_session_end, is_in_session


ATR_PERIOD = 14


def _instrument_spec(config: AppConfig, csymbol: str) -> InstrumentSpec:
    return config.universe.instrument_overrides.get(csymbol, config.universe.instrument_defaults)


def _build_signal(
    config: AppConfig,
    portfolio: PortfolioRuntime,
    environment: EnvironmentSnapshot,
    spec: InstrumentSpec,
    csymbol: str,
    direction: int,
    current_price: float,
    atr_value: float,
    current_eob: object,
    breakout_anchor: float,
) -> EntrySignal | None:
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


def maybe_generate_entry(
    config: AppConfig,
    portfolio: PortfolioRuntime,
    environment: EnvironmentSnapshot,
    csymbol: str,
    frame_5m: KlineFrame,
    current_eob: object,
) -> EntrySignal | None:
    if frame_5m.empty_frame or not environment.trend_ok:
        return None
    spec = _instrument_spec(config, csymbol)
    if not is_in_session(spec, current_eob):
        return None
    if blocked_by_session_end(spec, current_eob, config.universe.entry_frequency, config.strategy.entry.session_end_buffer_bars):
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

    breakout_buffer = config.strategy.entry.breakout_atr_buffer * atr_value
    direction = 0
    breakout_anchor = 0.0
    if close > upper + breakout_buffer:
        direction = 1
        breakout_anchor = upper
    elif close < lower - breakout_buffer:
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
