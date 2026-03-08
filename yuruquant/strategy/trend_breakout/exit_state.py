from __future__ import annotations

from yuruquant.app.config_schema import AppConfig
from yuruquant.core.frames import KlineFrame
from yuruquant.core.models import EntrySignal, EnvironmentSnapshot, ExitSignal, InstrumentSpec, ManagedPosition
from yuruquant.strategy.trend_breakout.session_windows import blocked_by_session_end, major_session_end_approaching


ARMED_FLUSH_TRIGGER = 'armed_flush'
SESSION_FLAT_TRIGGER = 'session_flat'
ASCENDED_PROFIT_FLOOR_R = 0.5


def build_managed_position(signal: EntrySignal, fill_price: float | None = None, fill_ts: object | None = None) -> ManagedPosition:
    signal_price = float(signal.price)
    entry_price = float(fill_price) if fill_price is not None else signal_price
    fill_shift = entry_price - signal_price
    entry_time = fill_ts if fill_ts is not None else signal.created_at
    initial_stop_loss = float(signal.stop_loss) + fill_shift
    protected_stop_price = float(signal.protected_stop_price) + fill_shift
    return ManagedPosition(
        entry_price=entry_price,
        direction=int(signal.direction),
        qty=int(signal.qty),
        entry_atr=float(signal.entry_atr),
        initial_stop_loss=initial_stop_loss,
        stop_loss=initial_stop_loss,
        protected_stop_price=protected_stop_price,
        phase='armed',
        campaign_id=signal.campaign_id,
        entry_eob=entry_time,
        breakout_anchor=float(signal.breakout_anchor),
        highest_price_since_entry=entry_price,
        lowest_price_since_entry=entry_price,
    )


def compute_exit_pnl(position: ManagedPosition, current_price: float, multiplier: float, cost_ratio: float) -> tuple[float, float]:
    gross = (float(current_price) - position.entry_price) * position.direction * multiplier * position.qty
    turnover = (abs(position.entry_price) + abs(float(current_price))) * multiplier * position.qty
    net = gross - turnover * max(float(cost_ratio), 0.0)
    return float(gross), float(net)


def _update_position(position: ManagedPosition, frame_5m: KlineFrame) -> float:
    current_close = frame_5m.latest_close()
    current_high = frame_5m.latest_high()
    current_low = frame_5m.latest_low()
    position.highest_price_since_entry = max(position.highest_price_since_entry, current_high)
    position.lowest_price_since_entry = min(position.lowest_price_since_entry, current_low)
    position.bars_in_trade += 1
    risk_r = max(abs(position.entry_price - position.initial_stop_loss), 1e-9)
    favorable = position.highest_price_since_entry - position.entry_price if position.direction > 0 else position.entry_price - position.lowest_price_since_entry
    position.mfe_r = max(position.mfe_r, favorable / risk_r)
    return current_close


def _protected_floor(position: ManagedPosition) -> float:
    if position.direction > 0:
        return max(position.stop_loss, position.protected_stop_price)
    return min(position.stop_loss, position.protected_stop_price)


def _ascended_floor(position: ManagedPosition) -> float:
    initial_risk = max(abs(position.entry_price - position.initial_stop_loss), 1e-9)
    if position.direction > 0:
        profit_floor = position.entry_price + ASCENDED_PROFIT_FLOOR_R * initial_risk
        return max(position.stop_loss, position.protected_stop_price, profit_floor)
    profit_floor = position.entry_price - ASCENDED_PROFIT_FLOOR_R * initial_risk
    return min(position.stop_loss, position.protected_stop_price, profit_floor)


def _apply_state_machine(config: AppConfig, position: ManagedPosition) -> None:
    if position.phase == 'armed' and position.mfe_r >= config.strategy.exit.protected_activate_r:
        position.phase = 'protected'
        position.stop_loss = _protected_floor(position)

    if position.phase != 'ascended' and position.mfe_r >= config.strategy.exit.ascended_activate_r:
        position.phase = 'ascended'
        position.stop_loss = _ascended_floor(position)


def _stop_trigger(position: ManagedPosition, current_price: float, environment: EnvironmentSnapshot) -> str | None:
    _ = environment
    stop_hit = current_price <= position.stop_loss if position.direction > 0 else current_price >= position.stop_loss
    if stop_hit:
        if position.phase == 'armed':
            return 'hard_stop'
        return 'protected_stop'
    return None


def _should_flatten_by_session_end(config: AppConfig, spec: InstrumentSpec, current_eob: object) -> bool:
    return blocked_by_session_end(
        spec=spec,
        eob=current_eob,
        frequency=config.universe.entry_frequency,
        buffer_bars=config.strategy.exit.session_flat_all_phases_buffer_bars,
    )


def _should_flush_armed_position(config: AppConfig, position: ManagedPosition, spec: InstrumentSpec, current_eob: object) -> bool:
    if position.phase != 'armed':
        return False
    return major_session_end_approaching(
        spec=spec,
        eob=current_eob,
        frequency=config.universe.entry_frequency,
        buffer_bars=config.strategy.exit.armed_flush_buffer_bars,
        min_gap_minutes=config.strategy.exit.armed_flush_min_gap_minutes,
    )


def _make_exit_signal(position: ManagedPosition, action: str, reason: str, trigger: str, current_price: float, current_eob: object, multiplier: float, cost_ratio: float) -> ExitSignal:
    gross, net = compute_exit_pnl(position, current_price, multiplier, cost_ratio)
    return ExitSignal(
        action=action,
        reason=reason,
        direction=position.direction,
        qty=position.qty,
        price=float(current_price),
        created_at=current_eob,
        exit_trigger=trigger,
        campaign_id=position.campaign_id,
        holding_bars=position.bars_in_trade,
        mfe_r=position.mfe_r,
        gross_pnl=gross,
        net_pnl=net,
        phase=position.phase,
    )


def evaluate_exit_signal(
    config: AppConfig,
    position: ManagedPosition,
    frame_5m: KlineFrame,
    environment: EnvironmentSnapshot,
    current_eob: object,
    spec: InstrumentSpec,
    multiplier: float,
    cost_ratio: float,
) -> ExitSignal | None:
    if frame_5m.empty_frame:
        return None
    current_price = _update_position(position, frame_5m)
    _apply_state_machine(config, position)
    action = 'close_long' if position.direction > 0 else 'close_short'
    trigger = _stop_trigger(position, current_price, environment)
    if trigger is not None:
        return _make_exit_signal(position, action, trigger.replace('_', ' '), trigger, current_price, current_eob, multiplier, cost_ratio)
    if _should_flatten_by_session_end(config, spec, current_eob):
        return _make_exit_signal(position, action, 'session flat', SESSION_FLAT_TRIGGER, current_price, current_eob, multiplier, cost_ratio)
    if _should_flush_armed_position(config, position, spec, current_eob):
        return _make_exit_signal(position, action, 'armed flush', ARMED_FLUSH_TRIGGER, current_price, current_eob, multiplier, cost_ratio)
    return None


def make_flatten_signal(position: ManagedPosition, current_price: float, current_eob: object, multiplier: float, cost_ratio: float, reason: str) -> ExitSignal:
    action = 'close_long' if position.direction > 0 else 'close_short'
    return _make_exit_signal(position, action, reason, 'portfolio_halt', current_price, current_eob, multiplier, cost_ratio)
