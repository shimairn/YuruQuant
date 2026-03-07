from __future__ import annotations

from yuruquant.app.config import AppConfig
from yuruquant.core.frames import KlineFrame
from yuruquant.core.indicators import latest_sma
from yuruquant.core.models import EntrySignal, ExitSignal, ManagedPosition


def build_managed_position(signal: EntrySignal) -> ManagedPosition:
    return ManagedPosition(
        entry_price=float(signal.price),
        direction=int(signal.direction),
        qty=int(signal.qty),
        entry_atr=float(signal.entry_atr),
        initial_stop_loss=float(signal.stop_loss),
        stop_loss=float(signal.stop_loss),
        protected_stop_price=float(signal.protected_stop_price),
        phase='armed',
        campaign_id=signal.campaign_id,
        entry_eob=signal.created_at,
        breakout_anchor=float(signal.breakout_anchor),
        highest_price_since_entry=float(signal.price),
        lowest_price_since_entry=float(signal.price),
    )


def _pnl(position: ManagedPosition, current_price: float, multiplier: float, cost_ratio: float) -> tuple[float, float]:
    gross = (float(current_price) - position.entry_price) * position.direction * multiplier * position.qty
    turnover = (abs(position.entry_price) + abs(float(current_price))) * multiplier * position.qty
    net = gross - turnover * max(float(cost_ratio), 0.0)
    return float(gross), float(net)


def _update_position(position: ManagedPosition, current_price: float) -> None:
    price = float(current_price)
    position.highest_price_since_entry = max(position.highest_price_since_entry, price)
    position.lowest_price_since_entry = min(position.lowest_price_since_entry, price)
    position.bars_in_trade += 1
    risk_r = max(abs(position.entry_price - position.initial_stop_loss), 1e-9)
    favorable = position.highest_price_since_entry - position.entry_price if position.direction > 0 else position.entry_price - position.lowest_price_since_entry
    position.mfe_r = max(position.mfe_r, favorable / risk_r)


def _apply_state_machine(config: AppConfig, position: ManagedPosition, frame_5m: KlineFrame) -> None:
    if position.phase == 'armed' and position.mfe_r >= config.strategy.exit.protected_activate_r:
        position.phase = 'protected'
        if position.direction > 0:
            position.stop_loss = max(position.stop_loss, position.protected_stop_price)
        else:
            position.stop_loss = min(position.stop_loss, position.protected_stop_price)

    if position.mfe_r >= config.strategy.exit.trend_ride_activate_r:
        position.phase = 'trend_ride'

    if position.phase == 'trend_ride' and len(frame_5m) >= config.strategy.exit.trailing_ma_period:
        trailing_ma = latest_sma(frame_5m, config.strategy.exit.trailing_ma_period)
        if position.direction > 0:
            position.stop_loss = max(position.protected_stop_price, trailing_ma)
        else:
            position.stop_loss = min(position.protected_stop_price, trailing_ma)


def _stop_trigger(position: ManagedPosition, current_price: float) -> str | None:
    stop_hit = current_price <= position.stop_loss if position.direction > 0 else current_price >= position.stop_loss
    if not stop_hit:
        return None
    if position.phase == 'armed':
        return 'hard_stop'
    if position.phase == 'protected':
        return 'protected_stop'
    return 'trend_ma_stop'


def _make_exit_signal(position: ManagedPosition, action: str, reason: str, trigger: str, current_price: float, current_eob: object, multiplier: float, cost_ratio: float) -> ExitSignal:
    gross, net = _pnl(position, current_price, multiplier, cost_ratio)
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


def evaluate_exit_signal(config: AppConfig, position: ManagedPosition, frame_5m: KlineFrame, current_eob: object, multiplier: float, cost_ratio: float) -> ExitSignal | None:
    if frame_5m.empty_frame:
        return None
    current_price = frame_5m.latest_close()
    _update_position(position, current_price)
    _apply_state_machine(config, position, frame_5m)
    action = 'close_long' if position.direction > 0 else 'close_short'
    trigger = _stop_trigger(position, current_price)
    if trigger is None:
        return None
    return _make_exit_signal(position, action, trigger.replace('_', ' '), trigger, current_price, current_eob, multiplier, cost_ratio)


def make_flatten_signal(position: ManagedPosition, current_price: float, current_eob: object, multiplier: float, cost_ratio: float, reason: str) -> ExitSignal:
    action = 'close_long' if position.direction > 0 else 'close_short'
    return _make_exit_signal(position, action, reason, 'portfolio_halt', current_price, current_eob, multiplier, cost_ratio)
