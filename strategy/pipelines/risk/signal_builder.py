from __future__ import annotations

from strategy.core.time_utils import make_event_id
from strategy.domain.instruments import get_instrument_spec
from strategy.pipelines.common import estimate_turnover_cost
from strategy.types import PositionRiskState, TradingSignal


def build_position_risk_state(
    runtime,
    state,
    direction: int,
    entry_price: float,
    atr_val: float,
    campaign_id: str,
    entry_eob: object,
    initial_stop_loss: float | None = None,
) -> PositionRiskState:
    spec = get_instrument_spec(runtime.cfg, state.csymbol)
    min_tick = max(float(spec.min_tick), 1e-6)
    atr_used = atr_val if atr_val > 0 else max(abs(entry_price) * 0.005, min_tick)
    dist = max(runtime.cfg.risk.hard_stop_atr * atr_used, min_tick)
    init_stop = (
        float(initial_stop_loss)
        if initial_stop_loss is not None and float(initial_stop_loss) > 0
        else (entry_price - dist if direction > 0 else entry_price + dist)
    )

    return PositionRiskState(
        entry_price=float(entry_price),
        direction=int(direction),
        entry_atr=float(atr_used),
        initial_stop_loss=float(init_stop),
        stop_loss=float(init_stop),
        campaign_id=campaign_id,
        entry_eob=entry_eob,
        highest_price_since_entry=float(entry_price),
        lowest_price_since_entry=float(entry_price),
    )


def seed_position_risk_from_entry_signal(runtime, state, signal, current_price: float, current_eob: object) -> None:
    if signal.action not in {"buy", "sell"}:
        return
    direction = 1 if signal.action == "buy" else -1
    entry_price = float(signal.price) if float(signal.price) > 0 else float(current_price)
    atr_val = float(signal.entry_atr) if float(signal.entry_atr) > 0 else 0.0
    campaign = signal.campaign_id or make_event_id(state.csymbol, current_eob)

    state.position_risk = build_position_risk_state(
        runtime=runtime,
        state=state,
        direction=direction,
        entry_price=entry_price,
        atr_val=atr_val,
        campaign_id=campaign,
        entry_eob=current_eob,
        initial_stop_loss=float(signal.stop_loss) if float(signal.stop_loss) > 0 else None,
    )


def make_close_signal(runtime, state, csymbol: str, action: str, reason: str, trigger: str, current_price: float, qty: int) -> TradingSignal:
    pos = state.position_risk
    direction = int(pos.direction) if pos is not None else (1 if action == "close_long" else -1)
    entry_price = float(pos.entry_price) if pos is not None else float(current_price)
    campaign = pos.campaign_id if pos is not None else ""
    multiplier = float(get_instrument_spec(runtime.cfg, csymbol).multiplier)

    gross_pnl = (float(current_price) - entry_price) * direction * multiplier * max(int(qty), 0)
    ratio = runtime.cfg.risk.backtest_commission_ratio + runtime.cfg.risk.backtest_slippage_ratio
    est_cost = estimate_turnover_cost(entry_price, current_price, qty, multiplier, ratio)

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
        campaign_id=campaign,
        created_eob=None,
        exit_trigger_type=trigger,
        est_cost=float(est_cost),
        gross_pnl=float(gross_pnl),
        net_pnl=float(gross_pnl - est_cost),
        holding_bars=int(pos.bars_in_trade) if pos is not None else 0,
        mfe_r=float(pos.mfe_r) if pos is not None else 0.0,
        daily_stopout_count=int(state.daily_stopout_count),
        trend_strength=float(state.h1_strength),
    )
