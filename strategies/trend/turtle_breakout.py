from __future__ import annotations

import math
from dataclasses import dataclass

from quantframe.app.config import AppConfig, LoadedResources
from quantframe.core.models import SignalDecision, TargetPosition
from quantframe.trend import DecisionContext, SignalModel, TargetAllocator, TrendStrategy
from strategies.trend.common import MaxContractRiskOverlay, TargetQuantityExecutionPlanner, average_true_range


@dataclass(frozen=True)
class TurtleSignalModel(SignalModel):
    entry_window: int
    exit_window: int
    atr_window: int
    breakout_buffer_atr: float = 0.0

    def generate(self, context: DecisionContext) -> SignalDecision | None:
        bars = list(context.bars)
        min_bars = max(self.entry_window, self.exit_window, self.atr_window) + 1
        if len(bars) < min_bars:
            return None

        history = bars[:-1]
        current = bars[-1]
        highs = [float(bar.high) for bar in history]
        lows = [float(bar.low) for bar in history]
        closes = [float(bar.close) for bar in history]
        atr = average_true_range(highs, lows, closes, self.atr_window)
        if atr <= 0:
            return None

        entry_high = max(highs[-self.entry_window :])
        entry_low = min(lows[-self.entry_window :])
        exit_high = max(highs[-self.exit_window :])
        exit_low = min(lows[-self.exit_window :])
        close = float(current.close)
        buffer_points = atr * max(self.breakout_buffer_atr, 0.0)

        direction = 0
        reason = ""
        trigger_price = 0.0
        current_qty = context.position.signed_qty
        if current_qty > 0 and close < exit_low:
            direction = 0
            reason = "turtle_exit_long"
            trigger_price = exit_low
        elif current_qty < 0 and close > exit_high:
            direction = 0
            reason = "turtle_exit_short"
            trigger_price = exit_high
        elif current_qty <= 0 and close > entry_high + buffer_points:
            direction = 1
            reason = "turtle_breakout_long"
            trigger_price = entry_high
        elif current_qty >= 0 and close < entry_low - buffer_points:
            direction = -1
            reason = "turtle_breakout_short"
            trigger_price = entry_low
        else:
            return None

        breakout_distance = abs(close - trigger_price)
        strength = min(max(breakout_distance / atr, 0.5), 2.0) / 2.0
        return SignalDecision(
            instrument_id=context.instrument.instrument_id,
            symbol=context.instrument.platform_symbol,
            direction=direction,
            strength=strength,
            reason=reason,
            metadata={
                "atr": atr,
                "entry_high": entry_high,
                "entry_low": entry_low,
                "exit_high": exit_high,
                "exit_low": exit_low,
                "close": close,
            },
        )


@dataclass(frozen=True)
class TurtleAtrTargetAllocator(TargetAllocator):
    risk_per_trade_ratio: float
    atr_stop_multiple: float
    max_position_ratio: float

    def allocate(self, context: DecisionContext, signal: SignalDecision | None) -> TargetPosition | None:
        if signal is None:
            return None
        if signal.direction == 0:
            return TargetPosition(
                instrument_id=context.instrument.instrument_id,
                symbol=context.instrument.platform_symbol,
                target_qty=0,
                notional_budget=0.0,
                reason=signal.reason,
                metadata=signal.metadata,
            )

        atr = float(signal.metadata.get("atr", 0.0) or 0.0)
        current_price = float(signal.metadata.get("close", context.bars[-1].close) or 0.0)
        if atr <= 0 or current_price <= 0:
            return None

        risk_budget = context.portfolio.equity * max(self.risk_per_trade_ratio, 0.0) * max(signal.strength, 0.25)
        risk_per_contract = atr * max(self.atr_stop_multiple, 0.1) * context.instrument.multiplier * context.instrument.lot_size
        notional_cap = context.portfolio.equity * max(self.max_position_ratio, 0.0)
        lot_notional = current_price * context.instrument.multiplier * context.instrument.lot_size
        if risk_per_contract <= 0 or lot_notional <= 0:
            return None

        risk_qty = math.floor(risk_budget / risk_per_contract)
        notional_qty = math.floor(notional_cap / lot_notional) if notional_cap > 0 else risk_qty
        qty = max(min(risk_qty, notional_qty), 0) * context.instrument.lot_size * signal.direction
        return TargetPosition(
            instrument_id=context.instrument.instrument_id,
            symbol=context.instrument.platform_symbol,
            target_qty=qty,
            notional_budget=risk_budget,
            reason=signal.reason,
            metadata=signal.metadata,
        )


def create_strategy(config: AppConfig, resources: LoadedResources) -> TrendStrategy:
    _ = resources
    params = dict(config.strategy.params)
    return TrendStrategy(
        name="trend.turtle_breakout",
        decision_frequency=str(params.get("decision_frequency", "1d")).strip(),
        history_bars=max(int(params.get("history_bars", 120) or 120), 30),
        signal_model=TurtleSignalModel(
            entry_window=max(int(params.get("entry_window", 20) or 20), 5),
            exit_window=max(int(params.get("exit_window", 10) or 10), 2),
            atr_window=max(int(params.get("atr_window", 20) or 20), 5),
            breakout_buffer_atr=max(float(params.get("breakout_buffer_atr", 0.0) or 0.0), 0.0),
        ),
        target_allocator=TurtleAtrTargetAllocator(
            risk_per_trade_ratio=max(float(params.get("risk_per_trade_ratio", 0.01) or 0.0), 0.0),
            atr_stop_multiple=max(float(params.get("atr_stop_multiple", 2.0) or 0.0), 0.1),
            max_position_ratio=max(float(params.get("max_position_ratio", 0.20) or 0.0), 0.0),
        ),
        risk_overlay=MaxContractRiskOverlay(
            max_abs_contracts=max(int(params.get("max_abs_contracts", 4) or 0), 0),
        ),
        execution_planner=TargetQuantityExecutionPlanner(),
    )
