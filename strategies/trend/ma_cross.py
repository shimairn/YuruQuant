from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import pstdev

from quantframe.app.config import AppConfig, LoadedResources
from quantframe.core.models import SignalDecision, TargetPosition
from quantframe.trend import DecisionContext, SignalModel, TargetAllocator, TrendStrategy
from strategies.trend.common import MaxContractRiskOverlay, TargetQuantityExecutionPlanner


def _sma(values: list[float], window: int) -> float:
    if len(values) < window or window <= 0:
        return 0.0
    subset = values[-window:]
    return sum(subset) / len(subset)


def _volatility(values: list[float], window: int) -> float:
    if len(values) <= window or window <= 1:
        return 0.0
    returns = []
    for index in range(1, len(values)):
        previous = values[index - 1]
        current = values[index]
        if previous <= 0 or current <= 0:
            continue
        returns.append(math.log(current / previous))
    if len(returns) < window:
        return 0.0
    return float(pstdev(returns[-window:]))


@dataclass(frozen=True)
class MovingAverageSignalModel(SignalModel):
    fast_window: int
    slow_window: int
    min_gap: float

    def generate(self, context: DecisionContext) -> SignalDecision | None:
        closes = [float(bar.close) for bar in context.bars]
        if len(closes) < self.slow_window:
            return None
        fast = _sma(closes, self.fast_window)
        slow = _sma(closes, self.slow_window)
        if slow == 0:
            return None
        gap = (fast - slow) / abs(slow)
        if abs(gap) < self.min_gap:
            return SignalDecision(
                instrument_id=context.instrument.instrument_id,
                symbol=context.instrument.platform_symbol,
                direction=0,
                strength=0.0,
                reason="ma_gap_below_threshold",
                metadata={"fast_ma": fast, "slow_ma": slow},
            )
        direction = 1 if gap > 0 else -1
        strength = min(abs(gap) * 20.0, 1.0)
        return SignalDecision(
            instrument_id=context.instrument.instrument_id,
            symbol=context.instrument.platform_symbol,
            direction=direction,
            strength=strength,
            reason="ma_cross_trend",
            metadata={"fast_ma": fast, "slow_ma": slow},
        )


@dataclass(frozen=True)
class VolatilityTargetAllocator(TargetAllocator):
    risk_budget_ratio: float
    max_position_ratio: float
    vol_window: int

    def allocate(self, context: DecisionContext, signal: SignalDecision | None) -> TargetPosition | None:
        if signal is None:
            return None
        if signal.direction == 0:
            return TargetPosition(
                instrument_id=context.instrument.instrument_id,
                symbol=context.instrument.platform_symbol,
                target_qty=0,
                notional_budget=0.0,
                reason="flat_signal",
            )
        closes = [float(bar.close) for bar in context.bars]
        latest_close = closes[-1]
        if latest_close <= 0:
            return None
        realized_vol = _volatility(closes, self.vol_window)
        vol_scalar = 1.0 / max(realized_vol, 0.005)
        base_budget = context.portfolio.equity * max(self.risk_budget_ratio, 0.0)
        capped_budget = context.portfolio.equity * max(self.max_position_ratio, 0.0)
        notional_budget = min(base_budget * signal.strength * vol_scalar, capped_budget)
        lot_notional = latest_close * context.instrument.multiplier * context.instrument.lot_size
        if lot_notional <= 0:
            return None
        raw_lots = math.floor(notional_budget / lot_notional)
        qty = max(raw_lots * context.instrument.lot_size, 0) * signal.direction
        return TargetPosition(
            instrument_id=context.instrument.instrument_id,
            symbol=context.instrument.platform_symbol,
            target_qty=qty,
            notional_budget=notional_budget,
            reason="vol_scaled_target",
            metadata={"realized_vol": realized_vol},
        )

def create_strategy(config: AppConfig, resources: LoadedResources) -> TrendStrategy:
    _ = resources
    params = dict(config.strategy.params)
    return TrendStrategy(
        name="trend.ma_cross",
        decision_frequency=str(params.get("decision_frequency", "1d")).strip(),
        history_bars=max(int(params.get("history_bars", 80) or 80), 20),
        signal_model=MovingAverageSignalModel(
            fast_window=max(int(params.get("fast_ma", 20) or 20), 2),
            slow_window=max(int(params.get("slow_ma", 60) or 60), 3),
            min_gap=max(float(params.get("min_signal_gap", 0.0025) or 0.0), 0.0),
        ),
        target_allocator=VolatilityTargetAllocator(
            risk_budget_ratio=max(float(params.get("risk_budget_ratio", 0.01) or 0.0), 0.0),
            max_position_ratio=max(float(params.get("max_position_ratio", 0.20) or 0.0), 0.0),
            vol_window=max(int(params.get("vol_window", 20) or 20), 5),
        ),
        risk_overlay=MaxContractRiskOverlay(
            max_abs_contracts=max(int(params.get("max_abs_contracts", 5) or 0), 0),
        ),
        execution_planner=TargetQuantityExecutionPlanner(),
    )
