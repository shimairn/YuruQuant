from __future__ import annotations

from dataclasses import dataclass

from quantframe.core.models import OrderRequest, TargetPosition
from quantframe.trend import DecisionContext, ExecutionPlanner, RiskOverlay


def average_true_range(highs: list[float], lows: list[float], closes: list[float], window: int) -> float:
    if window <= 0 or len(highs) != len(lows) or len(loses := closes) < 2:
        return 0.0
    true_ranges: list[float] = []
    previous_close = float(loses[0])
    for index in range(1, len(loses)):
        high = float(highs[index])
        low = float(lows[index])
        close = float(loses[index])
        true_ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
        previous_close = close
    if len(true_ranges) < window:
        return 0.0
    recent = true_ranges[-window:]
    return sum(recent) / len(recent)


@dataclass(frozen=True)
class MaxContractRiskOverlay(RiskOverlay):
    max_abs_contracts: int

    def apply(self, context: DecisionContext, target: TargetPosition | None) -> TargetPosition | None:
        _ = context
        if target is None or self.max_abs_contracts <= 0:
            return target
        clipped = max(-self.max_abs_contracts, min(self.max_abs_contracts, target.target_qty))
        if clipped == target.target_qty:
            return target
        return TargetPosition(
            instrument_id=target.instrument_id,
            symbol=target.symbol,
            target_qty=clipped,
            notional_budget=target.notional_budget,
            reason="clipped_max_contracts",
            metadata=target.metadata,
        )


class TargetQuantityExecutionPlanner(ExecutionPlanner):
    def plan(self, context: DecisionContext, target: TargetPosition | None) -> tuple[OrderRequest, ...]:
        if target is None:
            return ()
        current_qty = context.position.signed_qty
        if current_qty == target.target_qty:
            return ()
        return (
            OrderRequest(
                instrument_id=target.instrument_id,
                symbol=target.symbol,
                target_qty=target.target_qty,
                delta_qty=target.target_qty - current_qty,
                reason=target.reason,
            ),
        )
