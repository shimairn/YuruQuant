from __future__ import annotations

from dataclasses import dataclass

from yuruquant.app.config_schema import RiskThrottleStep


@dataclass(frozen=True)
class DrawdownScheduleDecision:
    risk_mult: float
    triggered_ratio: float


def resolve_drawdown_risk_schedule(
    drawdown_ratio: float,
    schedule: tuple[RiskThrottleStep, ...],
) -> DrawdownScheduleDecision:
    risk_mult = 1.0
    triggered_ratio = 0.0
    for step in schedule:
        if float(drawdown_ratio) < float(step.drawdown_ratio):
            break
        risk_mult = min(risk_mult, float(step.risk_mult))
        triggered_ratio = float(step.drawdown_ratio)
    return DrawdownScheduleDecision(risk_mult=max(risk_mult, 0.0), triggered_ratio=triggered_ratio)


__all__ = ['DrawdownScheduleDecision', 'resolve_drawdown_risk_schedule']
