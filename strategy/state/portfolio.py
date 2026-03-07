from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PortfolioRiskState:
    initial_equity: float = 0.0
    current_equity: float = 0.0
    equity_peak: float = 0.0
    daily_start_equity: float = 0.0
    current_date: str = ""
    drawdown_ratio: float = 0.0
    risk_state: str = "normal"
    effective_risk_mult: float = 1.0
    halt_flag: bool = False
    halt_reason: str = ""
    trades_count: int = 0
    wins: int = 0
    losses: int = 0
    realized_pnl: float = 0.0
    notes: str = ""
