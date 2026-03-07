from __future__ import annotations

from dataclasses import dataclass


SIGNAL_ACTIONS = {"none", "buy", "sell", "close_long", "close_short"}


@dataclass
class TradingSignal:
    action: str
    reason: str
    direction: int
    qty: int
    price: float
    stop_loss: float
    take_profit: float
    entry_atr: float
    risk_stage: str
    campaign_id: str
    created_eob: object
    exit_trigger_type: str = ""
    est_cost: float = 0.0
    gross_pnl: float = 0.0
    net_pnl: float = 0.0
    holding_bars: int = 0
    mfe_r: float = 0.0
    daily_stopout_count: int = 0
    trend_strength: float = 0.0

    def __post_init__(self) -> None:
        if self.action not in SIGNAL_ACTIONS:
            raise ValueError(f"signal action must be one of {sorted(SIGNAL_ACTIONS)}")
