from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PositionRiskState:
    entry_price: float
    direction: int
    entry_atr: float
    initial_stop_loss: float
    stop_loss: float
    campaign_id: str
    entry_eob: object
    bars_in_trade: int = 0
    highest_price_since_entry: float = 0.0
    lowest_price_since_entry: float = 0.0
    mfe_r: float = 0.0
    break_even_armed: bool = False
