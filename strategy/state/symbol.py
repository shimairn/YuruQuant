from __future__ import annotations

from dataclasses import dataclass

from strategy.contracts.signal import TradingSignal
from strategy.state.position import PositionRiskState


@dataclass
class SymbolState:
    csymbol: str
    main_symbol: str = ""
    last_5m_processed_eob: object | None = None
    last_h1_eob: object | None = None
    h1_trend: int = 0
    h1_strength: float = 0.0
    pending_signal: TradingSignal | None = None
    pending_signal_eob: object | None = None
    position_risk: PositionRiskState | None = None
    last_risk_signal_eob: object | None = None
    bar_index_5m: int = 0
    daily_entry_date: str = ""
    daily_entry_count: int = 0
    daily_stopout_date: str = ""
    daily_stopout_count: int = 0
    last_entry_bar_index: int = -10_000
    last_entry_direction: int = 0
