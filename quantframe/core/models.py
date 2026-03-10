from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


PlatformMode = Literal["BACKTEST", "LIVE"]
Direction = Literal[-1, 0, 1]


@dataclass(frozen=True)
class Instrument:
    instrument_id: str
    platform_symbol: str
    multiplier: float
    tick_size: float
    lot_size: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Bar:
    instrument_id: str
    symbol: str
    frequency: str
    timestamp: object
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class PortfolioSnapshot:
    equity: float
    cash: float = 0.0


@dataclass(frozen=True)
class Position:
    instrument_id: str
    symbol: str
    qty: int = 0
    avg_price: float = 0.0

    @property
    def signed_qty(self) -> int:
        return int(self.qty)


@dataclass(frozen=True)
class SignalDecision:
    instrument_id: str
    symbol: str
    direction: Direction
    strength: float
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TargetPosition:
    instrument_id: str
    symbol: str
    target_qty: int
    notional_budget: float
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OrderRequest:
    instrument_id: str
    symbol: str
    target_qty: int
    delta_qty: int
    reason: str
    order_type: str = "market"


@dataclass(frozen=True)
class OrderResult:
    request_id: str
    accepted: bool
    reason: str
    raw: Any = None


@dataclass(frozen=True)
class StrategyDecision:
    signal: SignalDecision | None
    target: TargetPosition | None
    orders: tuple[OrderRequest, ...]
