from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from strategy.contracts.settings import AppConfig
from strategy.state.portfolio import PortfolioRiskState
from strategy.state.symbol import SymbolState


@dataclass
class RuntimeContext:
    cfg: AppConfig
    states_by_csymbol: dict[str, SymbolState] = field(default_factory=dict)
    symbol_to_csymbol: dict[str, str] = field(default_factory=dict)
    portfolio_risk: PortfolioRiskState = field(default_factory=PortfolioRiskState)
    bar_store: dict[str, dict[str, Any]] = field(default_factory=dict)
    last_roll_date: str = ""
    last_daily_report_date: str = ""
    trade_report_path: Path | None = None
    daily_report_path: Path | None = None
    execution_report_path: Path | None = None
    startup_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    context: object | None = None
