from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol, runtime_checkable


Action = Literal['buy', 'sell', 'close_long', 'close_short']
Phase = Literal['armed', 'protected', 'ascended']


@dataclass(frozen=True)
class InstrumentSpec:
    multiplier: float
    min_tick: float
    min_lot: int
    lot_step: int
    sessions_day: list[tuple[str, str]]
    sessions_night: list[tuple[str, str]]


@dataclass(frozen=True)
class NormalizedBar:
    csymbol: str
    symbol: str
    frequency: str
    eob: object
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class MarketEvent:
    trade_time: object
    bars: list[NormalizedBar]


@dataclass(frozen=True)
class EnvironmentSnapshot:
    direction: int = 0
    trend_ok: bool = False
    close: float = 0.0
    moving_average: float = 0.0
    macd_histogram: float = 0.0


@dataclass(frozen=True)
class EntrySignal:
    action: Action
    reason: str
    direction: int
    qty: int
    price: float
    stop_loss: float
    protected_stop_price: float
    created_at: object
    entry_atr: float
    breakout_anchor: float
    campaign_id: str
    environment_ma: float
    macd_histogram: float


@dataclass(frozen=True)
class ExitSignal:
    action: Action
    reason: str
    direction: int
    qty: int
    price: float
    created_at: object
    exit_trigger: str
    campaign_id: str
    holding_bars: int
    mfe_r: float
    gross_pnl: float
    net_pnl: float
    phase: Phase


Signal = EntrySignal | ExitSignal


@dataclass(frozen=True)
class OrderIntent:
    symbol: str
    side: Literal['long', 'short']
    target_qty: int
    purpose: str


@dataclass(frozen=True)
class ExecutionResult:
    request_id: str
    intended_action: str
    intended_qty: int
    accepted: bool
    reason: str
    timestamp: str


@dataclass(frozen=True)
class ExecutionDiagnostics:
    execution_regime: str = 'normal'
    fill_gap_points: float = 0.0
    fill_gap_atr: float = 0.0


@dataclass(frozen=True)
class PositionSnapshot:
    symbol: str
    long_qty: int = 0
    short_qty: int = 0


@dataclass(frozen=True)
class PortfolioSnapshot:
    equity: float
    cash: float = 0.0


@dataclass
class ManagedPosition:
    entry_price: float
    direction: int
    qty: int
    entry_atr: float
    initial_stop_loss: float
    stop_loss: float
    protected_stop_price: float
    phase: Phase
    campaign_id: str
    entry_eob: object
    breakout_anchor: float
    highest_price_since_entry: float
    lowest_price_since_entry: float
    bars_in_trade: int = 0
    mfe_r: float = 0.0


@dataclass
class SymbolRuntime:
    csymbol: str
    main_symbol: str = ''
    environment: EnvironmentSnapshot = field(default_factory=EnvironmentSnapshot)
    pending_signal: Signal | None = None
    pending_signal_eob: object | None = None
    last_processed_eob: object | None = None
    bar_index_5m: int = 0
    position: ManagedPosition | None = None


@dataclass
class PortfolioRuntime:
    initial_equity: float = 0.0
    current_equity: float = 0.0
    equity_peak: float = 0.0
    daily_start_equity: float = 0.0
    current_date: str = ''
    drawdown_ratio: float = 0.0
    risk_state: str = 'normal'
    effective_risk_mult: float = 1.0
    halt_flag: bool = False
    halt_reason: str = ''
    trades_count: int = 0
    wins: int = 0
    losses: int = 0
    realized_pnl: float = 0.0


@dataclass
class ReportPaths:
    signals_path: Path | None = None
    executions_path: Path | None = None
    portfolio_daily_path: Path | None = None


@dataclass
class RuntimeState:
    states_by_csymbol: dict[str, SymbolRuntime] = field(default_factory=dict)
    symbol_to_csymbol: dict[str, str] = field(default_factory=dict)
    bar_store: dict[str, object] = field(default_factory=dict)
    portfolio: PortfolioRuntime = field(default_factory=PortfolioRuntime)
    reports: ReportPaths = field(default_factory=ReportPaths)
    context: object | None = None


@dataclass(frozen=True)
class GuardDecision:
    allow_entries: bool
    force_flatten: bool
    reason: str = ''


@runtime_checkable
class FillPolicy(Protocol):
    def queue(self, state: SymbolRuntime, signal: Signal, current_eob: object) -> None: ...

    def pop_due(self, state: SymbolRuntime, current_eob: object) -> Signal | None: ...


@runtime_checkable
class BrokerGateway(Protocol):
    def bind_context(self, context: object) -> None: ...

    def refresh_main_contracts(self, trade_time: object) -> None: ...

    def resolve_csymbol(self, symbol: str) -> str | None: ...

    def fetch_history(self, symbol: str, frequency: str, count: int): ...

    def get_position_snapshot(self, symbol: str) -> PositionSnapshot: ...

    def get_portfolio_snapshot(self) -> PortfolioSnapshot: ...

    def plan_order_intents(self, symbol: str, signal: Signal) -> list[OrderIntent]: ...

    def submit_order_intents(self, intents: list[OrderIntent]) -> list[ExecutionResult]: ...


@runtime_checkable
class ReportSink(Protocol):
    def ensure_ready(self, runtime: RuntimeState, mode: str, run_id: str) -> None: ...

    def record_signal(self, runtime: RuntimeState, mode: str, run_id: str, csymbol: str, symbol: str, signal: Signal) -> None: ...

    def record_executions(
        self,
        runtime: RuntimeState,
        mode: str,
        run_id: str,
        csymbol: str,
        symbol: str,
        signal: Signal,
        fill_ts: object,
        fill_price: float,
        diagnostics: ExecutionDiagnostics,
        results: list[ExecutionResult],
    ) -> None: ...

    def record_portfolio_day(self, runtime: RuntimeState, mode: str, run_id: str, trade_day: str, snapshot_ts: object) -> None: ...




