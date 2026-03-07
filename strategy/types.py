from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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


@dataclass
class RuntimeSettings:
    mode: str
    run_id: str
    symbols: list[str]
    freq_5m: str
    freq_1h: str
    warmup_5m: int
    warmup_1h: int


@dataclass
class StrategySettings:
    breakout_lookback_5m: int
    breakout_min_distance_atr: float
    breakout_width_min_atr: float
    breakout_width_max_atr: float
    volume_ratio_day_min: float
    volume_ratio_night_min: float
    trend_ema_fast_1h: int
    trend_ema_slow_1h: int
    trend_strength_min: float
    entry_cooldown_bars: int
    max_entries_per_day: int
    target_annual_vol: float
    atr_period: int


@dataclass
class RiskSettings:
    risk_per_trade_notional_ratio: float
    fixed_equity_percent: float
    max_pos_size_percent: float
    hard_stop_atr: float
    break_even_activate_r: float
    trail_activate_r: float
    trail_stop_atr: float
    dynamic_stop_enabled: bool
    dynamic_stop_atr: float
    dynamic_stop_activate_r: float
    time_stop_bars: int
    max_stopouts_per_day_per_symbol: int
    backtest_commission_ratio: float
    backtest_slippage_ratio: float


@dataclass
class PortfolioSettings:
    max_daily_loss_ratio: float
    max_drawdown_halt_ratio: float


@dataclass
class GMSettings:
    token: str
    strategy_id: str
    serv_addr: str
    backtest_start: str
    backtest_end: str
    backtest_max_days: int
    backtest_initial_cash: float
    backtest_match_mode: int
    backtest_intraday: bool
    subscribe_wait_group: bool
    wait_group_timeout: int


@dataclass
class ReportingSettings:
    enabled: bool
    output_dir: str
    trade_filename: str
    daily_filename: str
    execution_filename: str


@dataclass
class ObservabilitySettings:
    level: str
    sample_every_n: int


@dataclass
class VolumeRatioSettings:
    day: float
    night: float


@dataclass
class SessionSettings:
    day: list[tuple[str, str]] = field(default_factory=lambda: [("09:00", "11:30"), ("13:30", "15:00")])
    night: list[tuple[str, str]] = field(default_factory=lambda: [("21:00", "23:00")])


@dataclass
class InstrumentSpec:
    multiplier: float
    min_tick: float
    min_lot: int
    lot_step: int
    fixed_equity_percent: float
    max_pos_size_percent: float
    volume_ratio_min: VolumeRatioSettings
    sessions: SessionSettings


@dataclass
class InstrumentSettings:
    defaults: InstrumentSpec
    symbols: dict[str, InstrumentSpec]


@dataclass
class AppConfig:
    runtime: RuntimeSettings
    strategy: StrategySettings
    risk: RiskSettings
    portfolio: PortfolioSettings
    gm: GMSettings
    reporting: ReportingSettings
    observability: ObservabilitySettings
    instrument: InstrumentSettings


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
