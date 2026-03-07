from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional


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
    entry_platform_zg: float = 0.0
    entry_platform_zd: float = 0.0
    holding_bars: int = 0
    mfe_r: float = 0.0
    daily_stopout_count: int = 0
    atr_pause_flag: int = 0


@dataclass
class PositionRiskState:
    entry_price: float
    direction: int
    entry_atr: float
    initial_stop_loss: float
    stop_loss: float
    first_target_price: float
    campaign_id: str
    partial_exited: bool = False
    highest_price_since_entry: float = 0.0
    lowest_price_since_entry: float = 0.0
    bars_in_trade: int = 0
    mfe_r: float = 0.0
    stop_stage: str = "hard"
    entry_platform_zg: float = 0.0
    entry_platform_zd: float = 0.0
    initial_risk_r: float = 0.0
    is_half_closed: bool = False
    half_close_price: float = 0.0


@dataclass
class PlatformState:
    direction: int
    zg: float
    zd: float
    candidate_eob: object
    atr_at_candidate: float
    volume_ratio: float


@dataclass
class SymbolState:
    csymbol: str
    main_symbol: str = ""
    last_5m_processed_eob: Optional[object] = None
    last_h1_eob: Optional[object] = None
    h1_trend: int = 0
    h1_strength: float = 0.0
    pending_signal: Optional[TradingSignal] = None
    pending_signal_eob: Optional[object] = None
    pending_platform: Optional[PlatformState] = None
    last_risk_signal_eob: Optional[object] = None
    position_risk: Optional[PositionRiskState] = None
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
    halt_until_date: str = ""
    recovery_mode: bool = False
    trades_count: int = 0
    wins: int = 0
    losses: int = 0
    realized_pnl: float = 0.0
    notes: str = ""


@dataclass
class RuntimeSettings:
    mode: str = "BACKTEST"
    run_id: str = "run_001"
    symbols: list[str] = field(default_factory=lambda: ["DCE.p", "SHFE.ag", "DCE.jm"])
    freq_5m: str = "300s"
    freq_1h: str = "3600s"
    sub_count_5m: int = 180
    sub_count_1h: int = 120


@dataclass
class StrategySettings:
    min_tick: float = 1.0
    atr_period: int = 14
    fractal_confirm_bars: int = 2
    atr_multiplier: float = 2.0
    min_move_pct: float = 0.006
    require_next_bar_confirm: bool = True
    min_platform_width_ratio: float = 0.0018
    min_platform_width_atr: float = 0.45
    breakout_min_distance_atr: float = 0.10
    breakout_volume_ratio_min: float = 1.10
    day_volume_ratio_min: float = 1.15
    night_volume_ratio_min: float = 1.05
    h1_filter_mode: str = "soft"
    h1_neutral_size_mult: float = 0.5
    h1_strength_min: float = 0.15
    entry_cooldown_bars: int = 2
    max_entries_per_day: int = 3
    target_annual_vol: float = 0.10
    atr_pause_ratio: float = 2.0
    atr_pause_lookback: int = 50
    h1_ema_fast_period: int = 20
    h1_ema_slow_period: int = 60
    h1_rsi_period: int = 14
    h1_rsi_threshold: float = 50.0
    max_platform_width_atr: float = 4.0


@dataclass
class VolumeRatioSettings:
    day: float = 1.15
    night: float = 1.05


@dataclass
class SessionSettings:
    day: list[tuple[str, str]] = field(default_factory=lambda: [("08:30", "15:30")])
    night: list[tuple[str, str]] = field(default_factory=lambda: [("00:00", "02:30"), ("21:00", "23:59")])


@dataclass
class InstrumentSpec:
    multiplier: float = 10.0
    min_tick: float = 1.0
    min_lot: int = 1
    lot_step: int = 1
    fixed_equity_percent: float = 0.0
    max_pos_size_percent: float = 0.0
    volume_ratio_min: VolumeRatioSettings = field(default_factory=VolumeRatioSettings)
    sessions: SessionSettings = field(default_factory=SessionSettings)


@dataclass
class InstrumentSettings:
    defaults: InstrumentSpec = field(default_factory=InstrumentSpec)
    symbols: Dict[str, InstrumentSpec] = field(default_factory=dict)


@dataclass
class RiskSettings:
    risk_per_trade: float = 0.012
    hard_stop_atr: float = 2.8
    first_target_r_ratio: float = 2.0
    trail_activate_r: float = 1.0
    trail_stop_atr: float = 2.4
    enable_dynamic_stop: bool = False
    dynamic_stop_atr: float = 1.8
    dynamic_stop_activate_r: float = 0.8
    time_stop_bars: int = 12
    max_stopouts_per_day_per_symbol: int = 2
    backtest_commission_ratio: float = 0.0005
    backtest_slippage_ratio: float = 0.0010
    fixed_equity_percent: float = 0.05
    max_pos_size_percent: float = 0.20


@dataclass
class PortfolioSettings:
    max_daily_loss_ratio: float = 0.05
    dd_state_1: float = 0.08
    dd_state_2: float = 0.12
    dd_state_3: float = 0.15
    dd_risk_mult_1: float = 0.75
    dd_risk_mult_2: float = 0.50
    dd_risk_mult_3: float = 0.25


@dataclass
class GMSettings:
    token: str = ""
    strategy_id: str = ""
    serv_addr: str = ""
    backtest_start: str = "2026-01-12 00:00:00"
    backtest_end: str = "2026-02-12 15:00:00"
    backtest_max_days: int = 365


@dataclass
class ReportingSettings:
    enabled: bool = True
    output_dir: str = "reports"
    trade_filename: str = "trade_report.csv"
    daily_filename: str = "daily_report.csv"


@dataclass
class AppConfig:
    runtime: RuntimeSettings = field(default_factory=RuntimeSettings)
    strategy: StrategySettings = field(default_factory=StrategySettings)
    risk: RiskSettings = field(default_factory=RiskSettings)
    portfolio: PortfolioSettings = field(default_factory=PortfolioSettings)
    gm: GMSettings = field(default_factory=GMSettings)
    reporting: ReportingSettings = field(default_factory=ReportingSettings)
    instrument: InstrumentSettings = field(default_factory=InstrumentSettings)


@dataclass
class RuntimeContext:
    cfg: AppConfig
    states_by_csymbol: Dict[str, SymbolState] = field(default_factory=dict)
    symbol_to_csymbol: Dict[str, str] = field(default_factory=dict)
    portfolio_risk: PortfolioRiskState = field(default_factory=PortfolioRiskState)
    last_roll_date: str = ""
    last_daily_report_date: str = ""
    trade_report_path: Optional[Path] = None
    daily_report_path: Optional[Path] = None
    startup_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
