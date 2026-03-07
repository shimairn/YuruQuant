from __future__ import annotations

from dataclasses import dataclass, field


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
