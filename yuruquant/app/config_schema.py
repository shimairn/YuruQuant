from __future__ import annotations

from dataclasses import dataclass

from yuruquant.core.models import InstrumentSpec


@dataclass
class RuntimeConfig:
    mode: str
    run_id: str


@dataclass
class BacktestConfig:
    start: str
    end: str
    max_days: int
    initial_cash: float
    match_mode: int
    intraday: bool


@dataclass
class GMConfig:
    token: str
    strategy_id: str
    serv_addr: str
    backtest: BacktestConfig
    subscribe_wait_group: bool
    wait_group_timeout: int


@dataclass
class BrokerConfig:
    gm: GMConfig


@dataclass
class WarmupConfig:
    entry_bars: int
    trend_bars: int


@dataclass
class UniverseConfig:
    symbols: list[str]
    entry_frequency: str
    trend_frequency: str
    warmup: WarmupConfig
    instrument_defaults: InstrumentSpec
    instrument_overrides: dict[str, InstrumentSpec]


@dataclass
class EnvironmentConfig:
    ma_period: int
    macd_fast: int
    macd_slow: int
    macd_signal: int


@dataclass
class EntryConfig:
    donchian_lookback: int
    min_channel_width_atr: float
    breakout_atr_buffer: float
    session_end_buffer_bars: int
    entry_block_major_gap_bars: int


@dataclass
class ExitConfig:
    hard_stop_atr: float
    protected_activate_r: float
    armed_flush_buffer_bars: int
    armed_flush_min_gap_minutes: int
    session_flat_all_phases_buffer_bars: int
    session_flat_scope: str


@dataclass
class StrategyConfig:
    environment: EnvironmentConfig
    entry: EntryConfig
    exit: ExitConfig


@dataclass
class PortfolioConfig:
    risk_per_trade_ratio: float
    max_total_armed_risk_ratio: float
    max_daily_loss_ratio: float
    max_drawdown_halt_ratio: float


@dataclass
class ExecutionConfig:
    fill_policy: str
    backtest_commission_ratio: float
    backtest_slippage_ratio: float


@dataclass
class ReportingConfig:
    enabled: bool
    output_dir: str
    signals_filename: str
    executions_filename: str
    portfolio_daily_filename: str


@dataclass
class ObservabilityConfig:
    level: str
    sample_every_n: int


@dataclass
class AppConfig:
    runtime: RuntimeConfig
    broker: BrokerConfig
    universe: UniverseConfig
    strategy: StrategyConfig
    portfolio: PortfolioConfig
    execution: ExecutionConfig
    reporting: ReportingConfig
    observability: ObservabilityConfig


__all__ = [
    'AppConfig',
    'BacktestConfig',
    'BrokerConfig',
    'EntryConfig',
    'EnvironmentConfig',
    'ExecutionConfig',
    'ExitConfig',
    'GMConfig',
    'ObservabilityConfig',
    'PortfolioConfig',
    'ReportingConfig',
    'RuntimeConfig',
    'StrategyConfig',
    'UniverseConfig',
    'WarmupConfig',
]
