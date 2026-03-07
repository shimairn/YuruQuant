from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

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


@dataclass
class ExitConfig:
    hard_stop_atr: float
    protected_activate_r: float
    ascended_activate_r: float
    armed_flush_buffer_bars: int
    armed_flush_min_gap_minutes: int


@dataclass
class StrategyConfig:
    environment: EnvironmentConfig
    entry: EntryConfig
    exit: ExitConfig


@dataclass
class PortfolioConfig:
    risk_per_trade_ratio: float
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


DEFAULTS: dict[str, Any] = {
    'runtime': {'mode': 'BACKTEST', 'run_id': 'run_001'},
    'broker': {
        'gm': {
            'token': '',
            'strategy_id': '',
            'serv_addr': '',
            'backtest': {
                'start': '2025-08-20 00:00:00',
                'end': '2026-02-13 15:00:00',
                'max_days': 180,
                'initial_cash': 500000.0,
                'match_mode': 0,
                'intraday': False,
            },
            'subscribe_wait_group': True,
            'wait_group_timeout': 10,
        }
    },
    'universe': {
        'symbols': [],
        'entry_frequency': '300s',
        'trend_frequency': '3600s',
        'warmup': {'entry_bars': 180, 'trend_bars': 120},
        'instrument_defaults': {
            'multiplier': 10.0,
            'min_tick': 1.0,
            'min_lot': 1,
            'lot_step': 1,
            'sessions': {
                'day': [['09:00', '11:30'], ['13:30', '15:00']],
                'night': [['21:00', '23:00']],
            },
        },
        'instrument_overrides': {},
    },
    'strategy': {
        'environment': {
            'ma_period': 60,
            'macd_fast': 12,
            'macd_slow': 26,
            'macd_signal': 9,
        },
        'entry': {
            'donchian_lookback': 36,
            'min_channel_width_atr': 0.5,
            'breakout_atr_buffer': 0.30,
            'session_end_buffer_bars': 0,
        },
        'exit': {
            'hard_stop_atr': 2.2,
            'protected_activate_r': 1.2,
            'ascended_activate_r': 2.5,
            'armed_flush_buffer_bars': 0,
            'armed_flush_min_gap_minutes': 180,
        },
    },
    'portfolio': {
        'risk_per_trade_ratio': 0.015,
        'max_daily_loss_ratio': 0.05,
        'max_drawdown_halt_ratio': 0.15,
    },
    'execution': {
        'fill_policy': 'next_bar_open',
        'backtest_commission_ratio': 0.001,
        'backtest_slippage_ratio': 0.002,
    },
    'reporting': {
        'enabled': True,
        'output_dir': 'reports',
        'signals_filename': 'signals.csv',
        'executions_filename': 'executions.csv',
        'portfolio_daily_filename': 'portfolio_daily.csv',
    },
    'observability': {'level': 'WARN', 'sample_every_n': 50},
}

TOP_LEVEL_KEYS = {'runtime', 'broker', 'universe', 'strategy', 'portfolio', 'execution', 'reporting', 'observability'}
RUNTIME_KEYS = {'mode', 'run_id'}
BROKER_KEYS = {'gm'}
GM_KEYS = {'token', 'strategy_id', 'serv_addr', 'backtest', 'subscribe_wait_group', 'wait_group_timeout'}
BACKTEST_KEYS = {'start', 'end', 'max_days', 'initial_cash', 'match_mode', 'intraday'}
UNIVERSE_KEYS = {'symbols', 'entry_frequency', 'trend_frequency', 'warmup', 'instrument_defaults', 'instrument_overrides'}
WARMUP_KEYS = {'entry_bars', 'trend_bars'}
INSTRUMENT_KEYS = {'multiplier', 'min_tick', 'min_lot', 'lot_step', 'sessions'}
SESSIONS_KEYS = {'day', 'night'}
STRATEGY_KEYS = {'environment', 'entry', 'exit'}
ENVIRONMENT_KEYS = {'ma_period', 'macd_fast', 'macd_slow', 'macd_signal'}
ENTRY_KEYS = {'donchian_lookback', 'min_channel_width_atr', 'breakout_atr_buffer', 'session_end_buffer_bars'}
EXIT_KEYS = {'hard_stop_atr', 'protected_activate_r', 'ascended_activate_r', 'armed_flush_buffer_bars', 'armed_flush_min_gap_minutes'}
PORTFOLIO_KEYS = {'risk_per_trade_ratio', 'max_daily_loss_ratio', 'max_drawdown_halt_ratio'}
EXECUTION_KEYS = {'fill_policy', 'backtest_commission_ratio', 'backtest_slippage_ratio'}
REPORTING_KEYS = {'enabled', 'output_dir', 'signals_filename', 'executions_filename', 'portfolio_daily_filename'}
OBSERVABILITY_KEYS = {'level', 'sample_every_n'}


def _as_dict(value: object) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f'expected dict but got {type(value).__name__}')
    return dict(value)


def _merge_defaults(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in override.items():
        if isinstance(result.get(key), dict) and isinstance(value, dict):
            result[key] = _merge_defaults(result[key], value)
        else:
            result[key] = value
    return result


def _reject_unknown(section: str, payload: dict[str, Any], allowed: set[str]) -> None:
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise ValueError(f"unknown keys under {section}: {', '.join(unknown)}")


def _parse_sessions(path: str, payload: dict[str, Any]) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    _reject_unknown(f'{path}.sessions', payload, SESSIONS_KEYS)

    def _pairs(items: object) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        for item in list(items or []):
            if not isinstance(item, (list, tuple)) or len(item) != 2:
                raise ValueError(f'{path}.sessions entries must be [start, end]')
            out.append((str(item[0]), str(item[1])))
        return out

    day = _pairs(payload.get('day', []))
    night = _pairs(payload.get('night', []))
    if not day and not night:
        raise ValueError(f'{path}.sessions must define at least one session range')
    return day, night


def _parse_instrument(path: str, payload: dict[str, Any]) -> InstrumentSpec:
    _reject_unknown(path, payload, INSTRUMENT_KEYS)
    sessions = _as_dict(payload.get('sessions', {}))
    sessions_day, sessions_night = _parse_sessions(path, sessions)
    return InstrumentSpec(
        multiplier=max(float(payload.get('multiplier', 10.0)), 0.0),
        min_tick=max(float(payload.get('min_tick', 0.0)), 1e-9),
        min_lot=max(int(payload.get('min_lot', 1)), 1),
        lot_step=max(int(payload.get('lot_step', 1)), 1),
        sessions_day=sessions_day,
        sessions_night=sessions_night,
    )


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(config_path)

    root = _as_dict(yaml.safe_load(config_path.read_text(encoding='utf-8')) or {})
    _reject_unknown('config', root, TOP_LEVEL_KEYS)

    runtime_raw = _as_dict(root.get('runtime', {}))
    broker_raw = _as_dict(root.get('broker', {}))
    universe_raw = _as_dict(root.get('universe', {}))
    strategy_raw = _as_dict(root.get('strategy', {}))
    portfolio_raw = _as_dict(root.get('portfolio', {}))
    execution_raw = _as_dict(root.get('execution', {}))
    reporting_raw = _as_dict(root.get('reporting', {}))
    observability_raw = _as_dict(root.get('observability', {}))

    _reject_unknown('runtime', runtime_raw, RUNTIME_KEYS)
    _reject_unknown('broker', broker_raw, BROKER_KEYS)
    _reject_unknown('universe', universe_raw, UNIVERSE_KEYS)
    _reject_unknown('strategy', strategy_raw, STRATEGY_KEYS)
    _reject_unknown('portfolio', portfolio_raw, PORTFOLIO_KEYS)
    _reject_unknown('execution', execution_raw, EXECUTION_KEYS)
    _reject_unknown('reporting', reporting_raw, REPORTING_KEYS)
    _reject_unknown('observability', observability_raw, OBSERVABILITY_KEYS)

    gm_raw = _as_dict(broker_raw.get('gm', {}))
    _reject_unknown('broker.gm', gm_raw, GM_KEYS)
    backtest_raw = _as_dict(gm_raw.get('backtest', {}))
    _reject_unknown('broker.gm.backtest', backtest_raw, BACKTEST_KEYS)

    warmup_raw = _as_dict(universe_raw.get('warmup', {}))
    _reject_unknown('universe.warmup', warmup_raw, WARMUP_KEYS)
    if 'instrument_defaults' in universe_raw:
        _parse_instrument('universe.instrument_defaults', _as_dict(universe_raw.get('instrument_defaults', {})))
    overrides_raw = _as_dict(universe_raw.get('instrument_overrides', {}))
    for csymbol, spec in overrides_raw.items():
        _parse_instrument(f'universe.instrument_overrides.{csymbol}', _as_dict(spec))

    environment_raw = _as_dict(strategy_raw.get('environment', {}))
    entry_raw = _as_dict(strategy_raw.get('entry', {}))
    exit_raw = _as_dict(strategy_raw.get('exit', {}))
    _reject_unknown('strategy.environment', environment_raw, ENVIRONMENT_KEYS)
    _reject_unknown('strategy.entry', entry_raw, ENTRY_KEYS)
    _reject_unknown('strategy.exit', exit_raw, EXIT_KEYS)

    merged = _merge_defaults(DEFAULTS, root)
    runtime = _as_dict(merged['runtime'])
    broker = _as_dict(merged['broker'])
    gm = _as_dict(broker['gm'])
    backtest = _as_dict(gm['backtest'])
    universe = _as_dict(merged['universe'])
    warmup = _as_dict(universe['warmup'])
    defaults_spec = _parse_instrument('universe.instrument_defaults', _as_dict(universe['instrument_defaults']))
    overrides = {
        str(csymbol): _parse_instrument(f'universe.instrument_overrides.{csymbol}', _as_dict(spec))
        for csymbol, spec in _as_dict(universe['instrument_overrides']).items()
    }
    strategy = _as_dict(merged['strategy'])
    environment = _as_dict(strategy['environment'])
    entry = _as_dict(strategy['entry'])
    exit_cfg = _as_dict(strategy['exit'])
    portfolio = _as_dict(merged['portfolio'])
    execution = _as_dict(merged['execution'])
    reporting = _as_dict(merged['reporting'])
    observability = _as_dict(merged['observability'])

    mode = str(runtime['mode']).strip().upper()
    if mode not in {'BACKTEST', 'LIVE'}:
        raise ValueError('runtime.mode must be BACKTEST or LIVE')

    fill_policy = str(execution['fill_policy']).strip()
    if fill_policy != 'next_bar_open':
        raise ValueError('execution.fill_policy must be next_bar_open')

    symbols = [str(item).strip() for item in list(universe['symbols'] or []) if str(item).strip()]
    if not symbols:
        raise ValueError('universe.symbols must be a non-empty list')
    if int(environment['ma_period']) < 2:
        raise ValueError('strategy.environment.ma_period must be >= 2')
    if int(environment['macd_fast']) >= int(environment['macd_slow']):
        raise ValueError('strategy.environment.macd_fast must be smaller than macd_slow')
    if int(entry['donchian_lookback']) < 2:
        raise ValueError('strategy.entry.donchian_lookback must be >= 2')
    if float(entry['min_channel_width_atr']) <= 0:
        raise ValueError('strategy.entry.min_channel_width_atr must be > 0')
    if float(entry['breakout_atr_buffer']) < 0:
        raise ValueError('strategy.entry.breakout_atr_buffer must be >= 0')
    if int(entry['session_end_buffer_bars']) < 0:
        raise ValueError('strategy.entry.session_end_buffer_bars must be >= 0')

    if float(exit_cfg['hard_stop_atr']) <= 0:
        raise ValueError('strategy.exit.hard_stop_atr must be > 0')
    if float(exit_cfg['protected_activate_r']) <= 0:
        raise ValueError('strategy.exit.protected_activate_r must be > 0')
    if float(exit_cfg['ascended_activate_r']) <= 0:
        raise ValueError('strategy.exit.ascended_activate_r must be > 0')
    if float(exit_cfg['ascended_activate_r']) < float(exit_cfg['protected_activate_r']):
        raise ValueError('strategy.exit.ascended_activate_r must be >= protected_activate_r')
    if int(exit_cfg['armed_flush_buffer_bars']) < 0:
        raise ValueError('strategy.exit.armed_flush_buffer_bars must be >= 0')
    if int(exit_cfg['armed_flush_min_gap_minutes']) < 0:
        raise ValueError('strategy.exit.armed_flush_min_gap_minutes must be >= 0')

    if float(portfolio['risk_per_trade_ratio']) <= 0:
        raise ValueError('portfolio.risk_per_trade_ratio must be > 0')

    return AppConfig(
        runtime=RuntimeConfig(mode=mode, run_id=str(runtime['run_id']).strip() or 'run_001'),
        broker=BrokerConfig(
            gm=GMConfig(
                token=str(gm['token'] or '').strip(),
                strategy_id=str(gm['strategy_id'] or '').strip(),
                serv_addr=str(gm['serv_addr'] or '').strip(),
                backtest=BacktestConfig(
                    start=str(backtest['start']),
                    end=str(backtest['end']),
                    max_days=max(int(backtest['max_days']), 1),
                    initial_cash=max(float(backtest['initial_cash']), 0.0),
                    match_mode=int(backtest['match_mode']),
                    intraday=bool(backtest['intraday']),
                ),
                subscribe_wait_group=bool(gm['subscribe_wait_group']),
                wait_group_timeout=max(int(gm['wait_group_timeout']), 0),
            )
        ),
        universe=UniverseConfig(
            symbols=symbols,
            entry_frequency=str(universe['entry_frequency']),
            trend_frequency=str(universe['trend_frequency']),
            warmup=WarmupConfig(
                entry_bars=max(int(warmup['entry_bars']), 1),
                trend_bars=max(int(warmup['trend_bars']), 1),
            ),
            instrument_defaults=defaults_spec,
            instrument_overrides=overrides,
        ),
        strategy=StrategyConfig(
            environment=EnvironmentConfig(
                ma_period=max(int(environment['ma_period']), 2),
                macd_fast=max(int(environment['macd_fast']), 2),
                macd_slow=max(int(environment['macd_slow']), 3),
                macd_signal=max(int(environment['macd_signal']), 2),
            ),
            entry=EntryConfig(
                donchian_lookback=max(int(entry['donchian_lookback']), 2),
                min_channel_width_atr=max(float(entry['min_channel_width_atr']), 0.0),
                breakout_atr_buffer=max(float(entry['breakout_atr_buffer']), 0.0),
                session_end_buffer_bars=max(int(entry['session_end_buffer_bars']), 0),
            ),
            exit=ExitConfig(
                hard_stop_atr=max(float(exit_cfg['hard_stop_atr']), 0.01),
                protected_activate_r=max(float(exit_cfg['protected_activate_r']), 0.0),
                ascended_activate_r=max(float(exit_cfg['ascended_activate_r']), 0.0),
                armed_flush_buffer_bars=max(int(exit_cfg['armed_flush_buffer_bars']), 0),
                armed_flush_min_gap_minutes=max(int(exit_cfg['armed_flush_min_gap_minutes']), 0),
            ),
        ),
        portfolio=PortfolioConfig(
            risk_per_trade_ratio=max(float(portfolio['risk_per_trade_ratio']), 0.0),
            max_daily_loss_ratio=max(float(portfolio['max_daily_loss_ratio']), 0.0),
            max_drawdown_halt_ratio=max(float(portfolio['max_drawdown_halt_ratio']), 0.0),
        ),
        execution=ExecutionConfig(
            fill_policy=fill_policy,
            backtest_commission_ratio=max(float(execution['backtest_commission_ratio']), 0.0),
            backtest_slippage_ratio=max(float(execution['backtest_slippage_ratio']), 0.0),
        ),
        reporting=ReportingConfig(
            enabled=bool(reporting['enabled']),
            output_dir=str(reporting['output_dir']),
            signals_filename=str(reporting['signals_filename']),
            executions_filename=str(reporting['executions_filename']),
            portfolio_daily_filename=str(reporting['portfolio_daily_filename']),
        ),
        observability=ObservabilityConfig(
            level=str(observability['level']).strip().upper(),
            sample_every_n=max(int(observability['sample_every_n']), 1),
        ),
    )




