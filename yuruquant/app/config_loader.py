from __future__ import annotations

from pathlib import Path

import yaml

from yuruquant.app.config_defaults import (
    BACKTEST_KEYS,
    BROKER_KEYS,
    DEFAULTS,
    ENTRY_KEYS,
    ENVIRONMENT_KEYS,
    EXECUTION_KEYS,
    EXIT_KEYS,
    GM_KEYS,
    OBSERVABILITY_KEYS,
    PORTFOLIO_KEYS,
    REPORTING_KEYS,
    RUNTIME_KEYS,
    STRATEGY_KEYS,
    TOP_LEVEL_KEYS,
    UNIVERSE_KEYS,
    WARMUP_KEYS,
)
from yuruquant.app.config_schema import (
    AppConfig,
    BacktestConfig,
    BrokerConfig,
    EntryConfig,
    EnvironmentConfig,
    ExecutionConfig,
    ExitConfig,
    GMConfig,
    ObservabilityConfig,
    PortfolioConfig,
    ReportingConfig,
    RuntimeConfig,
    StrategyConfig,
    UniverseConfig,
    WarmupConfig,
)
from yuruquant.app.config_validation import as_dict, merge_defaults, parse_instrument, reject_unknown


def load_config(path: str | Path) -> AppConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding='utf-8')) or {}
    root = as_dict(raw)
    reject_unknown('config', root, TOP_LEVEL_KEYS)

    runtime_raw = as_dict(root.get('runtime'))
    broker_raw = as_dict(root.get('broker'))
    universe_raw = as_dict(root.get('universe'))
    strategy_raw = as_dict(root.get('strategy'))
    portfolio_raw = as_dict(root.get('portfolio'))
    execution_raw = as_dict(root.get('execution'))
    reporting_raw = as_dict(root.get('reporting'))
    observability_raw = as_dict(root.get('observability'))

    reject_unknown('runtime', runtime_raw, RUNTIME_KEYS)
    reject_unknown('broker', broker_raw, BROKER_KEYS)
    reject_unknown('universe', universe_raw, UNIVERSE_KEYS)
    reject_unknown('strategy', strategy_raw, STRATEGY_KEYS)
    reject_unknown('portfolio', portfolio_raw, PORTFOLIO_KEYS)
    reject_unknown('execution', execution_raw, EXECUTION_KEYS)
    reject_unknown('reporting', reporting_raw, REPORTING_KEYS)
    reject_unknown('observability', observability_raw, OBSERVABILITY_KEYS)

    gm_raw = as_dict(broker_raw.get('gm'))
    reject_unknown('broker.gm', gm_raw, GM_KEYS)
    backtest_raw = as_dict(gm_raw.get('backtest'))
    reject_unknown('broker.gm.backtest', backtest_raw, BACKTEST_KEYS)

    warmup_raw = as_dict(universe_raw.get('warmup'))
    reject_unknown('universe.warmup', warmup_raw, WARMUP_KEYS)
    instrument_defaults_raw = universe_raw.get('instrument_defaults')
    if instrument_defaults_raw is not None:
        parse_instrument('universe.instrument_defaults', as_dict(instrument_defaults_raw))
    overrides_raw = as_dict(universe_raw.get('instrument_overrides', {}))
    for csymbol, spec in overrides_raw.items():
        parse_instrument(f'universe.instrument_overrides.{csymbol}', as_dict(spec))

    environment_raw = as_dict(strategy_raw.get('environment', {}))
    entry_raw = as_dict(strategy_raw.get('entry', {}))
    exit_raw = as_dict(strategy_raw.get('exit', {}))
    reject_unknown('strategy.environment', environment_raw, ENVIRONMENT_KEYS)
    reject_unknown('strategy.entry', entry_raw, ENTRY_KEYS)
    reject_unknown('strategy.exit', exit_raw, EXIT_KEYS)

    merged = merge_defaults(DEFAULTS, root)
    runtime = as_dict(merged['runtime'])
    broker = as_dict(merged['broker'])
    gm = as_dict(broker['gm'])
    backtest = as_dict(gm['backtest'])
    universe = as_dict(merged['universe'])
    warmup = as_dict(universe['warmup'])
    defaults_spec = parse_instrument('universe.instrument_defaults', as_dict(universe['instrument_defaults']))
    overrides = {
        str(csymbol): parse_instrument(f'universe.instrument_overrides.{csymbol}', as_dict(spec))
        for csymbol, spec in as_dict(universe['instrument_overrides']).items()
    }
    strategy = as_dict(merged['strategy'])
    environment = as_dict(strategy['environment'])
    entry = as_dict(strategy['entry'])
    exit_cfg = as_dict(strategy['exit'])
    portfolio = as_dict(merged['portfolio'])
    execution = as_dict(merged['execution'])
    reporting = as_dict(merged['reporting'])
    observability = as_dict(merged['observability'])

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
    if int(entry['entry_block_major_gap_bars']) < 0:
        raise ValueError('strategy.entry.entry_block_major_gap_bars must be >= 0')

    if float(exit_cfg['hard_stop_atr']) <= 0:
        raise ValueError('strategy.exit.hard_stop_atr must be > 0')
    if float(exit_cfg['protected_activate_r']) <= 0:
        raise ValueError('strategy.exit.protected_activate_r must be > 0')
    if int(exit_cfg['armed_flush_buffer_bars']) < 0:
        raise ValueError('strategy.exit.armed_flush_buffer_bars must be >= 0')
    if int(exit_cfg['armed_flush_min_gap_minutes']) < 0:
        raise ValueError('strategy.exit.armed_flush_min_gap_minutes must be >= 0')
    if int(exit_cfg['session_flat_all_phases_buffer_bars']) < 0:
        raise ValueError('strategy.exit.session_flat_all_phases_buffer_bars must be >= 0')
    session_flat_scope = str(exit_cfg['session_flat_scope']).strip()
    if session_flat_scope not in {'disabled', 'all_session_ends', 'trading_day_end_only'}:
        raise ValueError('strategy.exit.session_flat_scope must be disabled, all_session_ends, or trading_day_end_only')

    if float(portfolio['risk_per_trade_ratio']) <= 0:
        raise ValueError('portfolio.risk_per_trade_ratio must be > 0')
    if float(portfolio['max_total_armed_risk_ratio']) < 0:
        raise ValueError('portfolio.max_total_armed_risk_ratio must be >= 0')

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
                entry_block_major_gap_bars=max(int(entry['entry_block_major_gap_bars']), 0),
            ),
            exit=ExitConfig(
                hard_stop_atr=max(float(exit_cfg['hard_stop_atr']), 0.01),
                protected_activate_r=max(float(exit_cfg['protected_activate_r']), 0.0),
                armed_flush_buffer_bars=max(int(exit_cfg['armed_flush_buffer_bars']), 0),
                armed_flush_min_gap_minutes=max(int(exit_cfg['armed_flush_min_gap_minutes']), 0),
                session_flat_all_phases_buffer_bars=max(int(exit_cfg['session_flat_all_phases_buffer_bars']), 0),
                session_flat_scope=session_flat_scope,
            ),
        ),
        portfolio=PortfolioConfig(
            risk_per_trade_ratio=max(float(portfolio['risk_per_trade_ratio']), 0.0),
            max_total_armed_risk_ratio=max(float(portfolio['max_total_armed_risk_ratio']), 0.0),
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


__all__ = ['load_config']
