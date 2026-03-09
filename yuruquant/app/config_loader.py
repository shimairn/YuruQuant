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
from yuruquant.app.config_validation import (
    as_dict,
    merge_defaults,
    parse_drawdown_risk_schedule,
    parse_instrument,
    parse_positive_int_sequence,
    parse_risk_clusters,
    reject_unknown,
)


def _load_root(path: str | Path) -> dict[str, object]:
    raw = yaml.safe_load(Path(path).read_text(encoding='utf-8')) or {}
    root = as_dict(raw)
    reject_unknown('config', root, TOP_LEVEL_KEYS)
    return root


def _extract_raw_sections(root: dict[str, object]) -> dict[str, dict[str, object]]:
    sections = {
        'runtime': as_dict(root.get('runtime')),
        'broker': as_dict(root.get('broker')),
        'universe': as_dict(root.get('universe')),
        'strategy': as_dict(root.get('strategy')),
        'portfolio': as_dict(root.get('portfolio')),
        'execution': as_dict(root.get('execution')),
        'reporting': as_dict(root.get('reporting')),
        'observability': as_dict(root.get('observability')),
    }
    sections['broker.gm'] = as_dict(sections['broker'].get('gm'))
    sections['broker.gm.backtest'] = as_dict(sections['broker.gm'].get('backtest'))
    sections['universe.warmup'] = as_dict(sections['universe'].get('warmup'))
    sections['strategy.environment'] = as_dict(sections['strategy'].get('environment', {}))
    sections['strategy.entry'] = as_dict(sections['strategy'].get('entry', {}))
    sections['strategy.exit'] = as_dict(sections['strategy'].get('exit', {}))
    return sections


def _validate_unknown_keys(sections: dict[str, dict[str, object]]) -> None:
    reject_unknown('runtime', sections['runtime'], RUNTIME_KEYS)
    reject_unknown('broker', sections['broker'], BROKER_KEYS)
    reject_unknown('universe', sections['universe'], UNIVERSE_KEYS)
    reject_unknown('strategy', sections['strategy'], STRATEGY_KEYS)
    reject_unknown('portfolio', sections['portfolio'], PORTFOLIO_KEYS)
    reject_unknown('execution', sections['execution'], EXECUTION_KEYS)
    reject_unknown('reporting', sections['reporting'], REPORTING_KEYS)
    reject_unknown('observability', sections['observability'], OBSERVABILITY_KEYS)
    reject_unknown('broker.gm', sections['broker.gm'], GM_KEYS)
    reject_unknown('broker.gm.backtest', sections['broker.gm.backtest'], BACKTEST_KEYS)
    reject_unknown('universe.warmup', sections['universe.warmup'], WARMUP_KEYS)
    reject_unknown('strategy.environment', sections['strategy.environment'], ENVIRONMENT_KEYS)
    reject_unknown('strategy.entry', sections['strategy.entry'], ENTRY_KEYS)
    reject_unknown('strategy.exit', sections['strategy.exit'], EXIT_KEYS)


def _validate_instrument_specs(sections: dict[str, dict[str, object]]) -> None:
    universe_raw = sections['universe']
    instrument_defaults_raw = universe_raw.get('instrument_defaults')
    if instrument_defaults_raw is not None:
        parse_instrument('universe.instrument_defaults', as_dict(instrument_defaults_raw))
    overrides_raw = as_dict(universe_raw.get('instrument_overrides', {}))
    for csymbol, spec in overrides_raw.items():
        parse_instrument(f'universe.instrument_overrides.{csymbol}', as_dict(spec))
    symbols = {str(item).strip() for item in list(universe_raw.get('symbols') or []) if str(item).strip()}
    parse_risk_clusters('universe.risk_clusters', universe_raw.get('risk_clusters', {}), symbols)


def _merge_sections(root: dict[str, object]) -> dict[str, dict[str, object]]:
    merged = merge_defaults(DEFAULTS, root)
    sections = {
        'runtime': as_dict(merged['runtime']),
        'broker': as_dict(merged['broker']),
        'universe': as_dict(merged['universe']),
        'strategy': as_dict(merged['strategy']),
        'portfolio': as_dict(merged['portfolio']),
        'execution': as_dict(merged['execution']),
        'reporting': as_dict(merged['reporting']),
        'observability': as_dict(merged['observability']),
    }
    sections['broker.gm'] = as_dict(sections['broker']['gm'])
    sections['broker.gm.backtest'] = as_dict(sections['broker.gm']['backtest'])
    sections['universe.warmup'] = as_dict(sections['universe']['warmup'])
    sections['strategy.environment'] = as_dict(sections['strategy']['environment'])
    sections['strategy.entry'] = as_dict(sections['strategy']['entry'])
    sections['strategy.exit'] = as_dict(sections['strategy']['exit'])
    return sections


def _validate_runtime_values(sections: dict[str, dict[str, object]]) -> tuple[str, str, list[str], str]:
    runtime = sections['runtime']
    universe = sections['universe']
    environment = sections['strategy.environment']
    entry = sections['strategy.entry']
    exit_cfg = sections['strategy.exit']
    portfolio = sections['portfolio']
    execution = sections['execution']

    mode = str(runtime['mode']).strip().upper()
    if mode not in {'BACKTEST', 'LIVE'}:
        raise ValueError('runtime.mode must be BACKTEST or LIVE')

    fill_policy = str(execution['fill_policy']).strip()
    if fill_policy != 'next_bar_open':
        raise ValueError('execution.fill_policy must be next_bar_open')

    symbols = [str(item).strip() for item in list(universe['symbols'] or []) if str(item).strip()]
    if not symbols:
        raise ValueError('universe.symbols must be a non-empty list')
    environment_mode = str(environment['mode']).strip()
    if environment_mode not in {'ma_macd', 'tsmom'}:
        raise ValueError('strategy.environment.mode must be ma_macd or tsmom')
    if int(environment['ma_period']) < 2:
        raise ValueError('strategy.environment.ma_period must be >= 2')
    if int(environment['macd_fast']) >= int(environment['macd_slow']):
        raise ValueError('strategy.environment.macd_fast must be smaller than macd_slow')
    tsmom_lookbacks = parse_positive_int_sequence('strategy.environment.tsmom_lookbacks', environment.get('tsmom_lookbacks', []))
    if not tsmom_lookbacks:
        raise ValueError('strategy.environment.tsmom_lookbacks must include at least one lookback')
    tsmom_min_agree = int(environment['tsmom_min_agree'])
    if tsmom_min_agree <= 0:
        raise ValueError('strategy.environment.tsmom_min_agree must be >= 1')
    if tsmom_min_agree > len(tsmom_lookbacks):
        raise ValueError('strategy.environment.tsmom_min_agree cannot exceed tsmom_lookbacks count')
    if environment_mode == 'tsmom' and int(sections['universe.warmup']['trend_bars']) < (max(tsmom_lookbacks) + 1):
        raise ValueError('universe.warmup.trend_bars must exceed the largest tsmom lookback')
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
    if float(portfolio['max_cluster_armed_risk_ratio']) < 0:
        raise ValueError('portfolio.max_cluster_armed_risk_ratio must be >= 0')
    if int(portfolio['max_same_direction_cluster_positions']) < 0:
        raise ValueError('portfolio.max_same_direction_cluster_positions must be >= 0')
    drawdown_halt_mode = str(portfolio['drawdown_halt_mode']).strip()
    if drawdown_halt_mode not in {'hard', 'disabled'}:
        raise ValueError('portfolio.drawdown_halt_mode must be hard or disabled')
    parse_drawdown_risk_schedule('portfolio.drawdown_risk_schedule', portfolio.get('drawdown_risk_schedule', []))

    return mode, fill_policy, symbols, session_flat_scope


def _build_runtime_config(runtime: dict[str, object], mode: str) -> RuntimeConfig:
    return RuntimeConfig(mode=mode, run_id=str(runtime['run_id']).strip() or 'run_001')


def _build_broker_config(sections: dict[str, dict[str, object]]) -> BrokerConfig:
    gm = sections['broker.gm']
    backtest = sections['broker.gm.backtest']
    return BrokerConfig(
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
    )


def _build_universe_config(sections: dict[str, dict[str, object]], symbols: list[str]) -> UniverseConfig:
    universe = sections['universe']
    warmup = sections['universe.warmup']
    defaults_spec = parse_instrument('universe.instrument_defaults', as_dict(universe['instrument_defaults']))
    overrides = {
        str(csymbol): parse_instrument(f'universe.instrument_overrides.{csymbol}', as_dict(spec))
        for csymbol, spec in as_dict(universe['instrument_overrides']).items()
    }
    risk_clusters = parse_risk_clusters('universe.risk_clusters', universe.get('risk_clusters', {}), set(symbols))
    return UniverseConfig(
        symbols=symbols,
        entry_frequency=str(universe['entry_frequency']),
        trend_frequency=str(universe['trend_frequency']),
        warmup=WarmupConfig(
            entry_bars=max(int(warmup['entry_bars']), 1),
            trend_bars=max(int(warmup['trend_bars']), 1),
        ),
        instrument_defaults=defaults_spec,
        instrument_overrides=overrides,
        risk_clusters=risk_clusters,
    )


def _build_strategy_config(sections: dict[str, dict[str, object]], session_flat_scope: str) -> StrategyConfig:
    environment = sections['strategy.environment']
    entry = sections['strategy.entry']
    exit_cfg = sections['strategy.exit']
    return StrategyConfig(
        environment=EnvironmentConfig(
            mode=str(environment['mode']).strip(),
            ma_period=max(int(environment['ma_period']), 2),
            macd_fast=max(int(environment['macd_fast']), 2),
            macd_slow=max(int(environment['macd_slow']), 3),
            macd_signal=max(int(environment['macd_signal']), 2),
            tsmom_lookbacks=parse_positive_int_sequence('strategy.environment.tsmom_lookbacks', environment.get('tsmom_lookbacks', [])),
            tsmom_min_agree=max(int(environment['tsmom_min_agree']), 1),
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
    )


def _build_portfolio_config(portfolio: dict[str, object]) -> PortfolioConfig:
    return PortfolioConfig(
        risk_per_trade_ratio=max(float(portfolio['risk_per_trade_ratio']), 0.0),
        max_total_armed_risk_ratio=max(float(portfolio['max_total_armed_risk_ratio']), 0.0),
        max_cluster_armed_risk_ratio=max(float(portfolio['max_cluster_armed_risk_ratio']), 0.0),
        max_same_direction_cluster_positions=max(int(portfolio['max_same_direction_cluster_positions']), 0),
        max_daily_loss_ratio=max(float(portfolio['max_daily_loss_ratio']), 0.0),
        max_drawdown_halt_ratio=max(float(portfolio['max_drawdown_halt_ratio']), 0.0),
        drawdown_halt_mode=str(portfolio['drawdown_halt_mode']).strip(),
        drawdown_risk_schedule=parse_drawdown_risk_schedule('portfolio.drawdown_risk_schedule', portfolio.get('drawdown_risk_schedule', [])),
    )


def _build_execution_config(execution: dict[str, object], fill_policy: str) -> ExecutionConfig:
    return ExecutionConfig(
        fill_policy=fill_policy,
        backtest_commission_ratio=max(float(execution['backtest_commission_ratio']), 0.0),
        backtest_slippage_ratio=max(float(execution['backtest_slippage_ratio']), 0.0),
    )


def _build_reporting_config(reporting: dict[str, object]) -> ReportingConfig:
    return ReportingConfig(
        enabled=bool(reporting['enabled']),
        output_dir=str(reporting['output_dir']),
        signals_filename=str(reporting['signals_filename']),
        executions_filename=str(reporting['executions_filename']),
        portfolio_daily_filename=str(reporting['portfolio_daily_filename']),
    )


def _build_observability_config(observability: dict[str, object]) -> ObservabilityConfig:
    return ObservabilityConfig(
        level=str(observability['level']).strip().upper(),
        sample_every_n=max(int(observability['sample_every_n']), 1),
    )


def load_config(path: str | Path) -> AppConfig:
    root = _load_root(path)
    raw_sections = _extract_raw_sections(root)
    _validate_unknown_keys(raw_sections)
    _validate_instrument_specs(raw_sections)

    merged_sections = _merge_sections(root)
    mode, fill_policy, symbols, session_flat_scope = _validate_runtime_values(merged_sections)

    return AppConfig(
        runtime=_build_runtime_config(merged_sections['runtime'], mode),
        broker=_build_broker_config(merged_sections),
        universe=_build_universe_config(merged_sections, symbols),
        strategy=_build_strategy_config(merged_sections, session_flat_scope),
        portfolio=_build_portfolio_config(merged_sections['portfolio']),
        execution=_build_execution_config(merged_sections['execution'], fill_policy),
        reporting=_build_reporting_config(merged_sections['reporting']),
        observability=_build_observability_config(merged_sections['observability']),
    )


__all__ = ['load_config']
