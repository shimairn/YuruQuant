from __future__ import annotations

from typing import Callable

from yuruquant.app.config_schema import AppConfig
from yuruquant.core.frames import SymbolFrames
from yuruquant.core.models import FillPolicy, ReportSink, RuntimeState, Signal, SymbolRuntime
from yuruquant.core.time import is_after, to_exchange_trade_day


def set_symbol_mapping(runtime: RuntimeState, csymbol: str, symbol: str) -> None:
    state = runtime.states_by_csymbol.setdefault(csymbol, SymbolRuntime(csymbol=csymbol))
    if state.main_symbol and state.main_symbol != symbol:
        runtime.symbol_to_csymbol.pop(state.main_symbol, None)
    state.main_symbol = symbol
    runtime.symbol_to_csymbol[symbol] = csymbol


def ensure_frames(runtime: RuntimeState, config: AppConfig, symbol: str) -> SymbolFrames:
    existing = runtime.bar_store.get(symbol)
    if isinstance(existing, SymbolFrames):
        return existing
    frames = SymbolFrames.create(
        symbol=symbol,
        entry_frequency=config.universe.entry_frequency,
        trend_frequency=config.universe.trend_frequency,
        entry_bars=config.universe.warmup.entry_bars,
        trend_bars=config.universe.warmup.trend_bars,
    )
    runtime.bar_store[symbol] = frames
    return frames


def backfill_if_needed(runtime: RuntimeState, config: AppConfig, gateway, symbol: str) -> None:
    frames = ensure_frames(runtime, config, symbol)
    if len(frames.entry) < config.universe.warmup.entry_bars:
        frames.entry.replace(gateway.fetch_history(symbol, config.universe.entry_frequency, config.universe.warmup.entry_bars))
    if len(frames.trend) < config.universe.warmup.trend_bars:
        frames.trend.replace(gateway.fetch_history(symbol, config.universe.trend_frequency, config.universe.warmup.trend_bars))


def queue_signal(
    config: AppConfig,
    report_sink: ReportSink,
    runtime: RuntimeState,
    fill_policy: FillPolicy,
    state: SymbolRuntime,
    csymbol: str,
    symbol: str,
    signal: Signal,
) -> None:
    fill_policy.queue(state, signal, signal.created_at)
    if config.reporting.enabled:
        report_sink.record_signal(runtime, config.runtime.mode, config.runtime.run_id, csymbol, symbol, signal)


def process_symbol(
    config: AppConfig,
    gateway,
    report_sink: ReportSink,
    fill_policy: FillPolicy,
    runtime: RuntimeState,
    state: SymbolRuntime,
    execute_due_signal: Callable[[SymbolRuntime, str, str, Signal], None],
    compute_environment_fn,
    evaluate_portfolio_guard_fn,
    make_flatten_signal_fn,
    evaluate_exit_signal_fn,
    maybe_generate_entry_fn,
    check_entry_against_armed_risk_cap_fn,
    check_entry_against_cluster_risk_fn,
    debug_fn,
) -> None:
    symbol = state.main_symbol
    if not symbol:
        return

    backfill_if_needed(runtime, config, gateway, symbol)
    frames = ensure_frames(runtime, config, symbol)
    if len(frames.entry) < config.universe.warmup.entry_bars or len(frames.trend) < config.universe.warmup.trend_bars:
        return

    current_eob = frames.entry.frame.latest_eob()
    if current_eob is None:
        return
    if state.last_processed_eob is not None and not is_after(current_eob, state.last_processed_eob):
        return

    state.last_processed_eob = current_eob
    state.bar_index_5m += 1

    due_signal = fill_policy.pop_due(state, current_eob)
    if due_signal is not None:
        execute_due_signal(state, state.csymbol, symbol, due_signal)

    state.environment = compute_environment_fn(
        frames.trend.frame,
        mode=config.strategy.environment.mode,
        ma_period=config.strategy.environment.ma_period,
        macd_fast=config.strategy.environment.macd_fast,
        macd_slow=config.strategy.environment.macd_slow,
        macd_signal=config.strategy.environment.macd_signal,
        tsmom_lookbacks=config.strategy.environment.tsmom_lookbacks,
        tsmom_min_agree=config.strategy.environment.tsmom_min_agree,
    )

    portfolio_snapshot = gateway.get_portfolio_snapshot()
    guard = evaluate_portfolio_guard_fn(
        state=runtime.portfolio,
        snapshot=portfolio_snapshot,
        max_daily_loss_ratio=config.portfolio.max_daily_loss_ratio,
        max_drawdown_halt_ratio=config.portfolio.max_drawdown_halt_ratio,
        trade_time=current_eob,
        fallback_equity=config.broker.gm.backtest.initial_cash,
        drawdown_halt_mode=config.portfolio.drawdown_halt_mode,
        drawdown_risk_schedule=config.portfolio.drawdown_risk_schedule,
    )
    current_price = frames.entry.frame.latest_close()
    spec = config.universe.instrument_overrides.get(state.csymbol, config.universe.instrument_defaults)

    if guard.force_flatten and state.position is not None and state.pending_signal is None:
        signal = make_flatten_signal_fn(state.position, current_price, current_eob, spec.multiplier, f'portfolio halt: {guard.reason}')
        queue_signal(config, report_sink, runtime, fill_policy, state, state.csymbol, symbol, signal)
        return

    if state.position is not None and state.pending_signal is None:
        signal = evaluate_exit_signal_fn(
            config=config,
            position=state.position,
            frame_5m=frames.entry.frame,
            environment=state.environment,
            current_eob=current_eob,
            spec=spec,
            multiplier=spec.multiplier,
        )
        if signal is not None:
            queue_signal(config, report_sink, runtime, fill_policy, state, state.csymbol, symbol, signal)

    if state.position is None and guard.allow_entries and state.pending_signal is None:
        signal = maybe_generate_entry_fn(
            config=config,
            portfolio=runtime.portfolio,
            environment=state.environment,
            csymbol=state.csymbol,
            frame_5m=frames.entry.frame,
            current_eob=current_eob,
        )
        if signal is not None:
            armed_risk_check = check_entry_against_armed_risk_cap_fn(config, runtime, state.csymbol, signal)
            if armed_risk_check.breached:
                debug_fn(
                    'engine.entry_blocked_armed_risk_cap',
                    csymbol=state.csymbol,
                    current_armed_risk_ratio=f'{armed_risk_check.current_ratio:.4f}',
                    proposed_entry_risk_ratio=f'{armed_risk_check.proposed_ratio:.4f}',
                    max_total_armed_risk_ratio=f'{armed_risk_check.cap_ratio:.4f}',
                    campaign_id=signal.campaign_id,
                )
            else:
                cluster_risk_check = check_entry_against_cluster_risk_fn(config, runtime, state.csymbol, signal)
                if cluster_risk_check.breached:
                    debug_fn(
                        'engine.entry_blocked_cluster_risk',
                        csymbol=state.csymbol,
                        cluster_names=','.join(detail.cluster_name for detail in cluster_risk_check.details),
                        breach_reasons=','.join(cluster_risk_check.breach_reasons),
                        max_cluster_armed_risk_ratio=f'{cluster_risk_check.max_cluster_armed_risk_ratio:.4f}',
                        max_same_direction_cluster_positions=cluster_risk_check.max_same_direction_cluster_positions,
                        campaign_id=signal.campaign_id,
                    )
                else:
                    queue_signal(config, report_sink, runtime, fill_policy, state, state.csymbol, symbol, signal)

    if config.reporting.enabled:
        report_sink.record_portfolio_day(runtime, config.runtime.mode, config.runtime.run_id, to_exchange_trade_day(current_eob), current_eob)

    if state.bar_index_5m % 100 == 0:
        debug_fn(
            'engine.heartbeat',
            csymbol=state.csymbol,
            symbol=symbol,
            bar_index_5m=state.bar_index_5m,
            direction=state.environment.direction,
            macd_histogram=f'{state.environment.macd_histogram:.4f}',
        )
