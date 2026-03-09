from __future__ import annotations

from dataclasses import replace

from yuruquant.app.config_schema import AppConfig
from yuruquant.core.execution_diagnostics import build_execution_diagnostics
from yuruquant.core.frames import SymbolFrames
from yuruquant.core.models import EntrySignal, ExitSignal, ExecutionResult, ReportSink, RuntimeState, Signal, SymbolRuntime
from yuruquant.reporting.logging import warn
from yuruquant.strategy.trend_breakout.exit_state import build_managed_position, compute_exit_pnl


def estimate_fill(runtime: RuntimeState, symbol: str, signal: Signal) -> tuple[object, float]:
    frames = runtime.bar_store.get(symbol)
    if isinstance(frames, SymbolFrames) and not frames.entry.frame.empty_frame:
        fill_ts = frames.entry.frame.latest_open_time() or signal.created_at
        fill_price = frames.entry.frame.latest_open() or float(signal.price)
        return fill_ts, float(fill_price)
    return signal.created_at, float(signal.price)


def signal_accepted(signal: Signal, results: list[ExecutionResult]) -> bool:
    if not results:
        return False
    accepted_values = [bool(getattr(item, 'accepted', False)) for item in results]
    if isinstance(signal, EntrySignal):
        return any(accepted_values)
    return all(accepted_values)


def update_trade_stats(runtime: RuntimeState, signal: ExitSignal) -> None:
    portfolio = runtime.portfolio
    portfolio.trades_count += 1
    portfolio.realized_pnl += float(signal.net_pnl)
    if signal.net_pnl >= 0:
        portfolio.wins += 1
    else:
        portfolio.losses += 1


def execute_due_signal(
    config: AppConfig,
    gateway,
    report_sink: ReportSink,
    runtime: RuntimeState,
    state: SymbolRuntime,
    csymbol: str,
    symbol: str,
    signal: Signal,
) -> None:
    fill_ts, fill_price = estimate_fill(runtime, symbol, signal)
    spec = config.universe.instrument_overrides.get(csymbol, config.universe.instrument_defaults)
    diagnostics = build_execution_diagnostics(signal, fill_ts, fill_price, spec, position=state.position)
    intents = gateway.plan_order_intents(symbol, signal)
    results = gateway.submit_order_intents(intents)
    if config.reporting.enabled:
        report_sink.record_executions(
            runtime,
            config.runtime.mode,
            config.runtime.run_id,
            csymbol,
            symbol,
            signal,
            fill_ts,
            fill_price,
            diagnostics,
            results,
        )
    if not signal_accepted(signal, results):
        if isinstance(signal, ExitSignal):
            warn('execution.close_rejected', csymbol=csymbol, symbol=symbol, action=signal.action)
        return

    if isinstance(signal, EntrySignal):
        state.position = build_managed_position(signal, fill_price=fill_price, fill_ts=fill_ts)
        return

    actual_signal = signal
    if state.position is not None:
        gross, net = compute_exit_pnl(state.position, fill_price, spec.multiplier)
        actual_signal = replace(signal, gross_pnl=gross, net_pnl=net)
    update_trade_stats(runtime, actual_signal)
    state.position = None
