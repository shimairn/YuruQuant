from __future__ import annotations

from yuruquant.app.config_schema import AppConfig
from yuruquant.core import engine_execution, engine_processing
from yuruquant.core.fill_policy import NextBarOpenFillPolicy
from yuruquant.core.frames import SymbolFrames
from yuruquant.core.models import BrokerGateway, ExitSignal, FillPolicy, ReportSink, RuntimeState, Signal, SymbolRuntime
from yuruquant.core.time import normalize_frequency
from yuruquant.portfolio.armed_exposure import check_entry_against_armed_risk_cap
from yuruquant.portfolio.cluster_risk import check_entry_against_cluster_risk
from yuruquant.portfolio.risk import evaluate_portfolio_guard
from yuruquant.reporting.logging import debug
from yuruquant.strategy.trend_breakout.entry_rules import maybe_generate_entry
from yuruquant.strategy.trend_breakout.environment import compute_environment
from yuruquant.strategy.trend_breakout.exit_state import evaluate_exit_signal, make_flatten_signal


class StrategyEngine:
    def __init__(self, config: AppConfig, gateway: BrokerGateway, report_sink: ReportSink, fill_policy: FillPolicy | None = None) -> None:
        self.config = config
        self.gateway = gateway
        self.report_sink = report_sink
        self.fill_policy = fill_policy or NextBarOpenFillPolicy()
        self.runtime = RuntimeState(states_by_csymbol={csymbol: SymbolRuntime(csymbol=csymbol) for csymbol in config.universe.symbols})

    def initialize(self, context: object) -> None:
        self.runtime.context = context
        self.gateway.bind_context(context)
        if self.config.reporting.enabled:
            self.report_sink.ensure_ready(self.runtime, self.config.runtime.mode, self.config.runtime.run_id)

    def _set_symbol_mapping(self, csymbol: str, symbol: str) -> None:
        engine_processing.set_symbol_mapping(self.runtime, csymbol, symbol)

    def _ensure_frames(self, symbol: str) -> SymbolFrames:
        return engine_processing.ensure_frames(self.runtime, self.config, symbol)

    def _backfill_if_needed(self, symbol: str) -> None:
        engine_processing.backfill_if_needed(self.runtime, self.config, self.gateway, symbol)

    def _queue_signal(self, state: SymbolRuntime, csymbol: str, symbol: str, signal: Signal) -> None:
        engine_processing.queue_signal(self.config, self.report_sink, self.runtime, self.fill_policy, state, csymbol, symbol, signal)

    def _accepted(self, signal: Signal, results: list[object]) -> bool:
        return engine_execution.signal_accepted(signal, results)

    def _update_trade_stats(self, signal: ExitSignal) -> None:
        engine_execution.update_trade_stats(self.runtime, signal)

    def _estimate_fill(self, symbol: str, signal: Signal) -> tuple[object, float]:
        return engine_execution.estimate_fill(self.runtime, symbol, signal)

    def _execute_due_signal(self, state: SymbolRuntime, csymbol: str, symbol: str, signal: Signal) -> None:
        engine_execution.execute_due_signal(self.config, self.gateway, self.report_sink, self.runtime, state, csymbol, symbol, signal)

    def _process_symbol(self, state: SymbolRuntime) -> None:
        engine_processing.process_symbol(
            config=self.config,
            gateway=self.gateway,
            report_sink=self.report_sink,
            fill_policy=self.fill_policy,
            runtime=self.runtime,
            state=state,
            execute_due_signal=self._execute_due_signal,
            compute_environment_fn=compute_environment,
            evaluate_portfolio_guard_fn=evaluate_portfolio_guard,
            make_flatten_signal_fn=make_flatten_signal,
            evaluate_exit_signal_fn=evaluate_exit_signal,
            maybe_generate_entry_fn=maybe_generate_entry,
            check_entry_against_armed_risk_cap_fn=check_entry_against_armed_risk_cap,
            check_entry_against_cluster_risk_fn=check_entry_against_cluster_risk,
            debug_fn=debug,
        )

    def on_market_event(self, event) -> None:
        triggered_symbols: set[str] = set()
        entry_frequency = normalize_frequency(self.config.universe.entry_frequency)
        trend_frequency = normalize_frequency(self.config.universe.trend_frequency)
        for bar in event.bars:
            self._set_symbol_mapping(bar.csymbol, bar.symbol)
            state = self.runtime.states_by_csymbol.get(bar.csymbol)
            if state is None:
                continue
            frames = self._ensure_frames(bar.symbol)
            payload = [{
                'eob': bar.eob,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume,
            }]
            bar_frequency = normalize_frequency(bar.frequency)
            if bar_frequency == entry_frequency:
                frames.entry.append(payload)
                triggered_symbols.add(bar.symbol)
            elif bar_frequency == trend_frequency:
                frames.trend.append(payload)

        for symbol in sorted(triggered_symbols):
            csymbol = self.runtime.symbol_to_csymbol.get(symbol)
            if not csymbol:
                continue
            state = self.runtime.states_by_csymbol.get(csymbol)
            if state is None:
                continue
            self._process_symbol(state)
