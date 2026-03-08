from __future__ import annotations

from dataclasses import replace

from yuruquant.app.config import AppConfig
from yuruquant.core.execution_diagnostics import build_execution_diagnostics
from yuruquant.core.fill_policy import NextBarOpenFillPolicy
from yuruquant.core.frames import SymbolFrames
from yuruquant.core.models import BrokerGateway, EntrySignal, ExitSignal, FillPolicy, ReportSink, RuntimeState, Signal, SymbolRuntime
from yuruquant.core.time import is_after, to_trade_day
from yuruquant.portfolio import check_entry_against_armed_risk_cap, evaluate_portfolio_guard, modeled_portfolio_snapshot
from yuruquant.reporting.logging import debug, warn
from yuruquant.strategy.trend_breakout import build_managed_position, compute_environment, compute_exit_pnl, evaluate_exit_signal, make_flatten_signal, maybe_generate_entry


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
        state = self.runtime.states_by_csymbol.setdefault(csymbol, SymbolRuntime(csymbol=csymbol))
        if state.main_symbol and state.main_symbol != symbol:
            self.runtime.symbol_to_csymbol.pop(state.main_symbol, None)
        state.main_symbol = symbol
        self.runtime.symbol_to_csymbol[symbol] = csymbol

    def _ensure_frames(self, symbol: str) -> SymbolFrames:
        existing = self.runtime.bar_store.get(symbol)
        if isinstance(existing, SymbolFrames):
            return existing
        frames = SymbolFrames.create(
            symbol=symbol,
            entry_frequency=self.config.universe.entry_frequency,
            trend_frequency=self.config.universe.trend_frequency,
            entry_bars=self.config.universe.warmup.entry_bars,
            trend_bars=self.config.universe.warmup.trend_bars,
        )
        self.runtime.bar_store[symbol] = frames
        return frames

    def _backfill_if_needed(self, symbol: str) -> None:
        frames = self._ensure_frames(symbol)
        if len(frames.entry) < self.config.universe.warmup.entry_bars:
            frames.entry.replace(self.gateway.fetch_history(symbol, self.config.universe.entry_frequency, self.config.universe.warmup.entry_bars))
        if len(frames.trend) < self.config.universe.warmup.trend_bars:
            frames.trend.replace(self.gateway.fetch_history(symbol, self.config.universe.trend_frequency, self.config.universe.warmup.trend_bars))

    def _queue_signal(self, state: SymbolRuntime, csymbol: str, symbol: str, signal: Signal) -> None:
        self.fill_policy.queue(state, signal, signal.created_at)
        if self.config.reporting.enabled:
            self.report_sink.record_signal(self.runtime, self.config.runtime.mode, self.config.runtime.run_id, csymbol, symbol, signal)

    def _accepted(self, signal: Signal, results: list[object]) -> bool:
        if not results:
            return False
        accepted_values = [bool(getattr(item, 'accepted', False)) for item in results]
        if isinstance(signal, EntrySignal):
            return any(accepted_values)
        return all(accepted_values)

    def _update_trade_stats(self, signal: ExitSignal) -> None:
        portfolio = self.runtime.portfolio
        portfolio.trades_count += 1
        portfolio.realized_pnl += float(signal.net_pnl)
        if signal.net_pnl >= 0:
            portfolio.wins += 1
        else:
            portfolio.losses += 1

    def _estimate_fill(self, symbol: str, signal: Signal) -> tuple[object, float]:
        frames = self.runtime.bar_store.get(symbol)
        if isinstance(frames, SymbolFrames) and not frames.entry.frame.empty_frame:
            fill_ts = frames.entry.frame.latest_open_time() or signal.created_at
            fill_price = frames.entry.frame.latest_open() or float(signal.price)
            return fill_ts, float(fill_price)
        return signal.created_at, float(signal.price)

    def _execute_due_signal(self, state: SymbolRuntime, csymbol: str, symbol: str, signal: Signal) -> None:
        fill_ts, fill_price = self._estimate_fill(symbol, signal)
        spec = self.config.universe.instrument_overrides.get(csymbol, self.config.universe.instrument_defaults)
        diagnostics = build_execution_diagnostics(signal, fill_ts, fill_price, spec, position=state.position)
        intents = self.gateway.plan_order_intents(symbol, signal)
        results = self.gateway.submit_order_intents(intents)
        if self.config.reporting.enabled:
            self.report_sink.record_executions(
                self.runtime,
                self.config.runtime.mode,
                self.config.runtime.run_id,
                csymbol,
                symbol,
                signal,
                fill_ts,
                fill_price,
                diagnostics,
                results,
            )
        if not self._accepted(signal, results):
            if isinstance(signal, ExitSignal):
                warn('execution.close_rejected', csymbol=csymbol, symbol=symbol, action=signal.action)
            return

        if isinstance(signal, EntrySignal):
            state.position = build_managed_position(signal, fill_price=fill_price, fill_ts=fill_ts)
            return

        actual_signal = signal
        if state.position is not None:
            cost_ratio = self.config.execution.backtest_commission_ratio + self.config.execution.backtest_slippage_ratio
            gross, net = compute_exit_pnl(state.position, fill_price, spec.multiplier, cost_ratio)
            actual_signal = replace(signal, gross_pnl=gross, net_pnl=net)
        self._update_trade_stats(actual_signal)
        state.position = None

    def _process_symbol(self, state: SymbolRuntime) -> None:
        symbol = state.main_symbol
        if not symbol:
            return

        self._backfill_if_needed(symbol)
        frames = self._ensure_frames(symbol)
        if len(frames.entry) < self.config.universe.warmup.entry_bars or len(frames.trend) < self.config.universe.warmup.trend_bars:
            return

        current_eob = frames.entry.frame.latest_eob()
        if current_eob is None:
            return
        if state.last_processed_eob is not None and not is_after(current_eob, state.last_processed_eob):
            return

        state.last_processed_eob = current_eob
        state.bar_index_5m += 1

        due_signal = self.fill_policy.pop_due(state, current_eob)
        if due_signal is not None:
            self._execute_due_signal(state, state.csymbol, symbol, due_signal)

        state.environment = compute_environment(
            frames.trend.frame,
            ma_period=self.config.strategy.environment.ma_period,
            macd_fast=self.config.strategy.environment.macd_fast,
            macd_slow=self.config.strategy.environment.macd_slow,
            macd_signal=self.config.strategy.environment.macd_signal,
        )

        portfolio_snapshot = self.gateway.get_portfolio_snapshot()
        if str(self.config.runtime.mode).upper() == 'BACKTEST':
            portfolio_snapshot = modeled_portfolio_snapshot(self.config, self.runtime, self.config.broker.gm.backtest.initial_cash)
        guard = evaluate_portfolio_guard(
            state=self.runtime.portfolio,
            snapshot=portfolio_snapshot,
            max_daily_loss_ratio=self.config.portfolio.max_daily_loss_ratio,
            max_drawdown_halt_ratio=self.config.portfolio.max_drawdown_halt_ratio,
            trade_time=current_eob,
            fallback_equity=self.config.broker.gm.backtest.initial_cash,
        )
        current_price = frames.entry.frame.latest_close()
        spec = self.config.universe.instrument_overrides.get(state.csymbol, self.config.universe.instrument_defaults)
        cost_ratio = self.config.execution.backtest_commission_ratio + self.config.execution.backtest_slippage_ratio

        if guard.force_flatten and state.position is not None and state.pending_signal is None:
            signal = make_flatten_signal(state.position, current_price, current_eob, spec.multiplier, cost_ratio, f'portfolio halt: {guard.reason}')
            self._queue_signal(state, state.csymbol, symbol, signal)
            return

        if state.position is not None and state.pending_signal is None:
            signal = evaluate_exit_signal(
                config=self.config,
                position=state.position,
                frame_5m=frames.entry.frame,
                environment=state.environment,
                current_eob=current_eob,
                spec=spec,
                multiplier=spec.multiplier,
                cost_ratio=cost_ratio,
            )
            if signal is not None:
                self._queue_signal(state, state.csymbol, symbol, signal)

        if state.position is None and guard.allow_entries and state.pending_signal is None:
            signal = maybe_generate_entry(
                config=self.config,
                portfolio=self.runtime.portfolio,
                environment=state.environment,
                csymbol=state.csymbol,
                frame_5m=frames.entry.frame,
                current_eob=current_eob,
            )
            if signal is not None:
                armed_risk_check = check_entry_against_armed_risk_cap(self.config, self.runtime, state.csymbol, signal)
                if armed_risk_check.breached:
                    debug(
                        'engine.entry_blocked_armed_risk_cap',
                        csymbol=state.csymbol,
                        current_armed_risk_ratio=f'{armed_risk_check.current_ratio:.4f}',
                        proposed_entry_risk_ratio=f'{armed_risk_check.proposed_ratio:.4f}',
                        max_total_armed_risk_ratio=f'{armed_risk_check.cap_ratio:.4f}',
                        campaign_id=signal.campaign_id,
                    )
                else:
                    self._queue_signal(state, state.csymbol, symbol, signal)

        if self.config.reporting.enabled:
            self.report_sink.record_portfolio_day(self.runtime, self.config.runtime.mode, self.config.runtime.run_id, to_trade_day(current_eob), current_eob)

        if state.bar_index_5m % 100 == 0:
            debug(
                'engine.heartbeat',
                csymbol=state.csymbol,
                symbol=symbol,
                bar_index_5m=state.bar_index_5m,
                direction=state.environment.direction,
                macd_histogram=f'{state.environment.macd_histogram:.4f}',
            )

    def on_market_event(self, event) -> None:
        triggered_symbols: set[str] = set()
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
            if bar.frequency == '5m':
                frames.entry.append(payload)
                triggered_symbols.add(bar.symbol)
            elif bar.frequency == '1h':
                frames.trend.append(payload)

        for symbol in sorted(triggered_symbols):
            csymbol = self.runtime.symbol_to_csymbol.get(symbol)
            if not csymbol:
                continue
            state = self.runtime.states_by_csymbol.get(csymbol)
            if state is None:
                continue
            self._process_symbol(state)





