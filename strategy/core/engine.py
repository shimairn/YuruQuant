from __future__ import annotations

from strategy.adapters.gm.market_data import fetch_kline
from strategy.adapters.gm.orders import PositionSide_Long, PositionSide_Short, get_position, position_volume
from strategy.core.bar_request_policy import has_enough_warmup, normalize_freq_alias, resolve_req_1h_count
from strategy.core.indicators import latest_atr_value
from strategy.core.runtime_state import ensure_daily_counter, ensure_symbol_store, initialize_runtime_state
from strategy.core.signal_queue import (
    flush_due_pending_signal,
    queue_risk_signal_if_needed,
    queue_signal_for_next_bar,
)
from strategy.core.time_utils import is_after
from strategy.observability.log import debug, warn
from strategy.pipelines.entry import process_entry_pipeline
from strategy.pipelines.risk import process_risk_pipeline
from strategy.pipelines.trend_1h import refresh_h1_trend_state
from strategy.reporting.daily_report import append_daily_report
from strategy.types import AppConfig, RuntimeContext, SymbolState, TradingSignal


class StrategyEngine:
    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg
        self.runtime = RuntimeContext(cfg=cfg)
        self.context = None

    def bind_context(self, context) -> None:
        self.context = context
        self.runtime.context = context

    def initialize_runtime(self, context) -> None:
        self.bind_context(context)
        initialize_runtime_state(self.runtime, self.cfg, context)

    def set_symbol_mapping(self, csymbol: str, symbol: str) -> None:
        state = self.runtime.states_by_csymbol.setdefault(csymbol, SymbolState(csymbol=csymbol))
        if state.main_symbol and state.main_symbol != symbol:
            self.runtime.symbol_to_csymbol.pop(state.main_symbol, None)

        state.main_symbol = symbol
        self.runtime.symbol_to_csymbol[symbol] = csymbol
        ensure_symbol_store(self.runtime, symbol)

    def queue_signal_for_next_bar(self, state: SymbolState, signal: TradingSignal, current_eob: object) -> None:
        queue_signal_for_next_bar(state, signal, current_eob)

    def flush_due_pending_signal(self, state: SymbolState, csymbol: str, symbol: str, current_eob: object) -> None:
        flush_due_pending_signal(self, state, csymbol, symbol, current_eob)

    def _resolve_req_1h_count(self) -> int:
        return resolve_req_1h_count(self.cfg)

    def _on_trade_executed(self, signal: TradingSignal) -> None:
        p = self.runtime.portfolio_risk
        if signal.action.startswith("close"):
            p.trades_count += 1
            p.realized_pnl += float(signal.net_pnl)
            if signal.net_pnl >= 0:
                p.wins += 1
            else:
                p.losses += 1

    @staticmethod
    def _build_row_from_bar(bar) -> dict[str, object]:
        return {
            "eob": getattr(bar, "eob", None),
            "open": getattr(bar, "open", None),
            "high": getattr(bar, "high", None),
            "low": getattr(bar, "low", None),
            "close": getattr(bar, "close", None),
            "volume": getattr(bar, "volume", None),
        }

    def _maybe_backfill(self, symbol: str) -> None:
        store = ensure_symbol_store(self.runtime, symbol)

        if len(store.frame_5m) < self.cfg.runtime.warmup_5m:
            frame = fetch_kline(self.context, symbol, self.cfg.runtime.freq_5m, self.cfg.runtime.warmup_5m)
            store.frame_5m.replace(frame)

        if len(store.frame_1h) < self.cfg.runtime.warmup_1h:
            frame = fetch_kline(self.context, symbol, self.cfg.runtime.freq_1h, self.cfg.runtime.warmup_1h)
            store.frame_1h.replace(frame)

    def _process_symbol(self, csymbol: str, state: SymbolState) -> None:
        symbol = state.main_symbol
        if not symbol:
            return

        store = ensure_symbol_store(self.runtime, symbol)
        self._maybe_backfill(symbol)

        frame_5m = store.frame_5m.frame
        frame_1h = store.frame_1h.frame

        warmup_ok, min_5m, min_1h = has_enough_warmup(
            frame_5m,
            frame_1h,
            self.cfg.runtime.warmup_5m,
            self._resolve_req_1h_count(),
        )
        if not warmup_ok:
            warn(
                "engine.warmup_shortage",
                sample_key=f"engine:warmup_shortage:{csymbol}",
                csymbol=csymbol,
                symbol=symbol,
                bars_5m=len(frame_5m),
                bars_1h=len(frame_1h),
                required_5m=min_5m,
                required_1h=min_1h,
            )
            return

        current_eob = frame_5m.latest_eob()
        if current_eob is None:
            return
        if state.last_5m_processed_eob is not None and not is_after(current_eob, state.last_5m_processed_eob):
            return

        state.last_5m_processed_eob = current_eob
        state.bar_index_5m += 1
        ensure_daily_counter(state, current_eob)

        self.flush_due_pending_signal(state, csymbol, symbol, current_eob)
        refresh_h1_trend_state(state, frame_1h, self.cfg.strategy)

        current_price = frame_5m.latest_close()
        atr_val = latest_atr_value(frame_5m, self.cfg.strategy.atr_period)

        long_qty = position_volume(get_position(self.context, symbol, PositionSide_Long))
        short_qty = position_volume(get_position(self.context, symbol, PositionSide_Short))

        stop_here, risk_signal = process_risk_pipeline(
            self.runtime,
            state,
            csymbol,
            symbol,
            current_eob,
            current_price,
            atr_val,
            long_qty,
            short_qty,
            frame_1h,
        )

        if queue_risk_signal_if_needed(state, risk_signal, current_eob):
            self.queue_signal_for_next_bar(state, risk_signal, current_eob)

        if not stop_here:
            entry_signal = process_entry_pipeline(
                self.runtime,
                state,
                csymbol,
                symbol,
                frame_5m,
                current_eob,
                current_price,
                atr_val,
                long_qty,
                short_qty,
            )
            if entry_signal is not None:
                self.queue_signal_for_next_bar(state, entry_signal, current_eob)

        if self.cfg.reporting.enabled:
            append_daily_report(self.runtime, current_eob)

    def process_symbols_by_bars(self, bars) -> None:
        if not self.runtime.symbol_to_csymbol:
            return

        allowed_5m = {normalize_freq_alias(self.cfg.runtime.freq_5m), "5m"}
        allowed_1h = {normalize_freq_alias(self.cfg.runtime.freq_1h), "1h"}

        trigger_symbols: set[str] = set()
        for bar in bars:
            symbol = getattr(bar, "symbol", None)
            if not symbol or symbol not in self.runtime.symbol_to_csymbol:
                continue

            freq = normalize_freq_alias(getattr(bar, "frequency", ""))
            store = ensure_symbol_store(self.runtime, symbol)
            row = self._build_row_from_bar(bar)

            if freq in allowed_5m:
                store.frame_5m.append([row])
                trigger_symbols.add(symbol)
            elif freq in allowed_1h:
                store.frame_1h.append([row])

        for symbol in sorted(trigger_symbols):
            csymbol = self.runtime.symbol_to_csymbol.get(symbol)
            if not csymbol:
                continue
            state = self.runtime.states_by_csymbol.get(csymbol)
            if state is None:
                continue
            self._process_symbol(csymbol, state)

            if state.bar_index_5m % 100 == 0:
                debug(
                    "engine.heartbeat",
                    csymbol=csymbol,
                    symbol=symbol,
                    bar_index_5m=state.bar_index_5m,
                    h1_trend=state.h1_trend,
                    h1_strength=f"{state.h1_strength:.3f}",
                )
