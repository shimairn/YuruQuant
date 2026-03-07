from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from strategy.gm.market_data import fetch_kline
from strategy.gm.orders import (
    PositionSide_Long,
    PositionSide_Short,
    execute_signal,
    get_position,
    position_volume,
)
from strategy.pipelines.chan_5m import calculate_atr
from strategy.pipelines.entry import process_entry_pipeline, signal_due
from strategy.pipelines.risk import process_risk_pipeline, seed_position_risk_from_entry_signal
from strategy.pipelines.trend_1h import calculate_h1_trailing_stop_ema, refresh_h1_trend_state
from strategy.reporting.daily_report import append_daily_report, ensure_daily_report
from strategy.reporting.trade_report import append_trade_report, ensure_trade_report
from strategy.types import AppConfig, RuntimeContext, SymbolState, TradingSignal


@dataclass
class StepHooks:
    trace: list[str]

    def mark(self, step: str) -> None:
        self.trace.append(step)


def _normalize_freq_alias(value: object) -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "5m": "5m",
        "5min": "5m",
        "5minute": "5m",
        "300s": "5m",
        "300sec": "5m",
        "300second": "5m",
    }
    return aliases.get(raw, raw)


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
        self.runtime.states_by_csymbol = {
            csymbol: SymbolState(csymbol=csymbol) for csymbol in self.cfg.runtime.symbols
        }
        self.runtime.symbol_to_csymbol = {}
        self.runtime.last_roll_date = ""
        if self.cfg.reporting.enabled:
            ensure_trade_report(self.runtime)
            ensure_daily_report(self.runtime)

    def set_symbol_mapping(self, csymbol: str, symbol: str) -> None:
        state = self.runtime.states_by_csymbol.setdefault(csymbol, SymbolState(csymbol=csymbol))
        state.main_symbol = symbol
        self.runtime.symbol_to_csymbol[symbol] = csymbol

    def queue_signal_for_next_bar(self, state: SymbolState, signal: TradingSignal, current_eob: object) -> None:
        state.pending_signal = signal
        state.pending_signal_eob = current_eob

    def _on_trade_executed(self, signal: TradingSignal) -> None:
        p = self.runtime.portfolio_risk
        if signal.action.startswith("close"):
            p.trades_count += 1
            p.realized_pnl += float(signal.net_pnl)
            if signal.net_pnl >= 0:
                p.wins += 1
            else:
                p.losses += 1

    def flush_due_pending_signal(self, state: SymbolState, csymbol: str, symbol: str, current_eob: object) -> None:
        if state.pending_signal is None:
            return
        if not signal_due(current_eob, state.pending_signal_eob):
            return

        sig = state.pending_signal
        sig.created_eob = current_eob
        execute_signal(self.context, state, sig, symbol)
        if sig.action in {"buy", "sell"}:
            seed_position_risk_from_entry_signal(self.runtime, state, sig, float(sig.price), current_eob)
        if self.cfg.reporting.enabled:
            append_trade_report(self.runtime, csymbol, symbol, current_eob, sig)
        self._on_trade_executed(sig)
        state.pending_signal = None
        state.pending_signal_eob = None

    def _ensure_daily_counter(self, state: SymbolState, current_eob: object) -> None:
        trade_day = pd.to_datetime(current_eob).strftime("%Y-%m-%d")
        if state.daily_entry_date != trade_day:
            state.daily_entry_date = trade_day
            state.daily_entry_count = 0
        if state.daily_stopout_date != trade_day:
            state.daily_stopout_date = trade_day
            state.daily_stopout_count = 0

    def _queue_risk_signal_if_needed(
        self,
        state: SymbolState,
        signal: Optional[TradingSignal],
        current_eob: object,
    ) -> None:
        if signal is None:
            return
        if signal.action == "none":
            return

        if state.last_risk_signal_eob is not None and pd.to_datetime(current_eob) <= pd.to_datetime(state.last_risk_signal_eob):
            return
        state.last_risk_signal_eob = current_eob
        self.queue_signal_for_next_bar(state, signal, current_eob)

    def _resolve_req_1h_count(self) -> int:
        configured = max(int(self.cfg.runtime.sub_count_1h), 1)
        target = max(
            configured,
            int(self.cfg.strategy.h1_ema_slow_period) + 5,
            int(self.cfg.strategy.h1_rsi_period) + 5,
            1,
        )

        if str(self.cfg.runtime.mode).upper() != "BACKTEST":
            return target
        if self.context is None or not hasattr(self.context, "now"):
            return target

        try:
            bt_start = pd.to_datetime(self.cfg.gm.backtest_start)
            now_ts = pd.to_datetime(self.context.now)
            if pd.isna(bt_start) or pd.isna(now_ts):
                return target
            elapsed_hours = int(max((now_ts - bt_start).total_seconds() // 3600, 0))
            available = max(1, elapsed_hours + 1)
            return min(target, available)
        except Exception:
            return target

    def process_symbol_on_5m(self, csymbol: str, state: SymbolState, hooks: StepHooks | None = None) -> None:
        symbol = state.main_symbol
        if not symbol:
            return

        req_5m = max(int(self.cfg.runtime.sub_count_5m), 1)
        # Ramp 1h request count during early backtest bars to avoid permission-boundary failures.
        req_1h = self._resolve_req_1h_count()
        df_5m = fetch_kline(self.context, symbol, self.cfg.runtime.freq_5m, req_5m)
        df_1h = fetch_kline(self.context, symbol, self.cfg.runtime.freq_1h, req_1h)

        # Warmup gate: free-tier boundary often returns one bar less at startup.
        # Keep execution alive near free-tier boundary where full cache depth may be unavailable.
        min_5m = max(20, req_5m // 2)
        min_1h = 1
        if len(df_5m) < min_5m or len(df_1h) < min_1h:
            print(
                f"process_symbol skip data_shortage csymbol={csymbol} symbol={symbol} "
                f"df_5m_len={len(df_5m)} df_1h_len={len(df_1h)} "
                f"req_5m={req_5m} req_1h={req_1h} min_5m={min_5m} min_1h={min_1h}"
            )
            return

        current_eob = df_5m.iloc[-1]["eob"]
        if state.last_5m_processed_eob is not None and pd.to_datetime(current_eob) <= pd.to_datetime(state.last_5m_processed_eob):
            return

        state.last_5m_processed_eob = current_eob
        state.bar_index_5m += 1
        self._ensure_daily_counter(state, current_eob)

        # 1) flush pending signal
        if hooks:
            hooks.mark("flush")
        self.flush_due_pending_signal(state, csymbol, symbol, current_eob)

        # 2) refresh 1h trend
        if hooks:
            hooks.mark("trend")
        refresh_h1_trend_state(state, df_1h, self.cfg.strategy)

        current_price = float(df_5m.iloc[-1]["close"])
        atr_series = calculate_atr(df_5m, self.cfg.strategy.atr_period)
        atr_val = float(atr_series.iloc[-1]) if len(atr_series) > 0 else 0.0

        long_pos = get_position(self.context, symbol, PositionSide_Long)
        short_pos = get_position(self.context, symbol, PositionSide_Short)
        long_qty = position_volume(long_pos)
        short_qty = position_volume(short_pos)

        # 3) risk pipeline
        if hooks:
            hooks.mark("risk")
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
            df_1h,
        )
        self._queue_risk_signal_if_needed(state, risk_signal, current_eob)

        # 4) entry pipeline
        if not stop_here:
            if hooks:
                hooks.mark("entry")
            entry_signal = process_entry_pipeline(
                self.runtime,
                state,
                csymbol,
                symbol,
                df_5m,
                current_eob,
                current_price,
                atr_val,
                long_qty,
                short_qty,
            )
            if entry_signal is not None:
                self.queue_signal_for_next_bar(state, entry_signal, current_eob)
            elif state.bar_index_5m % 50 == 0:
                print(
                    f"entry idle csymbol={csymbol} symbol={symbol} "
                    f"bar_idx={state.bar_index_5m} h1_trend={state.h1_trend} "
                    f"h1_strength={state.h1_strength:.3f} pending_platform={int(state.pending_platform is not None)} "
                    f"daily_entry_count={state.daily_entry_count}"
                )
        else:
            if hooks:
                hooks.mark("entry_skipped")

        # 5) daily report + logs
        if hooks:
            hooks.mark("daily")
        if self.cfg.reporting.enabled:
            append_daily_report(self.runtime, current_eob)

    def process_symbols_by_bars(self, bars) -> None:
        if not self.runtime.symbol_to_csymbol:
            return

        allowed_5m = {
            _normalize_freq_alias(self.cfg.runtime.freq_5m),
            "5m",
            "300s",
            "5min",
        }
        trigger_symbols = set()
        for bar in bars:
            symbol = getattr(bar, "symbol", None)
            freq = str(getattr(bar, "frequency", "")).lower()
            freq_norm = _normalize_freq_alias(freq)
            if not symbol:
                continue
            if symbol not in self.runtime.symbol_to_csymbol:
                continue
            if freq and freq not in allowed_5m and freq_norm not in allowed_5m:
                continue
            trigger_symbols.add(symbol)

        for symbol in sorted(trigger_symbols):
            csymbol = self.runtime.symbol_to_csymbol.get(symbol)
            if not csymbol:
                continue
            state = self.runtime.states_by_csymbol.get(csymbol)
            if state is None:
                continue
            if state.main_symbol != symbol:
                continue
            self.process_symbol_on_5m(csymbol, state)
