from __future__ import annotations

from strategy.core.bar_buffer import SymbolBarStore
from strategy.core.time_utils import to_trade_day
from strategy.reporting.daily_report import ensure_daily_report
from strategy.reporting.execution_report import ensure_execution_report
from strategy.reporting.trade_report import ensure_trade_report
from strategy.types import RuntimeContext, SymbolState


def initialize_runtime_state(runtime: RuntimeContext, cfg, context) -> None:
    runtime.context = context
    runtime.states_by_csymbol = {csymbol: SymbolState(csymbol=csymbol) for csymbol in cfg.runtime.symbols}
    runtime.symbol_to_csymbol = {}
    runtime.bar_store = {}
    runtime.last_roll_date = ""
    runtime.last_daily_report_date = ""

    if cfg.reporting.enabled:
        ensure_trade_report(runtime)
        ensure_daily_report(runtime)
        ensure_execution_report(runtime)


def ensure_symbol_store(runtime: RuntimeContext, symbol: str) -> SymbolBarStore:
    existing = runtime.bar_store.get(symbol)
    if isinstance(existing, SymbolBarStore):
        return existing

    store = SymbolBarStore.create(
        symbol=symbol,
        freq_5m=runtime.cfg.runtime.freq_5m,
        freq_1h=runtime.cfg.runtime.freq_1h,
        warmup_5m=runtime.cfg.runtime.warmup_5m,
        warmup_1h=runtime.cfg.runtime.warmup_1h,
    )
    runtime.bar_store[symbol] = store
    return store


def ensure_daily_counter(state: SymbolState, current_eob: object) -> None:
    trade_day = to_trade_day(current_eob)
    if state.daily_entry_date != trade_day:
        state.daily_entry_date = trade_day
        state.daily_entry_count = 0
