from __future__ import annotations

from strategy.adapters.gm.orders import execute_signal
from strategy.core.time_utils import is_after
from strategy.pipelines.risk import seed_position_risk_from_entry_signal
from strategy.reporting.execution_report import append_execution_report
from strategy.reporting.trade_report import append_trade_report
from strategy.types import SymbolState, TradingSignal
from strategy.observability.log import warn


def queue_signal_for_next_bar(state: SymbolState, signal: TradingSignal, current_eob: object) -> None:
    state.pending_signal = signal
    state.pending_signal_eob = current_eob


def signal_due(current_eob: object, signal_eob: object | None) -> bool:
    if signal_eob is None:
        return False
    return is_after(current_eob, signal_eob)


def _extract_accepted(item: object) -> bool | None:
    accepted = getattr(item, "accepted", None)
    if accepted is None and isinstance(item, dict):
        accepted = item.get("accepted")
    return accepted if accepted in {True, False} else None


def _all_accepted(results: list[object] | None) -> bool:
    if not results:
        return True
    return all(_extract_accepted(item) is not False for item in results)


def _any_accepted(results: list[object] | None) -> bool:
    if not results:
        return True
    return any(_extract_accepted(item) is True for item in results)


def flush_due_pending_signal(engine, state: SymbolState, csymbol: str, symbol: str, current_eob: object) -> None:
    if state.pending_signal is None:
        return
    if not signal_due(current_eob, state.pending_signal_eob):
        return

    sig = state.pending_signal
    sig.created_eob = current_eob
    execution_results = execute_signal(engine.context, state, sig, symbol)

    if engine.cfg.reporting.enabled:
        append_execution_report(engine.runtime, csymbol, symbol, current_eob, execution_results)

    accepted = _any_accepted(execution_results) if sig.action in {"buy", "sell"} else _all_accepted(execution_results)

    if sig.action in {"buy", "sell"} and accepted:
        seed_position_risk_from_entry_signal(engine.runtime, state, sig, float(sig.price), current_eob)

    if sig.action.startswith("close") and not accepted:
        warn("execution.close_rejected", csymbol=csymbol, symbol=symbol, action=sig.action)

    if accepted:
        engine._on_trade_executed(sig)
        if sig.action.startswith("close"):
            state.position_risk = None

    if engine.cfg.reporting.enabled:
        append_trade_report(engine.runtime, csymbol, symbol, current_eob, sig)

    state.pending_signal = None
    state.pending_signal_eob = None


def queue_risk_signal_if_needed(
    state: SymbolState,
    signal: TradingSignal | None,
    current_eob: object,
) -> bool:
    if signal is None or signal.action == "none":
        return False
    if state.last_risk_signal_eob is not None and not is_after(current_eob, state.last_risk_signal_eob):
        return False
    state.last_risk_signal_eob = current_eob
    return True
