from __future__ import annotations

from yuruquant.core.models import FillPolicy, Signal, SymbolRuntime
from yuruquant.core.time import is_after


class NextBarOpenFillPolicy(FillPolicy):
    def queue(self, state: SymbolRuntime, signal: Signal, current_eob: object) -> None:
        state.pending_signal = signal
        state.pending_signal_eob = current_eob

    def pop_due(self, state: SymbolRuntime, current_eob: object) -> Signal | None:
        if state.pending_signal is None or state.pending_signal_eob is None:
            return None
        if not is_after(current_eob, state.pending_signal_eob):
            return None
        signal = state.pending_signal
        state.pending_signal = None
        state.pending_signal_eob = None
        return signal
