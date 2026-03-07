from __future__ import annotations

from datetime import datetime, timedelta

from yuruquant.core.models import EntrySignal, ExecutionDiagnostics, ExitSignal, InstrumentSpec, ManagedPosition, Signal
from yuruquant.core.time import parse_datetime


EXCHANGE_UTC_OFFSET_HOURS = 8
SESSION_RESTART_WINDOW_MINUTES = 15
SESSION_RESTART_GAP_ATR_THRESHOLD = 0.25
STOP_EXIT_TRIGGERS = {'hard_stop', 'protected_stop'}
SESSION_RESTART_GAP_TRIGGERS = STOP_EXIT_TRIGGERS | {'portfolio_halt'}


def _exchange_datetime(value: object) -> datetime:
    return parse_datetime(value) + timedelta(hours=EXCHANGE_UTC_OFFSET_HOURS)


def _parse_session_start(value: str) -> tuple[int, int]:
    hour_text, minute_text = str(value).split(':', maxsplit=1)
    return int(hour_text), int(minute_text)


def is_session_restart_fill(fill_ts: object, spec: InstrumentSpec, window_minutes: int = SESSION_RESTART_WINDOW_MINUTES) -> bool:
    if fill_ts is None:
        return False
    local_dt = _exchange_datetime(fill_ts)
    session_starts = [start for start, _ in spec.sessions_day] + [start for start, _ in spec.sessions_night]
    for start in session_starts:
        hour, minute = _parse_session_start(start)
        session_open = local_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
        delta_minutes = (local_dt - session_open).total_seconds() / 60.0
        if 0.0 <= delta_minutes <= max(float(window_minutes), 0.0):
            return True
    return False


def _entry_atr(signal: Signal, position: ManagedPosition | None) -> float:
    if isinstance(signal, EntrySignal):
        return max(float(signal.entry_atr), 0.0)
    if isinstance(signal, ExitSignal) and position is not None:
        return max(float(position.entry_atr), 0.0)
    return 0.0


def _stop_breach_atr(signal: Signal, position: ManagedPosition | None, entry_atr: float) -> float:
    if not isinstance(signal, ExitSignal) or position is None or entry_atr <= 0:
        return 0.0
    if signal.exit_trigger not in STOP_EXIT_TRIGGERS:
        return 0.0
    breach_points = abs(float(signal.price) - float(position.stop_loss))
    return breach_points / entry_atr if breach_points > 0 else 0.0


def _portfolio_halt_gap_atr(signal: Signal, entry_atr: float, fill_price: float) -> float:
    if not isinstance(signal, ExitSignal) or entry_atr <= 0:
        return 0.0
    if signal.exit_trigger != 'portfolio_halt':
        return 0.0
    gap_points = abs(float(fill_price) - float(signal.price))
    return gap_points / entry_atr if gap_points > 0 else 0.0


def build_execution_diagnostics(
    signal: Signal,
    fill_ts: object,
    fill_price: float,
    spec: InstrumentSpec,
    position: ManagedPosition | None = None,
    window_minutes: int = SESSION_RESTART_WINDOW_MINUTES,
    gap_atr_threshold: float = SESSION_RESTART_GAP_ATR_THRESHOLD,
) -> ExecutionDiagnostics:
    gap_points = abs(float(fill_price) - float(signal.price))
    entry_atr = _entry_atr(signal, position)
    gap_atr = gap_points / entry_atr if entry_atr > 0 else 0.0
    regime = 'normal'
    stop_breach_atr = _stop_breach_atr(signal, position, entry_atr)
    portfolio_halt_gap_atr = _portfolio_halt_gap_atr(signal, entry_atr, fill_price)
    if (
        isinstance(signal, ExitSignal)
        and signal.exit_trigger in SESSION_RESTART_GAP_TRIGGERS
        and is_session_restart_fill(fill_ts, spec, window_minutes)
        and max(stop_breach_atr, portfolio_halt_gap_atr) >= max(float(gap_atr_threshold), 0.0)
    ):
        regime = 'session_restart_gap'
    return ExecutionDiagnostics(
        execution_regime=regime,
        fill_gap_points=gap_points,
        fill_gap_atr=gap_atr,
    )
