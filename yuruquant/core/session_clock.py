from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

from yuruquant.core.models import InstrumentSpec
from yuruquant.core.time import exchange_datetime, normalize_frequency, to_exchange_trade_day


MINUTES_PER_DAY = 24 * 60


@dataclass(frozen=True)
class SessionWindow:
    start: str
    end: str
    start_minute: int
    end_minute: int


@dataclass(frozen=True)
class SessionSnapshot:
    window: SessionWindow
    start_dt: datetime
    end_dt: datetime
    next_start_dt: datetime
    remaining_minutes: int
    gap_to_next_start_minutes: int
    current_trade_day: str
    next_trade_day: str


def parse_hhmm(value: str) -> int:
    hh, mm = value.split(':', 1)
    return int(hh) * 60 + int(mm)


def bar_minutes(frequency: str) -> int:
    normalized = normalize_frequency(frequency)
    if normalized.endswith('m') and normalized[:-1].isdigit():
        return max(int(normalized[:-1]), 1)
    raw = str(frequency or '').strip().lower()
    if raw.endswith('s') and raw[:-1].isdigit():
        return max(int(raw[:-1]) // 60, 1)
    return 5


def _session_windows(spec: InstrumentSpec) -> list[SessionWindow]:
    return [
        SessionWindow(
            start=start,
            end=end,
            start_minute=parse_hhmm(start),
            end_minute=parse_hhmm(end),
        )
        for start, end in [*spec.sessions_day, *spec.sessions_night]
    ]


def _combine(day: date, minute_of_day: int) -> datetime:
    hour, minute = divmod(minute_of_day % MINUTES_PER_DAY, 60)
    return datetime.combine(day, time(hour=hour, minute=minute))


def _is_weekend(day: date) -> bool:
    return day.weekday() >= 5


def _session_start_allowed(window: SessionWindow, day: date) -> bool:
    if window.start_minute <= window.end_minute:
        return not _is_weekend(day)
    return day.weekday() != 5


def _window_bounds(window: SessionWindow, current_dt: datetime) -> tuple[datetime, datetime] | None:
    minute_of_day = current_dt.hour * 60 + current_dt.minute
    current_day = current_dt.date()
    if window.start_minute <= window.end_minute:
        start_dt = _combine(current_day, window.start_minute)
        end_dt = _combine(current_day, window.end_minute)
        if start_dt <= current_dt <= end_dt:
            return start_dt, end_dt
        return None
    if minute_of_day >= window.start_minute:
        start_dt = _combine(current_day, window.start_minute)
        end_dt = _combine(current_day + timedelta(days=1), window.end_minute)
        if start_dt <= current_dt <= end_dt:
            return start_dt, end_dt
        return None
    if minute_of_day <= window.end_minute:
        start_dt = _combine(current_day - timedelta(days=1), window.start_minute)
        end_dt = _combine(current_day, window.end_minute)
        if start_dt <= current_dt <= end_dt:
            return start_dt, end_dt
    return None


def _next_session_start(spec: InstrumentSpec, session_end_dt: datetime) -> datetime:
    candidate_starts: list[datetime] = []
    base_day = session_end_dt.date()
    for offset in range(-1, 8):
        day = base_day + timedelta(days=offset)
        for window in _session_windows(spec):
            if not _session_start_allowed(window, day):
                continue
            candidate = _combine(day, window.start_minute)
            if candidate > session_end_dt:
                candidate_starts.append(candidate)
    if not candidate_starts:
        fallback_day = base_day + timedelta(days=1)
        while _is_weekend(fallback_day):
            fallback_day += timedelta(days=1)
        day_starts = [window.start_minute for window in _session_windows(spec) if window.start_minute <= window.end_minute]
        night_starts = [window.start_minute for window in _session_windows(spec) if window.start_minute > window.end_minute]
        first_start = min(day_starts or night_starts or [0])
        return _combine(fallback_day, first_start)
    return min(candidate_starts)


def current_session_snapshot(spec: InstrumentSpec, eob: object) -> SessionSnapshot | None:
    dt = exchange_datetime(eob).replace(second=0, microsecond=0)
    snapshots: list[SessionSnapshot] = []
    for window in _session_windows(spec):
        bounds = _window_bounds(window, dt)
        if bounds is None:
            continue
        start_dt, end_dt = bounds
        next_start_dt = _next_session_start(spec, end_dt)
        remaining_minutes = max(int((end_dt - dt).total_seconds() // 60), 0)
        gap_minutes = max(int((next_start_dt - end_dt).total_seconds() // 60), 0)
        snapshots.append(
            SessionSnapshot(
                window=window,
                start_dt=start_dt,
                end_dt=end_dt,
                next_start_dt=next_start_dt,
                remaining_minutes=remaining_minutes,
                gap_to_next_start_minutes=gap_minutes,
                current_trade_day=to_exchange_trade_day(dt),
                next_trade_day=to_exchange_trade_day(next_start_dt),
            )
        )
    if not snapshots:
        return None
    return min(snapshots, key=lambda item: item.remaining_minutes)


def current_session_window(spec: InstrumentSpec, eob: object) -> tuple[SessionWindow, int] | None:
    snapshot = current_session_snapshot(spec, eob)
    if snapshot is None:
        return None
    return snapshot.window, snapshot.remaining_minutes


def is_in_session(spec: InstrumentSpec, eob: object) -> bool:
    return current_session_snapshot(spec, eob) is not None


def minutes_until_session_end(spec: InstrumentSpec, eob: object) -> int | None:
    snapshot = current_session_snapshot(spec, eob)
    return snapshot.remaining_minutes if snapshot is not None else None


def blocked_by_session_end(spec: InstrumentSpec, eob: object, frequency: str, buffer_bars: int) -> bool:
    if max(int(buffer_bars), 0) <= 0:
        return False
    remaining = minutes_until_session_end(spec, eob)
    if remaining is None:
        return False
    return remaining <= max(int(buffer_bars), 0) * bar_minutes(frequency)


def major_session_end_approaching(
    spec: InstrumentSpec,
    eob: object,
    frequency: str,
    buffer_bars: int,
    min_gap_minutes: int,
) -> bool:
    if max(int(buffer_bars), 0) <= 0 or max(int(min_gap_minutes), 0) <= 0:
        return False
    snapshot = current_session_snapshot(spec, eob)
    if snapshot is None:
        return False
    if snapshot.gap_to_next_start_minutes < max(int(min_gap_minutes), 0):
        return False
    return snapshot.remaining_minutes <= max(int(buffer_bars), 0) * bar_minutes(frequency)


def session_end_crosses_trade_day(spec: InstrumentSpec, eob: object) -> bool:
    snapshot = current_session_snapshot(spec, eob)
    if snapshot is None:
        return False
    return snapshot.current_trade_day != snapshot.next_trade_day


def trading_day_end_approaching(spec: InstrumentSpec, eob: object, frequency: str, buffer_bars: int) -> bool:
    if max(int(buffer_bars), 0) <= 0:
        return False
    snapshot = current_session_snapshot(spec, eob)
    if snapshot is None:
        return False
    if snapshot.current_trade_day == snapshot.next_trade_day:
        return False
    return snapshot.remaining_minutes <= max(int(buffer_bars), 0) * bar_minutes(frequency)


__all__ = [
    'MINUTES_PER_DAY',
    'SessionSnapshot',
    'SessionWindow',
    'bar_minutes',
    'blocked_by_session_end',
    'current_session_snapshot',
    'current_session_window',
    'is_in_session',
    'major_session_end_approaching',
    'minutes_until_session_end',
    'parse_hhmm',
    'session_end_crosses_trade_day',
    'trading_day_end_approaching',
]
