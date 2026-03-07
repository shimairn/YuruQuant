from __future__ import annotations

from dataclasses import dataclass

from yuruquant.core.models import InstrumentSpec
from yuruquant.core.time import normalize_frequency, parse_datetime


MINUTES_PER_DAY = 24 * 60


@dataclass(frozen=True)
class SessionWindow:
    start: str
    end: str
    start_minute: int
    end_minute: int
    gap_to_next_start_minutes: int


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
    raw_windows = [
        (start, end, parse_hhmm(start), parse_hhmm(end))
        for start, end in spec.sessions_day + spec.sessions_night
    ]
    start_minutes = [start_minute for _, _, start_minute, _ in raw_windows]
    windows: list[SessionWindow] = []
    for start, end, start_minute, end_minute in raw_windows:
        candidate_gaps = [((next_start - end_minute) % MINUTES_PER_DAY) for next_start in start_minutes]
        positive_gaps = [gap for gap in candidate_gaps if gap > 0]
        gap_to_next_start = min(positive_gaps) if positive_gaps else 0
        windows.append(
            SessionWindow(
                start=start,
                end=end,
                start_minute=start_minute,
                end_minute=end_minute,
                gap_to_next_start_minutes=gap_to_next_start,
            )
        )
    return windows


def _remaining_minutes(minute_of_day: int, window: SessionWindow) -> int | None:
    if window.start_minute <= window.end_minute:
        if window.start_minute <= minute_of_day <= window.end_minute:
            return window.end_minute - minute_of_day
        return None
    if minute_of_day >= window.start_minute:
        return (MINUTES_PER_DAY - minute_of_day) + window.end_minute
    if minute_of_day <= window.end_minute:
        return window.end_minute - minute_of_day
    return None


def current_session_window(spec: InstrumentSpec, eob: object) -> tuple[SessionWindow, int] | None:
    dt = parse_datetime(eob)
    minute_of_day = dt.hour * 60 + dt.minute
    candidates = [
        (window, remaining)
        for window in _session_windows(spec)
        for remaining in [_remaining_minutes(minute_of_day, window)]
        if remaining is not None
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda item: item[1])


def is_in_session(spec: InstrumentSpec, eob: object) -> bool:
    return current_session_window(spec, eob) is not None


def minutes_until_session_end(spec: InstrumentSpec, eob: object) -> int | None:
    current = current_session_window(spec, eob)
    return current[1] if current is not None else None


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
    current = current_session_window(spec, eob)
    if current is None:
        return False
    window, remaining = current
    if window.gap_to_next_start_minutes < max(int(min_gap_minutes), 0):
        return False
    return remaining <= max(int(buffer_bars), 0) * bar_minutes(frequency)
