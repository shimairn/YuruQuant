from __future__ import annotations

from datetime import date, datetime, timedelta, timezone


EXCHANGE_UTC_OFFSET_HOURS = 8
EXCHANGE_TZ = timezone(timedelta(hours=EXCHANGE_UTC_OFFSET_HOURS))
NIGHT_SESSION_TRADE_DAY_CUTOFF_HOUR = 18
WEEKEND_WEEKDAYS = {5, 6}


def _coerce_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    text = '' if value is None else str(value).strip()
    if not text:
        raise ValueError('empty datetime value')
    text = text.replace('T', ' ').replace('Z', '+00:00')
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return datetime.strptime(text[:19], '%Y-%m-%d %H:%M:%S')


def parse_datetime(value: object) -> datetime:
    dt = _coerce_datetime(value)
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def exchange_datetime(value: object) -> datetime:
    dt = _coerce_datetime(value)
    if dt.tzinfo is not None:
        return dt.astimezone(EXCHANGE_TZ).replace(tzinfo=None)
    return dt


def is_after(left: object, right: object) -> bool:
    return exchange_datetime(left) > exchange_datetime(right)


def is_weekend_day(day: date) -> bool:
    return day.weekday() in WEEKEND_WEEKDAYS


def next_exchange_trade_date(day: date) -> date:
    candidate = day + timedelta(days=1)
    while is_weekend_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def normalize_exchange_trade_date(day: date) -> date:
    candidate = day
    while is_weekend_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def to_trade_day(value: object) -> str:
    return parse_datetime(value).strftime('%Y-%m-%d')


def to_exchange_trade_day(value: object) -> str:
    dt = exchange_datetime(value)
    trade_day = dt.date()
    if trade_day.weekday() == 5:
        return next_exchange_trade_date(trade_day).isoformat()
    if trade_day.weekday() == 6 and dt.hour < NIGHT_SESSION_TRADE_DAY_CUTOFF_HOUR:
        return next_exchange_trade_date(trade_day).isoformat()
    if dt.hour >= NIGHT_SESSION_TRADE_DAY_CUTOFF_HOUR:
        return next_exchange_trade_date(trade_day).isoformat()
    return trade_day.isoformat()


def make_event_id(prefix: str, eob: object) -> str:
    stamp = exchange_datetime(eob).strftime('%Y%m%d_%H%M%S')
    return f'{prefix}-{stamp}'


def normalize_frequency(value: object) -> str:
    raw = '' if value is None else str(value).strip().lower()
    aliases = {
        '5m': '5m',
        '5min': '5m',
        '300s': '5m',
        '15m': '15m',
        '900s': '15m',
        '1h': '1h',
        '60m': '1h',
        '3600s': '1h',
    }
    return aliases.get(raw, raw)


def frequency_to_timedelta(value: object) -> timedelta | None:
    normalized = normalize_frequency(value)
    if normalized == '5m':
        return timedelta(minutes=5)
    if normalized == '15m':
        return timedelta(minutes=15)
    if normalized == '1h':
        return timedelta(hours=1)
    if normalized.endswith('s') and normalized[:-1].isdigit():
        return timedelta(seconds=int(normalized[:-1]))
    if normalized.endswith('m') and normalized[:-1].isdigit():
        return timedelta(minutes=int(normalized[:-1]))
    if normalized.endswith('h') and normalized[:-1].isdigit():
        return timedelta(hours=int(normalized[:-1]))
    return None


__all__ = [
    'EXCHANGE_TZ',
    'EXCHANGE_UTC_OFFSET_HOURS',
    'NIGHT_SESSION_TRADE_DAY_CUTOFF_HOUR',
    'WEEKEND_WEEKDAYS',
    'exchange_datetime',
    'frequency_to_timedelta',
    'is_after',
    'is_weekend_day',
    'make_event_id',
    'next_exchange_trade_date',
    'normalize_exchange_trade_date',
    'normalize_frequency',
    'parse_datetime',
    'to_exchange_trade_day',
    'to_trade_day',
]
