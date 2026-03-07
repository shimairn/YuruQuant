from __future__ import annotations

from datetime import datetime, timedelta, timezone


def parse_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        text = "" if value is None else str(value).strip()
        if not text:
            raise ValueError("empty datetime value")
        text = text.replace("T", " ").replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except Exception:
            dt = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def is_after(left: object, right: object) -> bool:
    return parse_datetime(left) > parse_datetime(right)


def to_trade_day(value: object) -> str:
    return parse_datetime(value).strftime("%Y-%m-%d")


def make_event_id(prefix: str, eob: object) -> str:
    stamp = parse_datetime(eob).strftime("%Y%m%d_%H%M%S")
    return f"{prefix}-{stamp}"


def normalize_frequency(value: object) -> str:
    raw = "" if value is None else str(value).strip().lower()
    aliases = {
        "5m": "5m",
        "5min": "5m",
        "300s": "5m",
        "1h": "1h",
        "60m": "1h",
        "3600s": "1h",
    }
    return aliases.get(raw, raw)


def frequency_to_timedelta(value: object) -> timedelta | None:
    normalized = normalize_frequency(value)
    if normalized == '5m':
        return timedelta(minutes=5)
    if normalized == '1h':
        return timedelta(hours=1)
    if normalized.endswith('s') and normalized[:-1].isdigit():
        return timedelta(seconds=int(normalized[:-1]))
    if normalized.endswith('m') and normalized[:-1].isdigit():
        return timedelta(minutes=int(normalized[:-1]))
    if normalized.endswith('h') and normalized[:-1].isdigit():
        return timedelta(hours=int(normalized[:-1]))
    return None

