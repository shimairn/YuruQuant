from __future__ import annotations

from datetime import datetime, timezone


def parse_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value or "").strip()
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
    try:
        return parse_datetime(left) > parse_datetime(right)
    except Exception:
        return str(left) > str(right)


def to_trade_day(value: object) -> str:
    try:
        return parse_datetime(value).strftime("%Y-%m-%d")
    except Exception:
        text = str(value or "").replace("T", " ")
        return text[:10] if len(text) >= 10 else text


def make_event_id(prefix: str, eob: object) -> str:
    stamp = parse_datetime(eob).strftime("%Y%m%d_%H%M%S")
    return f"{prefix}-{stamp}"
