from __future__ import annotations

import csv
from pathlib import Path


def to_float(value: object, default: float = 0.0) -> float:
    try:
        text = '' if value is None else str(value).strip()
        return float(text) if text else default
    except Exception:
        return default


def to_int(value: object, default: int = 0) -> int:
    try:
        text = '' if value is None else str(value).strip()
        return int(float(text)) if text else default
    except Exception:
        return default


def normalize_optional(value: object) -> str:
    return '' if value is None else str(value).strip()


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open('r', newline='', encoding='utf-8-sig') as handle:
        return list(csv.DictReader(handle))


__all__ = ['load_csv_rows', 'normalize_optional', 'to_float', 'to_int']
