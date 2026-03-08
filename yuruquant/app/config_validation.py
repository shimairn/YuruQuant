from __future__ import annotations

from copy import deepcopy
from typing import Any

from yuruquant.core.models import InstrumentSpec

from yuruquant.app.config_defaults import INSTRUMENT_KEYS, SESSIONS_KEYS


def as_dict(value: object) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f'expected dict but got {type(value).__name__}')
    return dict(value)


def merge_defaults(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in override.items():
        if isinstance(result.get(key), dict) and isinstance(value, dict):
            result[key] = merge_defaults(result[key], value)
        else:
            result[key] = value
    return result


def reject_unknown(section: str, payload: dict[str, Any], allowed: set[str]) -> None:
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise ValueError(f"unknown keys under {section}: {', '.join(unknown)}")


def parse_sessions(path: str, payload: dict[str, Any]) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    reject_unknown(f'{path}.sessions', payload, SESSIONS_KEYS)

    def pairs(items: object) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        for item in list(items or []):
            if not isinstance(item, (list, tuple)) or len(item) != 2:
                raise ValueError(f'{path}.sessions entries must be [start, end]')
            start, end = str(item[0]).strip(), str(item[1]).strip()
            if not start or not end:
                raise ValueError(f'{path}.sessions entries must be non-empty')
            out.append((start, end))
        return out

    return pairs(payload.get('day')), pairs(payload.get('night'))


def parse_instrument(path: str, payload: dict[str, Any]) -> InstrumentSpec:
    reject_unknown(path, payload, INSTRUMENT_KEYS)
    day, night = parse_sessions(path, as_dict(payload.get('sessions', {})))
    return InstrumentSpec(
        multiplier=float(payload['multiplier']),
        min_tick=float(payload['min_tick']),
        min_lot=max(int(payload['min_lot']), 1),
        lot_step=max(int(payload['lot_step']), 1),
        sessions_day=day,
        sessions_night=night,
    )


__all__ = ['as_dict', 'merge_defaults', 'parse_instrument', 'parse_sessions', 'reject_unknown']
