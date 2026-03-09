from __future__ import annotations

from copy import deepcopy
from typing import Any

from yuruquant.app.config_schema import RiskThrottleStep
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


def parse_risk_clusters(path: str, payload: object, allowed_symbols: set[str]) -> dict[str, tuple[str, ...]]:
    raw_clusters = as_dict(payload)
    clusters: dict[str, tuple[str, ...]] = {}
    for raw_name, raw_members in raw_clusters.items():
        cluster_name = str(raw_name).strip()
        if not cluster_name:
            raise ValueError(f'{path} cluster names must be non-empty')
        if not isinstance(raw_members, list):
            raise ValueError(f'{path}.{cluster_name} must be a list of symbols')
        members: list[str] = []
        for raw_member in raw_members:
            csymbol = str(raw_member).strip()
            if not csymbol:
                raise ValueError(f'{path}.{cluster_name} members must be non-empty strings')
            if csymbol not in allowed_symbols:
                raise ValueError(f'{path}.{cluster_name} references unknown symbol {csymbol}')
            members.append(csymbol)
        if not members:
            raise ValueError(f'{path}.{cluster_name} must include at least one symbol')
        if len(set(members)) != len(members):
            raise ValueError(f'{path}.{cluster_name} contains duplicate symbols')
        clusters[cluster_name] = tuple(members)
    return clusters


def parse_drawdown_risk_schedule(path: str, payload: object) -> tuple[RiskThrottleStep, ...]:
    if payload is None:
        return ()
    if not isinstance(payload, list):
        raise ValueError(f'{path} must be a list of schedule steps')
    allowed_keys = {'drawdown_ratio', 'risk_mult'}
    steps: list[RiskThrottleStep] = []
    previous_ratio = -1.0
    for index, raw_step in enumerate(payload):
        step = as_dict(raw_step)
        reject_unknown(f'{path}[{index}]', step, allowed_keys)
        ratio = float(step.get('drawdown_ratio', 0.0))
        risk_mult = float(step.get('risk_mult', -1.0))
        if ratio <= 0.0:
            raise ValueError(f'{path}[{index}].drawdown_ratio must be > 0')
        if risk_mult < 0.0 or risk_mult > 1.0:
            raise ValueError(f'{path}[{index}].risk_mult must be between 0 and 1')
        if ratio <= previous_ratio:
            raise ValueError(f'{path} must be strictly increasing by drawdown_ratio')
        previous_ratio = ratio
        steps.append(RiskThrottleStep(drawdown_ratio=ratio, risk_mult=risk_mult))
    return tuple(steps)


def parse_positive_int_sequence(path: str, payload: object) -> tuple[int, ...]:
    if payload is None:
        return ()
    if not isinstance(payload, list):
        raise ValueError(f'{path} must be a list of integers')
    values: list[int] = []
    previous = 0
    for index, raw_value in enumerate(payload):
        value = int(raw_value)
        if value <= 0:
            raise ValueError(f'{path}[{index}] must be > 0')
        if value <= previous:
            raise ValueError(f'{path} must be strictly increasing')
        previous = value
        values.append(value)
    return tuple(values)


__all__ = [
    'as_dict',
    'merge_defaults',
    'parse_positive_int_sequence',
    'parse_drawdown_risk_schedule',
    'parse_instrument',
    'parse_risk_clusters',
    'parse_sessions',
    'reject_unknown',
]
