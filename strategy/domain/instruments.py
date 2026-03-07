from __future__ import annotations

from dataclasses import replace

from strategy.core.time_utils import parse_datetime
from strategy.types import AppConfig, InstrumentSpec


def get_instrument_spec(cfg: AppConfig, csymbol: str) -> InstrumentSpec:
    spec = cfg.instrument.symbols.get(csymbol)
    if spec is None:
        spec = cfg.instrument.defaults
    return replace(
        spec,
        volume_ratio_min=replace(spec.volume_ratio_min),
        sessions=replace(spec.sessions),
    )


def get_multiplier(cfg: AppConfig, csymbol: str) -> float:
    return float(get_instrument_spec(cfg, csymbol).multiplier)


def get_min_tick(cfg: AppConfig, csymbol: str) -> float:
    return float(get_instrument_spec(cfg, csymbol).min_tick)


def _parse_hhmm(value: str) -> int:
    hh, mm = value.split(":", 1)
    return int(hh) * 60 + int(mm)


def _in_ranges(minute_of_day: int, ranges: list[tuple[str, str]]) -> bool:
    for start, end in ranges:
        s = _parse_hhmm(start)
        e = _parse_hhmm(end)
        if s <= e:
            if s <= minute_of_day <= e:
                return True
        else:
            if minute_of_day >= s or minute_of_day <= e:
                return True
    return False


def is_in_sessions(csymbol: str, cfg: AppConfig, eob: object) -> bool:
    spec = get_instrument_spec(cfg, csymbol)
    dt = parse_datetime(eob)
    minute_of_day = dt.hour * 60 + dt.minute
    return _in_ranges(minute_of_day, spec.sessions.day) or _in_ranges(minute_of_day, spec.sessions.night)


def resolve_session_volume_ratio_min(csymbol: str, cfg: AppConfig, eob: object) -> float:
    spec = get_instrument_spec(cfg, csymbol)
    dt = parse_datetime(eob)
    minute_of_day = dt.hour * 60 + dt.minute
    if _in_ranges(minute_of_day, spec.sessions.day):
        return float(spec.volume_ratio_min.day)
    if _in_ranges(minute_of_day, spec.sessions.night):
        return float(spec.volume_ratio_min.night)
    return float(spec.volume_ratio_min.night)


def normalize_lot(qty: int, min_lot: int, lot_step: int) -> int:
    q = max(int(qty), 0)
    min_lot = max(int(min_lot), 1)
    step = max(int(lot_step), 1)
    if q < min_lot:
        return 0
    return q - ((q - min_lot) % step)
