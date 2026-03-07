from __future__ import annotations

import copy
import os
from typing import Any

from strategy.observability.log import info as _log_info

from .schema import (
    INSTRUMENT_SESSIONS_KEYS,
    INSTRUMENT_SPEC_KEYS,
    INSTRUMENT_TOP_KEYS,
    INSTRUMENT_VOLUME_RATIO_KEYS,
    SECTION_KEYS,
    TOP_LEVEL_KEYS,
)


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _raise(msg: str) -> None:
    raise ValueError(f"config error: {msg}")


def _ensure_known_keys(path: str, payload: dict[str, Any], allowed: set[str]) -> None:
    extras = sorted(k for k in payload.keys() if k not in allowed)
    if extras:
        _raise(f"unknown field(s) in {path}: {', '.join(extras)}")


def _ensure_required_keys(path: str, payload: dict[str, Any], required: set[str]) -> None:
    missing = sorted(k for k in required if k not in payload)
    if missing:
        _raise(f"missing required field(s) in {path}: {', '.join(missing)}")


def _validate_session_ranges(path: str, ranges: Any) -> None:
    if not isinstance(ranges, list):
        _raise(f"{path} must be a list of [HH:MM, HH:MM]")
    for item in ranges:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            _raise(f"{path} item must be [HH:MM, HH:MM]")
        start, end = item
        if not isinstance(start, str) or not isinstance(end, str):
            _raise(f"{path} item must be [HH:MM, HH:MM]")
        if len(start.split(":")) != 2 or len(end.split(":")) != 2:
            _raise(f"{path} item must be [HH:MM, HH:MM]")


def _validate_instrument_spec(path: str, payload: dict[str, Any]) -> None:
    _ensure_known_keys(path, payload, INSTRUMENT_SPEC_KEYS)
    _ensure_required_keys(path, payload, INSTRUMENT_SPEC_KEYS)

    vr = _as_dict(payload.get("volume_ratio_min"))
    _ensure_known_keys(f"{path}.volume_ratio_min", vr, INSTRUMENT_VOLUME_RATIO_KEYS)
    _ensure_required_keys(f"{path}.volume_ratio_min", vr, INSTRUMENT_VOLUME_RATIO_KEYS)

    sessions = _as_dict(payload.get("sessions"))
    _ensure_known_keys(f"{path}.sessions", sessions, INSTRUMENT_SESSIONS_KEYS)
    _ensure_required_keys(f"{path}.sessions", sessions, INSTRUMENT_SESSIONS_KEYS)
    _validate_session_ranges(f"{path}.sessions.day", sessions.get("day"))
    _validate_session_ranges(f"{path}.sessions.night", sessions.get("night"))


def validate_and_normalize_root(root: dict[str, Any]) -> dict[str, Any]:
    cfg = copy.deepcopy(root if isinstance(root, dict) else {})
    _ensure_known_keys("root", cfg, TOP_LEVEL_KEYS)
    _ensure_required_keys("root", cfg, TOP_LEVEL_KEYS)

    for section_name, keys in SECTION_KEYS.items():
        section = _as_dict(cfg.get(section_name))
        cfg[section_name] = section
        _ensure_known_keys(section_name, section, keys)
        _ensure_required_keys(section_name, section, keys)

    mode = str(cfg["runtime"].get("mode", "")).strip().upper()
    if mode not in {"BACKTEST", "LIVE"}:
        _raise("runtime.mode must be BACKTEST or LIVE")
    cfg["runtime"]["mode"] = mode

    symbols = cfg["runtime"].get("symbols")
    if not isinstance(symbols, list) or not symbols:
        _raise("runtime.symbols must be a non-empty list")
    if not all(isinstance(x, str) and x.strip() for x in symbols):
        _raise("runtime.symbols must contain non-empty strings")

    obs_level = str(cfg["observability"].get("level", "")).strip().upper()
    if obs_level not in {"DEBUG", "INFO", "WARN", "ERROR"}:
        _raise("observability.level must be one of DEBUG/INFO/WARN/ERROR")
    cfg["observability"]["level"] = obs_level

    instrument = _as_dict(cfg.get("instrument"))
    cfg["instrument"] = instrument
    _ensure_known_keys("instrument", instrument, INSTRUMENT_TOP_KEYS)
    _ensure_required_keys("instrument", instrument, INSTRUMENT_TOP_KEYS)

    defaults = _as_dict(instrument.get("defaults"))
    instrument["defaults"] = defaults
    _validate_instrument_spec("instrument.defaults", defaults)

    symbols_cfg = _as_dict(instrument.get("symbols"))
    instrument["symbols"] = symbols_cfg
    for csymbol, spec in symbols_cfg.items():
        if not isinstance(spec, dict):
            _raise(f"instrument.symbols.{csymbol} must be an object")
        _validate_instrument_spec(f"instrument.symbols.{csymbol}", spec)

    return cfg


def log_credential_source(token: str, strategy_id: str) -> None:
    env_token = os.getenv("GM_TOKEN", "").strip()
    env_strategy_id = os.getenv("GM_STRATEGY_ID", "").strip()

    sources: list[str] = []
    if token or strategy_id:
        sources.append("yaml")
    if env_token or env_strategy_id:
        sources.append("env")
    source = "+".join(sources) if sources else "none"
    _log_info("config credential source", source=source)
