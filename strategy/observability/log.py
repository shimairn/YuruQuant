from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from .rate_limit import should_emit


_LEVELS = {
    "DEBUG": 10,
    "INFO": 20,
    "WARN": 30,
    "WARNING": 30,
    "ERROR": 40,
}

_STATE = {
    "level": "WARN",
    "sample_every_n": 50,
}


def _coerce_level(value: str | None) -> str:
    raw = str(value or "").strip().upper()
    if raw in _LEVELS:
        return "WARN" if raw == "WARNING" else raw
    return "WARN"


def configure(level: str | None = None, sample_every_n: int | None = None) -> None:
    env_level = os.getenv("STRATEGY_LOG_LEVEL", "")
    env_sample = os.getenv("STRATEGY_LOG_SAMPLE_EVERY", "")

    if level is None and env_level:
        level = env_level
    if sample_every_n is None and env_sample.strip():
        try:
            sample_every_n = int(env_sample)
        except Exception:
            sample_every_n = None

    if level is not None:
        _STATE["level"] = _coerce_level(level)
    if sample_every_n is not None:
        _STATE["sample_every_n"] = max(int(sample_every_n), 1)


def _enabled(level: str) -> bool:
    current = _LEVELS[_STATE["level"]]
    target = _LEVELS[_coerce_level(level)]
    return target >= current


def _format_fields(fields: dict[str, Any]) -> str:
    if not fields:
        return ""
    parts: list[str] = []
    for key, value in fields.items():
        parts.append(f"{key}={value}")
    return " " + " ".join(parts)


def log(level: str, message: str, *, sample_key: str | None = None, sample_every_n: int | None = None, **fields: Any) -> None:
    lvl = _coerce_level(level)
    if not _enabled(lvl):
        return
    if sample_key:
        every = max(int(sample_every_n or _STATE["sample_every_n"]), 1)
        if not should_emit(sample_key, every):
            return
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"{ts} {lvl} {message}{_format_fields(fields)}")


def debug(message: str, **fields: Any) -> None:
    log("DEBUG", message, **fields)


def info(message: str, **fields: Any) -> None:
    log("INFO", message, **fields)


def warn(message: str, **fields: Any) -> None:
    log("WARN", message, **fields)


def error(message: str, **fields: Any) -> None:
    log("ERROR", message, **fields)

