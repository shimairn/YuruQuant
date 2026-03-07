from __future__ import annotations

import logging
from typing import Any


_LOGGER = logging.getLogger("yuruquant")
_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
}


def configure(level: str = "WARN", sample_every_n: int = 50) -> None:
    _ = sample_every_n
    target = _LEVELS.get(str(level).upper(), logging.WARNING)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=target, format="%(asctime)s %(levelname)s %(message)s")
    _LOGGER.setLevel(target)


def _emit(level: int, event: str, **kwargs: Any) -> None:
    fields = " ".join(f"{key}={value}" for key, value in sorted(kwargs.items()))
    message = event if not fields else f"{event} {fields}"
    _LOGGER.log(level, message)


def debug(event: str, **kwargs: Any) -> None:
    _emit(logging.DEBUG, event, **kwargs)


def info(event: str, **kwargs: Any) -> None:
    _emit(logging.INFO, event, **kwargs)


def warn(event: str, **kwargs: Any) -> None:
    _emit(logging.WARNING, event, **kwargs)


def error(event: str, **kwargs: Any) -> None:
    _emit(logging.ERROR, event, **kwargs)
