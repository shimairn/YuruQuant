from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass
class CLIArgs:
    mode: str | None
    config: Path
    run_id: str | None
    strategy_id: str | None
    token: str | None
    serv_addr: str | None


def _normalize_mode(value: str | None) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().upper()
    return raw if raw in {'BACKTEST', 'LIVE'} else None


def _env_default(name: str, fallback: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None:
        return fallback
    stripped = str(value).strip()
    return stripped if stripped else fallback


def parse_args(argv: list[str] | None = None) -> CLIArgs:
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', default=None)
    parser.add_argument('--config', default=_env_default('STRATEGY_CONFIG', 'config/strategy.yaml'))
    parser.add_argument('--run-id', default=None)
    parser.add_argument('--strategy_id', default=None)
    parser.add_argument('--token', default=None)
    parser.add_argument('--serv_addr', default=None)
    args, _unknown = parser.parse_known_args(argv)

    raw_mode = None if args.mode is None else str(args.mode).strip()
    mode = _normalize_mode(raw_mode)
    if raw_mode and mode is None:
        parser.error(f"argument --mode: invalid value '{args.mode}' (use BACKTEST/LIVE)")

    return CLIArgs(
        mode=mode,
        config=Path(args.config),
        run_id=args.run_id,
        strategy_id=args.strategy_id,
        token=args.token,
        serv_addr=args.serv_addr,
    )


def safe_parse_args() -> CLIArgs:
    try:
        return parse_args()
    except SystemExit:
        if any(arg in {'-h', '--help'} for arg in sys.argv[1:]):
            raise
        return CLIArgs(
            mode=None,
            config=Path(_env_default('STRATEGY_CONFIG', 'config/strategy.yaml') or 'config/strategy.yaml'),
            run_id=None,
            strategy_id=None,
            token=None,
            serv_addr=None,
        )
