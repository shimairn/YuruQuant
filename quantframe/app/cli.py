from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CLIArgs:
    config: Path
    mode: str | None
    run_id: str | None
    token: str | None
    strategy_id: str | None
    serv_addr: str | None


def parse_args(argv: list[str] | None = None) -> CLIArgs:
    parser = argparse.ArgumentParser(description="Quant framework bootstrap entrypoint.")
    parser.add_argument("--config", default="resources/configs/gm_turtle_breakout.yaml")
    parser.add_argument("--mode", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--token", default=None)
    parser.add_argument("--strategy-id", default=None)
    parser.add_argument("--serv-addr", default=None)
    args = parser.parse_args(argv)
    return CLIArgs(
        config=Path(args.config),
        mode=str(args.mode).strip().upper() if args.mode else None,
        run_id=str(args.run_id).strip() if args.run_id else None,
        token=str(args.token).strip() if args.token else None,
        strategy_id=str(args.strategy_id).strip() if args.strategy_id else None,
        serv_addr=str(args.serv_addr).strip() if args.serv_addr else None,
    )
