from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

import pandas as pd


DAILY_REPORT_HEADER = [
    "run_id",
    "date",
    "mode",
    "equity_start",
    "equity_end",
    "equity_peak",
    "drawdown_ratio",
    "risk_state",
    "effective_risk_mult",
    "trades_count",
    "wins",
    "losses",
    "realized_pnl",
    "halt_flag",
    "notes",
]


def _read_header(path: Path) -> list[str]:
    try:
        with path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            return next(reader, [])
    except Exception:
        return []


def _write_header(path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(DAILY_REPORT_HEADER)


def _backup_existing_report(path: Path) -> None:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{path.stem}.bak_{stamp}{path.suffix}"
    backup_path = path.with_name(backup_name)
    path.replace(backup_path)


def ensure_daily_report(runtime) -> None:
    out_dir = Path(runtime.cfg.reporting.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    runtime.daily_report_path = out_dir / runtime.cfg.reporting.daily_filename

    if runtime.daily_report_path.exists():
        header = _read_header(runtime.daily_report_path)
        if header == DAILY_REPORT_HEADER:
            return
        if header:
            _backup_existing_report(runtime.daily_report_path)
        _write_header(runtime.daily_report_path)
        return

    _write_header(runtime.daily_report_path)


def append_daily_report(runtime, current_eob: object) -> None:
    ensure_daily_report(runtime)
    trade_day = pd.to_datetime(current_eob).strftime("%Y-%m-%d")
    if runtime.last_daily_report_date == trade_day:
        return

    p = runtime.portfolio_risk
    equity_end = p.current_equity if p.current_equity > 0 else (p.initial_equity if p.initial_equity > 0 else p.daily_start_equity)
    with runtime.daily_report_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                runtime.cfg.runtime.run_id,
                trade_day,
                runtime.cfg.runtime.mode,
                f"{float(p.daily_start_equity):.6f}",
                f"{float(equity_end):.6f}",
                f"{float(p.equity_peak):.6f}",
                f"{float(p.drawdown_ratio):.6f}",
                p.risk_state,
                f"{float(p.effective_risk_mult):.6f}",
                int(p.trades_count),
                int(p.wins),
                int(p.losses),
                f"{float(p.realized_pnl):.6f}",
                int(bool(p.risk_state.startswith('halt'))),
                p.notes,
            ]
        )
    runtime.last_daily_report_date = trade_day
