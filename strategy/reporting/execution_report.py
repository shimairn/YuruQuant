from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path


EXECUTION_REPORT_HEADER = [
    "run_id",
    "ts",
    "mode",
    "csymbol",
    "symbol",
    "request_id",
    "intended_action",
    "intended_qty",
    "accepted",
    "reason",
    "event_timestamp",
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
        writer.writerow(EXECUTION_REPORT_HEADER)


def _backup_existing_report(path: Path) -> None:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{path.stem}.bak_{stamp}{path.suffix}"
    path.replace(path.with_name(backup_name))


def ensure_execution_report(runtime) -> None:
    out_dir = Path(runtime.cfg.reporting.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    runtime.execution_report_path = out_dir / runtime.cfg.reporting.execution_filename

    if runtime.execution_report_path.exists():
        header = _read_header(runtime.execution_report_path)
        if header == EXECUTION_REPORT_HEADER:
            return
        if header:
            _backup_existing_report(runtime.execution_report_path)
        _write_header(runtime.execution_report_path)
        return

    _write_header(runtime.execution_report_path)


def append_execution_report(runtime, csymbol: str, symbol: str, eob: object, results: list[object]) -> None:
    if not results:
        return
    ensure_execution_report(runtime)
    with runtime.execution_report_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for item in results:
            writer.writerow(
                [
                    runtime.cfg.runtime.run_id,
                    str(eob),
                    runtime.cfg.runtime.mode,
                    csymbol,
                    symbol,
                    getattr(item, "request_id", ""),
                    getattr(item, "intended_action", ""),
                    int(getattr(item, "intended_qty", 0) or 0),
                    int(bool(getattr(item, "accepted", False))),
                    getattr(item, "reason", ""),
                    getattr(item, "timestamp", ""),
                ]
            )
