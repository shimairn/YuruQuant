from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path


TRADE_REPORT_HEADER = [
    "run_id",
    "ts",
    "mode",
    "csymbol",
    "symbol",
    "action",
    "direction",
    "qty",
    "price",
    "signal_reason",
    "risk_stage",
    "entry_atr",
    "stop_loss",
    "take_profit",
    "est_cost",
    "gross_pnl",
    "net_pnl",
    "campaign_id",
    "holding_bars",
    "mfe_r",
    "daily_stopout_count",
    "atr_pause_flag",
]


def _write_header(path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(TRADE_REPORT_HEADER)


def _read_header(path: Path) -> list[str]:
    try:
        with path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            return next(reader, [])
    except Exception:
        return []


def _backup_existing_report(path: Path) -> None:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{path.stem}.bak_{stamp}{path.suffix}"
    backup_path = path.with_name(backup_name)
    path.replace(backup_path)


def ensure_trade_report(runtime) -> None:
    out_dir = Path(runtime.cfg.reporting.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    runtime.trade_report_path = out_dir / runtime.cfg.reporting.trade_filename

    if runtime.trade_report_path.exists():
        header = _read_header(runtime.trade_report_path)
        if header == TRADE_REPORT_HEADER:
            return
        if header:
            _backup_existing_report(runtime.trade_report_path)
        _write_header(runtime.trade_report_path)
        return

    _write_header(runtime.trade_report_path)


def append_trade_report(runtime, csymbol: str, symbol: str, eob: object, signal) -> None:
    ensure_trade_report(runtime)
    with runtime.trade_report_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                runtime.cfg.runtime.run_id,
                str(eob),
                runtime.cfg.runtime.mode,
                csymbol,
                symbol,
                signal.action,
                int(signal.direction),
                int(signal.qty),
                f"{float(signal.price):.6f}",
                signal.reason,
                signal.risk_stage,
                f"{float(signal.entry_atr):.6f}",
                f"{float(signal.stop_loss):.6f}",
                f"{float(signal.take_profit):.6f}",
                f"{float(signal.est_cost):.6f}",
                f"{float(signal.gross_pnl):.6f}",
                f"{float(signal.net_pnl):.6f}",
                signal.campaign_id,
                int(getattr(signal, "holding_bars", 0)),
                f"{float(getattr(signal, 'mfe_r', 0.0)):.6f}",
                int(getattr(signal, "daily_stopout_count", 0)),
                int(getattr(signal, "atr_pause_flag", 0)),
            ]
        )
