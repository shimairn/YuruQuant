from __future__ import annotations

import os
from datetime import datetime, timedelta

from strategy.core.time_utils import parse_datetime
from strategy.observability.log import info, warn


def permission_min_start(now: datetime | None = None) -> datetime:
    ref = parse_datetime(now or datetime.now())
    today = datetime(ref.year, ref.month, ref.day)
    return today - timedelta(days=180)


def clip_backtest_window_if_needed(cfg) -> None:
    if str(cfg.runtime.mode).upper() != "BACKTEST":
        return

    local_data_root = str(os.getenv("LOCAL_DATA_ROOT", "")).strip()
    max_days = int(getattr(cfg.gm, "backtest_max_days", 180) or 180)
    if max_days <= 0:
        max_days = 180

    permission_floor = None if local_data_root else permission_min_start()

    try:
        start = parse_datetime(cfg.gm.backtest_start)
        end = parse_datetime(cfg.gm.backtest_end)
    except Exception as exc:
        warn("gm.backtest_window.parse_failed", err=exc)
        return

    if permission_floor is not None and start < permission_floor:
        warn(
            "gm.backtest_window.start_clipped",
            start_original=start,
            start_clipped=permission_floor,
        )
        start = permission_floor

    if start >= end:
        end = parse_datetime(datetime.now()).replace(second=0, microsecond=0)
        start = end - timedelta(days=max_days)
        if permission_floor is not None:
            start = max(start, permission_floor)
        cfg.gm.backtest_start = start.strftime("%Y-%m-%d %H:%M:%S")
        cfg.gm.backtest_end = end.strftime("%Y-%m-%d %H:%M:%S")
        warn(
            "gm.backtest_window.reset",
            backtest_start=cfg.gm.backtest_start,
            backtest_end=cfg.gm.backtest_end,
            max_days=max_days,
        )
        return

    span_days = (end - start).total_seconds() / 86400.0
    if span_days > max_days:
        start = end - timedelta(days=max_days)
        if permission_floor is not None:
            start = max(start, permission_floor)
        cfg.gm.backtest_start = start.strftime("%Y-%m-%d %H:%M:%S")
        cfg.gm.backtest_end = end.strftime("%Y-%m-%d %H:%M:%S")
        info(
            "gm.backtest_window.clipped",
            backtest_start=cfg.gm.backtest_start,
            backtest_end=cfg.gm.backtest_end,
            max_days=max_days,
        )
        return

    cfg.gm.backtest_start = start.strftime("%Y-%m-%d %H:%M:%S")
    cfg.gm.backtest_end = end.strftime("%Y-%m-%d %H:%M:%S")
