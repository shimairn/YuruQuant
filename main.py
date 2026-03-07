from __future__ import annotations

import argparse
import os
import re
from datetime import datetime
from pathlib import Path

from strategy.config_loader import load_config
from strategy.gm.callbacks import build_gm_callbacks
from strategy.engine import StrategyEngine


_GM_CALLBACKS = None


def _normalize_mode(value: str | None) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().upper()
    numeric_mapping = {
        "1": "BACKTEST",
        "2": "LIVE",
    }
    try:
        from gm.api import MODE_BACKTEST, MODE_LIVE  # type: ignore

        numeric_mapping = {
            str(int(MODE_BACKTEST)): "BACKTEST",
            str(int(MODE_LIVE)): "LIVE",
        }
    except Exception:
        pass
    mapping = {
        **numeric_mapping,
        "BACKTEST": "BACKTEST",
        "LIVE": "LIVE",
        "BT": "BACKTEST",
        "LV": "LIVE",
    }
    return mapping.get(raw)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", default=None, help="BACKTEST/LIVE or legacy 1/2")
    p.add_argument("--config", default="config/strategy.yaml")
    p.add_argument("--run-id", default=None, help="Override run_id (default: auto-timestamped)")
    # GM launcher may inject extra args.
    # Keep these optional; prefer env vars / config to avoid leaking credentials in code.
    p.add_argument("--strategy_id", default=None)
    p.add_argument("--token", default=None)
    p.add_argument("--serv_addr", default=None)
    args, _unknown = p.parse_known_args()

    normalized = _normalize_mode(args.mode)
    if args.mode is not None and normalized is None:
        p.error(f"argument --mode: invalid value '{args.mode}' (use BACKTEST/LIVE or 1/2)")
    args.mode = normalized
    return args


def _append_run_id_timestamp_if_needed(runtime) -> None:
    """如果run_id没有时间戳后缀，自动追加（确保每次运行报告独立）"""
    current = getattr(runtime, "run_id", "run_001")
    if not current:
        current = "run_001"

    # 检测是否已包含时间戳格式 (run_001_20250214_153022)
    if re.search(r"_\d{8}_\d{6}$", current):
        return

    # 追加时间戳
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = str(current).strip().rstrip("_")
    if not base:
        base = "run_001"
    runtime.run_id = f"{base}_{ts}"
    print(f"run_id auto-timestamped: {runtime.run_id}")


def _safe_parse_args() -> argparse.Namespace:
    """Best-effort parse for callback-driven startup paths."""
    try:
        return _parse_args()
    except SystemExit:
        return argparse.Namespace(
            mode=None,
            config=os.getenv("STRATEGY_CONFIG", "config/strategy.yaml"),
            strategy_id=None,
            token=None,
            serv_addr=None,
            run_id=None,
        )


def _apply_overrides(cfg, args: argparse.Namespace) -> None:
    if args.mode is not None:
        cfg.runtime.mode = args.mode
    if args.token:
        cfg.gm.token = str(args.token).strip()
    if args.strategy_id:
        cfg.gm.strategy_id = str(args.strategy_id).strip()
    if args.serv_addr:
        cfg.gm.serv_addr = str(args.serv_addr).strip()

    if not cfg.gm.token:
        cfg.gm.token = os.getenv("GM_TOKEN", "").strip()
    if not cfg.gm.strategy_id:
        cfg.gm.strategy_id = os.getenv("GM_STRATEGY_ID", "").strip()
    if not cfg.gm.serv_addr:
        cfg.gm.serv_addr = os.getenv("GM_SERV_ADDR", "").strip()
    if not getattr(cfg, "runtime", None):
        return
    if not cfg.runtime.mode:
        cfg.runtime.mode = os.getenv("GM_MODE", "BACKTEST").strip().upper() or "BACKTEST"

    # 处理 --run-id 参数：用户指定则直接使用，否则自动追加时间戳
    if hasattr(args, "run_id") and args.run_id is not None:
        cfg.runtime.run_id = str(args.run_id).strip()
    else:
        _append_run_id_timestamp_if_needed(cfg.runtime)


def _ensure_callbacks_initialized() -> None:
    global _GM_CALLBACKS
    if _GM_CALLBACKS is not None:
        return

    args = _safe_parse_args()
    cfg_path = Path(getattr(args, "config", None) or os.getenv("STRATEGY_CONFIG", "config/strategy.yaml"))
    cfg = load_config(cfg_path)
    _apply_overrides(cfg, args)

    engine = StrategyEngine(cfg)
    _GM_CALLBACKS = build_gm_callbacks(engine)


def _dispatch(name: str, *args):
    _ensure_callbacks_initialized()
    fn = getattr(_GM_CALLBACKS, name)
    return fn(*args)


# Top-level callbacks for older GM SDK versions.
def init(context):
    return _dispatch("init", context)


def on_bar(context, bars):
    return _dispatch("on_bar", context, bars)


def on_order_status(context, order):
    return _dispatch("on_order_status", context, order)


def on_execution_report(context, execrpt):
    return _dispatch("on_execution_report", context, execrpt)


def on_error(context, code, info):
    return _dispatch("on_error", context, code, info)


def main() -> int:
    global _GM_CALLBACKS

    args = _safe_parse_args()
    cfg = load_config(Path(args.config))

    # Keep config path for callback-only startup process.
    os.environ["STRATEGY_CONFIG"] = str(Path(args.config))
    _apply_overrides(cfg, args)

    engine = StrategyEngine(cfg)
    callbacks = build_gm_callbacks(engine)
    _GM_CALLBACKS = callbacks

    callbacks.run_gm()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
