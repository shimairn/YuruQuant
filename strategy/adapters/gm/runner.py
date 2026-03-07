from __future__ import annotations

import inspect
import sys
from typing import Callable

from strategy.observability.log import warn

try:
    from gm.api import ADJUST_NONE, MODE_BACKTEST, MODE_LIVE, run, schedule  # type: ignore
except Exception:  # pragma: no cover
    ADJUST_NONE = 0
    MODE_BACKTEST = 1
    MODE_LIVE = 2
    run = None
    schedule = None

try:
    import gm as _gm_pkg  # type: ignore
except Exception:  # pragma: no cover
    _gm_pkg = None

_SDK_VERSION_LOGGED = False


def gm_sdk_version() -> str:
    if _gm_pkg is None:
        return ""
    value = getattr(_gm_pkg, "__version__", "") or ""
    if hasattr(value, "__version__"):
        value = getattr(value, "__version__", "") or ""
    return str(value).strip()


def _parse_version_tuple(version: str) -> tuple[int, ...]:
    parts: list[int] = []
    for token in str(version or "").strip().split("."):
        if not token:
            continue
        num = ""
        for ch in token:
            if ch.isdigit():
                num += ch
            else:
                break
        if num:
            parts.append(int(num))
        else:
            break
    return tuple(parts)


def _version_lt(left: str, right: str) -> bool:
    l = _parse_version_tuple(left)
    r = _parse_version_tuple(right)
    width = max(len(l), len(r))
    if width <= 0:
        return False
    l = l + (0,) * (width - len(l))
    r = r + (0,) * (width - len(r))
    return l < r


def log_sdk_compat_info_once() -> None:
    global _SDK_VERSION_LOGGED
    if _SDK_VERSION_LOGGED:
        return
    _SDK_VERSION_LOGGED = True

    version = gm_sdk_version()
    if not version:
        warn("gm.sdk.version_unknown")
        return

    warn("gm.sdk.version", version=version)
    if _version_lt(version, "3.0.180"):
        warn("gm.sdk.version_unsupported", current_version=version, min_version="3.0.180")


def _supports_callback_kwargs(run_fn: Callable | None) -> bool:
    if run_fn is None:
        return False
    try:
        sig = inspect.signature(run_fn)
        params = sig.parameters
        needed = {"init", "on_bar", "on_order_status", "on_execution_report", "on_error"}
        return needed.issubset(set(params.keys()))
    except Exception:
        return False


def _filter_supported_kwargs(run_fn: Callable | None, kwargs: dict) -> dict:
    if run_fn is None:
        return kwargs
    try:
        sig = inspect.signature(run_fn)
        params = sig.parameters
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            return kwargs
        return {k: v for k, v in kwargs.items() if k in params}
    except Exception:
        return kwargs


def _invoke_run_isolated(run_fn: Callable, kwargs: dict) -> None:
    original_argv = list(sys.argv)
    try:
        sys.argv = [sys.argv[0]]
        run_fn(**kwargs)
    finally:
        sys.argv = original_argv


def run_with_gm(cfg, callbacks) -> None:
    if run is None:
        raise RuntimeError("gm.api.run is unavailable; install gm package")
    if not cfg.gm.token or not cfg.gm.strategy_id:
        raise RuntimeError("GM token/strategy_id is required; set config or env")
    log_sdk_compat_info_once()

    mode = str(cfg.runtime.mode).upper()
    run_mode = MODE_LIVE if mode == "LIVE" else MODE_BACKTEST
    backtest_match_mode = int(getattr(cfg.gm, "backtest_match_mode", 0) or 0)
    backtest_initial_cash = float(getattr(cfg.gm, "backtest_initial_cash", 0.0) or 0.0)
    backtest_intraday = bool(getattr(cfg.gm, "backtest_intraday", False))

    base_kwargs = {
        "token": cfg.gm.token,
        "strategy_id": cfg.gm.strategy_id,
        "filename": "main.py",
        "mode": run_mode,
        "backtest_start_time": cfg.gm.backtest_start,
        "backtest_end_time": cfg.gm.backtest_end,
        "backtest_adjust": ADJUST_NONE,
        "backtest_commission_ratio": max(float(cfg.risk.backtest_commission_ratio), 0.0),
        "backtest_slippage_ratio": max(float(cfg.risk.backtest_slippage_ratio), 0.0),
        "backtest_match_mode": backtest_match_mode,
    }
    if backtest_initial_cash > 0:
        base_kwargs["backtest_initial_cash"] = backtest_initial_cash
    if backtest_intraday and run_mode == MODE_BACKTEST:
        base_kwargs["backtest_intraday"] = True

    serv_addr = str(getattr(cfg.gm, "serv_addr", "") or "").strip()
    if serv_addr:
        base_kwargs["serv_addr"] = serv_addr

    callback_kwargs = {
        "init": callbacks.init,
        "on_bar": callbacks.on_bar,
        "on_order_status": callbacks.on_order_status,
        "on_execution_report": callbacks.on_execution_report,
        "on_error": callbacks.on_error,
    }

    run_kwargs = _filter_supported_kwargs(run, dict(base_kwargs))
    if _supports_callback_kwargs(run):
        run_kwargs.update(callback_kwargs)
        run_kwargs = _filter_supported_kwargs(run, run_kwargs)

    try:
        _invoke_run_isolated(run, run_kwargs)
    except Exception as exc:
        if isinstance(exc, TypeError):
            msg = str(exc)
            callback_kw_error = (
                "unexpected keyword argument 'init'" in msg
                or "unexpected keyword argument 'on_bar'" in msg
                or "unexpected keyword argument 'on_order_status'" in msg
                or "unexpected keyword argument 'on_execution_report'" in msg
                or "unexpected keyword argument 'on_error'" in msg
            )
            if callback_kw_error:
                _invoke_run_isolated(run, _filter_supported_kwargs(run, dict(base_kwargs)))
                return
        raise
