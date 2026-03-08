from __future__ import annotations

import inspect
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

from yuruquant.app.config_schema import AppConfig
from yuruquant.core.time import parse_datetime
from yuruquant.reporting.logging import warn

try:
    from gm.api import ADJUST_NONE, MODE_BACKTEST, MODE_LIVE, run, schedule  # type: ignore
except Exception:  # pragma: no cover
    ADJUST_NONE = 0
    MODE_BACKTEST = 1
    MODE_LIVE = 2
    run = None
    schedule = None


_ENTRYPOINT_FILE = Path('yuruquant') / 'adapters' / 'gm' / 'entrypoint.py'


def _supports_callback_kwargs(run_fn: Callable | None) -> bool:
    if run_fn is None:
        return False
    try:
        params = inspect.signature(run_fn).parameters
    except Exception:
        return False
    return {'init', 'on_bar', 'on_order_status', 'on_execution_report', 'on_error'}.issubset(set(params))


def _filter_supported_kwargs(run_fn: Callable | None, kwargs: dict[str, object]) -> dict[str, object]:
    if run_fn is None:
        return kwargs
    try:
        params = inspect.signature(run_fn).parameters
    except Exception:
        return kwargs
    if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params.values()):
        return kwargs
    return {key: value for key, value in kwargs.items() if key in params}


def _permission_floor(now: datetime | None = None) -> datetime:
    ref = parse_datetime(now or datetime.now())
    return datetime(ref.year, ref.month, ref.day) - timedelta(days=180)


def clip_backtest_window_if_needed(config: AppConfig) -> None:
    if str(config.runtime.mode).upper() != 'BACKTEST':
        return
    gm = config.broker.gm.backtest
    permission_floor = None if str(os.getenv('LOCAL_DATA_ROOT', '')).strip() else _permission_floor()
    start = parse_datetime(gm.start)
    end = parse_datetime(gm.end)
    if permission_floor is not None and start < permission_floor:
        start = permission_floor
    if start >= end:
        end = parse_datetime(datetime.now()).replace(second=0, microsecond=0)
        start = end - timedelta(days=gm.max_days)
        if permission_floor is not None:
            start = max(start, permission_floor)
    span_days = (end - start).total_seconds() / 86400.0
    if span_days > gm.max_days:
        start = end - timedelta(days=gm.max_days)
        if permission_floor is not None:
            start = max(start, permission_floor)
    gm.start = start.strftime('%Y-%m-%d %H:%M:%S')
    gm.end = end.strftime('%Y-%m-%d %H:%M:%S')


def run_with_gm(config: AppConfig, callbacks) -> None:
    if run is None:
        raise RuntimeError('gm.api.run is unavailable; install gm package')
    if not config.broker.gm.token or not config.broker.gm.strategy_id:
        raise RuntimeError('GM token/strategy_id is required; set config or env')
    clip_backtest_window_if_needed(config)

    mode = MODE_LIVE if config.runtime.mode == 'LIVE' else MODE_BACKTEST
    backtest = config.broker.gm.backtest
    kwargs: dict[str, object] = {
        'token': config.broker.gm.token,
        'strategy_id': config.broker.gm.strategy_id,
        'filename': str(_ENTRYPOINT_FILE),
        'mode': mode,
        'backtest_start_time': backtest.start,
        'backtest_end_time': backtest.end,
        'backtest_adjust': ADJUST_NONE,
        'backtest_commission_ratio': config.execution.backtest_commission_ratio,
        'backtest_slippage_ratio': config.execution.backtest_slippage_ratio,
        'backtest_match_mode': int(backtest.match_mode),
    }
    if backtest.initial_cash > 0:
        kwargs['backtest_initial_cash'] = float(backtest.initial_cash)
    if backtest.intraday and mode == MODE_BACKTEST:
        kwargs['backtest_intraday'] = True
    if config.broker.gm.serv_addr:
        kwargs['serv_addr'] = config.broker.gm.serv_addr

    callback_kwargs = {
        'init': callbacks.init,
        'on_bar': callbacks.on_bar,
        'on_order_status': callbacks.on_order_status,
        'on_execution_report': callbacks.on_execution_report,
        'on_error': callbacks.on_error,
    }
    run_kwargs = _filter_supported_kwargs(run, kwargs)
    if _supports_callback_kwargs(run):
        run_kwargs.update(callback_kwargs)
        run_kwargs = _filter_supported_kwargs(run, run_kwargs)

    original_argv = list(sys.argv)
    original_flag = os.getenv('YURUQUANT_RUNTIME_ENV_ACTIVE')
    try:
        os.environ['YURUQUANT_RUNTIME_ENV_ACTIVE'] = '1'
        sys.argv = [sys.argv[0]]
        run(**run_kwargs)
    except TypeError as exc:
        if 'unexpected keyword argument' in str(exc):
            warn('gm.run.callback_kwargs_unsupported')
            sys.argv = [sys.argv[0]]
            run(**_filter_supported_kwargs(run, kwargs))
            return
        raise
    finally:
        sys.argv = original_argv
        if original_flag is None:
            os.environ.pop('YURUQUANT_RUNTIME_ENV_ACTIVE', None)
        else:
            os.environ['YURUQUANT_RUNTIME_ENV_ACTIVE'] = original_flag
