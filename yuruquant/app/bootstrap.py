from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime

from yuruquant.adapters.gm import GMCallbacks, GMGateway
from yuruquant.app.cli import CLIArgs
from yuruquant.app.config import AppConfig, load_config
from yuruquant.core.engine import StrategyEngine
from yuruquant.core.fill_policy import NextBarOpenFillPolicy
from yuruquant.reporting import CsvReportSink, configure, info


@dataclass
class Application:
    config: AppConfig
    gateway: GMGateway
    engine: StrategyEngine
    callbacks: GMCallbacks


def _append_run_id_timestamp_if_needed(config: AppConfig) -> None:
    current = str(config.runtime.run_id or 'run_001').strip() or 'run_001'
    if re.search(r'_\d{8}_\d{6}$', current):
        return
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    config.runtime.run_id = f"{current.rstrip('_')}_{stamp}"


def _runtime_env_active() -> bool:
    return str(os.getenv('YURUQUANT_RUNTIME_ENV_ACTIVE', '')).strip() == '1'


def _read_env(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped or None


def _resolve_mode(current: str | None, cli_value: str | None, runtime_env: bool) -> str:
    if cli_value is not None:
        return cli_value
    if runtime_env:
        return _read_env('GM_MODE') or str(current or 'BACKTEST').strip().upper() or 'BACKTEST'
    return str(current or 'BACKTEST').strip().upper() or 'BACKTEST'


def _resolve_string(current: str | None, cli_value: str | None, env_name: str, runtime_env: bool) -> str:
    if cli_value:
        return str(cli_value).strip()
    if runtime_env:
        return _read_env(env_name) or str(current or '').strip()
    if current:
        return str(current).strip()
    return _read_env(env_name) or ''


def _resolve_run_id(current: str | None, cli_value: str | None, runtime_env: bool) -> str:
    if cli_value is not None:
        return str(cli_value).strip() or str(current or '').strip()
    if runtime_env:
        return _read_env('GM_RUN_ID') or str(current or '').strip()
    return str(current or '').strip()


def _sync_runtime_env(config: AppConfig, args: CLIArgs) -> None:
    os.environ['STRATEGY_CONFIG'] = str(args.config)
    os.environ['GM_MODE'] = str(config.runtime.mode or 'BACKTEST').strip().upper()
    os.environ['GM_RUN_ID'] = str(config.runtime.run_id or '').strip()
    os.environ['GM_TOKEN'] = str(config.broker.gm.token or '').strip()
    os.environ['GM_STRATEGY_ID'] = str(config.broker.gm.strategy_id or '').strip()
    serv_addr = str(config.broker.gm.serv_addr or '').strip()
    if serv_addr:
        os.environ['GM_SERV_ADDR'] = serv_addr
    else:
        os.environ.pop('GM_SERV_ADDR', None)


def apply_overrides(config: AppConfig, args: CLIArgs) -> None:
    runtime_env = _runtime_env_active()

    config.runtime.mode = _resolve_mode(config.runtime.mode, args.mode, runtime_env)
    config.broker.gm.token = _resolve_string(config.broker.gm.token, args.token, 'GM_TOKEN', runtime_env)
    config.broker.gm.strategy_id = _resolve_string(config.broker.gm.strategy_id, args.strategy_id, 'GM_STRATEGY_ID', runtime_env)
    config.broker.gm.serv_addr = _resolve_string(config.broker.gm.serv_addr, args.serv_addr, 'GM_SERV_ADDR', runtime_env)
    config.runtime.run_id = _resolve_run_id(config.runtime.run_id, args.run_id, runtime_env)

    if not config.runtime.run_id:
        _append_run_id_timestamp_if_needed(config)


def build_application(args: CLIArgs) -> Application:
    config = load_config(args.config)
    apply_overrides(config, args)
    _sync_runtime_env(config, args)
    configure(level=config.observability.level, sample_every_n=config.observability.sample_every_n)
    info('app.bootstrap', mode=config.runtime.mode, run_id=config.runtime.run_id)

    gateway = GMGateway(config)
    report_sink = CsvReportSink(
        output_dir=config.reporting.output_dir,
        signals_filename=config.reporting.signals_filename,
        executions_filename=config.reporting.executions_filename,
        portfolio_daily_filename=config.reporting.portfolio_daily_filename,
    )
    engine = StrategyEngine(config=config, gateway=gateway, report_sink=report_sink, fill_policy=NextBarOpenFillPolicy())
    callbacks = GMCallbacks(config=config, gateway=gateway, engine=engine)
    return Application(config=config, gateway=gateway, engine=engine, callbacks=callbacks)
