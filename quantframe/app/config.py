from __future__ import annotations

from dataclasses import dataclass, field
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml

from quantframe.core.models import Instrument, PlatformMode


@dataclass(frozen=True)
class RuntimeConfig:
    mode: PlatformMode
    run_id: str


@dataclass(frozen=True)
class GMBacktestConfig:
    start: str
    end: str
    initial_cash: float
    commission_ratio: float = 0.0
    slippage_ratio: float = 0.0


@dataclass(frozen=True)
class GMPlatformConfig:
    token: str
    strategy_id: str
    serv_addr: str = ""
    subscribe_wait_group: bool = True
    wait_group_timeout: int = 10
    backtest: GMBacktestConfig = field(default_factory=lambda: GMBacktestConfig("", "", 0.0))


@dataclass(frozen=True)
class PlatformConfig:
    name: str
    gm: GMPlatformConfig | None = None


@dataclass(frozen=True)
class ResourceConfig:
    universe_path: Path
    instruments_path: Path


@dataclass(frozen=True)
class StrategyConfig:
    factory: str
    params: dict[str, Any]


@dataclass(frozen=True)
class ReportingConfig:
    enabled: bool
    output_dir: str


@dataclass(frozen=True)
class AppConfig:
    runtime: RuntimeConfig
    platform: PlatformConfig
    resources: ResourceConfig
    strategy: StrategyConfig
    reporting: ReportingConfig


@dataclass(frozen=True)
class LoadedResources:
    universe: tuple[Instrument, ...]
    by_id: dict[str, Instrument]
    by_symbol: dict[str, Instrument]


def _read_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"{path.as_posix()} must contain a mapping")
    return dict(payload)


def _resolve_path(base_path: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (base_path.parent / path).resolve()


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path).resolve()
    root = _read_yaml(config_path)
    runtime_raw = dict(root.get("runtime") or {})
    platform_raw = dict(root.get("platform") or {})
    resources_raw = dict(root.get("resources") or {})
    strategy_raw = dict(root.get("strategy") or {})
    reporting_raw = dict(root.get("reporting") or {})

    gm_raw = dict(platform_raw.get("gm") or {})
    gm_backtest_raw = dict(gm_raw.get("backtest") or {})
    config = AppConfig(
        runtime=RuntimeConfig(
            mode=str(runtime_raw.get("mode", "BACKTEST")).strip().upper(),  # type: ignore[arg-type]
            run_id=str(runtime_raw.get("run_id", "framework_run")).strip(),
        ),
        platform=PlatformConfig(
            name=str(platform_raw.get("name", "gm")).strip().lower(),
            gm=GMPlatformConfig(
                token=str(gm_raw.get("token", "")).strip(),
                strategy_id=str(gm_raw.get("strategy_id", "")).strip(),
                serv_addr=str(gm_raw.get("serv_addr", "")).strip(),
                subscribe_wait_group=bool(gm_raw.get("subscribe_wait_group", True)),
                wait_group_timeout=max(int(gm_raw.get("wait_group_timeout", 10)), 0),
                backtest=GMBacktestConfig(
                    start=str(gm_backtest_raw.get("start", "")).strip(),
                    end=str(gm_backtest_raw.get("end", "")).strip(),
                    initial_cash=float(gm_backtest_raw.get("initial_cash", 0.0) or 0.0),
                    commission_ratio=float(gm_backtest_raw.get("commission_ratio", 0.0) or 0.0),
                    slippage_ratio=float(gm_backtest_raw.get("slippage_ratio", 0.0) or 0.0),
                ),
            ),
        ),
        resources=ResourceConfig(
            universe_path=_resolve_path(config_path, str(resources_raw.get("universe", "resources/universes/cn_futures_core.yaml"))),
            instruments_path=_resolve_path(config_path, str(resources_raw.get("instruments", "resources/instruments/gm_cn_futures.yaml"))),
        ),
        strategy=StrategyConfig(
            factory=str(strategy_raw.get("factory", "strategies.trend.turtle_breakout:create_strategy")).strip(),
            params=dict(strategy_raw.get("params") or {}),
        ),
        reporting=ReportingConfig(
            enabled=bool(reporting_raw.get("enabled", True)),
            output_dir=str(reporting_raw.get("output_dir", "reports/framework")).strip(),
        ),
    )
    if config.runtime.mode not in {"BACKTEST", "LIVE"}:
        raise ValueError("runtime.mode must be BACKTEST or LIVE")
    if not config.strategy.factory:
        raise ValueError("strategy.factory must be configured")
    return config


def load_resources(config: AppConfig) -> LoadedResources:
    universe_raw = _read_yaml(config.resources.universe_path)
    instruments_raw = _read_yaml(config.resources.instruments_path)
    symbols = [str(item).strip() for item in list(universe_raw.get("symbols") or []) if str(item).strip()]
    instrument_map = dict(instruments_raw.get("instruments") or {})
    if not symbols:
        raise ValueError("resource universe must provide at least one symbol")

    universe: list[Instrument] = []
    by_id: dict[str, Instrument] = {}
    by_symbol: dict[str, Instrument] = {}
    for instrument_id in symbols:
        raw = dict(instrument_map.get(instrument_id) or {})
        if not raw:
            raise ValueError(f"missing instrument resource for {instrument_id}")
        instrument = Instrument(
            instrument_id=instrument_id,
            platform_symbol=str(raw.get("platform_symbol", instrument_id)).strip(),
            multiplier=float(raw.get("multiplier", 1.0) or 1.0),
            tick_size=float(raw.get("tick_size", 0.0) or 0.0),
            lot_size=max(int(raw.get("lot_size", 1) or 1), 1),
            metadata={key: value for key, value in raw.items() if key not in {"platform_symbol", "multiplier", "tick_size", "lot_size"}},
        )
        universe.append(instrument)
        by_id[instrument.instrument_id] = instrument
        by_symbol[instrument.platform_symbol] = instrument
    return LoadedResources(universe=tuple(universe), by_id=by_id, by_symbol=by_symbol)


def load_object(import_path: str):
    module_name, _, attr_name = str(import_path).partition(":")
    if not module_name or not attr_name:
        raise ValueError(f"invalid import path '{import_path}', expected module:attribute")
    module = import_module(module_name)
    return getattr(module, attr_name)
