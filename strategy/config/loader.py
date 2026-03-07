from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from strategy.types import (
    AppConfig,
    GMSettings,
    InstrumentSettings,
    InstrumentSpec,
    ObservabilitySettings,
    PortfolioSettings,
    ReportingSettings,
    RiskSettings,
    RuntimeSettings,
    SessionSettings,
    StrategySettings,
    VolumeRatioSettings,
)

from .validator import log_credential_source, validate_and_normalize_root


def _section(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    if isinstance(value, dict):
        return value
    raise ValueError(f"config error: section '{key}' must be an object")


def _f(data: dict[str, Any], key: str) -> float:
    try:
        return float(data[key])
    except Exception as exc:
        raise ValueError(f"config error: '{key}' must be float") from exc


def _i(data: dict[str, Any], key: str) -> int:
    try:
        return int(data[key])
    except Exception as exc:
        raise ValueError(f"config error: '{key}' must be int") from exc


def _b(data: dict[str, Any], key: str) -> bool:
    value = data[key]
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw in {"1", "true", "yes", "on"}:
            return True
        if raw in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"config error: '{key}' must be bool")


def _s(data: dict[str, Any], key: str) -> str:
    value = data[key]
    if value is None:
        return ""
    return str(value).strip()


def _parse_ranges(value: Any) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for item in value:
        start = str(item[0]).strip()
        end = str(item[1]).strip()
        out.append((start, end))
    return out


def _parse_instrument_spec(raw: dict[str, Any]) -> InstrumentSpec:
    vr = raw["volume_ratio_min"]
    sessions = raw["sessions"]
    return InstrumentSpec(
        multiplier=_f(raw, "multiplier"),
        min_tick=_f(raw, "min_tick"),
        min_lot=_i(raw, "min_lot"),
        lot_step=_i(raw, "lot_step"),
        fixed_equity_percent=_f(raw, "fixed_equity_percent"),
        max_pos_size_percent=_f(raw, "max_pos_size_percent"),
        volume_ratio_min=VolumeRatioSettings(
            day=float(vr["day"]),
            night=float(vr["night"]),
        ),
        sessions=SessionSettings(
            day=_parse_ranges(sessions["day"]),
            night=_parse_ranges(sessions["night"]),
        ),
    )


def load_config(path: Path) -> AppConfig:
    if not path.exists():
        raise FileNotFoundError(f"config file not found: {path}")

    text = path.read_text(encoding="utf-8-sig")
    raw = yaml.safe_load(text)
    if not isinstance(raw, dict):
        raise ValueError("config error: root must be an object")

    root = validate_and_normalize_root(raw)

    runtime_raw = _section(root, "runtime")
    strategy_raw = _section(root, "strategy")
    risk_raw = _section(root, "risk")
    portfolio_raw = _section(root, "portfolio")
    gm_raw = _section(root, "gm")
    reporting_raw = _section(root, "reporting")
    observability_raw = _section(root, "observability")
    instrument_raw = _section(root, "instrument")

    runtime = RuntimeSettings(
        mode=_s(runtime_raw, "mode").upper(),
        run_id=_s(runtime_raw, "run_id"),
        symbols=[str(x).strip() for x in runtime_raw["symbols"]],
        freq_5m=_s(runtime_raw, "freq_5m"),
        freq_1h=_s(runtime_raw, "freq_1h"),
        warmup_5m=max(_i(runtime_raw, "warmup_5m"), 1),
        warmup_1h=max(_i(runtime_raw, "warmup_1h"), 1),
    )

    strategy = StrategySettings(
        breakout_lookback_5m=max(_i(strategy_raw, "breakout_lookback_5m"), 2),
        breakout_min_distance_atr=max(_f(strategy_raw, "breakout_min_distance_atr"), 0.0),
        breakout_width_min_atr=max(_f(strategy_raw, "breakout_width_min_atr"), 0.0),
        breakout_width_max_atr=max(_f(strategy_raw, "breakout_width_max_atr"), 0.0),
        volume_ratio_day_min=max(_f(strategy_raw, "volume_ratio_day_min"), 0.0),
        volume_ratio_night_min=max(_f(strategy_raw, "volume_ratio_night_min"), 0.0),
        trend_ema_fast_1h=max(_i(strategy_raw, "trend_ema_fast_1h"), 2),
        trend_ema_slow_1h=max(_i(strategy_raw, "trend_ema_slow_1h"), 3),
        trend_strength_min=max(_f(strategy_raw, "trend_strength_min"), 0.0),
        entry_cooldown_bars=max(_i(strategy_raw, "entry_cooldown_bars"), 0),
        max_entries_per_day=max(_i(strategy_raw, "max_entries_per_day"), 1),
        target_annual_vol=max(_f(strategy_raw, "target_annual_vol"), 0.01),
        atr_period=max(_i(strategy_raw, "atr_period"), 2),
    )
    if strategy.trend_ema_fast_1h >= strategy.trend_ema_slow_1h:
        raise ValueError("config error: strategy.trend_ema_fast_1h must be smaller than trend_ema_slow_1h")

    risk = RiskSettings(
        risk_per_trade_notional_ratio=max(_f(risk_raw, "risk_per_trade_notional_ratio"), 0.0),
        fixed_equity_percent=max(_f(risk_raw, "fixed_equity_percent"), 0.0),
        max_pos_size_percent=max(_f(risk_raw, "max_pos_size_percent"), 0.0),
        hard_stop_atr=max(_f(risk_raw, "hard_stop_atr"), 0.01),
        break_even_activate_r=max(_f(risk_raw, "break_even_activate_r"), 0.0),
        trail_activate_r=max(_f(risk_raw, "trail_activate_r"), 0.0),
        trail_stop_atr=max(_f(risk_raw, "trail_stop_atr"), 0.01),
        dynamic_stop_enabled=_b(risk_raw, "dynamic_stop_enabled"),
        dynamic_stop_atr=max(_f(risk_raw, "dynamic_stop_atr"), 0.01),
        dynamic_stop_activate_r=max(_f(risk_raw, "dynamic_stop_activate_r"), 0.0),
        time_stop_bars=max(_i(risk_raw, "time_stop_bars"), 1),
        max_stopouts_per_day_per_symbol=max(_i(risk_raw, "max_stopouts_per_day_per_symbol"), 0),
        backtest_commission_ratio=max(_f(risk_raw, "backtest_commission_ratio"), 0.0),
        backtest_slippage_ratio=max(_f(risk_raw, "backtest_slippage_ratio"), 0.0),
    )

    portfolio = PortfolioSettings(
        max_daily_loss_ratio=max(_f(portfolio_raw, "max_daily_loss_ratio"), 0.0),
        max_drawdown_halt_ratio=max(_f(portfolio_raw, "max_drawdown_halt_ratio"), 0.0),
    )

    gm = GMSettings(
        token=_s(gm_raw, "token"),
        strategy_id=_s(gm_raw, "strategy_id"),
        serv_addr=_s(gm_raw, "serv_addr"),
        backtest_start=_s(gm_raw, "backtest_start"),
        backtest_end=_s(gm_raw, "backtest_end"),
        backtest_max_days=max(_i(gm_raw, "backtest_max_days"), 1),
        backtest_initial_cash=max(_f(gm_raw, "backtest_initial_cash"), 0.0),
        backtest_match_mode=_i(gm_raw, "backtest_match_mode"),
        backtest_intraday=_b(gm_raw, "backtest_intraday"),
        subscribe_wait_group=_b(gm_raw, "subscribe_wait_group"),
        wait_group_timeout=max(_i(gm_raw, "wait_group_timeout"), 0),
    )
    log_credential_source(gm.token, gm.strategy_id)

    reporting = ReportingSettings(
        enabled=_b(reporting_raw, "enabled"),
        output_dir=_s(reporting_raw, "output_dir"),
        trade_filename=_s(reporting_raw, "trade_filename"),
        daily_filename=_s(reporting_raw, "daily_filename"),
        execution_filename=_s(reporting_raw, "execution_filename"),
    )

    observability = ObservabilitySettings(
        level=_s(observability_raw, "level").upper(),
        sample_every_n=max(_i(observability_raw, "sample_every_n"), 1),
    )

    defaults_raw = _section(instrument_raw, "defaults")
    defaults = _parse_instrument_spec(defaults_raw)

    symbol_specs: dict[str, InstrumentSpec] = {}
    symbols_raw = _section(instrument_raw, "symbols")
    for csymbol, spec in symbols_raw.items():
        symbol_specs[str(csymbol)] = _parse_instrument_spec(spec)

    instrument = InstrumentSettings(defaults=defaults, symbols=symbol_specs)

    return AppConfig(
        runtime=runtime,
        strategy=strategy,
        risk=risk,
        portfolio=portfolio,
        gm=gm,
        reporting=reporting,
        observability=observability,
        instrument=instrument,
    )
