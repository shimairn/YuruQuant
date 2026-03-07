from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None

from .types import (
    AppConfig,
    GMSettings,
    InstrumentSettings,
    InstrumentSpec,
    PortfolioSettings,
    ReportingSettings,
    RiskSettings,
    RuntimeSettings,
    SessionSettings,
    StrategySettings,
    VolumeRatioSettings,
)


def _section(data: dict[str, Any], key: str) -> dict[str, Any]:
    raw = data.get(key, {})
    return raw if isinstance(raw, dict) else {}


def _pick(data: dict[str, Any], key: str, default: Any) -> Any:
    value = data.get(key, default)
    return default if value is None else value


def _parse_time_ranges(value: Any, default_ranges: list[tuple[str, str]]) -> list[tuple[str, str]]:
    if not isinstance(value, list):
        return list(default_ranges)
    out: list[tuple[str, str]] = []
    for item in value:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
        start = str(item[0]).strip()
        end = str(item[1]).strip()
        if not start or not end:
            continue
        out.append((start, end))
    return out or list(default_ranges)


def _parse_instrument_spec(raw: dict[str, Any], fallback: InstrumentSpec) -> InstrumentSpec:
    vr_raw = raw.get("volume_ratio_min", {}) if isinstance(raw.get("volume_ratio_min", {}), dict) else {}
    sessions_raw = raw.get("sessions", {}) if isinstance(raw.get("sessions", {}), dict) else {}

    volume_ratio_min = VolumeRatioSettings(
        day=float(_pick(vr_raw, "day", fallback.volume_ratio_min.day)),
        night=float(_pick(vr_raw, "night", fallback.volume_ratio_min.night)),
    )

    sessions = SessionSettings(
        day=_parse_time_ranges(sessions_raw.get("day", []), fallback.sessions.day),
        night=_parse_time_ranges(sessions_raw.get("night", []), fallback.sessions.night),
    )

    return InstrumentSpec(
        multiplier=float(_pick(raw, "multiplier", fallback.multiplier)),
        min_tick=float(_pick(raw, "min_tick", fallback.min_tick)),
        min_lot=int(_pick(raw, "min_lot", fallback.min_lot)),
        lot_step=int(_pick(raw, "lot_step", fallback.lot_step)),
        fixed_equity_percent=float(_pick(raw, "fixed_equity_percent", fallback.fixed_equity_percent)),
        max_pos_size_percent=float(_pick(raw, "max_pos_size_percent", fallback.max_pos_size_percent)),
        volume_ratio_min=volume_ratio_min,
        sessions=sessions,
    )


def load_config(path: Path) -> AppConfig:
    if not path.exists():
        raise FileNotFoundError(f"config file not found: {path}")

    text = path.read_text(encoding="utf-8-sig")
    if yaml is not None:
        root = yaml.safe_load(text) or {}
    else:
        # JSON is a strict subset of YAML.
        root = json.loads(text or "{}")

    runtime_raw = _section(root, "runtime")
    strategy_raw = _section(root, "strategy")
    risk_raw = _section(root, "risk")
    portfolio_raw = _section(root, "portfolio")
    gm_raw = _section(root, "gm")
    reporting_raw = _section(root, "reporting")
    instrument_raw = _section(root, "instrument")

    runtime = RuntimeSettings(
        mode=str(_pick(runtime_raw, "mode", "BACKTEST")).upper(),
        run_id=str(_pick(runtime_raw, "run_id", "run_001")),
        symbols=list(_pick(runtime_raw, "symbols", ["DCE.p", "SHFE.ag", "DCE.jm"])),
        freq_5m=str(_pick(runtime_raw, "freq_5m", "300s")),
        freq_1h=str(_pick(runtime_raw, "freq_1h", "3600s")),
        sub_count_5m=int(_pick(runtime_raw, "sub_count_5m", 180)),
        sub_count_1h=int(_pick(runtime_raw, "sub_count_1h", 120)),
    )

    strategy = StrategySettings(
        min_tick=float(_pick(strategy_raw, "min_tick", 1.0)),
        atr_period=int(_pick(strategy_raw, "atr_period", 14)),
        fractal_confirm_bars=int(_pick(strategy_raw, "fractal_confirm_bars", 2)),
        atr_multiplier=float(_pick(strategy_raw, "atr_multiplier", 2.0)),
        min_move_pct=float(_pick(strategy_raw, "min_move_pct", 0.006)),
        require_next_bar_confirm=bool(_pick(strategy_raw, "require_next_bar_confirm", True)),
        min_platform_width_ratio=float(_pick(strategy_raw, "min_platform_width_ratio", 0.0018)),
        min_platform_width_atr=float(_pick(strategy_raw, "min_platform_width_atr", 0.45)),
        breakout_min_distance_atr=float(_pick(strategy_raw, "breakout_min_distance_atr", 0.10)),
        breakout_volume_ratio_min=float(_pick(strategy_raw, "breakout_volume_ratio_min", 1.10)),
        day_volume_ratio_min=float(_pick(strategy_raw, "day_volume_ratio_min", 1.15)),
        night_volume_ratio_min=float(_pick(strategy_raw, "night_volume_ratio_min", 1.05)),
        h1_filter_mode=str(_pick(strategy_raw, "h1_filter_mode", "soft")),
        h1_neutral_size_mult=float(_pick(strategy_raw, "h1_neutral_size_mult", 0.5)),
        h1_strength_min=float(_pick(strategy_raw, "h1_strength_min", 0.15)),
        entry_cooldown_bars=int(_pick(strategy_raw, "entry_cooldown_bars", 2)),
        max_entries_per_day=int(_pick(strategy_raw, "max_entries_per_day", 3)),
        target_annual_vol=float(_pick(strategy_raw, "target_annual_vol", 0.10)),
        atr_pause_ratio=float(_pick(strategy_raw, "atr_pause_ratio", 2.0)),
        atr_pause_lookback=int(_pick(strategy_raw, "atr_pause_lookback", 50)),
        h1_ema_fast_period=int(_pick(strategy_raw, "h1_ema_fast_period", 20)),
        h1_ema_slow_period=int(_pick(strategy_raw, "h1_ema_slow_period", 60)),
        h1_rsi_period=int(_pick(strategy_raw, "h1_rsi_period", 14)),
        h1_rsi_threshold=float(_pick(strategy_raw, "h1_rsi_threshold", 50.0)),
        max_platform_width_atr=float(_pick(strategy_raw, "max_platform_width_atr", 4.0)),
    )

    risk = RiskSettings(
        risk_per_trade=float(_pick(risk_raw, "risk_per_trade", 0.012)),
        hard_stop_atr=float(_pick(risk_raw, "hard_stop_atr", 2.8)),
        first_target_r_ratio=float(_pick(risk_raw, "first_target_r_ratio", 2.0)),
        trail_activate_r=float(_pick(risk_raw, "trail_activate_r", 1.0)),
        trail_stop_atr=float(_pick(risk_raw, "trail_stop_atr", 2.4)),
        enable_dynamic_stop=bool(_pick(risk_raw, "enable_dynamic_stop", False)),
        dynamic_stop_atr=float(_pick(risk_raw, "dynamic_stop_atr", 1.8)),
        dynamic_stop_activate_r=float(_pick(risk_raw, "dynamic_stop_activate_r", 0.8)),
        time_stop_bars=int(_pick(risk_raw, "time_stop_bars", 12)),
        max_stopouts_per_day_per_symbol=int(_pick(risk_raw, "max_stopouts_per_day_per_symbol", 2)),
        backtest_commission_ratio=float(_pick(risk_raw, "backtest_commission_ratio", 0.0005)),
        backtest_slippage_ratio=float(_pick(risk_raw, "backtest_slippage_ratio", 0.0010)),
        fixed_equity_percent=float(_pick(risk_raw, "fixed_equity_percent", 0.05)),
        max_pos_size_percent=float(_pick(risk_raw, "max_pos_size_percent", 0.20)),
    )

    portfolio = PortfolioSettings(
        max_daily_loss_ratio=float(_pick(portfolio_raw, "max_daily_loss_ratio", 0.05)),
        dd_state_1=float(_pick(portfolio_raw, "dd_state_1", 0.08)),
        dd_state_2=float(_pick(portfolio_raw, "dd_state_2", 0.12)),
        dd_state_3=float(_pick(portfolio_raw, "dd_state_3", 0.15)),
        dd_risk_mult_1=float(_pick(portfolio_raw, "dd_risk_mult_1", 0.75)),
        dd_risk_mult_2=float(_pick(portfolio_raw, "dd_risk_mult_2", 0.50)),
        dd_risk_mult_3=float(_pick(portfolio_raw, "dd_risk_mult_3", 0.25)),
    )

    gm = GMSettings(
        token=str(_pick(gm_raw, "token", "")),
        strategy_id=str(_pick(gm_raw, "strategy_id", "")),
        serv_addr=str(_pick(gm_raw, "serv_addr", "")),
        backtest_start=str(_pick(gm_raw, "backtest_start", "2026-01-12 00:00:00")),
        backtest_end=str(_pick(gm_raw, "backtest_end", "2026-02-12 15:00:00")),
        backtest_max_days=int(_pick(gm_raw, "backtest_max_days", 365)),
    )

    reporting = ReportingSettings(
        enabled=bool(_pick(reporting_raw, "enabled", True)),
        output_dir=str(_pick(reporting_raw, "output_dir", "reports")),
        trade_filename=str(_pick(reporting_raw, "trade_filename", "trade_report.csv")),
        daily_filename=str(_pick(reporting_raw, "daily_filename", "daily_report.csv")),
    )

    instrument_defaults = InstrumentSpec(
        multiplier=10.0,
        min_tick=float(_pick(strategy_raw, "min_tick", 1.0)),
        min_lot=1,
        lot_step=1,
        fixed_equity_percent=float(_pick(risk_raw, "fixed_equity_percent", 0.012)),
        max_pos_size_percent=float(_pick(risk_raw, "max_pos_size_percent", 0.20)),
        volume_ratio_min=VolumeRatioSettings(
            day=float(_pick(strategy_raw, "day_volume_ratio_min", 1.15)),
            night=float(_pick(strategy_raw, "night_volume_ratio_min", 1.05)),
        ),
    )

    defaults_raw = _section(instrument_raw, "defaults")
    defaults_spec = _parse_instrument_spec(defaults_raw, instrument_defaults)

    symbols_raw = _section(instrument_raw, "symbols")
    symbol_specs: dict[str, InstrumentSpec] = {}
    for csymbol, spec_raw in symbols_raw.items():
        if not isinstance(spec_raw, dict):
            continue
        symbol_specs[str(csymbol)] = _parse_instrument_spec(spec_raw, defaults_spec)

    instrument = InstrumentSettings(defaults=defaults_spec, symbols=symbol_specs)

    return AppConfig(
        runtime=runtime,
        strategy=strategy,
        risk=risk,
        portfolio=portfolio,
        gm=gm,
        reporting=reporting,
        instrument=instrument,
    )
