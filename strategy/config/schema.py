from __future__ import annotations

TOP_LEVEL_KEYS = {
    "runtime",
    "strategy",
    "risk",
    "portfolio",
    "gm",
    "reporting",
    "observability",
    "instrument",
}

RUNTIME_KEYS = {
    "mode",
    "run_id",
    "symbols",
    "freq_5m",
    "freq_1h",
    "warmup_5m",
    "warmup_1h",
}

STRATEGY_KEYS = {
    "breakout_lookback_5m",
    "breakout_min_distance_atr",
    "breakout_width_min_atr",
    "breakout_width_max_atr",
    "volume_ratio_day_min",
    "volume_ratio_night_min",
    "trend_ema_fast_1h",
    "trend_ema_slow_1h",
    "trend_strength_min",
    "entry_cooldown_bars",
    "max_entries_per_day",
    "target_annual_vol",
    "atr_period",
}

RISK_KEYS = {
    "risk_per_trade_notional_ratio",
    "fixed_equity_percent",
    "max_pos_size_percent",
    "hard_stop_atr",
    "break_even_activate_r",
    "trail_activate_r",
    "trail_stop_atr",
    "dynamic_stop_enabled",
    "dynamic_stop_atr",
    "dynamic_stop_activate_r",
    "time_stop_bars",
    "max_stopouts_per_day_per_symbol",
    "backtest_commission_ratio",
    "backtest_slippage_ratio",
}

PORTFOLIO_KEYS = {
    "max_daily_loss_ratio",
    "max_drawdown_halt_ratio",
}

GM_KEYS = {
    "token",
    "strategy_id",
    "serv_addr",
    "backtest_start",
    "backtest_end",
    "backtest_max_days",
    "backtest_initial_cash",
    "backtest_match_mode",
    "backtest_intraday",
    "subscribe_wait_group",
    "wait_group_timeout",
}

REPORTING_KEYS = {
    "enabled",
    "output_dir",
    "trade_filename",
    "daily_filename",
    "execution_filename",
}

OBSERVABILITY_KEYS = {
    "level",
    "sample_every_n",
}

INSTRUMENT_TOP_KEYS = {
    "defaults",
    "symbols",
}

INSTRUMENT_SPEC_KEYS = {
    "multiplier",
    "min_tick",
    "min_lot",
    "lot_step",
    "fixed_equity_percent",
    "max_pos_size_percent",
    "volume_ratio_min",
    "sessions",
}

INSTRUMENT_VOLUME_RATIO_KEYS = {
    "day",
    "night",
}

INSTRUMENT_SESSIONS_KEYS = {
    "day",
    "night",
}

SECTION_KEYS = {
    "runtime": RUNTIME_KEYS,
    "strategy": STRATEGY_KEYS,
    "risk": RISK_KEYS,
    "portfolio": PORTFOLIO_KEYS,
    "gm": GM_KEYS,
    "reporting": REPORTING_KEYS,
    "observability": OBSERVABILITY_KEYS,
}
