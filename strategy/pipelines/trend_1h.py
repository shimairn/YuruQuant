from __future__ import annotations

from typing import Tuple

import pandas as pd

from strategy.types import StrategySettings, SymbolState


def _to_ts(value: object) -> pd.Timestamp:
    return pd.to_datetime(value)


def _calculate_rsi(series: pd.Series, period: int) -> pd.Series:
    """计算 RSI 指标"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def classify_trend_1h_ema_rsi(df_1h: pd.DataFrame, ema_fast_period: int, ema_slow_period: int, rsi_period: int, rsi_threshold: float) -> Tuple[int, float, float, float]:
    """
    DC_Fractal_Sniper: 基于 EMA/RSI 的 1H 趋势判定

    Args:
        df_1h: 1小时 K线数据
        ema_fast_period: 快速均线周期（默认 20）
        ema_slow_period: 慢速均线周期（默认 60）
        rsi_period: RSI 周期（默认 14）
        rsi_threshold: RSI 阈值（默认 50）

    Returns:
        (direction, ema_fast_val, ema_slow_val, rsi_val)
        direction: 1 (多头), -1 (空头), 0 (无趋势)
    """
    if df_1h is None or len(df_1h) < max(ema_slow_period, rsi_period) + 5:
        return 0, 0.0, 0.0, 0.0

    close = df_1h["close"]

    # 计算均线
    ema_fast = close.ewm(span=ema_fast_period, adjust=False).mean()
    ema_slow = close.ewm(span=ema_slow_period, adjust=False).mean()

    # 计算 RSI
    rsi = _calculate_rsi(close, rsi_period)

    current_price = float(close.iloc[-1])
    current_ema_fast = float(ema_fast.iloc[-1])
    current_ema_slow = float(ema_slow.iloc[-1])
    current_rsi = float(rsi.iloc[-1])

    # 多头条件：价格在长期均线上方 且 快线在慢线上方 且 RSI 强于阈值
    bullish_conditions = [
        current_price > current_ema_slow,
        current_ema_fast > current_ema_slow,
        current_rsi > rsi_threshold,
    ]

    # 空头条件：价格在长期均线下方 且 快线在慢线下方 且 RSI 弱于阈值
    bearish_conditions = [
        current_price < current_ema_slow,
        current_ema_fast < current_ema_slow,
        current_rsi < (100 - rsi_threshold),
    ]

    direction = 0
    if all(bullish_conditions):
        direction = 1
    elif all(bearish_conditions):
        direction = -1

    return direction, current_ema_fast, current_ema_slow, current_rsi


def calculate_h1_trailing_stop_ema(df_1h: pd.DataFrame, ema_period: int = 20) -> float:
    """
    DC_Fractal_Sniper: 计算 1H EMA 跟踪止损位
    """
    if df_1h is None or len(df_1h) < ema_period:
        return 0.0

    close = df_1h["close"]
    ema = close.ewm(span=ema_period, adjust=False).mean()
    return float(ema.iloc[-1])


def refresh_h1_trend_state(state: SymbolState, df_1h: pd.DataFrame, settings: StrategySettings) -> None:
    """更新 1H 趋势状态（使用 EMA/RSI 判定）"""
    if df_1h.empty:
        return

    latest_eob = df_1h.iloc[-1]["eob"]
    if state.last_h1_eob is not None and _to_ts(latest_eob) <= _to_ts(state.last_h1_eob):
        return

    # 使用配置的参数
    trend, ema_fast, ema_slow, rsi = classify_trend_1h_ema_rsi(
        df_1h,
        ema_fast_period=settings.h1_ema_fast_period,
        ema_slow_period=settings.h1_ema_slow_period,
        rsi_period=settings.h1_rsi_period,
        rsi_threshold=settings.h1_rsi_threshold,
    )

    state.h1_trend = trend

    # strength 基于均线排列和 RSI 强度
    ema_aligned = 1.0 if (trend > 0 and ema_fast > ema_slow) or (trend < 0 and ema_fast < ema_slow) else 0.0
    rsi_strength = abs(rsi - 50) / 50.0
    state.h1_strength = max(0.0, min(1.0, (ema_aligned + rsi_strength) / 2.0))

    state.last_h1_eob = latest_eob
