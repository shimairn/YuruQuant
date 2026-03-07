from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from strategy.core.kline_types import KlineFrame


@dataclass(frozen=True)
class TrendSnapshot:
    direction: int
    strength: float
    ema_fast: float
    ema_slow: float
    atr: float


def _alpha_from_period(period: int) -> float:
    p = max(int(period), 1)
    return 2.0 / (p + 1.0)


def atr_series(frame: KlineFrame, period: int) -> pl.Series:
    if frame.empty:
        return pl.Series(name="atr", values=[], dtype=pl.Float64)

    alpha = 1.0 / max(int(period), 1)
    df = frame.frame.with_columns(
        [
            pl.col("close").shift(1).alias("prev_close"),
        ]
    ).with_columns(
        [
            (pl.col("high") - pl.col("low")).abs().alias("tr1"),
            (pl.col("high") - pl.col("prev_close")).abs().alias("tr2"),
            (pl.col("low") - pl.col("prev_close")).abs().alias("tr3"),
        ]
    ).with_columns(
        [
            pl.max_horizontal("tr1", "tr2", "tr3").alias("tr"),
        ]
    ).with_columns(
        [
            pl.col("tr").ewm_mean(alpha=alpha, adjust=False).alias("atr"),
        ]
    )
    return df.get_column("atr")


def latest_atr_value(frame: KlineFrame, period: int) -> float:
    if frame.empty:
        return 0.0
    n = len(frame)
    if n <= 1:
        return 0.0

    alpha = 1.0 / max(int(period), 1)
    high = frame.frame.get_column("high").to_list()
    low = frame.frame.get_column("low").to_list()
    close = frame.frame.get_column("close").to_list()

    prev_close = float(close[0] or 0.0)
    atr = abs(float(high[0] or 0.0) - float(low[0] or 0.0))
    for i in range(1, n):
        h = float(high[i] or 0.0)
        l = float(low[i] or 0.0)
        tr = max(abs(h - l), abs(h - prev_close), abs(l - prev_close))
        atr = alpha * tr + (1.0 - alpha) * atr
        prev_close = float(close[i] or prev_close)
    return float(max(atr, 0.0))


def ema_series(frame: KlineFrame, period: int, column: str = "close") -> pl.Series:
    if frame.empty:
        return pl.Series(name=f"ema_{period}", values=[], dtype=pl.Float64)
    alpha = _alpha_from_period(period)
    return frame.frame.get_column(column).ewm_mean(alpha=alpha, adjust=False)


def latest_volume_ratio(frame: KlineFrame, window: int = 20) -> float:
    if frame.empty:
        return 0.0
    n = len(frame)
    if n < 2:
        return 0.0
    volume = frame.frame.get_column("volume")
    current = float(volume.item(-1) or 0.0)
    hist = frame.frame.select(pl.col("volume").tail(max(int(window), 1)).mean().alias("m")).item(0, 0)
    ma = float(hist or 0.0)
    if ma <= 0:
        return 0.0
    return current / ma


def latest_breakout_channel(frame: KlineFrame, lookback: int) -> tuple[float, float, float]:
    n = len(frame)
    if n <= max(int(lookback), 2):
        return 0.0, 0.0, 0.0

    lb = max(int(lookback), 2)
    hist = frame.frame.slice(max(0, n - lb - 1), lb)
    upper = float(hist.select(pl.col("high").max()).item(0, 0) or 0.0)
    lower = float(hist.select(pl.col("low").min()).item(0, 0) or 0.0)
    close = float(frame.frame.get_column("close").item(-1) or 0.0)
    return upper, lower, close


def compute_trend_1h(frame_1h: KlineFrame, ema_fast_period: int, ema_slow_period: int, atr_period: int) -> TrendSnapshot:
    if len(frame_1h) < max(int(ema_slow_period), int(atr_period)) + 3:
        return TrendSnapshot(direction=0, strength=0.0, ema_fast=0.0, ema_slow=0.0, atr=0.0)

    ema_fast = ema_series(frame_1h, ema_fast_period)
    ema_slow = ema_series(frame_1h, ema_slow_period)
    atr = atr_series(frame_1h, atr_period)

    fast_v = float(ema_fast.item(-1) or 0.0)
    slow_v = float(ema_slow.item(-1) or 0.0)
    atr_v = float(atr.item(-1) or 0.0)

    direction = 0
    if fast_v > slow_v:
        direction = 1
    elif fast_v < slow_v:
        direction = -1

    if atr_v <= 0:
        strength = 0.0
    else:
        strength = abs(fast_v - slow_v) / atr_v
    strength = max(0.0, min(1.0, float(strength)))

    return TrendSnapshot(
        direction=direction,
        strength=strength,
        ema_fast=fast_v,
        ema_slow=slow_v,
        atr=atr_v,
    )
