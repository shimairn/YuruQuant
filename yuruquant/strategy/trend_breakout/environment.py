from __future__ import annotations

from yuruquant.core.frames import KlineFrame
from yuruquant.core.indicators import latest_macd_histogram, latest_sma
from yuruquant.core.models import EnvironmentSnapshot


def compute_environment(frame_1h: KlineFrame, ma_period: int, macd_fast: int, macd_slow: int, macd_signal: int) -> EnvironmentSnapshot:
    required = max(int(ma_period), int(macd_slow) + int(macd_signal)) + 3
    if len(frame_1h) < required:
        return EnvironmentSnapshot()
    close = frame_1h.latest_close()
    moving_average = latest_sma(frame_1h, ma_period)
    histogram = latest_macd_histogram(frame_1h, macd_fast, macd_slow, macd_signal)
    if close > moving_average and histogram > 0:
        return EnvironmentSnapshot(direction=1, trend_ok=True, close=close, moving_average=moving_average, macd_histogram=histogram)
    if close < moving_average and histogram < 0:
        return EnvironmentSnapshot(direction=-1, trend_ok=True, close=close, moving_average=moving_average, macd_histogram=histogram)
    return EnvironmentSnapshot(direction=0, trend_ok=False, close=close, moving_average=moving_average, macd_histogram=histogram)
