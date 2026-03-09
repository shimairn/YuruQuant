from __future__ import annotations

from yuruquant.core.frames import KlineFrame
from yuruquant.core.indicators import latest_macd_histogram, latest_sma
from yuruquant.core.models import EnvironmentSnapshot


def _close_series(frame_1h: KlineFrame) -> list[float]:
    if frame_1h.empty_frame:
        return []
    return [float(value or 0.0) for value in frame_1h.frame.get_column('close').to_list()]


def _compute_ma_macd_environment(
    frame_1h: KlineFrame,
    ma_period: int,
    macd_fast: int,
    macd_slow: int,
    macd_signal: int,
) -> EnvironmentSnapshot:
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


def _compute_tsmom_environment(
    frame_1h: KlineFrame,
    ma_period: int,
    tsmom_lookbacks: tuple[int, ...],
    tsmom_min_agree: int,
) -> EnvironmentSnapshot:
    required = max(int(lookback) for lookback in tsmom_lookbacks) + 1
    if len(frame_1h) < required:
        return EnvironmentSnapshot()
    closes = _close_series(frame_1h)
    close = float(closes[-1])
    moving_average = latest_sma(frame_1h, ma_period)
    score = 0
    for lookback in tsmom_lookbacks:
        reference_close = float(closes[-(int(lookback) + 1)])
        if close > reference_close:
            score += 1
        elif close < reference_close:
            score -= 1
    if score >= int(tsmom_min_agree):
        return EnvironmentSnapshot(direction=1, trend_ok=True, close=close, moving_average=moving_average, macd_histogram=float(score))
    if score <= -int(tsmom_min_agree):
        return EnvironmentSnapshot(direction=-1, trend_ok=True, close=close, moving_average=moving_average, macd_histogram=float(score))
    return EnvironmentSnapshot(direction=0, trend_ok=False, close=close, moving_average=moving_average, macd_histogram=float(score))


def compute_environment(
    frame_1h: KlineFrame,
    ma_period: int,
    macd_fast: int,
    macd_slow: int,
    macd_signal: int,
    mode: str = 'ma_macd',
    tsmom_lookbacks: tuple[int, ...] = (24, 72, 168),
    tsmom_min_agree: int = 2,
) -> EnvironmentSnapshot:
    if str(mode).strip() == 'tsmom':
        return _compute_tsmom_environment(frame_1h, ma_period, tsmom_lookbacks, tsmom_min_agree)
    return _compute_ma_macd_environment(frame_1h, ma_period, macd_fast, macd_slow, macd_signal)
