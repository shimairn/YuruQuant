from __future__ import annotations

from yuruquant.core.frames import KlineFrame


def _series(frame: KlineFrame, column: str) -> list[float]:
    if frame.empty_frame:
        return []
    return [float(value or 0.0) for value in frame.frame.get_column(column).to_list()]


def sma(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    window = max(int(period), 1)
    out: list[float] = []
    running = 0.0
    for index, value in enumerate(values):
        running += float(value)
        if index >= window:
            running -= float(values[index - window])
        size = window if index + 1 >= window else index + 1
        out.append(running / size)
    return out


def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (max(int(period), 1) + 1.0)
    out: list[float] = []
    current = float(values[0])
    for value in values:
        current = alpha * float(value) + (1.0 - alpha) * current
        out.append(current)
    return out


def atr(values_high: list[float], values_low: list[float], values_close: list[float], period: int) -> list[float]:
    if not values_close:
        return []
    alpha = 1.0 / max(int(period), 1)
    out: list[float] = []
    prev_close = values_close[0]
    current = abs(values_high[0] - values_low[0])
    out.append(current)
    for idx in range(1, len(values_close)):
        tr = max(
            abs(values_high[idx] - values_low[idx]),
            abs(values_high[idx] - prev_close),
            abs(values_low[idx] - prev_close),
        )
        current = alpha * tr + (1.0 - alpha) * current
        out.append(current)
        prev_close = values_close[idx]
    return out


def macd_histogram(values: list[float], fast: int, slow: int, signal: int) -> list[float]:
    if not values:
        return []
    fast_line = ema(values, fast)
    slow_line = ema(values, slow)
    macd_line = [fast_value - slow_value for fast_value, slow_value in zip(fast_line, slow_line)]
    signal_line = ema(macd_line, signal)
    return [macd_value - signal_value for macd_value, signal_value in zip(macd_line, signal_line)]


def latest_sma(frame: KlineFrame, period: int, column: str = "close") -> float:
    out = sma(_series(frame, column), period)
    return float(out[-1]) if out else 0.0


def latest_atr(frame: KlineFrame, period: int) -> float:
    out = atr(_series(frame, "high"), _series(frame, "low"), _series(frame, "close"), period)
    return float(out[-1]) if out else 0.0


def latest_macd_histogram(frame: KlineFrame, fast: int, slow: int, signal: int, column: str = "close") -> float:
    out = macd_histogram(_series(frame, column), fast, slow, signal)
    return float(out[-1]) if out else 0.0


def latest_donchian_channel(frame: KlineFrame, lookback: int) -> tuple[float, float, float]:
    n = len(frame)
    lb = max(int(lookback), 2)
    if n <= lb:
        return 0.0, 0.0, 0.0
    hist = frame.frame.slice(max(0, n - lb - 1), lb)
    upper = float(hist.select("high").max().item(0, 0) or 0.0)
    lower = float(hist.select("low").min().item(0, 0) or 0.0)
    close = frame.latest_close()
    return upper, lower, close
