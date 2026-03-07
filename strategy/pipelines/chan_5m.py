from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

import pandas as pd


class FractalType(Enum):
    TOP = "top"
    BOTTOM = "bottom"


@dataclass
class Fractal:
    index: int
    eob: object
    high: float
    low: float
    fractal_type: FractalType


@dataclass
class Bi:
    start_fractal: Fractal
    end_fractal: Fractal
    start_price: float
    end_price: float
    direction: int


@dataclass
class Zhongshu:
    start_bi_index: int
    end_bi_index: int
    zd: float
    zg: float


@dataclass
class Breakout:
    is_breakout: bool
    direction: int
    zg: float
    zd: float


def safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def to_ts(value: object) -> pd.Timestamp:
    return pd.to_datetime(value)


def normalize_kline_df(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["eob", "open", "high", "low", "close", "volume"])
    out = df.copy()
    for col in ["eob", "open", "high", "low", "close", "volume"]:
        if col not in out.columns:
            out[col] = pd.NA
    out = out[["eob", "open", "high", "low", "close", "volume"]].copy()
    out["eob"] = pd.to_datetime(out["eob"], errors="coerce")
    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["eob", "high", "low", "close"]) 
    out = out.sort_values("eob").reset_index(drop=True)
    return out


def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    src = normalize_kline_df(df)
    if src.empty:
        return pd.Series(dtype=float)

    prev_close = src["close"].shift(1)
    tr = pd.concat(
        [
            (src["high"] - src["low"]).abs(),
            (src["high"] - prev_close).abs(),
            (src["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / max(int(period), 1), adjust=False).mean()


def merge_klines(df: pd.DataFrame) -> pd.DataFrame:
    src = normalize_kline_df(df)
    if src.empty:
        return src

    result: list[pd.Series] = []
    i = 0
    n = len(src)
    while i < n:
        current = src.iloc[i].copy()
        j = i + 1
        while j < n:
            nxt = src.iloc[j]
            contained = (
                (current["high"] >= nxt["high"] and current["low"] <= nxt["low"])
                or (nxt["high"] >= current["high"] and nxt["low"] <= current["low"])
            )
            if not contained:
                break

            current["high"] = max(float(current["high"]), float(nxt["high"]))
            current["low"] = min(float(current["low"]), float(nxt["low"]))
            current["close"] = nxt["close"]
            current["eob"] = nxt["eob"]
            current["volume"] = safe_float(current["volume"]) + safe_float(nxt["volume"])
            j += 1

        result.append(current)
        i = j

    return pd.DataFrame(result).reset_index(drop=True)


def identify_fractals(df: pd.DataFrame, min_confirm_bars: int = 2) -> list[Fractal]:
    src = normalize_kline_df(df)
    out: list[Fractal] = []
    n = len(src)
    if n < 5:
        return out

    for i in range(1, n - 1):
        confirm_idx = i + max(int(min_confirm_bars), 1)
        if confirm_idx >= n:
            break

        prev_bar = src.iloc[i - 1]
        curr_bar = src.iloc[i]
        next_bar = src.iloc[i + 1]

        is_top = (
            curr_bar["high"] >= prev_bar["high"]
            and curr_bar["high"] >= next_bar["high"]
            and curr_bar["low"] >= prev_bar["low"]
            and curr_bar["low"] >= next_bar["low"]
        )
        is_bottom = (
            curr_bar["low"] <= prev_bar["low"]
            and curr_bar["low"] <= next_bar["low"]
            and curr_bar["high"] <= prev_bar["high"]
            and curr_bar["high"] <= next_bar["high"]
        )

        if is_top == is_bottom:
            continue

        out.append(
            Fractal(
                index=i,
                eob=src.iloc[confirm_idx]["eob"],
                high=safe_float(curr_bar["high"]),
                low=safe_float(curr_bar["low"]),
                fractal_type=FractalType.TOP if is_top else FractalType.BOTTOM,
            )
        )
    return out


def build_bi(
    df: pd.DataFrame,
    fractals: list[Fractal],
    min_tick: float,
    atr_multiplier: float,
    min_move_pct: float,
) -> list[Bi]:
    src = normalize_kline_df(df)
    if src.empty or len(fractals) < 2:
        return []

    atr = calculate_atr(src, period=14)
    bis: list[Bi] = []
    pending = fractals[0]

    for current in fractals[1:]:
        if current.fractal_type == pending.fractal_type:
            if pending.fractal_type == FractalType.TOP and current.high >= pending.high:
                pending = current
            elif pending.fractal_type == FractalType.BOTTOM and current.low <= pending.low:
                pending = current
            continue

        if pending.fractal_type == FractalType.BOTTOM:
            direction = 1
            start_price = pending.low
            end_price = current.high
            move = end_price - start_price
        else:
            direction = -1
            start_price = pending.high
            end_price = current.low
            move = start_price - end_price

        idx = min(max(current.index, 0), len(src) - 1)
        atr_val = safe_float(atr.iloc[idx], 0.0)
        close_val = safe_float(src.iloc[idx]["close"], 0.0)
        threshold = max(float(atr_multiplier) * atr_val, 8.0 * float(min_tick), float(min_move_pct) * close_val)
        if move < threshold:
            continue

        bis.append(
            Bi(
                start_fractal=pending,
                end_fractal=current,
                start_price=float(start_price),
                end_price=float(end_price),
                direction=direction,
            )
        )
        pending = current

    return bis


def identify_zhongshu(bis: list[Bi]) -> list[Zhongshu]:
    if len(bis) < 3:
        return []

    result: list[Zhongshu] = []
    for i in range(2, len(bis)):
        seg = bis[i - 2 : i + 1]
        highs = [max(b.start_price, b.end_price) for b in seg]
        lows = [min(b.start_price, b.end_price) for b in seg]
        zg = min(highs)
        zd = max(lows)
        if zg <= zd:
            continue

        if not result:
            result.append(Zhongshu(i - 2, i, float(zd), float(zg)))
            continue

        last = result[-1]
        overlap = not (zg <= last.zd or zd >= last.zg)
        if last.end_bi_index >= i - 2 and overlap:
            last.end_bi_index = i
            last.zd = max(last.zd, float(zd))
            last.zg = min(last.zg, float(zg))
            if last.zg <= last.zd:
                result.pop()
        else:
            result.append(Zhongshu(i - 2, i, float(zd), float(zg)))

    return result


def get_latest_platform(zhongshus: list[Zhongshu]) -> Optional[Zhongshu]:
    if not zhongshus:
        return None
    return zhongshus[-1]


def check_platform_breakout(current_price: float, platform: Optional[Zhongshu]) -> Breakout:
    if platform is None:
        return Breakout(False, 0, 0.0, 0.0)
    if platform.zg <= platform.zd:
        return Breakout(False, 0, 0.0, 0.0)

    direction = 0
    if current_price > platform.zg:
        direction = 1
    elif current_price < platform.zd:
        direction = -1

    if direction == 0:
        return Breakout(False, 0, platform.zg, platform.zd)
    return Breakout(True, direction, platform.zg, platform.zd)


def resolve_session_volume_ratio_min(
    day_min: float,
    night_min: float,
    eob: object,
    day_ranges: list[tuple[str, str]] | None = None,
    night_ranges: list[tuple[str, str]] | None = None,
) -> float:
    dt = to_ts(eob)
    minute_of_day = dt.hour * 60 + dt.minute

    def _parse_hhmm(value: str) -> int:
        hh, mm = value.split(":", 1)
        return int(hh) * 60 + int(mm)

    def _in_ranges(ranges: list[tuple[str, str]]) -> bool:
        for start, end in ranges:
            s = _parse_hhmm(start)
            e = _parse_hhmm(end)
            if s <= e:
                if s <= minute_of_day <= e:
                    return True
            else:
                if minute_of_day >= s or minute_of_day <= e:
                    return True
        return False

    day_ranges = day_ranges or [("08:30", "15:30")]
    night_ranges = night_ranges or [("00:00", "02:30"), ("21:00", "23:59")]

    if _in_ranges(day_ranges):
        return float(day_min)
    if _in_ranges(night_ranges):
        return float(night_min)
    return float(night_min)


def check_breakout_volume(current_volume: float, volume_ma: float, min_ratio: float) -> tuple[bool, float]:
    if volume_ma <= 0:
        return False, 0.0
    ratio = float(current_volume) / float(volume_ma)
    return ratio >= float(min_ratio), ratio
