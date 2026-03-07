from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl

from yuruquant.core.time import frequency_to_timedelta, parse_datetime

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None


KLINE_COLUMNS = ['eob', 'open', 'high', 'low', 'close', 'volume']


def _extract(item: object, key: str, default: Any = None) -> Any:
    if isinstance(item, dict):
        return item.get(key, default)
    return getattr(item, key, default)


def _to_float(value: object) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def _normalize_rows(raw: object) -> list[dict[str, object]]:
    if raw is None:
        return []
    if isinstance(raw, KlineFrame):
        return raw.frame.to_dicts()
    if isinstance(raw, pl.DataFrame):
        return raw.to_dicts()
    if pd is not None:
        if isinstance(raw, pd.DataFrame):
            return _normalize_rows(raw.to_dict('records'))
        if isinstance(raw, pd.Series):
            return _normalize_rows(raw.to_dict())
    if isinstance(raw, dict):
        items = [raw]
    elif isinstance(raw, tuple):
        items = list(raw)
    elif isinstance(raw, list):
        items = raw
    else:
        to_dict = getattr(raw, 'to_dict', None)
        if callable(to_dict):
            try:
                converted = to_dict()
            except Exception:
                converted = None
            if converted is not None and converted is not raw:
                return _normalize_rows(converted)
        items = [raw]

    rows: list[dict[str, object]] = []
    for item in items:
        eob = _extract(item, 'eob')
        if eob is None:
            continue
        rows.append(
            {
                'eob': parse_datetime(eob),
                'open': _to_float(_extract(item, 'open', 0.0)),
                'high': _to_float(_extract(item, 'high', 0.0)),
                'low': _to_float(_extract(item, 'low', 0.0)),
                'close': _to_float(_extract(item, 'close', 0.0)),
                'volume': _to_float(_extract(item, 'volume', 0.0)),
            }
        )
    return rows


@dataclass(frozen=True)
class KlineFrame:
    frame: pl.DataFrame
    symbol: str = ''
    frequency: str = ''

    @staticmethod
    def empty(symbol: str = '', frequency: str = '') -> 'KlineFrame':
        return KlineFrame(
            frame=pl.DataFrame(
                {
                    'eob': pl.Series([], dtype=pl.Datetime('us')),
                    'open': pl.Series([], dtype=pl.Float64),
                    'high': pl.Series([], dtype=pl.Float64),
                    'low': pl.Series([], dtype=pl.Float64),
                    'close': pl.Series([], dtype=pl.Float64),
                    'volume': pl.Series([], dtype=pl.Float64),
                }
            ),
            symbol=symbol,
            frequency=frequency,
        )

    def __len__(self) -> int:
        return int(self.frame.height)

    @property
    def empty_frame(self) -> bool:
        return len(self) == 0

    def latest_eob(self):
        if self.empty_frame:
            return None
        return self.frame.get_column('eob').item(-1)

    def latest_open(self) -> float:
        if self.empty_frame:
            return 0.0
        return float(self.frame.get_column('open').item(-1) or 0.0)

    def latest_open_time(self):
        latest_eob = self.latest_eob()
        if latest_eob is None:
            return None
        delta = frequency_to_timedelta(self.frequency)
        if delta is None:
            return latest_eob
        return latest_eob - delta

    def latest_close(self) -> float:
        if self.empty_frame:
            return 0.0
        return float(self.frame.get_column('close').item(-1) or 0.0)

    def latest_high(self) -> float:
        if self.empty_frame:
            return 0.0
        return float(self.frame.get_column('high').item(-1) or 0.0)

    def latest_low(self) -> float:
        if self.empty_frame:
            return 0.0
        return float(self.frame.get_column('low').item(-1) or 0.0)

    def tail(self, n: int) -> 'KlineFrame':
        return KlineFrame(self.frame.tail(max(int(n), 0)), symbol=self.symbol, frequency=self.frequency)


def ensure_kline_frame(raw: object, symbol: str = '', frequency: str = '') -> KlineFrame:
    rows = _normalize_rows(raw)
    if not rows:
        return KlineFrame.empty(symbol=symbol, frequency=frequency)
    frame = pl.DataFrame(rows).sort('eob').unique(subset=['eob'], keep='last', maintain_order=True)
    return KlineFrame(frame=frame, symbol=symbol, frequency=frequency)


@dataclass
class BarBuffer:
    symbol: str
    frequency: str
    capacity: int
    frame: KlineFrame

    @staticmethod
    def create(symbol: str, frequency: str, capacity: int) -> 'BarBuffer':
        return BarBuffer(symbol=symbol, frequency=frequency, capacity=max(int(capacity), 1), frame=KlineFrame.empty(symbol, frequency))

    def __len__(self) -> int:
        return len(self.frame)

    def replace(self, raw: object) -> None:
        frame = ensure_kline_frame(raw, symbol=self.symbol, frequency=self.frequency)
        if len(frame) > self.capacity:
            frame = frame.tail(self.capacity)
        self.frame = frame

    def append(self, raw: object) -> None:
        incoming = ensure_kline_frame(raw, symbol=self.symbol, frequency=self.frequency)
        if incoming.empty_frame:
            return
        if self.frame.empty_frame:
            self.replace(incoming)
            return
        merged = pl.concat([self.frame.frame, incoming.frame], how='vertical_relaxed')
        merged = merged.sort('eob').unique(subset=['eob'], keep='last', maintain_order=True)
        if merged.height > self.capacity:
            merged = merged.tail(self.capacity)
        self.frame = KlineFrame(frame=merged, symbol=self.symbol, frequency=self.frequency)


@dataclass
class SymbolFrames:
    symbol: str
    entry: BarBuffer
    trend: BarBuffer

    @staticmethod
    def create(symbol: str, entry_frequency: str, trend_frequency: str, entry_bars: int, trend_bars: int) -> 'SymbolFrames':
        return SymbolFrames(
            symbol=symbol,
            entry=BarBuffer.create(symbol, entry_frequency, max(int(entry_bars) * 4, 240)),
            trend=BarBuffer.create(symbol, trend_frequency, max(int(trend_bars) * 4, 160)),
        )

