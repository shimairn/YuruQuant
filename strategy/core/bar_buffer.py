from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from strategy.core.kline_normalizer import ensure_kline_frame
from strategy.core.kline_types import KlineFrame


@dataclass
class BarBuffer:
    symbol: str
    frequency: str
    capacity: int
    frame: KlineFrame

    @staticmethod
    def create(symbol: str, frequency: str, capacity: int) -> "BarBuffer":
        cap = max(int(capacity), 1)
        return BarBuffer(
            symbol=symbol,
            frequency=frequency,
            capacity=cap,
            frame=KlineFrame.make_empty(symbol=symbol, frequency=frequency),
        )

    def __len__(self) -> int:
        return len(self.frame)

    def latest_eob(self):
        return self.frame.latest_eob()

    def replace(self, raw) -> None:
        out = ensure_kline_frame(raw, symbol=self.symbol, frequency=self.frequency)
        if len(out) > self.capacity:
            out = out.tail(self.capacity)
        self.frame = out

    def append(self, raw) -> None:
        incoming = ensure_kline_frame(raw, symbol=self.symbol, frequency=self.frequency)
        if incoming.empty:
            return
        if self.frame.empty:
            self.replace(incoming)
            return

        merged = pl.concat([self.frame.frame, incoming.frame], how="vertical_relaxed")
        merged = merged.sort("eob").unique(subset=["eob"], keep="last", maintain_order=True)
        if merged.height > self.capacity:
            merged = merged.tail(self.capacity)
        self.frame = KlineFrame(frame=merged, symbol=self.symbol, frequency=self.frequency)


@dataclass
class SymbolBarStore:
    symbol: str
    frame_5m: BarBuffer
    frame_1h: BarBuffer

    @staticmethod
    def create(symbol: str, freq_5m: str, freq_1h: str, warmup_5m: int, warmup_1h: int) -> "SymbolBarStore":
        cap_5m = max(int(warmup_5m) * 4, 240)
        cap_1h = max(int(warmup_1h) * 4, 120)
        return SymbolBarStore(
            symbol=symbol,
            frame_5m=BarBuffer.create(symbol, freq_5m, cap_5m),
            frame_1h=BarBuffer.create(symbol, freq_1h, cap_1h),
        )
