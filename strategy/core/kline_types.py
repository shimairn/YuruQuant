from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl

KLINE_COLUMNS = ["eob", "open", "high", "low", "close", "volume"]


@dataclass(frozen=True)
class KlineFrame:
    frame: pl.DataFrame
    symbol: str = ""
    frequency: str = ""

    @staticmethod
    def make_empty(symbol: str = "", frequency: str = "") -> "KlineFrame":
        return KlineFrame(
            frame=pl.DataFrame(
                {
                    "eob": pl.Series([], dtype=pl.Datetime("ns")),
                    "open": pl.Series([], dtype=pl.Float64),
                    "high": pl.Series([], dtype=pl.Float64),
                    "low": pl.Series([], dtype=pl.Float64),
                    "close": pl.Series([], dtype=pl.Float64),
                    "volume": pl.Series([], dtype=pl.Float64),
                }
            ),
            symbol=symbol,
            frequency=frequency,
        )

    def __len__(self) -> int:
        return int(self.frame.height)

    @property
    def empty(self) -> bool:
        return len(self) == 0

    def tail(self, n: int) -> "KlineFrame":
        return KlineFrame(frame=self.frame.tail(max(int(n), 0)), symbol=self.symbol, frequency=self.frequency)

    def latest(self, key: str, default: Any = None) -> Any:
        if self.empty or key not in self.frame.columns:
            return default
        return self.frame.get_column(key).item(-1)

    def latest_eob(self):
        return self.latest("eob")

    def latest_close(self) -> float:
        try:
            return float(self.latest("close", 0.0))
        except Exception:
            return 0.0
