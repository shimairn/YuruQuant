from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import polars as pl

from strategy.core.kline_types import KLINE_COLUMNS, KlineFrame


def _as_records_from_obj(raw: Any) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        out: list[dict[str, Any]] = []
        for item in raw:
            if isinstance(item, dict):
                out.append(dict(item))
                continue
            row = {col: getattr(item, col, None) for col in KLINE_COLUMNS}
            if any(v is not None for v in row.values()):
                out.append(row)
        return out

    if isinstance(raw, tuple):
        return _as_records_from_obj(list(raw))

    if isinstance(raw, dict):
        return [dict(raw)] if any(k in raw for k in KLINE_COLUMNS) else []

    to_dict = getattr(raw, "to_dict", None)
    if callable(to_dict):
        try:
            records = to_dict("records")
            if isinstance(records, list):
                return _as_records_from_obj(records)
        except Exception:
            pass

    if isinstance(raw, Iterable) and not isinstance(raw, (str, bytes)):
        return _as_records_from_obj(list(raw))

    row = {col: getattr(raw, col, None) for col in KLINE_COLUMNS}
    if any(v is not None for v in row.values()):
        return [row]
    return []


def _to_polars(raw: Any) -> pl.DataFrame:
    if raw is None:
        return pl.DataFrame()
    if isinstance(raw, KlineFrame):
        return raw.frame
    if isinstance(raw, pl.DataFrame):
        return raw
    if isinstance(raw, dict):
        try:
            return pl.DataFrame(raw)
        except Exception:
            return pl.DataFrame(_as_records_from_obj(raw))
    records = _as_records_from_obj(raw)
    if not records:
        return pl.DataFrame()
    return pl.DataFrame(records)


def normalize_kline_frame(raw: Any, *, symbol: str = "", frequency: str = "") -> KlineFrame:
    frame = _to_polars(raw)
    if frame.height <= 0:
        return KlineFrame.make_empty(symbol=symbol, frequency=frequency)

    for col in KLINE_COLUMNS:
        if col not in frame.columns:
            frame = frame.with_columns(pl.lit(None).alias(col))

    frame = frame.select(KLINE_COLUMNS)
    frame = frame.with_columns(
        [
            pl.col("eob").cast(pl.Utf8, strict=False).str.to_datetime(strict=False, time_unit="ns"),
            pl.col("open").cast(pl.Float64, strict=False),
            pl.col("high").cast(pl.Float64, strict=False),
            pl.col("low").cast(pl.Float64, strict=False),
            pl.col("close").cast(pl.Float64, strict=False),
            pl.col("volume").cast(pl.Float64, strict=False),
        ]
    )
    frame = frame.drop_nulls(subset=["eob", "high", "low", "close", "volume"]) 
    if frame.height <= 0:
        return KlineFrame.make_empty(symbol=symbol, frequency=frequency)

    frame = frame.sort("eob").unique(subset=["eob"], keep="last", maintain_order=True)
    return KlineFrame(frame=frame, symbol=symbol, frequency=frequency)


def ensure_kline_frame(raw: Any, *, symbol: str = "", frequency: str = "") -> KlineFrame:
    if isinstance(raw, KlineFrame):
        return raw
    return normalize_kline_frame(raw, symbol=symbol, frequency=frequency)
