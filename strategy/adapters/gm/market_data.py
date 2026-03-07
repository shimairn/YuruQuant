from __future__ import annotations

from typing import Any

from strategy.core.kline_normalizer import normalize_kline_frame
from strategy.core.kline_types import KlineFrame
from strategy.observability.log import warn


def fetch_kline(context, symbol: str, frequency: str, count: int) -> KlineFrame:
    requested_count = max(int(count), 1)
    kwargs = {
        "symbol": symbol,
        "frequency": frequency,
        "count": requested_count,
        "fields": "eob,open,high,low,close,volume",
    }
    try:
        raw: Any
        try:
            raw = context.data(**kwargs, format="row")
        except TypeError:
            raw = context.data(**kwargs)
        frame = normalize_kline_frame(raw, symbol=symbol, frequency=frequency)
        if len(frame) < requested_count:
            warn(
                "gm.market_data.short_frame",
                sample_key=f"market_data:short_frame:{symbol}:{frequency}",
                symbol=symbol,
                frequency=frequency,
                requested_count=requested_count,
                received_count=len(frame),
            )
        return frame
    except Exception as exc:
        warn(
            "gm.market_data.fetch_failed",
            sample_key=f"market_data:fetch_failed:{symbol}:{frequency}",
            symbol=symbol,
            frequency=frequency,
            requested_count=requested_count,
            err=exc,
        )
        return KlineFrame.make_empty(symbol=symbol, frequency=frequency)
