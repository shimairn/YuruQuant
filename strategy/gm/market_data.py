from __future__ import annotations

from typing import Any

import pandas as pd


def _safe_empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["eob", "open", "high", "low", "close", "volume"])


def _normalize_kline_df(df: Any) -> pd.DataFrame:
    if df is None or len(df) == 0:
        print("market_data warning empty input frame")
        return _safe_empty_frame()

    out = df.copy()
    for col in ["eob", "open", "high", "low", "close", "volume"]:
        if col not in out.columns:
            out[col] = pd.NA
            print(f"market_data warning missing column filled col={col}")
    out = out[["eob", "open", "high", "low", "close", "volume"]].copy()
    raw_len = len(out)
    out["eob"] = pd.to_datetime(out["eob"], errors="coerce")
    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["eob", "high", "low", "close"])
    cleaned_len = len(out)
    if cleaned_len < raw_len:
        print(f"market_data warning dropped_rows raw={raw_len} cleaned={cleaned_len}")
    out = out.sort_values("eob").drop_duplicates(subset=["eob"], keep="last").reset_index(drop=True)
    return out


def fetch_kline(context, symbol: str, frequency: str, count: int) -> pd.DataFrame:
    requested = max(int(count), 1)
    attempt = requested
    last_exc: Exception | None = None

    while attempt >= 1:
        try:
            raw = context.data(
                symbol=symbol,
                frequency=frequency,
                count=attempt,
                fields="eob,open,high,low,close,volume",
            )
            out = _normalize_kline_df(raw)
            if len(out) < attempt:
                print(f"market_data warning short_frame symbol={symbol} freq={frequency} req={attempt} got={len(out)}")
            return out
        except Exception as exc:
            last_exc = exc
            if attempt == 1:
                break
            next_attempt = max(1, attempt // 2)
            print(
                f"market_data warning fetch_retry symbol={symbol} freq={frequency} "
                f"count={attempt} -> {next_attempt} err={exc}"
            )
            if next_attempt == attempt:
                break
            attempt = next_attempt

    print(
        f"market_data warning fetch_failed symbol={symbol} freq={frequency} "
        f"req={requested} err={last_exc}"
    )
    return _safe_empty_frame()
