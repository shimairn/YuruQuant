from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


def _safe_empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["eob", "open", "high", "low", "close", "volume"])


def csymbol_to_sina_symbol(csymbol: str) -> str:
    """
    Convert runtime symbol format (e.g. CZCE.ap, CFFEX.IC) to Sina futures symbol (e.g. AP0, IC0).
    """
    text = str(csymbol or "").strip()
    if not text:
        raise ValueError("csymbol is empty")
    if "." in text:
        _exchange, product = text.split(".", 1)
    else:
        product = text
    product = product.strip().upper()
    if not product:
        raise ValueError(f"invalid csymbol: {csymbol}")
    return f"{product}0"


def freq_to_ak_period(freq: str) -> str | None:
    raw = str(freq or "").strip().lower()
    mapping = {
        "60s": "1",
        "1m": "1",
        "300s": "5",
        "5m": "5",
        "900s": "15",
        "15m": "15",
        "1800s": "30",
        "30m": "30",
        "3600s": "60",
        "1h": "60",
    }
    return mapping.get(raw)


def _normalize_ak_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return _safe_empty_frame()

    out = df.copy()
    rename_map = {
        "datetime": "eob",
        "date": "eob",
        "time": "eob",
    }
    out = out.rename(columns=rename_map)

    required = ["eob", "open", "high", "low", "close", "volume"]
    for col in required:
        if col not in out.columns:
            out[col] = pd.NA
    out = out[required].copy()

    out["eob"] = pd.to_datetime(out["eob"], errors="coerce")
    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["eob", "high", "low", "close"])
    out = out.sort_values("eob").drop_duplicates(subset=["eob"], keep="last").reset_index(drop=True)
    return out


@dataclass
class AKShareFuturesClient:
    request_sleep_seconds: float = 1.0

    def __post_init__(self) -> None:
        try:
            import akshare as ak  # type: ignore
        except Exception as exc:  # pragma: no cover - depends on local environment
            raise RuntimeError(
                "AKShare is not installed. Run: pip install akshare"
            ) from exc
        self._ak = ak

    def fetch_minute(self, sina_symbol: str, period: str) -> pd.DataFrame:
        raw = self._ak.futures_zh_minute_sina(symbol=sina_symbol, period=period)
        if self.request_sleep_seconds > 0:
            time.sleep(float(self.request_sleep_seconds))
        return _normalize_ak_frame(raw)

    def fetch_daily(self, sina_symbol: str) -> pd.DataFrame:
        raw = self._ak.futures_zh_daily_sina(symbol=sina_symbol)
        if self.request_sleep_seconds > 0:
            time.sleep(float(self.request_sleep_seconds))
        return _normalize_ak_frame(raw)

    def fetch_by_csymbol(self, csymbol: str, freq: str) -> pd.DataFrame:
        sina_symbol = csymbol_to_sina_symbol(csymbol)
        period = freq_to_ak_period(freq)
        if period is not None:
            return self.fetch_minute(sina_symbol=sina_symbol, period=period)
        if str(freq).strip().lower() in {"1d", "d", "day"}:
            return self.fetch_daily(sina_symbol=sina_symbol)
        raise ValueError(f"unsupported freq for akshare: {freq}")


def save_frame(df: pd.DataFrame, output_file: Path) -> Path:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8")
    return output_file

