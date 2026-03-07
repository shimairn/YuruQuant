from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

# Ensure project root is importable when running as `python scripts/...`.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from akshare_adapter.futures_client import AKShareFuturesClient, save_frame


def _parse_list(value: object) -> list[str]:
    # Accept both:
    # - "--symbols a,b,c" (single arg, comma-separated)
    # - "--symbols a b c" (multiple args, PowerShell-friendly)
    if isinstance(value, (list, tuple)):
        tokens = [str(x) for x in value]
    else:
        tokens = [str(value or "")]
    out: list[str] = []
    for token in tokens:
        parts = [x.strip() for x in str(token).split(",")]
        out.extend([x for x in parts if x])
    return out


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Download futures bars from AKShare and save to local CSV cache."
    )
    p.add_argument(
        "--symbols",
        required=True,
        nargs="+",
        help="Runtime symbols, e.g. CZCE.ap CFFEX.IC SHFE.rb (or comma-separated in one arg).",
    )
    p.add_argument(
        "--freqs",
        nargs="+",
        default=["300s", "3600s"],
        help="Frequencies, e.g. 300s 3600s (or comma-separated in one arg).",
    )
    p.add_argument(
        "--out-dir",
        default="data/akshare",
        help="Output directory for cached CSV files.",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=1.0,
        help="Sleep seconds between API requests (avoid being blocked).",
    )
    p.add_argument(
        "--start",
        default="",
        help="Optional start datetime, e.g. 2025-08-20 00:00:00",
    )
    p.add_argument(
        "--end",
        default="",
        help="Optional end datetime, e.g. 2026-02-13 15:00:00",
    )
    return p.parse_args()


def _clip_frame_by_time(df: pd.DataFrame, start_text: str, end_text: str) -> pd.DataFrame:
    out = df.copy()
    if len(out) == 0:
        return out
    if str(start_text or "").strip():
        start_ts = pd.to_datetime(start_text)
        out = out[out["eob"] >= start_ts]
    if str(end_text or "").strip():
        end_ts = pd.to_datetime(end_text)
        out = out[out["eob"] <= end_ts]
    return out.reset_index(drop=True)


def main() -> int:
    args = _parse_args()
    symbols = _parse_list(args.symbols)
    freqs = _parse_list(args.freqs)
    out_dir = Path(args.out_dir)

    if not symbols:
        raise ValueError("symbols is empty")
    if not freqs:
        raise ValueError("freqs is empty")

    client = AKShareFuturesClient(request_sleep_seconds=float(args.sleep))

    print(
        f"akshare prepare start symbols={len(symbols)} freqs={len(freqs)} "
        f"out_dir={out_dir.as_posix()} start={args.start or '-'} end={args.end or '-'}"
    )
    for csymbol in symbols:
        for freq in freqs:
            df = client.fetch_by_csymbol(csymbol=csymbol, freq=freq)
            df = _clip_frame_by_time(df, start_text=args.start, end_text=args.end)
            out_file = out_dir / csymbol / f"{freq}.csv"
            save_frame(df, out_file)
            print(f"saved csymbol={csymbol} freq={freq} rows={len(df)} file={out_file.as_posix()}")
    print("akshare prepare done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
