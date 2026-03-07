from __future__ import annotations

import statistics
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from strategy.config import load_config
from strategy.core.engine import StrategyEngine


@dataclass
class Bar:
    symbol: str
    frequency: str
    eob: object
    open: float
    high: float
    low: float
    close: float
    volume: float


class _Account:
    cash = {"nav": 500000.0}

    def position(self, symbol: str, side):
        _ = symbol
        _ = side
        return None


class _Context:
    def __init__(self) -> None:
        self.now = datetime(2026, 1, 5, 9, 0, 0)

    def account(self):
        return _Account()

    def data(self, **kwargs):
        symbol = kwargs["symbol"]
        freq = kwargs["frequency"]
        count = int(kwargs["count"])
        step = 5 if str(freq).lower() in {"300s", "5m", "5min"} else 60
        base = self.now - timedelta(minutes=step * count)
        out = []
        price = 100.0
        for i in range(count):
            t = base + timedelta(minutes=step * (i + 1))
            out.append(
                {
                    "symbol": symbol,
                    "eob": t,
                    "open": price,
                    "high": price + 1.0,
                    "low": price - 1.0,
                    "close": price + 0.1,
                    "volume": 1000.0,
                }
            )
            price += 0.03
        return out


def main() -> None:
    cfg = load_config(Path("config/strategy.yaml"))
    cfg.runtime.symbols = [f"DCE.t{i:02d}" for i in range(1, 51)]

    engine = StrategyEngine(cfg)
    ctx = _Context()
    engine.initialize_runtime(ctx)
    for csymbol in cfg.runtime.symbols:
        engine.set_symbol_mapping(csymbol, f"{csymbol}.SIM")

    latencies: list[float] = []
    base = datetime(2026, 1, 6, 9, 0, 0)

    for idx in range(150):
        ctx.now = base + timedelta(minutes=5 * idx)
        bars = []
        for i, csymbol in enumerate(cfg.runtime.symbols):
            px = 100.0 + i * 0.1 + idx * 0.02
            bars.append(
                Bar(
                    symbol=f"{csymbol}.SIM",
                    frequency=cfg.runtime.freq_5m,
                    eob=ctx.now,
                    open=px,
                    high=px + 0.8,
                    low=px - 0.6,
                    close=px + 0.1,
                    volume=1000.0 + i,
                )
            )

        t0 = time.perf_counter()
        engine.process_symbols_by_bars(bars)
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000.0)

    latencies.sort()
    p50 = statistics.median(latencies)
    p95 = latencies[int(len(latencies) * 0.95) - 1]
    p99 = latencies[int(len(latencies) * 0.99) - 1]
    print(f"bars={len(latencies)} p50={p50:.2f}ms p95={p95:.2f}ms p99={p99:.2f}ms")


if __name__ == "__main__":
    main()
