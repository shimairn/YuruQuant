import unittest

import pandas as pd

from strategy.gm.market_data import fetch_kline


class _CtxPermissionBackoff:
    def __init__(self) -> None:
        self.calls: list[int] = []

    def data(self, symbol, frequency, count, fields):
        _ = symbol, frequency, fields
        c = int(count)
        self.calls.append(c)
        if c > 2:
            raise Exception("ERR_NO_DATA_PERMISSION")
        idx = pd.date_range(end=pd.Timestamp("2026-02-14 10:00:00"), periods=max(c, 1), freq="5min")
        return pd.DataFrame(
            {
                "eob": idx,
                "open": [1.0] * len(idx),
                "high": [1.1] * len(idx),
                "low": [0.9] * len(idx),
                "close": [1.0] * len(idx),
                "volume": [100] * len(idx),
            }
        )


class _CtxAlwaysPermissionError:
    def __init__(self) -> None:
        self.calls: list[int] = []

    def data(self, symbol, frequency, count, fields):
        _ = symbol, frequency, fields
        self.calls.append(int(count))
        raise Exception("ERR_NO_DATA_PERMISSION")


class SmokeMarketDataTests(unittest.TestCase):
    def test_fetch_kline_permission_backoff(self) -> None:
        ctx = _CtxPermissionBackoff()
        out = fetch_kline(ctx, "DCE.p2605", "300s", 8)
        self.assertGreaterEqual(len(out), 1)
        self.assertEqual(ctx.calls[:3], [8, 4, 2])

    def test_fetch_kline_permission_all_failed_returns_empty(self) -> None:
        ctx = _CtxAlwaysPermissionError()
        out = fetch_kline(ctx, "DCE.p2605", "3600s", 8)
        self.assertEqual(len(out), 0)
        self.assertEqual(ctx.calls, [8, 4, 2, 1])


if __name__ == "__main__":
    unittest.main()
