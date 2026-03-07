import unittest

import pandas as pd

from yuruquant.core.frames import ensure_kline_frame


class FrameNormalizationTest(unittest.TestCase):
    def test_pandas_dataframe_history_normalizes(self):
        raw = pd.DataFrame(
            [
                {"eob": "2026-01-05 09:00:00", "open": 100, "high": 101, "low": 99, "close": 100.5, "volume": 1200},
                {"eob": "2026-01-05 09:05:00", "open": 100.5, "high": 102, "low": 100, "close": 101.2, "volume": 1800},
            ]
        )

        frame = ensure_kline_frame(raw, symbol="DCE.P2409", frequency="300s")

        self.assertEqual(2, len(frame))
        self.assertEqual(101.2, frame.latest_close())
        self.assertEqual("DCE.P2409", frame.symbol)
        self.assertEqual("300s", frame.frequency)


if __name__ == '__main__':
    unittest.main()
