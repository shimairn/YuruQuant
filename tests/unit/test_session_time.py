from __future__ import annotations

from datetime import datetime, timezone
import unittest
from pathlib import Path

from yuruquant.app.config import load_config
from yuruquant.core.time import exchange_datetime, to_exchange_trade_day
from yuruquant.core.session_clock import blocked_by_session_end, current_session_snapshot, current_session_window, major_session_end_approaching, trading_day_end_approaching


class SessionTimeTest(unittest.TestCase):
    def test_exchange_datetime_and_trade_day_handle_utc_and_night_session(self):
        local_dt = exchange_datetime(datetime(2026, 1, 5, 1, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(datetime(2026, 1, 5, 9, 0, 0), local_dt)
        self.assertEqual('2026-01-06', to_exchange_trade_day(datetime(2026, 1, 5, 21, 5, 0)))
        self.assertEqual('2026-01-06', to_exchange_trade_day(datetime(2026, 1, 6, 1, 5, 0)))

    def test_weekend_night_session_maps_to_next_trading_day(self):
        self.assertEqual('2026-01-12', to_exchange_trade_day(datetime(2026, 1, 9, 21, 5, 0)))
        self.assertEqual('2026-01-12', to_exchange_trade_day(datetime(2026, 1, 10, 1, 5, 0)))
        self.assertEqual('2026-01-12', to_exchange_trade_day(datetime(2026, 1, 11, 10, 0, 0)))

    def test_lunch_break_is_out_of_session(self):
        config = load_config(Path('config/strategy.yaml'))
        spec = config.universe.instrument_defaults
        current = current_session_window(spec, datetime(2026, 1, 5, 11, 25, 0))
        self.assertIsNotNone(current)
        self.assertEqual(5, current[1])
        self.assertIsNone(current_session_window(spec, datetime(2026, 1, 5, 12, 0, 0)))
        self.assertFalse(major_session_end_approaching(spec, datetime(2026, 1, 5, 11, 25, 0), '300s', 1, 180))
        self.assertFalse(trading_day_end_approaching(spec, datetime(2026, 1, 5, 11, 25, 0), '300s', 1))

    def test_cross_midnight_night_session_uses_exchange_local_clock(self):
        config = load_config(Path('config/smoke_dual_core.yaml'))
        spec = config.universe.instrument_overrides['SHFE.AG']
        current = current_session_window(spec, datetime(2026, 1, 6, 2, 25, 0))
        self.assertIsNotNone(current)
        self.assertEqual(5, current[1])
        self.assertTrue(blocked_by_session_end(spec, datetime(2026, 1, 6, 2, 25, 0), '300s', 1))
        self.assertFalse(trading_day_end_approaching(spec, datetime(2026, 1, 6, 2, 25, 0), '300s', 1))

    def test_friday_night_session_rolls_next_start_across_weekend(self):
        config = load_config(Path('config/smoke_dual_core.yaml'))
        spec = config.universe.instrument_overrides['SHFE.AG']
        snapshot = current_session_snapshot(spec, datetime(2026, 1, 9, 23, 15, 0))
        self.assertIsNotNone(snapshot)
        self.assertEqual('2026-01-12', snapshot.current_trade_day)
        self.assertEqual('2026-01-12', snapshot.next_trade_day)
        self.assertEqual(datetime(2026, 1, 11, 21, 0, 0), snapshot.next_start_dt)
        self.assertFalse(trading_day_end_approaching(spec, datetime(2026, 1, 10, 2, 25, 0), '300s', 1))

    def test_aware_timestamp_maps_to_day_close_and_major_gap(self):
        config = load_config(Path('config/strategy.yaml'))
        spec = config.universe.instrument_defaults
        aware_utc = datetime(2026, 1, 5, 6, 55, 0, tzinfo=timezone.utc)
        self.assertTrue(blocked_by_session_end(spec, aware_utc, '300s', 1))
        self.assertTrue(major_session_end_approaching(spec, aware_utc, '300s', 1, 180))
        self.assertTrue(trading_day_end_approaching(spec, aware_utc, '300s', 1))


if __name__ == '__main__':
    unittest.main()
