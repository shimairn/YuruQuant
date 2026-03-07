from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import unittest

import polars as pl

from yuruquant.app.config import load_config
from yuruquant.core.frames import KlineFrame
from yuruquant.core.models import EntrySignal, PortfolioRuntime
from yuruquant.core.time import make_event_id
from yuruquant.strategy.trend_breakout import build_managed_position, compute_environment, evaluate_exit_signal, maybe_generate_entry
from yuruquant.strategy.trend_breakout.risk_model import resolve_order_qty


def build_trend_frame(direction: int) -> KlineFrame:
    rows: list[dict[str, object]] = []
    base = datetime(2026, 1, 1, 9, 0, 0)
    for index in range(140):
        close = 100.0 + index * 0.25 if direction > 0 else 140.0 - index * 0.25
        rows.append(
            {
                'eob': base + timedelta(hours=index),
                'open': close - 0.1,
                'high': close + 0.5,
                'low': close - 0.5,
                'close': close,
                'volume': 1000.0,
            }
        )
    return KlineFrame(frame=pl.DataFrame(rows), symbol='DCE.P2409', frequency='3600s')


def build_breakout_frame(
    up: bool = True,
    latest_close: float | None = None,
    latest_high: float | None = None,
    latest_low: float | None = None,
) -> KlineFrame:
    rows: list[dict[str, object]] = []
    base = datetime(2026, 1, 5, 8, 20, 0)
    for index in range(80):
        mid = 100.0 + (index % 6) * 0.05
        rows.append(
            {
                'eob': base + timedelta(minutes=5 * index),
                'open': mid - 0.15,
                'high': mid + 0.60,
                'low': mid - 0.60,
                'close': mid + 0.05,
                'volume': 1000.0 + index,
            }
        )
    if latest_close is None:
        latest_close = float(rows[-2]['high']) + 0.70 if up else float(rows[-2]['low']) - 0.70
    rows[-1]['close'] = latest_close
    rows[-1]['high'] = latest_high if latest_high is not None else max(float(rows[-1]['high']), latest_close + 0.20)
    rows[-1]['low'] = latest_low if latest_low is not None else min(float(rows[-1]['low']), latest_close - 0.20)
    return KlineFrame(frame=pl.DataFrame(rows), symbol='DCE.P2409', frequency='300s')


def build_trend_ride_frame(last_values: list[float]) -> KlineFrame:
    rows: list[dict[str, object]] = []
    base = datetime(2026, 1, 5, 8, 20, 0)
    for index in range(70):
        close = 102.0 + index * 0.05
        if index >= 68:
            close = last_values[index - 68]
        rows.append(
            {
                'eob': base + timedelta(minutes=5 * index),
                'open': close - 0.1,
                'high': close + 0.4,
                'low': close - 0.4,
                'close': close,
                'volume': 1200.0,
            }
        )
    return KlineFrame(frame=pl.DataFrame(rows), symbol='DCE.P2409', frequency='300s')


class StrategyRulesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = load_config(Path('config/strategy.yaml'))
        self.portfolio = PortfolioRuntime(current_equity=500000.0, effective_risk_mult=1.0)

    def test_environment_detects_long_short_and_flat(self):
        long_env = compute_environment(build_trend_frame(direction=1), 60, 12, 26, 9)
        short_env = compute_environment(build_trend_frame(direction=-1), 60, 12, 26, 9)
        flat_env = compute_environment(KlineFrame(frame=pl.DataFrame([]), symbol='DCE.P2409', frequency='3600s'), 60, 12, 26, 9)
        self.assertEqual(long_env.direction, 1)
        self.assertTrue(long_env.trend_ok)
        self.assertEqual(short_env.direction, -1)
        self.assertTrue(short_env.trend_ok)
        self.assertFalse(flat_env.trend_ok)

    def test_breakout_requires_matching_environment(self):
        environment = compute_environment(build_trend_frame(direction=1), 60, 12, 26, 9)
        frame = build_breakout_frame(up=True)
        signal = maybe_generate_entry(self.config, self.portfolio, environment, 'DCE.P', frame, frame.latest_eob())
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, 'buy')

        short_environment = compute_environment(build_trend_frame(direction=-1), 60, 12, 26, 9)
        blocked = maybe_generate_entry(self.config, self.portfolio, short_environment, 'DCE.P', frame, frame.latest_eob())
        self.assertIsNone(blocked)

    def test_channel_width_filter_blocks_narrow_setup(self):
        environment = compute_environment(build_trend_frame(direction=1), 60, 12, 26, 9)
        frame = build_breakout_frame(up=True)
        self.config.strategy.entry.min_channel_width_atr = 2.0
        signal = maybe_generate_entry(self.config, self.portfolio, environment, 'DCE.P', frame, frame.latest_eob())
        self.assertIsNone(signal)

    def test_breakout_atr_buffer_blocks_marginal_breakout(self):
        environment = compute_environment(build_trend_frame(direction=1), 60, 12, 26, 9)
        frame = build_breakout_frame(up=True)
        self.config.strategy.entry.breakout_atr_buffer = 1.5
        signal = maybe_generate_entry(self.config, self.portfolio, environment, 'DCE.P', frame, frame.latest_eob())
        self.assertIsNone(signal)

    def test_close_position_filter_blocks_weak_breakout_bar(self):
        environment = compute_environment(build_trend_frame(direction=1), 60, 12, 26, 9)
        frame = build_breakout_frame(up=True, latest_close=101.25, latest_high=102.05, latest_low=101.05)
        signal = maybe_generate_entry(self.config, self.portfolio, environment, 'DCE.P', frame, frame.latest_eob())
        self.assertIsNone(signal)

    def test_risk_budget_position_sizing_rounds_down_and_rejects_too_small(self):
        spec = self.config.universe.instrument_defaults
        qty = resolve_order_qty(self.portfolio, spec, 0.015, 100.0, 2.0, 2.2)
        self.assertEqual(170, qty)
        tiny = resolve_order_qty(self.portfolio, spec, 0.000001, 100.0, 20.0, 2.2)
        self.assertEqual(0, tiny)

    def test_exit_state_machine_moves_to_protected_and_trend_ride(self):
        entry = EntrySignal(
            action='buy',
            reason='dual_core_breakout',
            direction=1,
            qty=2,
            price=100.0,
            stop_loss=97.8,
            protected_stop_price=100.6,
            created_at=datetime(2026, 1, 5, 10, 0, 0),
            entry_atr=1.0,
            breakout_anchor=100.5,
            campaign_id=make_event_id('DCE.P', datetime(2026, 1, 5, 10, 0, 0)),
            environment_ma=99.0,
            macd_histogram=0.4,
        )
        position = build_managed_position(entry)

        protected_frame = build_breakout_frame(up=True, latest_close=103.5)
        signal = evaluate_exit_signal(self.config, position, protected_frame, protected_frame.latest_eob(), 10.0, 0.003)
        self.assertIsNone(signal)
        self.assertEqual('protected', position.phase)
        self.assertAlmostEqual(100.6, position.stop_loss, places=6)

        trend_ride_frame = build_trend_ride_frame([106.0, 105.8])
        signal = evaluate_exit_signal(self.config, position, trend_ride_frame, trend_ride_frame.latest_eob(), 10.0, 0.003)
        self.assertIsNone(signal)
        self.assertEqual('trend_ride', position.phase)
        self.assertGreaterEqual(position.stop_loss, position.protected_stop_price)

    def test_trend_ride_exits_on_ma_stop(self):
        entry = EntrySignal(
            action='buy',
            reason='dual_core_breakout',
            direction=1,
            qty=2,
            price=100.0,
            stop_loss=97.8,
            protected_stop_price=100.6,
            created_at=datetime(2026, 1, 5, 10, 0, 0),
            entry_atr=1.0,
            breakout_anchor=100.5,
            campaign_id=make_event_id('DCE.P', datetime(2026, 1, 5, 10, 0, 0)),
            environment_ma=99.0,
            macd_histogram=0.4,
        )
        position = build_managed_position(entry)
        setup_frame = build_trend_ride_frame([106.0, 105.8])
        evaluate_exit_signal(self.config, position, setup_frame, setup_frame.latest_eob(), 10.0, 0.003)
        self.assertEqual('trend_ride', position.phase)

        exit_frame = build_trend_ride_frame([106.0, 102.0])
        signal = evaluate_exit_signal(self.config, position, exit_frame, exit_frame.latest_eob(), 10.0, 0.003)
        self.assertIsNotNone(signal)
        self.assertEqual('trend_ma_stop', signal.exit_trigger)


if __name__ == '__main__':
    unittest.main()
