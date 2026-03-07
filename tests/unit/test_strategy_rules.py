from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import unittest
from unittest.mock import patch

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


def build_hourly_reversal_frame_for_long_exit() -> KlineFrame:
    rows: list[dict[str, object]] = []
    base = datetime(2026, 1, 1, 9, 0, 0)
    for index in range(139):
        close = 100.0 + index * 0.25
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
    close = 115.0
    rows.append(
        {
            'eob': base + timedelta(hours=139),
            'open': close + 0.3,
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
    end_eob: datetime | None = None,
) -> KlineFrame:
    rows: list[dict[str, object]] = []
    total_bars = 80
    first_eob = end_eob - timedelta(minutes=5 * (total_bars - 1)) if end_eob is not None else datetime(2026, 1, 5, 8, 20, 0)
    for index in range(total_bars):
        mid = 100.0 + (index % 6) * 0.05
        rows.append(
            {
                'eob': first_eob + timedelta(minutes=5 * index),
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


class StrategyRulesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = load_config(Path('config/strategy.yaml'))
        self.portfolio = PortfolioRuntime(current_equity=500000.0, effective_risk_mult=1.0)
        self.spec = self.config.universe.instrument_defaults
        self.long_environment = compute_environment(build_trend_frame(direction=1), 60, 12, 26, 9)
        self.short_environment = compute_environment(build_trend_frame(direction=-1), 60, 12, 26, 9)
        self.reversal_environment = compute_environment(build_hourly_reversal_frame_for_long_exit(), 60, 12, 26, 9)

    def _entry_signal(self) -> EntrySignal:
        created_at = datetime(2026, 1, 5, 10, 0, 0)
        return EntrySignal(
            action='buy',
            reason='dual_core_breakout',
            direction=1,
            qty=2,
            price=100.0,
            stop_loss=97.8,
            protected_stop_price=100.6,
            created_at=created_at,
            entry_atr=1.0,
            breakout_anchor=100.5,
            campaign_id=make_event_id('DCE.P', created_at),
            environment_ma=99.0,
            macd_histogram=0.4,
        )

    def test_environment_detects_long_short_and_flat(self):
        flat_env = compute_environment(KlineFrame(frame=pl.DataFrame([]), symbol='DCE.P2409', frequency='3600s'), 60, 12, 26, 9)
        self.assertEqual(self.long_environment.direction, 1)
        self.assertTrue(self.long_environment.trend_ok)
        self.assertEqual(self.short_environment.direction, -1)
        self.assertTrue(self.short_environment.trend_ok)
        self.assertFalse(flat_env.trend_ok)

    def test_environment_requires_macd_confirmation(self):
        with patch('yuruquant.strategy.trend_breakout.environment.latest_macd_histogram', return_value=-9.0):
            environment = compute_environment(build_trend_frame(direction=1), 60, 12, 26, 9)
        self.assertFalse(environment.trend_ok)
        self.assertEqual(0, environment.direction)
        self.assertEqual(-9.0, environment.macd_histogram)

    def test_breakout_requires_matching_environment(self):
        frame = build_breakout_frame(up=True)
        signal = maybe_generate_entry(self.config, self.portfolio, self.long_environment, 'DCE.P', frame, frame.latest_eob())
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, 'buy')

        blocked = maybe_generate_entry(self.config, self.portfolio, self.short_environment, 'DCE.P', frame, frame.latest_eob())
        self.assertIsNone(blocked)

    def test_channel_width_filter_blocks_narrow_setup(self):
        frame = build_breakout_frame(up=True)
        self.config.strategy.entry.min_channel_width_atr = 2.0
        signal = maybe_generate_entry(self.config, self.portfolio, self.long_environment, 'DCE.P', frame, frame.latest_eob())
        self.assertIsNone(signal)

    def test_breakout_atr_buffer_blocks_marginal_breakout(self):
        frame = build_breakout_frame(up=True)
        self.config.strategy.entry.breakout_atr_buffer = 1.5
        signal = maybe_generate_entry(self.config, self.portfolio, self.long_environment, 'DCE.P', frame, frame.latest_eob())
        self.assertIsNone(signal)

    def test_session_end_buffer_blocks_late_breakout(self):
        frame = build_breakout_frame(up=True)
        self.config.strategy.entry.session_end_buffer_bars = 3
        signal = maybe_generate_entry(self.config, self.portfolio, self.long_environment, 'DCE.P', frame, frame.latest_eob())
        self.assertIsNone(signal)

    def test_weak_close_position_breakout_is_now_allowed(self):
        frame = build_breakout_frame(up=True, latest_close=101.25, latest_high=102.05, latest_low=101.05)
        signal = maybe_generate_entry(self.config, self.portfolio, self.long_environment, 'DCE.P', frame, frame.latest_eob())
        self.assertIsNotNone(signal)
        self.assertEqual('buy', signal.action)

    def test_risk_budget_position_sizing_rounds_down_and_rejects_too_small(self):
        qty = resolve_order_qty(self.portfolio, self.spec, 0.015, 100.0, 2.0, 2.2)
        self.assertEqual(170, qty)
        tiny = resolve_order_qty(self.portfolio, self.spec, 0.000001, 100.0, 20.0, 2.2)
        self.assertEqual(0, tiny)

    def test_mfe_uses_bar_high_and_moves_to_protected(self):
        position = build_managed_position(self._entry_signal())
        frame = build_breakout_frame(up=True, latest_close=101.0, latest_high=103.0, latest_low=100.7)
        signal = evaluate_exit_signal(self.config, position, frame, self.long_environment, frame.latest_eob(), self.spec, 10.0, 0.003)
        self.assertIsNone(signal)
        self.assertEqual('protected', position.phase)
        self.assertGreaterEqual(position.mfe_r, 1.2)
        self.assertAlmostEqual(100.6, position.stop_loss, places=6)

    def test_protected_moves_to_ascended_at_2_5r(self):
        position = build_managed_position(self._entry_signal())
        protected_frame = build_breakout_frame(up=True, latest_close=101.0, latest_high=103.0, latest_low=100.7)
        evaluate_exit_signal(self.config, position, protected_frame, self.long_environment, protected_frame.latest_eob(), self.spec, 10.0, 0.003)
        ascended_frame = build_breakout_frame(up=True, latest_close=104.0, latest_high=105.8, latest_low=103.8)
        signal = evaluate_exit_signal(self.config, position, ascended_frame, self.long_environment, ascended_frame.latest_eob(), self.spec, 10.0, 0.003)
        self.assertIsNone(signal)
        self.assertEqual('ascended', position.phase)
        self.assertAlmostEqual(100.6, position.stop_loss, places=6)

    def test_ascended_ignores_5m_noise_above_protected_floor(self):
        position = build_managed_position(self._entry_signal())
        position.phase = 'ascended'
        position.stop_loss = 100.6
        position.protected_stop_price = 100.6
        frame = build_breakout_frame(up=True, latest_close=100.8, latest_high=101.4, latest_low=100.0)
        signal = evaluate_exit_signal(self.config, position, frame, self.long_environment, frame.latest_eob(), self.spec, 10.0, 0.003)
        self.assertIsNone(signal)
        self.assertEqual('ascended', position.phase)

    def test_ascended_exits_on_hourly_ma_reversal(self):
        position = build_managed_position(self._entry_signal())
        position.phase = 'ascended'
        position.stop_loss = 100.6
        position.protected_stop_price = 100.6
        frame = build_breakout_frame(up=True, latest_close=101.0, latest_high=101.2, latest_low=100.8)
        signal = evaluate_exit_signal(self.config, position, frame, self.reversal_environment, frame.latest_eob(), self.spec, 10.0, 0.003)
        self.assertIsNotNone(signal)
        self.assertEqual('hourly_ma_stop', signal.exit_trigger)
        self.assertEqual('ascended', signal.phase)

    def test_armed_flush_triggers_only_before_major_session_gap(self):
        self.config.strategy.exit.armed_flush_buffer_bars = 1
        self.config.strategy.exit.armed_flush_min_gap_minutes = 180
        position = build_managed_position(self._entry_signal())
        frame = build_breakout_frame(
            up=True,
            latest_close=100.4,
            latest_high=100.8,
            latest_low=99.8,
            end_eob=datetime(2026, 1, 5, 14, 55, 0),
        )
        signal = evaluate_exit_signal(self.config, position, frame, self.long_environment, frame.latest_eob(), self.spec, 10.0, 0.003)
        self.assertIsNotNone(signal)
        self.assertEqual('armed_flush', signal.exit_trigger)
        self.assertEqual('armed', signal.phase)

    def test_armed_flush_skips_short_break_and_non_armed_positions(self):
        self.config.strategy.exit.armed_flush_buffer_bars = 1
        self.config.strategy.exit.armed_flush_min_gap_minutes = 180

        lunch_position = build_managed_position(self._entry_signal())
        lunch_frame = build_breakout_frame(
            up=True,
            latest_close=100.4,
            latest_high=100.8,
            latest_low=99.8,
            end_eob=datetime(2026, 1, 5, 11, 25, 0),
        )
        lunch_signal = evaluate_exit_signal(self.config, lunch_position, lunch_frame, self.long_environment, lunch_frame.latest_eob(), self.spec, 10.0, 0.003)
        self.assertIsNone(lunch_signal)

        protected_position = build_managed_position(self._entry_signal())
        protected_position.phase = 'protected'
        protected_position.stop_loss = 100.6
        protected_position.protected_stop_price = 100.6
        day_close_frame = build_breakout_frame(
            up=True,
            latest_close=100.8,
            latest_high=101.0,
            latest_low=100.7,
            end_eob=datetime(2026, 1, 5, 14, 55, 0),
        )
        protected_signal = evaluate_exit_signal(self.config, protected_position, day_close_frame, self.long_environment, day_close_frame.latest_eob(), self.spec, 10.0, 0.003)
        self.assertIsNone(protected_signal)


if __name__ == '__main__':
    unittest.main()
