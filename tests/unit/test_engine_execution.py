from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import unittest

import polars as pl

from yuruquant.app.config import load_config
from yuruquant.core.engine_execution import execute_due_signal, signal_accepted
from yuruquant.core.frames import KlineFrame, SymbolFrames
from yuruquant.core.models import EntrySignal, ExecutionResult, ExitSignal, ManagedPosition, OrderIntent, RuntimeState, SymbolRuntime


class _NoOpReportSink:
    def record_executions(self, *args, **kwargs) -> None:
        _ = args
        _ = kwargs


class _AcceptedGateway:
    def plan_order_intents(self, symbol: str, signal) -> list[OrderIntent]:
        return [OrderIntent(symbol=symbol, side='long', target_qty=int(signal.qty), purpose=signal.action)]

    def submit_order_intents(self, intents: list[OrderIntent]) -> list[ExecutionResult]:
        first = intents[0]
        return [ExecutionResult('ok-1', first.purpose, first.target_qty, True, 'accepted', datetime.now().isoformat())]


class _RejectedGateway(_AcceptedGateway):
    def submit_order_intents(self, intents: list[OrderIntent]) -> list[ExecutionResult]:
        first = intents[0]
        return [ExecutionResult('ng-1', first.purpose, first.target_qty, False, 'rejected', datetime.now().isoformat())]


def _build_entry_frame(last_open: float, last_close: float, last_eob: datetime) -> KlineFrame:
    rows: list[dict[str, object]] = []
    base = last_eob - timedelta(minutes=20)
    for index in range(4):
        price = 100.0 + index * 0.1
        rows.append(
            {
                'eob': base + timedelta(minutes=5 * index),
                'open': price - 0.1,
                'high': price + 0.4,
                'low': price - 0.4,
                'close': price,
                'volume': 1000.0,
            }
        )
    rows.append(
        {
            'eob': last_eob,
            'open': last_open,
            'high': max(last_open, last_close) + 0.3,
            'low': min(last_open, last_close) - 0.3,
            'close': last_close,
            'volume': 1200.0,
        }
    )
    return KlineFrame(frame=pl.DataFrame(rows), symbol='DCE.P2409', frequency='300s')


class EngineExecutionTest(unittest.TestCase):
    def test_signal_accepted_requires_any_for_entry_and_all_for_exit(self) -> None:
        entry = EntrySignal(
            action='buy',
            reason='demo',
            direction=1,
            qty=1,
            price=100.0,
            stop_loss=97.8,
            protected_stop_price=100.6,
            created_at=datetime(2026, 1, 5, 9, 30, 0),
            entry_atr=1.0,
            breakout_anchor=100.0,
            campaign_id='entry',
            environment_ma=99.0,
            macd_histogram=0.4,
        )
        exit_results = [
            ExecutionResult('1', 'close_long', 1, True, 'ok', datetime.now().isoformat()),
            ExecutionResult('2', 'close_long', 1, False, 'rejected', datetime.now().isoformat()),
        ]
        entry_results = [
            ExecutionResult('1', 'buy', 1, False, 'rejected', datetime.now().isoformat()),
            ExecutionResult('2', 'buy', 1, True, 'ok', datetime.now().isoformat()),
        ]
        exit_signal = ExitSignal(
            action='close_long',
            reason='hard stop',
            direction=1,
            qty=1,
            price=98.0,
            created_at=datetime(2026, 1, 5, 9, 35, 0),
            exit_trigger='hard_stop',
            campaign_id='exit',
            holding_bars=1,
            mfe_r=0.0,
            gross_pnl=-29.0,
            net_pnl=-29.0,
            phase='armed',
        )

        self.assertTrue(signal_accepted(entry, entry_results))
        self.assertFalse(signal_accepted(entry, []))
        self.assertFalse(signal_accepted(entry, [ExecutionResult('3', 'buy', 1, False, 'rejected', datetime.now().isoformat())]))
        self.assertFalse(signal_accepted(exit_signal, exit_results))

    def test_execute_due_signal_builds_position_from_fill_price(self) -> None:
        config = load_config(Path('config/strategy.yaml'))
        config.reporting.enabled = False
        runtime = RuntimeState(states_by_csymbol={'DCE.P': SymbolRuntime(csymbol='DCE.P')})
        state = runtime.states_by_csymbol['DCE.P']
        state.main_symbol = 'DCE.P2409'
        frames = SymbolFrames.create('DCE.P2409', config.universe.entry_frequency, config.universe.trend_frequency, 5, 5)
        frames.entry.replace(_build_entry_frame(last_open=100.9, last_close=101.0, last_eob=datetime(2026, 1, 5, 9, 35, 0)))
        runtime.bar_store['DCE.P2409'] = frames

        signal = EntrySignal(
            action='buy',
            reason='demo',
            direction=1,
            qty=1,
            price=100.0,
            stop_loss=97.8,
            protected_stop_price=100.6,
            created_at=datetime(2026, 1, 5, 9, 30, 0),
            entry_atr=1.0,
            breakout_anchor=100.0,
            campaign_id='demo-entry',
            environment_ma=99.0,
            macd_histogram=0.4,
        )

        execute_due_signal(config, _AcceptedGateway(), _NoOpReportSink(), runtime, state, 'DCE.P', 'DCE.P2409', signal)

        self.assertIsNotNone(state.position)
        assert state.position is not None
        self.assertAlmostEqual(100.9, state.position.entry_price)
        self.assertAlmostEqual(98.7, state.position.initial_stop_loss)
        self.assertEqual(datetime(2026, 1, 5, 9, 30, 0), state.position.entry_eob)

    def test_execute_due_signal_rejected_exit_preserves_position(self) -> None:
        config = load_config(Path('config/strategy.yaml'))
        config.reporting.enabled = False
        runtime = RuntimeState(states_by_csymbol={'DCE.P': SymbolRuntime(csymbol='DCE.P', main_symbol='DCE.P2409')})
        state = runtime.states_by_csymbol['DCE.P']
        state.position = ManagedPosition(
            entry_price=100.9,
            direction=1,
            qty=1,
            entry_atr=1.0,
            initial_stop_loss=97.8,
            stop_loss=97.8,
            protected_stop_price=100.6,
            phase='armed',
            campaign_id='demo-exit',
            entry_eob=datetime(2026, 1, 5, 9, 0, 0),
            breakout_anchor=100.0,
            highest_price_since_entry=100.9,
            lowest_price_since_entry=100.9,
        )
        signal = ExitSignal(
            action='close_long',
            reason='hard stop',
            direction=1,
            qty=1,
            price=98.0,
            created_at=datetime(2026, 1, 5, 9, 35, 0),
            exit_trigger='hard_stop',
            campaign_id='demo-exit',
            holding_bars=1,
            mfe_r=0.0,
            gross_pnl=-29.0,
            net_pnl=-29.0,
            phase='armed',
        )

        execute_due_signal(config, _RejectedGateway(), _NoOpReportSink(), runtime, state, 'DCE.P', 'DCE.P2409', signal)

        self.assertIsNotNone(state.position)
        self.assertEqual(0, runtime.portfolio.trades_count)


if __name__ == '__main__':
    unittest.main()
