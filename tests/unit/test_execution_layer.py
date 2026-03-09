from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import polars as pl

from yuruquant.adapters.gm.gateway import GMGateway
from yuruquant.app.config import load_config
from yuruquant.core.engine import StrategyEngine
from yuruquant.core.frames import KlineFrame, SymbolFrames
from yuruquant.core.models import EntrySignal, ExecutionResult, ExitSignal, ManagedPosition, MarketEvent, NormalizedBar, OrderIntent, PortfolioSnapshot, SymbolRuntime
from yuruquant.core.time import make_event_id
from yuruquant.reporting.csv_sink import CsvReportSink


class _StubAccount:
    def __init__(self) -> None:
        self.cash = {'nav': 500000.0}

    def position(self, symbol: str, side):
        _ = side
        if symbol == 'DCE.P2409':
            return {'qty': 2}
        return None


class _StubContext:
    def __init__(self) -> None:
        self.now = datetime(2026, 1, 5, 9, 0, 0)
        self._account = _StubAccount()

    def account(self):
        return self._account


class _RejectedGateway:
    def __init__(self) -> None:
        self.results = [ExecutionResult('1', 'close_long', 1, False, 'rejected', datetime.now().isoformat())]

    def bind_context(self, context: object) -> None:
        _ = context

    def refresh_main_contracts(self, trade_time: object) -> None:
        _ = trade_time

    def resolve_csymbol(self, symbol: str) -> str | None:
        _ = symbol
        return 'DCE.P'

    def fetch_history(self, symbol: str, frequency: str, count: int):
        raise AssertionError('not used')

    def get_position_snapshot(self, symbol: str):
        _ = symbol
        return None

    def get_portfolio_snapshot(self):
        return PortfolioSnapshot(equity=500000.0, cash=500000.0)

    def plan_order_intents(self, symbol: str, signal):
        _ = symbol
        _ = signal
        return []

    def submit_order_intents(self, intents):
        _ = intents
        return self.results


class _AcceptedGateway:
    def __init__(self) -> None:
        self.plan_calls: list[tuple[str, object]] = []
        self.submit_calls: list[list[OrderIntent]] = []

    def bind_context(self, context: object) -> None:
        _ = context

    def refresh_main_contracts(self, trade_time: object) -> None:
        _ = trade_time

    def resolve_csymbol(self, symbol: str) -> str | None:
        _ = symbol
        return 'DCE.P'

    def fetch_history(self, symbol: str, frequency: str, count: int):
        raise AssertionError('history should already be warm')

    def get_position_snapshot(self, symbol: str):
        _ = symbol
        return None

    def get_portfolio_snapshot(self):
        return PortfolioSnapshot(equity=500000.0, cash=500000.0)

    def plan_order_intents(self, symbol: str, signal):
        self.plan_calls.append((symbol, signal))
        purpose = signal.action
        return [OrderIntent(symbol=symbol, side='long', target_qty=int(signal.qty), purpose=purpose)]

    def submit_order_intents(self, intents):
        self.submit_calls.append(list(intents))
        if not intents:
            return []
        first = intents[0]
        return [ExecutionResult('ok-1', first.purpose, first.target_qty, True, 'accepted', datetime.now().isoformat())]


def _build_entry_frame(last_close: float, last_high: float, last_low: float, last_eob: datetime) -> KlineFrame:
    rows: list[dict[str, object]] = []
    base = last_eob - timedelta(minutes=20)
    for index in range(4):
        close = 100.0 + index * 0.1
        rows.append(
            {
                'eob': base + timedelta(minutes=5 * index),
                'open': close - 0.1,
                'high': close + 0.4,
                'low': close - 0.4,
                'close': close,
                'volume': 1000.0,
            }
        )
    rows.append(
        {
            'eob': last_eob,
            'open': last_close - 0.1,
            'high': last_high,
            'low': last_low,
            'close': last_close,
            'volume': 1200.0,
        }
    )
    return KlineFrame(frame=pl.DataFrame(rows), symbol='DCE.P2409', frequency='300s')



def _build_hourly_reversal_frame() -> KlineFrame:
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


def _build_demo_entry_signal(campaign_id: str, created_at: datetime, qty: int = 100) -> EntrySignal:
    return EntrySignal(
        action='buy',
        reason='demo',
        direction=1,
        qty=qty,
        price=101.0,
        stop_loss=98.8,
        protected_stop_price=101.6,
        created_at=created_at,
        entry_atr=1.0,
        breakout_anchor=100.0,
        campaign_id=campaign_id,
        environment_ma=99.0,
        macd_histogram=0.4,
    )


class ExecutionLayerTest(unittest.TestCase):
    def _build_entry_engine(self) -> tuple[StrategyEngine, _AcceptedGateway, SymbolRuntime, datetime]:
        config = load_config(Path('config/strategy.yaml'))
        config.reporting.enabled = False
        config.universe.warmup.entry_bars = 5
        config.universe.warmup.trend_bars = 120

        gateway = _AcceptedGateway()
        sink = CsvReportSink('reports', 'signals_test.csv', 'executions_test.csv', 'portfolio_test.csv')
        engine = StrategyEngine(config=config, gateway=gateway, report_sink=sink)

        state = engine.runtime.states_by_csymbol['DCE.P']
        state.main_symbol = 'DCE.P2409'
        engine.runtime.symbol_to_csymbol['DCE.P2409'] = 'DCE.P'

        frames = SymbolFrames.create('DCE.P2409', config.universe.entry_frequency, config.universe.trend_frequency, 5, 120)
        last_eob = datetime(2026, 1, 5, 9, 35, 0)
        frames.entry.replace(_build_entry_frame(last_close=101.0, last_high=101.4, last_low=100.8, last_eob=last_eob))
        frames.trend.replace(_build_hourly_reversal_frame())
        engine.runtime.bar_store['DCE.P2409'] = frames
        return engine, gateway, state, last_eob

    def test_reverse_plan_closes_short_then_opens_long(self):
        config = load_config(Path('config/strategy.yaml'))
        gateway = GMGateway(config)
        gateway.bind_context(_StubContext())
        signal = EntrySignal(
            action='buy',
            reason='demo',
            direction=1,
            qty=3,
            price=100.0,
            stop_loss=97.8,
            protected_stop_price=100.6,
            created_at=datetime(2026, 1, 5, 9, 0, 0),
            entry_atr=1.0,
            breakout_anchor=100.0,
            campaign_id=make_event_id('DCE.P', datetime(2026, 1, 5, 9, 0, 0)),
            environment_ma=99.0,
            macd_histogram=0.4,
        )
        intents = gateway.plan_order_intents('DCE.P2409', signal)
        self.assertEqual([item.purpose for item in intents], ['buy:close_short', 'buy:open_long'])

    def test_rejected_close_does_not_clear_position(self):
        config = load_config(Path('config/strategy.yaml'))
        sink = CsvReportSink('reports', 'signals_test.csv', 'executions_test.csv', 'portfolio_test.csv')
        engine = StrategyEngine(config=config, gateway=_RejectedGateway(), report_sink=sink)
        state = SymbolRuntime(csymbol='DCE.P', main_symbol='DCE.P2409')
        state.position = ManagedPosition(
            entry_price=100.0,
            direction=1,
            qty=1,
            entry_atr=1.0,
            initial_stop_loss=97.8,
            stop_loss=97.8,
            protected_stop_price=100.6,
            phase='armed',
            campaign_id='demo',
            entry_eob=datetime(2026, 1, 5, 9, 0, 0),
            breakout_anchor=100.0,
            highest_price_since_entry=100.0,
            lowest_price_since_entry=100.0,
        )
        engine.runtime.states_by_csymbol['DCE.P'] = state
        signal = ExitSignal(
            action='close_long',
            reason='hard stop',
            direction=1,
            qty=1,
            price=97.8,
            created_at=datetime(2026, 1, 5, 9, 5, 0),
            exit_trigger='hard_stop',
            campaign_id='demo',
            holding_bars=1,
            mfe_r=0.0,
            gross_pnl=-22.0,
            net_pnl=-25.0,
            phase='armed',
        )
        engine._execute_due_signal(state, 'DCE.P', 'DCE.P2409', signal)
        self.assertIsNotNone(state.position)

    def test_entry_execution_uses_fill_open_price_and_time(self):
        config = load_config(Path('config/strategy.yaml'))
        config.reporting.enabled = False
        gateway = _AcceptedGateway()
        sink = CsvReportSink('reports', 'signals_test.csv', 'executions_test.csv', 'portfolio_test.csv')
        engine = StrategyEngine(config=config, gateway=gateway, report_sink=sink)

        state = SymbolRuntime(csymbol='DCE.P', main_symbol='DCE.P2409')
        engine.runtime.states_by_csymbol['DCE.P'] = state
        engine.runtime.symbol_to_csymbol['DCE.P2409'] = 'DCE.P'

        frames = SymbolFrames.create('DCE.P2409', config.universe.entry_frequency, config.universe.trend_frequency, 5, 5)
        last_eob = datetime(2026, 1, 5, 9, 35, 0)
        frames.entry.replace(_build_entry_frame(last_close=101.0, last_high=101.4, last_low=100.8, last_eob=last_eob))
        engine.runtime.bar_store['DCE.P2409'] = frames

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

        engine._execute_due_signal(state, 'DCE.P', 'DCE.P2409', signal)

        self.assertIsNotNone(state.position)
        assert state.position is not None
        self.assertAlmostEqual(state.position.entry_price, 100.9)
        self.assertAlmostEqual(state.position.initial_stop_loss, 98.7)
        self.assertAlmostEqual(state.position.stop_loss, 98.7)
        self.assertAlmostEqual(state.position.protected_stop_price, 101.5)
        self.assertEqual(state.position.entry_eob, datetime(2026, 1, 5, 9, 30, 0))

    def test_exit_execution_uses_fill_price_for_realized_pnl(self):
        config = load_config(Path('config/strategy.yaml'))
        config.reporting.enabled = False
        gateway = _AcceptedGateway()
        sink = CsvReportSink('reports', 'signals_test.csv', 'executions_test.csv', 'portfolio_test.csv')
        engine = StrategyEngine(config=config, gateway=gateway, report_sink=sink)

        state = SymbolRuntime(csymbol='DCE.P', main_symbol='DCE.P2409')
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
        engine.runtime.states_by_csymbol['DCE.P'] = state
        engine.runtime.symbol_to_csymbol['DCE.P2409'] = 'DCE.P'

        frames = SymbolFrames.create('DCE.P2409', config.universe.entry_frequency, config.universe.trend_frequency, 5, 5)
        last_eob = datetime(2026, 1, 5, 9, 35, 0)
        frames.entry.replace(_build_entry_frame(last_close=98.0, last_high=98.3, last_low=97.5, last_eob=last_eob))
        engine.runtime.bar_store['DCE.P2409'] = frames

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
            net_pnl=-34.0,
            phase='armed',
        )

        engine._execute_due_signal(state, 'DCE.P', 'DCE.P2409', signal)

        expected_gross = (97.9 - 100.9) * 10.0
        expected_turnover = (100.9 + 97.9) * 10.0
        expected_net = expected_gross - expected_turnover * (config.execution.backtest_commission_ratio + config.execution.backtest_slippage_ratio)
        self.assertIsNone(state.position)
        self.assertAlmostEqual(engine.runtime.portfolio.realized_pnl, expected_net)
        self.assertEqual(engine.runtime.portfolio.losses, 1)
        self.assertEqual(engine.runtime.portfolio.trades_count, 1)


    def test_hourly_ma_reversal_no_longer_queues_exit(self):
        config = load_config(Path('config/smoke_dual_core.yaml'))
        config.reporting.enabled = False
        config.universe.warmup.entry_bars = 3
        config.universe.warmup.trend_bars = 120

        gateway = _AcceptedGateway()
        with tempfile.TemporaryDirectory() as temp_dir:
            sink = CsvReportSink(temp_dir, 'signals.csv', 'executions.csv', 'portfolio.csv')
            engine = StrategyEngine(config=config, gateway=gateway, report_sink=sink)

            state = SymbolRuntime(csymbol='DCE.P', main_symbol='DCE.P2409')
            state.position = ManagedPosition(
                entry_price=100.0,
                direction=1,
                qty=2,
                entry_atr=1.0,
                initial_stop_loss=97.8,
                stop_loss=100.6,
                protected_stop_price=100.6,
                phase='protected',
                campaign_id='demo',
                entry_eob=datetime(2026, 1, 5, 9, 0, 0),
                breakout_anchor=100.5,
                highest_price_since_entry=105.0,
                lowest_price_since_entry=100.0,
            )
            engine.runtime.states_by_csymbol['DCE.P'] = state
            engine.runtime.symbol_to_csymbol['DCE.P2409'] = 'DCE.P'

            frames = SymbolFrames.create('DCE.P2409', config.universe.entry_frequency, config.universe.trend_frequency, config.universe.warmup.entry_bars, config.universe.warmup.trend_bars)
            first_eob = datetime(2026, 1, 7, 9, 35, 0)
            frames.entry.replace(_build_entry_frame(last_close=101.0, last_high=101.4, last_low=100.8, last_eob=first_eob))
            frames.trend.replace(_build_hourly_reversal_frame())
            engine.runtime.bar_store['DCE.P2409'] = frames

            engine._process_symbol(state)

            self.assertIsNone(state.pending_signal)
            self.assertEqual(0, len(gateway.submit_calls))
            self.assertIsNotNone(state.position)
            self.assertEqual('protected', state.position.phase)

    def test_armed_risk_cap_blocks_entry_when_open_armed_risk_is_full(self):
        engine, _, state, last_eob = self._build_entry_engine()
        engine.config.portfolio.max_total_armed_risk_ratio = 0.005

        armed_state = engine.runtime.states_by_csymbol['DCE.M']
        armed_state.position = ManagedPosition(
            entry_price=100.0,
            direction=1,
            qty=100,
            entry_atr=1.0,
            initial_stop_loss=97.8,
            stop_loss=97.8,
            protected_stop_price=100.6,
            phase='armed',
            campaign_id='armed-open',
            entry_eob=datetime(2026, 1, 5, 9, 0, 0),
            breakout_anchor=100.0,
            highest_price_since_entry=100.0,
            lowest_price_since_entry=100.0,
        )

        with patch('yuruquant.core.engine.maybe_generate_entry', return_value=_build_demo_entry_signal('new-entry', last_eob)):
            engine._process_symbol(state)

        self.assertIsNone(state.pending_signal)

    def test_armed_risk_cap_counts_pending_entries_before_fill(self):
        engine, _, state, last_eob = self._build_entry_engine()
        engine.config.portfolio.max_total_armed_risk_ratio = 0.005

        queued_state = engine.runtime.states_by_csymbol['DCE.M']
        queued_state.pending_signal = _build_demo_entry_signal('queued-entry', last_eob - timedelta(minutes=5))
        queued_state.pending_signal_eob = last_eob - timedelta(minutes=5)

        with patch('yuruquant.core.engine.maybe_generate_entry', return_value=_build_demo_entry_signal('new-entry', last_eob)):
            engine._process_symbol(state)

        self.assertIsNone(state.pending_signal)

    def test_armed_risk_cap_ignores_protected_positions(self):
        engine, _, state, last_eob = self._build_entry_engine()
        engine.config.portfolio.max_total_armed_risk_ratio = 0.005

        protected_state = engine.runtime.states_by_csymbol['DCE.M']
        protected_state.position = ManagedPosition(
            entry_price=100.0,
            direction=1,
            qty=100,
            entry_atr=1.0,
            initial_stop_loss=97.8,
            stop_loss=100.6,
            protected_stop_price=100.6,
            phase='protected',
            campaign_id='protected-open',
            entry_eob=datetime(2026, 1, 5, 9, 0, 0),
            breakout_anchor=100.0,
            highest_price_since_entry=100.0,
            lowest_price_since_entry=100.0,
        )

        with patch('yuruquant.core.engine.maybe_generate_entry', return_value=_build_demo_entry_signal('new-entry', last_eob)):
            engine._process_symbol(state)

        self.assertIsNotNone(state.pending_signal)
        self.assertEqual('new-entry', state.pending_signal.campaign_id)
    def test_on_market_event_accepts_15m_entry_frequency(self):
        gateway = _AcceptedGateway()
        config = load_config(Path('config/strategy.yaml'))
        config.universe.entry_frequency = '900s'
        config.universe.trend_frequency = '3600s'
        config.reporting.enabled = False
        engine = StrategyEngine(config, gateway, CsvReportSink('reports', 'signals.csv', 'executions.csv', 'portfolio_daily.csv'))

        entry_eob = datetime(2026, 1, 5, 9, 15, 0)
        trend_eob = datetime(2026, 1, 5, 10, 0, 0)
        event = MarketEvent(
            trade_time=entry_eob,
            bars=[
                NormalizedBar(
                    csymbol='DCE.P',
                    symbol='DCE.P2409',
                    frequency='900s',
                    eob=entry_eob,
                    open=100.0,
                    high=101.0,
                    low=99.5,
                    close=100.8,
                    volume=1000.0,
                ),
                NormalizedBar(
                    csymbol='DCE.P',
                    symbol='DCE.P2409',
                    frequency='3600s',
                    eob=trend_eob,
                    open=100.0,
                    high=101.5,
                    low=99.0,
                    close=100.9,
                    volume=5000.0,
                ),
            ],
        )

        with patch.object(engine, '_process_symbol') as mock_process:
            engine.on_market_event(event)

        frames = engine.runtime.bar_store['DCE.P2409']
        self.assertEqual(1, len(frames.entry))
        self.assertEqual(1, len(frames.trend))
        mock_process.assert_called_once()



if __name__ == '__main__':
    unittest.main()

