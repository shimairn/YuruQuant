from __future__ import annotations

from datetime import datetime
from pathlib import Path
import unittest

from yuruquant.adapters.gm.gateway import GMGateway
from yuruquant.app.config import load_config
from yuruquant.core.engine import StrategyEngine
from yuruquant.core.models import EntrySignal, ExecutionResult, ExitSignal, ManagedPosition, SymbolRuntime
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


class _FakeGateway:
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
        return type('Snapshot', (), {'equity': 500000.0, 'cash': 500000.0})()

    def plan_order_intents(self, symbol: str, signal):
        _ = symbol
        _ = signal
        return []

    def submit_order_intents(self, intents):
        _ = intents
        return self.results


class ExecutionLayerTest(unittest.TestCase):
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
        engine = StrategyEngine(config=config, gateway=_FakeGateway(), report_sink=sink)
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


if __name__ == '__main__':
    unittest.main()
