from __future__ import annotations

from datetime import datetime
import unittest

from yuruquant.adapters.gm.callbacks import GMCallbacks
from yuruquant.core.models import MarketEvent


class _FakeGateway:
    def __init__(self) -> None:
        self.bound = None
        self.refreshed = 0
        self.event = MarketEvent(trade_time=datetime(2026, 1, 5, 9, 0, 0), bars=[])

    def bind_context(self, context: object) -> None:
        self.bound = context

    def refresh_main_contracts(self, trade_time: object) -> None:
        _ = trade_time
        self.refreshed += 1

    def build_market_event(self, bars, trade_time: object):
        _ = bars
        self.event = MarketEvent(trade_time=trade_time, bars=[])
        return self.event


class _FakeEngine:
    def __init__(self) -> None:
        self.initialized = None
        self.events = []

    def initialize(self, context: object) -> None:
        self.initialized = context

    def on_market_event(self, event) -> None:
        self.events.append(event)


class _FakeContext:
    def __init__(self) -> None:
        self.now = datetime(2026, 1, 5, 9, 0, 0)


class GMCallbacksTest(unittest.TestCase):
    def test_init_and_on_bar_delegate_to_gateway_and_engine(self):
        gateway = _FakeGateway()
        engine = _FakeEngine()
        callbacks = GMCallbacks(config=type('Config', (), {'runtime': type('Runtime', (), {'mode': 'BACKTEST'})(), 'universe': type('Universe', (), {'symbols': ['DCE.P']})()})(), gateway=gateway, engine=engine)
        context = _FakeContext()
        callbacks.init(context)
        callbacks.on_bar(context, [])
        self.assertIs(engine.initialized, context)
        self.assertGreaterEqual(gateway.refreshed, 2)
        self.assertEqual(len(engine.events), 1)


if __name__ == '__main__':
    unittest.main()
