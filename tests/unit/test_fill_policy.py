from __future__ import annotations

from datetime import datetime, timedelta
import unittest

from yuruquant.core.fill_policy import NextBarOpenFillPolicy
from yuruquant.core.models import EntrySignal, SymbolRuntime
from yuruquant.core.time import make_event_id


class FillPolicyTest(unittest.TestCase):
    def test_next_bar_open_only_releases_on_next_bar(self):
        policy = NextBarOpenFillPolicy()
        state = SymbolRuntime(csymbol='DCE.P')
        created_at = datetime(2026, 1, 5, 9, 0, 0)
        signal = EntrySignal(
            action='buy',
            reason='demo',
            direction=1,
            qty=1,
            price=100.0,
            stop_loss=97.8,
            protected_stop_price=100.6,
            created_at=created_at,
            entry_atr=1.0,
            breakout_anchor=100.0,
            campaign_id=make_event_id('DCE.P', created_at),
            environment_ma=99.0,
            macd_histogram=0.5,
        )
        policy.queue(state, signal, created_at)
        self.assertIsNone(policy.pop_due(state, created_at))
        self.assertIs(policy.pop_due(state, created_at + timedelta(minutes=5)), signal)


if __name__ == '__main__':
    unittest.main()
