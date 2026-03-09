from __future__ import annotations

from datetime import datetime
import unittest

from yuruquant.app.config import load_config
from yuruquant.core.models import EntrySignal, ManagedPosition, RuntimeState, SymbolRuntime
from yuruquant.portfolio.cluster_risk import check_entry_against_cluster_risk


def _entry_signal(csymbol: str, campaign_id: str, direction: int = 1, qty: int = 100) -> EntrySignal:
    action = 'buy' if direction > 0 else 'sell'
    price = 101.0 if direction > 0 else 99.0
    stop_loss = 98.8 if direction > 0 else 101.2
    protected_stop = 101.6 if direction > 0 else 98.4
    return EntrySignal(
        action=action,
        reason='demo',
        direction=direction,
        qty=qty,
        price=price,
        stop_loss=stop_loss,
        protected_stop_price=protected_stop,
        created_at=datetime(2026, 1, 5, 9, 0, 0),
        entry_atr=1.0,
        breakout_anchor=100.0,
        campaign_id=f'{csymbol}-{campaign_id}',
        environment_ma=99.0,
        macd_histogram=0.4,
    )


def _position(campaign_id: str, phase: str = 'armed', direction: int = 1, qty: int = 100) -> ManagedPosition:
    entry_price = 101.0 if direction > 0 else 99.0
    initial_stop = 98.8 if direction > 0 else 101.2
    protected_stop = 101.6 if direction > 0 else 98.4
    return ManagedPosition(
        entry_price=entry_price,
        direction=direction,
        qty=qty,
        entry_atr=1.0,
        initial_stop_loss=initial_stop,
        stop_loss=initial_stop if phase == 'armed' else protected_stop,
        protected_stop_price=protected_stop,
        phase=phase,
        campaign_id=campaign_id,
        entry_eob=datetime(2026, 1, 5, 9, 0, 0),
        breakout_anchor=100.0,
        highest_price_since_entry=entry_price,
        lowest_price_since_entry=entry_price,
    )


class ClusterRiskTest(unittest.TestCase):
    def _build_config(self):
        config = load_config('config/strategy.yaml')
        config.universe.symbols = ['DCE.P', 'DCE.M', 'DCE.Y']
        config.universe.risk_clusters = {'soy_complex': ('DCE.P', 'DCE.M', 'DCE.Y')}
        return config

    def _build_runtime(self) -> RuntimeState:
        return RuntimeState(
            states_by_csymbol={
                'DCE.P': SymbolRuntime(csymbol='DCE.P'),
                'DCE.M': SymbolRuntime(csymbol='DCE.M'),
                'DCE.Y': SymbolRuntime(csymbol='DCE.Y'),
            }
        )

    def test_cluster_armed_risk_cap_counts_open_and_pending_entries(self) -> None:
        config = self._build_config()
        config.portfolio.max_cluster_armed_risk_ratio = 0.010
        runtime = self._build_runtime()
        runtime.states_by_csymbol['DCE.M'].position = _position('armed-open', phase='armed')
        runtime.states_by_csymbol['DCE.Y'].pending_signal = _entry_signal('DCE.Y', 'pending-entry')

        check = check_entry_against_cluster_risk(config, runtime, 'DCE.P', _entry_signal('DCE.P', 'candidate'))

        self.assertTrue(check.breached)
        self.assertEqual(('soy_complex:cluster_armed_risk_cap',), check.breach_reasons)
        self.assertEqual(1, len(check.details))
        detail = check.details[0]
        self.assertEqual('soy_complex', detail.cluster_name)
        self.assertAlmostEqual(0.0088, detail.current_armed_risk_ratio, places=4)
        self.assertAlmostEqual(0.0132, detail.proposed_armed_risk_ratio, places=4)

    def test_cluster_armed_risk_ignores_protected_positions(self) -> None:
        config = self._build_config()
        config.portfolio.max_cluster_armed_risk_ratio = 0.005
        runtime = self._build_runtime()
        runtime.states_by_csymbol['DCE.M'].position = _position('protected-open', phase='protected')

        check = check_entry_against_cluster_risk(config, runtime, 'DCE.P', _entry_signal('DCE.P', 'candidate'))

        self.assertFalse(check.breached)
        self.assertEqual(tuple(), check.breach_reasons)
        self.assertEqual(1, len(check.details))
        detail = check.details[0]
        self.assertAlmostEqual(0.0, detail.current_armed_risk_ratio, places=6)
        self.assertAlmostEqual(0.0044, detail.proposed_armed_risk_ratio, places=4)

    def test_same_direction_limit_counts_protected_positions_and_pending_entries(self) -> None:
        config = self._build_config()
        config.portfolio.max_same_direction_cluster_positions = 2
        runtime = self._build_runtime()
        runtime.states_by_csymbol['DCE.M'].position = _position('protected-open', phase='protected')
        runtime.states_by_csymbol['DCE.Y'].pending_signal = _entry_signal('DCE.Y', 'pending-entry')

        check = check_entry_against_cluster_risk(config, runtime, 'DCE.P', _entry_signal('DCE.P', 'candidate'))

        self.assertTrue(check.breached)
        self.assertEqual(('soy_complex:same_direction_cluster_positions',), check.breach_reasons)
        detail = check.details[0]
        self.assertEqual(2, detail.current_same_direction_positions)
        self.assertEqual(3, detail.proposed_same_direction_positions)

    def test_default_off_keeps_cluster_check_inactive(self) -> None:
        config = self._build_config()
        runtime = self._build_runtime()
        runtime.states_by_csymbol['DCE.M'].position = _position('armed-open', phase='armed')
        runtime.states_by_csymbol['DCE.Y'].pending_signal = _entry_signal('DCE.Y', 'pending-entry')

        check = check_entry_against_cluster_risk(config, runtime, 'DCE.P', _entry_signal('DCE.P', 'candidate'))

        self.assertFalse(check.breached)
        self.assertEqual(tuple(), check.details)
        self.assertEqual(tuple(), check.breach_reasons)


if __name__ == '__main__':
    unittest.main()
