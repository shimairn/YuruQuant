from __future__ import annotations

import unittest
from types import SimpleNamespace

from quantframe.app.config import load_config, load_resources
from quantframe.core.models import OrderRequest
from quantframe.platforms.gm import adapter as gm_adapter


class _FakeAccount:
    def __init__(self) -> None:
        self.position_calls: list[tuple[str, int]] = []

    def position(self, symbol, side):
        self.position_calls.append((symbol, side))
        if side == gm_adapter.PositionSide_Long:
            return SimpleNamespace(volume=2, available_now=2, vwap=101.0)
        return SimpleNamespace(volume=0, available_now=0, vwap=0.0)


class _FakeContext:
    def __init__(self) -> None:
        self.now = SimpleNamespace()

    def account(self):
        return _FakeAccount()


class GMAdapterTest(unittest.TestCase):
    def setUp(self) -> None:
        self._original_get_continuous_contracts = gm_adapter.get_continuous_contracts
        self._original_get_previous_trading_date = gm_adapter.get_previous_trading_date
        self._original_order_target_volume = gm_adapter.order_target_volume

    def tearDown(self) -> None:
        gm_adapter.get_continuous_contracts = self._original_get_continuous_contracts
        gm_adapter.get_previous_trading_date = self._original_get_previous_trading_date
        gm_adapter.order_target_volume = self._original_order_target_volume

    def _build_platform(self):
        config = load_config("resources/configs/gm_trend_ma.yaml")
        resources = load_resources(config)
        platform = gm_adapter.GMPlatform(config)
        platform.subscribe(resources.universe, "1d", 10)
        return platform, resources

    def test_resolve_actual_symbol_from_continuous_contract(self):
        gm_adapter.get_continuous_contracts = lambda **kwargs: [{"symbol": "DCE.p2605"}]
        gm_adapter.get_previous_trading_date = None
        platform, resources = self._build_platform()
        instrument = resources.by_id["DCE.P"]
        actual = platform._resolve_actual_symbol(instrument, "2026-03-10")
        self.assertEqual(actual, "DCE.p2605")
        self.assertIs(platform.by_symbol["DCE.p2605"], instrument)

    def test_submit_orders_uses_resolved_actual_symbol(self):
        submitted: list[tuple[str, int, int]] = []

        gm_adapter.get_continuous_contracts = lambda **kwargs: [{"symbol": "DCE.p2605"}]
        gm_adapter.get_previous_trading_date = None

        def fake_order_target_volume(symbol, volume, position_side, order_type):
            _ = order_type
            submitted.append((symbol, volume, position_side))

        gm_adapter.order_target_volume = fake_order_target_volume
        platform, resources = self._build_platform()
        account = _FakeAccount()
        platform.bind_context(SimpleNamespace(account=lambda: account, now="2026-03-10 09:00:00"))
        instrument = resources.by_id["DCE.P"]
        orders = [
            OrderRequest(
                instrument_id=instrument.instrument_id,
                symbol=instrument.platform_symbol,
                target_qty=3,
                delta_qty=3,
                reason="test_long",
            )
        ]
        results = platform.submit_orders(orders)
        self.assertTrue(results)
        self.assertEqual(submitted[-1][0], "DCE.p2605")
        self.assertEqual(submitted[-1][1], 3)


if __name__ == "__main__":
    unittest.main()
