import unittest
from pathlib import Path

from strategy.config_loader import load_config
from strategy.domain.instruments import get_instrument_spec, normalize_lot


class SmokeBootTests(unittest.TestCase):
    def test_load_config(self) -> None:
        cfg = load_config(Path('config/strategy.yaml'))
        self.assertIn(cfg.runtime.mode, {'BACKTEST', 'LIVE'})
        self.assertGreaterEqual(len(cfg.runtime.symbols), 1)

    def test_instrument_defaults_loaded(self) -> None:
        cfg = load_config(Path('config/strategy.yaml'))
        self.assertGreater(cfg.instrument.defaults.multiplier, 0)
        self.assertGreaterEqual(cfg.instrument.defaults.min_lot, 1)
        self.assertGreaterEqual(cfg.instrument.defaults.lot_step, 1)

    def test_instrument_symbol_override_loaded(self) -> None:
        cfg = load_config(Path('config/strategy.yaml'))
        spec = cfg.instrument.symbols.get('DCE.p')
        self.assertIsNotNone(spec)
        self.assertEqual(spec.multiplier, 10.0)

    def test_instrument_position_overrides_loaded(self) -> None:
        cfg = load_config(Path('config/strategy.yaml'))
        spec = cfg.instrument.symbols.get('SHFE.au')
        self.assertIsNotNone(spec)
        self.assertGreater(spec.fixed_equity_percent, 0.0)
        self.assertGreater(spec.max_pos_size_percent, 0.0)

    def test_get_instrument_spec_fallback(self) -> None:
        cfg = load_config(Path('config/strategy.yaml'))
        spec = get_instrument_spec(cfg, 'UNKNOWN.symbol')
        self.assertEqual(spec.multiplier, cfg.instrument.defaults.multiplier)

    def test_normalize_lot(self) -> None:
        self.assertEqual(normalize_lot(7, 2, 2), 6)
        self.assertEqual(normalize_lot(1, 2, 2), 0)


if __name__ == '__main__':
    unittest.main()
