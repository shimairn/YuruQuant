from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from yuruquant.app.config import load_config


class ConfigContractTest(unittest.TestCase):
    def test_load_config(self):
        config = load_config(Path('config/strategy.yaml'))
        self.assertEqual(config.runtime.mode, 'BACKTEST')
        self.assertEqual(config.runtime.run_id, 'dual_core_run')
        self.assertEqual(config.execution.fill_policy, 'next_bar_open')
        self.assertEqual(config.strategy.environment.macd_signal, 9)
        self.assertEqual(config.portfolio.risk_per_trade_ratio, 0.015)
        self.assertEqual(config.strategy.entry.breakout_atr_buffer, 0.25)
        self.assertEqual(config.strategy.entry.breakout_close_position_min, 0.70)
        self.assertEqual(config.universe.warmup.entry_bars, 180)
        self.assertEqual(config.reporting.output_dir, 'reports')
        self.assertTrue(config.broker.gm.subscribe_wait_group)
        self.assertFalse(hasattr(config.strategy.exit, 'time_stop_bars'))

    def test_defaults_fill_optional_new_fields(self):
        text = """
runtime:
  mode: BACKTEST
  run_id: demo
broker:
  gm:
    token: ""
    strategy_id: ""
    serv_addr: ""
    backtest:
      start: "2025-01-01 00:00:00"
      end: "2025-01-31 15:00:00"
universe:
  symbols: [DCE.P]
strategy:
  environment: {}
  entry: {}
  exit: {}
portfolio:
  max_daily_loss_ratio: 0.05
  max_drawdown_halt_ratio: 0.15
execution:
  backtest_commission_ratio: 0.001
  backtest_slippage_ratio: 0.002
reporting:
  enabled: true
observability:
  level: WARN
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / 'config.yaml'
            path.write_text(text, encoding='utf-8')
            config = load_config(path)
        self.assertEqual(config.execution.fill_policy, 'next_bar_open')
        self.assertEqual(config.strategy.environment.ma_period, 60)
        self.assertEqual(config.strategy.entry.donchian_lookback, 36)
        self.assertEqual(config.strategy.entry.breakout_atr_buffer, 0.25)
        self.assertEqual(config.strategy.entry.breakout_close_position_min, 0.70)
        self.assertEqual(config.strategy.exit.trailing_ma_period, 60)
        self.assertEqual(config.portfolio.risk_per_trade_ratio, 0.015)

    def test_reject_legacy_schema(self):
        text = """
runtime:
  mode: BACKTEST
  run_id: demo
strategy:
  trend:
    ema_fast: 20
portfolio:
  max_daily_loss_ratio: 0.05
reporting:
  enabled: true
observability:
  level: WARN
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / 'bad.yaml'
            path.write_text(text, encoding='utf-8')
            with self.assertRaises(ValueError):
                load_config(path)


if __name__ == '__main__':
    unittest.main()
