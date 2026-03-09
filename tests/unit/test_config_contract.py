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
        self.assertEqual(config.strategy.environment.mode, 'ma_macd')
        self.assertEqual(config.strategy.environment.macd_signal, 9)
        self.assertEqual(config.strategy.environment.tsmom_lookbacks, (24, 48, 96))
        self.assertEqual(config.strategy.environment.tsmom_min_agree, 2)
        self.assertEqual(config.portfolio.risk_per_trade_ratio, 0.015)
        self.assertEqual(config.portfolio.max_total_armed_risk_ratio, 0.0)
        self.assertEqual(config.portfolio.max_cluster_armed_risk_ratio, 0.0)
        self.assertEqual(config.portfolio.max_same_direction_cluster_positions, 0)
        self.assertEqual(config.portfolio.drawdown_halt_mode, 'hard')
        self.assertEqual(config.portfolio.drawdown_risk_schedule, ())
        self.assertEqual(config.strategy.entry.breakout_atr_buffer, 0.30)
        self.assertEqual(config.strategy.entry.session_end_buffer_bars, 0)
        self.assertEqual(config.strategy.entry.entry_block_major_gap_bars, 0)
        self.assertFalse(hasattr(config.strategy.entry, 'breakout_close_position_min'))
        self.assertEqual(config.strategy.exit.protected_activate_r, 1.2)
        self.assertFalse(hasattr(config.strategy.exit, 'ascended_activate_r'))
        self.assertEqual(config.strategy.exit.armed_flush_buffer_bars, 0)
        self.assertEqual(config.strategy.exit.armed_flush_min_gap_minutes, 180)
        self.assertEqual(config.strategy.exit.session_flat_all_phases_buffer_bars, 0)
        self.assertEqual(config.strategy.exit.session_flat_scope, 'disabled')
        self.assertEqual(config.universe.warmup.entry_bars, 180)
        self.assertEqual(config.universe.risk_clusters, {})
        self.assertEqual(config.reporting.output_dir, 'reports')
        self.assertTrue(config.broker.gm.subscribe_wait_group)
        self.assertFalse(hasattr(config.strategy.exit, 'trend_ride_activate_r'))
        self.assertFalse(hasattr(config.strategy.exit, 'trailing_ma_period'))

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
        self.assertEqual(config.strategy.environment.mode, 'ma_macd')
        self.assertEqual(config.strategy.environment.ma_period, 60)
        self.assertEqual(config.strategy.environment.tsmom_lookbacks, (24, 48, 96))
        self.assertEqual(config.strategy.environment.tsmom_min_agree, 2)
        self.assertEqual(config.strategy.entry.donchian_lookback, 36)
        self.assertEqual(config.strategy.entry.breakout_atr_buffer, 0.30)
        self.assertEqual(config.strategy.entry.session_end_buffer_bars, 0)
        self.assertEqual(config.strategy.entry.entry_block_major_gap_bars, 0)
        self.assertFalse(hasattr(config.strategy.entry, 'breakout_close_position_min'))
        self.assertEqual(config.strategy.exit.protected_activate_r, 1.2)
        self.assertFalse(hasattr(config.strategy.exit, 'ascended_activate_r'))
        self.assertEqual(config.strategy.exit.armed_flush_buffer_bars, 0)
        self.assertEqual(config.strategy.exit.armed_flush_min_gap_minutes, 180)
        self.assertEqual(config.strategy.exit.session_flat_all_phases_buffer_bars, 0)
        self.assertEqual(config.strategy.exit.session_flat_scope, 'disabled')
        self.assertEqual(config.portfolio.risk_per_trade_ratio, 0.015)
        self.assertEqual(config.portfolio.max_total_armed_risk_ratio, 0.0)
        self.assertEqual(config.portfolio.max_cluster_armed_risk_ratio, 0.0)
        self.assertEqual(config.portfolio.max_same_direction_cluster_positions, 0)
        self.assertEqual(config.portfolio.drawdown_halt_mode, 'hard')
        self.assertEqual(config.portfolio.drawdown_risk_schedule, ())
        self.assertEqual(config.universe.risk_clusters, {})

    def test_load_config_parses_risk_clusters(self):
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
  symbols: [DCE.P, DCE.M, DCE.Y]
  risk_clusters:
    oils: [DCE.P, DCE.Y]
    meals: [DCE.M]
strategy:
  environment: {}
  entry: {}
  exit: {}
portfolio:
  risk_per_trade_ratio: 0.015
  max_total_armed_risk_ratio: 0.0
  max_cluster_armed_risk_ratio: 0.03
  max_same_direction_cluster_positions: 2
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
            path = Path(temp_dir) / 'cluster.yaml'
            path.write_text(text, encoding='utf-8')
            config = load_config(path)
        self.assertEqual({'oils': ('DCE.P', 'DCE.Y'), 'meals': ('DCE.M',)}, config.universe.risk_clusters)
        self.assertEqual(0.03, config.portfolio.max_cluster_armed_risk_ratio)
        self.assertEqual(2, config.portfolio.max_same_direction_cluster_positions)

    def test_load_config_parses_drawdown_risk_schedule(self):
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
  risk_per_trade_ratio: 0.015
  max_total_armed_risk_ratio: 0.0
  max_daily_loss_ratio: 0.05
  max_drawdown_halt_ratio: 0.15
  drawdown_halt_mode: disabled
  drawdown_risk_schedule:
    - drawdown_ratio: 0.08
      risk_mult: 0.50
    - drawdown_ratio: 0.12
      risk_mult: 0.25
execution:
  backtest_commission_ratio: 0.001
  backtest_slippage_ratio: 0.002
reporting:
  enabled: true
observability:
  level: WARN
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / 'drawdown_schedule.yaml'
            path.write_text(text, encoding='utf-8')
            config = load_config(path)
        self.assertEqual('disabled', config.portfolio.drawdown_halt_mode)
        self.assertEqual(2, len(config.portfolio.drawdown_risk_schedule))
        self.assertEqual(0.08, config.portfolio.drawdown_risk_schedule[0].drawdown_ratio)
        self.assertEqual(0.25, config.portfolio.drawdown_risk_schedule[1].risk_mult)

    def test_load_config_parses_tsmom_environment(self):
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
  environment:
    mode: tsmom
    tsmom_lookbacks: [12, 36, 84]
    tsmom_min_agree: 2
  entry: {}
  exit: {}
portfolio:
  risk_per_trade_ratio: 0.015
  max_total_armed_risk_ratio: 0.0
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
            path = Path(temp_dir) / 'tsmom.yaml'
            path.write_text(text, encoding='utf-8')
            config = load_config(path)
        self.assertEqual('tsmom', config.strategy.environment.mode)
        self.assertEqual((12, 36, 84), config.strategy.environment.tsmom_lookbacks)
        self.assertEqual(2, config.strategy.environment.tsmom_min_agree)

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


    def test_reject_removed_exit_keys(self):
        template = """
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
  exit:
    {key}: 2.5
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
        for key in ('trend_ride_activate_r', 'ascended_activate_r'):
            with self.subTest(key=key):
                with tempfile.TemporaryDirectory() as temp_dir:
                    path = Path(temp_dir) / 'bad_exit.yaml'
                    path.write_text(template.replace('{key}', key), encoding='utf-8')
                    with self.assertRaises(ValueError):
                        load_config(path)

    def test_reject_removed_close_position_key(self):
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
  entry:
    breakout_close_position_min: 0.70
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
            path = Path(temp_dir) / 'bad_entry.yaml'
            path.write_text(text, encoding='utf-8')
            with self.assertRaises(ValueError):
                load_config(path)

    def test_reject_negative_session_end_buffer_bars(self):
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
  entry:
    session_end_buffer_bars: -1
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
            path = Path(temp_dir) / 'bad_buffer.yaml'
            path.write_text(text, encoding='utf-8')
            with self.assertRaises(ValueError):
                load_config(path)

    def test_reject_negative_entry_block_major_gap_bars(self):
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
  entry:
    entry_block_major_gap_bars: -1
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
            path = Path(temp_dir) / 'bad_major_gap_entry.yaml'
            path.write_text(text, encoding='utf-8')
            with self.assertRaises(ValueError):
                load_config(path)

    def test_reject_negative_armed_flush_fields(self):
        template = """
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
  exit:
    {key}: {value}
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
        for key in ('armed_flush_buffer_bars', 'armed_flush_min_gap_minutes'):
            with self.subTest(key=key):
                with tempfile.TemporaryDirectory() as temp_dir:
                    path = Path(temp_dir) / f'bad_{key}.yaml'
                    payload = template.replace('{key}', key).replace('{value}', '-1')
                    path.write_text(payload, encoding='utf-8')
                    with self.assertRaises(ValueError):
                        load_config(path)

    def test_reject_negative_session_flat_all_phases_buffer_bars(self):
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
  exit:
    session_flat_all_phases_buffer_bars: -1
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
            path = Path(temp_dir) / 'bad_session_flat.yaml'
            path.write_text(text, encoding='utf-8')
            with self.assertRaises(ValueError):
                load_config(path)

    def test_reject_invalid_session_flat_scope(self):
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
  exit:
    session_flat_scope: every_boundary
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
            path = Path(temp_dir) / 'bad_session_flat_scope.yaml'
            path.write_text(text, encoding='utf-8')
            with self.assertRaises(ValueError):
                load_config(path)

    def test_reject_invalid_environment_mode(self):
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
  environment:
    mode: trend_lab
  entry: {}
  exit: {}
portfolio:
  risk_per_trade_ratio: 0.015
  max_total_armed_risk_ratio: 0.0
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
            path = Path(temp_dir) / 'bad_environment_mode.yaml'
            path.write_text(text, encoding='utf-8')
            with self.assertRaises(ValueError):
                load_config(path)

    def test_reject_invalid_tsmom_environment(self):
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
  environment:
    mode: tsmom
    tsmom_lookbacks: [36, 12]
    tsmom_min_agree: 3
  entry: {}
  exit: {}
portfolio:
  risk_per_trade_ratio: 0.015
  max_total_armed_risk_ratio: 0.0
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
            path = Path(temp_dir) / 'bad_tsmom.yaml'
            path.write_text(text, encoding='utf-8')
            with self.assertRaises(ValueError):
                load_config(path)

    def test_reject_negative_max_total_armed_risk_ratio(self):
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
  risk_per_trade_ratio: 0.015
  max_total_armed_risk_ratio: -0.01
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
            path = Path(temp_dir) / 'bad_armed_risk_cap.yaml'
            path.write_text(text, encoding='utf-8')
            with self.assertRaises(ValueError):
                load_config(path)

    def test_reject_negative_cluster_risk_limits(self):
        template = """
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
  risk_per_trade_ratio: 0.015
  max_total_armed_risk_ratio: 0.0
  {key}: {value}
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
        for key, value in (('max_cluster_armed_risk_ratio', '-0.01'), ('max_same_direction_cluster_positions', '-1')):
            with self.subTest(key=key):
                with tempfile.TemporaryDirectory() as temp_dir:
                    path = Path(temp_dir) / f'bad_{key}.yaml'
                    path.write_text(template.replace('{key}', key).replace('{value}', value), encoding='utf-8')
                    with self.assertRaises(ValueError):
                        load_config(path)

    def test_reject_invalid_drawdown_halt_mode(self):
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
  risk_per_trade_ratio: 0.015
  max_total_armed_risk_ratio: 0.0
  max_daily_loss_ratio: 0.05
  max_drawdown_halt_ratio: 0.15
  drawdown_halt_mode: soft
execution:
  backtest_commission_ratio: 0.001
  backtest_slippage_ratio: 0.002
reporting:
  enabled: true
observability:
  level: WARN
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / 'bad_drawdown_mode.yaml'
            path.write_text(text, encoding='utf-8')
            with self.assertRaises(ValueError):
                load_config(path)

    def test_reject_invalid_drawdown_risk_schedule(self):
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
  risk_per_trade_ratio: 0.015
  max_total_armed_risk_ratio: 0.0
  max_daily_loss_ratio: 0.05
  max_drawdown_halt_ratio: 0.15
  drawdown_halt_mode: disabled
  drawdown_risk_schedule:
    - drawdown_ratio: 0.12
      risk_mult: 0.50
    - drawdown_ratio: 0.08
      risk_mult: 1.20
execution:
  backtest_commission_ratio: 0.001
  backtest_slippage_ratio: 0.002
reporting:
  enabled: true
observability:
  level: WARN
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / 'bad_drawdown_schedule.yaml'
            path.write_text(text, encoding='utf-8')
            with self.assertRaises(ValueError):
                load_config(path)

    def test_reject_risk_clusters_with_unknown_symbol(self):
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
  risk_clusters:
    oils: [DCE.P, DCE.M]
strategy:
  environment: {}
  entry: {}
  exit: {}
portfolio:
  risk_per_trade_ratio: 0.015
  max_total_armed_risk_ratio: 0.0
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
            path = Path(temp_dir) / 'bad_cluster_symbol.yaml'
            path.write_text(text, encoding='utf-8')
            with self.assertRaises(ValueError):
                load_config(path)


if __name__ == '__main__':
    unittest.main()
