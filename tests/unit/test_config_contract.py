from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from strategy.config import load_config


class ConfigContractTest(unittest.TestCase):
    def test_load_config(self):
        cfg = load_config(Path("config/strategy.yaml"))
        self.assertEqual(cfg.runtime.mode, "BACKTEST")
        self.assertGreater(cfg.runtime.warmup_5m, 0)
        self.assertGreater(cfg.runtime.warmup_1h, 0)

    def test_reject_unknown_old_field(self):
        text = """
runtime:
  mode: BACKTEST
  run_id: run_001
  symbols: [DCE.p]
  freq_5m: 300s
  freq_1h: 3600s
  warmup_5m: 10
  warmup_1h: 10
strategy:
  breakout_lookback_5m: 20
  breakout_min_distance_atr: 0.1
  breakout_width_min_atr: 0.3
  breakout_width_max_atr: 4.0
  volume_ratio_day_min: 1.1
  volume_ratio_night_min: 1.0
  trend_ema_fast_1h: 20
  trend_ema_slow_1h: 60
  trend_strength_min: 0.1
  entry_cooldown_bars: 2
  max_entries_per_day: 3
  target_annual_vol: 0.1
  atr_period: 14
  require_next_bar_confirm: false
risk:
  risk_per_trade_notional_ratio: 0.02
  fixed_equity_percent: 0.1
  max_pos_size_percent: 0.3
  hard_stop_atr: 2.0
  break_even_activate_r: 1.0
  trail_activate_r: 1.0
  trail_stop_atr: 2.0
  dynamic_stop_enabled: true
  dynamic_stop_atr: 1.5
  dynamic_stop_activate_r: 0.5
  time_stop_bars: 8
  max_stopouts_per_day_per_symbol: 2
  backtest_commission_ratio: 0.001
  backtest_slippage_ratio: 0.002
portfolio:
  max_daily_loss_ratio: 0.05
  max_drawdown_halt_ratio: 0.15
gm:
  token: ""
  strategy_id: ""
  serv_addr: ""
  backtest_start: "2025-01-01 00:00:00"
  backtest_end: "2025-01-31 15:00:00"
  backtest_max_days: 180
  backtest_initial_cash: 500000
  backtest_match_mode: 0
  backtest_intraday: false
  subscribe_wait_group: true
  wait_group_timeout: 10
reporting:
  enabled: true
  output_dir: reports
  trade_filename: trade_report.csv
  daily_filename: daily_report.csv
  execution_filename: execution_report.csv
observability:
  level: WARN
  sample_every_n: 50
instrument:
  defaults:
    multiplier: 10.0
    min_tick: 1.0
    min_lot: 1
    lot_step: 1
    fixed_equity_percent: 0.1
    max_pos_size_percent: 0.3
    volume_ratio_min: {day: 1.2, night: 1.0}
    sessions:
      day: [["09:00", "11:30"], ["13:30", "15:00"]]
      night: [["21:00", "23:00"]]
  symbols: {}
"""
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "bad.yaml"
            cfg_path.write_text(text, encoding="utf-8")
            with self.assertRaises(ValueError):
                load_config(cfg_path)


if __name__ == "__main__":
    unittest.main()
