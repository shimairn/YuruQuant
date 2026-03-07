from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from yuruquant.app.bootstrap import build_application
from yuruquant.app.cli import CLIArgs, parse_args


class CLIArgsTest(unittest.TestCase):
    def test_parse_args_only_uses_strategy_config_from_environment(self):
        with patch.dict(
            os.environ,
            {
                'STRATEGY_CONFIG': 'config/smoke_dual_core.yaml',
                'GM_MODE': 'BACKTEST',
                'GM_RUN_ID': 'env_run',
                'GM_STRATEGY_ID': 'env_strategy',
                'GM_TOKEN': 'env_token',
                'GM_SERV_ADDR': 'tcp://127.0.0.1:1234',
            },
            clear=False,
        ):
            args = parse_args([])
        self.assertEqual(args.config, Path('config/smoke_dual_core.yaml'))
        self.assertIsNone(args.mode)
        self.assertIsNone(args.run_id)
        self.assertIsNone(args.strategy_id)
        self.assertIsNone(args.token)
        self.assertIsNone(args.serv_addr)

    def test_build_application_exports_effective_runtime_environment(self):
        config_text = """
runtime:
  mode: BACKTEST
  run_id: demo
broker:
  gm:
    token: "config_token"
    strategy_id: "config_strategy"
    serv_addr: ""
    backtest:
      start: "2025-01-01 09:00:00"
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
  enabled: false
observability:
  level: WARN
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / 'config.yaml'
            config_path.write_text(config_text, encoding='utf-8')
            args = CLIArgs(
                mode='LIVE',
                config=config_path,
                run_id='manual_run',
                strategy_id='override_strategy',
                token='override_token',
                serv_addr='tcp://127.0.0.1:1000',
            )
            with patch.dict(os.environ, {}, clear=False):
                app = build_application(args)
                self.assertEqual(app.config.runtime.mode, 'LIVE')
                self.assertEqual(os.environ['STRATEGY_CONFIG'], str(config_path))
                self.assertEqual(os.environ['GM_MODE'], 'LIVE')
                self.assertEqual(os.environ['GM_RUN_ID'], 'manual_run')
                self.assertEqual(os.environ['GM_TOKEN'], 'override_token')
                self.assertEqual(os.environ['GM_STRATEGY_ID'], 'override_strategy')
                self.assertEqual(os.environ['GM_SERV_ADDR'], 'tcp://127.0.0.1:1000')


if __name__ == '__main__':
    unittest.main()
