from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from yuruquant.adapters.gm.runner import run_with_gm
from yuruquant.app.config import load_config


class _Callbacks:
    def init(self, context):
        return None

    def on_bar(self, context, bars):
        return None

    def on_order_status(self, context, order):
        return None

    def on_execution_report(self, context, execrpt):
        return None

    def on_error(self, context, code, info_msg):
        return None


class GMRunnerTest(unittest.TestCase):
    def test_run_uses_dedicated_gm_entrypoint(self):
        config_text = """
runtime:
  mode: BACKTEST
  run_id: demo
broker:
  gm:
    token: "token"
    strategy_id: "strategy"
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
            config = load_config(config_path)

        with patch('yuruquant.adapters.gm.runner.run') as run_mock:
            run_with_gm(config, _Callbacks())

        kwargs = run_mock.call_args.kwargs
        self.assertEqual(kwargs['filename'], str(Path('yuruquant') / 'adapters' / 'gm' / 'entrypoint.py'))
        self.assertEqual(kwargs['token'], 'token')
        self.assertEqual(kwargs['strategy_id'], 'strategy')


if __name__ == '__main__':
    unittest.main()
