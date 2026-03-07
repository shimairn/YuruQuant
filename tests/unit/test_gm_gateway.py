from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from yuruquant.adapters.gm.gateway import GMGateway
from yuruquant.app.config import load_config


class GMGatewayRollTest(unittest.TestCase):
    def _load_config(self):
        text = """
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
            path = Path(temp_dir) / 'config.yaml'
            path.write_text(text, encoding='utf-8')
            return load_config(path)

    def test_refresh_main_contracts_falls_back_to_previous_trading_day(self):
        gateway = GMGateway(self._load_config())

        def continuous_side_effect(csymbol: str, start_date: str, end_date: str):
            self.assertEqual(start_date, end_date)
            if start_date == '2025-12-13':
                return []
            if start_date == '2025-12-12':
                return [{'symbol': 'DCE.p2605'}]
            return []

        with patch('yuruquant.adapters.gm.gateway.get_continuous_contracts', side_effect=continuous_side_effect) as continuous_mock, \
             patch('yuruquant.adapters.gm.gateway.get_previous_trading_date', return_value='2025-12-12') as previous_mock, \
             patch('yuruquant.adapters.gm.gateway.subscribe') as subscribe_mock:
            gateway.refresh_main_contracts(datetime(2025, 12, 13, 9, 0, 0))

        self.assertEqual(gateway.current_main_symbol('DCE.P'), 'DCE.p2605')
        previous_mock.assert_called_once_with('DCE', '2025-12-13')
        self.assertGreaterEqual(continuous_mock.call_count, 2)
        self.assertEqual(subscribe_mock.call_count, 2)


if __name__ == '__main__':
    unittest.main()
