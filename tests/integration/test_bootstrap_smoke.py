from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import yaml

from yuruquant.app.bootstrap import build_application
from yuruquant.app.cli import CLIArgs


class _SmokeAccount:
    def __init__(self) -> None:
        self.cash = {'nav': 500000.0, 'available': 500000.0}

    def position(self, symbol: str, side):
        _ = symbol
        _ = side
        return None


class _SmokeContext:
    def __init__(self) -> None:
        self.now = datetime(2026, 1, 5, 15, 0, 0)
        self._account = _SmokeAccount()

    def account(self):
        return self._account

    def data(self, symbol: str, frequency: str, count: int, fields: str, format: str = 'row'):
        _ = fields
        _ = format
        step = timedelta(minutes=5 if frequency == '300s' else 60)
        start = self.now - step * count
        rows = []
        for index in range(count):
            close = 100.0 + index * 0.20 if frequency == '3600s' else 100.0 + (index % 6) * 0.05
            rows.append(
                {
                    'symbol': symbol,
                    'eob': start + step * index,
                    'open': close - 0.1,
                    'high': close + 0.6,
                    'low': close - 0.6,
                    'close': close,
                    'volume': 1200 + index,
                }
            )
        if frequency == '300s':
            rows[-1]['close'] = float(rows[-2]['high']) + 0.7
            rows[-1]['high'] = float(rows[-1]['close']) + 0.2
        return rows


class _Bar:
    def __init__(self, symbol: str, frequency: str, eob: datetime, open_price: float, high: float, low: float, close: float, volume: float) -> None:
        self.symbol = symbol
        self.frequency = frequency
        self.eob = eob
        self.open = open_price
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume


class BootstrapSmokeTest(unittest.TestCase):
    def test_build_application_and_process_bar(self):
        source = yaml.safe_load(Path('config/strategy.yaml').read_text(encoding='utf-8'))
        source['broker']['gm']['token'] = ''
        source['universe']['symbols'] = ['DCE.P']
        with tempfile.TemporaryDirectory() as temp_dir:
            source.setdefault('reporting', {})
            source['reporting']['output_dir'] = temp_dir
            config_path = Path(temp_dir) / 'strategy.yaml'
            config_path.write_text(yaml.safe_dump(source, sort_keys=False, allow_unicode=True), encoding='utf-8')
            app = build_application(CLIArgs(mode='BACKTEST', config=config_path, run_id='smoke', strategy_id=None, token=None, serv_addr=None))
            context = _SmokeContext()
            app.callbacks.init(context)
            symbol = app.gateway.current_main_symbol('DCE.P') or 'DCE.P.SIM'
            bar_1h = _Bar(symbol, '3600s', context.now, 100.0, 101.0, 99.0, 100.8, 1500.0)
            bar_5m = _Bar(symbol, '300s', context.now, 100.0, 101.2, 99.6, 101.0, 2800.0)
            app.callbacks.on_bar(context, [bar_1h, bar_5m])
            self.assertTrue((Path(temp_dir) / 'signals.csv').exists())
            self.assertTrue((Path(temp_dir) / 'executions.csv').exists())
            self.assertTrue((Path(temp_dir) / 'portfolio_daily.csv').exists())


if __name__ == '__main__':
    unittest.main()

