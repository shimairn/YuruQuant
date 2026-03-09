from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from yuruquant.app.config import load_config
from yuruquant.research import workflows


class ResearchWorkflowTest(unittest.TestCase):
    def test_yaml_round_trip(self) -> None:
        payload = {'runtime': {'mode': 'BACKTEST', 'run_id': 'demo'}, 'universe': {'symbols': ['DCE.P']}}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'config.yaml'
            workflows.write_yaml(path, payload)
            loaded = workflows.load_yaml(path)

        self.assertEqual(payload, loaded)

    def test_reports_exist_requires_standard_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertFalse(workflows.reports_exist(root))
            for name in ('signals.csv', 'executions.csv', 'portfolio_daily.csv'):
                (root / name).write_text('', encoding='utf-8')
            self.assertTrue(workflows.reports_exist(root))

    def test_build_backtest_command_uses_main_entrypoint(self) -> None:
        repo_root = Path('C:/demo/repo')
        command = workflows.build_backtest_command(repo_root, 'python.exe', repo_root / 'config.yaml', 'demo_run')
        self.assertEqual(
            ['python.exe', str(repo_root / 'main.py'), '--mode', 'BACKTEST', '--config', str(repo_root / 'config.yaml'), '--run-id', 'demo_run'],
            command,
        )

    def test_run_backtest_executes_subprocess_with_repo_cwd(self) -> None:
        repo_root = Path('C:/demo/repo')
        with patch('yuruquant.research.workflows.subprocess.run') as run_mock:
            workflows.run_backtest(repo_root, 'python.exe', repo_root / 'config.yaml', 'demo_run')

        kwargs = run_mock.call_args.kwargs
        self.assertEqual(str(repo_root), kwargs['cwd'])
        self.assertTrue(kwargs['check'])

    def test_build_multiplier_lookup_merges_overrides(self) -> None:
        config = load_config(Path('config/strategy.yaml'))
        config.universe.symbols = ['DCE.P', 'SHFE.AG']
        config.universe.instrument_overrides = {'SHFE.AG': load_config(Path('config/smoke_dual_core.yaml')).universe.instrument_overrides['SHFE.AG']}

        lookup = workflows.build_multiplier_lookup(config)

        self.assertEqual(config.universe.instrument_defaults.multiplier, lookup['DCE.P'])
        self.assertEqual(config.universe.instrument_overrides['SHFE.AG'].multiplier, lookup['SHFE.AG'])

    def test_write_rows_csv_writes_header_and_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'summary.csv'
            workflows.write_rows_csv(path, ['name', 'value'], [{'name': 'demo', 'value': 1}])
            with path.open('r', encoding='utf-8', newline='') as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual([{'name': 'demo', 'value': '1'}], rows)


if __name__ == '__main__':
    unittest.main()
