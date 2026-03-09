from __future__ import annotations

import csv
import importlib.util
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / 'scripts' / 'grid_search_dual_core.py'
SPEC = importlib.util.spec_from_file_location('grid_search_dual_core_test', MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class GridSearchDualCoreTest(unittest.TestCase):
    def test_default_grid_generates_positive_combinations_only(self):
        combinations, skipped = MODULE.build_combinations(list(MODULE.DEFAULT_PROTECTED_RS) + [0.0, -1.0])
        self.assertEqual(combinations, [1.5, 1.8])
        self.assertEqual(skipped, [0.0, -1.0])

    def test_write_summary_csv_sorts_by_equity_then_drawdown(self):
        rows = [
            {'label': 'b', 'end_equity': 530000.0, 'max_drawdown': 0.05},
            {'label': 'a', 'end_equity': 520000.0, 'max_drawdown': 0.10},
            {'label': 'c', 'end_equity': 520000.0, 'max_drawdown': 0.08},
        ]
        for row in rows:
            for column in MODULE.SUMMARY_COLUMNS:
                row.setdefault(column, 0)
            row['output_dir'] = f"reports/{row['label']}"
            row['protected_activate_r'] = 1.5
            row['breakout_atr_buffer'] = 0.3

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'summary.csv'
            MODULE.write_summary_csv(path, rows)
            with path.open('r', encoding='utf-8', newline='') as handle:
                saved = list(csv.DictReader(handle))

        self.assertEqual([row['label'] for row in saved], ['b', 'c', 'a'])


if __name__ == '__main__':
    unittest.main()
