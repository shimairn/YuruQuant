from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / 'scripts' / 'prune_local_artifacts.py'
SPEC = importlib.util.spec_from_file_location('prune_local_artifacts_test', MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class PruneLocalArtifactsTest(unittest.TestCase):
    def _make_dir(self, root: Path, relative: str) -> Path:
        path = root / relative
        path.mkdir(parents=True, exist_ok=True)
        (path / '.keep').write_text('x', encoding='utf-8')
        return path

    def test_build_prune_plan_marks_legacy_reports_and_missing_canonical(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._make_dir(repo_root, 'reports/liquid_top10_dual_core')
            self._make_dir(repo_root, 'reports/liquid_top20_dual_core_20260309')
            legacy = self._make_dir(repo_root, 'reports/legacy_grid_v1')

            plan = MODULE.build_prune_plan(repo_root)

            self.assertIn(legacy.resolve(), plan.prune_dirs)
            self.assertIn((repo_root / 'reports/minimal_stable_top10_v2').resolve(), plan.missing_canonical_dirs)
            self.assertIn((repo_root / 'reports/grid_protected_top10_3m').resolve(), plan.missing_canonical_dirs)
            self.assertNotIn((repo_root / 'reports/liquid_top10_dual_core').resolve(), plan.prune_dirs)

    def test_apply_prune_plan_dry_run_does_not_delete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            legacy = self._make_dir(repo_root, 'reports/legacy_grid_v1')
            plan = MODULE.build_prune_plan(repo_root)

            deleted = MODULE.apply_prune_plan(plan, dry_run=True)

            self.assertEqual([], deleted)
            self.assertTrue(legacy.exists())

    def test_apply_prune_plan_requires_canonical_reports_before_execute(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._make_dir(repo_root, 'reports/legacy_grid_v1')
            plan = MODULE.build_prune_plan(repo_root)

            with self.assertRaises(RuntimeError):
                MODULE.apply_prune_plan(plan, dry_run=False)

    def test_apply_prune_plan_executes_when_canonical_reports_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            for relative in MODULE.CANONICAL_KEEP_DIRS:
                self._make_dir(repo_root, relative)
            legacy = self._make_dir(repo_root, 'reports/legacy_grid_v1')
            temp_configs = self._make_dir(repo_root, 'reports/temp_configs')
            plan = MODULE.build_prune_plan(repo_root)

            deleted = MODULE.apply_prune_plan(plan, dry_run=False)

            deleted_paths = {path.as_posix() for path in deleted}
            self.assertIn(legacy.resolve().as_posix(), deleted_paths)
            self.assertIn(temp_configs.resolve().as_posix(), deleted_paths)
            self.assertFalse(legacy.exists())
            self.assertFalse(temp_configs.exists())

    def test_gmcache_is_only_pruned_when_explicitly_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._make_dir(repo_root, 'gmcache/session_cache')
            default_plan = MODULE.build_prune_plan(repo_root)
            explicit_plan = MODULE.build_prune_plan(repo_root, include_gmcache=True)

            self.assertEqual(tuple(), tuple(path for path in default_plan.prune_dirs if path.name == 'gmcache'))
            self.assertIn((repo_root / 'gmcache').resolve(), explicit_plan.prune_dirs)


if __name__ == '__main__':
    unittest.main()
