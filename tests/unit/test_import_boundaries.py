from __future__ import annotations

import ast
import unittest
from pathlib import Path


class ImportBoundaryTest(unittest.TestCase):
    BANNED_IMPORTS = {
        'yuruquant.adapters.gm',
        'yuruquant.app.config',
        'yuruquant.portfolio',
        'yuruquant.reporting.analysis',
        'yuruquant.strategy.trend_breakout',
    }

    def test_runtime_code_avoids_internal_facade_imports(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        source_roots = [repo_root / 'yuruquant', repo_root / 'scripts']
        violations: list[str] = []

        for source_root in source_roots:
            for path in source_root.rglob('*.py'):
                if path.name == '__init__.py':
                    continue
                relative = path.relative_to(repo_root).as_posix()
                tree = ast.parse(path.read_text(encoding='utf-8-sig'), filename=str(path))
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            if alias.name in self.BANNED_IMPORTS:
                                violations.append(f'{relative}:{node.lineno} imports {alias.name}')
                    elif isinstance(node, ast.ImportFrom):
                        module = node.module or ''
                        if module in self.BANNED_IMPORTS:
                            violations.append(f'{relative}:{node.lineno} imports from {module}')

        self.assertEqual([], violations)
