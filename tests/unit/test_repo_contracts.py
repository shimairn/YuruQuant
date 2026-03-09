from __future__ import annotations

import re
import unittest
from pathlib import Path


class RepoContractsTest(unittest.TestCase):
    def test_strategy_docs_exist_and_legacy_summary_is_removed(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        self.assertTrue((repo_root / 'docs' / 'strategy_doctrine.md').exists())
        self.assertTrue((repo_root / 'docs' / 'research_roadmap.md').exists())
        self.assertFalse((repo_root / 'docs' / 'dual_core_v3_research_summary.md').exists())

    def test_tracked_docs_do_not_reintroduce_removed_exit_narrative(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        tracked_docs = [repo_root / 'README.md', *(repo_root / 'docs').glob('*.md')]
        banned_patterns = (
            r'\bascended(_activate_r)?\b',
            r'\bhourly_ma_stop\b',
            r'three-state',
            r'3-stage',
        )

        violations: list[str] = []
        for path in tracked_docs:
            content = path.read_text(encoding='utf-8')
            for pattern in banned_patterns:
                if re.search(pattern, content, flags=re.IGNORECASE):
                    violations.append(f'{path.relative_to(repo_root).as_posix()} matched {pattern}')

        self.assertEqual([], violations)


if __name__ == '__main__':
    unittest.main()
