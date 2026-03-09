from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / 'scripts' / 'top20_drawdown_recovery_research.py'
SPEC = importlib.util.spec_from_file_location('top20_drawdown_recovery_research_test', MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class Top20DrawdownRecoveryResearchTest(unittest.TestCase):
    def test_build_run_payload_overrides_only_portfolio_recovery_controls(self) -> None:
        base_payload = MODULE.load_yaml(REPO_ROOT / 'config.example' / 'liquid_top20_dual_core.yaml')
        payload = MODULE.build_run_payload(
            base_payload,
            MODULE.PROFILES[2],
            REPO_ROOT / 'reports' / 'tmp_drawdown_recovery',
        )

        self.assertEqual(0.015, payload['portfolio']['risk_per_trade_ratio'])
        self.assertEqual(0.20, payload['portfolio']['max_drawdown_halt_ratio'])
        self.assertEqual('reports/liquid_top20_dual_core', base_payload['reporting']['output_dir'])
        self.assertEqual(0.15, base_payload['portfolio']['max_drawdown_halt_ratio'])

    def test_recommendation_requires_better_return_and_shorter_lockout(self) -> None:
        candidate = MODULE.recommendation(
            {
                'net_return_ratio': 0.02,
                'max_drawdown': 0.16,
                'portfolio_halt_count': 10,
                'lockout_halt_days': 8,
                'max_consecutive_halt_days': 5,
            },
            {
                'net_return_ratio': -0.08,
                'max_drawdown': 0.17,
                'portfolio_halt_count': 54,
                'lockout_halt_days': 52,
                'max_consecutive_halt_days': 53,
            },
        )
        rejected = MODULE.recommendation(
            {
                'net_return_ratio': -0.10,
                'max_drawdown': 0.19,
                'portfolio_halt_count': 50,
                'lockout_halt_days': 40,
                'max_consecutive_halt_days': 40,
            },
            {
                'net_return_ratio': -0.08,
                'max_drawdown': 0.17,
                'portfolio_halt_count': 54,
                'lockout_halt_days': 52,
                'max_consecutive_halt_days': 53,
            },
        )

        self.assertEqual('candidate', candidate)
        self.assertEqual('do_not_promote', rejected)

    def test_build_baseline_comparison_rows_tracks_lockout_and_streak_deltas(self) -> None:
        rows = MODULE.build_baseline_comparison_rows(
            [
                {
                    'label': 'p1',
                    'net_return_ratio': -0.04,
                    'max_drawdown': 0.15,
                    'portfolio_halt_count': 20,
                    'lockout_halt_days': 18,
                    'max_consecutive_halt_days': 12,
                    'post_first_halt_trade_entries': 5,
                    'recommendation': 'candidate',
                }
            ],
            {
                'label': 'baseline_current',
                'net_return_ratio': -0.08,
                'max_drawdown': 0.17,
                'portfolio_halt_count': 54,
                'lockout_halt_days': 52,
                'max_consecutive_halt_days': 53,
                'post_first_halt_trade_entries': 3,
            },
        )

        self.assertEqual(-34, rows[0]['lockout_halt_days_delta'])
        self.assertEqual(-41, rows[0]['max_consecutive_halt_days_delta'])
        self.assertEqual(2, rows[0]['post_first_halt_trade_entries_delta'])
        self.assertEqual('candidate', rows[0]['recommendation'])


if __name__ == '__main__':
    unittest.main()
