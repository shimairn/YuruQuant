from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / 'scripts' / 'top20_drawdown_schedule_research.py'
SPEC = importlib.util.spec_from_file_location('top20_drawdown_schedule_research_test', MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class Top20DrawdownScheduleResearchTest(unittest.TestCase):
    def test_build_run_payload_applies_drawdown_schedule_without_mutating_base_payload(self) -> None:
        base_payload = MODULE.load_yaml(REPO_ROOT / 'config.example' / 'liquid_top20_dual_core.yaml')
        payload = MODULE.build_run_payload(
            base_payload,
            MODULE.PROFILES[1],
            REPO_ROOT / 'reports' / 'tmp_drawdown_schedule',
        )

        self.assertEqual('disabled', payload['portfolio']['drawdown_halt_mode'])
        self.assertEqual(
            [
                {'drawdown_ratio': 0.08, 'risk_mult': 0.50},
                {'drawdown_ratio': 0.12, 'risk_mult': 0.25},
                {'drawdown_ratio': 0.16, 'risk_mult': 0.10},
            ],
            payload['portfolio']['drawdown_risk_schedule'],
        )
        self.assertEqual(0.015, payload['portfolio']['risk_per_trade_ratio'])
        self.assertEqual(0.15, base_payload['portfolio']['max_drawdown_halt_ratio'])
        self.assertNotIn('drawdown_halt_mode', base_payload['portfolio'])
        self.assertNotIn('drawdown_risk_schedule', base_payload['portfolio'])

    def test_resolve_profiles_keeps_control_reference_for_single_candidate_runs(self) -> None:
        profiles = MODULE.resolve_profiles('schedule_disablehalt_r10')

        self.assertEqual([MODULE.CONTROL_PROFILE_LABEL, 'schedule_disablehalt_r10'], [profile.label for profile in profiles])

    def test_recommend_requires_shorter_lockout_and_non_worse_return(self) -> None:
        candidate = MODULE.recommend(
            {
                'net_return_ratio': -0.04,
                'max_drawdown': 0.16,
                'portfolio_halt_count': 18,
                'lockout_halt_days': 12,
                'max_consecutive_halt_days': 6,
                'post_first_halt_trade_entries': 5,
            },
            {
                'net_return_ratio': -0.08,
                'max_drawdown': 0.17,
                'portfolio_halt_count': 54,
                'lockout_halt_days': 52,
                'max_consecutive_halt_days': 53,
                'post_first_halt_trade_entries': 3,
            },
        )
        rejected = MODULE.recommend(
            {
                'net_return_ratio': -0.02,
                'max_drawdown': 0.16,
                'portfolio_halt_count': 18,
                'lockout_halt_days': 52,
                'max_consecutive_halt_days': 40,
                'post_first_halt_trade_entries': 5,
            },
            {
                'net_return_ratio': -0.08,
                'max_drawdown': 0.17,
                'portfolio_halt_count': 54,
                'lockout_halt_days': 52,
                'max_consecutive_halt_days': 53,
                'post_first_halt_trade_entries': 3,
            },
        )

        self.assertEqual('candidate', candidate)
        self.assertEqual('do_not_promote', rejected)

    def test_build_comparison_rows_tracks_lockout_and_reentry_deltas(self) -> None:
        rows = MODULE.build_comparison_rows(
            [
                {
                    'label': 'schedule_disablehalt_r15',
                    'net_return_ratio': -0.05,
                    'max_drawdown': 0.16,
                    'portfolio_halt_count': 20,
                    'lockout_halt_days': 16,
                    'max_consecutive_halt_days': 7,
                    'post_first_halt_trade_entries': 6,
                    'recommendation': 'candidate',
                }
            ],
            {
                'label': MODULE.CONTROL_PROFILE_LABEL,
                'net_return_ratio': -0.08,
                'max_drawdown': 0.17,
                'portfolio_halt_count': 54,
                'lockout_halt_days': 52,
                'max_consecutive_halt_days': 53,
                'post_first_halt_trade_entries': 2,
            },
        )

        self.assertEqual(-36, rows[0]['lockout_halt_days_delta'])
        self.assertEqual(-46, rows[0]['max_consecutive_halt_days_delta'])
        self.assertEqual(4, rows[0]['post_first_halt_trade_entries_delta'])
        self.assertEqual('candidate', rows[0]['recommendation'])


if __name__ == '__main__':
    unittest.main()
