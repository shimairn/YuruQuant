from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / 'scripts' / 'top20_cluster_risk_research.py'
SPEC = importlib.util.spec_from_file_location('top20_cluster_risk_research_test', MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class Top20ClusterRiskResearchTest(unittest.TestCase):
    def test_load_base_payload_backfills_risk_clusters_from_canonical_template(self) -> None:
        base_payload = MODULE.load_yaml(REPO_ROOT / 'config.example' / 'liquid_top20_dual_core.yaml')
        base_payload['universe'].pop('risk_clusters', None)

        merged = MODULE.ensure_risk_clusters(
            base_payload,
            MODULE.load_yaml(REPO_ROOT / 'config.example' / 'liquid_top20_dual_core.yaml'),
        )

        self.assertNotIn('risk_clusters', base_payload['universe'])
        self.assertIn('soy_complex', merged['universe']['risk_clusters'])
        self.assertIn('ferrous', merged['universe']['risk_clusters'])

    def test_build_run_payload_applies_cluster_limits_and_trend_identity(self) -> None:
        base_payload = MODULE.load_yaml(REPO_ROOT / 'config.example' / 'liquid_top20_dual_core.yaml')
        payload = MODULE.build_run_payload(
            base_payload,
            MODULE.PROFILES[1],
            REPO_ROOT / 'reports' / 'tmp_cluster',
        )

        self.assertEqual('900s', payload['universe']['entry_frequency'])
        self.assertEqual('3600s', payload['universe']['trend_frequency'])
        self.assertEqual(12, payload['strategy']['entry']['donchian_lookback'])
        self.assertEqual(1, payload['strategy']['entry']['entry_block_major_gap_bars'])
        self.assertEqual(1.8, payload['strategy']['exit']['protected_activate_r'])
        self.assertEqual(1, payload['strategy']['exit']['armed_flush_buffer_bars'])
        self.assertEqual('disabled', payload['strategy']['exit']['session_flat_scope'])
        self.assertEqual(0.010, payload['portfolio']['risk_per_trade_ratio'])
        self.assertEqual(0.020, payload['portfolio']['max_total_armed_risk_ratio'])
        self.assertEqual(0.010, payload['portfolio']['max_cluster_armed_risk_ratio'])
        self.assertEqual(2, payload['portfolio']['max_same_direction_cluster_positions'])
        self.assertIn('soy_complex', payload['universe']['risk_clusters'])

    def test_gate_status_prefers_lower_halt_profiles(self) -> None:
        passed, status = MODULE.gate_status(
            {
                'return_ratio': -0.08,
                'max_drawdown': 0.12,
                'portfolio_halt_count_costed': 10,
                'protected_reach_count': 3,
            }
        )
        failed, failed_status = MODULE.gate_status(
            {
                'return_ratio': -0.18,
                'max_drawdown': 0.20,
                'portfolio_halt_count_costed': 33,
                'protected_reach_count': 1,
            }
        )

        self.assertEqual(1, passed)
        self.assertEqual('ok', status)
        self.assertEqual(0, failed)
        self.assertIn('halt_days>20', failed_status)

    def test_build_baseline_comparison_rows_marks_non_improving_profiles(self) -> None:
        comparison = MODULE.build_baseline_comparison_rows(
            [
                {
                    'label': 'candidate_a',
                    'net_return_ratio': -0.10,
                    'max_drawdown': 0.12,
                    'portfolio_halt_count_costed': 10,
                    'protected_reach_count': 4,
                    'trades': 18,
                },
                {
                    'label': 'candidate_b',
                    'net_return_ratio': -0.15,
                    'max_drawdown': 0.18,
                    'portfolio_halt_count_costed': 24,
                    'protected_reach_count': 1,
                    'trades': 11,
                },
            ],
            {
                'label': 'baseline_current',
                'net_return_ratio': -0.12,
                'max_drawdown': 0.15,
                'portfolio_halt_count_costed': 20,
                'protected_reach_count': 2,
                'trades': 15,
            },
        )

        self.assertEqual('candidate', comparison[0]['promotion_verdict'])
        self.assertEqual(-10, comparison[0]['portfolio_halt_count_delta'])
        self.assertEqual('do_not_promote', comparison[1]['promotion_verdict'])
        self.assertEqual(4, comparison[1]['portfolio_halt_count_delta'])


if __name__ == '__main__':
    unittest.main()
