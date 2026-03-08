from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / 'scripts' / 'dual_branch_effectiveness_research.py'
SPEC = importlib.util.spec_from_file_location('dual_branch_effectiveness_research_test', MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class DualBranchResearchScriptTest(unittest.TestCase):
    def test_trend_branch_payload_uses_15m_identity_settings(self):
        payload = MODULE.build_run_payload(
            MODULE.load_yaml(REPO_ROOT / 'config' / 'liquid_top10_dual_core.yaml'),
            MODULE.TREND_BRANCH,
            MODULE.RISK_PROFILES[0],
            REPO_ROOT / 'reports' / 'tmp_trend',
        )
        self.assertEqual('900s', payload['universe']['entry_frequency'])
        self.assertEqual('3600s', payload['universe']['trend_frequency'])
        self.assertEqual(12, payload['strategy']['entry']['donchian_lookback'])
        self.assertEqual(1, payload['strategy']['entry']['entry_block_major_gap_bars'])
        self.assertEqual(1, payload['strategy']['exit']['armed_flush_buffer_bars'])
        self.assertEqual(0, payload['strategy']['exit']['session_flat_all_phases_buffer_bars'])
        self.assertAlmostEqual(0.012, payload['portfolio']['risk_per_trade_ratio'])
        self.assertAlmostEqual(0.024, payload['portfolio']['max_total_armed_risk_ratio'])
        self.assertAlmostEqual(0.0, payload['execution']['backtest_commission_ratio'])
        self.assertAlmostEqual(0.0, payload['execution']['backtest_slippage_ratio'])

    def test_intraday_branch_payload_uses_session_flat_settings(self):
        payload = MODULE.build_run_payload(
            MODULE.load_yaml(REPO_ROOT / 'config' / 'liquid_top10_dual_core.yaml'),
            MODULE.INTRADAY_BRANCH,
            MODULE.RISK_PROFILES[1],
            REPO_ROOT / 'reports' / 'tmp_intraday',
        )
        self.assertEqual('300s', payload['universe']['entry_frequency'])
        self.assertEqual(36, payload['strategy']['entry']['donchian_lookback'])
        self.assertEqual(0, payload['strategy']['exit']['armed_flush_buffer_bars'])
        self.assertEqual(1, payload['strategy']['exit']['session_flat_all_phases_buffer_bars'])
        self.assertAlmostEqual(0.015, payload['portfolio']['risk_per_trade_ratio'])
        self.assertAlmostEqual(0.030, payload['portfolio']['max_total_armed_risk_ratio'])

    def test_gate_status_matches_branch_specific_thresholds(self):
        trend_ok, trend_status = MODULE.gate_status(
            MODULE.TREND_BRANCH,
            {
                'net_return_ratio': 0.01,
                'max_drawdown': 0.10,
                'portfolio_halt_count_costed': 0,
                'ascended_exit_count': 2,
                'multi_session_hold_count': 3,
            },
        )
        intraday_ok, intraday_status = MODULE.gate_status(
            MODULE.INTRADAY_BRANCH,
            {
                'net_return_ratio': 0.01,
                'max_drawdown': 0.08,
                'portfolio_halt_count_costed': 0,
                'multi_session_hold_count': 0,
                'session_flat_exit_count': 1,
            },
        )
        self.assertTrue(trend_ok)
        self.assertEqual('ok', trend_status)
        self.assertTrue(intraday_ok)
        self.assertEqual('ok', intraday_status)


if __name__ == '__main__':
    unittest.main()
