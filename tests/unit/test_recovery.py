from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from yuruquant.reporting.recovery import build_halt_recovery_report
from yuruquant.reporting.trade_records import TradeRecord


def _build_trade(campaign_id: str, entry_ts: str, exit_ts: str) -> TradeRecord:
    return TradeRecord(
        campaign_id=campaign_id,
        csymbol='DCE.I',
        entry_signal_ts=entry_ts,
        entry_fill_ts=entry_ts,
        exit_signal_ts=exit_ts,
        exit_fill_ts=exit_ts,
        direction=-1,
        qty=1,
        entry_signal_price=100.0,
        entry_fill_price=100.0,
        exit_signal_price=95.0,
        exit_fill_price=95.0,
        initial_stop_loss=102.0,
        protected_stop_price=99.0,
        exit_reason='diagnostic',
        exit_trigger='protected_stop',
        phase_at_exit='protected',
        mfe_r=2.0,
        multiplier=10.0,
        pnl_points=5.0,
        gross_pnl=50.0,
        theoretical_stop_price=99.0,
        theoretical_stop_gross_pnl=10.0,
        overshoot_pnl=0.0,
        overshoot_ratio=0.0,
        exit_execution_regime='normal',
        exit_fill_gap_points=0.0,
        exit_fill_gap_atr=0.0,
    )


def _write_portfolio_daily(path: Path, rows: list[tuple[str, float, float, float, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                'run_id',
                'mode',
                'date',
                'snapshot_ts',
                'equity_start',
                'equity_end',
                'equity_peak',
                'drawdown_ratio',
                'risk_state',
                'effective_risk_mult',
                'trades_count',
                'wins',
                'losses',
                'realized_pnl',
                'halt_flag',
                'halt_reason',
            ]
        )
        for trade_day, equity_end, equity_peak, drawdown_ratio, halt_flag in rows:
            writer.writerow(
                [
                    'r',
                    'BACKTEST',
                    trade_day,
                    f'{trade_day} 15:00:00',
                    '500000',
                    f'{equity_end:.2f}',
                    f'{equity_peak:.2f}',
                    f'{drawdown_ratio:.6f}',
                    'halt_drawdown' if halt_flag == '1' else 'normal',
                    '0.0' if halt_flag == '1' else '1.0',
                    '0',
                    '0',
                    '0',
                    f'{equity_end - 500000:.2f}',
                    halt_flag,
                    'drawdown' if halt_flag == '1' else '',
                ]
            )


class HaltRecoveryTest(unittest.TestCase):
    def test_build_halt_recovery_report_flags_persistent_lockout(self) -> None:
        trades = [_build_trade('c1', '2026-01-05 09:00:00', '2026-01-07 10:00:00')]
        with tempfile.TemporaryDirectory() as tmp:
            portfolio_path = Path(tmp) / 'portfolio_daily.csv'
            _write_portfolio_daily(
                portfolio_path,
                [
                    ('2026-01-05', 500000.0, 500000.0, 0.0, '0'),
                    ('2026-01-06', 470000.0, 500000.0, 0.06, '0'),
                    ('2026-01-07', 420000.0, 500000.0, 0.16, '1'),
                    ('2026-01-08', 420000.0, 500000.0, 0.16, '1'),
                    ('2026-01-09', 420000.0, 500000.0, 0.16, '1'),
                ],
            )
            report = build_halt_recovery_report(trades, portfolio_path)

        self.assertEqual('persistent_lockout', report.summary['recovery_assessment'])
        self.assertEqual(3, report.summary['halt_days'])
        self.assertEqual(2, report.summary['lockout_halt_days'])
        self.assertEqual(1, report.summary['halt_streak_count'])
        self.assertEqual(3, report.summary['max_consecutive_halt_days'])
        self.assertEqual(0, report.summary['post_first_halt_trade_entries'])
        self.assertEqual(2, report.halt_streak_rows[0]['lockout_days'])

    def test_build_halt_recovery_report_tracks_recovery_episode(self) -> None:
        trades = [_build_trade('c1', '2026-01-05 09:00:00', '2026-01-06 10:00:00')]
        with tempfile.TemporaryDirectory() as tmp:
            portfolio_path = Path(tmp) / 'portfolio_daily.csv'
            _write_portfolio_daily(
                portfolio_path,
                [
                    ('2026-01-05', 500000.0, 500000.0, 0.0, '0'),
                    ('2026-01-06', 470000.0, 500000.0, 0.06, '0'),
                    ('2026-01-07', 440000.0, 500000.0, 0.12, '1'),
                    ('2026-01-08', 500000.0, 500000.0, 0.0, '0'),
                ],
            )
            report = build_halt_recovery_report(trades, portfolio_path)

        self.assertEqual('recovered_after_halt', report.summary['recovery_assessment'])
        self.assertEqual(1, report.summary['halt_days'])
        self.assertEqual(1, report.summary['lockout_halt_days'])
        self.assertEqual(1, report.halt_streak_rows[0]['recovered_to_peak'])
        self.assertEqual(1, report.halt_streak_rows[0]['days_to_recover_peak'])
        self.assertEqual(3, report.summary['max_drawdown_duration_days'])
        self.assertEqual(1, report.drawdown_episode_rows[0]['recovered'])


if __name__ == '__main__':
    unittest.main()
