from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from yuruquant.reporting.diversification import build_cluster_lookup, build_diversification_report
from yuruquant.reporting.trade_records import TradeRecord


def _build_trade(
    campaign_id: str,
    csymbol: str,
    direction: int,
    entry_ts: str,
    exit_ts: str,
    gross_pnl: float,
) -> TradeRecord:
    entry_price = 100.0
    exit_price = 105.0 if direction > 0 else 95.0
    return TradeRecord(
        campaign_id=campaign_id,
        csymbol=csymbol,
        entry_signal_ts=entry_ts,
        entry_fill_ts=entry_ts,
        exit_signal_ts=exit_ts,
        exit_fill_ts=exit_ts,
        direction=direction,
        qty=1,
        entry_signal_price=entry_price,
        entry_fill_price=entry_price,
        exit_signal_price=exit_price,
        exit_fill_price=exit_price,
        initial_stop_loss=95.0,
        protected_stop_price=101.0,
        exit_reason='diagnostic',
        exit_trigger='protected_stop',
        phase_at_exit='protected',
        mfe_r=2.0,
        multiplier=10.0,
        pnl_points=(exit_price - entry_price) if direction > 0 else (entry_price - exit_price),
        gross_pnl=gross_pnl,
        theoretical_stop_price=101.0,
        theoretical_stop_gross_pnl=10.0,
        overshoot_pnl=0.0,
        overshoot_ratio=0.0,
        exit_execution_regime='normal',
        exit_fill_gap_points=0.0,
        exit_fill_gap_atr=0.0,
    )


def _write_portfolio_daily(path: Path, rows: list[tuple[str, str]]) -> None:
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
        for trade_day, halt_flag in rows:
            writer.writerow(
                [
                    'r',
                    'BACKTEST',
                    trade_day,
                    f'{trade_day} 15:00:00',
                    '500000',
                    '500000',
                    '500000',
                    '0.0',
                    'normal',
                    '1.0',
                    '0',
                    '0',
                    '0',
                    '0',
                    halt_flag,
                    'drawdown' if halt_flag == '1' else '',
                ]
            )


class DiversificationTest(unittest.TestCase):
    def test_build_diversification_report_flags_mixed_lockout(self) -> None:
        cluster_lookup = build_cluster_lookup(
            symbols=['DCE.I', 'SHFE.RB', 'DCE.M'],
            risk_clusters={
                'ferrous': ('DCE.I', 'SHFE.RB'),
                'soy_complex': ('DCE.M',),
            },
        )
        trades = [
            _build_trade('c1', 'DCE.I', -1, '2026-01-05 09:00:00', '2026-01-06 10:00:00', 50.0),
            _build_trade('c2', 'SHFE.RB', -1, '2026-01-05 09:05:00', '2026-01-06 11:00:00', -30.0),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            portfolio_path = Path(tmp) / 'portfolio_daily.csv'
            _write_portfolio_daily(
                portfolio_path,
                [
                    ('2026-01-06', '1'),
                    ('2026-01-07', '1'),
                    ('2026-01-08', '1'),
                ],
            )
            report = build_diversification_report(trades, portfolio_path, cluster_lookup)

        self.assertEqual('mixed_lockout', report.summary['pressure_assessment'])
        self.assertEqual(3, report.summary['halt_days'])
        self.assertEqual(1, report.summary['halt_days_with_active_positions'])
        self.assertEqual(2, report.summary['halt_days_without_positions'])
        self.assertEqual(2, report.summary['max_cluster_same_direction_positions'])
        self.assertEqual('ferrous', report.summary['dominant_cluster_by_halt_days'])
        self.assertEqual(1, report.cluster_rows[0]['halt_day_count'])

    def test_build_diversification_report_flags_cluster_dominant_pressure(self) -> None:
        cluster_lookup = build_cluster_lookup(
            symbols=['DCE.I', 'SHFE.RB', 'DCE.M'],
            risk_clusters={
                'ferrous': ('DCE.I', 'SHFE.RB'),
                'soy_complex': ('DCE.M',),
            },
        )
        trades = [
            _build_trade('c1', 'DCE.I', -1, '2026-01-05 09:00:00', '2026-01-09 10:00:00', 40.0),
            _build_trade('c2', 'SHFE.RB', -1, '2026-01-05 09:05:00', '2026-01-09 11:00:00', 30.0),
            _build_trade('c3', 'DCE.M', 1, '2026-01-06 09:00:00', '2026-01-06 14:00:00', -10.0),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            portfolio_path = Path(tmp) / 'portfolio_daily.csv'
            _write_portfolio_daily(
                portfolio_path,
                [
                    ('2026-01-05', '1'),
                    ('2026-01-06', '1'),
                    ('2026-01-07', '1'),
                ],
            )
            report = build_diversification_report(trades, portfolio_path, cluster_lookup)

        self.assertEqual('cluster_dominant', report.summary['pressure_assessment'])
        self.assertEqual(3, report.summary['halt_days_with_active_positions'])
        self.assertGreaterEqual(report.summary['top_cluster_halt_day_share'], 1.0)
        self.assertEqual(2, report.summary['max_cluster_same_direction_positions'])
        self.assertEqual('ferrous', report.summary['dominant_cluster_by_trade_share'])


if __name__ == '__main__':
    unittest.main()
