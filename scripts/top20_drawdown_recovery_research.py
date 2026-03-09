from __future__ import annotations

import argparse
import sys
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from yuruquant.app.config_loader import load_config
from yuruquant.app.config_schema import AppConfig
from yuruquant.reporting.recovery import build_halt_recovery_report
from yuruquant.reporting.summary import summarize_backtest_run
from yuruquant.reporting.trade_records import build_trade_records
from yuruquant.research.workflows import build_multiplier_lookup, load_yaml, reports_exist, run_backtest, write_rows_csv, write_yaml


DEFAULT_PYTHON_EXE = r'C:\Users\wuktt\miniconda3\envs\minner\python.exe'
SUMMARY_COLUMNS = [
    'label',
    'risk_per_trade_ratio',
    'max_drawdown_halt_ratio',
    'trades',
    'wins',
    'losses',
    'win_rate',
    'net_profit',
    'net_return_ratio',
    'max_drawdown',
    'portfolio_halt_count',
    'lockout_halt_days',
    'lockout_halt_share',
    'halt_streak_count',
    'max_consecutive_halt_days',
    'first_halt_drawdown_ratio',
    'post_first_halt_trade_entries',
    'max_drawdown_duration_days',
    'recovery_assessment',
    'recommendation',
    'output_dir',
]
BASELINE_COMPARISON_COLUMNS = [
    'label',
    'baseline_label',
    'net_return_ratio_delta',
    'max_drawdown_delta',
    'portfolio_halt_count_delta',
    'lockout_halt_days_delta',
    'max_consecutive_halt_days_delta',
    'post_first_halt_trade_entries_delta',
    'recommendation',
]


@dataclass(frozen=True)
class RecoveryProfile:
    label: str
    risk_per_trade_ratio: float
    max_drawdown_halt_ratio: float


PROFILES = (
    RecoveryProfile('control_r15_dd15', 0.015, 0.15),
    RecoveryProfile('relax_dd18_r15', 0.015, 0.18),
    RecoveryProfile('relax_dd20_r15', 0.015, 0.20),
    RecoveryProfile('relax_dd20_r10', 0.010, 0.20),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run Top20 drawdown-halt and recovery research on the GM-only CTA baseline.')
    parser.add_argument('--base-config', default='config/liquid_top20_dual_core.yaml')
    parser.add_argument('--baseline-output-dir')
    parser.add_argument('--python-exe', default=DEFAULT_PYTHON_EXE)
    parser.add_argument('--output-root', default='reports/top20_drawdown_recovery_v1')
    parser.add_argument('--profile', choices=[item.label for item in PROFILES])
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--keep-configs', action='store_true')
    return parser.parse_args()


def resolve_output_dir(raw_output_dir: str) -> Path:
    output_dir = Path(raw_output_dir)
    if output_dir.is_absolute():
        return output_dir
    return (REPO_ROOT / output_dir).resolve()


def build_run_payload(base_payload: dict[str, Any], profile: RecoveryProfile, output_dir: Path) -> dict[str, Any]:
    payload = deepcopy(base_payload)
    payload.setdefault('runtime', {})
    payload.setdefault('portfolio', {})
    payload.setdefault('reporting', {})
    payload['runtime']['run_id'] = profile.label
    payload['portfolio']['risk_per_trade_ratio'] = float(profile.risk_per_trade_ratio)
    payload['portfolio']['max_drawdown_halt_ratio'] = float(profile.max_drawdown_halt_ratio)
    payload['reporting']['output_dir'] = output_dir.as_posix()
    return payload


def recommendation(row: dict[str, Any], baseline: dict[str, Any] | None = None) -> str:
    if baseline is None:
        return 'research_only'
    return_delta = float(row.get('net_return_ratio', 0.0) or 0.0) - float(baseline.get('net_return_ratio', 0.0) or 0.0)
    drawdown_delta = float(row.get('max_drawdown', 0.0) or 0.0) - float(baseline.get('max_drawdown', 0.0) or 0.0)
    halt_delta = int(row.get('portfolio_halt_count', 0) or 0) - int(baseline.get('portfolio_halt_count', 0) or 0)
    lockout_delta = int(row.get('lockout_halt_days', 0) or 0) - int(baseline.get('lockout_halt_days', 0) or 0)
    streak_delta = int(row.get('max_consecutive_halt_days', 0) or 0) - int(baseline.get('max_consecutive_halt_days', 0) or 0)
    if return_delta >= 0.0 and lockout_delta < 0 and streak_delta < 0 and drawdown_delta <= 0.02 and halt_delta <= 0:
        return 'candidate'
    return 'do_not_promote'


def collect_run_summary(label: str, config: AppConfig, output_dir: Path, baseline: dict[str, Any] | None = None) -> dict[str, Any]:
    signals_path = output_dir / 'signals.csv'
    executions_path = output_dir / 'executions.csv'
    portfolio_daily_path = output_dir / 'portfolio_daily.csv'
    multipliers = build_multiplier_lookup(config)
    trades = build_trade_records(signals_path, multipliers, executions_path)
    backtest = summarize_backtest_run(signals_path, portfolio_daily_path, multipliers, executions_path)
    recovery = build_halt_recovery_report(trades, portfolio_daily_path)
    row = {
        'label': label,
        'risk_per_trade_ratio': config.portfolio.risk_per_trade_ratio,
        'max_drawdown_halt_ratio': config.portfolio.max_drawdown_halt_ratio,
        'trades': int(backtest.get('trades', 0) or 0),
        'wins': int(backtest.get('wins', 0) or 0),
        'losses': int(backtest.get('losses', 0) or 0),
        'win_rate': float(backtest.get('win_rate', 0.0) or 0.0),
        'net_profit': float(backtest.get('net_profit', 0.0) or 0.0),
        'net_return_ratio': float(backtest.get('return_ratio', 0.0) or 0.0),
        'max_drawdown': float(backtest.get('max_drawdown', 0.0) or 0.0),
        'portfolio_halt_count': int(backtest.get('halt_days', 0) or 0),
        'lockout_halt_days': int(recovery.summary.get('lockout_halt_days', 0) or 0),
        'lockout_halt_share': float(recovery.summary.get('lockout_halt_share', 0.0) or 0.0),
        'halt_streak_count': int(recovery.summary.get('halt_streak_count', 0) or 0),
        'max_consecutive_halt_days': int(recovery.summary.get('max_consecutive_halt_days', 0) or 0),
        'first_halt_drawdown_ratio': float(recovery.summary.get('first_halt_drawdown_ratio', 0.0) or 0.0),
        'post_first_halt_trade_entries': int(recovery.summary.get('post_first_halt_trade_entries', 0) or 0),
        'max_drawdown_duration_days': int(recovery.summary.get('max_drawdown_duration_days', 0) or 0),
        'recovery_assessment': recovery.summary.get('recovery_assessment', ''),
        'output_dir': output_dir.as_posix(),
    }
    row['recommendation'] = recommendation(row, baseline)
    return row


def build_baseline_comparison_rows(rows: list[dict[str, Any]], baseline: dict[str, Any] | None) -> list[dict[str, Any]]:
    if baseline is None:
        return []
    comparison_rows: list[dict[str, Any]] = []
    for row in rows:
        comparison_rows.append(
            {
                'label': row['label'],
                'baseline_label': baseline.get('label', 'baseline'),
                'net_return_ratio_delta': float(row.get('net_return_ratio', 0.0) or 0.0) - float(baseline.get('net_return_ratio', 0.0) or 0.0),
                'max_drawdown_delta': float(row.get('max_drawdown', 0.0) or 0.0) - float(baseline.get('max_drawdown', 0.0) or 0.0),
                'portfolio_halt_count_delta': int(row.get('portfolio_halt_count', 0) or 0) - int(baseline.get('portfolio_halt_count', 0) or 0),
                'lockout_halt_days_delta': int(row.get('lockout_halt_days', 0) or 0) - int(baseline.get('lockout_halt_days', 0) or 0),
                'max_consecutive_halt_days_delta': int(row.get('max_consecutive_halt_days', 0) or 0) - int(baseline.get('max_consecutive_halt_days', 0) or 0),
                'post_first_halt_trade_entries_delta': int(row.get('post_first_halt_trade_entries', 0) or 0) - int(baseline.get('post_first_halt_trade_entries', 0) or 0),
                'recommendation': row.get('recommendation', 'research_only'),
            }
        )
    return comparison_rows


def order_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            0 if row.get('recommendation') == 'candidate' else 1,
            int(row.get('lockout_halt_days', 9999) or 9999),
            int(row.get('max_consecutive_halt_days', 9999) or 9999),
            -float(row.get('net_return_ratio', -1.0) or -1.0),
            float(row.get('max_drawdown', 1.0) or 1.0),
            str(row.get('label', '')),
        ),
    )


def write_decision(path: Path, rows: list[dict[str, Any]], baseline: dict[str, Any] | None = None) -> None:
    ordered = order_rows(rows)
    best = ordered[0] if ordered else None
    lines = [
        '# Top20 Drawdown Recovery Decision',
        '',
        '- branch: `trend_identity`',
        '- objective: `reduce extended drawdown stall without distorting the GM-only CTA mainline`',
        f"- promotion_recommendation: `{best.get('recommendation', 'research_only') if best else 'research_only'}`",
    ]
    if baseline is not None:
        lines.extend(
            [
                f"- baseline_label: `{baseline.get('label', 'baseline_current')}`",
                f"- baseline_return_ratio: `{float(baseline.get('net_return_ratio', 0.0) or 0.0):.4f}`",
                f"- baseline_max_drawdown: `{float(baseline.get('max_drawdown', 0.0) or 0.0):.4f}`",
                f"- baseline_lockout_halt_days: `{int(baseline.get('lockout_halt_days', 0) or 0)}`",
                f"- baseline_max_consecutive_halt_days: `{int(baseline.get('max_consecutive_halt_days', 0) or 0)}`",
            ]
        )
    if best is not None:
        lines.extend(
            [
                f"- best_label: `{best['label']}`",
                f"- best_return_ratio: `{float(best.get('net_return_ratio', 0.0) or 0.0):.4f}`",
                f"- best_max_drawdown: `{float(best.get('max_drawdown', 0.0) or 0.0):.4f}`",
                f"- best_lockout_halt_days: `{int(best.get('lockout_halt_days', 0) or 0)}`",
                f"- best_recovery_assessment: `{best.get('recovery_assessment', '')}`",
            ]
        )
    lines.extend(
        [
            '',
            '## Profile Snapshot',
            '',
            '| label | risk/trade | dd halt | return | max dd | halt days | lockout days | max halt streak | assessment | recommendation |',
            '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |',
        ]
    )
    for row in ordered:
        lines.append(
            f"| {row['label']} | {float(row.get('risk_per_trade_ratio', 0.0)):.3f} | {float(row.get('max_drawdown_halt_ratio', 0.0)):.3f} | {float(row.get('net_return_ratio', 0.0) or 0.0)*100:.2f}% | {float(row.get('max_drawdown', 0.0) or 0.0)*100:.2f}% | {int(row.get('portfolio_halt_count', 0) or 0)} | {int(row.get('lockout_halt_days', 0) or 0)} | {int(row.get('max_consecutive_halt_days', 0) or 0)} | {row.get('recovery_assessment', '')} | {row.get('recommendation', '')} |"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def main() -> int:
    args = parse_args()
    base_config_path = (REPO_ROOT / args.base_config).resolve()
    output_root = (REPO_ROOT / args.output_root).resolve()
    configs_dir = output_root / 'configs'
    base_payload = load_yaml(base_config_path)
    baseline_config = load_config(base_config_path)
    baseline_output_dir = resolve_output_dir(
        args.baseline_output_dir if args.baseline_output_dir else baseline_config.reporting.output_dir
    )
    baseline_summary: dict[str, Any] | None = None
    if reports_exist(baseline_output_dir):
        baseline_summary = collect_run_summary('baseline_current', baseline_config, baseline_output_dir)

    profiles = [item for item in PROFILES if args.profile is None or item.label == args.profile]
    rows: list[dict[str, Any]] = []
    for index, profile in enumerate(profiles, start=1):
        run_dir = output_root / profile.label
        config_path = configs_dir / f'{profile.label}.yaml'
        payload = build_run_payload(base_payload, profile, run_dir)
        write_yaml(config_path, payload)
        config = load_config(config_path)
        print(
            f"[{index}/{len(profiles)}] {profile.label}: "
            f"risk={config.portfolio.risk_per_trade_ratio:.3f}, "
            f"dd_halt={config.portfolio.max_drawdown_halt_ratio:.3f}"
        )
        if args.force or not reports_exist(run_dir):
            run_backtest(REPO_ROOT, args.python_exe, config_path, config.runtime.run_id)
        else:
            print(f'  skipping existing raw reports at {run_dir.as_posix()}')
        row = collect_run_summary(profile.label, config, run_dir, baseline_summary)
        rows.append(row)
        write_rows_csv(run_dir / 'summary_research.csv', SUMMARY_COLUMNS, [row])
        if not args.keep_configs and config_path.exists():
            config_path.unlink()

    if not args.keep_configs and configs_dir.exists() and not any(configs_dir.iterdir()):
        configs_dir.rmdir()
    ordered = order_rows(rows)
    write_rows_csv(output_root / 'summary_research.csv', SUMMARY_COLUMNS, ordered)
    comparison_rows = build_baseline_comparison_rows(ordered, baseline_summary)
    if comparison_rows:
        write_rows_csv(output_root / 'baseline_comparison.csv', BASELINE_COMPARISON_COLUMNS, comparison_rows)
    write_decision(output_root / 'decision.md', ordered, baseline_summary)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
