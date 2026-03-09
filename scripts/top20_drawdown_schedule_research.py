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
CONTROL_PROFILE_LABEL = 'control_hardhalt_r15'
SUMMARY_COLUMNS = [
    'label',
    'drawdown_halt_mode',
    'drawdown_risk_schedule',
    'risk_per_trade_ratio',
    'net_profit',
    'net_return_ratio',
    'max_drawdown',
    'portfolio_halt_count',
    'lockout_halt_days',
    'max_consecutive_halt_days',
    'post_first_halt_trade_entries',
    'recovery_assessment',
    'recommendation',
    'output_dir',
]
COMPARISON_COLUMNS = [
    'label',
    'control_label',
    'net_return_ratio_delta',
    'max_drawdown_delta',
    'portfolio_halt_count_delta',
    'lockout_halt_days_delta',
    'max_consecutive_halt_days_delta',
    'post_first_halt_trade_entries_delta',
    'recommendation',
]


@dataclass(frozen=True)
class ScheduleProfile:
    label: str
    risk_per_trade_ratio: float
    drawdown_halt_mode: str
    drawdown_risk_schedule: tuple[tuple[float, float], ...]


PROFILES = (
    ScheduleProfile(CONTROL_PROFILE_LABEL, 0.015, 'hard', ()),
    ScheduleProfile('schedule_disablehalt_r15', 0.015, 'disabled', ((0.08, 0.50), (0.12, 0.25), (0.16, 0.10))),
    ScheduleProfile('schedule_disablehalt_r10', 0.010, 'disabled', ((0.08, 0.50), (0.12, 0.25), (0.16, 0.10))),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run Top20 non-absorbing drawdown schedule research on the GM-only CTA baseline.')
    parser.add_argument('--base-config', default='config/liquid_top20_dual_core.yaml')
    parser.add_argument('--python-exe', default=DEFAULT_PYTHON_EXE)
    parser.add_argument('--output-root', default='reports/top20_drawdown_schedule_v1')
    parser.add_argument('--profile', choices=[item.label for item in PROFILES])
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--keep-configs', action='store_true')
    return parser.parse_args()


def _schedule_payload(profile: ScheduleProfile) -> list[dict[str, float]]:
    return [{'drawdown_ratio': ratio, 'risk_mult': risk_mult} for ratio, risk_mult in profile.drawdown_risk_schedule]


def build_run_payload(base_payload: dict[str, Any], profile: ScheduleProfile, output_dir: Path) -> dict[str, Any]:
    payload = deepcopy(base_payload)
    payload.setdefault('runtime', {})
    payload.setdefault('portfolio', {})
    payload.setdefault('reporting', {})
    payload['runtime']['run_id'] = profile.label
    payload['portfolio']['risk_per_trade_ratio'] = float(profile.risk_per_trade_ratio)
    payload['portfolio']['drawdown_halt_mode'] = profile.drawdown_halt_mode
    payload['portfolio']['drawdown_risk_schedule'] = _schedule_payload(profile)
    payload['reporting']['output_dir'] = output_dir.as_posix()
    return payload


def _format_schedule(profile: ScheduleProfile) -> str:
    if not profile.drawdown_risk_schedule:
        return 'none'
    return ';'.join(f'{ratio:.2f}->{risk_mult:.2f}' for ratio, risk_mult in profile.drawdown_risk_schedule)


def resolve_profiles(selected_label: str | None) -> list[ScheduleProfile]:
    if selected_label is None:
        return list(PROFILES)
    selected = next(item for item in PROFILES if item.label == selected_label)
    if selected.label == CONTROL_PROFILE_LABEL:
        return [selected]
    control = next(item for item in PROFILES if item.label == CONTROL_PROFILE_LABEL)
    return [control, selected]


def collect_summary(profile: ScheduleProfile, config: AppConfig, output_dir: Path) -> dict[str, Any]:
    signals_path = output_dir / 'signals.csv'
    executions_path = output_dir / 'executions.csv'
    portfolio_daily_path = output_dir / 'portfolio_daily.csv'
    multipliers = build_multiplier_lookup(config)
    trades = build_trade_records(signals_path, multipliers, executions_path)
    backtest = summarize_backtest_run(signals_path, portfolio_daily_path, multipliers, executions_path)
    recovery = build_halt_recovery_report(trades, portfolio_daily_path)
    return {
        'label': profile.label,
        'drawdown_halt_mode': config.portfolio.drawdown_halt_mode,
        'drawdown_risk_schedule': _format_schedule(profile),
        'risk_per_trade_ratio': config.portfolio.risk_per_trade_ratio,
        'net_profit': float(backtest.get('net_profit', 0.0) or 0.0),
        'net_return_ratio': float(backtest.get('return_ratio', 0.0) or 0.0),
        'max_drawdown': float(backtest.get('max_drawdown', 0.0) or 0.0),
        'portfolio_halt_count': int(backtest.get('halt_days', 0) or 0),
        'lockout_halt_days': int(recovery.summary.get('lockout_halt_days', 0) or 0),
        'max_consecutive_halt_days': int(recovery.summary.get('max_consecutive_halt_days', 0) or 0),
        'post_first_halt_trade_entries': int(recovery.summary.get('post_first_halt_trade_entries', 0) or 0),
        'recovery_assessment': recovery.summary.get('recovery_assessment', ''),
        'output_dir': output_dir.as_posix(),
    }


def recommend(row: dict[str, Any], control: dict[str, Any]) -> str:
    return_delta = float(row.get('net_return_ratio', 0.0) or 0.0) - float(control.get('net_return_ratio', 0.0) or 0.0)
    drawdown_delta = float(row.get('max_drawdown', 0.0) or 0.0) - float(control.get('max_drawdown', 0.0) or 0.0)
    halt_delta = int(row.get('portfolio_halt_count', 0) or 0) - int(control.get('portfolio_halt_count', 0) or 0)
    lockout_delta = int(row.get('lockout_halt_days', 0) or 0) - int(control.get('lockout_halt_days', 0) or 0)
    streak_delta = int(row.get('max_consecutive_halt_days', 0) or 0) - int(control.get('max_consecutive_halt_days', 0) or 0)
    reentry_delta = int(row.get('post_first_halt_trade_entries', 0) or 0) - int(control.get('post_first_halt_trade_entries', 0) or 0)
    if return_delta >= 0.0 and lockout_delta < 0 and streak_delta < 0 and drawdown_delta <= 0.02 and halt_delta <= 0 and reentry_delta >= 0:
        return 'candidate'
    return 'do_not_promote'


def build_comparison_rows(rows: list[dict[str, Any]], control: dict[str, Any]) -> list[dict[str, Any]]:
    comparison_rows: list[dict[str, Any]] = []
    for row in rows:
        comparison_rows.append(
            {
                'label': row['label'],
                'control_label': control['label'],
                'net_return_ratio_delta': float(row.get('net_return_ratio', 0.0) or 0.0) - float(control.get('net_return_ratio', 0.0) or 0.0),
                'max_drawdown_delta': float(row.get('max_drawdown', 0.0) or 0.0) - float(control.get('max_drawdown', 0.0) or 0.0),
                'portfolio_halt_count_delta': int(row.get('portfolio_halt_count', 0) or 0) - int(control.get('portfolio_halt_count', 0) or 0),
                'lockout_halt_days_delta': int(row.get('lockout_halt_days', 0) or 0) - int(control.get('lockout_halt_days', 0) or 0),
                'max_consecutive_halt_days_delta': int(row.get('max_consecutive_halt_days', 0) or 0) - int(control.get('max_consecutive_halt_days', 0) or 0),
                'post_first_halt_trade_entries_delta': int(row.get('post_first_halt_trade_entries', 0) or 0) - int(control.get('post_first_halt_trade_entries', 0) or 0),
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


def write_decision(path: Path, rows: list[dict[str, Any]], control: dict[str, Any]) -> None:
    ordered = order_rows(rows)
    best = ordered[0] if ordered else control
    lines = [
        '# Top20 Drawdown Schedule Decision',
        '',
        '- branch: `trend_identity`',
        '- objective: `test non-absorbing drawdown control instead of permanent hard-halt lockout`',
        f"- control_label: `{control['label']}`",
        f"- promotion_recommendation: `{best.get('recommendation', 'research_only')}`",
        f"- control_return_ratio: `{float(control.get('net_return_ratio', 0.0) or 0.0):.4f}`",
        f"- control_max_drawdown: `{float(control.get('max_drawdown', 0.0) or 0.0):.4f}`",
        f"- control_lockout_halt_days: `{int(control.get('lockout_halt_days', 0) or 0)}`",
        '',
        '## Profile Snapshot',
        '',
        '| label | halt mode | schedule | risk/trade | return | max dd | halts | lockout | streak | recommendation |',
        '| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |',
    ]
    for row in ordered:
        lines.append(
            f"| {row['label']} | {row['drawdown_halt_mode']} | {row['drawdown_risk_schedule']} | {float(row.get('risk_per_trade_ratio', 0.0)):.3f} | {float(row.get('net_return_ratio', 0.0) or 0.0)*100:.2f}% | {float(row.get('max_drawdown', 0.0) or 0.0)*100:.2f}% | {int(row.get('portfolio_halt_count', 0) or 0)} | {int(row.get('lockout_halt_days', 0) or 0)} | {int(row.get('max_consecutive_halt_days', 0) or 0)} | {row.get('recommendation', '')} |"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def main() -> int:
    args = parse_args()
    base_config_path = (REPO_ROOT / args.base_config).resolve()
    output_root = (REPO_ROOT / args.output_root).resolve()
    configs_dir = output_root / 'configs'
    base_payload = load_yaml(base_config_path)
    profiles = resolve_profiles(args.profile)
    rows: list[dict[str, Any]] = []

    for index, profile in enumerate(profiles, start=1):
        run_dir = output_root / profile.label
        config_path = configs_dir / f'{profile.label}.yaml'
        payload = build_run_payload(base_payload, profile, run_dir)
        write_yaml(config_path, payload)
        config = load_config(config_path)
        print(
            f"[{index}/{len(profiles)}] {profile.label}: "
            f"mode={config.portfolio.drawdown_halt_mode}, "
            f"risk={config.portfolio.risk_per_trade_ratio:.3f}, "
            f"schedule={_format_schedule(profile)}"
        )
        if args.force or not reports_exist(run_dir):
            run_backtest(REPO_ROOT, args.python_exe, config_path, config.runtime.run_id)
        else:
            print(f'  skipping existing raw reports at {run_dir.as_posix()}')
        row = collect_summary(profile, config, run_dir)
        rows.append(row)
        write_rows_csv(run_dir / 'summary_research.csv', SUMMARY_COLUMNS, [row])
        if not args.keep_configs and config_path.exists():
            config_path.unlink()

    if not args.keep_configs and configs_dir.exists() and not any(configs_dir.iterdir()):
        configs_dir.rmdir()

    control = next(row for row in rows if row['label'] == CONTROL_PROFILE_LABEL)
    for row in rows:
        row['recommendation'] = recommend(row, control)

    ordered = order_rows(rows)
    write_rows_csv(output_root / 'summary_research.csv', SUMMARY_COLUMNS, ordered)
    write_rows_csv(output_root / 'control_comparison.csv', COMPARISON_COLUMNS, build_comparison_rows(ordered, control))
    write_decision(output_root / 'decision.md', ordered, control)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
