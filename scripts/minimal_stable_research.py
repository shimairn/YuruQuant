from __future__ import annotations

import argparse
import csv
import subprocess
import sys
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from yuruquant.app.config_loader import load_config
from yuruquant.app.config_schema import AppConfig
from yuruquant.reporting.diagnostics import build_trade_diagnostics, write_trade_diagnostics_csv
from yuruquant.reporting.summary import summarize_backtest_run
from yuruquant.reporting.trade_records import TradeRecord, build_trade_records


DEFAULT_PYTHON_EXE = r'C:\Users\wuktt\miniconda3\envs\minner\python.exe'
ENTRY_BLOCK_MAJOR_GAP_BARS = 1
ARMED_FLUSH_BUFFER_BARS = 1
ARMED_FLUSH_MIN_GAP_MINUTES = 180
PROTECTED_ACTIVATE_R = 1.8
ASCENDED_ACTIVATE_R = 2.0
GATE_MAX_DRAWDOWN = 0.12
GATE_MIN_RETURN = -0.12
NEIGHBOR_MAX_DRAWDOWN = 0.13
NEIGHBOR_MIN_RETURN = -0.14


@dataclass(frozen=True)
class RiskProfile:
    label: str
    risk_per_trade_ratio: float
    max_total_armed_risk_ratio: float
    is_control: bool = False


CONTROL_PROFILE = RiskProfile('control_baseline', 0.0, 0.0, is_control=True)
CANDIDATE_PROFILES = (
    RiskProfile('r10_cap20', 0.010, 0.020),
    RiskProfile('r12_cap24', 0.012, 0.024),
    RiskProfile('r15_cap30', 0.015, 0.030),
)
SUMMARY_COLUMNS = [
    'label',
    'profile_kind',
    'rank',
    'risk_per_trade_ratio',
    'max_total_armed_risk_ratio',
    'entry_block_major_gap_bars',
    'armed_flush_buffer_bars',
    'armed_flush_min_gap_minutes',
    'protected_activate_r',
    'ascended_activate_r',
    'trades',
    'wins',
    'losses',
    'win_rate',
    'hard_stop_count',
    'protected_stop_count',
    'hourly_ma_stop_count',
    'ascended_exit_count',
    'portfolio_halt_count',
    'hard_stop_overshoot_avg',
    'protected_stop_overshoot_avg',
    'return_ratio',
    'max_drawdown',
    'end_equity',
    'ascended_negative_count',
    'gate_passed',
    'gate_status',
    'output_dir',
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run the minimal stable Top10 research pack for Dual-Core Trend Breakout.')
    parser.add_argument('--base-config', default='config/liquid_top10_dual_core.yaml')
    parser.add_argument('--python-exe', default=DEFAULT_PYTHON_EXE)
    parser.add_argument('--output-root', default='reports/minimal_stable_top10_v1')
    parser.add_argument('--force', action='store_true', help='Re-run profiles even if raw reports already exist.')
    parser.add_argument('--keep-configs', action='store_true', help='Keep generated per-run config files.')
    parser.add_argument('--skip-control', action='store_true', help='Skip the unchanged baseline control run.')
    return parser.parse_args()


def load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding='utf-8')) or {}


def write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=False), encoding='utf-8')


def reports_exist(output_dir: Path) -> bool:
    return all((output_dir / name).exists() for name in ('signals.csv', 'executions.csv', 'portfolio_daily.csv'))


def build_multiplier_lookup(config: AppConfig) -> dict[str, float]:
    multipliers = {csymbol: config.universe.instrument_defaults.multiplier for csymbol in config.universe.symbols}
    multipliers.update({csymbol: spec.multiplier for csymbol, spec in config.universe.instrument_overrides.items()})
    return multipliers


def run_backtest(python_exe: str, config_path: Path, run_id: str) -> None:
    command = [python_exe, str(REPO_ROOT / 'main.py'), '--mode', 'BACKTEST', '--config', str(config_path), '--run-id', run_id]
    subprocess.run(command, cwd=str(REPO_ROOT), check=True)


def apply_candidate_overrides(payload: dict[str, Any], profile: RiskProfile) -> None:
    payload.setdefault('strategy', {})
    payload['strategy'].setdefault('entry', {})
    payload['strategy'].setdefault('exit', {})
    payload.setdefault('portfolio', {})
    payload['strategy']['entry']['entry_block_major_gap_bars'] = ENTRY_BLOCK_MAJOR_GAP_BARS
    payload['strategy']['exit']['armed_flush_buffer_bars'] = ARMED_FLUSH_BUFFER_BARS
    payload['strategy']['exit']['armed_flush_min_gap_minutes'] = ARMED_FLUSH_MIN_GAP_MINUTES
    payload['strategy']['exit']['protected_activate_r'] = PROTECTED_ACTIVATE_R
    payload['strategy']['exit']['ascended_activate_r'] = ASCENDED_ACTIVATE_R
    payload['portfolio']['risk_per_trade_ratio'] = float(profile.risk_per_trade_ratio)
    payload['portfolio']['max_total_armed_risk_ratio'] = float(profile.max_total_armed_risk_ratio)


def build_run_payload(base_payload: dict[str, Any], profile: RiskProfile, output_dir: Path) -> dict[str, Any]:
    payload = deepcopy(base_payload)
    payload.setdefault('runtime', {})
    payload['runtime']['run_id'] = f'minimal_{profile.label}'
    if not profile.is_control:
        apply_candidate_overrides(payload, profile)
    payload.setdefault('reporting', {})
    payload['reporting']['output_dir'] = output_dir.relative_to(REPO_ROOT).as_posix()
    return payload


def ascended_negative_count(trades: list[TradeRecord]) -> int:
    return sum(1 for trade in trades if trade.phase_at_exit == 'ascended' and float(trade.gross_pnl) < 0.0)


def gate_status(summary: dict[str, Any], trades: list[TradeRecord]) -> tuple[bool, str]:
    failures: list[str] = []
    if int(summary.get('trades', 0) or 0) < 8:
        failures.append('trades<8')
    if float(summary.get('max_drawdown', 0.0) or 0.0) > GATE_MAX_DRAWDOWN:
        failures.append('max_drawdown>12%')
    if float(summary.get('return_ratio', 0.0) or 0.0) < GATE_MIN_RETURN:
        failures.append('return<-12%')
    if int(summary.get('portfolio_halt_count', 0) or 0) != 0:
        failures.append('portfolio_halt!=0')
    if ascended_negative_count(trades) != 0:
        failures.append('ascended_negative_count!=0')
    return (not failures), ('ok' if not failures else ';'.join(failures))


def collect_summary(profile: RiskProfile, config: AppConfig, output_dir: Path, multiplier_lookup: dict[str, float]) -> dict[str, Any]:
    signals_path = output_dir / 'signals.csv'
    portfolio_daily_path = output_dir / 'portfolio_daily.csv'
    executions_path = output_dir / 'executions.csv'
    trades = build_trade_records(signals_path, multiplier_lookup, executions_path)
    write_trade_diagnostics_csv(output_dir / 'trade_diagnostics.csv', build_trade_diagnostics(trades))
    summary = summarize_backtest_run(
        signals_path=signals_path,
        portfolio_daily_path=portfolio_daily_path,
        multiplier_by_csymbol=multiplier_lookup,
        executions_path=executions_path,
    )
    passed_gate, status = gate_status(summary, trades)
    summary['label'] = profile.label
    summary['profile_kind'] = 'control' if profile.is_control else 'candidate'
    summary['rank'] = ''
    summary['risk_per_trade_ratio'] = config.portfolio.risk_per_trade_ratio
    summary['max_total_armed_risk_ratio'] = config.portfolio.max_total_armed_risk_ratio
    summary['entry_block_major_gap_bars'] = config.strategy.entry.entry_block_major_gap_bars
    summary['armed_flush_buffer_bars'] = config.strategy.exit.armed_flush_buffer_bars
    summary['armed_flush_min_gap_minutes'] = config.strategy.exit.armed_flush_min_gap_minutes
    summary['protected_activate_r'] = config.strategy.exit.protected_activate_r
    summary['ascended_activate_r'] = config.strategy.exit.ascended_activate_r
    summary['ascended_negative_count'] = ascended_negative_count(trades)
    summary['gate_passed'] = int(passed_gate)
    summary['gate_status'] = status
    summary['output_dir'] = output_dir.relative_to(REPO_ROOT).as_posix()
    return summary


def rank_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = sorted(
        rows,
        key=lambda row: (
            float(row.get('max_drawdown', 0.0) or 0.0),
            -float(row.get('return_ratio', 0.0) or 0.0),
            int(row.get('portfolio_halt_count', 0) or 0),
        ),
    )
    for index, row in enumerate(ranked, start=1):
        row['rank'] = index
    return ranked


def ordered_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    controls = [row for row in rows if row.get('profile_kind') == 'control']
    candidates = rank_candidates([row for row in rows if row.get('profile_kind') == 'candidate'])
    return controls + candidates


def write_summary_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        for row in ordered_rows(rows):
            writer.writerow({column: row.get(column, '') for column in SUMMARY_COLUMNS})


def neighborhood_status(best: dict[str, Any] | None, candidates: list[dict[str, Any]]) -> tuple[bool, str]:
    if best is None:
        return False, 'no_candidate'
    neighbors = [row for row in candidates if row['label'] != best['label']]
    failing = [
        row['label']
        for row in neighbors
        if float(row.get('max_drawdown', 0.0) or 0.0) > NEIGHBOR_MAX_DRAWDOWN
        or float(row.get('return_ratio', 0.0) or 0.0) < NEIGHBOR_MIN_RETURN
    ]
    return (not failing), ('ok' if not failing else ','.join(failing))


def write_decision_report(path: Path, base_config_path: Path, rows: list[dict[str, Any]]) -> None:
    ordered = ordered_rows(rows)
    controls = [row for row in ordered if row.get('profile_kind') == 'control']
    candidates = [row for row in ordered if row.get('profile_kind') == 'candidate']
    best = candidates[0] if candidates else None
    neighborhood_ok, neighborhood_detail = neighborhood_status(best, candidates)
    should_promote = bool(best and int(best.get('gate_passed', 0) or 0) == 1 and neighborhood_ok)

    lines = [
        '# Minimal Stable Research Decision',
        '',
        f'- base_config: `{base_config_path.relative_to(REPO_ROOT).as_posix()}`',
        f'- control_included: `{1 if controls else 0}`',
        f'- promotion_decision: `{"promote" if should_promote else "hold_baseline"}`',
        f'- neighborhood_status: `{neighborhood_detail}`',
        '',
        '## Candidates',
        '',
        '| label | rank | risk | armed cap | return | max dd | halts | asc<0 | gate |',
        '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |',
    ]
    for row in candidates:
        lines.append(
            f"| {row['label']} | {row['rank']} | {float(row['risk_per_trade_ratio']):.3f} | {float(row['max_total_armed_risk_ratio']):.3f} | {float(row['return_ratio']) * 100:.2f}% | {float(row['max_drawdown']) * 100:.2f}% | {int(row['portfolio_halt_count'])} | {int(row['ascended_negative_count'])} | {row['gate_status']} |"
        )
    if controls:
        control = controls[0]
        lines.extend(
            [
                '',
                '## Control',
                '',
                f"- `{control['label']}`: return `{float(control['return_ratio']) * 100:.2f}%`, max_drawdown `{float(control['max_drawdown']) * 100:.2f}%`, halts `{int(control['portfolio_halt_count'])}`",
            ]
        )
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def print_summary(rows: list[dict[str, Any]]) -> None:
    print('label            kind     rank risk   cap    gap entry flush return% max_dd% halts asc<0 gate')
    for row in ordered_rows(rows):
        print(
            f"{row['label']:16s} "
            f"{row['profile_kind']:8s} "
            f"{str(row['rank'] or '-'):>4s} "
            f"{float(row['risk_per_trade_ratio']):>5.3f} "
            f"{float(row['max_total_armed_risk_ratio']):>6.3f} "
            f"{int(row['entry_block_major_gap_bars']):>3d} "
            f"{int(row['armed_flush_buffer_bars']):>5d} "
            f"{float(row['return_ratio']) * 100:>7.2f} "
            f"{float(row['max_drawdown']) * 100:>7.2f} "
            f"{int(row['portfolio_halt_count']):>5d} "
            f"{int(row['ascended_negative_count']):>5d} "
            f"{row['gate_status']}"
        )


def main() -> int:
    args = parse_args()
    base_config_path = (REPO_ROOT / args.base_config).resolve()
    output_root = (REPO_ROOT / args.output_root).resolve()
    configs_dir = output_root / 'configs'
    summary_path = output_root / 'summary.csv'
    decision_path = output_root / 'decision.md'
    base_payload = load_yaml(base_config_path)

    profiles = list(CANDIDATE_PROFILES)
    if not args.skip_control:
        profiles.insert(0, CONTROL_PROFILE)

    rows: list[dict[str, Any]] = []
    total = len(profiles)
    for index, profile in enumerate(profiles, start=1):
        output_dir = output_root / profile.label
        config_path = configs_dir / f'{profile.label}.yaml'
        payload = build_run_payload(base_payload, profile, output_dir)
        write_yaml(config_path, payload)
        config = load_config(config_path)
        multiplier_lookup = build_multiplier_lookup(config)
        run_id = str(payload.get('runtime', {}).get('run_id') or profile.label)

        print(f'[{index}/{total}] {profile.label}: risk={config.portfolio.risk_per_trade_ratio:.3f}, cap={config.portfolio.max_total_armed_risk_ratio:.3f}')
        if args.force or not reports_exist(output_dir):
            run_backtest(args.python_exe, config_path, run_id)
        else:
            print(f'  skipping existing raw reports at {output_dir.as_posix()}')

        rows.append(collect_summary(profile, config, output_dir, multiplier_lookup))
        write_summary_csv(summary_path, rows)

        if not args.keep_configs and config_path.exists():
            config_path.unlink()

    if not args.keep_configs and configs_dir.exists() and not any(configs_dir.iterdir()):
        configs_dir.rmdir()

    write_decision_report(decision_path, base_config_path, rows)
    print(f'\nsummary saved to {summary_path.as_posix()}')
    print(f'decision saved to {decision_path.as_posix()}\n')
    print_summary(rows)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
