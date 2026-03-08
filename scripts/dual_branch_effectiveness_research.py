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
from yuruquant.reporting.cost_overlay import (
    COSTED_TRADE_DIAGNOSTIC_COLUMNS,
    PORTFOLIO_DAILY_COSTED_COLUMNS,
    SUMMARY_COSTED_COLUMNS,
    SYMBOL_COST_DRAG_COLUMNS,
    apply_cost_overlay,
    load_cost_profile,
    write_csv,
)
from yuruquant.reporting.summary import summarize_backtest_run
from yuruquant.reporting.trade_records import TradeRecord, build_trade_records


DEFAULT_PYTHON_EXE = r'C:\Users\wuktt\miniconda3\envs\minner\python.exe'
RAW_NEUTRAL_COMMISSION_RATIO = 0.0
RAW_NEUTRAL_SLIPPAGE_RATIO = 0.0
REALISTIC_COST_PROFILE_PATH = REPO_ROOT / 'research' / 'cost_profiles' / 'realistic_top10_v1.csv'
RAW_SUMMARY_COLUMNS = [
    'branch',
    'risk_label',
    'entry_frequency',
    'trend_frequency',
    'donchian_lookback',
    'risk_per_trade_ratio',
    'max_total_armed_risk_ratio',
    'trades',
    'wins',
    'losses',
    'win_rate',
    'hard_stop_count',
    'protected_stop_count',
    'hourly_ma_stop_count',
    'armed_flush_count',
    'session_flat_exit_count',
    'ascended_exit_count',
    'portfolio_halt_count',
    'return_ratio',
    'max_drawdown',
    'end_equity',
    'output_dir',
]
BRANCH_SUMMARY_COSTED_COLUMNS = [
    'branch',
    'risk_label',
    'entry_frequency',
    'trend_frequency',
    'donchian_lookback',
    'risk_per_trade_ratio',
    'max_total_armed_risk_ratio',
    *SUMMARY_COSTED_COLUMNS,
    'gate_passed',
    'gate_status',
    'output_dir',
]


@dataclass(frozen=True)
class RiskProfile:
    label: str
    risk_per_trade_ratio: float
    max_total_armed_risk_ratio: float


@dataclass(frozen=True)
class BranchSpec:
    label: str
    entry_frequency: str
    trend_frequency: str
    donchian_lookback: int
    entry_block_major_gap_bars: int
    armed_flush_buffer_bars: int
    armed_flush_min_gap_minutes: int
    session_flat_all_phases_buffer_bars: int
    protected_activate_r: float
    ascended_activate_r: float


TREND_BRANCH = BranchSpec(
    label='trend_identity',
    entry_frequency='900s',
    trend_frequency='3600s',
    donchian_lookback=12,
    entry_block_major_gap_bars=1,
    armed_flush_buffer_bars=1,
    armed_flush_min_gap_minutes=180,
    session_flat_all_phases_buffer_bars=0,
    protected_activate_r=1.8,
    ascended_activate_r=2.0,
)
INTRADAY_BRANCH = BranchSpec(
    label='intraday_flat',
    entry_frequency='300s',
    trend_frequency='3600s',
    donchian_lookback=36,
    entry_block_major_gap_bars=1,
    armed_flush_buffer_bars=0,
    armed_flush_min_gap_minutes=180,
    session_flat_all_phases_buffer_bars=1,
    protected_activate_r=1.8,
    ascended_activate_r=2.0,
)
BRANCHES = (TREND_BRANCH, INTRADAY_BRANCH)
RISK_PROFILES = (
    RiskProfile('r12_cap24', 0.012, 0.024),
    RiskProfile('r15_cap30', 0.015, 0.030),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run dual-branch effectiveness research with research-side cost overlays.')
    parser.add_argument('--base-config', default='config/liquid_top10_dual_core.yaml')
    parser.add_argument('--python-exe', default=DEFAULT_PYTHON_EXE)
    parser.add_argument('--output-root', default='reports/dual_branch_effectiveness_v1')
    parser.add_argument('--force', action='store_true', help='Re-run raw backtests even if raw reports already exist.')
    parser.add_argument('--keep-configs', action='store_true', help='Keep generated per-run config files.')
    return parser.parse_args()


def load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding='utf-8')) or {}


def write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=False), encoding='utf-8')


def reports_exist(output_dir: Path) -> bool:
    return all((output_dir / name).exists() for name in ('signals.csv', 'executions.csv', 'portfolio_daily.csv'))


def write_rows_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, '') for name in fieldnames})


def run_backtest(python_exe: str, config_path: Path, run_id: str) -> None:
    command = [python_exe, str(REPO_ROOT / 'main.py'), '--mode', 'BACKTEST', '--config', str(config_path), '--run-id', run_id]
    subprocess.run(command, cwd=str(REPO_ROOT), check=True)


def build_run_payload(base_payload: dict[str, Any], branch: BranchSpec, risk: RiskProfile, output_dir: Path) -> dict[str, Any]:
    payload = deepcopy(base_payload)
    payload.setdefault('runtime', {})
    payload.setdefault('universe', {})
    payload.setdefault('strategy', {})
    payload['strategy'].setdefault('entry', {})
    payload['strategy'].setdefault('exit', {})
    payload.setdefault('portfolio', {})
    payload.setdefault('execution', {})
    payload.setdefault('reporting', {})

    payload['runtime']['run_id'] = f'{branch.label}_{risk.label}'
    payload['universe']['entry_frequency'] = branch.entry_frequency
    payload['universe']['trend_frequency'] = branch.trend_frequency
    payload['strategy']['entry']['donchian_lookback'] = int(branch.donchian_lookback)
    payload['strategy']['entry']['entry_block_major_gap_bars'] = int(branch.entry_block_major_gap_bars)
    payload['strategy']['exit']['protected_activate_r'] = float(branch.protected_activate_r)
    payload['strategy']['exit']['ascended_activate_r'] = float(branch.ascended_activate_r)
    payload['strategy']['exit']['armed_flush_buffer_bars'] = int(branch.armed_flush_buffer_bars)
    payload['strategy']['exit']['armed_flush_min_gap_minutes'] = int(branch.armed_flush_min_gap_minutes)
    payload['strategy']['exit']['session_flat_all_phases_buffer_bars'] = int(branch.session_flat_all_phases_buffer_bars)
    payload['portfolio']['risk_per_trade_ratio'] = float(risk.risk_per_trade_ratio)
    payload['portfolio']['max_total_armed_risk_ratio'] = float(risk.max_total_armed_risk_ratio)
    payload['execution']['backtest_commission_ratio'] = RAW_NEUTRAL_COMMISSION_RATIO
    payload['execution']['backtest_slippage_ratio'] = RAW_NEUTRAL_SLIPPAGE_RATIO
    payload['reporting']['output_dir'] = output_dir.as_posix()
    return payload


def build_multiplier_lookup(config: AppConfig) -> dict[str, float]:
    multipliers = {csymbol: config.universe.instrument_defaults.multiplier for csymbol in config.universe.symbols}
    multipliers.update({csymbol: spec.multiplier for csymbol, spec in config.universe.instrument_overrides.items()})
    return multipliers


def collect_raw_summary(branch: BranchSpec, risk: RiskProfile, config: AppConfig, output_dir: Path) -> tuple[dict[str, Any], list[TradeRecord]]:
    signals_path = output_dir / 'signals.csv'
    executions_path = output_dir / 'executions.csv'
    portfolio_daily_path = output_dir / 'portfolio_daily.csv'
    trades = build_trade_records(signals_path, build_multiplier_lookup(config), executions_path)
    summary = summarize_backtest_run(signals_path, portfolio_daily_path, build_multiplier_lookup(config), executions_path)
    summary.update(
        {
            'branch': branch.label,
            'risk_label': risk.label,
            'entry_frequency': config.universe.entry_frequency,
            'trend_frequency': config.universe.trend_frequency,
            'donchian_lookback': config.strategy.entry.donchian_lookback,
            'risk_per_trade_ratio': config.portfolio.risk_per_trade_ratio,
            'max_total_armed_risk_ratio': config.portfolio.max_total_armed_risk_ratio,
            'session_flat_exit_count': sum(1 for trade in trades if trade.exit_trigger == 'session_flat'),
            'output_dir': output_dir.as_posix(),
        }
    )
    return summary, trades


def gate_status(branch: BranchSpec, summary: dict[str, Any]) -> tuple[bool, str]:
    failures: list[str] = []
    if float(summary.get('net_return_ratio', 0.0) or 0.0) <= 0.0:
        failures.append('net_return<=0')
    halt_count = int(summary.get('portfolio_halt_count_costed', 0) or 0)
    if halt_count != 0:
        failures.append('halt_count!=0')
    if branch.label == TREND_BRANCH.label:
        if float(summary.get('max_drawdown', 0.0) or 0.0) > 0.12:
            failures.append('max_drawdown>0.12')
        if int(summary.get('ascended_exit_count', 0) or 0) < 2:
            failures.append('ascended_exit_count<2')
        if int(summary.get('multi_session_hold_count', 0) or 0) < 2:
            failures.append('multi_session_hold_count<2')
    else:
        if float(summary.get('max_drawdown', 0.0) or 0.0) > 0.10:
            failures.append('max_drawdown>0.10')
        if int(summary.get('multi_session_hold_count', 0) or 0) != 0:
            failures.append('multi_session_hold_count!=0')
        if int(summary.get('session_flat_exit_count', 0) or 0) < 1:
            failures.append('session_flat_exit_count<1')
    return (not failures), ('ok' if not failures else ','.join(failures))


def order_costed_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            row.get('branch', ''),
            row.get('cost_profile', ''),
            -int(bool(row.get('gate_passed', 0))),
            float(row.get('max_drawdown', 1.0) or 1.0),
            -float(row.get('net_return_ratio', -1.0) or -1.0),
            row.get('risk_label', ''),
        ),
    )


def write_run_decision(path: Path, branch: BranchSpec, risk: RiskProfile, costed_rows: list[dict[str, Any]]) -> None:
    realistic_row = next((row for row in costed_rows if row.get('cost_profile') == 'realistic_top10_v1'), None)
    lines = [
        '# Dual Branch Run Decision',
        '',
        f'- branch: `{branch.label}`',
        f'- risk_label: `{risk.label}`',
    ]
    if realistic_row is not None:
        lines.extend(
            [
                f"- realistic_gate: `{'pass' if int(realistic_row.get('gate_passed', 0) or 0) == 1 else 'fail'}`",
                f"- realistic_gate_status: `{realistic_row.get('gate_status', 'n/a')}`",
                f"- realistic_net_return_ratio: `{float(realistic_row.get('net_return_ratio', 0.0)):.4f}`",
                f"- realistic_max_drawdown: `{float(realistic_row.get('max_drawdown', 0.0)):.4f}`",
                f"- realistic_portfolio_halt_count_costed: `{int(realistic_row.get('portfolio_halt_count_costed', 0) or 0)}`",
            ]
        )
    lines.extend(
        [
            '',
            '## Cost Profiles',
            '',
            '| cost_profile | gate | net_return | max_drawdown | halts | multi_session | session_flat | ascended |',
            '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |',
        ]
    )
    for row in costed_rows:
        lines.append(
            f"| {row['cost_profile']} | {row.get('gate_status', 'n/a')} | {float(row.get('net_return_ratio', 0.0)):.4f} | {float(row.get('max_drawdown', 0.0)):.4f} | {int(row.get('portfolio_halt_count_costed', 0) or 0)} | {int(row.get('multi_session_hold_count', 0) or 0)} | {int(row.get('session_flat_exit_count', 0) or 0)} | {int(row.get('ascended_exit_count', 0) or 0)} |"
        )
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def write_branch_decision(path: Path, branch: BranchSpec, costed_rows: list[dict[str, Any]]) -> None:
    realistic_rows = [row for row in costed_rows if row.get('cost_profile') == 'realistic_top10_v1']
    ordered = sorted(realistic_rows, key=lambda row: (-int(bool(row.get('gate_passed', 0))), -float(row.get('net_return_ratio', -1.0) or -1.0), float(row.get('max_drawdown', 1.0) or 1.0), row.get('risk_label', '')))
    best = ordered[0] if ordered else None
    lines = [
        '# Branch Decision',
        '',
        f'- branch: `{branch.label}`',
        f"- research_status: `{'candidate_found' if best and int(best.get('gate_passed', 0) or 0) == 1 else 'not_proven'}`",
    ]
    if best is not None:
        lines.extend(
            [
                f"- best_risk_label: `{best['risk_label']}`",
                f"- best_gate_status: `{best['gate_status']}`",
                f"- best_net_return_ratio: `{float(best.get('net_return_ratio', 0.0)):.4f}`",
                f"- best_max_drawdown: `{float(best.get('max_drawdown', 0.0)):.4f}`",
            ]
        )
    lines.extend(
        [
            '',
            '## Realistic Cost Results',
            '',
            '| risk_label | gate | net_return | max_drawdown | halts | multi_session | session_flat | ascended |',
            '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |',
        ]
    )
    for row in ordered:
        lines.append(
            f"| {row['risk_label']} | {row['gate_status']} | {float(row.get('net_return_ratio', 0.0)):.4f} | {float(row.get('max_drawdown', 0.0)):.4f} | {int(row.get('portfolio_halt_count_costed', 0) or 0)} | {int(row.get('multi_session_hold_count', 0) or 0)} | {int(row.get('session_flat_exit_count', 0) or 0)} | {int(row.get('ascended_exit_count', 0) or 0)} |"
        )
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def write_root_decision(path: Path, costed_rows: list[dict[str, Any]]) -> None:
    lines = [
        '# Dual Branch Effectiveness Decision',
        '',
        '- default_runtime_change: `none`',
        '- strategy_upgrade: `deferred`',
        '- note: `both branches stay as research branches until a later explicit promotion decision`',
        '',
        '## Branch Snapshot',
        '',
        '| branch | risk_label | gate | net_return | max_drawdown | halts | cost_profile |',
        '| --- | --- | --- | ---: | ---: | ---: | --- |',
    ]
    realistic_rows = [row for row in costed_rows if row.get('cost_profile') == 'realistic_top10_v1']
    for branch in BRANCHES:
        branch_rows = [row for row in realistic_rows if row.get('branch') == branch.label]
        branch_rows.sort(key=lambda row: (-int(bool(row.get('gate_passed', 0))), -float(row.get('net_return_ratio', -1.0) or -1.0), float(row.get('max_drawdown', 1.0) or 1.0), row.get('risk_label', '')))
        if branch_rows:
            row = branch_rows[0]
            lines.append(f"| {row['branch']} | {row['risk_label']} | {row['gate_status']} | {float(row.get('net_return_ratio', 0.0)):.4f} | {float(row.get('max_drawdown', 0.0)):.4f} | {int(row.get('portfolio_halt_count_costed', 0) or 0)} | {row['cost_profile']} |")
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def print_summary(raw_rows: list[dict[str, Any]], costed_rows: list[dict[str, Any]]) -> None:
    print('raw summary')
    for row in raw_rows:
        print(f"  {row['branch']:14s} {row['risk_label']:10s} return={float(row.get('return_ratio', 0.0))*100:7.2f}% max_dd={float(row.get('max_drawdown', 0.0))*100:6.2f}% trades={int(row.get('trades', 0) or 0):2d}")
    print('costed summary (realistic_top10_v1)')
    for row in order_costed_rows([item for item in costed_rows if item.get('cost_profile') == 'realistic_top10_v1']):
        print(f"  {row['branch']:14s} {row['risk_label']:10s} gate={row['gate_status']:24s} net={float(row.get('net_return_ratio', 0.0))*100:7.2f}% max_dd={float(row.get('max_drawdown', 0.0))*100:6.2f}%")


def main() -> int:
    args = parse_args()
    base_config_path = (REPO_ROOT / args.base_config).resolve()
    output_root = (REPO_ROOT / args.output_root).resolve()
    configs_dir = output_root / 'configs'
    base_payload = load_yaml(base_config_path)
    reference_config = load_config(base_config_path)
    realistic_costs = load_cost_profile(REALISTIC_COST_PROFILE_PATH)

    all_raw_rows: list[dict[str, Any]] = []
    all_costed_rows: list[dict[str, Any]] = []

    for branch in BRANCHES:
        branch_root = output_root / branch.label
        branch_raw_rows: list[dict[str, Any]] = []
        branch_costed_rows: list[dict[str, Any]] = []

        for risk in RISK_PROFILES:
            run_dir = branch_root / risk.label
            config_path = configs_dir / branch.label / f'{risk.label}.yaml'
            payload = build_run_payload(base_payload, branch, risk, run_dir)
            write_yaml(config_path, payload)
            config = load_config(config_path)
            run_id = config.runtime.run_id

            print(f'[{branch.label}/{risk.label}] entry={config.universe.entry_frequency}, risk={config.portfolio.risk_per_trade_ratio:.3f}, cap={config.portfolio.max_total_armed_risk_ratio:.3f}')
            if args.force or not reports_exist(run_dir):
                run_backtest(args.python_exe, config_path, run_id)
            else:
                print(f'  skipping existing raw reports at {run_dir.as_posix()}')

            raw_summary, trades = collect_raw_summary(branch, risk, config, run_dir)
            branch_raw_rows.append(raw_summary)
            all_raw_rows.append(raw_summary)
            write_rows_csv(run_dir / 'summary_raw.csv', RAW_SUMMARY_COLUMNS, [raw_summary])

            costed_rows: list[dict[str, Any]] = []
            trade_diagnostics_rows: list[dict[str, Any]] = []
            portfolio_daily_rows: list[dict[str, Any]] = []
            symbol_cost_rows: list[dict[str, Any]] = []
            cost_profiles = [
                ('current_high_cost', apply_cost_overlay(trades, run_dir / 'portfolio_daily.csv', config, 'current_high_cost', current_high_cost=(reference_config.execution.backtest_commission_ratio, reference_config.execution.backtest_slippage_ratio))),
                ('realistic_top10_v1', apply_cost_overlay(trades, run_dir / 'portfolio_daily.csv', config, 'realistic_top10_v1', profile_rows=realistic_costs)),
            ]
            for cost_profile, overlay in cost_profiles:
                summary_row = {
                    'branch': branch.label,
                    'risk_label': risk.label,
                    'entry_frequency': config.universe.entry_frequency,
                    'trend_frequency': config.universe.trend_frequency,
                    'donchian_lookback': config.strategy.entry.donchian_lookback,
                    'risk_per_trade_ratio': config.portfolio.risk_per_trade_ratio,
                    'max_total_armed_risk_ratio': config.portfolio.max_total_armed_risk_ratio,
                    **overlay.summary,
                    'output_dir': run_dir.as_posix(),
                }
                passed, status = gate_status(branch, summary_row) if cost_profile == 'realistic_top10_v1' else (False, 'reference_only')
                summary_row['gate_passed'] = int(passed)
                summary_row['gate_status'] = status
                costed_rows.append(summary_row)
                branch_costed_rows.append(summary_row)
                all_costed_rows.append(summary_row)
                trade_diagnostics_rows.extend(overlay.trade_diagnostics)
                portfolio_daily_rows.extend(overlay.portfolio_daily)
                symbol_cost_rows.extend(overlay.symbol_cost_drag)

            write_rows_csv(run_dir / 'summary_costed.csv', BRANCH_SUMMARY_COSTED_COLUMNS, costed_rows)
            write_csv(run_dir / 'trade_diagnostics_costed.csv', COSTED_TRADE_DIAGNOSTIC_COLUMNS, trade_diagnostics_rows)
            write_csv(run_dir / 'portfolio_daily_costed.csv', PORTFOLIO_DAILY_COSTED_COLUMNS, portfolio_daily_rows)
            write_csv(run_dir / 'symbol_cost_drag.csv', SYMBOL_COST_DRAG_COLUMNS, symbol_cost_rows)
            write_run_decision(run_dir / 'decision.md', branch, risk, costed_rows)

            if not args.keep_configs and config_path.exists():
                config_path.unlink()

        write_rows_csv(branch_root / 'summary_raw.csv', RAW_SUMMARY_COLUMNS, branch_raw_rows)
        write_rows_csv(branch_root / 'summary_costed.csv', BRANCH_SUMMARY_COSTED_COLUMNS, order_costed_rows(branch_costed_rows))
        write_branch_decision(branch_root / 'decision.md', branch, branch_costed_rows)

    if not args.keep_configs:
        for directory in sorted(configs_dir.glob('*'), reverse=True):
            if directory.is_dir() and not any(directory.iterdir()):
                directory.rmdir()
        if configs_dir.exists() and not any(configs_dir.rglob('*.yaml')):
            for directory in sorted(configs_dir.rglob('*'), reverse=True):
                if directory.is_dir() and not any(directory.iterdir()):
                    directory.rmdir()
            if configs_dir.exists() and not any(configs_dir.iterdir()):
                configs_dir.rmdir()

    write_rows_csv(output_root / 'summary_raw.csv', RAW_SUMMARY_COLUMNS, all_raw_rows)
    write_rows_csv(output_root / 'summary_costed.csv', BRANCH_SUMMARY_COSTED_COLUMNS, order_costed_rows(all_costed_rows))
    write_root_decision(output_root / 'decision.md', all_costed_rows)
    print_summary(all_raw_rows, all_costed_rows)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
