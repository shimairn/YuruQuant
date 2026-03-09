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
    'branch', 'risk_label', 'entry_frequency', 'trend_frequency', 'donchian_lookback',
    'risk_per_trade_ratio', 'max_total_armed_risk_ratio', 'trades', 'wins', 'losses',
    'win_rate', 'hard_stop_count', 'protected_stop_count', 'armed_flush_count',
    'session_flat_exit_count', 'portfolio_halt_count', 'return_ratio', 'max_drawdown',
    'end_equity', 'output_dir',
]
BRANCH_SUMMARY_COSTED_COLUMNS = [
    'branch', 'risk_label', 'entry_frequency', 'trend_frequency', 'donchian_lookback',
    'risk_per_trade_ratio', 'max_total_armed_risk_ratio', *SUMMARY_COSTED_COLUMNS,
    'gate_passed', 'gate_status', 'stability_status', 'output_dir',
]
COST_ASSUMPTION_COLUMNS = [
    'model_layer',
    'cost_profile',
    'scope',
    'csymbol',
    'profile_path',
    'commission_ratio_applied',
    'slippage_ratio_applied',
    'slippage_ticks_per_side',
    'source_url',
    'as_of_date',
    'notes',
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
    session_flat_scope: str
    protected_activate_r: float


TREND_BRANCH = BranchSpec(
    label='trend_identity',
    entry_frequency='900s',
    trend_frequency='3600s',
    donchian_lookback=12,
    entry_block_major_gap_bars=1,
    armed_flush_buffer_bars=1,
    armed_flush_min_gap_minutes=180,
    session_flat_all_phases_buffer_bars=0,
    session_flat_scope='disabled',
    protected_activate_r=1.8,
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
    session_flat_scope='trading_day_end_only',
    protected_activate_r=1.8,
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
    parser.add_argument('--output-root', default='reports/dual_branch_effectiveness_v3')
    parser.add_argument('--branch', choices=[branch.label for branch in BRANCHES])
    parser.add_argument('--risk-label', choices=[risk.label for risk in RISK_PROFILES])
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--keep-configs', action='store_true')
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


def _relative_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT).as_posix()
    except Exception:
        return path.as_posix()


def build_cost_assumption_rows(reference_config: AppConfig, profile_path: Path, realistic_costs: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [
        {
            'model_layer': 'raw_run',
            'cost_profile': 'raw_fill_capture',
            'scope': 'ALL',
            'csymbol': '*',
            'profile_path': '',
            'commission_ratio_applied': RAW_NEUTRAL_COMMISSION_RATIO,
            'slippage_ratio_applied': RAW_NEUTRAL_SLIPPAGE_RATIO,
            'slippage_ticks_per_side': '',
            'source_url': '',
            'as_of_date': '',
            'notes': 'Raw backtest runs use 0 commission and 0 slippage to capture fills; acceptance is based on summary_costed.csv, not summary_raw.csv.',
        },
        {
            'model_layer': 'research_overlay',
            'cost_profile': 'current_high_cost',
            'scope': 'ALL',
            'csymbol': '*',
            'profile_path': '',
            'commission_ratio_applied': float(reference_config.execution.backtest_commission_ratio),
            'slippage_ratio_applied': float(reference_config.execution.backtest_slippage_ratio),
            'slippage_ticks_per_side': '',
            'source_url': '',
            'as_of_date': '',
            'notes': 'Reference stress overlay from base config execution.backtest_commission_ratio and execution.backtest_slippage_ratio, both applied to round-trip turnover.',
        },
    ]
    for csymbol, row in sorted(realistic_costs.items()):
        rows.append(
            {
                'model_layer': 'research_overlay',
                'cost_profile': 'realistic_top10_v1',
                'scope': 'PER_SYMBOL',
                'csymbol': csymbol,
                'profile_path': _relative_path(profile_path),
                'commission_ratio_applied': float(getattr(row, 'commission_ratio_per_side', 0.0) or 0.0),
                'slippage_ratio_applied': '',
                'slippage_ticks_per_side': float(getattr(row, 'slippage_ticks_per_side', 0.0) or 0.0),
                'source_url': getattr(row, 'source_url', ''),
                'as_of_date': getattr(row, 'as_of_date', ''),
                'notes': getattr(row, 'notes', ''),
            }
        )
    return rows


def write_cost_model(path: Path, reference_config: AppConfig, profile_path: Path, realistic_costs: dict[str, Any]) -> None:
    lines = [
        '# Cost Model',
        '',
        '- `summary_raw.csv` reflects fill capture only; raw runs intentionally use zero commission and zero slippage.',
        '- `summary_costed.csv` is the acceptance surface; all net-return, drawdown, and halt conclusions come from research-side overlays.',
        f"- `current_high_cost` uses round-trip turnover ratios from the base config: commission `{float(reference_config.execution.backtest_commission_ratio):.6f}`, slippage `{float(reference_config.execution.backtest_slippage_ratio):.6f}`.",
        f"- `realistic_top10_v1` loads per-symbol fees from `{_relative_path(profile_path)}` for `{len(realistic_costs)}` symbols.",
        '- `realistic_top10_v1` commission is modeled as per-side ratio on each leg; slippage is modeled as ticks per side times min tick times multiplier times quantity times 2.',
        '- Read `cost_assumptions.csv` for the exact applied values and source links.',
        '- `summary_costed.csv` exposes total turnover, gross PnL, commission, slippage, total cost, net PnL, and cost ratios at the run level.',
        '- `portfolio_daily_costed.csv` exposes daily and cumulative commission, slippage, total cost, turnover, gross PnL, and net PnL.',
        '- `trade_diagnostics_costed.csv` and `symbol_cost_drag.csv` expose per-trade and per-symbol turnover plus explicit cost attribution.',
        '',
        '## Decision Rule',
        '',
        '- Ignore `summary_raw.csv` for promotion decisions.',
        '- Use `summary_costed.csv`, `portfolio_daily_costed.csv`, `trade_diagnostics_costed.csv`, and `symbol_cost_drag.csv` as the decision set.',
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


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
    payload['strategy']['exit']['armed_flush_buffer_bars'] = int(branch.armed_flush_buffer_bars)
    payload['strategy']['exit']['armed_flush_min_gap_minutes'] = int(branch.armed_flush_min_gap_minutes)
    payload['strategy']['exit']['session_flat_all_phases_buffer_bars'] = int(branch.session_flat_all_phases_buffer_bars)
    payload['strategy']['exit']['session_flat_scope'] = str(branch.session_flat_scope)
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
    multipliers = build_multiplier_lookup(config)
    trades = build_trade_records(signals_path, multipliers, executions_path)
    summary = summarize_backtest_run(signals_path, portfolio_daily_path, multipliers, executions_path)
    summary.update({
        'branch': branch.label,
        'risk_label': risk.label,
        'entry_frequency': config.universe.entry_frequency,
        'trend_frequency': config.universe.trend_frequency,
        'donchian_lookback': config.strategy.entry.donchian_lookback,
        'risk_per_trade_ratio': config.portfolio.risk_per_trade_ratio,
        'max_total_armed_risk_ratio': config.portfolio.max_total_armed_risk_ratio,
        'output_dir': output_dir.as_posix(),
    })
    return summary, trades


def gate_status(branch: BranchSpec, summary: dict[str, Any]) -> tuple[bool, str]:
    failures: list[str] = []
    if float(summary.get('net_return_ratio', 0.0) or 0.0) <= 0:
        failures.append('net_return_ratio<=0')
    if branch.label == TREND_BRANCH.label:
        if float(summary.get('max_drawdown', 1.0) or 1.0) > 0.12:
            failures.append('max_drawdown>0.12')
        if int(summary.get('portfolio_halt_count_costed', 0) or 0) != 0:
            failures.append('portfolio_halt_count_costed!=0')
        if int(summary.get('multi_session_hold_count', 0) or 0) < 2:
            failures.append('multi_session_hold_count<2')
        if int(summary.get('protected_reach_count', 0) or 0) < 5:
            failures.append('protected_reach_count<5')
    else:
        if float(summary.get('max_drawdown', 1.0) or 1.0) > 0.10:
            failures.append('max_drawdown>0.10')
        if int(summary.get('portfolio_halt_count_costed', 0) or 0) != 0:
            failures.append('portfolio_halt_count_costed!=0')
        if int(summary.get('overnight_hold_count', 0) or 0) != 0:
            failures.append('overnight_hold_count!=0')
        if int(summary.get('trading_day_flat_exit_count', 0) or 0) < 1:
            failures.append('trading_day_flat_exit_count<1')
    return (not failures), ('ok' if not failures else ','.join(failures))


def stability_status(summary: dict[str, Any]) -> str:
    if float(summary.get('net_return_ratio', 0.0) or 0.0) <= 0:
        return 'n/a'
    return 'concentration_fragile' if float(summary.get('top_symbol_pnl_share', 0.0) or 0.0) > 0.60 else 'balanced'


def branch_decision_status(summary: dict[str, Any]) -> str:
    if int(summary.get('gate_passed', 0) or 0) != 1:
        return 'not_proven'
    if stability_status(summary) == 'concentration_fragile':
        return 'concentration_fragile'
    return 'candidate_found'


def order_costed_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            row.get('branch', ''),
            -int(bool(row.get('gate_passed', 0))),
            -float(row.get('net_return_ratio', -1.0) or -1.0),
            float(row.get('max_drawdown', 1.0) or 1.0),
            row.get('risk_label', ''),
            row.get('cost_profile', ''),
        ),
    )


def write_run_decision(path: Path, branch: BranchSpec, risk: RiskProfile, rows: list[dict[str, Any]]) -> None:
    realistic_row = next((row for row in rows if row.get('cost_profile') == 'realistic_top10_v1'), None)
    lines = [
        '# Run Decision',
        '',
        f'- branch: `{branch.label}`',
        f'- risk_label: `{risk.label}`',
    ]
    if realistic_row is not None:
        lines.extend([
            f"- realistic_gate_status: `{realistic_row.get('gate_status', 'n/a')}`",
            f"- realistic_stability_status: `{realistic_row.get('stability_status', 'n/a')}`",
            f"- realistic_net_return_ratio: `{float(realistic_row.get('net_return_ratio', 0.0)):.4f}`",
            f"- realistic_total_cost: `{float(realistic_row.get('total_cost', 0.0)):.2f}`",
            f"- realistic_cost_to_turnover_ratio: `{float(realistic_row.get('cost_to_turnover_ratio', 0.0)):.4%}`",
            f"- realistic_max_drawdown: `{float(realistic_row.get('max_drawdown', 0.0)):.4f}`",
        ])
    lines.extend([
        '',
        '## Cost Profiles',
        '',
        '| cost_profile | gate | stability | gross_pnl | total_cost | net_pnl | max_drawdown | protected | overnight | day_flat |',
        '| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |',
    ])
    for row in rows:
        lines.append(
            f"| {row['cost_profile']} | {row.get('gate_status', 'n/a')} | {row.get('stability_status', 'n/a')} | {float(row.get('gross_pnl', 0.0)):.2f} | {float(row.get('total_cost', 0.0)):.2f} | {float(row.get('net_pnl', 0.0)):.2f} | {float(row.get('max_drawdown', 0.0)):.4f} | {int(row.get('protected_reach_count', 0) or 0)} | {int(row.get('overnight_hold_count', 0) or 0)} | {int(row.get('trading_day_flat_exit_count', 0) or 0)} |"
        )
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def write_branch_decision(path: Path, branch: BranchSpec, costed_rows: list[dict[str, Any]]) -> None:
    realistic_rows = [row for row in costed_rows if row.get('cost_profile') == 'realistic_top10_v1']
    ordered = sorted(realistic_rows, key=lambda row: (-int(bool(row.get('gate_passed', 0))), float(row.get('top_symbol_pnl_share', 9.0) or 9.0), -float(row.get('net_return_ratio', -1.0) or -1.0), float(row.get('max_drawdown', 1.0) or 1.0), row.get('risk_label', '')))
    best = ordered[0] if ordered else None
    lines = [
        '# Branch Decision',
        '',
        f'- branch: `{branch.label}`',
        f"- research_status: `{branch_decision_status(best) if best is not None else 'not_proven'}`",
    ]
    if best is not None:
        lines.extend([
            f"- best_risk_label: `{best['risk_label']}`",
            f"- best_gate_status: `{best['gate_status']}`",
            f"- best_stability_status: `{best.get('stability_status', 'n/a')}`",
            f"- best_net_return_ratio: `{float(best.get('net_return_ratio', 0.0)):.4f}`",
            f"- best_total_cost: `{float(best.get('total_cost', 0.0)):.2f}`",
            f"- best_cost_to_turnover_ratio: `{float(best.get('cost_to_turnover_ratio', 0.0)):.4%}`",
            f"- best_max_drawdown: `{float(best.get('max_drawdown', 0.0)):.4f}`",
            f"- best_top_symbol_pnl_share: `{float(best.get('top_symbol_pnl_share', 0.0)):.4f}`",
        ])
    lines.extend([
        '',
        '## Realistic Cost Results',
        '',
        '| risk_label | gate | stability | gross_pnl | total_cost | net_pnl | max_drawdown | protected | overnight | day_flat | top_share |',
        '| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |',
    ])
    for row in ordered:
        lines.append(
            f"| {row['risk_label']} | {row['gate_status']} | {row.get('stability_status', 'n/a')} | {float(row.get('gross_pnl', 0.0)):.2f} | {float(row.get('total_cost', 0.0)):.2f} | {float(row.get('net_pnl', 0.0)):.2f} | {float(row.get('max_drawdown', 0.0)):.4f} | {int(row.get('protected_reach_count', 0) or 0)} | {int(row.get('overnight_hold_count', 0) or 0)} | {int(row.get('trading_day_flat_exit_count', 0) or 0)} | {float(row.get('top_symbol_pnl_share', 0.0)):.4f} |"
        )
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def write_root_decision(path: Path, costed_rows: list[dict[str, Any]]) -> None:
    realistic_rows = [row for row in costed_rows if row.get('cost_profile') == 'realistic_top10_v1']
    passed_rows = [row for row in realistic_rows if int(row.get('gate_passed', 0) or 0) == 1]
    balanced_rows = [row for row in passed_rows if row.get('stability_status') != 'concentration_fragile']
    verdict = 'at_least_one_branch_candidate' if balanced_rows else ('concentration_fragile' if passed_rows else 'breakout_family_not_proven')
    lines = [
        '# Dual Branch Effectiveness Decision',
        '',
        '- default_runtime_change: `none`',
        '- strategy_upgrade: `deferred`',
        '- cost_model: `raw_fill_capture_plus_research_overlay`',
        '- universe_policy: `shared_pool_across_branches`',
        f"- verdict: `{verdict}`",
        '- note: `this run keeps two explicit mainlines; intraday is 5m only, trend is 15m only`',
        '',
        '## Branch Snapshot',
        '',
        '| branch | risk_label | gate | stability | net_return | total_cost | max_drawdown | top_share | cost_profile |',
        '| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |',
    ]
    for branch in BRANCHES:
        branch_rows = [row for row in realistic_rows if row.get('branch') == branch.label]
        branch_rows.sort(key=lambda row: (-int(bool(row.get('gate_passed', 0))), float(row.get('top_symbol_pnl_share', 9.0) or 9.0), -float(row.get('net_return_ratio', -1.0) or -1.0), float(row.get('max_drawdown', 1.0) or 1.0), row.get('risk_label', '')))
        if branch_rows:
            row = branch_rows[0]
            lines.append(f"| {row['branch']} | {row['risk_label']} | {row['gate_status']} | {row.get('stability_status', 'n/a')} | {float(row.get('net_return_ratio', 0.0)):.4f} | {float(row.get('total_cost', 0.0)):.2f} | {float(row.get('max_drawdown', 0.0)):.4f} | {float(row.get('top_symbol_pnl_share', 0.0)):.4f} | {row['cost_profile']} |")
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def print_summary(raw_rows: list[dict[str, Any]], costed_rows: list[dict[str, Any]]) -> None:
    print('raw summary')
    for row in raw_rows:
        print(f"  {row['branch']:14s} {row['risk_label']:10s} return={float(row.get('return_ratio', 0.0))*100:7.2f}% max_dd={float(row.get('max_drawdown', 0.0))*100:6.2f}% trades={int(row.get('trades', 0) or 0):2d}")
    print('costed summary (realistic_top10_v1)')
    for row in order_costed_rows([item for item in costed_rows if item.get('cost_profile') == 'realistic_top10_v1']):
        print(
            f"  {row['branch']:14s} {row['risk_label']:10s} gate={row['gate_status']:24s} stability={row.get('stability_status', 'n/a'):22s} "
            f"net={float(row.get('net_return_ratio', 0.0))*100:7.2f}% max_dd={float(row.get('max_drawdown', 0.0))*100:6.2f}% "
            f"cost={float(row.get('total_cost', 0.0)):10.2f} turnover_drag={float(row.get('cost_to_turnover_ratio', 0.0))*100:6.2f}%"
        )


def main() -> int:
    args = parse_args()
    base_config_path = (REPO_ROOT / args.base_config).resolve()
    output_root = (REPO_ROOT / args.output_root).resolve()
    configs_dir = output_root / 'configs'
    base_payload = load_yaml(base_config_path)
    reference_config = load_config(base_config_path)
    realistic_costs = load_cost_profile(REALISTIC_COST_PROFILE_PATH)
    selected_branches = [branch for branch in BRANCHES if args.branch is None or branch.label == args.branch]
    selected_risks = [risk for risk in RISK_PROFILES if args.risk_label is None or risk.label == args.risk_label]
    cost_assumption_rows = build_cost_assumption_rows(reference_config, REALISTIC_COST_PROFILE_PATH, realistic_costs)
    write_rows_csv(output_root / 'cost_assumptions.csv', COST_ASSUMPTION_COLUMNS, cost_assumption_rows)
    write_cost_model(output_root / 'cost_model.md', reference_config, REALISTIC_COST_PROFILE_PATH, realistic_costs)

    all_raw_rows: list[dict[str, Any]] = []
    all_costed_rows: list[dict[str, Any]] = []

    for branch in selected_branches:
        branch_root = output_root / branch.label
        write_rows_csv(branch_root / 'cost_assumptions.csv', COST_ASSUMPTION_COLUMNS, cost_assumption_rows)
        write_cost_model(branch_root / 'cost_model.md', reference_config, REALISTIC_COST_PROFILE_PATH, realistic_costs)
        branch_raw_rows: list[dict[str, Any]] = []
        branch_costed_rows: list[dict[str, Any]] = []
        for risk in selected_risks:
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
            trade_rows: list[dict[str, Any]] = []
            portfolio_rows: list[dict[str, Any]] = []
            symbol_rows: list[dict[str, Any]] = []
            overlays = [
                ('current_high_cost', apply_cost_overlay(trades, run_dir / 'portfolio_daily.csv', config, 'current_high_cost', current_high_cost=(reference_config.execution.backtest_commission_ratio, reference_config.execution.backtest_slippage_ratio))),
                ('realistic_top10_v1', apply_cost_overlay(trades, run_dir / 'portfolio_daily.csv', config, 'realistic_top10_v1', profile_rows=realistic_costs)),
            ]
            for cost_profile, overlay in overlays:
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
                summary_row['stability_status'] = stability_status(summary_row) if cost_profile == 'realistic_top10_v1' else 'reference_only'
                costed_rows.append(summary_row)
                branch_costed_rows.append(summary_row)
                all_costed_rows.append(summary_row)
                trade_rows.extend(overlay.trade_diagnostics)
                portfolio_rows.extend(overlay.portfolio_daily)
                symbol_rows.extend(overlay.symbol_cost_drag)

            write_rows_csv(run_dir / 'summary_costed.csv', BRANCH_SUMMARY_COSTED_COLUMNS, costed_rows)
            write_csv(run_dir / 'trade_diagnostics_costed.csv', COSTED_TRADE_DIAGNOSTIC_COLUMNS, trade_rows)
            write_csv(run_dir / 'portfolio_daily_costed.csv', PORTFOLIO_DAILY_COSTED_COLUMNS, portfolio_rows)
            write_csv(run_dir / 'symbol_cost_drag.csv', SYMBOL_COST_DRAG_COLUMNS, symbol_rows)
            write_run_decision(run_dir / 'decision.md', branch, risk, costed_rows)
            if not args.keep_configs and config_path.exists():
                config_path.unlink()

        write_rows_csv(branch_root / 'summary_raw.csv', RAW_SUMMARY_COLUMNS, branch_raw_rows)
        write_rows_csv(branch_root / 'summary_costed.csv', BRANCH_SUMMARY_COSTED_COLUMNS, order_costed_rows(branch_costed_rows))
        write_branch_decision(branch_root / 'decision.md', branch, branch_costed_rows)

    if not args.keep_configs and configs_dir.exists():
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

