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
from yuruquant.research.workflows import build_multiplier_lookup, load_yaml, reports_exist, run_backtest, write_rows_csv, write_yaml
from yuruquant.reporting.cost_overlay import (
    COSTED_TRADE_DIAGNOSTIC_COLUMNS,
    PORTFOLIO_DAILY_COSTED_COLUMNS,
    SUMMARY_COSTED_COLUMNS,
    SYMBOL_COST_DRAG_COLUMNS,
    build_platform_cost_report,
    write_csv,
)
from yuruquant.reporting.summary import summarize_backtest_run
from yuruquant.reporting.trade_records import TradeRecord, build_trade_records


DEFAULT_PYTHON_EXE = r'C:\Users\wuktt\miniconda3\envs\minner\python.exe'
GM_BUILTIN_COST_PROFILE = 'gm_builtin_unified'
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
    parser = argparse.ArgumentParser(description='Run dual-branch effectiveness research with GM built-in commission/slippage.')
    parser.add_argument('--base-config', default='config/liquid_top10_dual_core.yaml')
    parser.add_argument('--python-exe', default=DEFAULT_PYTHON_EXE)
    parser.add_argument('--output-root', default='reports/dual_branch_effectiveness_v3')
    parser.add_argument('--branch', choices=[branch.label for branch in BRANCHES])
    parser.add_argument('--risk-label', choices=[risk.label for risk in RISK_PROFILES])
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--keep-configs', action='store_true')
    return parser.parse_args()

def build_cost_assumption_rows(reference_config: AppConfig) -> list[dict[str, Any]]:
    return [
        {
            'model_layer': 'gm_backtest',
            'cost_profile': GM_BUILTIN_COST_PROFILE,
            'scope': 'ALL',
            'csymbol': '*',
            'profile_path': '',
            'commission_ratio_applied': float(reference_config.execution.backtest_commission_ratio),
            'slippage_ratio_applied': float(reference_config.execution.backtest_slippage_ratio),
            'slippage_ticks_per_side': '',
            'source_url': '',
            'as_of_date': '',
            'notes': 'Commission and slippage are configured once via gm.api.run backtest parameters; no research-side overlay is applied.',
        },
    ]


def write_cost_model(path: Path, reference_config: AppConfig) -> None:
    lines = [
        '# Cost Model',
        '',
        '- `summary_raw.csv` and `summary_costed.csv` both come from the same GM backtest run.',
        f"- GM is configured once with commission `{float(reference_config.execution.backtest_commission_ratio):.6f}` and slippage `{float(reference_config.execution.backtest_slippage_ratio):.6f}`.",
        '- This script no longer forces `0/0` raw runs and no longer applies any local fee/slippage overlay.',
        '- `summary_costed.csv`, `portfolio_daily_costed.csv`, `trade_diagnostics_costed.csv`, and `symbol_cost_drag.csv` are compatibility reports under `gm_builtin_unified`.',
        '- Explicit per-trade commission and slippage attribution is not reconstructed locally; those columns remain zero in compatibility outputs.',
        '- Use the GM backtest UI/report as the authoritative cost ledger when auditing fee and slippage drag.',
        '',
        '## Decision Rule',
        '',
        '- Promotion decisions use the GM-configured `net_return_ratio`, `max_drawdown`, and halt metrics.',
        '- Trade-structure diagnostics continue to come from local trade reconstruction, without any extra cost overlay.',
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')

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
    payload['reporting']['output_dir'] = output_dir.as_posix()
    return payload

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
    platform_row = next((row for row in rows if row.get('cost_profile') == GM_BUILTIN_COST_PROFILE), None)
    lines = [
        '# Run Decision',
        '',
        f'- branch: `{branch.label}`',
        f'- risk_label: `{risk.label}`',
    ]
    if platform_row is not None:
        lines.extend([
            f"- gate_status: `{platform_row.get('gate_status', 'n/a')}`",
            f"- stability_status: `{platform_row.get('stability_status', 'n/a')}`",
            f"- net_return_ratio: `{float(platform_row.get('net_return_ratio', 0.0)):.4f}`",
            f"- max_drawdown: `{float(platform_row.get('max_drawdown', 0.0)):.4f}`",
            f"- total_cost_local: `{float(platform_row.get('total_cost', 0.0)):.2f}`",
            '- note: `commission/slippage are sourced from GM; local explicit cost attribution is disabled`',
        ])
    lines.extend([
        '',
        '## Cost Profiles',
        '',
        '| cost_profile | gate | stability | net_pnl | max_drawdown | protected | overnight | day_flat |',
        '| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |',
    ])
    for row in rows:
        lines.append(
            f"| {row['cost_profile']} | {row.get('gate_status', 'n/a')} | {row.get('stability_status', 'n/a')} | {float(row.get('net_pnl', 0.0)):.2f} | {float(row.get('max_drawdown', 0.0)):.4f} | {int(row.get('protected_reach_count', 0) or 0)} | {int(row.get('overnight_hold_count', 0) or 0)} | {int(row.get('trading_day_flat_exit_count', 0) or 0)} |"
        )
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def write_branch_decision(path: Path, branch: BranchSpec, costed_rows: list[dict[str, Any]]) -> None:
    platform_rows = [row for row in costed_rows if row.get('cost_profile') == GM_BUILTIN_COST_PROFILE]
    ordered = sorted(platform_rows, key=lambda row: (-int(bool(row.get('gate_passed', 0))), float(row.get('top_symbol_pnl_share', 9.0) or 9.0), -float(row.get('net_return_ratio', -1.0) or -1.0), float(row.get('max_drawdown', 1.0) or 1.0), row.get('risk_label', '')))
    best = ordered[0] if ordered else None
    lines = [
        '# Branch Decision',
        '',
        f'- branch: `{branch.label}`',
        f"- research_status: `{branch_decision_status(best) if best is not None else 'not_proven'}`",
        f"- cost_profile: `{GM_BUILTIN_COST_PROFILE}`",
    ]
    if best is not None:
        lines.extend([
            f"- best_risk_label: `{best['risk_label']}`",
            f"- best_gate_status: `{best['gate_status']}`",
            f"- best_stability_status: `{best.get('stability_status', 'n/a')}`",
            f"- best_net_return_ratio: `{float(best.get('net_return_ratio', 0.0)):.4f}`",
            f"- best_max_drawdown: `{float(best.get('max_drawdown', 0.0)):.4f}`",
            f"- best_top_symbol_pnl_share: `{float(best.get('top_symbol_pnl_share', 0.0)):.4f}`",
        ])
    lines.extend([
        '',
        '## GM Built-in Cost Results',
        '',
        '| risk_label | gate | stability | net_pnl | max_drawdown | protected | overnight | day_flat | top_share |',
        '| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |',
    ])
    for row in ordered:
        lines.append(
            f"| {row['risk_label']} | {row['gate_status']} | {row.get('stability_status', 'n/a')} | {float(row.get('net_pnl', 0.0)):.2f} | {float(row.get('max_drawdown', 0.0)):.4f} | {int(row.get('protected_reach_count', 0) or 0)} | {int(row.get('overnight_hold_count', 0) or 0)} | {int(row.get('trading_day_flat_exit_count', 0) or 0)} | {float(row.get('top_symbol_pnl_share', 0.0)):.4f} |"
        )
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def write_root_decision(path: Path, costed_rows: list[dict[str, Any]]) -> None:
    platform_rows = [row for row in costed_rows if row.get('cost_profile') == GM_BUILTIN_COST_PROFILE]
    passed_rows = [row for row in platform_rows if int(row.get('gate_passed', 0) or 0) == 1]
    balanced_rows = [row for row in passed_rows if row.get('stability_status') != 'concentration_fragile']
    verdict = 'at_least_one_branch_candidate' if balanced_rows else ('concentration_fragile' if passed_rows else 'breakout_family_not_proven')
    lines = [
        '# Dual Branch Effectiveness Decision',
        '',
        '- default_runtime_change: `none`',
        '- strategy_upgrade: `deferred`',
        '- cost_model: `gm_builtin_unified_from_gm`',
        '- universe_policy: `shared_pool_across_branches`',
        f"- verdict: `{verdict}`",
        '- note: `this run keeps two explicit mainlines; intraday is 5m only, trend is 15m only`',
        '',
        '## Branch Snapshot',
        '',
        '| branch | risk_label | gate | stability | net_return | max_drawdown | top_share | cost_profile |',
        '| --- | --- | --- | --- | ---: | ---: | ---: | --- |',
    ]
    for branch in BRANCHES:
        branch_rows = [row for row in platform_rows if row.get('branch') == branch.label]
        branch_rows.sort(key=lambda row: (-int(bool(row.get('gate_passed', 0))), float(row.get('top_symbol_pnl_share', 9.0) or 9.0), -float(row.get('net_return_ratio', -1.0) or -1.0), float(row.get('max_drawdown', 1.0) or 1.0), row.get('risk_label', '')))
        if branch_rows:
            row = branch_rows[0]
            lines.append(f"| {row['branch']} | {row['risk_label']} | {row['gate_status']} | {row.get('stability_status', 'n/a')} | {float(row.get('net_return_ratio', 0.0)):.4f} | {float(row.get('max_drawdown', 0.0)):.4f} | {float(row.get('top_symbol_pnl_share', 0.0)):.4f} | {row['cost_profile']} |")
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def print_summary(raw_rows: list[dict[str, Any]], costed_rows: list[dict[str, Any]]) -> None:
    print('raw summary')
    for row in raw_rows:
        print(f"  {row['branch']:14s} {row['risk_label']:10s} return={float(row.get('return_ratio', 0.0))*100:7.2f}% max_dd={float(row.get('max_drawdown', 0.0))*100:6.2f}% trades={int(row.get('trades', 0) or 0):2d}")
    print(f'costed summary ({GM_BUILTIN_COST_PROFILE})')
    for row in order_costed_rows([item for item in costed_rows if item.get('cost_profile') == GM_BUILTIN_COST_PROFILE]):
        print(
            f"  {row['branch']:14s} {row['risk_label']:10s} gate={row['gate_status']:24s} stability={row.get('stability_status', 'n/a'):22s} "
            f"net={float(row.get('net_return_ratio', 0.0))*100:7.2f}% max_dd={float(row.get('max_drawdown', 0.0))*100:6.2f}% "
            f"top_share={float(row.get('top_symbol_pnl_share', 0.0))*100:6.2f}%"
        )


def main() -> int:
    args = parse_args()
    base_config_path = (REPO_ROOT / args.base_config).resolve()
    output_root = (REPO_ROOT / args.output_root).resolve()
    configs_dir = output_root / 'configs'
    base_payload = load_yaml(base_config_path)
    reference_config = load_config(base_config_path)
    selected_branches = [branch for branch in BRANCHES if args.branch is None or branch.label == args.branch]
    selected_risks = [risk for risk in RISK_PROFILES if args.risk_label is None or risk.label == args.risk_label]
    cost_assumption_rows = build_cost_assumption_rows(reference_config)
    write_rows_csv(output_root / 'cost_assumptions.csv', COST_ASSUMPTION_COLUMNS, cost_assumption_rows)
    write_cost_model(output_root / 'cost_model.md', reference_config)

    all_raw_rows: list[dict[str, Any]] = []
    all_costed_rows: list[dict[str, Any]] = []

    for branch in selected_branches:
        branch_root = output_root / branch.label
        write_rows_csv(branch_root / 'cost_assumptions.csv', COST_ASSUMPTION_COLUMNS, cost_assumption_rows)
        write_cost_model(branch_root / 'cost_model.md', reference_config)
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
                run_backtest(REPO_ROOT, args.python_exe, config_path, run_id)
            else:
                print(f'  skipping existing raw reports at {run_dir.as_posix()}')

            raw_summary, trades = collect_raw_summary(branch, risk, config, run_dir)
            branch_raw_rows.append(raw_summary)
            all_raw_rows.append(raw_summary)
            write_rows_csv(run_dir / 'summary_raw.csv', RAW_SUMMARY_COLUMNS, [raw_summary])

            platform_report = build_platform_cost_report(trades, run_dir / 'portfolio_daily.csv', config, GM_BUILTIN_COST_PROFILE)
            summary_row = {
                'branch': branch.label,
                'risk_label': risk.label,
                'entry_frequency': config.universe.entry_frequency,
                'trend_frequency': config.universe.trend_frequency,
                'donchian_lookback': config.strategy.entry.donchian_lookback,
                'risk_per_trade_ratio': config.portfolio.risk_per_trade_ratio,
                'max_total_armed_risk_ratio': config.portfolio.max_total_armed_risk_ratio,
                **platform_report.summary,
                'gross_pnl': float(raw_summary.get('net_profit', 0.0) or 0.0),
                'net_pnl': float(raw_summary.get('net_profit', 0.0) or 0.0),
                'gross_return_ratio': float(raw_summary.get('return_ratio', 0.0) or 0.0),
                'net_return_ratio': float(raw_summary.get('return_ratio', 0.0) or 0.0),
                'max_drawdown': float(raw_summary.get('max_drawdown', 0.0) or 0.0),
                'end_equity': float(raw_summary.get('end_equity', 0.0) or 0.0),
                'portfolio_halt_count_costed': int(raw_summary.get('halt_days', 0) or 0),
                'output_dir': run_dir.as_posix(),
            }
            passed, status = gate_status(branch, summary_row)
            summary_row['gate_passed'] = int(passed)
            summary_row['gate_status'] = status
            summary_row['stability_status'] = stability_status(summary_row)

            branch_costed_rows.append(summary_row)
            all_costed_rows.append(summary_row)

            write_rows_csv(run_dir / 'summary_costed.csv', BRANCH_SUMMARY_COSTED_COLUMNS, [summary_row])
            write_csv(run_dir / 'trade_diagnostics_costed.csv', COSTED_TRADE_DIAGNOSTIC_COLUMNS, platform_report.trade_diagnostics)
            write_csv(run_dir / 'portfolio_daily_costed.csv', PORTFOLIO_DAILY_COSTED_COLUMNS, platform_report.portfolio_daily)
            write_csv(run_dir / 'symbol_cost_drag.csv', SYMBOL_COST_DRAG_COLUMNS, platform_report.symbol_cost_drag)
            write_run_decision(run_dir / 'decision.md', branch, risk, [summary_row])
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
