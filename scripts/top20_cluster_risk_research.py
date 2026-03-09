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
DEFAULT_CLUSTER_TEMPLATE = REPO_ROOT / 'config.example' / 'liquid_top20_dual_core.yaml'
TREND_ENTRY_FREQUENCY = '900s'
TREND_FREQUENCY = '3600s'
TREND_DONCHIAN = 12
TREND_PROTECTED_R = 1.8
TREND_ENTRY_BLOCK_MAJOR_GAP_BARS = 1
TREND_ARMED_FLUSH_BUFFER_BARS = 1
TREND_ARMED_FLUSH_MIN_GAP_MINUTES = 180
GM_BUILTIN_COST_PROFILE = 'gm_builtin_unified'
SUMMARY_COLUMNS = [
    'label',
    'risk_per_trade_ratio',
    'max_total_armed_risk_ratio',
    'max_cluster_armed_risk_ratio',
    'max_same_direction_cluster_positions',
    *SUMMARY_COSTED_COLUMNS,
    'gate_passed',
    'gate_status',
    'output_dir',
]
BASELINE_COMPARISON_COLUMNS = [
    'label',
    'baseline_label',
    'net_return_ratio_delta',
    'max_drawdown_delta',
    'portfolio_halt_count_delta',
    'protected_reach_count_delta',
    'trades_delta',
    'promotion_verdict',
]


@dataclass(frozen=True)
class ClusterProfile:
    label: str
    risk_per_trade_ratio: float
    max_total_armed_risk_ratio: float
    max_cluster_armed_risk_ratio: float
    max_same_direction_cluster_positions: int


PROFILES = (
    ClusterProfile('control_r10_cap20', 0.010, 0.020, 0.0, 0),
    ClusterProfile('cluster_r10_cap20_c010_p2', 0.010, 0.020, 0.010, 2),
    ClusterProfile('cluster_r10_cap20_c015_p2', 0.010, 0.020, 0.015, 2),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run Top20 trend-identity cluster-risk research on the GM-only CTA baseline.')
    parser.add_argument('--base-config', default='config/liquid_top20_dual_core.yaml')
    parser.add_argument('--baseline-output-dir')
    parser.add_argument('--python-exe', default=DEFAULT_PYTHON_EXE)
    parser.add_argument('--output-root', default='reports/top20_cluster_risk_v1')
    parser.add_argument('--profile', choices=[item.label for item in PROFILES])
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--keep-configs', action='store_true')
    return parser.parse_args()


def ensure_risk_clusters(base_payload: dict[str, Any], cluster_source_payload: dict[str, Any]) -> dict[str, Any]:
    payload = deepcopy(base_payload)
    universe = payload.setdefault('universe', {})
    if universe.get('risk_clusters'):
        return payload

    cluster_source = cluster_source_payload.get('universe', {}).get('risk_clusters', {})
    if not cluster_source:
        raise ValueError('cluster source payload does not define universe.risk_clusters')
    universe['risk_clusters'] = deepcopy(cluster_source)
    return payload


def load_base_payload(base_config_path: Path, cluster_template_path: Path = DEFAULT_CLUSTER_TEMPLATE) -> dict[str, Any]:
    base_payload = load_yaml(base_config_path)
    cluster_template = load_yaml(cluster_template_path)
    return ensure_risk_clusters(base_payload, cluster_template)


def resolve_output_dir(raw_output_dir: str) -> Path:
    output_dir = Path(raw_output_dir)
    if output_dir.is_absolute():
        return output_dir
    return (REPO_ROOT / output_dir).resolve()


def build_run_payload(base_payload: dict[str, Any], profile: ClusterProfile, output_dir: Path) -> dict[str, Any]:
    payload = deepcopy(base_payload)
    payload.setdefault('runtime', {})
    payload.setdefault('universe', {})
    payload.setdefault('strategy', {})
    payload['strategy'].setdefault('entry', {})
    payload['strategy'].setdefault('exit', {})
    payload.setdefault('portfolio', {})
    payload.setdefault('reporting', {})
    payload['runtime']['run_id'] = profile.label
    payload['universe']['entry_frequency'] = TREND_ENTRY_FREQUENCY
    payload['universe']['trend_frequency'] = TREND_FREQUENCY
    payload['strategy']['entry']['donchian_lookback'] = TREND_DONCHIAN
    payload['strategy']['entry']['entry_block_major_gap_bars'] = TREND_ENTRY_BLOCK_MAJOR_GAP_BARS
    payload['strategy']['exit']['protected_activate_r'] = TREND_PROTECTED_R
    payload['strategy']['exit']['armed_flush_buffer_bars'] = TREND_ARMED_FLUSH_BUFFER_BARS
    payload['strategy']['exit']['armed_flush_min_gap_minutes'] = TREND_ARMED_FLUSH_MIN_GAP_MINUTES
    payload['strategy']['exit']['session_flat_scope'] = 'disabled'
    payload['portfolio']['risk_per_trade_ratio'] = float(profile.risk_per_trade_ratio)
    payload['portfolio']['max_total_armed_risk_ratio'] = float(profile.max_total_armed_risk_ratio)
    payload['portfolio']['max_cluster_armed_risk_ratio'] = float(profile.max_cluster_armed_risk_ratio)
    payload['portfolio']['max_same_direction_cluster_positions'] = int(profile.max_same_direction_cluster_positions)
    payload['reporting']['output_dir'] = output_dir.as_posix()
    return payload


def collect_run_summary(label: str, config: AppConfig, output_dir: Path) -> tuple[dict[str, Any], list[TradeRecord]]:
    signals_path = output_dir / 'signals.csv'
    executions_path = output_dir / 'executions.csv'
    portfolio_daily_path = output_dir / 'portfolio_daily.csv'
    multipliers = build_multiplier_lookup(config)
    trades = build_trade_records(signals_path, multipliers, executions_path)
    raw_summary = summarize_backtest_run(signals_path, portfolio_daily_path, multipliers, executions_path)
    platform_report = build_platform_cost_report(trades, portfolio_daily_path, config, GM_BUILTIN_COST_PROFILE)
    summary = {
        'label': label,
        'risk_per_trade_ratio': config.portfolio.risk_per_trade_ratio,
        'max_total_armed_risk_ratio': config.portfolio.max_total_armed_risk_ratio,
        'max_cluster_armed_risk_ratio': config.portfolio.max_cluster_armed_risk_ratio,
        'max_same_direction_cluster_positions': config.portfolio.max_same_direction_cluster_positions,
        **platform_report.summary,
        'gross_pnl': float(raw_summary.get('net_profit', 0.0) or 0.0),
        'net_pnl': float(raw_summary.get('net_profit', 0.0) or 0.0),
        'gross_return_ratio': float(raw_summary.get('return_ratio', 0.0) or 0.0),
        'net_return_ratio': float(raw_summary.get('return_ratio', 0.0) or 0.0),
        'max_drawdown': float(raw_summary.get('max_drawdown', 0.0) or 0.0),
        'end_equity': float(raw_summary.get('end_equity', 0.0) or 0.0),
        'portfolio_halt_count_costed': int(raw_summary.get('halt_days', 0) or 0),
        'output_dir': output_dir.as_posix(),
    }
    return summary, trades


def collect_summary(profile: ClusterProfile, config: AppConfig, output_dir: Path) -> tuple[dict[str, Any], list[TradeRecord]]:
    summary, trades = collect_run_summary(profile.label, config, output_dir)
    summary['gate_passed'], summary['gate_status'] = gate_status(summary)
    return summary, trades


def gate_status(summary: dict[str, Any]) -> tuple[int, str]:
    failures: list[str] = []
    if float(summary.get('return_ratio', summary.get('net_return_ratio', 0.0)) or 0.0) <= -0.12:
        failures.append('return<=-12%')
    if float(summary.get('max_drawdown', 1.0) or 1.0) > 0.16:
        failures.append('max_drawdown>16%')
    if int(summary.get('portfolio_halt_count_costed', 0) or 0) > 20:
        failures.append('halt_days>20')
    if int(summary.get('protected_reach_count', 0) or 0) < 2:
        failures.append('protected_reach_count<2')
    return (0 if failures else 1), ('ok' if not failures else ','.join(failures))


def order_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            -int(bool(row.get('gate_passed', 0))),
            int(row.get('portfolio_halt_count_costed', 9999) or 9999),
            -float(row.get('net_return_ratio', -1.0) or -1.0),
            float(row.get('max_drawdown', 1.0) or 1.0),
            row.get('label', ''),
        ),
    )


def build_baseline_comparison_rows(
    rows: list[dict[str, Any]],
    baseline: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if baseline is None:
        return []
    baseline_return = float(baseline.get('net_return_ratio', 0.0) or 0.0)
    baseline_drawdown = float(baseline.get('max_drawdown', 0.0) or 0.0)
    baseline_halts = int(baseline.get('portfolio_halt_count_costed', 0) or 0)
    baseline_protected = int(baseline.get('protected_reach_count', 0) or 0)
    baseline_trades = int(baseline.get('trades', 0) or 0)
    comparison_rows: list[dict[str, Any]] = []

    for row in rows:
        halt_delta = int(row.get('portfolio_halt_count_costed', 0) or 0) - baseline_halts
        return_delta = float(row.get('net_return_ratio', 0.0) or 0.0) - baseline_return
        drawdown_delta = float(row.get('max_drawdown', 0.0) or 0.0) - baseline_drawdown
        protected_delta = int(row.get('protected_reach_count', 0) or 0) - baseline_protected
        trades_delta = int(row.get('trades', 0) or 0) - baseline_trades
        verdict = 'candidate'
        if halt_delta >= 0 or return_delta < 0.0 or drawdown_delta > 0.0:
            verdict = 'do_not_promote'
        comparison_rows.append(
            {
                'label': row['label'],
                'baseline_label': baseline.get('label', 'baseline'),
                'net_return_ratio_delta': return_delta,
                'max_drawdown_delta': drawdown_delta,
                'portfolio_halt_count_delta': halt_delta,
                'protected_reach_count_delta': protected_delta,
                'trades_delta': trades_delta,
                'promotion_verdict': verdict,
            }
        )
    return comparison_rows


def write_decision(path: Path, rows: list[dict[str, Any]], baseline: dict[str, Any] | None = None) -> None:
    ordered = order_rows(rows)
    best = ordered[0] if ordered else None
    comparison_rows = build_baseline_comparison_rows(ordered, baseline)
    promoted = next((row['label'] for row in comparison_rows if row['promotion_verdict'] == 'candidate'), 'do_not_promote')
    lines = [
        '# Top20 Cluster Risk Decision',
        '',
        '- branch: `trend_identity`',
        '- objective: `reduce portfolio halts without changing entry micro-logic`',
        f'- promotion_recommendation: `{promoted}`',
    ]
    if baseline is not None:
        lines.extend(
            [
                f"- baseline_label: `{baseline.get('label', 'baseline_current')}`",
                f"- baseline_return_ratio: `{float(baseline.get('net_return_ratio', 0.0) or 0.0):.4f}`",
                f"- baseline_max_drawdown: `{float(baseline.get('max_drawdown', 0.0) or 0.0):.4f}`",
                f"- baseline_portfolio_halt_days: `{int(baseline.get('portfolio_halt_count_costed', 0) or 0)}`",
            ]
        )
    else:
        lines.append('- baseline_status: `skipped (missing reports)`')
    if best is not None:
        lines.extend(
            [
                f"- best_label: `{best['label']}`",
                f"- best_gate_status: `{best['gate_status']}`",
                f"- best_return_ratio: `{float(best.get('net_return_ratio', 0.0)):.4f}`",
                f"- best_max_drawdown: `{float(best.get('max_drawdown', 0.0)):.4f}`",
                f"- best_portfolio_halt_days: `{int(best.get('portfolio_halt_count_costed', 0) or 0)}`",
            ]
        )
    lines.extend(
        [
            '',
            '## Profile Snapshot',
            '',
            '| label | total cap | cluster cap | cluster positions | halts | return | max dd | gate |',
            '| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |',
        ]
    )
    for row in ordered:
        lines.append(
            f"| {row['label']} | {float(row['max_total_armed_risk_ratio']):.3f} | {float(row['max_cluster_armed_risk_ratio']):.3f} | {int(row['max_same_direction_cluster_positions'])} | {int(row.get('portfolio_halt_count_costed', 0) or 0)} | {float(row.get('net_return_ratio', 0.0))*100:.2f}% | {float(row.get('max_drawdown', 0.0))*100:.2f}% | {row['gate_status']} |"
        )
    if comparison_rows:
        lines.extend(
            [
                '',
                '## Baseline Comparison',
                '',
                '| label | halt delta | return delta | max dd delta | protected delta | trades delta | verdict |',
                '| --- | ---: | ---: | ---: | ---: | ---: | --- |',
            ]
        )
        for row in comparison_rows:
            lines.append(
                f"| {row['label']} | {int(row['portfolio_halt_count_delta'])} | {float(row['net_return_ratio_delta'])*100:.2f}% | {float(row['max_drawdown_delta'])*100:.2f}% | {int(row['protected_reach_count_delta'])} | {int(row['trades_delta'])} | {row['promotion_verdict']} |"
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def main() -> int:
    args = parse_args()
    base_config_path = (REPO_ROOT / args.base_config).resolve()
    output_root = (REPO_ROOT / args.output_root).resolve()
    configs_dir = output_root / 'configs'
    base_payload = load_base_payload(base_config_path)
    baseline_config = load_config(base_config_path)
    baseline_output_dir = resolve_output_dir(
        args.baseline_output_dir if args.baseline_output_dir else baseline_config.reporting.output_dir
    )
    baseline_summary: dict[str, Any] | None = None
    if reports_exist(baseline_output_dir):
        baseline_summary, _ = collect_run_summary('baseline_current', baseline_config, baseline_output_dir)
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
            f"total_cap={config.portfolio.max_total_armed_risk_ratio:.3f}, "
            f"cluster_cap={config.portfolio.max_cluster_armed_risk_ratio:.3f}, "
            f"cluster_pos={config.portfolio.max_same_direction_cluster_positions}"
        )
        if args.force or not reports_exist(run_dir):
            run_backtest(REPO_ROOT, args.python_exe, config_path, config.runtime.run_id)
        else:
            print(f'  skipping existing raw reports at {run_dir.as_posix()}')
        summary, trades = collect_summary(profile, config, run_dir)
        rows.append(summary)
        write_rows_csv(run_dir / 'summary_costed.csv', SUMMARY_COLUMNS, [summary])
        platform_report = build_platform_cost_report(trades, run_dir / 'portfolio_daily.csv', config, GM_BUILTIN_COST_PROFILE)
        write_csv(run_dir / 'trade_diagnostics_costed.csv', COSTED_TRADE_DIAGNOSTIC_COLUMNS, platform_report.trade_diagnostics)
        write_csv(run_dir / 'portfolio_daily_costed.csv', PORTFOLIO_DAILY_COSTED_COLUMNS, platform_report.portfolio_daily)
        write_csv(run_dir / 'symbol_cost_drag.csv', SYMBOL_COST_DRAG_COLUMNS, platform_report.symbol_cost_drag)
        if not args.keep_configs and config_path.exists():
            config_path.unlink()

    if not args.keep_configs and configs_dir.exists() and not any(configs_dir.iterdir()):
        configs_dir.rmdir()
    write_rows_csv(output_root / 'summary_costed.csv', SUMMARY_COLUMNS, order_rows(rows))
    comparison_rows = build_baseline_comparison_rows(order_rows(rows), baseline_summary)
    if comparison_rows:
        write_rows_csv(output_root / 'baseline_comparison.csv', BASELINE_COMPARISON_COLUMNS, comparison_rows)
    write_decision(output_root / 'decision.md', rows, baseline_summary)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
