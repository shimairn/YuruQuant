from __future__ import annotations

import argparse
import csv
import subprocess
import sys
from copy import deepcopy
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
from yuruquant.reporting.trade_records import build_trade_records


DEFAULT_PROTECTED_RS = (1.5, 1.8)
SUMMARY_COLUMNS = [
    'label', 'breakout_atr_buffer', 'protected_activate_r', 'trades', 'wins', 'losses',
    'win_rate', 'hard_stop_count', 'hard_stop_ratio', 'protected_stop_count',
    'protected_stop_ratio', 'armed_flush_count', 'armed_flush_ratio',
    'session_flat_exit_count', 'session_flat_exit_ratio', 'portfolio_halt_count',
    'session_restart_gap_exit_count', 'session_restart_gap_exit_ratio',
    'session_restart_gap_portfolio_halt_count', 'session_restart_gap_portfolio_halt_ratio',
    'session_restart_gap_stop_count', 'session_restart_gap_stop_ratio',
    'session_restart_gap_overshoot_sum', 'session_restart_gap_overshoot_ratio',
    'hard_stop_overshoot_avg', 'hard_stop_overshoot_max', 'protected_stop_overshoot_avg',
    'protected_stop_overshoot_max', 'avg_win_pnl', 'avg_loss_pnl', 'avg_win_loss_ratio',
    'best_trade_pnl', 'worst_trade_pnl', 'start_equity', 'end_equity', 'net_profit',
    'return_ratio', 'max_drawdown', 'halt_days', 'final_realized_pnl', 'output_dir',
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run protected-stop grid search for Dual-Core Trend Breakout.')
    parser.add_argument('--base-config', default='config/liquid_top10_dual_core.yaml')
    parser.add_argument('--python-exe', default=r'C:\Users\wuktt\miniconda3\envs\minner\python.exe')
    parser.add_argument('--output-root', default='reports/grid_protected_top10_3m')
    parser.add_argument('--protected-rs', nargs='+', type=float, default=list(DEFAULT_PROTECTED_RS))
    parser.add_argument('--limit', type=int, default=0)
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--keep-configs', action='store_true')
    return parser.parse_args()


def load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding='utf-8')) or {}


def run_label(protected_value: float) -> str:
    return f"p{f'{protected_value:.1f}'.replace('.', '')}"


def build_combinations(protected_rs: list[float]) -> tuple[list[float], list[float]]:
    valid = [float(value) for value in protected_rs if float(value) > 0]
    skipped = [float(value) for value in protected_rs if float(value) <= 0]
    return valid, skipped


def build_run_payload(base_payload: dict[str, Any], label: str, protected_value: float, output_dir: str) -> dict[str, Any]:
    payload = deepcopy(base_payload)
    payload.setdefault('runtime', {})
    payload.setdefault('strategy', {})
    payload['strategy'].setdefault('exit', {})
    payload.setdefault('reporting', {})
    payload['runtime']['run_id'] = f'grid_{label}'
    payload['strategy']['exit']['protected_activate_r'] = float(protected_value)
    payload['reporting']['output_dir'] = output_dir.replace('\\', '/')
    return payload


def write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=False), encoding='utf-8')


def build_multiplier_lookup(config: AppConfig) -> dict[str, float]:
    multipliers = {csymbol: config.universe.instrument_defaults.multiplier for csymbol in config.universe.symbols}
    multipliers.update({csymbol: spec.multiplier for csymbol, spec in config.universe.instrument_overrides.items()})
    return multipliers


def reports_exist(output_dir: Path) -> bool:
    return all((output_dir / name).exists() for name in ('signals.csv', 'executions.csv', 'portfolio_daily.csv'))


def run_backtest(python_exe: str, config_path: Path, run_id: str) -> None:
    subprocess.run([python_exe, str(REPO_ROOT / 'main.py'), '--mode', 'BACKTEST', '--config', str(config_path), '--run-id', run_id], cwd=str(REPO_ROOT), check=True)


def collect_summary(label: str, config: AppConfig, protected_value: float, output_dir: Path, multiplier_lookup: dict[str, float]) -> dict[str, Any]:
    signals_path = output_dir / 'signals.csv'
    portfolio_daily_path = output_dir / 'portfolio_daily.csv'
    executions_path = output_dir / 'executions.csv'
    trades = build_trade_records(signals_path, multiplier_lookup, executions_path)
    write_trade_diagnostics_csv(output_dir / 'trade_diagnostics.csv', build_trade_diagnostics(trades))
    summary = summarize_backtest_run(signals_path, portfolio_daily_path, multiplier_lookup, executions_path)
    summary['label'] = label
    summary['breakout_atr_buffer'] = config.strategy.entry.breakout_atr_buffer
    summary['protected_activate_r'] = protected_value
    summary['output_dir'] = output_dir.relative_to(REPO_ROOT).as_posix()
    return summary


def sort_summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: (-float(row.get('end_equity', 0.0) or 0.0), float(row.get('max_drawdown', 0.0) or 0.0), row.get('label', '')))


def write_summary_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        for row in sort_summary_rows(rows):
            writer.writerow({column: row.get(column, '') for column in SUMMARY_COLUMNS})


def print_summary(rows: list[dict[str, Any]]) -> None:
    print('label    protected trades session_flat% halt_gap% return% max_dd% end_equity')
    for row in sort_summary_rows(rows):
        print(
            f"{row['label']:9s} {float(row['protected_activate_r']):>9.1f} {int(row['trades']):>6d} "
            f"{float(row['session_flat_exit_ratio']) * 100:>12.2f} {float(row['session_restart_gap_portfolio_halt_ratio']) * 100:>9.2f} "
            f"{float(row['return_ratio']) * 100:>7.2f} {float(row['max_drawdown']) * 100:>7.2f} {float(row['end_equity']):>11.2f}"
        )


def main() -> int:
    args = parse_args()
    base_config_path = (REPO_ROOT / args.base_config).resolve()
    output_root = (REPO_ROOT / args.output_root).resolve()
    configs_dir = output_root / 'configs'
    summary_path = output_root / 'summary.csv'
    base_payload = load_yaml(base_config_path)
    combinations, skipped = build_combinations(args.protected_rs)
    if args.limit > 0:
        combinations = combinations[: args.limit]
    for protected_value in skipped:
        print(f'skipping invalid combination: protected={protected_value:.1f}')
    rows: list[dict[str, Any]] = []
    total = len(combinations)
    for index, protected_value in enumerate(combinations, start=1):
        label = run_label(protected_value)
        run_id = f'grid_{label}'
        output_dir = output_root / label
        config_path = configs_dir / f'{label}.yaml'
        payload = build_run_payload(base_payload, label, protected_value, output_dir.as_posix())
        write_yaml(config_path, payload)
        config = load_config(config_path)
        multiplier_lookup = build_multiplier_lookup(config)
        print(f'[{index}/{total}] {label}: protected={protected_value:.1f}')
        if args.force or not reports_exist(output_dir):
            run_backtest(args.python_exe, config_path, run_id)
        else:
            print(f'  skipping existing raw reports at {output_dir.as_posix()}')
        rows.append(collect_summary(label, config, protected_value, output_dir, multiplier_lookup))
        write_summary_csv(summary_path, rows)
        if not args.keep_configs and config_path.exists():
            config_path.unlink()
    if not args.keep_configs and configs_dir.exists() and not any(configs_dir.iterdir()):
        configs_dir.rmdir()
    print(f'\nsummary saved to {summary_path.as_posix()}\n')
    print_summary(rows)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
