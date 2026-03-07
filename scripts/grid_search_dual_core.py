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

from yuruquant.app.config import AppConfig, load_config
from yuruquant.reporting.analysis import summarize_backtest_run


DEFAULT_BUFFERS = (0.25, 0.30, 0.35)
DEFAULT_PROTECTED_RS = (1.2, 1.3, 1.4, 1.5)
SUMMARY_COLUMNS = [
    'label',
    'breakout_atr_buffer',
    'protected_activate_r',
    'trades',
    'wins',
    'losses',
    'win_rate',
    'hard_stop_count',
    'hard_stop_ratio',
    'protected_stop_count',
    'protected_stop_ratio',
    'trend_ma_stop_count',
    'trend_ma_stop_ratio',
    'avg_win_pnl',
    'avg_loss_pnl',
    'avg_win_loss_ratio',
    'best_trade_pnl',
    'worst_trade_pnl',
    'start_equity',
    'end_equity',
    'net_profit',
    'return_ratio',
    'max_drawdown',
    'halt_days',
    'final_realized_pnl',
    'output_dir',
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run a local grid search for Dual-Core Trend Breakout.')
    parser.add_argument('--base-config', default='config/smoke_dual_core.yaml')
    parser.add_argument('--python-exe', default=r'C:\Users\wuktt\miniconda3\envs\minner\python.exe')
    parser.add_argument('--output-root', default='reports/grid_dual_core_2x3m')
    parser.add_argument('--buffers', nargs='+', type=float, default=list(DEFAULT_BUFFERS))
    parser.add_argument('--protected-rs', nargs='+', type=float, default=list(DEFAULT_PROTECTED_RS))
    parser.add_argument('--limit', type=int, default=0, help='Run only the first N combinations for quick checks.')
    parser.add_argument('--force', action='store_true', help='Re-run combinations even if output files already exist.')
    parser.add_argument('--keep-configs', action='store_true', help='Keep generated per-run config files.')
    return parser.parse_args()


def load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding='utf-8')) or {}


def run_label(buffer_value: float, protected_value: float) -> str:
    buffer_text = f'{buffer_value:.2f}'.replace('.', '')
    protected_text = f'{protected_value:.1f}'.replace('.', '')
    return f'b{buffer_text}_p{protected_text}'


def build_run_payload(base_payload: dict[str, Any], label: str, buffer_value: float, protected_value: float, output_dir: str) -> dict[str, Any]:
    payload = deepcopy(base_payload)
    payload.setdefault('runtime', {})
    payload.setdefault('strategy', {})
    payload['runtime']['run_id'] = f'grid_{label}'
    payload['strategy'].setdefault('entry', {})
    payload['strategy'].setdefault('exit', {})
    payload['strategy']['entry']['breakout_atr_buffer'] = float(buffer_value)
    payload['strategy']['exit']['protected_activate_r'] = float(protected_value)
    payload.setdefault('reporting', {})
    payload['reporting']['output_dir'] = output_dir.replace('\\', '/')
    return payload


def write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=False), encoding='utf-8')


def build_multiplier_lookup(config: AppConfig) -> dict[str, float]:
    multipliers = {csymbol: config.universe.instrument_defaults.multiplier for csymbol in config.universe.symbols}
    for csymbol, spec in config.universe.instrument_overrides.items():
        multipliers[csymbol] = spec.multiplier
    return multipliers


def reports_exist(output_dir: Path) -> bool:
    return (output_dir / 'signals.csv').exists() and (output_dir / 'portfolio_daily.csv').exists()


def run_backtest(python_exe: str, config_path: Path, run_id: str) -> None:
    command = [python_exe, 'main.py', '--mode', 'BACKTEST', '--config', str(config_path), '--run-id', run_id]
    subprocess.run(command, cwd=str(REPO_ROOT), check=True)


def collect_summary(label: str, buffer_value: float, protected_value: float, output_dir: Path, multiplier_lookup: dict[str, float]) -> dict[str, Any]:
    summary = summarize_backtest_run(
        signals_path=output_dir / 'signals.csv',
        portfolio_daily_path=output_dir / 'portfolio_daily.csv',
        multiplier_by_csymbol=multiplier_lookup,
    )
    summary['label'] = label
    summary['breakout_atr_buffer'] = buffer_value
    summary['protected_activate_r'] = protected_value
    summary['output_dir'] = output_dir.relative_to(REPO_ROOT).as_posix()
    return summary


def write_summary_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered_rows = sorted(rows, key=lambda row: (float(row['end_equity']), -float(row['max_drawdown'])), reverse=True)
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        for row in ordered_rows:
            writer.writerow({column: row.get(column, '') for column in SUMMARY_COLUMNS})


def print_summary(rows: list[dict[str, Any]]) -> None:
    ordered_rows = sorted(rows, key=lambda row: float(row['end_equity']), reverse=True)
    print('label    buffer protected trades hard_stop% protected% win_rate% return% max_dd% end_equity')
    for row in ordered_rows:
        print(
            f"{row['label']:7s} "
            f"{float(row['breakout_atr_buffer']):>6.2f} "
            f"{float(row['protected_activate_r']):>9.1f} "
            f"{int(row['trades']):>6d} "
            f"{float(row['hard_stop_ratio']) * 100:>9.2f} "
            f"{float(row['protected_stop_ratio']) * 100:>10.2f} "
            f"{float(row['win_rate']) * 100:>8.2f} "
            f"{float(row['return_ratio']) * 100:>7.2f} "
            f"{float(row['max_drawdown']) * 100:>7.2f} "
            f"{float(row['end_equity']):>11.2f}"
        )


def main() -> int:
    args = parse_args()
    base_config_path = (REPO_ROOT / args.base_config).resolve()
    output_root = (REPO_ROOT / args.output_root).resolve()
    configs_dir = output_root / 'configs'
    summary_path = output_root / 'summary.csv'
    base_payload = load_yaml(base_config_path)

    combinations: list[tuple[float, float]] = []
    for buffer_value in args.buffers:
        for protected_value in args.protected_rs:
            combinations.append((float(buffer_value), float(protected_value)))
    if args.limit > 0:
        combinations = combinations[: args.limit]

    rows: list[dict[str, Any]] = []
    for index, (buffer_value, protected_value) in enumerate(combinations, start=1):
        label = run_label(buffer_value, protected_value)
        run_id = f'grid_{label}'
        output_dir = output_root / label
        config_path = configs_dir / f'{label}.yaml'
        payload = build_run_payload(
            base_payload=base_payload,
            label=label,
            buffer_value=buffer_value,
            protected_value=protected_value,
            output_dir=output_dir.as_posix(),
        )
        write_yaml(config_path, payload)
        config = load_config(config_path)
        multiplier_lookup = build_multiplier_lookup(config)
        print(f'[{index}/{len(combinations)}] {label}: buffer={buffer_value:.2f}, protected={protected_value:.1f}')
        if args.force or not reports_exist(output_dir):
            run_backtest(args.python_exe, config_path, run_id)
        else:
            print(f'  skipping existing run at {output_dir.as_posix()}')
        rows.append(collect_summary(label, buffer_value, protected_value, output_dir, multiplier_lookup))
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

