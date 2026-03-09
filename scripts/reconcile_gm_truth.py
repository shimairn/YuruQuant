from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from yuruquant.app.config_loader import load_config
from yuruquant.reporting.reconciliation import build_reconciliation_row, format_reconciliation_markdown, reconcile_backtest_run
from yuruquant.research.workflows import build_multiplier_lookup


RECONCILIATION_COLUMNS = [
    'status',
    'portfolio_start_equity',
    'portfolio_end_equity',
    'portfolio_net_profit',
    'portfolio_return_ratio',
    'portfolio_max_drawdown',
    'portfolio_halt_days',
    'reconstructed_trade_count',
    'reconstructed_gross_pnl',
    'reconstructed_win_rate',
    'session_restart_gap_exit_count',
    'portfolio_halt_exit_count',
    'pnl_gap',
    'pnl_gap_ratio_to_start_equity',
    'issue_codes',
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Reconcile GM portfolio truth against local trade reconstruction.')
    parser.add_argument('--config', default='config/strategy.yaml')
    parser.add_argument('--run-dir', default='')
    parser.add_argument('--signals', default='')
    parser.add_argument('--executions', default='')
    parser.add_argument('--portfolio-daily', default='')
    parser.add_argument('--output-md', default='')
    parser.add_argument('--output-csv', default='')
    return parser.parse_args()


def resolve_run_files(config_path: Path, run_dir: str, signals: str, executions: str, portfolio_daily: str) -> tuple[Path, Path, Path, Path, Path]:
    config = load_config(config_path)
    base_dir = Path(run_dir).resolve() if run_dir else (REPO_ROOT / config.reporting.output_dir).resolve()
    signals_path = Path(signals).resolve() if signals else base_dir / config.reporting.signals_filename
    executions_path = Path(executions).resolve() if executions else base_dir / config.reporting.executions_filename
    portfolio_daily_path = Path(portfolio_daily).resolve() if portfolio_daily else base_dir / config.reporting.portfolio_daily_filename
    return config_path, base_dir, signals_path, executions_path, portfolio_daily_path


def write_csv(path: Path, row: dict[str, float | int | str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=RECONCILIATION_COLUMNS)
        writer.writeheader()
        writer.writerow({field: row.get(field, '') for field in RECONCILIATION_COLUMNS})


def main() -> int:
    args = parse_args()
    config_path = (REPO_ROOT / args.config).resolve()
    config = load_config(config_path)
    _, base_dir, signals_path, executions_path, portfolio_daily_path = resolve_run_files(
        config_path=config_path,
        run_dir=args.run_dir,
        signals=args.signals,
        executions=args.executions,
        portfolio_daily=args.portfolio_daily,
    )
    result = reconcile_backtest_run(
        signals_path=signals_path,
        portfolio_daily_path=portfolio_daily_path,
        multiplier_by_csymbol=build_multiplier_lookup(config),
        executions_path=executions_path,
    )

    output_md = Path(args.output_md).resolve() if args.output_md else base_dir / 'reconciliation.md'
    output_csv = Path(args.output_csv).resolve() if args.output_csv else base_dir / 'reconciliation.csv'
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_md.write_text(format_reconciliation_markdown(result), encoding='utf-8')
    write_csv(output_csv, build_reconciliation_row(result))
    print(f'status={result.status} pnl_gap={result.pnl_gap:.6f} markdown={output_md.as_posix()} csv={output_csv.as_posix()}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
