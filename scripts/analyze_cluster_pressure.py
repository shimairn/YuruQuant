from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from yuruquant.app.config_loader import load_config
from yuruquant.reporting.diversification import (
    CLUSTER_PRESSURE_COLUMNS,
    DIVERSIFICATION_SUMMARY_COLUMNS,
    HALT_DAY_COLUMNS,
    build_cluster_lookup,
    build_diversification_report,
    format_diversification_markdown,
)
from yuruquant.reporting.trade_records import build_trade_records
from yuruquant.research.workflows import build_multiplier_lookup


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Analyze cluster pressure and halt attribution for a GM run.')
    parser.add_argument('--config', default='config/strategy.yaml')
    parser.add_argument('--run-dir', default='')
    parser.add_argument('--signals', default='')
    parser.add_argument('--executions', default='')
    parser.add_argument('--portfolio-daily', default='')
    parser.add_argument('--output-dir', default='')
    return parser.parse_args()


def resolve_run_files(config_path: Path, run_dir: str, signals: str, executions: str, portfolio_daily: str) -> tuple[Path, Path, Path, Path]:
    config = load_config(config_path)
    base_dir = Path(run_dir).resolve() if run_dir else (REPO_ROOT / config.reporting.output_dir).resolve()
    signals_path = Path(signals).resolve() if signals else base_dir / config.reporting.signals_filename
    executions_path = Path(executions).resolve() if executions else base_dir / config.reporting.executions_filename
    portfolio_daily_path = Path(portfolio_daily).resolve() if portfolio_daily else base_dir / config.reporting.portfolio_daily_filename
    return base_dir, signals_path, executions_path, portfolio_daily_path


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, float | int | str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, '') for field in fieldnames})


def main() -> int:
    args = parse_args()
    config_path = (REPO_ROOT / args.config).resolve()
    config = load_config(config_path)
    base_dir, signals_path, executions_path, portfolio_daily_path = resolve_run_files(
        config_path=config_path,
        run_dir=args.run_dir,
        signals=args.signals,
        executions=args.executions,
        portfolio_daily=args.portfolio_daily,
    )
    output_dir = Path(args.output_dir).resolve() if args.output_dir else base_dir

    trades = build_trade_records(
        signals_path=signals_path,
        multiplier_by_csymbol=build_multiplier_lookup(config),
        executions_path=executions_path,
    )
    cluster_lookup = build_cluster_lookup(config.universe.symbols, config.universe.risk_clusters)
    report = build_diversification_report(trades, portfolio_daily_path, cluster_lookup)

    write_csv(output_dir / 'cluster_pressure_summary.csv', DIVERSIFICATION_SUMMARY_COLUMNS, [report.summary])
    write_csv(output_dir / 'cluster_pressure_by_cluster.csv', CLUSTER_PRESSURE_COLUMNS, list(report.cluster_rows))
    write_csv(output_dir / 'cluster_pressure_halt_days.csv', HALT_DAY_COLUMNS, list(report.halt_day_rows))
    output_dir.mkdir(parents=True, exist_ok=True)
    markdown_path = output_dir / 'cluster_pressure.md'
    markdown_path.write_text(format_diversification_markdown(report), encoding='utf-8')

    print(
        'assessment={assessment} halt_days={halt_days} active_halt_days={active_halt_days} markdown={markdown}'.format(
            assessment=report.summary.get('pressure_assessment', ''),
            halt_days=int(report.summary.get('halt_days', 0) or 0),
            active_halt_days=int(report.summary.get('halt_days_with_active_positions', 0) or 0),
            markdown=markdown_path.as_posix(),
        )
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
