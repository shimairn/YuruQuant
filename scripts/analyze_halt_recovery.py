from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from yuruquant.app.config_loader import load_config
from yuruquant.reporting.recovery import (
    DRAWDOWN_EPISODE_COLUMNS,
    HALT_RECOVERY_SUMMARY_COLUMNS,
    HALT_STREAK_COLUMNS,
    build_halt_recovery_report,
    format_halt_recovery_markdown,
)
from yuruquant.reporting.trade_records import build_trade_records
from yuruquant.research.workflows import build_multiplier_lookup


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Analyze drawdown lockout and halt recovery for a completed GM run.')
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
    report = build_halt_recovery_report(trades, portfolio_daily_path)

    write_csv(output_dir / 'halt_recovery_summary.csv', HALT_RECOVERY_SUMMARY_COLUMNS, [report.summary])
    write_csv(output_dir / 'halt_recovery_streaks.csv', HALT_STREAK_COLUMNS, list(report.halt_streak_rows))
    write_csv(output_dir / 'drawdown_episodes.csv', DRAWDOWN_EPISODE_COLUMNS, list(report.drawdown_episode_rows))
    output_dir.mkdir(parents=True, exist_ok=True)
    markdown_path = output_dir / 'halt_recovery.md'
    markdown_path.write_text(format_halt_recovery_markdown(report), encoding='utf-8')
    print(
        'assessment={assessment} halt_days={halt_days} lockout_halt_days={lockout_halt_days} markdown={markdown}'.format(
            assessment=report.summary.get('recovery_assessment', ''),
            halt_days=int(report.summary.get('halt_days', 0) or 0),
            lockout_halt_days=int(report.summary.get('lockout_halt_days', 0) or 0),
            markdown=markdown_path.as_posix(),
        )
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
