from __future__ import annotations

import csv
import subprocess
from pathlib import Path
from typing import Any

import yaml

from yuruquant.app.config_schema import AppConfig


def load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding='utf-8')) or {}


def write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=False), encoding='utf-8')


def reports_exist(output_dir: Path) -> bool:
    return all((output_dir / name).exists() for name in ('signals.csv', 'executions.csv', 'portfolio_daily.csv'))


def build_backtest_command(repo_root: Path, python_exe: str, config_path: Path, run_id: str) -> list[str]:
    return [python_exe, str(repo_root / 'main.py'), '--mode', 'BACKTEST', '--config', str(config_path), '--run-id', run_id]


def run_backtest(repo_root: Path, python_exe: str, config_path: Path, run_id: str) -> None:
    subprocess.run(build_backtest_command(repo_root, python_exe, config_path, run_id), cwd=str(repo_root), check=True)


def build_multiplier_lookup(config: AppConfig) -> dict[str, float]:
    multipliers = {csymbol: config.universe.instrument_defaults.multiplier for csymbol in config.universe.symbols}
    multipliers.update({csymbol: spec.multiplier for csymbol, spec in config.universe.instrument_overrides.items()})
    return multipliers


def write_rows_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, '') for name in fieldnames})


__all__ = [
    'build_backtest_command',
    'build_multiplier_lookup',
    'load_yaml',
    'reports_exist',
    'run_backtest',
    'write_rows_csv',
    'write_yaml',
]
