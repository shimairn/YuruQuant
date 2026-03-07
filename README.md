# YuruQuant

YuruQuant is a GM-only futures trading runtime for domestic futures trend-following strategies.
The current codebase is organized around a thin GM adapter, a pure internal engine, and a simplified dual-core trend breakout strategy.

## Layout

- `main.py`: single startup entrypoint.
- `config/strategy.yaml`: primary live/backtest config.
- `config/smoke_dual_core.yaml`: small 2-symbol smoke backtest config.
- `scripts/grid_search_dual_core.py`: local parameter sweep runner.
- `yuruquant/app`: CLI parsing, config loading, dependency assembly, runtime bootstrap.
- `yuruquant/adapters/gm`: all `gm.api` integration, contract mapping, subscriptions, order submission, callbacks.
- `yuruquant/core`: engine loop, frames, indicators, fill policy, runtime models.
- `yuruquant/strategy/trend_breakout`: environment filter, breakout entry, ATR risk sizing, exit state machine.
- `yuruquant/portfolio`: portfolio-level halts and daily risk guards.
- `yuruquant/reporting`: CSV sinks, logging, backtest result analysis.
- `tests/unit`: isolated contract and behavior tests.
- `tests/integration`: startup and callback smoke coverage.

## Strategy

Current strategy defaults are intentionally minimal:

- Environment: `1h SMA60 + MACD(12,26,9) histogram`
- Entry: `5m Donchian(36)` breakout with minimum channel width filter
- Breakout quality: `ATR breakout buffer` plus single-bar `close position` filter
- Sizing: fixed `risk_per_trade_ratio` with ATR-based hard-stop distance
- Exit state machine:
  - `Armed`: hard stop at `2.2 ATR`
  - `Protected`: move to breakeven plus cost compensation after `1.5R`
  - `TrendRide`: exit on `5m SMA60` break after `2.5R`
- Execution: `next_bar_open`
- Portfolio protection: daily loss halt and drawdown halt

## Run

Use the local `minner` environment directly on this machine:

```powershell
C:\Users\wuktt\miniconda3\envs\minner\python.exe main.py --mode BACKTEST --config config\strategy.yaml
C:\Users\wuktt\miniconda3\envs\minner\python.exe main.py --mode LIVE --config config\strategy.yaml
```

## Grid Search

```powershell
C:\Users\wuktt\miniconda3\envs\minner\python.exe scripts\grid_search_dual_core.py --force
```

Summary output is written to `reports/grid_dual_core_2x3m/summary.csv`.

## Test

```powershell
C:\Users\wuktt\miniconda3\envs\minner\python.exe -m unittest discover -s tests -p "test_*.py" -v
```
