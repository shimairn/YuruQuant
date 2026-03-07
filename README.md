# YuruQuant

YuruQuant is a GM-only domestic futures trading runtime built around a thin GM adapter, a pure internal event engine, and a simplified cross-period trend breakout strategy.

## Layout

- `main.py`: single startup entrypoint.
- `config/strategy.yaml`: primary live/backtest config.
- `config/smoke_dual_core.yaml`: small 2-symbol smoke backtest config.
- `scripts/grid_search_dual_core.py`: local parameter sweep runner.
- `yuruquant/app`: CLI parsing, config loading, dependency assembly, runtime bootstrap.
- `yuruquant/adapters/gm`: all `gm.api` integration, contract mapping, subscriptions, order submission, callbacks.
- `yuruquant/core`: engine loop, frames, indicators, fill policy, runtime models.
- `yuruquant/strategy/trend_breakout`: 1h environment, 5m breakout entry, ATR risk sizing, cross-period exit state machine.
- `yuruquant/portfolio`: portfolio-level halts and daily risk guards.
- `yuruquant/reporting`: CSV sinks, logging, backtest result analysis.
- `tests/unit`: isolated contract and behavior tests.
- `tests/integration`: startup and callback smoke coverage.

## Strategy

Current strategy defaults are intentionally minimal and fully V3-aligned:

- Environment: `1h SMA60 + MACD(12,26,9) histogram`
  - Long environment: `1h close > SMA60` and `MACD histogram > 0`
  - Short environment: `1h close < SMA60` and `MACD histogram < 0`
- Entry: `5m Donchian(36)` breakout
  - Minimum channel width: `(upper - lower) > 0.5 * ATR14`
  - Breakout buffer: `0.30 * ATR14`
  - Breakout bar close position filter: long `>= 0.70`, short `<= 0.30`
- Sizing: ATR risk budget
  - `risk_per_trade_ratio = 1.5%`
  - `qty = floor((equity * risk_ratio) / (2.2 * ATR * multiplier))`
  - Then rounded down by `min_lot` and `lot_step`
- Exit state machine:
  - `Armed`: fixed hard stop at `2.2 ATR(5m)`
  - `Protected`: when `MFE >= 1.2R`, move stop to breakeven plus estimated round-turn cost compensation
  - `Ascended`: when `MFE >= 2.5R`, keep the protected floor and stop using 5m trailing logic
  - `Ascended` primary exit: latest closed `1h close` reverses through `1h SMA60`
  - Exit trigger labels: `hard_stop`, `protected_stop`, `hourly_ma_stop`
- Execution: `next_bar_open`
  - Signals are produced on the current `5m` processing point and executed on the next `5m` bar open
- Portfolio protection: daily loss halt and max drawdown halt

## Run

Use the verified local `minner` interpreter on this machine:

```powershell
C:\Users\wuktt\miniconda3\envs\minner\python.exe main.py --mode BACKTEST --config config\strategy.yaml
C:\Users\wuktt\miniconda3\envs\minner\python.exe main.py --mode LIVE --config config\strategy.yaml
```

## Smoke Backtest

```powershell
C:\Users\wuktt\miniconda3\envs\minner\python.exe main.py --mode BACKTEST --config config\smoke_dual_core.yaml --run-id smoke_v3_cross_period
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
