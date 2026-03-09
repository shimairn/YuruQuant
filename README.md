# YuruQuant

YuruQuant is a GM-only China futures research and execution stack. The project is intentionally narrow:

- Mainline: mid-frequency domestic futures CTA with `trend_identity` style trend-following.
- Secondary research line: `intraday_flat` style day-flat validation only.
- Non-goal: generic multi-broker or multi-family quant platform expansion.

## Architecture

- `main.py`: single runtime entrypoint.
- `config/strategy.yaml`: primary live and backtest config.
- `config/smoke_dual_core.yaml`: small smoke backtest config.
- `config.example/*.yaml`: tracked config templates for the ignored local `config/` folder.
- `scripts/*.py`: stable offline entrypoints for research, cleanup, and reconciliation.
- `yuruquant/app`: CLI parsing, config loading, dependency assembly, and runtime bootstrap.
- `yuruquant/adapters/gm`: all `gm.api` integration, subscriptions, order submission, and callback wiring.
- `yuruquant/core`: event orchestration, bar processing, execution flow, fill policy, and runtime state.
- `yuruquant/strategy/trend_breakout`: environment filter, breakout entry, ATR sizing, and the live two-phase exit logic.
- `yuruquant/portfolio`: daily guards and portfolio risk controls.
- `yuruquant/reporting`: CSV sinks, diagnostics, cost reports, and GM truth reconciliation.
- `yuruquant/research`: shared offline helpers for research scripts only.
- `docs/strategy_doctrine.md`: system identity, truth priorities, and promotion rules.
- `docs/research_roadmap.md`: phased R&D direction and canonical report roots.
- `docs/frontier_research_notes.md`: literature-backed frontier direction for the CTA mainline.

## Strategy Snapshot

Runtime defaults stay conservative and execution-compatible:

- Environment: default `1h SMA60 + MACD(12,26,9)`; optional research mode `multi-horizon TSMOM`
- Main entry trigger: `5m Donchian(36)` breakout with ATR width and ATR breakout buffer
- Sizing: ATR risk budget with `hard_stop_atr = 2.2`
- Execution: `next_bar_open`
- Exit logic:
  - `armed`: fixed ATR hard stop
  - `protected`: cost-compensated floor after `protected_activate_r`
- Portfolio protection: daily loss halt and drawdown halt
- Optional default-off cluster controls:
  - `universe.risk_clusters`
  - `portfolio.max_cluster_armed_risk_ratio`
  - `portfolio.max_same_direction_cluster_positions`
- Optional research-only trend environment:
  - `strategy.environment.mode = tsmom`
  - `strategy.environment.tsmom_lookbacks`
  - `strategy.environment.tsmom_min_agree`

Research may validate alternate entry cadence for the secondary branch, but live system identity remains the GM-only CTA mainline above.

## Truth Sources

The project uses a strict reporting contract:

1. `portfolio_daily.csv` and the GM backtest equity ledger are the canonical PnL truth.
2. `executions.csv` and `signals.csv` are execution and intent truth.
3. Local trade reconstruction and diagnostics are structural analysis only.

Use `scripts/reconcile_gm_truth.py` whenever a run needs explicit reconciliation between GM portfolio truth and local trade reconstruction.

## Stable Offline Entrypoints

- `scripts/grid_search_dual_core.py`
  - protected-stop sweep
  - canonical output root: `reports/grid_protected_top10_3m`
- `scripts/minimal_stable_research.py`
  - defensive baseline pack
  - canonical output root: `reports/minimal_stable_top10_v2`
- `scripts/dual_branch_effectiveness_research.py`
  - mainline versus secondary branch comparison
  - canonical output root: `reports/dual_branch_effectiveness_v3`
- `scripts/prune_local_artifacts.py`
  - safe cleanup for local report clutter
  - default mode is `--dry-run`
- `scripts/reconcile_gm_truth.py`
  - GM equity versus local trade reconstruction reconciliation
- `scripts/top20_cluster_risk_research.py`
  - Top20 trend-identity cluster-risk validation
  - canonical output root: `reports/top20_cluster_risk_v1`
- `scripts/top20_drawdown_recovery_research.py`
  - Top20 drawdown-halt and recovery-threshold validation
  - canonical output root: `reports/top20_drawdown_recovery_v1`
- `scripts/top20_drawdown_schedule_research.py`
  - Top20 non-absorbing drawdown schedule validation
  - canonical output root: `reports/top20_drawdown_schedule_v1`
- `scripts/analyze_cluster_pressure.py`
  - halt attribution and cluster/diversification diagnostics for a completed run
- `scripts/analyze_halt_recovery.py`
  - drawdown lockout and halt-recovery diagnostics for a completed run

## Config Setup

The local `config/` folder is ignored by Git. Copy tracked templates before running:

```powershell
New-Item -ItemType Directory -Force config | Out-Null
Copy-Item config.example\strategy.yaml config\strategy.yaml
Copy-Item config.example\smoke_dual_core.yaml config\smoke_dual_core.yaml
Copy-Item config.example\liquid_top10_dual_core.yaml config\liquid_top10_dual_core.yaml
Copy-Item config.example\liquid_top20_dual_core.yaml config\liquid_top20_dual_core.yaml
```

Then fill `broker.gm.token` and `broker.gm.strategy_id` locally, or export them:

```powershell
$env:GM_TOKEN = "your-gm-token"
$env:GM_STRATEGY_ID = "your-gm-strategy-id"
```

## Run

Use the verified local interpreter on this machine:

```powershell
C:\Users\wuktt\miniconda3\envs\minner\python.exe main.py --mode BACKTEST --config config\strategy.yaml
C:\Users\wuktt\miniconda3\envs\minner\python.exe main.py --mode LIVE --config config\strategy.yaml
```

## Smoke Backtest

```powershell
C:\Users\wuktt\miniconda3\envs\minner\python.exe main.py --mode BACKTEST --config config\smoke_dual_core.yaml --run-id smoke_cta
```

## Cleanup Preview

```powershell
C:\Users\wuktt\miniconda3\envs\minner\python.exe scripts\prune_local_artifacts.py --dry-run
```

## Test

```powershell
C:\Users\wuktt\miniconda3\envs\minner\python.exe -m unittest discover -s tests -p "test_*.py" -v
```
