# Dual-Core Trend Breakout V3 Research Summary

## AI Snapshot

```yaml
system_name: dual_core_trend_breakout_v3
repo: C:/Users/wuktt/Application/量化
last_updated: 2026-03-08
canonical_research_config: C:/Users/wuktt/Application/量化/config/liquid_top10_dual_core.yaml
canonical_execution_semantics: next_bar_open
current_research_baseline:
  universe: [SHFE.AG, DCE.V, CZCE.TA, DCE.JM, DCE.M, CZCE.MA, DCE.EG, DCE.P, DCE.PP, DCE.L]
  macro_filter: 1h SMA60 + 1h MACD histogram
  trigger: 5m Donchian(36) + 0.30 * ATR14 breakout buffer
  width_filter: channel_width > 0.5 * ATR14_5m
  close_position_filter: removed
  hard_stop_atr: 2.2
  protected_activate_r: 1.8
  ascended_activate_r: 2.0
  risk_per_trade_ratio: 0.015
  max_total_armed_risk_ratio: 0.0
  session_end_buffer_bars: 0
  entry_block_major_gap_bars: 0
  armed_flush_buffer_bars: 0
  armed_flush_min_gap_minutes: 180
current_code_status:
  armed_flush: implemented_but_disabled
  major_gap_entry_blocker: implemented_but_disabled
  max_total_armed_risk_ratio: implemented_but_disabled
  config_module_refactor: completed
  reporting_module_refactor: completed
current_main_problem:
  no_hourly_ma_stop_realization_under_10_symbol_research_runs
  protected_stop_and_portfolio_halt_still_dominate_exits
  session_restart_gap_and_concurrent_armed_exposure_both_matter
best_post_diagnostic_defensive_result:
  experiment: ArmedRiskCap_3p0
  portfolio_halt_count: 1
  ascended_exit_count: 3
  hourly_ma_stop_count: 0
  return_pct: -15.38
  max_drawdown_pct: 15.38
recommended_interpretation:
  keep_entry_logic_core
  keep_MACD_filter
  keep_close_position_filter_removed
  continue_research_from_post-diagnostic baseline
```

---

## 1. Purpose

This document is the current strategy and research handoff for the Dual-Core Trend Breakout system. It is written to be AI-readable and engineering-readable.

It covers:

- the current strategy specification
- execution semantics and risk model
- implementation boundaries in code
- the experiment ledger from V2 to V3 research
- the conclusions that are already reliable
- the current recommended next direction

---

## 2. System Definition

### 2.1 Strategy identity

This is a **cross-period trend-following breakout system** with the following philosophy:

- use **5m** bars for cheap trial entries
- use **1h** bars for directional context
- cap per-trade loss tightly
- allow only trades aligned with higher-timeframe trend
- use a three-state lifecycle for position management

### 2.2 Core design goals

- keep the signal engine minimal
- reuse existing indicators instead of adding complex factor logic
- isolate broker and execution semantics from strategy logic
- keep portfolio risk control explicit and mechanical
- produce diagnosis-friendly reports for every backtest run

---

## 3. Current Strategy Spec

### 3.1 Macro environment filter

Timeframe: `1h`

Long environment:

- `close_1h > SMA60_1h`
- `MACD_histogram_1h > 0`

Short environment:

- `close_1h < SMA60_1h`
- `MACD_histogram_1h < 0`

Implementation:

- [environment.py](/C:/Users/wuktt/Application/量化/yuruquant/strategy/trend_breakout/environment.py)

### 3.2 Micro trigger

Timeframe: `5m`

A trade is eligible only if all of the following hold:

- macro environment already points in that direction
- `Donchian(36)` breakout is present
- breakout distance is greater than `0.30 * ATR14_5m`
- channel width is greater than `0.5 * ATR14_5m`
- session rules allow entry

Long trigger:

- `close_5m > DonchianUpper + 0.30 * ATR14`

Short trigger:

- `close_5m < DonchianLower - 0.30 * ATR14`

Important note:

- the old `breakout_close_position_min` filter has been **deleted**, not merely disabled
- this is a deliberate clean removal because B1 proved it added no filtering power in real samples

Implementation:

- [entry_rules.py:64](/C:/Users/wuktt/Application/量化/yuruquant/strategy/trend_breakout/entry_rules.py:64)

### 3.3 Position sizing and per-trade risk

Per-trade risk budget:

- `risk_per_trade_ratio = 1.5%`

Current formula:

```text
PositionQty = floor( Equity * RiskRatio / (HardStopDistance * Multiplier) )
HardStopDistance = 2.2 * ATR14_5m
```

Additional notes:

- quantity is normalized by `min_lot` and `lot_step`
- the system uses volatility-normalized risk, not notional value sizing
- this is the mechanical basis for risk parity across instruments

Implementation:

- [risk_model.py](/C:/Users/wuktt/Application/量化/yuruquant/strategy/trend_breakout/risk_model.py)

### 3.4 Exit state machine

The trade lifecycle is:

- `armed`
- `protected`
- `ascended`

#### `armed`

- initial hard stop at `2.2 * ATR14_5m`
- this is the full-risk trial state

#### `protected`

Activation:

- when `MFE >= 1.8R` in current 10-symbol research baseline

Action:

- stop is lifted to a protected floor at `entry price + cost compensation` for longs
- or `entry price - cost compensation` for shorts

#### `ascended`

Activation:

- when `MFE >= 2.0R` in current 10-symbol research baseline

Action:

- the protected floor remains active as a disaster floor
- the intended trend exit becomes higher-timeframe reversal logic

Primary ascended exit rule:

- long: `1h close < 1h SMA60`
- short: `1h close > 1h SMA60`

Important implementation note:

- `MFE` is computed from `5m high/low` extremes, not just close
- this was changed intentionally because state transitions are excursion-based

Implementation:

- [exit_state.py:120](/C:/Users/wuktt/Application/量化/yuruquant/strategy/trend_breakout/exit_state.py:120)
- activation thresholds applied at:
  - [exit_state.py:59](/C:/Users/wuktt/Application/量化/yuruquant/strategy/trend_breakout/exit_state.py:59)
  - [exit_state.py:63](/C:/Users/wuktt/Application/量化/yuruquant/strategy/trend_breakout/exit_state.py:63)

---

## 4. Execution Semantics

### 4.1 Fill model

Execution is intentionally fixed to:

- `next_bar_open`

This means:

- signal is generated on bar `T`
- actual execution occurs at the **next 5m bar open**

This semantic is preserved throughout all experiments. It has not been “fixed away”. Instead, the project now measures its consequences explicitly.

Implementation:

- [engine.py:124](/C:/Users/wuktt/Application/量化/yuruquant/core/engine.py:124)

### 4.2 Why this matters

This semantic is the main source of physical execution distortion in Chinese commodity futures around:

- 09:00 day open
- 10:30 restart after short break
- 13:30 afternoon restart
- 21:00 night open
- other session restart points depending on symbol

The project now records:

- `signal_ts`
- `fill_ts`
- `signal_price`
- `fill_price`
- `execution_regime`
- `fill_gap_points`
- `fill_gap_atr`

So overshoot is now measurable rather than guessed.

---

## 5. Portfolio Risk Controls

### 5.1 Existing portfolio guard

Portfolio-level guard rails:

- `max_daily_loss_ratio = 5%`
- `max_drawdown_halt_ratio = 15%`

If breached:

- new entries are blocked
- forced flattening is allowed

Implementation:

- [risk.py:7](/C:/Users/wuktt/Application/量化/yuruquant/portfolio/risk.py:7)

### 5.2 New but currently disabled portfolio armed exposure cap

Implemented field:

- `portfolio.max_total_armed_risk_ratio`

Meaning:

- sum the theoretical risk of all current `armed` positions
- also include queued but not-yet-filled `EntrySignal`s
- if a new entry would push total armed risk above the cap, reject the new entry

This is implemented but currently disabled in research baseline with:

- `max_total_armed_risk_ratio: 0.0`

Implementation:

- [armed_exposure.py:50](/C:/Users/wuktt/Application/量化/yuruquant/portfolio/armed_exposure.py:50)

### 5.3 Why queued entries are included

Without counting pending entries, the system can still accept multiple entries on the same 5m cycle and exceed the intended naked risk cap on the next open. The implementation explicitly prevents that loophole.

---

## 6. Current Research Configs

### 6.1 Current 10-symbol research baseline

File:

- [liquid_top10_dual_core.yaml](/C:/Users/wuktt/Application/量化/config/liquid_top10_dual_core.yaml)

Key values:

- `ma_period = 60`
- `macd_fast = 12`
- `macd_slow = 26`
- `macd_signal = 9`
- `donchian_lookback = 36`
- `min_channel_width_atr = 0.5`
- `breakout_atr_buffer = 0.30`
- `hard_stop_atr = 2.2`
- `protected_activate_r = 1.8`
- `ascended_activate_r = 2.0`
- `risk_per_trade_ratio = 0.015`
- `max_total_armed_risk_ratio = 0.0`
- `entry_block_major_gap_bars = 0`
- `armed_flush_buffer_bars = 0`

Universe:

- `SHFE.AG`
- `DCE.V`
- `CZCE.TA`
- `DCE.JM`
- `DCE.M`
- `CZCE.MA`
- `DCE.EG`
- `DCE.P`
- `DCE.PP`
- `DCE.L`

### 6.2 Global generic default config

File:

- [strategy.yaml](/C:/Users/wuktt/Application/量化/config/strategy.yaml)

This remains more conservative and generic, including:

- `protected_activate_r = 1.2`
- `ascended_activate_r = 2.5`

So there is a difference between:

- **generic project defaults**
- **current 10-symbol research baseline**

---

## 7. Code Architecture After Refactor

### 7.1 Strategy layer

- environment: [environment.py](/C:/Users/wuktt/Application/量化/yuruquant/strategy/trend_breakout/environment.py)
- entry rules: [entry_rules.py](/C:/Users/wuktt/Application/量化/yuruquant/strategy/trend_breakout/entry_rules.py)
- exit state machine: [exit_state.py](/C:/Users/wuktt/Application/量化/yuruquant/strategy/trend_breakout/exit_state.py)
- session logic: [session_windows.py](/C:/Users/wuktt/Application/量化/yuruquant/strategy/trend_breakout/session_windows.py)

### 7.2 Core orchestration

- execution and bar processing: [engine.py](/C:/Users/wuktt/Application/量化/yuruquant/core/engine.py)

### 7.3 Portfolio layer

- portfolio guard: [risk.py](/C:/Users/wuktt/Application/量化/yuruquant/portfolio/risk.py)
- MTM accounting: [accounting.py](/C:/Users/wuktt/Application/量化/yuruquant/portfolio/accounting.py)
- armed exposure control: [armed_exposure.py](/C:/Users/wuktt/Application/量化/yuruquant/portfolio/armed_exposure.py)

### 7.4 Config layer

- schema dataclasses: [config_schema.py](/C:/Users/wuktt/Application/量化/yuruquant/app/config_schema.py)
- defaults and key contracts: [config_defaults.py](/C:/Users/wuktt/Application/量化/yuruquant/app/config_defaults.py)
- validation helpers: [config_validation.py](/C:/Users/wuktt/Application/量化/yuruquant/app/config_validation.py)
- loader: [config_loader.py](/C:/Users/wuktt/Application/量化/yuruquant/app/config_loader.py)
- stable facade: [config.py](/C:/Users/wuktt/Application/量化/yuruquant/app/config.py)

### 7.5 Reporting layer

- trade record parsing: [trade_records.py](/C:/Users/wuktt/Application/量化/yuruquant/reporting/trade_records.py)
- diagnostics generation: [diagnostics.py](/C:/Users/wuktt/Application/量化/yuruquant/reporting/diagnostics.py)
- summary calculations: [summary.py](/C:/Users/wuktt/Application/量化/yuruquant/reporting/summary.py)
- stable facade: [analysis.py](/C:/Users/wuktt/Application/量化/yuruquant/reporting/analysis.py)

This split is important because future work is now easier to place cleanly.

---

## 8. Reporting Products

Each fully diagnosed run can produce:

- `signals.csv`
- `executions.csv`
- `portfolio_daily.csv`
- `trade_diagnostics.csv`
- `summary.csv`

### 8.1 Key diagnostic fields now available

Signals / executions / diagnostics collectively expose:

- `phase`
- `mfe_r`
- `protected_stop_price`
- `signal_ts`
- `fill_ts`
- `signal_price`
- `fill_price`
- `execution_regime`
- `fill_gap_points`
- `fill_gap_atr`
- `theoretical_stop_price`
- `theoretical_stop_gross_pnl`
- `actual_gross_pnl`
- `overshoot_pnl`
- `overshoot_ratio`

This is what allowed the project to move from “strategy feels wrong” to “exact execution pathology is measurable”.

---

## 9. Experiment Ledger

## 9.1 Historical context: prior 2-symbol baseline

From earlier validated conversation context, the earlier V2-style baseline achieved:

- start equity: `500,000`
- end equity: `577,000`
- total trades: `29`
- halt days: `0`
- max drawdown: `14.75%`

This was important because it established that the core risk budgeting logic was sound before the 10-symbol execution pathologies were fully exposed.

## 9.2 Important comparability note

Not all experiments are equally comparable.

Use the following rule:

- **pre-scoreboard-fix experiments** are useful mainly for **logical ablation conclusions**
- **post-diagnostic / post-accounting-fix experiments** are the canonical reference for current system behavior

For current decision making, the canonical benchmark is:

- [exec_diag_baseline_top10_v2/summary.csv](/C:/Users/wuktt/Application/量化/reports/exec_diag_baseline_top10_v2/summary.csv)

---

## 10. Strategy Ablation Results

### 10.1 A-direction bridge experiment

Goal:

- test whether the `1.2R -> 2.5R` death valley was caused by bridge thresholds alone

Grid:

- `protected_activate_r`: `[1.5, 1.8]`
- `ascended_activate_r`: `[2.0, 2.2, 2.5]`

Key result rows:

| Experiment | Protected R | Ascended R | Trades | Hard Stop | Protected Stop | Ascended Exit | Portfolio Halt | Return % | Max DD % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| A_grid_p15_a20 | 1.5 | 2.0 | 12 | 3 | 5 | 3 | 4 | -18.04 | 18.04 |
| A_grid_p18_a20 | 1.8 | 2.0 | 13 | 6 | 6 | 4 | 1 | -15.09 | 15.09 |

Observed conclusion:

- lowering `ascended_activate_r` to `2.0` and raising `protected_activate_r` to `1.8` materially improved bridge survival versus more conservative bridge settings
- this is why `p18_a20` became the ongoing research baseline for the 10-symbol program

Artifact:

- [grid_a_bridge_top10_3m/summary.csv](/C:/Users/wuktt/Application/量化/reports/grid_a_bridge_top10_3m/summary.csv)

### 10.2 B1: remove `close_position` filter

Hypothesis:

- maybe the single-candle close-location requirement was forcing climax entries

Result:

- **null result**
- `B1_no_close_position` was identical to `A_grid_p18_a20`

Interpretation:

- the filter was not an active bottleneck under real samples
- it was deleted cleanly from code

Artifact:

- [b1_top10_no_close_position_grid/summary.csv](/C:/Users/wuktt/Application/量化/reports/b1_top10_no_close_position_grid/summary.csv)

### 10.3 B2: `SMA60 -> SMA20`

Hypothesis:

- maybe macro timing lag from `SMA60` was causing late entries

Result:

- **null result** again
- `B2_sma20` was identical to `B1`

Interpretation:

- under the current 5m breakout regime, changing macro MA speed alone did not alter the practical sample set
- this strongly suggested the macro MA was not the dominant bottleneck at that stage

Artifact:

- [b2_top10_sma20_grid/summary.csv](/C:/Users/wuktt/Application/量化/reports/b2_top10_sma20_grid/summary.csv)

### 10.4 B3: remove MACD confirmation

Hypothesis:

- maybe MACD confirmation was blocking the earliest trend ignition points

Result:

| Experiment | Trades | Hard Stop | Protected Stop | Ascended Exit | Portfolio Halt | Return % | Max DD % |
|---|---:|---:|---:|---:|---:|---:|---:|
| B3_sma20_no_macd | 29 | 15 | 5 | 2 | 9 | -41.36 | 48.51 |

Interpretation:

- this was decisively negative
- trade count exploded, losses expanded, and drawdown became unacceptable
- MACD is not dead weight; it is a useful protective filter in this system

Artifact:

- [b3_top10_sma20_no_macd_grid/summary.csv](/C:/Users/wuktt/Application/量化/reports/b3_top10_sma20_no_macd_grid/summary.csv)

### 10.5 Reliable logic conclusions from B-series

These are now considered stable conclusions:

- keep `1h SMA60 + MACD histogram`
- keep the `close_position` filter removed
- do not blame macro MA period alone for the bridge problem
- do not remove MACD from the environment filter

---

## 11. Execution and Defense Experiments

These are the most important experiments for **current** decision making.

### 11.1 Canonical post-diagnostic baseline

| Experiment | Trades | Portfolio Halt | Gap Portfolio Halt | Ascended Exit | Hourly MA Stop | Return % | Max DD % |
|---|---:|---:|---:|---:|---:|---:|---:|
| ExecDiag_Baseline | 8 | 8 | 3 | 0 | 0 | -15.28 | 15.30 |

Key observation:

- all 8 trades died via `portfolio_halt`
- no strategy-level `hard_stop` or `protected_stop` dominated the exit distribution in this canonical benchmark
- this proved that the real bottleneck had become **portfolio-level gap/correlation damage**, not just local trade management

Artifact:

- [exec_diag_baseline_top10_v2/summary.csv](/C:/Users/wuktt/Application/量化/reports/exec_diag_baseline_top10_v2/summary.csv)
- [exec_diag_baseline_top10_v2/p18_a20/trade_diagnostics.csv](/C:/Users/wuktt/Application/量化/reports/exec_diag_baseline_top10_v2/p18_a20/trade_diagnostics.csv)

### 11.2 Armed flush only

Goal:

- flatten naked `armed` positions before major session gaps

Result:

| Experiment | Trades | Armed Flush | Portfolio Halt | Return % | Max DD % |
|---|---:|---:|---:|---:|---:|
| ArmedFlush | 11 | 3 | 7 | -17.16 | 17.16 |

Conclusion:

- it removed some specific risk, but not enough
- it does not protect against last-bar signals that become new positions at the next major open

Artifact:

- [armed_flush_baseline_top10_v1/summary.csv](/C:/Users/wuktt/Application/量化/reports/armed_flush_baseline_top10_v1/summary.csv)

### 11.3 Full shield: armed flush + major-gap entry blocker

Goal:

- defend both old naked positions and new last-bar entries before major gaps

Result:

| Experiment | Trades | Armed Flush | Portfolio Halt | Return % | Max DD % |
|---|---:|---:|---:|---:|---:|
| FullShield | 11 | 3 | 7 | -16.80 | 16.80 |

What it did prove:

- the major-gap entry blocker works mechanically
- it successfully removed the known problematic last-bar gap entry pattern

What it did **not** solve:

- total portfolio halts remained high
- this shifted the diagnosis toward **concurrent armed exposure / correlation spike**, not merely session-boundary entry timing

Artifact:

- [full_shield_top10_v1/summary.csv](/C:/Users/wuktt/Application/量化/reports/full_shield_top10_v1/summary.csv)

### 11.4 Armed risk cap at 4.5%

Goal:

- cap total concurrent naked risk around 3 full-sized armed trials

Result:

| Experiment | Trades | Portfolio Halt | Ascended Exit | Return % | Max DD % |
|---|---:|---:|---:|---:|---:|
| ArmedRiskCap_4p5 | 10 | 9 | 0 | -17.20 | 17.20 |

Conclusion:

- too loose
- did not improve the system

Artifact:

- [armed_risk_cap_top10_v1/summary.csv](/C:/Users/wuktt/Application/量化/reports/armed_risk_cap_top10_v1/summary.csv)

### 11.5 Armed risk cap at 3.0%

Goal:

- cap total concurrent naked risk around 2 full-sized armed trials

Result:

| Experiment | Trades | Hard Stop | Protected Stop | Ascended Exit | Ascended Protected Stop | Portfolio Halt | Hourly MA Stop | Return % | Max DD % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ArmedRiskCap_3p0 | 10 | 3 | 6 | 3 | 3 | 1 | 0 | -15.38 | 15.38 |

This is the most important defense experiment so far.

What changed materially:

- `portfolio_halt_count` dropped from `8` to `1`
- `ascended_exit_count` rose from `0` to `3`
- the system stopped dying almost entirely through portfolio-level halts
- failure mode shifted back toward strategy-level exits

What still did not happen:

- `hourly_ma_stop_count` stayed at `0`
- ascended trades still fell back to `protected_stop`
- stop overshoot remained significant

Artifacts:

- [armed_risk_cap_top10_cap030_v1/summary.csv](/C:/Users/wuktt/Application/量化/reports/armed_risk_cap_top10_cap030_v1/summary.csv)
- [armed_risk_cap_top10_cap030_v1/p18_a20/trade_diagnostics.csv](/C:/Users/wuktt/Application/量化/reports/armed_risk_cap_top10_cap030_v1/p18_a20/trade_diagnostics.csv)

Example ascended fallback trades in this run:

| Campaign | Symbol | Exit Trigger | Phase at Exit | Overshoot PnL |
|---|---|---|---|---:|
| `DCE.V-20251211_141000` | `DCE.V` | `protected_stop` | `ascended` | 13857.84 |
| `SHFE.AG-20251211_141000` | `SHFE.AG` | `protected_stop` | `ascended` | 1715.25 |
| `DCE.P-20251215_143500` | `DCE.P` | `protected_stop` | `ascended` | 1786.32 |

This proves the bridge is no longer completely broken, but the system still does not convert ascended trades into true `hourly_ma_stop` exits.

---

## 12. Unified Experiment Table

| Experiment | Trades | Win Rate % | Hard Stop | Protected Stop | Hourly MA Stop | Ascended Exit | Ascended Protected | Armed Flush | Portfolio Halt | Gap Halt | Return % | Max DD % | End Equity |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| A_grid_p15_a20 | 12 | 58.33 | 3 | 5 | 0 | 3 | 2 | 0 | 4 | 0 | -18.04 | 18.04 | 409818.09 |
| A_grid_p18_a20 | 13 | 46.15 | 6 | 6 | 0 | 4 | 3 | 0 | 1 | 0 | -15.09 | 15.09 | 424546.09 |
| B1_no_close_position | 13 | 46.15 | 6 | 6 | 0 | 4 | 3 | 0 | 1 | 0 | -15.09 | 15.09 | 424546.09 |
| B2_sma20 | 13 | 46.15 | 6 | 6 | 0 | 4 | 3 | 0 | 1 | 0 | -15.09 | 15.09 | 424546.09 |
| B3_sma20_no_macd | 29 | 37.93 | 15 | 5 | 0 | 2 | 2 | 0 | 9 | 0 | -41.36 | 48.51 | 293223.19 |
| ExecDiag_Baseline | 8 | 62.50 | 0 | 0 | 0 | 0 | 0 | 0 | 8 | 3 | -15.28 | 15.30 | 423577.30 |
| ArmedFlush | 11 | 72.73 | 0 | 1 | 0 | 0 | 0 | 3 | 7 | 0 | -17.16 | 17.16 | 414182.18 |
| FullShield | 11 | 63.64 | 0 | 1 | 0 | 0 | 0 | 3 | 7 | 0 | -16.80 | 16.80 | 416015.08 |
| ArmedRiskCap_4p5 | 10 | 60.00 | 0 | 1 | 0 | 0 | 0 | 0 | 9 | 3 | -17.20 | 17.20 | 413990.63 |
| ArmedRiskCap_3p0 | 10 | 50.00 | 3 | 6 | 0 | 3 | 3 | 0 | 1 | 0 | -15.38 | 15.38 | 423106.02 |

---

## 13. What Is Now Considered Proven

### 13.1 Proven about entry logic

- keep `1h SMA60 + MACD histogram`
- removing `close_position` filter was correct
- replacing `SMA60` with `SMA20` alone did not improve actual samples
- removing MACD is harmful

### 13.2 Proven about execution

- `next_bar_open` is a major source of slippage and stop overshoot around session restarts
- this effect is structural, not anecdotal
- the project can now quantify it via diagnostics instead of inferring it indirectly

### 13.3 Proven about portfolio behavior

- concurrent naked `armed` exposure matters materially
- major-gap entry blocking and armed flush help only partially
- a tighter portfolio-level armed risk cap changes the failure mode in a meaningful way

### 13.4 Proven about the remaining bottleneck

The remaining bottleneck is **not** “no trend detection at all”.

The remaining bottleneck is:

- trades can reach `ascended`
- but they still tend to revert to `protected_stop`
- they rarely survive long enough to realize `hourly_ma_stop`

So the system is now able to **bridge into ascended state in some cases**, but not yet to **monetize ascended state through the intended higher-timeframe exit**.

---

## 14. Current Recommended Baseline for Further Research

### 14.1 Logic baseline to keep

Keep these fixed unless a future experiment explicitly proves otherwise:

- `1h SMA60 + 1h MACD histogram`
- `5m Donchian(36)`
- `0.30 * ATR14` breakout buffer
- `channel_width > 0.5 * ATR14`
- no `close_position` filter
- `hard_stop_atr = 2.2`
- `next_bar_open`

### 14.2 Research baseline config

Use:

- [liquid_top10_dual_core.yaml](/C:/Users/wuktt/Application/量化/config/liquid_top10_dual_core.yaml)

Current research values:

- `protected_activate_r = 1.8`
- `ascended_activate_r = 2.0`

### 14.3 Optional defensive feature flags currently available

Implemented but disabled in baseline:

- `entry_block_major_gap_bars`
- `armed_flush_buffer_bars`
- `max_total_armed_risk_ratio`

These should remain off in the baseline unless a dedicated experiment shows clear outperformance.

---

## 15. Recommended Next Steps

### Option A: small sweep around total armed risk cap

Most justified immediate follow-up:

- test `max_total_armed_risk_ratio` in `[0.025, 0.030, 0.035]`
- keep all other logic fixed

Why:

- `4.5%` was too loose
- `3.0%` materially improved failure structure
- this is the cleanest current continuation

### Option B: redesign post-ascended downside floor behavior

Reason:

- `ascended` exists in samples now, but still reverts to `protected_stop`
- if the goal is genuine higher-timeframe monetization, the protected floor may still be too restrictive after ascension

This is a higher-risk logic change and should come **after** a narrow armed-risk-cap sweep, not before.

### Option C: session-restart-aware stop semantics

Reason:

- overshoot remains materially large on stop exits
- even after portfolio halts are reduced, stop execution quality remains a structural cost center

This is an execution-layer refinement, not a signal-layer refinement.

---

## 16. Canonical Reference Files

### Strategy and config

- [liquid_top10_dual_core.yaml](/C:/Users/wuktt/Application/量化/config/liquid_top10_dual_core.yaml)
- [strategy.yaml](/C:/Users/wuktt/Application/量化/config/strategy.yaml)
- [entry_rules.py](/C:/Users/wuktt/Application/量化/yuruquant/strategy/trend_breakout/entry_rules.py)
- [exit_state.py](/C:/Users/wuktt/Application/量化/yuruquant/strategy/trend_breakout/exit_state.py)
- [engine.py](/C:/Users/wuktt/Application/量化/yuruquant/core/engine.py)
- [risk.py](/C:/Users/wuktt/Application/量化/yuruquant/portfolio/risk.py)
- [armed_exposure.py](/C:/Users/wuktt/Application/量化/yuruquant/portfolio/armed_exposure.py)

### Reports and experiments

- [exec_diag_baseline_top10_v2/summary.csv](/C:/Users/wuktt/Application/量化/reports/exec_diag_baseline_top10_v2/summary.csv)
- [armed_flush_baseline_top10_v1/summary.csv](/C:/Users/wuktt/Application/量化/reports/armed_flush_baseline_top10_v1/summary.csv)
- [full_shield_top10_v1/summary.csv](/C:/Users/wuktt/Application/量化/reports/full_shield_top10_v1/summary.csv)
- [armed_risk_cap_top10_v1/summary.csv](/C:/Users/wuktt/Application/量化/reports/armed_risk_cap_top10_v1/summary.csv)
- [armed_risk_cap_top10_cap030_v1/summary.csv](/C:/Users/wuktt/Application/量化/reports/armed_risk_cap_top10_cap030_v1/summary.csv)
- [grid_a_bridge_top10_3m/summary.csv](/C:/Users/wuktt/Application/量化/reports/grid_a_bridge_top10_3m/summary.csv)
- [b1_top10_no_close_position_grid/summary.csv](/C:/Users/wuktt/Application/量化/reports/b1_top10_no_close_position_grid/summary.csv)
- [b2_top10_sma20_grid/summary.csv](/C:/Users/wuktt/Application/量化/reports/b2_top10_sma20_grid/summary.csv)
- [b3_top10_sma20_no_macd_grid/summary.csv](/C:/Users/wuktt/Application/量化/reports/b3_top10_sma20_no_macd_grid/summary.csv)

---

## 17. Final Bottom Line

If an AI agent continues this project from here, it should assume:

1. the **entry logic core is mostly correct** and should not be casually rewritten
2. the **MACD filter is valuable** and should stay
3. the old close-position filter is already dead and removed
4. the **execution layer and portfolio exposure layer** are the current leverage points
5. the best currently validated structural improvement is a **tighter concurrent armed risk cap**, with `3.0%` being the first meaningful positive structural result
6. the next real objective is not merely “get into ascended”, but “convert ascended trades into actual `hourly_ma_stop` exits”
