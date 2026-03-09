# Strategy Doctrine

## System Identity

YuruQuant is a GM-only China futures CTA research and execution stack.

- Mainline: `trend_identity`
  - Market focus: liquid domestic commodity, black, and energy futures
  - Holding horizon: hours to multi-session trend participation
  - Objective: controlled trend exposure with explicit portfolio guards
- Secondary line: `intraday_flat`
  - Purpose: validate whether a day-flat branch deserves its own future program
  - Constraint: never redefine the mainline identity or live defaults

This repository is not a generic quant platform and is not a multi-broker framework.

## Truth Priority

The project uses a fixed truth hierarchy:

1. GM platform equity and `portfolio_daily.csv`
   - canonical PnL truth
   - canonical drawdown truth
   - canonical halt truth
2. `executions.csv` and `signals.csv`
   - execution intent and accepted-fill truth
   - signal timing and fill-gap truth
3. local trade reconstruction and diagnostics
   - campaign pairing, stop overshoot, trigger mix, hold-length diagnostics
   - never the primary source for final net return claims

Any research note or decision report that conflicts with the GM equity ledger must defer to the GM ledger.

## Scope Boundaries

In scope:

- GM backtest and live execution
- China futures CTA research
- execution diagnostics
- portfolio-level risk control
- branch studies that still respect the mainline identity

Out of scope:

- other brokers
- equities, options, or crypto product expansion
- hyper-parameter sweeps without prior truth reconciliation
- claiming performance quality from local trade reconstruction alone

## Mainline and Secondary Responsibilities

Mainline requirements:

- keep the GM-only runtime stable
- preserve `next_bar_open` execution semantics unless a dedicated execution project replaces it
- prioritize portfolio exposure and correlation control over entry micro-tuning
- treat `trend_identity` as the only live-default direction

Secondary line requirements:

- remain research-only
- use separate gate criteria from the mainline
- never overwrite mainline defaults just because a short-horizon run looks better

## Promotion Gates

A branch or runtime change is eligible for promotion only when all of the following hold on GM truth:

- positive net return on the evaluated baseline
- drawdown remains inside the declared budget
- halt behavior is materially reduced or justified
- symbol concentration is not excessive

If these gates fail, the result stays in research only.

## GM Operating Constraints

- `gm==3.0.183` is the pinned SDK baseline in this repository.
- GM entrypoint file naming stays stable because the runner contract depends on it.
- Backtest and live must share the same adapter contract.
- Any future SDK upgrade needs a regression pass against canonical report roots before being accepted.
