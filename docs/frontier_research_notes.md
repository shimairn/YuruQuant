# Frontier Research Notes

Date baseline: 2026-03-09

This note translates external CTA literature into the next practical agenda for the live GM-only China futures stack.

## Primary Research Inputs

- [Moskowitz, Ooi, Pedersen: Time Series Momentum](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2554010)
  - Supports keeping the mainline anchored on trend-following and sign-consistent cross-market exposure.
- [Hurst, Ooi, Pedersen: A Century of Evidence on Trend-Following Investing](https://www.aqr.com/Insights/Research/Journal-Article/A-Century-of-Evidence-on-Trend-Following-Investing)
  - Reinforces that the long-run edge comes from diversified trend capture, not from one narrow entry variant.
- [Moreira, Muir: Volatility-Managed Portfolios](https://www.nber.org/papers/w22208)
  - Strong evidence that risk scaling can improve payoff quality when volatility changes faster than expected returns.
- [Lopez de Prado: Building Diversified Portfolios that Outperform Out of Sample](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2708678)
  - Correlation structure should drive diversification design more than hand-written sector labels alone.
- [Zhang, Zohren, Roberts: Deep Momentum Networks](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3850084)
  - Frontier direction is state-aware sizing and turnover-aware signal control, but this repository should borrow diagnostics and sizing ideas before adding model complexity.
- [Chibane, Hallin, Peeters: Slow Momentum With Fast Reversion Around Turning Points](https://academic.oup.com/jfec/article/22/4/1128/7603138)
  - Trend systems are most fragile around turning points; reversal detection matters most when the baseline is already in drawdown.
- [Ait-Sahalia, Hurlin, Perignon, Ravanelli: Sharpe Ratio Timing with Stop-Loss Strategies](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=5511318)
  - Recent evidence favors combining risk timing with stop logic rather than treating stop-loss as a standalone cure.

## What This Means for YuruQuant

The repository should remain a GM-only, mid-frequency China futures CTA stack. The frontier upgrade path is not to turn it into a generic platform or a deep-learning lab.

The literature points to four priorities:

1. Trend core stays intact.
   - `trend_identity` remains the mainline.
   - Avoid replacing the system with fast mean reversion or intraday-only logic.

2. Portfolio control should become state-aware, not only threshold-based.
   - Static cluster caps are too blunt as a first-line answer.
   - Drawdown and volatility state should decide whether risk is cut to zero, partially restored, or kept normal.

3. Correlation and concentration research should precede new caps.
   - Hand-labeled clusters are useful priors.
   - Promotion-grade controls should eventually be justified by observed co-movement and concentration diagnostics.

4. Recovery design is now a first-order issue.
   - A hard drawdown halt can freeze a trend program for too long after an early adverse episode.
   - Before changing entry or exit micro-parameters again, the stack needs explicit evidence on lockout duration, recovery failure, and post-halt opportunity cost.

## Local Evidence from the Current Top20 Baseline

Current canonical Top20 findings:

- `reports/liquid_top20_dual_core_20260309/reconciliation.csv`
  - GM net profit remains the primary truth and diverges materially from reconstructed trade PnL.
- `reports/liquid_top20_dual_core_20260309/cluster_pressure_summary.csv`
  - `54` halt days, but only `2` halt days with day-active positions.
  - The main issue is not sustained active crowding after the halt.
- `reports/liquid_top20_dual_core_20260309/halt_recovery_summary.csv`
  - The run falls into an extended drawdown stall after the first halt episode, with `52` lockout halt days and a `53`-day halt streak.
- `reports/top20_drawdown_recovery_v1/summary_research.csv`
  - relaxing `max_drawdown_halt_ratio` from `0.15` to `0.18` or `0.20` does not reduce lockout halt days or the maximum halt streak under the current runtime path.
  - reducing `risk_per_trade_ratio` to `0.010` without redesigning the halt logic also fails to break the stall.
  - the rerun control profile does not reproduce the archived `reports/liquid_top20_dual_core_20260309` result exactly, so archived baselines should be treated as historical references, not as bitwise-reproducible truth under the current repo state.

This means the next research step should not be another entry sweep or another static cluster-cap sweep.

## Frontier Agenda for This Repo

Near-term:

- keep GM truth reconciliation mandatory
- keep cluster-pressure diagnostics mandatory
- keep halt-recovery diagnostics mandatory
- research graduated drawdown response instead of binary permanent lockout

Medium-term:

- add rolling concentration and correlation diagnostics before promoting new cluster maps
- redesign profit realization around hold quality and recovery preservation
- test state-aware risk multipliers that remain compatible with `next_bar_open` and current GM runtime contracts

Out of scope for now:

- multi-broker expansion
- deep learning in live runtime
- broad hyper-parameter grid searches before portfolio-state redesign
