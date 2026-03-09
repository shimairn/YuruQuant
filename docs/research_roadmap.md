# Research Roadmap

## Canonical Baselines

Canonical report roots are the directories the repository treats as current baselines:

- `reports/liquid_top10_dual_core`
- `reports/liquid_top20_dual_core_20260309`
- `reports/minimal_stable_top10_v2`
- `reports/dual_branch_effectiveness_v3`
- `reports/grid_protected_top10_3m`

Legacy report roots outside this list are disposable local artifacts once the canonical roots exist.

## Phase 1: Truth Alignment and Cleanup

Deliverables:

- current README aligned to the live runtime
- doctrine and roadmap docs
- repo contract test against legacy exit narrative drift
- `scripts/prune_local_artifacts.py`

Acceptance:

- tracked docs match the live two-phase exit logic
- cleanup script defaults to `--dry-run`
- no tracked document reintroduces the removed legacy exit narrative

## Phase 2: GM Truth Reconciliation

Deliverables:

- `yuruquant.reporting.reconciliation`
- `scripts/reconcile_gm_truth.py`
- unit tests for aligned runs, fill-gap divergence, portfolio-halt divergence, and missing-file handling

Acceptance:

- every canonical run can produce a reconciliation report
- reports separate GM equity truth from local structural diagnostics
- decision notes use GM net metrics when making claims about performance

## Phase 3: Portfolio and Cluster Risk Upgrade

Deliverables:

- cluster-aware risk configuration, default off
- portfolio risk module for cluster checks
- `scripts/analyze_cluster_pressure.py` for halt attribution and diversification diagnostics
- tests that include open and pending exposure at the same time

Acceptance:

- default behavior stays backward compatible
- cluster controls can reduce portfolio halts on the Top20 baseline without entry micro-tuning
- if cluster controls fail to promote, cluster-pressure diagnostics must explain whether the bottleneck is active crowding or post-loss lockout before further strategy changes

## Phase 4: Strategy Evolution

Entry condition:

- start only after Phases 1 to 3 are stable

Mainline focus:

- profit realization redesign
- multi-session hold quality
- execution-aware downside control
- halt-recovery diagnostics before any drawdown-control redesign

Secondary focus:

- day-flat branch validation under separate gates

Acceptance:

- no strategy change is promoted unless GM net return, drawdown, halt count, and concentration gates all improve or remain within budget
