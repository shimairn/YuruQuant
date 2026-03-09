from __future__ import annotations

from dataclasses import dataclass

from yuruquant.app.config_schema import AppConfig
from yuruquant.core.models import EntrySignal, RuntimeState
from yuruquant.portfolio.armed_exposure import armed_risk_base_equity, entry_signal_risk_amount, position_risk_amount


@dataclass(frozen=True)
class ClusterExposureDetail:
    cluster_name: str
    current_armed_risk_ratio: float
    proposed_armed_risk_ratio: float
    current_same_direction_positions: int
    proposed_same_direction_positions: int


@dataclass(frozen=True)
class ClusterRiskCheck:
    breached: bool
    max_cluster_armed_risk_ratio: float
    max_same_direction_cluster_positions: int
    details: tuple[ClusterExposureDetail, ...]
    breach_reasons: tuple[str, ...]


def cluster_names_for_symbol(config: AppConfig, csymbol: str) -> tuple[str, ...]:
    return tuple(name for name, members in config.universe.risk_clusters.items() if csymbol in members)


def current_cluster_armed_risk_ratio(config: AppConfig, runtime: RuntimeState, cluster_name: str) -> float:
    members = set(config.universe.risk_clusters.get(cluster_name, ()))
    if not members:
        return 0.0
    equity = armed_risk_base_equity(config, runtime)
    if equity <= 0:
        return 0.0

    total_risk = 0.0
    for csymbol, state in runtime.states_by_csymbol.items():
        if csymbol not in members:
            continue
        if state.position is not None and state.position.phase == 'armed':
            total_risk += position_risk_amount(config, csymbol, state.position)
            continue
        if state.position is None and isinstance(state.pending_signal, EntrySignal):
            total_risk += entry_signal_risk_amount(config, csymbol, state.pending_signal)
    return total_risk / equity


def current_same_direction_cluster_positions(config: AppConfig, runtime: RuntimeState, cluster_name: str, direction: int) -> int:
    members = set(config.universe.risk_clusters.get(cluster_name, ()))
    if not members:
        return 0

    count = 0
    for csymbol, state in runtime.states_by_csymbol.items():
        if csymbol not in members:
            continue
        if state.position is not None and int(state.position.direction) == int(direction):
            count += 1
            continue
        if state.position is None and isinstance(state.pending_signal, EntrySignal) and int(state.pending_signal.direction) == int(direction):
            count += 1
    return count


def check_entry_against_cluster_risk(config: AppConfig, runtime: RuntimeState, csymbol: str, signal: EntrySignal) -> ClusterRiskCheck:
    risk_cap = max(float(config.portfolio.max_cluster_armed_risk_ratio), 0.0)
    position_cap = max(int(config.portfolio.max_same_direction_cluster_positions), 0)
    cluster_names = cluster_names_for_symbol(config, csymbol)

    if not cluster_names or (risk_cap <= 0 and position_cap <= 0):
        return ClusterRiskCheck(
            breached=False,
            max_cluster_armed_risk_ratio=risk_cap,
            max_same_direction_cluster_positions=position_cap,
            details=tuple(),
            breach_reasons=tuple(),
        )

    equity = armed_risk_base_equity(config, runtime)
    proposed_risk_ratio = (entry_signal_risk_amount(config, csymbol, signal) / equity) if equity > 0 else 0.0
    details: list[ClusterExposureDetail] = []
    breach_reasons: list[str] = []

    for cluster_name in cluster_names:
        current_risk_ratio = current_cluster_armed_risk_ratio(config, runtime, cluster_name)
        current_positions = current_same_direction_cluster_positions(config, runtime, cluster_name, signal.direction)
        detail = ClusterExposureDetail(
            cluster_name=cluster_name,
            current_armed_risk_ratio=current_risk_ratio,
            proposed_armed_risk_ratio=current_risk_ratio + proposed_risk_ratio,
            current_same_direction_positions=current_positions,
            proposed_same_direction_positions=current_positions + 1,
        )
        details.append(detail)

        if risk_cap > 0 and detail.proposed_armed_risk_ratio > risk_cap + 1e-12:
            breach_reasons.append(f'{cluster_name}:cluster_armed_risk_cap')
        if position_cap > 0 and detail.proposed_same_direction_positions > position_cap:
            breach_reasons.append(f'{cluster_name}:same_direction_cluster_positions')

    return ClusterRiskCheck(
        breached=bool(breach_reasons),
        max_cluster_armed_risk_ratio=risk_cap,
        max_same_direction_cluster_positions=position_cap,
        details=tuple(details),
        breach_reasons=tuple(breach_reasons),
    )


__all__ = [
    'ClusterExposureDetail',
    'ClusterRiskCheck',
    'check_entry_against_cluster_risk',
    'cluster_names_for_symbol',
    'current_cluster_armed_risk_ratio',
    'current_same_direction_cluster_positions',
]
