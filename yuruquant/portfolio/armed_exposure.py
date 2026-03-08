from __future__ import annotations

from dataclasses import dataclass

from yuruquant.app.config_schema import AppConfig
from yuruquant.core.models import EntrySignal, ManagedPosition, RuntimeState


@dataclass(frozen=True)
class ArmedRiskCheck:
    breached: bool
    current_ratio: float
    proposed_ratio: float
    cap_ratio: float


def _armed_risk_base_equity(config: AppConfig, runtime: RuntimeState) -> float:
    portfolio = runtime.portfolio
    if portfolio.current_equity > 0:
        return float(portfolio.current_equity)
    if portfolio.initial_equity > 0:
        return float(portfolio.initial_equity)
    return float(config.broker.gm.backtest.initial_cash)


def _entry_signal_risk_amount(config: AppConfig, csymbol: str, signal: EntrySignal) -> float:
    spec = config.universe.instrument_overrides.get(csymbol, config.universe.instrument_defaults)
    return abs(float(signal.price) - float(signal.stop_loss)) * float(spec.multiplier) * max(int(signal.qty), 0)


def _position_risk_amount(config: AppConfig, csymbol: str, position: ManagedPosition) -> float:
    spec = config.universe.instrument_overrides.get(csymbol, config.universe.instrument_defaults)
    return abs(float(position.entry_price) - float(position.initial_stop_loss)) * float(spec.multiplier) * max(int(position.qty), 0)


def current_armed_risk_ratio(config: AppConfig, runtime: RuntimeState) -> float:
    equity = _armed_risk_base_equity(config, runtime)
    if equity <= 0:
        return 0.0
    total_risk = 0.0
    for csymbol, state in runtime.states_by_csymbol.items():
        if state.position is not None and state.position.phase == 'armed':
            total_risk += _position_risk_amount(config, csymbol, state.position)
            continue
        if state.position is None and isinstance(state.pending_signal, EntrySignal):
            total_risk += _entry_signal_risk_amount(config, csymbol, state.pending_signal)
    return total_risk / equity


def check_entry_against_armed_risk_cap(config: AppConfig, runtime: RuntimeState, csymbol: str, signal: EntrySignal) -> ArmedRiskCheck:
    cap_ratio = max(float(config.portfolio.max_total_armed_risk_ratio), 0.0)
    if cap_ratio <= 0:
        return ArmedRiskCheck(breached=False, current_ratio=0.0, proposed_ratio=0.0, cap_ratio=cap_ratio)

    equity = _armed_risk_base_equity(config, runtime)
    if equity <= 0:
        return ArmedRiskCheck(breached=False, current_ratio=0.0, proposed_ratio=0.0, cap_ratio=cap_ratio)

    current_ratio = current_armed_risk_ratio(config, runtime)
    proposed_ratio = _entry_signal_risk_amount(config, csymbol, signal) / equity
    breached = current_ratio + proposed_ratio > cap_ratio + 1e-12
    return ArmedRiskCheck(
        breached=breached,
        current_ratio=current_ratio,
        proposed_ratio=proposed_ratio,
        cap_ratio=cap_ratio,
    )
