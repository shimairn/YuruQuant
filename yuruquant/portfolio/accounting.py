from __future__ import annotations

from yuruquant.app.config_schema import AppConfig
from yuruquant.core.frames import SymbolFrames
from yuruquant.core.models import PortfolioSnapshot, RuntimeState
from yuruquant.strategy.trend_breakout.exit_state import compute_exit_pnl


def modeled_portfolio_snapshot(config: AppConfig, runtime: RuntimeState, fallback_cash: float) -> PortfolioSnapshot:
    portfolio = runtime.portfolio
    equity = float(portfolio.initial_equity or fallback_cash or 0.0) + float(portfolio.realized_pnl)
    for state in runtime.states_by_csymbol.values():
        position = state.position
        if position is None or not state.main_symbol:
            continue
        frames = runtime.bar_store.get(state.main_symbol)
        current_price = position.entry_price
        if isinstance(frames, SymbolFrames) and not frames.entry.frame.empty_frame:
            current_price = frames.entry.frame.latest_close() or position.entry_price
        spec = config.universe.instrument_overrides.get(state.csymbol, config.universe.instrument_defaults)
        _, net = compute_exit_pnl(position, current_price, spec.multiplier)
        equity += net
    return PortfolioSnapshot(equity=equity, cash=equity)
