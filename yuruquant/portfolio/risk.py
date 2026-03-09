from __future__ import annotations

from yuruquant.core.models import GuardDecision, PortfolioRuntime, PortfolioSnapshot
from yuruquant.core.time import to_exchange_trade_day


def evaluate_portfolio_guard(state: PortfolioRuntime, snapshot: PortfolioSnapshot, max_daily_loss_ratio: float, max_drawdown_halt_ratio: float, trade_time: object, fallback_equity: float) -> GuardDecision:
    equity = float(snapshot.equity or 0.0)
    if equity <= 0:
        equity = float(fallback_equity)
    state.current_equity = equity

    trade_day = to_exchange_trade_day(trade_time)
    if state.initial_equity <= 0:
        state.initial_equity = equity
        state.current_equity = equity
        state.equity_peak = equity
        state.daily_start_equity = equity
        state.current_date = trade_day
        state.drawdown_ratio = 0.0
        state.risk_state = "normal"
        state.effective_risk_mult = 1.0
        state.halt_flag = False
        state.halt_reason = ""
        return GuardDecision(allow_entries=True, force_flatten=False)

    if state.current_date != trade_day:
        state.current_date = trade_day
        state.daily_start_equity = equity
        state.halt_flag = False
        state.halt_reason = ""
        state.risk_state = "normal"
        state.effective_risk_mult = 1.0

    state.equity_peak = max(state.equity_peak, equity)
    state.drawdown_ratio = (state.equity_peak - equity) / state.equity_peak if state.equity_peak > 0 else 0.0

    if state.daily_start_equity > 0:
        daily_loss = (state.daily_start_equity - equity) / state.daily_start_equity
        if daily_loss >= max_daily_loss_ratio:
            state.risk_state = "halt_daily_loss"
            state.effective_risk_mult = 0.0
            state.halt_flag = True
            state.halt_reason = f"daily_loss={daily_loss:.2%}"
            return GuardDecision(allow_entries=False, force_flatten=True, reason=state.halt_reason)

    if state.drawdown_ratio >= max_drawdown_halt_ratio:
        state.risk_state = "halt_drawdown"
        state.effective_risk_mult = 0.0
        state.halt_flag = True
        state.halt_reason = f"drawdown={state.drawdown_ratio:.2%}"
        return GuardDecision(allow_entries=False, force_flatten=True, reason=state.halt_reason)

    state.risk_state = "normal"
    state.effective_risk_mult = 1.0
    state.halt_flag = False
    state.halt_reason = ""
    return GuardDecision(allow_entries=True, force_flatten=False)

