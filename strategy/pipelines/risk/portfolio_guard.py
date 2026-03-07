from __future__ import annotations

from datetime import datetime

from strategy.core.time_utils import to_trade_day


def get_account_equity(context) -> float:
    try:
        cash = context.account().cash
        if isinstance(cash, dict):
            for key in ("nav", "balance", "equity", "total", "cash", "available"):
                if key in cash:
                    return float(cash.get(key) or 0.0)
        return float(cash or 0.0)
    except Exception:
        return 0.0


def update_portfolio_risk(runtime, context) -> tuple[bool, str]:
    state = runtime.portfolio_risk
    equity = get_account_equity(context)
    if equity <= 0:
        equity = 500000.0
    state.current_equity = equity

    today = to_trade_day(getattr(context, "now", datetime.now()))
    if state.initial_equity <= 0:
        state.initial_equity = equity
        state.current_equity = equity
        state.equity_peak = equity
        state.daily_start_equity = equity
        state.current_date = today
        state.drawdown_ratio = 0.0
        state.risk_state = "normal"
        state.effective_risk_mult = 1.0
        state.halt_flag = False
        state.halt_reason = ""
        return True, ""

    if state.current_date != today:
        state.daily_start_equity = equity
        state.current_date = today

    state.equity_peak = max(state.equity_peak, equity)
    state.drawdown_ratio = (state.equity_peak - equity) / state.equity_peak if state.equity_peak > 0 else 0.0

    if state.daily_start_equity > 0:
        daily_loss = (state.daily_start_equity - equity) / state.daily_start_equity
        if daily_loss >= runtime.cfg.portfolio.max_daily_loss_ratio:
            state.risk_state = "halt_daily_loss"
            state.effective_risk_mult = 0.0
            state.halt_flag = True
            state.halt_reason = f"daily_loss={daily_loss:.2%}"
            return False, state.halt_reason

    if state.drawdown_ratio >= runtime.cfg.portfolio.max_drawdown_halt_ratio:
        state.risk_state = "halt_drawdown"
        state.effective_risk_mult = 0.0
        state.halt_flag = True
        state.halt_reason = f"drawdown={state.drawdown_ratio:.2%}"
        return False, state.halt_reason

    state.risk_state = "normal"
    state.effective_risk_mult = 1.0
    state.halt_flag = False
    state.halt_reason = ""
    return True, ""
