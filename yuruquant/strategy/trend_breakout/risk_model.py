from __future__ import annotations

import math

from yuruquant.core.models import InstrumentSpec, PortfolioRuntime


def _normalize_lot(qty: int, min_lot: int, lot_step: int) -> int:
    qty = max(int(qty), 0)
    min_lot = max(int(min_lot), 1)
    lot_step = max(int(lot_step), 1)
    if qty < min_lot:
        return 0
    return qty - ((qty - min_lot) % lot_step)


def resolve_order_qty(portfolio: PortfolioRuntime, spec: InstrumentSpec, risk_per_trade_ratio: float, current_price: float, atr_value: float, hard_stop_atr: float) -> int:
    if current_price <= 0 or atr_value <= 0 or spec.multiplier <= 0:
        return 0
    equity = portfolio.current_equity if portfolio.current_equity > 0 else max(portfolio.initial_equity, 500000.0)
    risk_mult = max(float(portfolio.effective_risk_mult), 0.0)
    if equity <= 0 or risk_mult <= 0:
        return 0
    stop_distance = max(float(hard_stop_atr), 0.0) * float(atr_value)
    risk_per_lot = stop_distance * float(spec.multiplier)
    if risk_per_lot <= 0:
        return 0
    max_loss = equity * max(float(risk_per_trade_ratio), 0.0) * risk_mult
    qty = math.floor(max_loss / risk_per_lot)
    return _normalize_lot(qty, spec.min_lot, spec.lot_step)
