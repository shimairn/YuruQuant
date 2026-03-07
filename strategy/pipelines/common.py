from __future__ import annotations


def estimate_turnover_cost(entry_price: float, exit_price: float, qty: int, multiplier: float, ratio: float) -> float:
    turnover = (abs(float(entry_price)) + abs(float(exit_price))) * float(multiplier) * max(int(qty), 0)
    return turnover * max(float(ratio), 0.0)
