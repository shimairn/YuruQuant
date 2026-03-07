from __future__ import annotations

from typing import Any

from strategy.domain.instruments import get_multiplier

try:
    from gm.api import OrderType_Market, PositionSide_Long, PositionSide_Short, order_target_volume  # type: ignore
except Exception:  # pragma: no cover
    OrderType_Market = None
    PositionSide_Long = 1
    PositionSide_Short = 2
    order_target_volume = None


def safe_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def get_position(context, symbol: str, side: Any) -> Any:
    try:
        return context.account().position(symbol=symbol, side=side)
    except Exception:
        return None


def position_volume(pos: Any) -> int:
    if pos is None:
        return 0
    for key in ("available_now", "available", "volume", "qty"):
        val = pos.get(key) if isinstance(pos, dict) else getattr(pos, key, None)
        qty = safe_int(val, 0)
        if qty > 0:
            return qty
    return 0


def position_vwap(pos: Any, default_price: float) -> float:
    if pos is None:
        return float(default_price)
    for key in ("vwap", "price", "cost"):
        val = pos.get(key) if isinstance(pos, dict) else getattr(pos, key, None)
        px = safe_float(val, 0.0)
        if px > 0:
            return px
    return float(default_price)


def contract_multiplier(csymbol: str, cfg: object | None = None) -> float:
    if cfg is not None:
        try:
            return get_multiplier(cfg, csymbol)
        except Exception:
            pass
    builtins = {
        "DCE.p": 10.0,
        "SHFE.ag": 15.0,
        "DCE.jm": 60.0,
    }
    return float(builtins.get(csymbol, 10.0))


def submit_target_volume(context, symbol: str, target_qty: int, side: Any) -> None:
    qty = max(int(target_qty), 0)
    if hasattr(context, "submit_target_volume"):
        context.submit_target_volume(symbol, qty, side)
        return

    if order_target_volume is None:
        return

    order_target_volume(
        symbol=symbol,
        volume=qty,
        position_side=side,
        order_type=OrderType_Market,
    )


def execute_signal(context, state, signal, symbol: str) -> None:
    action = signal.action
    if action == "none":
        return

    if action == "buy":
        submit_target_volume(context, symbol, 0, PositionSide_Short)
        submit_target_volume(context, symbol, signal.qty, PositionSide_Long)
    elif action == "sell":
        submit_target_volume(context, symbol, 0, PositionSide_Long)
        submit_target_volume(context, symbol, signal.qty, PositionSide_Short)
    elif action == "close_long":
        submit_target_volume(context, symbol, 0, PositionSide_Long)
    elif action == "close_short":
        submit_target_volume(context, symbol, 0, PositionSide_Short)
    elif action == "close_half_long":
        long_pos = get_position(context, symbol, PositionSide_Long)
        cur = position_volume(long_pos)
        close_qty = max(1, signal.qty)
        submit_target_volume(context, symbol, max(0, cur - close_qty), PositionSide_Long)
    elif action == "close_half_short":
        short_pos = get_position(context, symbol, PositionSide_Short)
        cur = position_volume(short_pos)
        close_qty = max(1, signal.qty)
        submit_target_volume(context, symbol, max(0, cur - close_qty), PositionSide_Short)
