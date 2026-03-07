from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

try:
    from gm.api import OrderType_Market, PositionSide_Long, PositionSide_Short, order_target_volume  # type: ignore
except Exception:  # pragma: no cover
    OrderType_Market = None
    PositionSide_Long = 1
    PositionSide_Short = 2
    order_target_volume = None


@dataclass
class ExecutionResult:
    request_id: str
    intended_action: str
    intended_qty: int
    accepted: bool
    reason: str
    timestamp: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_request_id() -> str:
    return uuid4().hex


def safe_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
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


def submit_target_volume(context, symbol: str, target_qty: int, side: Any, intended_action: str) -> ExecutionResult:
    qty = max(int(target_qty), 0)
    request_id = _new_request_id()
    ts = _now_iso()

    try:
        if order_target_volume is None:
            return ExecutionResult(
                request_id=request_id,
                intended_action=intended_action,
                intended_qty=qty,
                accepted=False,
                reason="gm_order_api_unavailable",
                timestamp=ts,
            )

        order_target_volume(
            symbol=symbol,
            volume=qty,
            position_side=side,
            order_type=OrderType_Market,
        )
        return ExecutionResult(
            request_id=request_id,
            intended_action=intended_action,
            intended_qty=qty,
            accepted=True,
            reason="submitted",
            timestamp=ts,
        )
    except Exception as exc:
        return ExecutionResult(
            request_id=request_id,
            intended_action=intended_action,
            intended_qty=qty,
            accepted=False,
            reason=f"submit_error:{exc}",
            timestamp=ts,
        )


def execute_signal(context, state, signal, symbol: str) -> list[ExecutionResult]:
    _ = state
    action = signal.action
    results: list[ExecutionResult] = []
    if action == "none":
        return results

    if action == "buy":
        results.append(submit_target_volume(context, symbol, 0, PositionSide_Short, "buy:close_short"))
        results.append(submit_target_volume(context, symbol, signal.qty, PositionSide_Long, "buy:open_long"))
    elif action == "sell":
        results.append(submit_target_volume(context, symbol, 0, PositionSide_Long, "sell:close_long"))
        results.append(submit_target_volume(context, symbol, signal.qty, PositionSide_Short, "sell:open_short"))
    elif action == "close_long":
        results.append(submit_target_volume(context, symbol, 0, PositionSide_Long, "close_long"))
    elif action == "close_short":
        results.append(submit_target_volume(context, symbol, 0, PositionSide_Short, "close_short"))
    return results
