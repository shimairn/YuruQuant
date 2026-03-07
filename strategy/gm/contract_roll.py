from __future__ import annotations

from datetime import datetime
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

try:
    from gm.api import get_continuous_contracts, subscribe, unsubscribe  # type: ignore
except Exception:  # pragma: no cover
    get_continuous_contracts = None
    subscribe = None
    unsubscribe = None


def _is_permission_error(exc: Exception) -> bool:
    msg = str(exc)
    return "ERR_NO_DATA_PERMISSION" in msg or "PermissionDenied" in msg


def _subscribe_with_fallback_count(symbol: str, frequency: str, count: int) -> tuple[bool, bool, int]:
    if subscribe is None:
        req = max(int(count), 1)
        return True, False, req
    req = max(int(count), 1)
    attempt = req

    while attempt >= 1:
        try:
            subscribe(symbols=symbol, frequency=frequency, count=attempt, wait_group=False)
            if attempt != req:
                print(f"roll subscribe fallback symbol={symbol} freq={frequency} count={req}->{attempt}")
            return True, attempt != req, attempt
        except Exception as exc:
            if not _is_permission_error(exc) or attempt == 1:
                print(f"roll subscribe failed symbol={symbol} freq={frequency} count={attempt} err={exc}")
                return False, False, attempt
            next_attempt = max(1, attempt // 2)
            if next_attempt == attempt:
                print(f"roll subscribe failed symbol={symbol} freq={frequency} count={attempt} err={exc}")
                return False, False, attempt
            attempt = next_attempt
    return False, False, req


def _subscribe_symbol(symbol: str, freq_5m: str, freq_1h: str, count_5m: int, count_1h: int) -> bool:
    ok_5m, fallback_5m, used_5m = _subscribe_with_fallback_count(symbol, freq_5m, count_5m)
    ok_1h, fallback_1h, used_1h = _subscribe_with_fallback_count(symbol, freq_1h, count_1h)
    ok = ok_5m and ok_1h
    if ok:
        if fallback_5m or fallback_1h:
            print(
                f"roll subscribe degraded symbol={symbol} f5m={freq_5m}:{used_5m}/{int(count_5m)} "
                f"f1h={freq_1h}:{used_1h}/{int(count_1h)} - will retry full counts"
            )
            return False
        print(f"roll subscribe ok symbol={symbol} f5m={freq_5m} f1h={freq_1h}")
        return True
    return False


def _continuous_csymbol_candidates(csymbol: str) -> list[str]:
    raw = str(csymbol or "").strip()
    if not raw:
        return []

    seen: set[str] = set()
    out: list[str] = []

    def _add(value: str) -> None:
        v = str(value or "").strip()
        if v and v not in seen:
            seen.add(v)
            out.append(v)

    _add(raw)
    if "." not in raw:
        return out

    exch, product = raw.split(".", 1)
    exch = exch.upper().strip()
    product = product.strip()
    if not exch or not product:
        return out

    _add(f"{exch}.{product}")
    _add(f"{exch}.{product.upper()}")
    _add(f"{exch}.{product.lower()}")

    # Support IC88/AP88-like aliases by removing trailing digits.
    base = re.sub(r"\d+$", "", product)
    if base and base != product:
        _add(f"{exch}.{base}")
        _add(f"{exch}.{base.upper()}")
        _add(f"{exch}.{base.lower()}")
    return out


def _resolve_main_symbol(csymbol: str, trade_day: str) -> str | None:
    if get_continuous_contracts is None:
        return None

    for candidate in _continuous_csymbol_candidates(csymbol):
        try:
            mapping = get_continuous_contracts(csymbol=candidate, start_date=trade_day, end_date=trade_day)
        except Exception as exc:
            print(f"roll mapping error csymbol={csymbol} candidate={candidate} trade_day={trade_day} err={exc}")
            continue
        if not mapping:
            continue
        symbol = str(mapping[-1].get("symbol", "")).strip()
        if not symbol:
            continue
        if candidate != csymbol:
            print(f"roll mapping fallback csymbol={csymbol} candidate={candidate} symbol={symbol}")
        return symbol
    return None


def roll_main_contract(engine, context) -> None:
    """主力合约换月处理。订阅失败时记录状态供后续重试，避免长时间mapped=0"""
    runtime = engine.runtime
    trade_day = context.now.strftime("%Y-%m-%d") if hasattr(context, "now") else datetime.now().strftime("%Y-%m-%d")

    # 初始化订阅失败计数器（如果不存在）
    if not hasattr(runtime, "_subscribe_fail_counts"):
        runtime._subscribe_fail_counts = {}

    if runtime.last_roll_date == trade_day and runtime.symbol_to_csymbol:
        return

    for csymbol in engine.cfg.runtime.symbols:
        state = runtime.states_by_csymbol.get(csymbol)
        if state is None:
            continue

        old_symbol = state.main_symbol

        # Local simulation or unavailable GM mapping.
        if get_continuous_contracts is None or getattr(context, "local_simulation", False):
            new_symbol = f"{csymbol}.SIM"
        else:
            new_symbol = _resolve_main_symbol(csymbol, trade_day)
            if not new_symbol:
                print(f"roll mapping empty csymbol={csymbol} trade_day={trade_day}")
                continue

        if old_symbol and old_symbol != new_symbol:
            if unsubscribe is not None:
                try:
                    unsubscribe(symbols=old_symbol, frequency=engine.cfg.runtime.freq_5m)
                    unsubscribe(symbols=old_symbol, frequency=engine.cfg.runtime.freq_1h)
                    print(f"roll unsubscribe ok csymbol={csymbol} old={old_symbol}")
                except Exception as exc:
                    print(f"roll unsubscribe failed csymbol={csymbol} old={old_symbol} err={exc}")
            runtime.symbol_to_csymbol.pop(old_symbol, None)

        if old_symbol != new_symbol:
            ok = _subscribe_symbol(
                new_symbol,
                engine.cfg.runtime.freq_5m,
                engine.cfg.runtime.freq_1h,
                engine.cfg.runtime.sub_count_5m,
                engine.cfg.runtime.sub_count_1h,
            )
            if not ok:
                # 记录订阅失败，后续 on_bar 可以重试
                runtime._subscribe_fail_counts[new_symbol] = runtime._subscribe_fail_counts.get(new_symbol, 0) + 1
                fail_count = runtime._subscribe_fail_counts[new_symbol]
                print(f"roll subscribe failed csymbol={csymbol} symbol={new_symbol} fail_count={fail_count} - will retry on next bar")
                # 不要 continue，允许后续逻辑清理状态
            else:
                # 订阅成功，清除失败计数
                runtime._subscribe_fail_counts.pop(new_symbol, None)

            state.pending_signal = None
            state.pending_signal_eob = None
            state.pending_platform = None
            state.last_risk_signal_eob = None
            print(f"roll switched csymbol={csymbol} old={old_symbol} new={new_symbol} pending_cleared=1")

        engine.set_symbol_mapping(csymbol, new_symbol)

    runtime.last_roll_date = trade_day
