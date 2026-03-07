from __future__ import annotations

from datetime import datetime
import re

try:
    from gm.api import get_continuous_contracts, subscribe, unsubscribe  # type: ignore
except Exception:  # pragma: no cover
    get_continuous_contracts = None
    subscribe = None
    unsubscribe = None

from strategy.observability.log import info, warn


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
            warn(
                "gm.roll.mapping_error",
                csymbol=csymbol,
                candidate_csymbol=candidate,
                trade_day=trade_day,
                err=exc,
            )
            continue
        if not mapping:
            continue
        symbol = str(mapping[-1].get("symbol", "")).strip()
        if symbol:
            return symbol
    return None


def _subscribe(
    symbol: str,
    freq_5m: str,
    freq_1h: str,
    warmup_5m: int,
    warmup_1h: int,
    wait_group: bool,
    wait_group_timeout: int,
) -> bool:
    if subscribe is None:
        return True

    try:
        kwargs_5m = {
            "symbols": symbol,
            "frequency": freq_5m,
            "count": int(warmup_5m),
            "wait_group": bool(wait_group),
        }
        kwargs_1h = {
            "symbols": symbol,
            "frequency": freq_1h,
            "count": int(warmup_1h),
            "wait_group": bool(wait_group),
        }
        if int(wait_group_timeout) > 0:
            kwargs_5m["wait_group_timeout"] = int(wait_group_timeout)
            kwargs_1h["wait_group_timeout"] = int(wait_group_timeout)

        subscribe(**kwargs_5m)
        subscribe(**kwargs_1h)
        return True
    except TypeError:
        try:
            subscribe(symbols=symbol, frequency=freq_5m, count=int(warmup_5m), wait_group=bool(wait_group))
            subscribe(symbols=symbol, frequency=freq_1h, count=int(warmup_1h), wait_group=bool(wait_group))
            return True
        except Exception as exc:
            warn(
                "gm.roll.subscribe_failed",
                symbol=symbol,
                freq_5m=freq_5m,
                freq_1h=freq_1h,
                warmup_5m=int(warmup_5m),
                warmup_1h=int(warmup_1h),
                err=exc,
            )
            return False
    except Exception as exc:
        warn(
            "gm.roll.subscribe_failed",
            symbol=symbol,
            freq_5m=freq_5m,
            freq_1h=freq_1h,
            warmup_5m=int(warmup_5m),
            warmup_1h=int(warmup_1h),
            err=exc,
        )
        return False


def roll_main_contract(engine, context) -> None:
    runtime = engine.runtime
    trade_day = context.now.strftime("%Y-%m-%d") if hasattr(context, "now") else datetime.now().strftime("%Y-%m-%d")

    if runtime.last_roll_date == trade_day and runtime.symbol_to_csymbol:
        return

    for csymbol in engine.cfg.runtime.symbols:
        state = runtime.states_by_csymbol.get(csymbol)
        if state is None:
            continue

        old_symbol = state.main_symbol

        if get_continuous_contracts is None:
            new_symbol = f"{csymbol}.SIM"
        else:
            new_symbol = _resolve_main_symbol(csymbol, trade_day)
            if not new_symbol:
                warn("gm.roll.mapping_empty", csymbol=csymbol, trade_day=trade_day)
                continue

        if old_symbol and old_symbol != new_symbol and unsubscribe is not None:
            try:
                unsubscribe(symbols=old_symbol, frequency=engine.cfg.runtime.freq_5m)
                unsubscribe(symbols=old_symbol, frequency=engine.cfg.runtime.freq_1h)
            except Exception as exc:
                warn(
                    "gm.roll.unsubscribe_failed",
                    csymbol=csymbol,
                    old_symbol=old_symbol,
                    trade_day=trade_day,
                    err=exc,
                )

        if old_symbol != new_symbol:
            ok = _subscribe(
                new_symbol,
                engine.cfg.runtime.freq_5m,
                engine.cfg.runtime.freq_1h,
                engine.cfg.runtime.warmup_5m,
                engine.cfg.runtime.warmup_1h,
                wait_group=bool(engine.cfg.gm.subscribe_wait_group),
                wait_group_timeout=int(engine.cfg.gm.wait_group_timeout),
            )
            if not ok:
                continue
            info(
                "gm.roll.switched",
                csymbol=csymbol,
                old_symbol=old_symbol,
                new_symbol=new_symbol,
                trade_day=trade_day,
            )

        engine.set_symbol_mapping(csymbol, new_symbol)

    runtime.last_roll_date = trade_day
