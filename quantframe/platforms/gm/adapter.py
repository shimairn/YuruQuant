from __future__ import annotations

import inspect
import re
from datetime import datetime
from pathlib import Path
from typing import Sequence
from uuid import uuid4

from quantframe.app.config import AppConfig
from quantframe.core.models import Bar, Instrument, OrderRequest, OrderResult, PortfolioSnapshot, Position
from quantframe.platforms.registry import register_platform

try:
    from gm.api import (  # type: ignore
        MODE_BACKTEST,
        MODE_LIVE,
        OrderType_Market,
        PositionSide_Long,
        PositionSide_Short,
        current,
        get_continuous_contracts,
        get_previous_trading_date,
        order_target_volume,
        run,
        subscribe,
    )
except Exception:  # pragma: no cover
    MODE_BACKTEST = 1
    MODE_LIVE = 2
    OrderType_Market = None
    PositionSide_Long = 1
    PositionSide_Short = 2
    current = None
    get_continuous_contracts = None
    get_previous_trading_date = None
    order_target_volume = None
    run = None
    subscribe = None


class GMPlatform:
    name = "gm"

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.mode = config.runtime.mode
        self.context = None
        self.by_symbol: dict[str, Instrument] = {}
        self.by_id: dict[str, Instrument] = {}
        self.actual_symbols: dict[str, str] = {}

    def _context_time(self) -> datetime:
        now = getattr(self.context, "now", None)
        if isinstance(now, datetime):
            return now
        return datetime.now()

    def _normalize_trade_day(self, value: object) -> str:
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d")
        text = str(value or "").strip()
        return text[:10] if text else self._context_time().strftime("%Y-%m-%d")

    def _is_continuous_symbol(self, symbol: str) -> bool:
        raw = str(symbol or "").strip()
        if "." not in raw:
            return False
        _, product = raw.split(".", 1)
        return bool(re.fullmatch(r"[A-Za-z]+", product))

    def _continuous_symbol(self, instrument: Instrument) -> str:
        explicit = str(instrument.metadata.get("continuous_symbol", "") or "").strip()
        if explicit:
            return explicit
        return instrument.platform_symbol

    def _mapping_symbol_from_payload(self, payload: object) -> str:
        if isinstance(payload, dict):
            return str(payload.get("symbol", "")).strip()
        return str(getattr(payload, "symbol", "") or "").strip()

    def _resolve_actual_symbol(self, instrument: Instrument, trade_day: object | None = None) -> str:
        base_symbol = instrument.platform_symbol
        continuous_symbol = self._continuous_symbol(instrument)
        if not self._is_continuous_symbol(continuous_symbol):
            self.actual_symbols[instrument.instrument_id] = base_symbol
            return base_symbol
        if get_continuous_contracts is None:
            self.actual_symbols[instrument.instrument_id] = continuous_symbol
            return continuous_symbol

        requested_day = self._normalize_trade_day(trade_day or self._context_time())
        candidate_days = [requested_day]
        if get_previous_trading_date is not None:
            current_day = requested_day
            for _ in range(2):
                try:
                    previous_day = get_previous_trading_date(str(instrument.metadata.get("exchange", "") or ""), current_day)
                except Exception:
                    break
                normalized = self._normalize_trade_day(previous_day)
                if not normalized or normalized in candidate_days:
                    break
                candidate_days.append(normalized)
                current_day = normalized

        for day in candidate_days:
            try:
                mapping = get_continuous_contracts(csymbol=continuous_symbol, start_date=day, end_date=day)
            except Exception:
                continue
            if isinstance(mapping, dict):
                mapping = [mapping]
            if isinstance(mapping, (list, tuple)):
                for item in reversed(list(mapping)):
                    symbol = self._mapping_symbol_from_payload(item)
                    if symbol:
                        self.actual_symbols[instrument.instrument_id] = symbol
                        self.by_symbol[symbol] = instrument
                        return symbol

        self.actual_symbols[instrument.instrument_id] = continuous_symbol
        self.by_symbol[continuous_symbol] = instrument
        return continuous_symbol

    def refresh_contract_mappings(self, trade_time: object | None = None) -> None:
        for instrument in self.by_id.values():
            self._resolve_actual_symbol(instrument, trade_day=trade_time)

    def bind_context(self, context: object | None) -> None:
        self.context = context

    def initialize(self) -> None:
        return None

    def subscribe(self, instruments: Sequence[Instrument], frequency: str, history_bars: int) -> None:
        self.by_symbol = {}
        for item in instruments:
            self.by_symbol[item.platform_symbol] = item
            continuous_symbol = self._continuous_symbol(item)
            self.by_symbol[continuous_symbol] = item
        self.by_id = {item.instrument_id: item for item in instruments}
        self.refresh_contract_mappings(self._context_time())
        if subscribe is None:
            return
        symbols = ",".join(self._continuous_symbol(item) for item in instruments)
        kwargs = {
            "symbols": symbols,
            "frequency": frequency,
            "count": max(int(history_bars), 1),
            "wait_group": bool(self.config.platform.gm and self.config.platform.gm.subscribe_wait_group),
        }
        gm = self.config.platform.gm
        if gm is not None and gm.wait_group_timeout > 0:
            kwargs["wait_group_timeout"] = f"{gm.wait_group_timeout}s"
        subscribe(**kwargs)

    def fetch_history(self, instrument: Instrument, frequency: str, count: int) -> list[Bar]:
        if self.context is None:
            return []
        data_fn = getattr(self.context, "data", None)
        if not callable(data_fn):
            return []
        history_symbol = self._continuous_symbol(instrument)
        raw = data_fn(
            symbol=history_symbol,
            frequency=frequency,
            count=max(int(count), 1),
            fields="eob,open,high,low,close,volume",
        )
        return self._normalize_rows(instrument, frequency, raw)

    def normalize_bars(self, raw_bars: Sequence[object]) -> list[Bar]:
        bars: list[Bar] = []
        for item in list(raw_bars or []):
            trade_time = getattr(item, "eob", getattr(item, "bob", None))
            self.refresh_contract_mappings(trade_time)
            symbol = str(getattr(item, "symbol", "") or getattr(item, "sec_id", "") or "").strip()
            instrument = self.by_symbol.get(symbol)
            if instrument is None:
                continue
            frequency = str(getattr(item, "frequency", "") or "").strip()
            bars.append(
                Bar(
                    instrument_id=instrument.instrument_id,
                    symbol=symbol,
                    frequency=frequency,
                    timestamp=trade_time,
                    open=float(getattr(item, "open", 0.0) or 0.0),
                    high=float(getattr(item, "high", 0.0) or 0.0),
                    low=float(getattr(item, "low", 0.0) or 0.0),
                    close=float(getattr(item, "close", 0.0) or 0.0),
                    volume=float(getattr(item, "volume", 0.0) or 0.0),
                )
            )
        return bars

    def _normalize_rows(self, instrument: Instrument, frequency: str, raw: object) -> list[Bar]:
        if raw is None:
            return []
        if isinstance(raw, dict):
            rows = [raw]
        elif isinstance(raw, (list, tuple)):
            rows = list(raw)
        else:
            to_dict = getattr(raw, "to_dict", None)
            if callable(to_dict):
                try:
                    converted = to_dict("records")
                except Exception:
                    converted = to_dict()
                rows = list(converted) if isinstance(converted, list) else [converted]
            else:
                rows = [raw]
        normalized: list[Bar] = []
        for item in rows:
            if isinstance(item, dict):
                eob = item.get("eob")
                open_price = item.get("open", 0.0)
                high_price = item.get("high", 0.0)
                low_price = item.get("low", 0.0)
                close_price = item.get("close", 0.0)
                volume = item.get("volume", 0.0)
            else:
                eob = getattr(item, "eob", None)
                open_price = getattr(item, "open", 0.0)
                high_price = getattr(item, "high", 0.0)
                low_price = getattr(item, "low", 0.0)
                close_price = getattr(item, "close", 0.0)
                volume = getattr(item, "volume", 0.0)
            if eob is None:
                continue
            normalized.append(
                Bar(
                    instrument_id=instrument.instrument_id,
                    symbol=instrument.platform_symbol,
                    frequency=frequency,
                    timestamp=eob,
                    open=float(open_price or 0.0),
                    high=float(high_price or 0.0),
                    low=float(low_price or 0.0),
                    close=float(close_price or 0.0),
                    volume=float(volume or 0.0),
                )
            )
        normalized.sort(key=lambda item: str(item.timestamp))
        return normalized

    def _account(self):
        if self.context is None:
            return None
        account_fn = getattr(self.context, "account", None)
        if not callable(account_fn):
            return None
        try:
            return account_fn()
        except Exception:
            return None

    def get_portfolio_snapshot(self) -> PortfolioSnapshot:
        account = self._account()
        if account is None:
            gm = self.config.platform.gm
            initial_cash = gm.backtest.initial_cash if gm is not None else 0.0
            return PortfolioSnapshot(equity=initial_cash, cash=initial_cash)
        cash = getattr(account, "cash", None)
        if isinstance(cash, dict):
            equity = float(cash.get("nav") or cash.get("balance") or cash.get("equity") or 0.0)
            free_cash = float(cash.get("available") or cash.get("cash") or equity)
            return PortfolioSnapshot(equity=equity, cash=free_cash)
        equity = float(cash or 0.0)
        return PortfolioSnapshot(equity=equity, cash=equity)

    def get_position(self, instrument: Instrument) -> Position:
        account = self._account()
        if account is None:
            return Position(instrument_id=instrument.instrument_id, symbol=instrument.platform_symbol, qty=0, avg_price=0.0)
        actual_symbol = self._resolve_actual_symbol(instrument)
        try:
            long_position = account.position(symbol=actual_symbol, side=PositionSide_Long)
        except Exception:
            long_position = None
        try:
            short_position = account.position(symbol=actual_symbol, side=PositionSide_Short)
        except Exception:
            short_position = None
        long_qty = int(getattr(long_position, "volume", 0) or getattr(long_position, "available_now", 0) or 0)
        short_qty = int(getattr(short_position, "volume", 0) or getattr(short_position, "available_now", 0) or 0)
        avg_price = float(getattr(long_position, "vwap", 0.0) or getattr(short_position, "vwap", 0.0) or 0.0)
        return Position(
            instrument_id=instrument.instrument_id,
            symbol=actual_symbol,
            qty=long_qty - short_qty,
            avg_price=avg_price,
        )

    def _submit_target(self, symbol: str, target_qty: int, position_side: int) -> OrderResult:
        request_id = uuid4().hex
        if order_target_volume is None:
            return OrderResult(request_id=request_id, accepted=False, reason="gm_order_api_unavailable")
        try:
            order_target_volume(
                symbol=symbol,
                volume=max(int(target_qty), 0),
                position_side=position_side,
                order_type=OrderType_Market,
            )
        except Exception as exc:
            return OrderResult(request_id=request_id, accepted=False, reason=f"submit_error:{exc}")
        return OrderResult(request_id=request_id, accepted=True, reason="submitted")

    def submit_orders(self, orders: Sequence[OrderRequest]) -> list[OrderResult]:
        results: list[OrderResult] = []
        for order in orders:
            instrument = self.by_id.get(order.instrument_id)
            if instrument is None:
                results.append(OrderResult(request_id=uuid4().hex, accepted=False, reason="unknown_instrument"))
                continue
            actual_symbol = self._resolve_actual_symbol(instrument)
            current_position = self.get_position(instrument)
            target_qty = int(order.target_qty)
            if target_qty > 0:
                if current_position.signed_qty < 0:
                    results.append(self._submit_target(actual_symbol, 0, PositionSide_Short))
                results.append(self._submit_target(actual_symbol, target_qty, PositionSide_Long))
            elif target_qty < 0:
                if current_position.signed_qty > 0:
                    results.append(self._submit_target(actual_symbol, 0, PositionSide_Long))
                results.append(self._submit_target(actual_symbol, abs(target_qty), PositionSide_Short))
            else:
                if current_position.signed_qty >= 0:
                    results.append(self._submit_target(actual_symbol, 0, PositionSide_Long))
                if current_position.signed_qty <= 0:
                    results.append(self._submit_target(actual_symbol, 0, PositionSide_Short))
        return results

    def run(self, callbacks: object) -> None:
        if run is None:
            raise RuntimeError("gm.api.run is unavailable")
        gm = self.config.platform.gm
        if gm is None:
            raise ValueError("platform.gm config is required for the GM adapter")
        mode = MODE_LIVE if self.mode == "LIVE" else MODE_BACKTEST
        kwargs = {
            "strategy_id": gm.strategy_id,
            "filename": str(Path("quantframe") / "platforms" / "gm" / "entrypoint.py"),
            "mode": mode,
            "token": gm.token,
            "backtest_start_time": gm.backtest.start,
            "backtest_end_time": gm.backtest.end,
            "backtest_initial_cash": gm.backtest.initial_cash,
            "backtest_commission_ratio": gm.backtest.commission_ratio,
            "backtest_slippage_ratio": gm.backtest.slippage_ratio,
        }
        if gm.serv_addr:
            kwargs["serv_addr"] = gm.serv_addr
        callback_kwargs = {
            "init": callbacks.initialize,
            "on_bar": callbacks.on_bar,
            "on_order_status": callbacks.on_order_status,
            "on_execution_report": callbacks.on_execution_report,
            "on_error": callbacks.on_error,
        }
        try:
            signature = inspect.signature(run)
            if all(name in signature.parameters for name in callback_kwargs):
                kwargs.update(callback_kwargs)
        except Exception:
            pass
        run(**kwargs)


def _build_platform(config: AppConfig) -> GMPlatform:
    return GMPlatform(config)


register_platform("gm", _build_platform)
