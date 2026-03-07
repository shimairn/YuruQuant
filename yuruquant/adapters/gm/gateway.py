from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from yuruquant.app.config import AppConfig
from yuruquant.core.frames import KlineFrame, ensure_kline_frame
from yuruquant.core.models import ExecutionResult, MarketEvent, NormalizedBar, OrderIntent, PortfolioSnapshot, PositionSnapshot, Signal
from yuruquant.core.time import normalize_frequency
from yuruquant.reporting.logging import info, warn

try:
    from gm.api import OrderType_Market, PositionSide_Long, PositionSide_Short, get_continuous_contracts, get_previous_trading_date, order_target_volume, subscribe, unsubscribe  # type: ignore
except Exception:  # pragma: no cover
    OrderType_Market = None
    PositionSide_Long = 1
    PositionSide_Short = 2
    get_continuous_contracts = None
    get_previous_trading_date = None
    order_target_volume = None
    subscribe = None
    unsubscribe = None


class GMGateway:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.context = None
        self.csymbol_to_symbol: dict[str, str] = {}
        self.symbol_to_csymbol: dict[str, str] = {}
        self.last_roll_date = ''

    def bind_context(self, context: object) -> None:
        self.context = context

    def _continuous_candidates(self, csymbol: str) -> list[str]:
        raw = str(csymbol or '').strip()
        if not raw:
            return []
        if '.' not in raw:
            return [raw]
        exch, product = raw.split('.', 1)
        return [raw, f'{exch}.{product.upper()}', f'{exch}.{product.lower()}']

    def _exchange_from_csymbol(self, csymbol: str) -> str:
        raw = str(csymbol or '').strip()
        if '.' not in raw:
            return ''
        return raw.split('.', 1)[0].strip().upper()

    def _normalize_trade_day(self, value: object) -> str:
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%d')
        text = str(value or '').strip()
        return text[:10] if text else ''

    def _candidate_trade_days(self, csymbol: str, trade_day: str, max_fallbacks: int = 3):
        requested_day = self._normalize_trade_day(trade_day)
        if not requested_day:
            return

        yield requested_day
        exchange = self._exchange_from_csymbol(csymbol)
        if get_previous_trading_date is None or not exchange:
            return

        current_day = requested_day
        seen = {requested_day}
        for _ in range(max(int(max_fallbacks), 0)):
            try:
                previous_day = get_previous_trading_date(exchange, current_day)
            except Exception as exc:
                warn('gm.roll.previous_trade_day_error', csymbol=csymbol, trade_day=current_day, err=exc)
                return
            normalized = self._normalize_trade_day(previous_day)
            if not normalized or normalized in seen:
                return
            yield normalized
            seen.add(normalized)
            current_day = normalized

    def _lookup_main_symbol_for_day(self, csymbol: str, trade_day: str) -> str | None:
        for candidate in self._continuous_candidates(csymbol):
            try:
                mapping = get_continuous_contracts(csymbol=candidate, start_date=trade_day, end_date=trade_day)
            except Exception as exc:
                warn('gm.roll.mapping_error', csymbol=csymbol, candidate=candidate, err=exc)
                continue
            if not mapping:
                continue
            symbol = str(mapping[-1].get('symbol', '')).strip()
            if symbol:
                return symbol
        return None

    def _resolve_main_symbol(self, csymbol: str, trade_day: str) -> str | None:
        if get_continuous_contracts is None or not self.config.broker.gm.token:
            return f'{csymbol}.SIM'

        requested_day = self._normalize_trade_day(trade_day)
        for lookup_day in self._candidate_trade_days(csymbol, requested_day):
            symbol = self._lookup_main_symbol_for_day(csymbol, lookup_day)
            if symbol:
                if lookup_day != requested_day:
                    info('gm.roll.trade_day_fallback', csymbol=csymbol, requested_trade_day=requested_day, fallback_trade_day=lookup_day, symbol=symbol)
                return symbol
        return None

    def _subscribe_symbol(self, symbol: str) -> None:
        if subscribe is None:
            return
        kwargs = {
            'symbols': symbol,
            'wait_group': bool(self.config.broker.gm.subscribe_wait_group),
        }
        timeout = int(self.config.broker.gm.wait_group_timeout)
        if timeout > 0:
            kwargs['wait_group_timeout'] = f'{timeout}s'
        subscribe(frequency=self.config.universe.entry_frequency, count=int(self.config.universe.warmup.entry_bars), **kwargs)
        subscribe(frequency=self.config.universe.trend_frequency, count=int(self.config.universe.warmup.trend_bars), **kwargs)

    def refresh_main_contracts(self, trade_time: object) -> None:
        trade_day = self._normalize_trade_day(trade_time)
        if self.last_roll_date == trade_day and self.symbol_to_csymbol:
            return
        for csymbol in self.config.universe.symbols:
            symbol = self._resolve_main_symbol(csymbol, trade_day)
            if not symbol:
                warn('gm.roll.mapping_empty', csymbol=csymbol, trade_day=trade_day)
                continue
            old_symbol = self.csymbol_to_symbol.get(csymbol)
            if old_symbol and old_symbol != symbol and unsubscribe is not None:
                try:
                    unsubscribe(symbols=old_symbol, frequency=self.config.universe.entry_frequency)
                    unsubscribe(symbols=old_symbol, frequency=self.config.universe.trend_frequency)
                except Exception as exc:
                    warn('gm.roll.unsubscribe_failed', csymbol=csymbol, old_symbol=old_symbol, err=exc)
            if old_symbol != symbol:
                self._subscribe_symbol(symbol)
                info('gm.roll.switched', csymbol=csymbol, old_symbol=old_symbol, new_symbol=symbol)
            self.csymbol_to_symbol[csymbol] = symbol
            self.symbol_to_csymbol[symbol] = csymbol
        self.last_roll_date = trade_day

    def resolve_csymbol(self, symbol: str) -> str | None:
        if symbol in self.symbol_to_csymbol:
            return self.symbol_to_csymbol[symbol]
        if symbol in self.config.universe.symbols:
            return symbol
        return None

    def current_main_symbol(self, csymbol: str) -> str:
        return self.csymbol_to_symbol.get(csymbol, '')

    def build_market_event(self, bars: list[object], trade_time: object) -> MarketEvent:
        normalized: list[NormalizedBar] = []
        for item in bars:
            symbol = str(getattr(item, 'symbol', '') or '').strip()
            csymbol = self.resolve_csymbol(symbol)
            if not symbol or not csymbol:
                continue
            normalized.append(
                NormalizedBar(
                    csymbol=csymbol,
                    symbol=symbol,
                    frequency=normalize_frequency(getattr(item, 'frequency', '')),
                    eob=getattr(item, 'eob', trade_time),
                    open=float(getattr(item, 'open', 0.0) or 0.0),
                    high=float(getattr(item, 'high', 0.0) or 0.0),
                    low=float(getattr(item, 'low', 0.0) or 0.0),
                    close=float(getattr(item, 'close', 0.0) or 0.0),
                    volume=float(getattr(item, 'volume', 0.0) or 0.0),
                )
            )
        return MarketEvent(trade_time=trade_time, bars=normalized)

    def fetch_history(self, symbol: str, frequency: str, count: int) -> KlineFrame:
        if self.context is None:
            return KlineFrame.empty(symbol=symbol, frequency=frequency)
        kwargs = {
            'symbol': symbol,
            'frequency': frequency,
            'count': max(int(count), 1),
            'fields': 'eob,open,high,low,close,volume',
        }
        try:
            try:
                raw = self.context.data(**kwargs, format='row')
            except TypeError:
                raw = self.context.data(**kwargs)
        except Exception as exc:
            warn('gm.market_data.fetch_failed', symbol=symbol, frequency=frequency, err=exc)
            return KlineFrame.empty(symbol=symbol, frequency=frequency)
        return ensure_kline_frame(raw, symbol=symbol, frequency=frequency)

    def _account(self):
        if self.context is None:
            return None
        try:
            return self.context.account()
        except Exception:
            return None

    def _position_volume(self, position: object) -> int:
        if position is None:
            return 0
        if isinstance(position, dict):
            for key in ('available_now', 'available', 'volume', 'qty'):
                if key in position and int(position[key] or 0) > 0:
                    return int(position[key])
            return 0
        for key in ('available_now', 'available', 'volume', 'qty'):
            value = getattr(position, key, None)
            if value is not None and int(value or 0) > 0:
                return int(value)
        return 0

    def get_position_snapshot(self, symbol: str) -> PositionSnapshot:
        account = self._account()
        if account is None:
            return PositionSnapshot(symbol=symbol)
        try:
            long_pos = account.position(symbol=symbol, side=PositionSide_Long)
        except Exception:
            long_pos = None
        try:
            short_pos = account.position(symbol=symbol, side=PositionSide_Short)
        except Exception:
            short_pos = None
        return PositionSnapshot(
            symbol=symbol,
            long_qty=self._position_volume(long_pos),
            short_qty=self._position_volume(short_pos),
        )

    def get_portfolio_snapshot(self) -> PortfolioSnapshot:
        account = self._account()
        if account is None:
            return PortfolioSnapshot(equity=self.config.broker.gm.backtest.initial_cash)
        try:
            cash = account.cash
        except Exception:
            return PortfolioSnapshot(equity=self.config.broker.gm.backtest.initial_cash)
        if isinstance(cash, dict):
            for key in ('nav', 'balance', 'equity', 'total', 'cash', 'available'):
                if key in cash:
                    value = float(cash.get(key) or 0.0)
                    return PortfolioSnapshot(equity=value, cash=float(cash.get('available', value) or value))
        value = float(cash or 0.0)
        return PortfolioSnapshot(equity=value, cash=value)

    def plan_order_intents(self, symbol: str, signal: Signal) -> list[OrderIntent]:
        snapshot = self.get_position_snapshot(symbol)
        intents: list[OrderIntent] = []
        if signal.action == 'buy':
            if snapshot.short_qty > 0:
                intents.append(OrderIntent(symbol=symbol, side='short', target_qty=0, purpose='buy:close_short'))
            intents.append(OrderIntent(symbol=symbol, side='long', target_qty=signal.qty, purpose='buy:open_long'))
        elif signal.action == 'sell':
            if snapshot.long_qty > 0:
                intents.append(OrderIntent(symbol=symbol, side='long', target_qty=0, purpose='sell:close_long'))
            intents.append(OrderIntent(symbol=symbol, side='short', target_qty=signal.qty, purpose='sell:open_short'))
        elif signal.action == 'close_long':
            intents.append(OrderIntent(symbol=symbol, side='long', target_qty=0, purpose='close_long'))
        elif signal.action == 'close_short':
            intents.append(OrderIntent(symbol=symbol, side='short', target_qty=0, purpose='close_short'))
        return intents

    def submit_order_intents(self, intents: list[OrderIntent]) -> list[ExecutionResult]:
        results: list[ExecutionResult] = []
        for intent in intents:
            request_id = uuid4().hex
            timestamp = datetime.utcnow().isoformat()
            side = PositionSide_Long if intent.side == 'long' else PositionSide_Short
            if order_target_volume is None:
                results.append(ExecutionResult(request_id, intent.purpose, intent.target_qty, False, 'gm_order_api_unavailable', timestamp))
                continue
            try:
                order_target_volume(symbol=intent.symbol, volume=max(int(intent.target_qty), 0), position_side=side, order_type=OrderType_Market)
                results.append(ExecutionResult(request_id, intent.purpose, intent.target_qty, True, 'submitted', timestamp))
            except Exception as exc:
                results.append(ExecutionResult(request_id, intent.purpose, intent.target_qty, False, f'submit_error:{exc}', timestamp))
        return results
