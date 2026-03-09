from __future__ import annotations

from collections import defaultdict
from datetime import date

from yuruquant.core.time import next_exchange_trade_date, to_exchange_trade_day
from yuruquant.reporting.trade_records import TradeRecord


def trade_day_span(trade: TradeRecord) -> tuple[str, ...]:
    start_day = date.fromisoformat(to_exchange_trade_day(trade.entry_fill_ts))
    end_day = date.fromisoformat(to_exchange_trade_day(trade.exit_fill_ts))
    if end_day < start_day:
        end_day = start_day
    days: list[str] = []
    current_day = start_day
    while current_day <= end_day:
        days.append(current_day.isoformat())
        if current_day == end_day:
            break
        current_day = next_exchange_trade_date(current_day)
    return tuple(days)


def build_trade_day_trade_map(trades: list[TradeRecord]) -> dict[str, list[TradeRecord]]:
    trade_map: dict[str, list[TradeRecord]] = defaultdict(list)
    for trade in trades:
        for trade_day in trade_day_span(trade):
            trade_map[trade_day].append(trade)
    return trade_map


__all__ = ['build_trade_day_trade_map', 'trade_day_span']
