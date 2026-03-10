from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from quantframe.core.models import Bar, Instrument, OrderRequest, PortfolioSnapshot, Position, SignalDecision, StrategyDecision, TargetPosition
from quantframe.platforms.base import PlatformAdapter


@dataclass(frozen=True)
class DecisionContext:
    instrument: Instrument
    bars: tuple[Bar, ...]
    portfolio: PortfolioSnapshot
    position: Position


@runtime_checkable
class SignalModel(Protocol):
    def generate(self, context: DecisionContext) -> SignalDecision | None: ...


@runtime_checkable
class TargetAllocator(Protocol):
    def allocate(self, context: DecisionContext, signal: SignalDecision | None) -> TargetPosition | None: ...


@runtime_checkable
class RiskOverlay(Protocol):
    def apply(self, context: DecisionContext, target: TargetPosition | None) -> TargetPosition | None: ...


@runtime_checkable
class ExecutionPlanner(Protocol):
    def plan(self, context: DecisionContext, target: TargetPosition | None) -> tuple[OrderRequest, ...]: ...


@dataclass(frozen=True)
class TrendStrategy:
    name: str
    decision_frequency: str
    history_bars: int
    signal_model: SignalModel
    target_allocator: TargetAllocator
    risk_overlay: RiskOverlay
    execution_planner: ExecutionPlanner

    def evaluate(self, context: DecisionContext) -> StrategyDecision:
        signal = self.signal_model.generate(context)
        target = self.target_allocator.allocate(context, signal)
        target = self.risk_overlay.apply(context, target)
        orders = self.execution_planner.plan(context, target)
        return StrategyDecision(signal=signal, target=target, orders=orders)


class TrendEngine:
    def __init__(self, platform: PlatformAdapter, instruments: Sequence[Instrument], strategy: TrendStrategy, reporter) -> None:
        self.platform = platform
        self.strategy = strategy
        self.reporter = reporter
        self.instruments_by_id = {item.instrument_id: item for item in instruments}
        self.bar_store: dict[str, deque[Bar]] = {
            item.instrument_id: deque(maxlen=max(strategy.history_bars * 2, strategy.history_bars))
            for item in instruments
        }
        self._primed: set[str] = set()

    def _prime_history(self, instrument: Instrument) -> None:
        if instrument.instrument_id in self._primed:
            return
        history = self.platform.fetch_history(instrument, self.strategy.decision_frequency, self.strategy.history_bars)
        store = self.bar_store[instrument.instrument_id]
        merged = {getattr(bar, "timestamp"): bar for bar in store}
        for bar in history:
            merged[getattr(bar, "timestamp")] = bar
        ordered = sorted(merged.values(), key=lambda item: getattr(item, "timestamp"))
        store.clear()
        for bar in ordered[-store.maxlen :]:
            store.append(bar)
        self._primed.add(instrument.instrument_id)

    def _append_bar(self, bar: Bar) -> None:
        store = self.bar_store.get(bar.instrument_id)
        if store is None:
            return
        if store and store[-1].timestamp == bar.timestamp:
            store[-1] = bar
            return
        store.append(bar)

    def on_bars(self, bars: Sequence[Bar]) -> list[StrategyDecision]:
        decisions: list[StrategyDecision] = []
        grouped_ids: dict[str, list[Bar]] = defaultdict(list)
        for bar in bars:
            if bar.instrument_id not in self.instruments_by_id:
                continue
            self._append_bar(bar)
            if str(bar.frequency).strip() == self.strategy.decision_frequency:
                grouped_ids[bar.instrument_id].append(bar)

        for instrument_id in sorted(grouped_ids):
            instrument = self.instruments_by_id[instrument_id]
            self._prime_history(instrument)
            store = self.bar_store[instrument_id]
            if len(store) < self.strategy.history_bars:
                continue
            context = DecisionContext(
                instrument=instrument,
                bars=tuple(store),
                portfolio=self.platform.get_portfolio_snapshot(),
                position=self.platform.get_position(instrument),
            )
            decision = self.strategy.evaluate(context)
            self.reporter.record_decision(decision)
            if decision.orders:
                results = self.platform.submit_orders(decision.orders)
                self.reporter.record_order_results(decision.orders, results)
            decisions.append(decision)
        return decisions
