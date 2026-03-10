from __future__ import annotations

import csv
from pathlib import Path
from typing import Sequence

from quantframe.core.models import OrderRequest, OrderResult, StrategyDecision


class CsvReporter:
    def __init__(self, output_dir: str, enabled: bool = True) -> None:
        self.enabled = bool(enabled)
        self.output_dir = Path(output_dir)
        self.signals_path = self.output_dir / "signals.csv"
        self.targets_path = self.output_dir / "targets.csv"
        self.orders_path = self.output_dir / "orders.csv"
        if self.enabled:
            self._prepare()

    def _prepare(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._reset(self.signals_path, ["instrument_id", "symbol", "direction", "strength", "reason"])
        self._reset(self.targets_path, ["instrument_id", "symbol", "target_qty", "notional_budget", "reason"])
        self._reset(self.orders_path, ["instrument_id", "symbol", "target_qty", "delta_qty", "reason", "accepted", "result_reason"])

    def _reset(self, path: Path, header: list[str]) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(header)

    def record_decision(self, decision: StrategyDecision) -> None:
        if not self.enabled:
            return
        if decision.signal is not None:
            with self.signals_path.open("a", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(
                    [
                        decision.signal.instrument_id,
                        decision.signal.symbol,
                        decision.signal.direction,
                        f"{decision.signal.strength:.6f}",
                        decision.signal.reason,
                    ]
                )
        if decision.target is not None:
            with self.targets_path.open("a", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(
                    [
                        decision.target.instrument_id,
                        decision.target.symbol,
                        decision.target.target_qty,
                        f"{decision.target.notional_budget:.6f}",
                        decision.target.reason,
                    ]
                )

    def record_order_results(self, orders: Sequence[OrderRequest], results: Sequence[OrderResult]) -> None:
        if not self.enabled:
            return
        rows = []
        result_list = list(results)
        for index, order in enumerate(orders):
            result = result_list[index] if index < len(result_list) else OrderResult(request_id="", accepted=False, reason="missing_result")
            rows.append(
                [
                    order.instrument_id,
                    order.symbol,
                    order.target_qty,
                    order.delta_qty,
                    order.reason,
                    int(bool(result.accepted)),
                    result.reason,
                ]
            )
        if not rows:
            return
        with self.orders_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerows(rows)
