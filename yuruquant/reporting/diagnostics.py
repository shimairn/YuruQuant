from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from yuruquant.reporting.trade_records import TRADE_DIAGNOSTIC_COLUMNS, TradeRecord


def build_trade_diagnostics(trades: list[TradeRecord]) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    for trade in trades:
        diagnostics.append(
            {
                'campaign_id': trade.campaign_id,
                'csymbol': trade.csymbol,
                'entry_ts': trade.entry_signal_ts,
                'exit_ts': trade.exit_signal_ts,
                'entry_signal_ts': trade.entry_signal_ts,
                'entry_fill_ts': trade.entry_fill_ts,
                'exit_signal_ts': trade.exit_signal_ts,
                'exit_fill_ts': trade.exit_fill_ts,
                'exit_trigger': trade.exit_trigger,
                'phase_at_exit': trade.phase_at_exit,
                'entry_price': trade.entry_fill_price,
                'exit_price': trade.exit_fill_price,
                'entry_signal_price': trade.entry_signal_price,
                'entry_fill_price': trade.entry_fill_price,
                'exit_signal_price': trade.exit_signal_price,
                'exit_fill_price': trade.exit_fill_price,
                'initial_stop_loss': trade.initial_stop_loss,
                'protected_stop_price': trade.protected_stop_price,
                'theoretical_stop_price': trade.theoretical_stop_price,
                'theoretical_stop_gross_pnl': trade.theoretical_stop_gross_pnl,
                'actual_gross_pnl': trade.gross_pnl,
                'overshoot_pnl': trade.overshoot_pnl,
                'overshoot_ratio': trade.overshoot_ratio,
                'exit_execution_regime': trade.exit_execution_regime,
                'exit_fill_gap_points': trade.exit_fill_gap_points,
                'exit_fill_gap_atr': trade.exit_fill_gap_atr,
            }
        )
    return diagnostics


def write_trade_diagnostics_csv(path: Path, diagnostics: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=TRADE_DIAGNOSTIC_COLUMNS)
        writer.writeheader()
        for row in diagnostics:
            writer.writerow({column: row.get(column, '') for column in TRADE_DIAGNOSTIC_COLUMNS})


__all__ = ['build_trade_diagnostics', 'write_trade_diagnostics_csv']
