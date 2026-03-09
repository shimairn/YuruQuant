from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


TRADE_DIAGNOSTIC_COLUMNS = [
    'campaign_id',
    'csymbol',
    'entry_ts',
    'exit_ts',
    'entry_signal_ts',
    'entry_fill_ts',
    'exit_signal_ts',
    'exit_fill_ts',
    'exit_trigger',
    'phase_at_exit',
    'entry_price',
    'exit_price',
    'entry_signal_price',
    'entry_fill_price',
    'exit_signal_price',
    'exit_fill_price',
    'initial_stop_loss',
    'protected_stop_price',
    'theoretical_stop_price',
    'theoretical_stop_gross_pnl',
    'actual_gross_pnl',
    'overshoot_pnl',
    'overshoot_ratio',
    'exit_execution_regime',
    'exit_fill_gap_points',
    'exit_fill_gap_atr',
]


@dataclass(frozen=True)
class TradeRecord:
    campaign_id: str
    csymbol: str
    entry_signal_ts: str
    entry_fill_ts: str
    exit_signal_ts: str
    exit_fill_ts: str
    direction: int
    qty: int
    entry_signal_price: float
    entry_fill_price: float
    exit_signal_price: float
    exit_fill_price: float
    initial_stop_loss: float
    protected_stop_price: float
    exit_reason: str
    exit_trigger: str
    phase_at_exit: str
    mfe_r: float
    multiplier: float
    pnl_points: float
    gross_pnl: float
    theoretical_stop_price: float | None
    theoretical_stop_gross_pnl: float | None
    overshoot_pnl: float | None
    overshoot_ratio: float | None
    exit_execution_regime: str
    exit_fill_gap_points: float
    exit_fill_gap_atr: float


def to_float(value: object, default: float = 0.0) -> float:
    try:
        text = '' if value is None else str(value).strip()
        return float(text) if text else default
    except Exception:
        return default


def to_int(value: object, default: int = 0) -> int:
    try:
        text = '' if value is None else str(value).strip()
        return int(float(text)) if text else default
    except Exception:
        return default


def normalize_optional(value: object) -> str:
    return '' if value is None else str(value).strip()


def is_accepted(value: object) -> bool:
    return normalize_optional(value).lower() in {'1', 'true', 'yes'}


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open('r', newline='', encoding='utf-8-sig') as handle:
        return list(csv.DictReader(handle))


def realized_stop_prices(entry_signal_price: float, entry_fill_price: float, initial_stop_loss: float, protected_stop_price: float) -> tuple[float, float]:
    fill_shift = float(entry_fill_price) - float(entry_signal_price)
    return float(initial_stop_loss) + fill_shift, float(protected_stop_price) + fill_shift


def theoretical_stop_metrics(
    direction: int,
    qty: int,
    multiplier: float,
    entry_price: float,
    initial_stop_loss: float,
    protected_stop_price: float,
    exit_trigger: str,
) -> tuple[float | None, float | None, float | None, float | None]:
    theoretical_stop_price: float | None = None
    if exit_trigger == 'hard_stop' and initial_stop_loss > 0:
        theoretical_stop_price = initial_stop_loss
    elif exit_trigger == 'protected_stop' and protected_stop_price > 0:
        theoretical_stop_price = protected_stop_price

    if theoretical_stop_price is None:
        return None, None, None, None

    pnl_points = (theoretical_stop_price - entry_price) if direction > 0 else (entry_price - theoretical_stop_price)
    theoretical_gross = pnl_points * qty * multiplier
    return theoretical_stop_price, pnl_points, theoretical_gross, abs(theoretical_gross)


def load_execution_lookup(executions_path: Path | None) -> dict[tuple[str, str], list[dict[str, str]]]:
    if executions_path is None or not executions_path.exists():
        return {}
    lookup: dict[tuple[str, str], list[dict[str, str]]] = {}
    for row in load_csv_rows(executions_path):
        if not is_accepted(row.get('accepted')):
            continue
        campaign_id = normalize_optional(row.get('campaign_id'))
        signal_action = normalize_optional(row.get('signal_action'))
        if not campaign_id or not signal_action:
            continue
        lookup.setdefault((campaign_id, signal_action), []).append(row)
    return lookup


def select_execution(rows: list[dict[str, str]], signal_action: str) -> dict[str, str] | None:
    if not rows:
        return None
    if signal_action in {'buy', 'sell'}:
        for row in rows:
            if 'open_' in normalize_optional(row.get('intended_action')):
                return row
    else:
        for row in rows:
            intended_action = normalize_optional(row.get('intended_action'))
            if intended_action == signal_action or intended_action.startswith('close_'):
                return row
    return rows[-1]


def build_trade_records(
    signals_path: Path,
    multiplier_by_csymbol: Mapping[str, float],
    executions_path: Path | None = None,
) -> list[TradeRecord]:
    rows = load_csv_rows(signals_path)
    execution_lookup = load_execution_lookup(executions_path)
    exits_by_campaign: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        action = normalize_optional(row.get('action'))
        if action.startswith('close_'):
            exits_by_campaign.setdefault(normalize_optional(row.get('campaign_id')), []).append(row)

    trades: list[TradeRecord] = []
    for row in rows:
        action = normalize_optional(row.get('action'))
        if action not in {'buy', 'sell'}:
            continue

        campaign_id = normalize_optional(row.get('campaign_id'))
        exit_rows = exits_by_campaign.get(campaign_id, [])
        if not exit_rows:
            continue
        exit_row = exit_rows[-1]

        csymbol = normalize_optional(row.get('csymbol'))
        direction = 1 if action == 'buy' else -1
        qty = to_int(row.get('qty'))
        multiplier = float(multiplier_by_csymbol.get(csymbol, 1.0) or 1.0)
        entry_signal_price = to_float(row.get('price'))
        exit_signal_price = to_float(exit_row.get('price'))
        entry_action = action
        exit_action = normalize_optional(exit_row.get('action'))
        entry_execution = select_execution(execution_lookup.get((campaign_id, entry_action), []), entry_action)
        exit_execution = select_execution(execution_lookup.get((campaign_id, exit_action), []), exit_action)
        entry_fill_price = to_float(entry_execution.get('fill_price') if entry_execution else None, entry_signal_price)
        exit_fill_price = to_float(exit_execution.get('fill_price') if exit_execution else None, exit_signal_price)
        entry_signal_ts = normalize_optional(row.get('ts'))
        entry_fill_ts = normalize_optional(entry_execution.get('fill_ts') if entry_execution else entry_signal_ts)
        exit_signal_ts = normalize_optional(exit_row.get('ts'))
        exit_fill_ts = normalize_optional(exit_execution.get('fill_ts') if exit_execution else exit_signal_ts)
        exit_execution_regime = normalize_optional(exit_execution.get('execution_regime') if exit_execution else '')
        exit_fill_gap_points = to_float(exit_execution.get('fill_gap_points') if exit_execution else None)
        exit_fill_gap_atr = to_float(exit_execution.get('fill_gap_atr') if exit_execution else None)
        initial_stop_loss, protected_stop_price = realized_stop_prices(
            entry_signal_price=entry_signal_price,
            entry_fill_price=entry_fill_price,
            initial_stop_loss=to_float(row.get('stop_or_trigger')),
            protected_stop_price=to_float(row.get('protected_stop_price')),
        )
        pnl_points = (exit_fill_price - entry_fill_price) if direction > 0 else (entry_fill_price - exit_fill_price)
        gross_pnl = pnl_points * qty * multiplier
        theoretical_stop_price, _, theoretical_stop_gross_pnl, theoretical_stop_abs = theoretical_stop_metrics(
            direction=direction,
            qty=qty,
            multiplier=multiplier,
            entry_price=entry_fill_price,
            initial_stop_loss=initial_stop_loss,
            protected_stop_price=protected_stop_price,
            exit_trigger=normalize_optional(exit_row.get('stop_or_trigger')),
        )
        overshoot_pnl = None
        overshoot_ratio = None
        if theoretical_stop_abs and theoretical_stop_abs > 0 and theoretical_stop_gross_pnl is not None:
            overshoot_pnl = theoretical_stop_gross_pnl - gross_pnl
            overshoot_ratio = max(overshoot_pnl, 0.0) / theoretical_stop_abs

        trades.append(
            TradeRecord(
                campaign_id=campaign_id,
                csymbol=csymbol,
                entry_signal_ts=entry_signal_ts,
                entry_fill_ts=entry_fill_ts,
                exit_signal_ts=exit_signal_ts,
                exit_fill_ts=exit_fill_ts,
                direction=direction,
                qty=qty,
                entry_signal_price=entry_signal_price,
                entry_fill_price=entry_fill_price,
                exit_signal_price=exit_signal_price,
                exit_fill_price=exit_fill_price,
                initial_stop_loss=initial_stop_loss,
                protected_stop_price=protected_stop_price,
                exit_reason=normalize_optional(exit_row.get('reason')),
                exit_trigger=normalize_optional(exit_row.get('stop_or_trigger')),
                phase_at_exit=normalize_optional(exit_row.get('phase')),
                mfe_r=to_float(exit_row.get('mfe_r')),
                multiplier=multiplier,
                pnl_points=pnl_points,
                gross_pnl=gross_pnl,
                theoretical_stop_price=theoretical_stop_price,
                theoretical_stop_gross_pnl=theoretical_stop_gross_pnl,
                overshoot_pnl=overshoot_pnl,
                overshoot_ratio=overshoot_ratio,
                exit_execution_regime=exit_execution_regime,
                exit_fill_gap_points=exit_fill_gap_points,
                exit_fill_gap_atr=exit_fill_gap_atr,
            )
        )
    return trades


__all__ = [
    'TRADE_DIAGNOSTIC_COLUMNS',
    'TradeRecord',
    'build_trade_records',
    'is_accepted',
    'load_csv_rows',
    'normalize_optional',
    'to_float',
    'to_int',
]
