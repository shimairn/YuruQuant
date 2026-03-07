from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


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


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        text = '' if value is None else str(value).strip()
        return float(text) if text else default
    except Exception:
        return default


def _to_int(value: object, default: int = 0) -> int:
    try:
        text = '' if value is None else str(value).strip()
        return int(float(text)) if text else default
    except Exception:
        return default


def _normalize_optional(value: object) -> str:
    return '' if value is None else str(value).strip()


def _is_accepted(value: object) -> bool:
    return _normalize_optional(value).lower() in {'1', 'true', 'yes'}


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open('r', newline='', encoding='utf-8-sig') as handle:
        return list(csv.DictReader(handle))


def _theoretical_stop_metrics(
    direction: int,
    qty: int,
    multiplier: float,
    entry_price: float,
    initial_stop_loss: float,
    protected_stop_price: float,
    exit_trigger: str,
) -> tuple[float | None, float | None, float | None, float | None]:
    theoretical_stop_price: float | None = None
    if exit_trigger == 'hard_stop':
        theoretical_stop_price = initial_stop_loss if initial_stop_loss > 0 else None
    elif exit_trigger == 'protected_stop':
        theoretical_stop_price = protected_stop_price if protected_stop_price > 0 else None

    if theoretical_stop_price is None:
        return None, None, None, None

    pnl_points = (theoretical_stop_price - entry_price) if direction > 0 else (entry_price - theoretical_stop_price)
    theoretical_gross = pnl_points * qty * multiplier
    return theoretical_stop_price, pnl_points, theoretical_gross, abs(theoretical_gross)


def _load_execution_lookup(executions_path: Path | None) -> dict[tuple[str, str], list[dict[str, str]]]:
    if executions_path is None or not executions_path.exists():
        return {}
    lookup: dict[tuple[str, str], list[dict[str, str]]] = {}
    for row in load_csv_rows(executions_path):
        if not _is_accepted(row.get('accepted')):
            continue
        campaign_id = _normalize_optional(row.get('campaign_id'))
        signal_action = _normalize_optional(row.get('signal_action'))
        if not campaign_id or not signal_action:
            continue
        lookup.setdefault((campaign_id, signal_action), []).append(row)
    return lookup


def _select_execution(rows: list[dict[str, str]], signal_action: str) -> dict[str, str] | None:
    if not rows:
        return None
    if signal_action in {'buy', 'sell'}:
        for row in rows:
            if 'open_' in _normalize_optional(row.get('intended_action')):
                return row
    else:
        for row in rows:
            intended_action = _normalize_optional(row.get('intended_action'))
            if intended_action == signal_action or intended_action.startswith('close_'):
                return row
    return rows[-1]


def build_trade_records(
    signals_path: Path,
    multiplier_by_csymbol: Mapping[str, float],
    executions_path: Path | None = None,
) -> list[TradeRecord]:
    rows = load_csv_rows(signals_path)
    execution_lookup = _load_execution_lookup(executions_path)
    exits_by_campaign: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        action = _normalize_optional(row.get('action'))
        if action.startswith('close_'):
            exits_by_campaign.setdefault(_normalize_optional(row.get('campaign_id')), []).append(row)

    trades: list[TradeRecord] = []
    for row in rows:
        action = _normalize_optional(row.get('action'))
        if action not in {'buy', 'sell'}:
            continue

        campaign_id = _normalize_optional(row.get('campaign_id'))
        exit_rows = exits_by_campaign.get(campaign_id)
        if not exit_rows:
            continue
        exit_row = exit_rows[0]

        csymbol = _normalize_optional(row.get('csymbol'))
        direction = _to_int(row.get('direction'))
        qty = _to_int(row.get('qty'))
        entry_signal_price = _to_float(row.get('price'))
        exit_signal_price = _to_float(exit_row.get('price'))
        initial_stop_loss = _to_float(row.get('stop_or_trigger'))
        protected_stop_price = _to_float(row.get('protected_stop_price'))
        exit_trigger = _normalize_optional(exit_row.get('stop_or_trigger'))
        phase_at_exit = _normalize_optional(exit_row.get('phase'))
        mfe_r = _to_float(exit_row.get('mfe_r'))
        multiplier = float(multiplier_by_csymbol.get(csymbol, 1.0))

        entry_execution = _select_execution(execution_lookup.get((campaign_id, action), []), action)
        exit_action = _normalize_optional(exit_row.get('action'))
        exit_execution = _select_execution(execution_lookup.get((campaign_id, exit_action), []), exit_action)

        entry_fill_price = _to_float(entry_execution.get('fill_price') if entry_execution else None, entry_signal_price)
        exit_fill_price = _to_float(exit_execution.get('fill_price') if exit_execution else None, exit_signal_price)
        entry_fill_ts = _normalize_optional(entry_execution.get('fill_ts') if entry_execution else row.get('ts')) or _normalize_optional(row.get('ts'))
        exit_fill_ts = _normalize_optional(exit_execution.get('fill_ts') if exit_execution else exit_row.get('ts')) or _normalize_optional(exit_row.get('ts'))
        entry_signal_ts = _normalize_optional(row.get('ts'))
        exit_signal_ts = _normalize_optional(exit_row.get('ts'))
        exit_execution_regime = _normalize_optional(exit_execution.get('execution_regime') if exit_execution else None) or 'normal'
        exit_fill_gap_points = _to_float(exit_execution.get('fill_gap_points') if exit_execution else None)
        exit_fill_gap_atr = _to_float(exit_execution.get('fill_gap_atr') if exit_execution else None)

        pnl_points = (exit_fill_price - entry_fill_price) if direction > 0 else (entry_fill_price - exit_fill_price)
        gross_pnl = pnl_points * qty * multiplier

        theoretical_stop_price, _, theoretical_stop_gross_pnl, theoretical_scale = _theoretical_stop_metrics(
            direction=direction,
            qty=qty,
            multiplier=multiplier,
            entry_price=entry_fill_price,
            initial_stop_loss=initial_stop_loss,
            protected_stop_price=protected_stop_price,
            exit_trigger=exit_trigger,
        )

        overshoot_pnl: float | None = None
        overshoot_ratio: float | None = None
        if theoretical_stop_gross_pnl is not None:
            overshoot_pnl = theoretical_stop_gross_pnl - gross_pnl
            if theoretical_scale and theoretical_scale > 0:
                overshoot_ratio = overshoot_pnl / theoretical_scale

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
                exit_reason=_normalize_optional(exit_row.get('reason')),
                exit_trigger=exit_trigger,
                phase_at_exit=phase_at_exit,
                mfe_r=mfe_r,
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


def build_trade_diagnostics(trades: list[TradeRecord]) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    for trade in trades:
        diagnostics.append(
            {
                'campaign_id': trade.campaign_id,
                'csymbol': trade.csymbol,
                'entry_ts': trade.entry_fill_ts,
                'exit_ts': trade.exit_fill_ts,
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


def _overshoot_stats(trades: list[TradeRecord], trigger: str) -> tuple[float, float]:
    values = [trade.overshoot_pnl for trade in trades if trade.exit_trigger == trigger and trade.overshoot_pnl is not None]
    if not values:
        return 0.0, 0.0
    average = sum(values) / len(values)
    maximum = max(max(values), 0.0)
    return average, maximum


def _session_restart_gap_stats(trades: list[TradeRecord]) -> tuple[int, float, float, float]:
    stop_trades = [trade for trade in trades if trade.exit_trigger in {'hard_stop', 'protected_stop'}]
    if not stop_trades:
        return 0, 0.0, 0.0, 0.0
    gap_trades = [trade for trade in stop_trades if trade.exit_execution_regime == 'session_restart_gap']
    stop_overshoot_total = sum(float(trade.overshoot_pnl or 0.0) for trade in stop_trades if trade.overshoot_pnl is not None)
    gap_overshoot_total = sum(float(trade.overshoot_pnl or 0.0) for trade in gap_trades if trade.overshoot_pnl is not None)
    gap_ratio = len(gap_trades) / len(stop_trades)
    overshoot_ratio = gap_overshoot_total / stop_overshoot_total if stop_overshoot_total > 0 else 0.0
    return len(gap_trades), gap_ratio, gap_overshoot_total, overshoot_ratio


def _session_restart_gap_exit_stats(trades: list[TradeRecord]) -> tuple[int, float]:
    if not trades:
        return 0, 0.0
    gap_trades = [trade for trade in trades if trade.exit_execution_regime == 'session_restart_gap']
    return len(gap_trades), len(gap_trades) / len(trades)


def _session_restart_gap_portfolio_halt_stats(trades: list[TradeRecord]) -> tuple[int, float]:
    portfolio_halts = [trade for trade in trades if trade.exit_trigger == 'portfolio_halt']
    if not portfolio_halts:
        return 0, 0.0
    gap_halts = [trade for trade in portfolio_halts if trade.exit_execution_regime == 'session_restart_gap']
    return len(gap_halts), len(gap_halts) / len(portfolio_halts)


def summarize_trades(trades: list[TradeRecord]) -> dict[str, float | int]:
    if not trades:
        return {
            'trades': 0,
            'wins': 0,
            'losses': 0,
            'win_rate': 0.0,
            'hard_stop_count': 0,
            'hard_stop_ratio': 0.0,
            'protected_stop_count': 0,
            'protected_stop_ratio': 0.0,
            'hourly_ma_stop_count': 0,
            'hourly_ma_stop_ratio': 0.0,
            'ascended_exit_count': 0,
            'ascended_exit_ratio': 0.0,
            'ascended_protected_stop_count': 0,
            'ascended_hourly_ma_stop_count': 0,
            'armed_flush_count': 0,
            'armed_flush_ratio': 0.0,
            'portfolio_halt_count': 0,
            'session_restart_gap_exit_count': 0,
            'session_restart_gap_exit_ratio': 0.0,
            'session_restart_gap_portfolio_halt_count': 0,
            'session_restart_gap_portfolio_halt_ratio': 0.0,
            'session_restart_gap_stop_count': 0,
            'session_restart_gap_stop_ratio': 0.0,
            'session_restart_gap_overshoot_sum': 0.0,
            'session_restart_gap_overshoot_ratio': 0.0,
            'hard_stop_overshoot_avg': 0.0,
            'hard_stop_overshoot_max': 0.0,
            'protected_stop_overshoot_avg': 0.0,
            'protected_stop_overshoot_max': 0.0,
            'avg_win_pnl': 0.0,
            'avg_loss_pnl': 0.0,
            'avg_win_loss_ratio': 0.0,
            'best_trade_pnl': 0.0,
            'worst_trade_pnl': 0.0,
        }

    wins = [trade.gross_pnl for trade in trades if trade.gross_pnl > 0]
    losses = [trade.gross_pnl for trade in trades if trade.gross_pnl <= 0]
    trades_count = len(trades)
    hard_stop_count = sum(1 for trade in trades if trade.exit_trigger == 'hard_stop')
    protected_stop_count = sum(1 for trade in trades if trade.exit_trigger == 'protected_stop')
    hourly_ma_stop_count = sum(1 for trade in trades if trade.exit_trigger == 'hourly_ma_stop')
    ascended_exit_count = sum(1 for trade in trades if trade.phase_at_exit == 'ascended')
    ascended_protected_stop_count = sum(1 for trade in trades if trade.phase_at_exit == 'ascended' and trade.exit_trigger == 'protected_stop')
    ascended_hourly_ma_stop_count = sum(1 for trade in trades if trade.phase_at_exit == 'ascended' and trade.exit_trigger == 'hourly_ma_stop')
    armed_flush_count = sum(1 for trade in trades if trade.exit_trigger == 'armed_flush')
    portfolio_halt_count = sum(1 for trade in trades if trade.exit_trigger == 'portfolio_halt')
    session_restart_gap_exit_count, session_restart_gap_exit_ratio = _session_restart_gap_exit_stats(trades)
    session_restart_gap_portfolio_halt_count, session_restart_gap_portfolio_halt_ratio = _session_restart_gap_portfolio_halt_stats(trades)
    session_restart_gap_stop_count, session_restart_gap_stop_ratio, session_restart_gap_overshoot_sum, session_restart_gap_overshoot_ratio = _session_restart_gap_stats(trades)
    hard_stop_overshoot_avg, hard_stop_overshoot_max = _overshoot_stats(trades, 'hard_stop')
    protected_stop_overshoot_avg, protected_stop_overshoot_max = _overshoot_stats(trades, 'protected_stop')
    avg_win_pnl = sum(wins) / len(wins) if wins else 0.0
    avg_loss_pnl = sum(losses) / len(losses) if losses else 0.0
    avg_win_loss_ratio = abs(avg_win_pnl / avg_loss_pnl) if wins and losses and avg_loss_pnl != 0 else 0.0

    return {
        'trades': trades_count,
        'wins': len(wins),
        'losses': len(losses),
        'win_rate': len(wins) / trades_count,
        'hard_stop_count': hard_stop_count,
        'hard_stop_ratio': hard_stop_count / trades_count,
        'protected_stop_count': protected_stop_count,
        'protected_stop_ratio': protected_stop_count / trades_count,
        'hourly_ma_stop_count': hourly_ma_stop_count,
        'hourly_ma_stop_ratio': hourly_ma_stop_count / trades_count,
        'ascended_exit_count': ascended_exit_count,
        'ascended_exit_ratio': ascended_exit_count / trades_count,
        'ascended_protected_stop_count': ascended_protected_stop_count,
        'ascended_hourly_ma_stop_count': ascended_hourly_ma_stop_count,
        'armed_flush_count': armed_flush_count,
        'armed_flush_ratio': armed_flush_count / trades_count,
        'portfolio_halt_count': portfolio_halt_count,
        'session_restart_gap_exit_count': session_restart_gap_exit_count,
        'session_restart_gap_exit_ratio': session_restart_gap_exit_ratio,
        'session_restart_gap_portfolio_halt_count': session_restart_gap_portfolio_halt_count,
        'session_restart_gap_portfolio_halt_ratio': session_restart_gap_portfolio_halt_ratio,
        'session_restart_gap_stop_count': session_restart_gap_stop_count,
        'session_restart_gap_stop_ratio': session_restart_gap_stop_ratio,
        'session_restart_gap_overshoot_sum': session_restart_gap_overshoot_sum,
        'session_restart_gap_overshoot_ratio': session_restart_gap_overshoot_ratio,
        'hard_stop_overshoot_avg': hard_stop_overshoot_avg,
        'hard_stop_overshoot_max': hard_stop_overshoot_max,
        'protected_stop_overshoot_avg': protected_stop_overshoot_avg,
        'protected_stop_overshoot_max': protected_stop_overshoot_max,
        'avg_win_pnl': avg_win_pnl,
        'avg_loss_pnl': avg_loss_pnl,
        'avg_win_loss_ratio': avg_win_loss_ratio,
        'best_trade_pnl': max((trade.gross_pnl for trade in trades), default=0.0),
        'worst_trade_pnl': min((trade.gross_pnl for trade in trades), default=0.0),
    }


def _collapse_portfolio_daily_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    collapsed: list[dict[str, str]] = []
    for row in rows:
        date = _normalize_optional(row.get('date'))
        if not date:
            continue
        if collapsed and _normalize_optional(collapsed[-1].get('date')) == date:
            latest = dict(row)
            latest['equity_start'] = collapsed[-1].get('equity_start', row.get('equity_start', ''))
            collapsed[-1] = latest
            continue
        collapsed.append(dict(row))
    return collapsed


def summarize_portfolio_daily(portfolio_daily_path: Path) -> dict[str, float | int]:
    raw_rows = load_csv_rows(portfolio_daily_path)
    if not raw_rows:
        return {
            'days': 0,
            'start_equity': 0.0,
            'end_equity': 0.0,
            'net_profit': 0.0,
            'return_ratio': 0.0,
            'max_drawdown': 0.0,
            'halt_days': 0,
            'final_realized_pnl': 0.0,
        }

    rows = _collapse_portfolio_daily_rows(raw_rows)
    start_equity = _to_float(rows[0].get('equity_start'))
    end_equity = _to_float(rows[-1].get('equity_end'))
    max_drawdown = max(_to_float(row.get('drawdown_ratio')) for row in raw_rows)
    halt_days = len({_normalize_optional(row.get('date')) for row in raw_rows if _normalize_optional(row.get('halt_flag')) == '1'})
    final_realized_pnl = _to_float(rows[-1].get('realized_pnl'))
    net_profit = end_equity - start_equity
    return {
        'days': len(rows),
        'start_equity': start_equity,
        'end_equity': end_equity,
        'net_profit': net_profit,
        'return_ratio': (net_profit / start_equity) if start_equity else 0.0,
        'max_drawdown': max_drawdown,
        'halt_days': halt_days,
        'final_realized_pnl': final_realized_pnl,
    }


def summarize_backtest_run(
    signals_path: Path,
    portfolio_daily_path: Path,
    multiplier_by_csymbol: Mapping[str, float],
    executions_path: Path | None = None,
) -> dict[str, float | int]:
    trades = build_trade_records(signals_path, multiplier_by_csymbol, executions_path)
    summary: dict[str, float | int] = {}
    summary.update(summarize_trades(trades))
    summary.update(summarize_portfolio_daily(portfolio_daily_path))
    return summary











