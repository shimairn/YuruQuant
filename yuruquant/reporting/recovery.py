from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from yuruquant.reporting.csv_utils import load_csv_rows, normalize_optional, to_float
from yuruquant.reporting.summary import collapse_portfolio_daily_rows
from yuruquant.reporting.trade_day_activity import build_trade_day_trade_map
from yuruquant.reporting.trade_records import TradeRecord


HALT_RECOVERY_SUMMARY_COLUMNS = [
    'total_days',
    'halt_days',
    'halt_day_share',
    'first_halt_day',
    'first_halt_days_from_start',
    'first_halt_drawdown_ratio',
    'first_halt_day_active_positions',
    'halt_streak_count',
    'max_consecutive_halt_days',
    'lockout_halt_days',
    'lockout_halt_share',
    'post_first_halt_trade_entries',
    'recovery_to_peak_days_after_first_halt',
    'max_drawdown_duration_days',
    'ended_in_drawdown',
    'recovery_assessment',
]

HALT_STREAK_COLUMNS = [
    'streak_id',
    'start_day',
    'end_day',
    'halt_days',
    'lockout_days',
    'start_drawdown_ratio',
    'end_drawdown_ratio',
    'start_equity_end',
    'end_equity_end',
    'recovered_to_peak',
    'days_to_recover_peak',
]

DRAWDOWN_EPISODE_COLUMNS = [
    'episode_id',
    'start_day',
    'trough_day',
    'end_day',
    'duration_days',
    'max_drawdown_ratio',
    'recovered',
]


@dataclass(frozen=True)
class HaltRecoveryReport:
    summary: dict[str, float | int | str]
    halt_streak_rows: tuple[dict[str, float | int | str], ...]
    drawdown_episode_rows: tuple[dict[str, float | int | str], ...]


def _load_daily_rows(portfolio_daily_path: Path) -> list[dict[str, str]]:
    rows = collapse_portfolio_daily_rows(load_csv_rows(portfolio_daily_path))
    return [row for row in rows if normalize_optional(row.get('date'))]


def _recovery_assessment(
    halt_days: int,
    lockout_halt_days: int,
    post_first_halt_trade_entries: int,
    max_consecutive_halt_days: int,
    recovery_to_peak_days_after_first_halt: int | None,
) -> str:
    if halt_days <= 0:
        return 'no_halt'
    lockout_share = (lockout_halt_days / halt_days) if halt_days else 0.0
    if recovery_to_peak_days_after_first_halt is not None:
        return 'recovered_after_halt'
    if lockout_share >= 0.65 and post_first_halt_trade_entries <= 0:
        return 'persistent_lockout'
    if max_consecutive_halt_days >= 5:
        return 'extended_drawdown_stall'
    return 'active_halt_stress'


def _build_halt_streak_rows(
    rows: list[dict[str, str]],
    active_positions_by_day: dict[str, int],
) -> tuple[list[dict[str, float | int | str]], int]:
    streaks: list[dict[str, float | int | str]] = []
    streak_id = 0
    index = 0
    while index < len(rows):
        if normalize_optional(rows[index].get('halt_flag')) != '1':
            index += 1
            continue
        start_index = index
        peak_before = to_float(rows[index].get('equity_peak'))
        while index + 1 < len(rows) and normalize_optional(rows[index + 1].get('halt_flag')) == '1':
            index += 1
        end_index = index
        streak_id += 1
        recovery_index = None
        for probe in range(end_index + 1, len(rows)):
            if to_float(rows[probe].get('equity_end')) >= peak_before and peak_before > 0:
                recovery_index = probe
                break
        streak_days = rows[start_index : end_index + 1]
        lockout_days = sum(
            1 for row in streak_days if active_positions_by_day.get(normalize_optional(row.get('date')), 0) <= 0
        )
        streaks.append(
            {
                'streak_id': streak_id,
                'start_day': normalize_optional(rows[start_index].get('date')),
                'end_day': normalize_optional(rows[end_index].get('date')),
                'halt_days': len(streak_days),
                'lockout_days': lockout_days,
                'start_drawdown_ratio': to_float(rows[start_index].get('drawdown_ratio')),
                'end_drawdown_ratio': to_float(rows[end_index].get('drawdown_ratio')),
                'start_equity_end': to_float(rows[start_index].get('equity_end')),
                'end_equity_end': to_float(rows[end_index].get('equity_end')),
                'recovered_to_peak': 1 if recovery_index is not None else 0,
                'days_to_recover_peak': (recovery_index - start_index) if recovery_index is not None else '',
            }
        )
        index += 1
    max_consecutive = max((int(row.get('halt_days', 0) or 0) for row in streaks), default=0)
    return streaks, max_consecutive


def _build_drawdown_episode_rows(rows: list[dict[str, str]]) -> tuple[list[dict[str, float | int | str]], int]:
    episodes: list[dict[str, float | int | str]] = []
    episode_id = 0
    index = 0
    max_duration = 0
    while index < len(rows):
        if to_float(rows[index].get('drawdown_ratio')) <= 0.0:
            index += 1
            continue
        start_index = index
        trough_index = index
        trough_ratio = to_float(rows[index].get('drawdown_ratio'))
        while index + 1 < len(rows) and to_float(rows[index + 1].get('drawdown_ratio')) > 0.0:
            index += 1
            current_ratio = to_float(rows[index].get('drawdown_ratio'))
            if current_ratio >= trough_ratio:
                trough_ratio = current_ratio
                trough_index = index
        end_index = index
        recovery_index = end_index + 1 if end_index + 1 < len(rows) and to_float(rows[end_index + 1].get('drawdown_ratio')) <= 0.0 else None
        recovered = 1 if recovery_index is not None else 0
        episode_id += 1
        duration = ((recovery_index if recovery_index is not None else end_index) - start_index + 1)
        max_duration = max(max_duration, duration)
        episodes.append(
            {
                'episode_id': episode_id,
                'start_day': normalize_optional(rows[start_index].get('date')),
                'trough_day': normalize_optional(rows[trough_index].get('date')),
                'end_day': normalize_optional(rows[recovery_index].get('date')) if recovery_index is not None else '',
                'duration_days': duration,
                'max_drawdown_ratio': trough_ratio,
                'recovered': recovered,
            }
        )
        index += 1
    return episodes, max_duration


def build_halt_recovery_report(trades: list[TradeRecord], portfolio_daily_path: Path) -> HaltRecoveryReport:
    rows = _load_daily_rows(portfolio_daily_path)
    total_days = len(rows)
    halt_rows = [row for row in rows if normalize_optional(row.get('halt_flag')) == '1']
    trade_day_map = build_trade_day_trade_map(trades)
    active_positions_by_day = {trade_day: len(day_trades) for trade_day, day_trades in trade_day_map.items()}
    halt_streak_rows, max_consecutive_halt_days = _build_halt_streak_rows(rows, active_positions_by_day)
    drawdown_episode_rows, max_drawdown_duration_days = _build_drawdown_episode_rows(rows)

    first_halt_index = next((index for index, row in enumerate(rows) if normalize_optional(row.get('halt_flag')) == '1'), None)
    first_halt_day = normalize_optional(rows[first_halt_index].get('date')) if first_halt_index is not None else ''
    first_halt_drawdown_ratio = to_float(rows[first_halt_index].get('drawdown_ratio')) if first_halt_index is not None else 0.0
    first_halt_active_positions = active_positions_by_day.get(first_halt_day, 0) if first_halt_day else 0
    lockout_halt_days = sum(
        1 for row in halt_rows if active_positions_by_day.get(normalize_optional(row.get('date')), 0) <= 0
    )
    post_first_halt_trade_entries = (
        sum(1 for trade in trades if normalize_optional(trade.entry_fill_ts) and normalize_optional(trade.entry_fill_ts)[:10] > first_halt_day)
        if first_halt_day
        else 0
    )
    recovery_to_peak_days_after_first_halt: int | None = None
    if first_halt_index is not None:
        peak_before_halt = to_float(rows[first_halt_index].get('equity_peak'))
        for probe in range(first_halt_index + 1, len(rows)):
            if peak_before_halt > 0 and to_float(rows[probe].get('equity_end')) >= peak_before_halt:
                recovery_to_peak_days_after_first_halt = probe - first_halt_index
                break

    halt_days = len(halt_rows)
    summary = {
        'total_days': total_days,
        'halt_days': halt_days,
        'halt_day_share': (halt_days / total_days) if total_days else 0.0,
        'first_halt_day': first_halt_day,
        'first_halt_days_from_start': first_halt_index if first_halt_index is not None else '',
        'first_halt_drawdown_ratio': first_halt_drawdown_ratio,
        'first_halt_day_active_positions': first_halt_active_positions,
        'halt_streak_count': len(halt_streak_rows),
        'max_consecutive_halt_days': max_consecutive_halt_days,
        'lockout_halt_days': lockout_halt_days,
        'lockout_halt_share': (lockout_halt_days / halt_days) if halt_days else 0.0,
        'post_first_halt_trade_entries': post_first_halt_trade_entries,
        'recovery_to_peak_days_after_first_halt': (
            recovery_to_peak_days_after_first_halt if recovery_to_peak_days_after_first_halt is not None else ''
        ),
        'max_drawdown_duration_days': max_drawdown_duration_days,
        'ended_in_drawdown': 1 if rows and to_float(rows[-1].get('drawdown_ratio')) > 0.0 else 0,
    }
    summary['recovery_assessment'] = _recovery_assessment(
        halt_days=halt_days,
        lockout_halt_days=lockout_halt_days,
        post_first_halt_trade_entries=post_first_halt_trade_entries,
        max_consecutive_halt_days=max_consecutive_halt_days,
        recovery_to_peak_days_after_first_halt=recovery_to_peak_days_after_first_halt,
    )
    return HaltRecoveryReport(
        summary=summary,
        halt_streak_rows=tuple(halt_streak_rows),
        drawdown_episode_rows=tuple(drawdown_episode_rows),
    )


def format_halt_recovery_markdown(report: HaltRecoveryReport) -> str:
    summary = report.summary
    lines = [
        '# Halt Recovery Diagnostics',
        '',
        f"- recovery_assessment: `{summary.get('recovery_assessment', '')}`",
        f"- halt_days: `{int(summary.get('halt_days', 0) or 0)}`",
        f"- lockout_halt_days: `{int(summary.get('lockout_halt_days', 0) or 0)}`",
        f"- halt_streak_count: `{int(summary.get('halt_streak_count', 0) or 0)}`",
        f"- max_consecutive_halt_days: `{int(summary.get('max_consecutive_halt_days', 0) or 0)}`",
        f"- first_halt_day: `{summary.get('first_halt_day', '')}`",
        f"- first_halt_drawdown_ratio: `{float(summary.get('first_halt_drawdown_ratio', 0.0) or 0.0):.4f}`",
        f"- first_halt_day_active_positions: `{int(summary.get('first_halt_day_active_positions', 0) or 0)}`",
        f"- post_first_halt_trade_entries: `{int(summary.get('post_first_halt_trade_entries', 0) or 0)}`",
        f"- max_drawdown_duration_days: `{int(summary.get('max_drawdown_duration_days', 0) or 0)}`",
        '',
        '## Interpretation',
        '',
    ]

    assessment = str(summary.get('recovery_assessment', 'active_halt_stress'))
    if assessment == 'persistent_lockout':
        lines.append('- The run falls into a persistent drawdown lockout after the first halt. A permanent drawdown halt is likely dominating later outcomes.')
    elif assessment == 'recovered_after_halt':
        lines.append('- The run recovers to its previous equity peak after a halt episode. Graduated re-risking is worth evaluating before a full hard halt.')
    elif assessment == 'extended_drawdown_stall':
        lines.append('- Halt episodes stretch across multiple consecutive trade days without returning to peak. Recovery design is a first-order research item.')
    elif assessment == 'no_halt':
        lines.append('- No halt episode is present. Recovery logic is not the current bottleneck.')
    else:
        lines.append('- Halt behavior remains active during stressed exposure. Recovery research should be paired with exposure compression analysis.')

    lines.extend(
        [
            '',
            '## Halt Streaks',
            '',
            '| streak | start | end | days | lockout days | start dd | end dd | recovered | recover days |',
            '| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |',
        ]
    )
    for row in report.halt_streak_rows:
        lines.append(
            f"| {int(row.get('streak_id', 0) or 0)} | {row.get('start_day', '')} | {row.get('end_day', '')} | {int(row.get('halt_days', 0) or 0)} | {int(row.get('lockout_days', 0) or 0)} | {float(row.get('start_drawdown_ratio', 0.0) or 0.0):.4f} | {float(row.get('end_drawdown_ratio', 0.0) or 0.0):.4f} | {int(row.get('recovered_to_peak', 0) or 0)} | {row.get('days_to_recover_peak', '')} |"
        )
    if not report.halt_streak_rows:
        lines.append('| none |  |  | 0 | 0 | 0.0000 | 0.0000 | 0 |  |')

    lines.extend(
        [
            '',
            '## Drawdown Episodes',
            '',
            '| episode | start | trough | end | duration | max dd | recovered |',
            '| --- | --- | --- | --- | ---: | ---: | ---: |',
        ]
    )
    for row in report.drawdown_episode_rows:
        lines.append(
            f"| {int(row.get('episode_id', 0) or 0)} | {row.get('start_day', '')} | {row.get('trough_day', '')} | {row.get('end_day', '')} | {int(row.get('duration_days', 0) or 0)} | {float(row.get('max_drawdown_ratio', 0.0) or 0.0):.4f} | {int(row.get('recovered', 0) or 0)} |"
        )
    if not report.drawdown_episode_rows:
        lines.append('| none |  |  |  | 0 | 0.0000 | 0 |')
    return '\n'.join(lines) + '\n'


__all__ = [
    'DRAWDOWN_EPISODE_COLUMNS',
    'HALT_RECOVERY_SUMMARY_COLUMNS',
    'HALT_STREAK_COLUMNS',
    'HaltRecoveryReport',
    'build_halt_recovery_report',
    'format_halt_recovery_markdown',
]
