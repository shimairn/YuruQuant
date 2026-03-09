from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from yuruquant.reporting.csv_utils import load_csv_rows, normalize_optional
from yuruquant.reporting.summary import collapse_portfolio_daily_rows
from yuruquant.reporting.trade_day_activity import build_trade_day_trade_map
from yuruquant.reporting.trade_records import TradeRecord


DIVERSIFICATION_SUMMARY_COLUMNS = [
    'configured_clusters',
    'traded_clusters',
    'unassigned_symbols',
    'total_trades',
    'halt_days',
    'halt_days_with_active_positions',
    'halt_days_without_positions',
    'halt_days_with_multi_cluster_positions',
    'top_cluster_trade_share',
    'top_cluster_turnover_share',
    'top_cluster_halt_day_share',
    'dominant_cluster_by_trade_share',
    'dominant_cluster_by_halt_days',
    'max_total_concurrent_positions',
    'max_cluster_concurrent_positions',
    'max_cluster_same_direction_positions',
    'max_active_clusters_on_halt_day',
    'pressure_assessment',
]

CLUSTER_PRESSURE_COLUMNS = [
    'cluster',
    'symbols',
    'trades',
    'wins',
    'losses',
    'win_rate',
    'gross_pnl',
    'turnover',
    'trade_share',
    'turnover_share',
    'max_concurrent_positions',
    'max_same_direction_positions',
    'halt_day_count',
    'halt_day_share',
]

HALT_DAY_COLUMNS = [
    'trade_day',
    'day_active_positions',
    'day_active_clusters',
    'top_cluster',
    'top_cluster_day_active_positions',
    'top_cluster_day_active_same_direction_positions',
    'cluster_breakdown',
]


@dataclass(frozen=True)
class DiversificationReport:
    summary: dict[str, float | int | str]
    cluster_rows: tuple[dict[str, float | int | str], ...]
    halt_day_rows: tuple[dict[str, float | int | str], ...]


def build_cluster_lookup(symbols: list[str], risk_clusters: Mapping[str, tuple[str, ...]] | Mapping[str, list[str]]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for cluster, members in risk_clusters.items():
        for csymbol in members:
            lookup[str(csymbol)] = str(cluster)
    for csymbol in symbols:
        lookup.setdefault(str(csymbol), 'unassigned')
    return lookup


def trade_turnover(trade: TradeRecord) -> float:
    entry_notional = abs(float(trade.entry_fill_price) * int(trade.qty) * float(trade.multiplier))
    exit_notional = abs(float(trade.exit_fill_price) * int(trade.qty) * float(trade.multiplier))
    return entry_notional + exit_notional


def _load_halt_days(portfolio_daily_path: Path) -> list[str]:
    rows = collapse_portfolio_daily_rows(load_csv_rows(portfolio_daily_path))
    halt_days: list[str] = []
    for row in rows:
        if normalize_optional(row.get('halt_flag')) != '1':
            continue
        trade_day = normalize_optional(row.get('date'))
        if trade_day:
            halt_days.append(trade_day)
    return halt_days


def _event_peaks(trades: list[TradeRecord], cluster_by_csymbol: Mapping[str, str]) -> tuple[int, dict[str, int], dict[str, int]]:
    events: list[tuple[str, int, int]] = []
    trade_meta: dict[int, tuple[str, int]] = {}
    for index, trade in enumerate(trades):
        trade_meta[index] = (cluster_by_csymbol.get(trade.csymbol, 'unassigned'), int(trade.direction))
        events.append((trade.entry_fill_ts, 1, index))
        events.append((trade.exit_fill_ts, 0, index))
    events.sort(key=lambda item: (item[0], item[1]))

    active_ids: set[int] = set()
    cluster_totals: dict[str, int] = defaultdict(int)
    cluster_directions: dict[tuple[str, int], int] = defaultdict(int)
    cluster_peak_totals: dict[str, int] = defaultdict(int)
    cluster_peak_same_direction: dict[str, int] = defaultdict(int)
    max_total = 0

    for _, is_entry, trade_id in events:
        cluster, direction = trade_meta[trade_id]
        if is_entry:
            if trade_id in active_ids:
                continue
            active_ids.add(trade_id)
            cluster_totals[cluster] += 1
            cluster_directions[(cluster, direction)] += 1
            max_total = max(max_total, len(active_ids))
            cluster_peak_totals[cluster] = max(cluster_peak_totals[cluster], cluster_totals[cluster])
            cluster_peak_same_direction[cluster] = max(
                cluster_peak_same_direction[cluster],
                cluster_directions[(cluster, direction)],
            )
            continue

        if trade_id not in active_ids:
            continue
        active_ids.remove(trade_id)
        cluster_totals[cluster] = max(cluster_totals[cluster] - 1, 0)
        cluster_directions[(cluster, direction)] = max(cluster_directions[(cluster, direction)] - 1, 0)

    return max_total, cluster_peak_totals, cluster_peak_same_direction


def _build_halt_day_rows(
    trades: list[TradeRecord],
    halt_days: list[str],
    cluster_by_csymbol: Mapping[str, str],
) -> tuple[list[dict[str, float | int | str]], dict[str, int]]:
    active_trade_map = build_trade_day_trade_map(trades)

    halt_rows: list[dict[str, float | int | str]] = []
    halt_day_presence: dict[str, int] = defaultdict(int)
    for trade_day in halt_days:
        active_entries = [
            (cluster_by_csymbol.get(trade.csymbol, 'unassigned'), int(trade.direction))
            for trade in active_trade_map.get(trade_day, [])
        ]
        cluster_counts = Counter(cluster for cluster, _ in active_entries)
        cluster_direction_counts = Counter((cluster, direction) for cluster, direction in active_entries)
        for cluster in cluster_counts:
            halt_day_presence[cluster] += 1

        top_cluster = ''
        top_cluster_positions = 0
        top_cluster_same_direction = 0
        if cluster_counts:
            top_cluster, top_cluster_positions = max(cluster_counts.items(), key=lambda item: (item[1], item[0]))
            top_cluster_same_direction = max(
                count for (cluster, _direction), count in cluster_direction_counts.items() if cluster == top_cluster
            )

        breakdown = ';'.join(f'{cluster}:{count}' for cluster, count in sorted(cluster_counts.items()))
        halt_rows.append(
            {
                'trade_day': trade_day,
                'day_active_positions': sum(cluster_counts.values()),
                'day_active_clusters': len(cluster_counts),
                'top_cluster': top_cluster,
                'top_cluster_day_active_positions': top_cluster_positions,
                'top_cluster_day_active_same_direction_positions': top_cluster_same_direction,
                'cluster_breakdown': breakdown,
            }
        )
    return halt_rows, halt_day_presence


def _pressure_assessment(
    halt_days: int,
    halt_days_with_active_positions: int,
    top_cluster_halt_day_share: float,
    max_cluster_same_direction_positions: int,
    max_active_clusters_on_halt_day: int,
) -> str:
    if halt_days <= 0:
        return 'no_halt'
    if halt_days_with_active_positions <= 0:
        return 'path_dependent_lockout'
    if halt_days_with_active_positions < halt_days:
        return 'mixed_lockout'
    if top_cluster_halt_day_share >= 0.60 and max_cluster_same_direction_positions >= 2:
        return 'cluster_dominant'
    if max_active_clusters_on_halt_day >= 2:
        return 'broad_multi_cluster_pressure'
    return 'mixed'


def build_diversification_report(
    trades: list[TradeRecord],
    portfolio_daily_path: Path,
    cluster_by_csymbol: Mapping[str, str],
) -> DiversificationReport:
    halt_days = _load_halt_days(portfolio_daily_path)
    halt_day_rows, halt_day_presence = _build_halt_day_rows(trades, halt_days, cluster_by_csymbol)
    max_total_concurrent, cluster_peak_totals, cluster_peak_same_direction = _event_peaks(trades, cluster_by_csymbol)

    grouped: dict[str, list[TradeRecord]] = defaultdict(list)
    symbols_by_cluster: dict[str, set[str]] = defaultdict(set)
    for trade in trades:
        cluster = cluster_by_csymbol.get(trade.csymbol, 'unassigned')
        grouped[cluster].append(trade)
        symbols_by_cluster[cluster].add(trade.csymbol)

    total_turnover = sum(trade_turnover(trade) for trade in trades)
    total_trades = len(trades)
    cluster_rows: list[dict[str, float | int | str]] = []
    for cluster, cluster_trades in grouped.items():
        wins = sum(1 for trade in cluster_trades if trade.gross_pnl > 0)
        losses = len(cluster_trades) - wins
        turnover = sum(trade_turnover(trade) for trade in cluster_trades)
        cluster_rows.append(
            {
                'cluster': cluster,
                'symbols': ','.join(sorted(symbols_by_cluster[cluster])),
                'trades': len(cluster_trades),
                'wins': wins,
                'losses': losses,
                'win_rate': (wins / len(cluster_trades)) if cluster_trades else 0.0,
                'gross_pnl': sum(float(trade.gross_pnl) for trade in cluster_trades),
                'turnover': turnover,
                'trade_share': (len(cluster_trades) / total_trades) if total_trades else 0.0,
                'turnover_share': (turnover / total_turnover) if total_turnover else 0.0,
                'max_concurrent_positions': int(cluster_peak_totals.get(cluster, 0)),
                'max_same_direction_positions': int(cluster_peak_same_direction.get(cluster, 0)),
                'halt_day_count': int(halt_day_presence.get(cluster, 0)),
                'halt_day_share': (int(halt_day_presence.get(cluster, 0)) / len(halt_days)) if halt_days else 0.0,
            }
        )
    cluster_rows.sort(
        key=lambda row: (
            -int(row.get('halt_day_count', 0) or 0),
            -float(row.get('trade_share', 0.0) or 0.0),
            str(row.get('cluster', '')),
        )
    )

    traded_clusters = len(cluster_rows)
    unassigned_symbols = sorted({trade.csymbol for trade in trades if cluster_by_csymbol.get(trade.csymbol, 'unassigned') == 'unassigned'})
    top_trade_cluster = cluster_rows[0]['cluster'] if cluster_rows else ''
    top_trade_share = max((float(row.get('trade_share', 0.0) or 0.0) for row in cluster_rows), default=0.0)
    top_turnover_row = max(cluster_rows, key=lambda row: float(row.get('turnover_share', 0.0) or 0.0), default=None)
    top_halt_row = max(cluster_rows, key=lambda row: float(row.get('halt_day_share', 0.0) or 0.0), default=None)
    halt_days_with_active_positions = sum(1 for row in halt_day_rows if int(row.get('day_active_positions', 0) or 0) > 0)
    halt_days_with_multi_cluster_positions = sum(1 for row in halt_day_rows if int(row.get('day_active_clusters', 0) or 0) > 1)
    max_active_clusters_on_halt_day = max((int(row.get('day_active_clusters', 0) or 0) for row in halt_day_rows), default=0)
    max_cluster_concurrent_positions = max((int(row.get('max_concurrent_positions', 0) or 0) for row in cluster_rows), default=0)
    max_cluster_same_direction_positions = max((int(row.get('max_same_direction_positions', 0) or 0) for row in cluster_rows), default=0)
    top_cluster_halt_day_share = float(top_halt_row.get('halt_day_share', 0.0) or 0.0) if top_halt_row else 0.0

    summary = {
        'configured_clusters': len({value for value in cluster_by_csymbol.values() if value != 'unassigned'}),
        'traded_clusters': traded_clusters,
        'unassigned_symbols': ','.join(unassigned_symbols),
        'total_trades': total_trades,
        'halt_days': len(halt_days),
        'halt_days_with_active_positions': halt_days_with_active_positions,
        'halt_days_without_positions': max(len(halt_days) - halt_days_with_active_positions, 0),
        'halt_days_with_multi_cluster_positions': halt_days_with_multi_cluster_positions,
        'top_cluster_trade_share': top_trade_share,
        'top_cluster_turnover_share': float(top_turnover_row.get('turnover_share', 0.0) or 0.0) if top_turnover_row else 0.0,
        'top_cluster_halt_day_share': top_cluster_halt_day_share,
        'dominant_cluster_by_trade_share': top_trade_cluster,
        'dominant_cluster_by_halt_days': str(top_halt_row.get('cluster', '')) if top_halt_row else '',
        'max_total_concurrent_positions': max_total_concurrent,
        'max_cluster_concurrent_positions': max_cluster_concurrent_positions,
        'max_cluster_same_direction_positions': max_cluster_same_direction_positions,
        'max_active_clusters_on_halt_day': max_active_clusters_on_halt_day,
        'pressure_assessment': _pressure_assessment(
            halt_days=len(halt_days),
            halt_days_with_active_positions=halt_days_with_active_positions,
            top_cluster_halt_day_share=top_cluster_halt_day_share,
            max_cluster_same_direction_positions=max_cluster_same_direction_positions,
            max_active_clusters_on_halt_day=max_active_clusters_on_halt_day,
        ),
    }

    return DiversificationReport(
        summary=summary,
        cluster_rows=tuple(cluster_rows),
        halt_day_rows=tuple(halt_day_rows),
    )


def format_diversification_markdown(report: DiversificationReport) -> str:
    summary = report.summary
    lines = [
        '# Cluster Pressure Diagnostics',
        '',
        f"- pressure_assessment: `{summary.get('pressure_assessment', '')}`",
        f"- halt_days: `{int(summary.get('halt_days', 0) or 0)}`",
        f"- halt_days_with_active_positions: `{int(summary.get('halt_days_with_active_positions', 0) or 0)}`",
        f"- halt_days_without_positions: `{int(summary.get('halt_days_without_positions', 0) or 0)}`",
        f"- dominant_cluster_by_trade_share: `{summary.get('dominant_cluster_by_trade_share', '')}`",
        f"- dominant_cluster_by_halt_days: `{summary.get('dominant_cluster_by_halt_days', '')}`",
        f"- max_total_concurrent_positions: `{int(summary.get('max_total_concurrent_positions', 0) or 0)}`",
        f"- max_cluster_same_direction_positions: `{int(summary.get('max_cluster_same_direction_positions', 0) or 0)}`",
        '',
        '## Interpretation',
        '',
    ]

    assessment = str(summary.get('pressure_assessment', 'mixed'))
    if assessment == 'path_dependent_lockout':
        lines.append('- Most halt days happened without active positions. Static cluster caps are unlikely to fix the baseline by themselves.')
    elif assessment == 'mixed_lockout':
        lines.append('- Halt behavior is partly driven by post-loss lockout and only partly by active cluster crowding.')
    elif assessment == 'cluster_dominant':
        lines.append('- Halt days are concentrated inside one cluster while same-direction overlap is elevated. Cluster-specific controls deserve deeper follow-up.')
    elif assessment == 'broad_multi_cluster_pressure':
        lines.append('- Halt days involve several active clusters at once. Broad portfolio regime control matters more than a single static cluster cap.')
    else:
        lines.append('- Cluster pressure is mixed. Use the per-cluster and halt-day tables before promoting any allocation rule.')

    lines.extend(
        [
            '',
            '## Cluster Snapshot',
            '',
            '| cluster | trades | trade share | turnover share | halt days | max same-dir | gross pnl |',
            '| --- | ---: | ---: | ---: | ---: | ---: | ---: |',
        ]
    )
    for row in report.cluster_rows:
        lines.append(
            f"| {row['cluster']} | {int(row.get('trades', 0) or 0)} | {float(row.get('trade_share', 0.0) or 0.0)*100:.2f}% | {float(row.get('turnover_share', 0.0) or 0.0)*100:.2f}% | {int(row.get('halt_day_count', 0) or 0)} | {int(row.get('max_same_direction_positions', 0) or 0)} | {float(row.get('gross_pnl', 0.0) or 0.0):.2f} |"
        )

    lines.extend(
        [
            '',
            '## Halt Day Snapshot',
            '',
            '| day | day-active positions | day-active clusters | top cluster | top cluster day-active | top cluster same-dir day-active |',
            '| --- | ---: | ---: | --- | ---: | ---: |',
        ]
    )
    for row in report.halt_day_rows:
        lines.append(
            f"| {row['trade_day']} | {int(row.get('day_active_positions', 0) or 0)} | {int(row.get('day_active_clusters', 0) or 0)} | {row.get('top_cluster', '')} | {int(row.get('top_cluster_day_active_positions', 0) or 0)} | {int(row.get('top_cluster_day_active_same_direction_positions', 0) or 0)} |"
        )
    if not report.halt_day_rows:
        lines.append('| none | 0 | 0 |  | 0 | 0 |')
    return '\n'.join(lines) + '\n'


__all__ = [
    'CLUSTER_PRESSURE_COLUMNS',
    'DIVERSIFICATION_SUMMARY_COLUMNS',
    'HALT_DAY_COLUMNS',
    'DiversificationReport',
    'build_cluster_lookup',
    'build_diversification_report',
    'format_diversification_markdown',
    'trade_turnover',
]
