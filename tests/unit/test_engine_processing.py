from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import unittest

import polars as pl

from yuruquant.app.config import load_config
from yuruquant.core.engine_processing import process_symbol, queue_signal
from yuruquant.core.fill_policy import NextBarOpenFillPolicy
from yuruquant.core.frames import KlineFrame, SymbolFrames
from yuruquant.core.models import EntrySignal, EnvironmentSnapshot, GuardDecision, PortfolioSnapshot, RuntimeState, SymbolRuntime


class _RecordingReportSink:
    def __init__(self) -> None:
        self.signals: list[tuple[str, str, str]] = []
        self.portfolio_days: list[str] = []

    def record_signal(self, runtime, mode: str, run_id: str, csymbol: str, symbol: str, signal) -> None:
        _ = runtime
        _ = mode
        _ = run_id
        self.signals.append((csymbol, symbol, signal.campaign_id))

    def record_portfolio_day(self, runtime, mode: str, run_id: str, trade_day: str, snapshot_ts: object) -> None:
        _ = runtime
        _ = mode
        _ = run_id
        _ = snapshot_ts
        self.portfolio_days.append(trade_day)


class _Gateway:
    def __init__(self, history: KlineFrame | None = None) -> None:
        self.history = history
        self.fetch_calls: list[tuple[str, str, int]] = []

    def fetch_history(self, symbol: str, frequency: str, count: int):
        self.fetch_calls.append((symbol, frequency, count))
        return self.history if self.history is not None else KlineFrame.empty(symbol=symbol, frequency=frequency)

    def get_portfolio_snapshot(self):
        return PortfolioSnapshot(equity=500000.0, cash=500000.0)


def _build_entry_signal(campaign_id: str, created_at: datetime) -> EntrySignal:
    return EntrySignal(
        action='buy',
        reason='demo',
        direction=1,
        qty=1,
        price=101.0,
        stop_loss=98.8,
        protected_stop_price=101.6,
        created_at=created_at,
        entry_atr=1.0,
        breakout_anchor=100.0,
        campaign_id=campaign_id,
        environment_ma=99.0,
        macd_histogram=0.4,
    )


def _build_frame(rows: int, frequency: str, start: datetime, step_minutes: int) -> KlineFrame:
    payload: list[dict[str, object]] = []
    for index in range(rows):
        close = 100.0 + index * 0.1
        payload.append(
            {
                'eob': start + timedelta(minutes=step_minutes * index),
                'open': close - 0.1,
                'high': close + 0.2,
                'low': close - 0.2,
                'close': close,
                'volume': 1000.0 + index,
            }
        )
    return KlineFrame(frame=pl.DataFrame(payload), symbol='DCE.P2409', frequency=frequency)


def _build_symbol_frames(config, latest_eob: datetime) -> SymbolFrames:
    frames = SymbolFrames.create('DCE.P2409', config.universe.entry_frequency, config.universe.trend_frequency, config.universe.warmup.entry_bars, config.universe.warmup.trend_bars)
    entry_start = latest_eob - timedelta(minutes=5 * (config.universe.warmup.entry_bars - 1))
    trend_start = latest_eob - timedelta(hours=config.universe.warmup.trend_bars - 1)
    frames.entry.replace(_build_frame(config.universe.warmup.entry_bars, config.universe.entry_frequency, entry_start, 5))
    frames.trend.replace(_build_frame(config.universe.warmup.trend_bars, config.universe.trend_frequency, trend_start, 60))
    return frames


class EngineProcessingTest(unittest.TestCase):
    def test_queue_signal_updates_pending_state_and_records_signal(self) -> None:
        config = load_config(Path('config/strategy.yaml'))
        config.reporting.enabled = True
        runtime = RuntimeState(states_by_csymbol={'DCE.P': SymbolRuntime(csymbol='DCE.P')})
        state = runtime.states_by_csymbol['DCE.P']
        fill_policy = NextBarOpenFillPolicy()
        report_sink = _RecordingReportSink()
        signal = _build_entry_signal('queued-entry', datetime(2026, 1, 5, 9, 35, 0))

        queue_signal(config, report_sink, runtime, fill_policy, state, 'DCE.P', 'DCE.P2409', signal)

        self.assertIs(state.pending_signal, signal)
        self.assertEqual([('DCE.P', 'DCE.P2409', 'queued-entry')], report_sink.signals)

    def test_process_symbol_skips_when_warmup_still_incomplete_after_backfill(self) -> None:
        config = load_config(Path('config/strategy.yaml'))
        config.reporting.enabled = False
        runtime = RuntimeState(states_by_csymbol={'DCE.P': SymbolRuntime(csymbol='DCE.P', main_symbol='DCE.P2409')})
        state = runtime.states_by_csymbol['DCE.P']
        gateway = _Gateway()

        process_symbol(
            config=config,
            gateway=gateway,
            report_sink=_RecordingReportSink(),
            fill_policy=NextBarOpenFillPolicy(),
            runtime=runtime,
            state=state,
            execute_due_signal=lambda *_: None,
            compute_environment_fn=lambda *args, **kwargs: EnvironmentSnapshot(direction=1, trend_ok=True, macd_histogram=0.4),
            evaluate_portfolio_guard_fn=lambda **kwargs: GuardDecision(allow_entries=True, force_flatten=False),
            make_flatten_signal_fn=lambda *args, **kwargs: None,
            evaluate_exit_signal_fn=lambda **kwargs: None,
            maybe_generate_entry_fn=lambda **kwargs: _build_entry_signal('new-entry', datetime(2026, 1, 5, 9, 35, 0)),
            check_entry_against_armed_risk_cap_fn=lambda *args, **kwargs: type('RiskCheck', (), {'breached': False})(),
            check_entry_against_cluster_risk_fn=lambda *args, **kwargs: type('ClusterCheck', (), {'breached': False})(),
            debug_fn=lambda *args, **kwargs: None,
        )

        self.assertIsNone(state.last_processed_eob)
        self.assertIsNone(state.pending_signal)
        self.assertGreaterEqual(len(gateway.fetch_calls), 2)

    def test_process_symbol_executes_due_signal_before_queueing_new_entry(self) -> None:
        config = load_config(Path('config/strategy.yaml'))
        config.reporting.enabled = False
        latest_eob = datetime(2026, 1, 5, 9, 35, 0)
        runtime = RuntimeState(states_by_csymbol={'DCE.P': SymbolRuntime(csymbol='DCE.P', main_symbol='DCE.P2409')})
        state = runtime.states_by_csymbol['DCE.P']
        runtime.bar_store['DCE.P2409'] = _build_symbol_frames(config, latest_eob)
        fill_policy = NextBarOpenFillPolicy()
        due_signal = _build_entry_signal('due-entry', latest_eob - timedelta(minutes=5))
        fill_policy.queue(state, due_signal, due_signal.created_at)
        executed: list[str] = []

        process_symbol(
            config=config,
            gateway=_Gateway(),
            report_sink=_RecordingReportSink(),
            fill_policy=fill_policy,
            runtime=runtime,
            state=state,
            execute_due_signal=lambda current_state, csymbol, symbol, signal: executed.append(signal.campaign_id),
            compute_environment_fn=lambda *args, **kwargs: EnvironmentSnapshot(direction=1, trend_ok=True, macd_histogram=0.4),
            evaluate_portfolio_guard_fn=lambda **kwargs: GuardDecision(allow_entries=True, force_flatten=False),
            make_flatten_signal_fn=lambda *args, **kwargs: None,
            evaluate_exit_signal_fn=lambda **kwargs: None,
            maybe_generate_entry_fn=lambda **kwargs: _build_entry_signal('new-entry', latest_eob),
            check_entry_against_armed_risk_cap_fn=lambda *args, **kwargs: type('RiskCheck', (), {'breached': False})(),
            check_entry_against_cluster_risk_fn=lambda *args, **kwargs: type('ClusterCheck', (), {'breached': False})(),
            debug_fn=lambda *args, **kwargs: None,
        )

        self.assertEqual(['due-entry'], executed)
        self.assertIsNotNone(state.pending_signal)
        assert state.pending_signal is not None
        self.assertEqual('new-entry', state.pending_signal.campaign_id)

    def test_process_symbol_deduplicates_same_latest_bar(self) -> None:
        config = load_config(Path('config/strategy.yaml'))
        config.reporting.enabled = False
        latest_eob = datetime(2026, 1, 5, 9, 35, 0)
        runtime = RuntimeState(states_by_csymbol={'DCE.P': SymbolRuntime(csymbol='DCE.P', main_symbol='DCE.P2409')})
        state = runtime.states_by_csymbol['DCE.P']
        runtime.bar_store['DCE.P2409'] = _build_symbol_frames(config, latest_eob)
        calls: list[str] = []

        def maybe_generate_entry_fn(**kwargs):
            _ = kwargs
            calls.append('called')
            return None

        kwargs = {
            'config': config,
            'gateway': _Gateway(),
            'report_sink': _RecordingReportSink(),
            'fill_policy': NextBarOpenFillPolicy(),
            'runtime': runtime,
            'state': state,
            'execute_due_signal': lambda *_: None,
            'compute_environment_fn': lambda *args, **kwargs: EnvironmentSnapshot(direction=1, trend_ok=True, macd_histogram=0.4),
            'evaluate_portfolio_guard_fn': lambda **kwargs: GuardDecision(allow_entries=True, force_flatten=False),
            'make_flatten_signal_fn': lambda *args, **kwargs: None,
            'evaluate_exit_signal_fn': lambda **kwargs: None,
            'maybe_generate_entry_fn': maybe_generate_entry_fn,
            'check_entry_against_armed_risk_cap_fn': lambda *args, **kwargs: type('RiskCheck', (), {'breached': False})(),
            'check_entry_against_cluster_risk_fn': lambda *args, **kwargs: type('ClusterCheck', (), {'breached': False})(),
            'debug_fn': lambda *args, **kwargs: None,
        }

        process_symbol(**kwargs)
        process_symbol(**kwargs)

        self.assertEqual(['called'], calls)


if __name__ == '__main__':
    unittest.main()
