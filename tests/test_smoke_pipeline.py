import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from strategy.config_loader import load_config
from strategy.domain.instruments import is_in_sessions
from strategy.engine import StepHooks, StrategyEngine
from strategy.types import PlatformState, PositionRiskState, TradingSignal


class _DummyAccount:
    def __init__(self):
        self.cash = {"nav": 500000.0, "balance": 500000.0, "available": 500000.0}

    def position(self, symbol, side):
        return None


class _DummyContext:
    def __init__(self, cfg):
        self.cfg = cfg
        self.local_simulation = True
        self.now = datetime(2026, 2, 12, 10, 0, 0)
        self._account = _DummyAccount()
        self.frames = {}
        symbol = f"{cfg.runtime.symbols[0]}.SIM"
        idx_5m = pd.date_range(end=self.now, periods=200, freq='5min')
        idx_1h = pd.date_range(end=self.now, periods=120, freq='1h')
        self.frames[(symbol, cfg.runtime.freq_5m)] = pd.DataFrame(
            {
                'eob': idx_5m,
                'open': [100 + i * 0.1 for i in range(200)],
                'high': [101 + i * 0.1 for i in range(200)],
                'low': [99 + i * 0.1 for i in range(200)],
                'close': [100.2 + i * 0.1 for i in range(200)],
                'volume': [1200 + i % 30 for i in range(200)],
            }
        )
        self.frames[(symbol, cfg.runtime.freq_1h)] = pd.DataFrame(
            {
                'eob': idx_1h,
                'open': [100 + i for i in range(120)],
                'high': [102 + i for i in range(120)],
                'low': [98 + i for i in range(120)],
                'close': [100.5 + i for i in range(120)],
                'volume': [3000 + i % 20 for i in range(120)],
            }
        )

    def account(self):
        return self._account

    def data(self, symbol, frequency, count, fields):
        return self.frames[(symbol, frequency)].tail(count).copy()


class SmokePipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        cfg = load_config(Path('config/strategy.yaml'))
        cfg.runtime.symbols = [cfg.runtime.symbols[0]]
        self.engine = StrategyEngine(cfg)
        self.ctx = _DummyContext(cfg)
        self.engine.initialize_runtime(self.ctx)
        cs = cfg.runtime.symbols[0]
        self.engine.set_symbol_mapping(cs, f"{cs}.SIM")
        self.state = self.engine.runtime.states_by_csymbol[cs]

    def test_process_order_trace(self) -> None:
        hooks = StepHooks(trace=[])
        with patch('strategy.engine.process_risk_pipeline', return_value=(False, None)), patch(
            'strategy.engine.process_entry_pipeline', return_value=None
        ):
            self.engine.process_symbol_on_5m(self.state.csymbol, self.state, hooks=hooks)
        self.assertEqual(hooks.trace, ['flush', 'trend', 'risk', 'entry', 'daily'])

    def test_engine_fetches_min_required_1h_count(self) -> None:
        required_1h = max(
            int(self.engine.cfg.strategy.h1_ema_slow_period),
            int(self.engine.cfg.strategy.h1_rsi_period),
        ) + 5
        expected_1h = max(int(self.engine.cfg.runtime.sub_count_1h), required_1h)
        calls: list[tuple[str, int]] = []

        def _fake_fetch(context, symbol, frequency, count):
            _ = context
            calls.append((str(frequency), int(count)))
            return self.ctx.frames[(symbol, frequency)].tail(int(count)).copy()

        with patch("strategy.engine.fetch_kline", side_effect=_fake_fetch), patch(
            "strategy.engine.process_risk_pipeline", return_value=(True, None)
        ):
            self.engine.process_symbol_on_5m(self.state.csymbol, self.state)

        one_h_calls = [count for freq, count in calls if freq == self.engine.cfg.runtime.freq_1h]
        self.assertTrue(one_h_calls)
        self.assertEqual(one_h_calls[0], expected_1h)

    def test_engine_caps_1h_request_near_backtest_start(self) -> None:
        self.engine.cfg.runtime.mode = "BACKTEST"
        self.engine.cfg.gm.backtest_start = "2026-02-12 10:00:00"
        self.ctx.now = datetime(2026, 2, 12, 10, 30, 0)
        req_1h = self.engine._resolve_req_1h_count()
        self.assertEqual(req_1h, 1)

    def test_risk_priority_skips_entry(self) -> None:
        hooks = StepHooks(trace=[])
        sig = TradingSignal(
            action='none',
            reason='pause',
            direction=0,
            qty=0,
            price=0.0,
            stop_loss=0.0,
            take_profit=0.0,
            entry_atr=0.0,
            risk_stage='halt',
            campaign_id='',
            created_eob=None,
            exit_trigger_type='portfolio_pause',
        )
        with patch('strategy.engine.process_risk_pipeline', return_value=(True, sig)), patch(
            'strategy.engine.process_entry_pipeline', return_value=None
        ) as entry_mock:
            self.engine.process_symbol_on_5m(self.state.csymbol, self.state, hooks=hooks)
        entry_mock.assert_not_called()
        self.assertEqual(hooks.trace, ['flush', 'trend', 'risk', 'entry_skipped', 'daily'])

    def test_pending_signal_executes_on_next_bar(self) -> None:
        sig = TradingSignal(
            action='buy',
            reason='test',
            direction=1,
            qty=1,
            price=100.0,
            stop_loss=95.0,
            take_profit=110.0,
            entry_atr=2.0,
            risk_stage='normal',
            campaign_id='c1',
            created_eob=None,
        )
        eob = datetime(2026, 2, 12, 10, 0, 0)
        self.state.pending_signal = sig
        self.state.pending_signal_eob = eob

        with patch('strategy.engine.execute_signal') as ex_mock, patch('strategy.engine.append_trade_report') as tr_mock:
            self.engine.flush_due_pending_signal(self.state, self.state.csymbol, self.state.main_symbol, eob)
            self.assertEqual(ex_mock.call_count, 0)
            self.assertEqual(tr_mock.call_count, 0)

            self.engine.flush_due_pending_signal(
                self.state,
                self.state.csymbol,
                self.state.main_symbol,
                eob + timedelta(minutes=5),
            )

        self.assertEqual(ex_mock.call_count, 1)
        self.assertEqual(tr_mock.call_count, 1)

    def test_frequency_aliases_trigger(self) -> None:
        cs = self.state.csymbol
        symbol = self.state.main_symbol
        bars = [
            type('Bar', (), {'symbol': symbol, 'frequency': '5m'})(),
            type('Bar', (), {'symbol': symbol, 'frequency': '300s'})(),
            type('Bar', (), {'symbol': symbol, 'frequency': '5min'})(),
        ]
        with patch.object(self.engine, 'process_symbol_on_5m') as proc_mock:
            self.engine.process_symbols_by_bars(bars)
        self.assertEqual(proc_mock.call_count, 1)
        called_csymbol = proc_mock.call_args[0][0]
        self.assertEqual(called_csymbol, cs)

    def test_require_next_bar_confirm_false_allows_same_bar_confirm(self) -> None:
        self.engine.cfg.strategy.require_next_bar_confirm = False
        self.state.pending_platform = PlatformState(
            direction=1,
            zg=100.0,
            zd=98.0,
            candidate_eob=datetime(2026, 2, 12, 10, 0, 0),
            atr_at_candidate=2.0,
            volume_ratio=1.2,
        )
        self.state.h1_trend = 1
        self.state.h1_strength = 1.0
        self.state.bar_index_5m = 10
        self.state.daily_entry_count = 0

        df_5m = self.ctx.frames[(self.state.main_symbol, self.engine.cfg.runtime.freq_5m)].copy()
        current_eob = self.state.pending_platform.candidate_eob

        with patch('strategy.pipelines.entry._build_entry_signal') as build_mock:
            build_mock.return_value = TradingSignal(
                action='buy',
                reason='test',
                direction=1,
                qty=1,
                price=101.0,
                stop_loss=99.0,
                take_profit=105.0,
                entry_atr=2.0,
                risk_stage='normal',
                campaign_id='x',
                created_eob=current_eob,
            )
            from strategy.pipelines.entry import process_entry_pipeline

            sig = process_entry_pipeline(
                self.engine.runtime,
                self.state,
                self.state.csymbol,
                self.state.main_symbol,
                df_5m,
                current_eob,
                current_price=101.0,
                atr_val=2.0,
                long_qty=0,
                short_qty=0,
            )

        self.assertIsNotNone(sig)
        self.assertIsNone(self.state.pending_platform)

    def test_session_boundaries(self) -> None:
        from strategy.domain.instruments import get_instrument_spec

        cfg = self.engine.cfg
        cs = self.state.csymbol
        spec = get_instrument_spec(cfg, cs)
        self.assertTrue(len(spec.sessions.day) > 0)
        start_str, end_str = spec.sessions.day[0]
        sh, sm = [int(x) for x in start_str.split(":")]
        eh, em = [int(x) for x in end_str.split(":")]

        start_dt = datetime(2026, 2, 12, sh, sm)
        end_dt = datetime(2026, 2, 12, eh, em)
        self.assertFalse(is_in_sessions(cs, cfg, start_dt - timedelta(minutes=1)))
        self.assertTrue(is_in_sessions(cs, cfg, start_dt))
        self.assertTrue(is_in_sessions(cs, cfg, end_dt))
        self.assertFalse(is_in_sessions(cs, cfg, end_dt + timedelta(minutes=1)))

    def test_entry_skips_outside_sessions(self) -> None:
        self.state.pending_platform = None
        self.state.h1_trend = 1
        self.state.h1_strength = 1.0
        self.state.bar_index_5m = 20

        from strategy.pipelines.entry import process_entry_pipeline

        df_5m = self.ctx.frames[(self.state.main_symbol, self.engine.cfg.runtime.freq_5m)].copy()
        sig = process_entry_pipeline(
            self.engine.runtime,
            self.state,
            self.state.csymbol,
            self.state.main_symbol,
            df_5m,
            datetime(2026, 2, 12, 15, 31),
            current_price=float(df_5m.iloc[-1]['close']),
            atr_val=2.0,
            long_qty=0,
            short_qty=0,
        )
        self.assertIsNone(sig)

    def test_stopout_limit_blocks_entry_and_clears_pending(self) -> None:
        self.state.pending_platform = PlatformState(
            direction=1,
            zg=100.0,
            zd=98.0,
            candidate_eob=datetime(2026, 2, 12, 9, 55, 0),
            atr_at_candidate=2.0,
            volume_ratio=1.2,
        )
        self.state.daily_stopout_count = self.engine.cfg.risk.max_stopouts_per_day_per_symbol

        from strategy.pipelines.entry import process_entry_pipeline

        df_5m = self.ctx.frames[(self.state.main_symbol, self.engine.cfg.runtime.freq_5m)].copy()
        with patch("strategy.pipelines.entry._compute_atr_pause_flag", return_value=0):
            sig = process_entry_pipeline(
                self.engine.runtime,
                self.state,
                self.state.csymbol,
                self.state.main_symbol,
                df_5m,
                datetime(2026, 2, 12, 10, 0, 0),
                current_price=101.0,
                atr_val=2.0,
                long_qty=0,
                short_qty=0,
            )

        self.assertIsNone(sig)
        self.assertIsNone(self.state.pending_platform)

    def test_atr_pause_blocks_entry(self) -> None:
        self.state.pending_platform = PlatformState(
            direction=1,
            zg=100.0,
            zd=98.0,
            candidate_eob=datetime(2026, 2, 12, 9, 55, 0),
            atr_at_candidate=2.0,
            volume_ratio=1.2,
        )
        self.state.daily_stopout_count = 0

        from strategy.pipelines.entry import process_entry_pipeline

        df_5m = self.ctx.frames[(self.state.main_symbol, self.engine.cfg.runtime.freq_5m)].copy()
        with patch("strategy.pipelines.entry._compute_atr_pause_flag", return_value=1):
            sig = process_entry_pipeline(
                self.engine.runtime,
                self.state,
                self.state.csymbol,
                self.state.main_symbol,
                df_5m,
                datetime(2026, 2, 12, 10, 0, 0),
                current_price=101.0,
                atr_val=2.0,
                long_qty=0,
                short_qty=0,
            )

        self.assertIsNone(sig)
        self.assertIsNone(self.state.pending_platform)

    def test_entry_qty_falls_back_to_min_lot_when_fixed_equity_too_small(self) -> None:
        from strategy.pipelines.entry import _build_entry_signal

        runtime = self.engine.runtime
        runtime.portfolio_risk.initial_equity = 500000.0
        runtime.portfolio_risk.equity_peak = 500000.0
        runtime.cfg.risk.fixed_equity_percent = 0.05
        runtime.cfg.risk.max_pos_size_percent = 0.20
        runtime.cfg.risk.risk_per_trade = 1.00

        sig = _build_entry_signal(
            runtime=runtime,
            csymbol=self.state.csymbol,
            direction=1,
            current_price=7000.0,
            atr=50.0,
            current_eob=datetime(2026, 2, 12, 10, 0, 0),
            h1_size_mult=1.0,
            entry_platform_zg=6990.0,
            entry_platform_zd=6950.0,
            daily_stopout_count=0,
            atr_pause_flag=0,
        )

        self.assertIsNotNone(sig)
        self.assertEqual(sig.qty, 1)

    def test_entry_uses_instrument_position_overrides(self) -> None:
        from strategy.pipelines.entry import _build_entry_signal

        runtime = self.engine.runtime
        runtime.portfolio_risk.initial_equity = 1000000.0
        runtime.portfolio_risk.equity_peak = 1000000.0
        runtime.cfg.risk.fixed_equity_percent = 0.01
        runtime.cfg.risk.max_pos_size_percent = 0.05
        runtime.cfg.risk.risk_per_trade = 1.00

        spec = runtime.cfg.instrument.symbols.get(self.state.csymbol)
        if spec is not None:
            spec.fixed_equity_percent = 0.20
            spec.max_pos_size_percent = 10.0

        sig = _build_entry_signal(
            runtime=runtime,
            csymbol=self.state.csymbol,
            direction=1,
            current_price=7000.0,
            atr=50.0,
            current_eob=datetime(2026, 2, 12, 10, 0, 0),
            h1_size_mult=1.0,
            entry_platform_zg=6990.0,
            entry_platform_zd=6950.0,
            daily_stopout_count=0,
            atr_pause_flag=0,
        )
        self.assertIsNotNone(sig)
        self.assertGreaterEqual(sig.qty, 1)

    def test_entry_size_multipliers_reduce_qty(self) -> None:
        from strategy.pipelines.entry import _build_entry_signal

        runtime = self.engine.runtime
        runtime.portfolio_risk.initial_equity = 1000000.0
        runtime.portfolio_risk.current_equity = 1000000.0
        runtime.portfolio_risk.effective_risk_mult = 1.0
        runtime.cfg.risk.risk_per_trade = 1.00

        sig_full = _build_entry_signal(
            runtime=runtime,
            csymbol=self.state.csymbol,
            direction=1,
            current_price=7000.0,
            atr=50.0,
            current_eob=datetime(2026, 2, 12, 10, 0, 0),
            h1_size_mult=1.0,
            entry_platform_zg=6990.0,
            entry_platform_zd=6950.0,
            daily_stopout_count=0,
            atr_pause_flag=0,
        )
        self.assertIsNotNone(sig_full)

        runtime.portfolio_risk.effective_risk_mult = 0.5
        sig_half = _build_entry_signal(
            runtime=runtime,
            csymbol=self.state.csymbol,
            direction=1,
            current_price=7000.0,
            atr=50.0,
            current_eob=datetime(2026, 2, 12, 10, 0, 0),
            h1_size_mult=0.5,
            entry_platform_zg=6990.0,
            entry_platform_zd=6950.0,
            daily_stopout_count=0,
            atr_pause_flag=0,
        )
        self.assertIsNotNone(sig_half)
        self.assertLessEqual(sig_half.qty, sig_full.qty)

    def test_entry_risk_per_trade_caps_qty(self) -> None:
        from strategy.pipelines.entry import _build_entry_signal

        runtime = self.engine.runtime
        runtime.portfolio_risk.initial_equity = 1000000.0
        runtime.portfolio_risk.current_equity = 1000000.0
        runtime.portfolio_risk.effective_risk_mult = 1.0
        runtime.cfg.risk.fixed_equity_percent = 0.80
        runtime.cfg.risk.max_pos_size_percent = 5.00
        runtime.cfg.risk.risk_per_trade = 0.001

        spec = runtime.cfg.instrument.symbols.get(self.state.csymbol)
        if spec is not None:
            spec.fixed_equity_percent = 0.80
            spec.max_pos_size_percent = 5.00
            spec.min_lot = 1
            spec.lot_step = 1

        sig = _build_entry_signal(
            runtime=runtime,
            csymbol=self.state.csymbol,
            direction=1,
            current_price=100.0,
            atr=1.0,
            current_eob=datetime(2026, 2, 12, 10, 0, 0),
            h1_size_mult=1.0,
            entry_platform_zg=99.0,
            entry_platform_zd=98.0,
            daily_stopout_count=0,
            atr_pause_flag=0,
        )
        self.assertIsNotNone(sig)
        self.assertEqual(sig.qty, 1)

    def test_entry_risk_per_trade_blocks_when_below_min_lot(self) -> None:
        from strategy.pipelines.entry import _build_entry_signal

        runtime = self.engine.runtime
        runtime.portfolio_risk.initial_equity = 1000000.0
        runtime.portfolio_risk.current_equity = 1000000.0
        runtime.portfolio_risk.effective_risk_mult = 1.0
        runtime.cfg.risk.fixed_equity_percent = 0.80
        runtime.cfg.risk.max_pos_size_percent = 5.00
        runtime.cfg.risk.risk_per_trade = 0.00005

        spec = runtime.cfg.instrument.symbols.get(self.state.csymbol)
        if spec is not None:
            spec.fixed_equity_percent = 0.80
            spec.max_pos_size_percent = 5.00
            spec.min_lot = 1
            spec.lot_step = 1

        sig = _build_entry_signal(
            runtime=runtime,
            csymbol=self.state.csymbol,
            direction=1,
            current_price=100.0,
            atr=1.0,
            current_eob=datetime(2026, 2, 12, 10, 0, 0),
            h1_size_mult=1.0,
            entry_platform_zg=99.0,
            entry_platform_zd=98.0,
            daily_stopout_count=0,
            atr_pause_flag=0,
        )
        self.assertIsNone(sig)

    def test_time_stop_breakout_failure_increments_stopout_count(self) -> None:
        from strategy.pipelines.risk import process_risk_pipeline

        trade_time = datetime(2026, 2, 12, 10, 0, 0)
        self.state.daily_stopout_date = "2026-02-12"
        self.state.daily_stopout_count = 0
        self.state.position_risk = PositionRiskState(
            entry_price=100.0,
            direction=1,
            entry_atr=2.0,
            initial_stop_loss=90.0,
            stop_loss=90.0,
            first_target_price=120.0,
            campaign_id="c1",
            highest_price_since_entry=101.0,
            lowest_price_since_entry=99.0,
            bars_in_trade=self.engine.cfg.risk.time_stop_bars - 1,
            entry_platform_zg=100.0,
            entry_platform_zd=98.0,
        )

        stop_here, signal = process_risk_pipeline(
            self.engine.runtime,
            self.state,
            self.state.csymbol,
            self.state.main_symbol,
            trade_time,
            current_price=99.5,
            atr_val=2.0,
            long_qty=1,
            short_qty=0,
        )

        self.assertTrue(stop_here)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.exit_trigger_type, "time_stop")
        self.assertEqual(self.state.daily_stopout_count, 1)

    def test_time_stop_skipped_after_half_close(self) -> None:
        from strategy.pipelines.risk import process_risk_pipeline

        self.engine.cfg.risk.enable_dynamic_stop = False
        self.engine.cfg.risk.trail_activate_r = 100.0

        trade_time = datetime(2026, 2, 12, 10, 0, 0)
        self.state.position_risk = PositionRiskState(
            entry_price=100.0,
            direction=1,
            entry_atr=2.0,
            initial_stop_loss=90.0,
            stop_loss=100.0,
            first_target_price=120.0,
            campaign_id="c1",
            highest_price_since_entry=101.0,
            lowest_price_since_entry=99.0,
            bars_in_trade=self.engine.cfg.risk.time_stop_bars + 1,
            entry_platform_zg=100.0,
            entry_platform_zd=98.0,
            is_half_closed=True,
            partial_exited=True,
        )

        stop_here, signal = process_risk_pipeline(
            self.engine.runtime,
            self.state,
            self.state.csymbol,
            self.state.main_symbol,
            trade_time,
            current_price=99.0,
            atr_val=2.0,
            long_qty=1,
            short_qty=0,
        )

        self.assertFalse(stop_here)
        self.assertIsNone(signal)

    def test_1r_half_close_triggers(self) -> None:
        """DC_Fractal_Sniper: 测试 1:1 盈亏比触发减半仓"""
        from strategy.pipelines.risk import process_risk_pipeline

        trade_time = datetime(2026, 2, 12, 10, 0, 0)
        self.state.daily_stopout_date = "2026-02-12"
        self.state.daily_stopout_count = 0

        entry_price = 100.0
        initial_stop = 95.0  # R = 5
        self.state.position_risk = PositionRiskState(
            entry_price=entry_price,
            direction=1,
            entry_atr=2.0,
            initial_stop_loss=initial_stop,
            stop_loss=initial_stop,
            first_target_price=120.0,
            campaign_id="c1",
            highest_price_since_entry=entry_price,
            lowest_price_since_entry=entry_price,
            bars_in_trade=1,
            entry_platform_zg=98.0,
            entry_platform_zd=96.0,
            initial_risk_r=abs(entry_price - initial_stop),  # R = 5
            is_half_closed=False,
        )

        # 构造 df_1h 用于 1H 止损测试
        df_1h = self.ctx.frames[(self.state.main_symbol, self.engine.cfg.runtime.freq_1h)].copy()

        # 价格达到 1:1 = 105 (entry_price + R)
        stop_here, signal = process_risk_pipeline(
            self.engine.runtime,
            self.state,
            self.state.csymbol,
            self.state.main_symbol,
            trade_time,
            current_price=105.0,
            atr_val=2.0,
            long_qty=1,
            short_qty=0,
            df_1h=df_1h,
        )

        self.assertTrue(stop_here)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "close_half_long")
        self.assertEqual(signal.exit_trigger_type, "split_exit_1r")
        self.assertTrue(self.state.position_risk.is_half_closed)

    def test_h1_trailing_stop_after_half_close(self) -> None:
        """DC_Fractal_Sniper: 测试减半后使用 1H EMA 止损"""
        from strategy.pipelines.risk import process_risk_pipeline

        trade_time = datetime(2026, 2, 12, 10, 0, 0)
        self.state.daily_stopout_date = "2026-02-12"
        self.state.daily_stopout_count = 0

        entry_price = 100.0
        initial_stop = 95.0
        self.state.position_risk = PositionRiskState(
            entry_price=entry_price,
            direction=1,
            entry_atr=2.0,
            initial_stop_loss=initial_stop,
            stop_loss=entry_price,  # 止损已移到开仓价（保本）
            first_target_price=120.0,
            campaign_id="c1",
            highest_price_since_entry=105.0,
            lowest_price_since_entry=entry_price,
            bars_in_trade=5,
            entry_platform_zg=98.0,
            entry_platform_zd=96.0,
            initial_risk_r=abs(entry_price - initial_stop),
            is_half_closed=True,  # 已减半
        )

        # 构造 df_1h，EMA20 低于当前价格
        df_1h = self.ctx.frames[(self.state.main_symbol, self.engine.cfg.runtime.freq_1h)].copy()
        # 让 close 价格上涨，EMA20 也会上涨
        df_1h.loc[len(df_1h) - 1, "close"] = 102.0

        stop_here, signal = process_risk_pipeline(
            self.engine.runtime,
            self.state,
            self.state.csymbol,
            self.state.main_symbol,
            trade_time,
            current_price=101.0,
            atr_val=2.0,
            long_qty=1,
            short_qty=0,
            df_1h=df_1h,
        )

        # 应该触发 1H 趋势止损（价格跌破 EMA20）
        self.assertTrue(stop_here)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "close_long")
        self.assertEqual(signal.exit_trigger_type, "trend_following_stop")

    def test_dynamic_stop_triggers_after_activation(self) -> None:
        from strategy.pipelines.risk import process_risk_pipeline

        self.engine.cfg.risk.enable_dynamic_stop = True
        self.engine.cfg.risk.dynamic_stop_atr = 1.0
        self.engine.cfg.risk.dynamic_stop_activate_r = 0.5
        self.engine.cfg.risk.trail_activate_r = 100.0

        trade_time = datetime(2026, 2, 12, 10, 0, 0)
        self.state.position_risk = PositionRiskState(
            entry_price=100.0,
            direction=1,
            entry_atr=2.0,
            initial_stop_loss=95.0,
            stop_loss=95.0,
            first_target_price=120.0,
            campaign_id="c1",
            highest_price_since_entry=103.0,
            lowest_price_since_entry=99.0,
            bars_in_trade=1,
            initial_risk_r=5.0,
        )

        stop_here, signal = process_risk_pipeline(
            self.engine.runtime,
            self.state,
            self.state.csymbol,
            self.state.main_symbol,
            trade_time,
            current_price=101.0,
            atr_val=2.0,
            long_qty=1,
            short_qty=0,
        )

        self.assertTrue(stop_here)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "close_long")
        self.assertEqual(signal.exit_trigger_type, "dynamic_stop")

    def test_dynamic_stop_moves_one_way_for_long(self) -> None:
        from strategy.pipelines.risk import process_risk_pipeline

        self.engine.cfg.risk.enable_dynamic_stop = True
        self.engine.cfg.risk.dynamic_stop_atr = 1.0
        self.engine.cfg.risk.dynamic_stop_activate_r = 0.0
        self.engine.cfg.risk.trail_activate_r = 100.0

        trade_time = datetime(2026, 2, 12, 10, 0, 0)
        self.state.position_risk = PositionRiskState(
            entry_price=100.0,
            direction=1,
            entry_atr=2.0,
            initial_stop_loss=95.0,
            stop_loss=95.0,
            first_target_price=120.0,
            campaign_id="c1",
            highest_price_since_entry=104.0,
            lowest_price_since_entry=99.0,
            bars_in_trade=1,
            initial_risk_r=5.0,
        )

        stop_here_1, _ = process_risk_pipeline(
            self.engine.runtime,
            self.state,
            self.state.csymbol,
            self.state.main_symbol,
            trade_time,
            current_price=103.0,
            atr_val=2.0,
            long_qty=1,
            short_qty=0,
        )
        self.assertFalse(stop_here_1)
        first_stop = self.state.position_risk.stop_loss
        self.assertEqual(first_stop, 102.0)

        stop_here_2, _ = process_risk_pipeline(
            self.engine.runtime,
            self.state,
            self.state.csymbol,
            self.state.main_symbol,
            trade_time + timedelta(minutes=5),
            current_price=103.5,
            atr_val=5.0,
            long_qty=1,
            short_qty=0,
        )
        self.assertFalse(stop_here_2)
        self.assertEqual(self.state.position_risk.stop_loss, first_stop)

    def test_dynamic_stop_not_active_before_threshold(self) -> None:
        from strategy.pipelines.risk import process_risk_pipeline

        self.engine.cfg.risk.enable_dynamic_stop = True
        self.engine.cfg.risk.dynamic_stop_atr = 1.0
        self.engine.cfg.risk.dynamic_stop_activate_r = 1.2
        self.engine.cfg.risk.trail_activate_r = 100.0

        trade_time = datetime(2026, 2, 12, 10, 0, 0)
        self.state.position_risk = PositionRiskState(
            entry_price=100.0,
            direction=1,
            entry_atr=2.0,
            initial_stop_loss=95.0,
            stop_loss=95.0,
            first_target_price=120.0,
            campaign_id="c1",
            highest_price_since_entry=102.0,
            lowest_price_since_entry=99.0,
            bars_in_trade=1,
            initial_risk_r=5.0,
        )

        stop_here, _ = process_risk_pipeline(
            self.engine.runtime,
            self.state,
            self.state.csymbol,
            self.state.main_symbol,
            trade_time,
            current_price=101.0,
            atr_val=2.0,
            long_qty=1,
            short_qty=0,
        )
        self.assertFalse(stop_here)
        self.assertEqual(self.state.position_risk.stop_loss, 95.0)

    def test_entry_platform_width_upper_bound_path_no_name_error(self) -> None:
        from strategy.pipelines.entry import process_entry_pipeline

        df_5m = self.ctx.frames[(self.state.main_symbol, self.engine.cfg.runtime.freq_5m)].copy()
        current_eob = datetime(2026, 2, 12, 10, 0, 0)
        self.state.pending_platform = None
        self.state.h1_trend = 1
        self.state.h1_strength = 1.0
        self.state.daily_stopout_count = 0

        fake_platform = SimpleNamespace(zg=110.0, zd=90.0)
        with patch("strategy.pipelines.entry.merge_klines", return_value=df_5m), patch(
            "strategy.pipelines.entry.identify_fractals", return_value=["f1", "f2", "f3"]
        ), patch("strategy.pipelines.entry.build_bi", return_value=["b1", "b2", "b3"]), patch(
            "strategy.pipelines.entry.identify_zhongshu", return_value=[fake_platform]
        ), patch("strategy.pipelines.entry.get_latest_platform", return_value=fake_platform):
            sig = process_entry_pipeline(
                self.engine.runtime,
                self.state,
                self.state.csymbol,
                self.state.main_symbol,
                df_5m,
                current_eob,
                current_price=100.0,
                atr_val=1.0,
                long_qty=0,
                short_qty=0,
            )
        self.assertIsNone(sig)


if __name__ == '__main__':
    unittest.main()
