from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from strategy.config_loader import load_config
from strategy.engine import StrategyEngine
from strategy.gm.callbacks import build_gm_callbacks


class SmokeCallbackTests(unittest.TestCase):
    def test_continuous_csymbol_candidates_support_88_alias(self) -> None:
        from strategy.gm.contract_roll import _continuous_csymbol_candidates

        cands_ic = _continuous_csymbol_candidates("CFFEX.IC88")
        self.assertIn("CFFEX.IC88", cands_ic)
        self.assertIn("CFFEX.IC", cands_ic)

        cands_ap = _continuous_csymbol_candidates("CZCE.AP88")
        self.assertIn("CZCE.AP88", cands_ap)
        self.assertIn("CZCE.ap", cands_ap)

    def test_roll_mapping_fallback_from_88_alias(self) -> None:
        from strategy.gm.contract_roll import roll_main_contract

        cfg = load_config(__import__("pathlib").Path("config/strategy.yaml"))
        cfg.runtime.symbols = ["CFFEX.IC88"]
        engine = StrategyEngine(cfg)
        ctx = SimpleNamespace(now=dt.datetime(2026, 2, 12, 10, 0, 0), local_simulation=False)
        engine.initialize_runtime(ctx)

        def _fake_mapping(csymbol, start_date, end_date):
            _ = start_date, end_date
            if csymbol == "CFFEX.IC":
                return [{"symbol": "CFFEX.IC2602"}]
            return []

        with patch("strategy.gm.contract_roll.get_continuous_contracts", side_effect=_fake_mapping), patch(
            "strategy.gm.contract_roll._subscribe_symbol", return_value=True
        ):
            roll_main_contract(engine, ctx)

        self.assertEqual(engine.runtime.symbol_to_csymbol.get("CFFEX.IC2602"), "CFFEX.IC88")
        self.assertEqual(engine.runtime.states_by_csymbol["CFFEX.IC88"].main_symbol, "CFFEX.IC2602")

    def test_subscribe_fallback_count_on_permission_error(self) -> None:
        from strategy.gm.contract_roll import _subscribe_symbol

        calls = []

        def _fake_subscribe(symbols, frequency, count, wait_group=False):
            calls.append((symbols, frequency, int(count)))
            if int(count) > 1:
                raise Exception("ERR_NO_DATA_PERMISSION")

        with patch("strategy.gm.contract_roll.subscribe", side_effect=_fake_subscribe):
            ok = _subscribe_symbol("DCE.p2605", "300s", "3600s", 40, 30)
        # fallback subscription should keep returning False so caller keeps retrying full counts
        self.assertFalse(ok)
        self.assertIn(("DCE.p2605", "300s", 1), calls)
        self.assertIn(("DCE.p2605", "3600s", 1), calls)

    def test_init_and_on_bar(self) -> None:
        cfg = load_config(__import__('pathlib').Path('config/strategy.yaml'))
        engine = StrategyEngine(cfg)
        cb = build_gm_callbacks(engine)

        context = SimpleNamespace(
            now=dt.datetime(2026, 2, 12, 10, 0, 0),
            local_simulation=True,
        )

        # minimal account/data/submit api required by engine
        from strategy.gm.callbacks import _LocalContext

        ctx = _LocalContext(cfg)
        ctx.now = context.now

        cb.init(ctx)
        self.assertTrue(engine.runtime.symbol_to_csymbol)

        bars = []
        for cs in cfg.runtime.symbols:
            bars.append(SimpleNamespace(symbol=f"{cs}.SIM", frequency=cfg.runtime.freq_5m))

        cb.on_bar(ctx, bars)

    def test_roll_clears_pending_state(self) -> None:
        cfg = load_config(__import__('pathlib').Path('config/strategy.yaml'))
        cfg.runtime.symbols = [cfg.runtime.symbols[0]]
        engine = StrategyEngine(cfg)
        cb = build_gm_callbacks(engine)

        from strategy.gm.callbacks import _LocalContext

        ctx = _LocalContext(cfg)
        ctx.now = dt.datetime(2026, 2, 12, 10, 0, 0)
        cb.init(ctx)

        state = engine.runtime.states_by_csymbol[cfg.runtime.symbols[0]]
        state.pending_signal = object()
        state.pending_signal_eob = ctx.now
        state.pending_platform = object()
        state.last_risk_signal_eob = ctx.now

        state.main_symbol = f"{cfg.runtime.symbols[0]}.OLD"
        engine.runtime.symbol_to_csymbol[state.main_symbol] = cfg.runtime.symbols[0]
        engine.runtime.last_roll_date = ""

        cb.on_bar(ctx, [SimpleNamespace(symbol=f"{cfg.runtime.symbols[0]}.SIM", frequency=cfg.runtime.freq_5m)])

        self.assertIsNone(state.pending_signal)
        self.assertIsNone(state.pending_signal_eob)
        self.assertIsNone(state.pending_platform)
        self.assertIsNone(state.last_risk_signal_eob)

    def test_top_level_init_lazy_bootstrap(self) -> None:
        import main
        from strategy.gm.callbacks import _LocalContext

        # Simulate old SDK path: callback invoked before main() runs.
        main._GM_CALLBACKS = None
        cfg = load_config(__import__("pathlib").Path("config/strategy.yaml"))
        ctx = _LocalContext(cfg)
        ctx.now = dt.datetime(2026, 2, 12, 10, 0, 0)

        main.init(ctx)
        self.assertIsNotNone(main._GM_CALLBACKS)

    def test_run_id_auto_timestamp_keeps_base(self) -> None:
        import main

        runtime = SimpleNamespace(run_id="run_001")
        main._append_run_id_timestamp_if_needed(runtime)
        self.assertTrue(runtime.run_id.startswith("run_001_"))

    def test_run_gm_retries_without_callback_kwargs(self) -> None:
        cfg = load_config(__import__('pathlib').Path('config/strategy.yaml'))
        cfg.gm.token = 't'
        cfg.gm.strategy_id = 's'
        engine = StrategyEngine(cfg)
        cb = build_gm_callbacks(engine)

        with patch('strategy.gm.callbacks.run') as run_mock, patch(
            'strategy.gm.callbacks.inspect.signature'
        ) as sig_mock:
            sig_mock.return_value = __import__('inspect').Signature(
                parameters=[
                    __import__('inspect').Parameter('token', __import__('inspect').Parameter.KEYWORD_ONLY),
                    __import__('inspect').Parameter('strategy_id', __import__('inspect').Parameter.KEYWORD_ONLY),
                    __import__('inspect').Parameter('filename', __import__('inspect').Parameter.KEYWORD_ONLY),
                    __import__('inspect').Parameter('mode', __import__('inspect').Parameter.KEYWORD_ONLY),
                    __import__('inspect').Parameter('backtest_start_time', __import__('inspect').Parameter.KEYWORD_ONLY),
                    __import__('inspect').Parameter('backtest_end_time', __import__('inspect').Parameter.KEYWORD_ONLY),
                    __import__('inspect').Parameter('backtest_adjust', __import__('inspect').Parameter.KEYWORD_ONLY),
                    __import__('inspect').Parameter('init', __import__('inspect').Parameter.KEYWORD_ONLY),
                    __import__('inspect').Parameter('on_bar', __import__('inspect').Parameter.KEYWORD_ONLY),
                    __import__('inspect').Parameter('on_order_status', __import__('inspect').Parameter.KEYWORD_ONLY),
                    __import__('inspect').Parameter('on_execution_report', __import__('inspect').Parameter.KEYWORD_ONLY),
                    __import__('inspect').Parameter('on_error', __import__('inspect').Parameter.KEYWORD_ONLY),
                ]
            )
            run_mock.side_effect = [TypeError("unexpected keyword argument 'on_bar'"), None]
            cb.run_gm()

        self.assertEqual(run_mock.call_count, 2)

    def test_backtest_window_clipped_by_max_days(self) -> None:
        cfg = load_config(__import__('pathlib').Path('config/strategy.yaml'))
        cfg.runtime.mode = 'BACKTEST'
        cfg.gm.backtest_start = '2024-01-01 00:00:00'
        cfg.gm.backtest_end = '2026-02-12 15:00:00'
        cfg.gm.backtest_max_days = 30
        engine = StrategyEngine(cfg)
        cb = build_gm_callbacks(engine)

        with patch.object(cb, "_permission_min_start", return_value=__import__("pandas").Timestamp("2025-08-19 00:00:00")):
            cb._clip_backtest_window_if_needed()

        end_ts = dt.datetime.strptime(cfg.gm.backtest_end, '%Y-%m-%d %H:%M:%S')
        start_ts = dt.datetime.strptime(cfg.gm.backtest_start, '%Y-%m-%d %H:%M:%S')
        self.assertEqual((end_ts - start_ts).days, 30)

    def test_permission_min_start_is_180_days_before_today(self) -> None:
        cfg = load_config(__import__('pathlib').Path('config/strategy.yaml'))
        engine = StrategyEngine(cfg)
        cb = build_gm_callbacks(engine)
        now = __import__("pandas").Timestamp("2026-02-15 12:34:56")
        self.assertEqual(str(cb._permission_min_start(now)), "2025-08-19 00:00:00")

    def test_force_local_bypasses_gm_run(self) -> None:
        cfg = load_config(__import__('pathlib').Path('config/strategy.yaml'))
        cfg.runtime.mode = 'LIVE'
        cfg.gm.token = 't'
        cfg.gm.strategy_id = 's'
        engine = StrategyEngine(cfg)
        cb = build_gm_callbacks(engine)

        with patch.dict('os.environ', {'GM_FORCE_LOCAL': '1'}), patch('strategy.gm.callbacks.run') as run_mock:
            cb.run_gm()

        run_mock.assert_not_called()

    def test_daily_report_equity_end_uses_current_equity(self) -> None:
        import csv

        cfg = load_config(__import__('pathlib').Path('config/strategy.yaml'))
        engine = StrategyEngine(cfg)

        from strategy.gm.callbacks import _LocalContext

        ctx = _LocalContext(cfg)
        ctx.now = dt.datetime(2026, 2, 12, 10, 0, 0)

        with tempfile.TemporaryDirectory() as td:
            cfg.reporting.output_dir = td
            engine.initialize_runtime(ctx)
            engine.runtime.portfolio_risk.current_equity = 123456.0
            from strategy.reporting.daily_report import append_daily_report

            append_daily_report(engine.runtime, ctx.now)

            p = Path(td) / cfg.reporting.daily_filename
            with p.open("r", newline="", encoding="utf-8") as f:
                rows = list(csv.reader(f))
            # header + 1 row
            self.assertGreaterEqual(len(rows), 2)
            header = rows[0]
            data = rows[1]
            run_id_idx = header.index("run_id")
            equity_end_idx = header.index("equity_end")
            self.assertEqual(data[run_id_idx], cfg.runtime.run_id)
            self.assertEqual(float(data[equity_end_idx]), 123456.0)

    def test_local_context_reads_csv_with_time_window(self) -> None:
        cfg = load_config(Path("config/strategy.yaml"))
        cfg.runtime.symbols = [cfg.runtime.symbols[0]]
        cfg.gm.backtest_start = "2026-02-12 09:00:00"
        cfg.gm.backtest_end = "2026-02-12 10:00:00"

        csymbol = cfg.runtime.symbols[0]
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            symbol_dir = root / csymbol
            symbol_dir.mkdir(parents=True, exist_ok=True)

            idx_5m = pd.date_range(start="2026-02-12 09:00:00", end="2026-02-12 10:00:00", freq="5min")
            idx_1h = pd.date_range(start="2026-02-12 09:00:00", end="2026-02-12 10:00:00", freq="1h")
            pd.DataFrame(
                {
                    "eob": idx_5m,
                    "open": [100.0] * len(idx_5m),
                    "high": [101.0] * len(idx_5m),
                    "low": [99.0] * len(idx_5m),
                    "close": [100.5] * len(idx_5m),
                    "volume": [1000] * len(idx_5m),
                }
            ).to_csv(symbol_dir / f"{cfg.runtime.freq_5m}.csv", index=False, encoding="utf-8")
            pd.DataFrame(
                {
                    "eob": idx_1h,
                    "open": [100.0] * len(idx_1h),
                    "high": [101.0] * len(idx_1h),
                    "low": [99.0] * len(idx_1h),
                    "close": [100.5] * len(idx_1h),
                    "volume": [1000] * len(idx_1h),
                }
            ).to_csv(symbol_dir / f"{cfg.runtime.freq_1h}.csv", index=False, encoding="utf-8")

            with patch.dict("os.environ", {"LOCAL_DATA_ROOT": str(root)}):
                from strategy.gm.callbacks import _LocalContext

                ctx = _LocalContext(cfg)
                self.assertTrue(ctx.using_external_data)
                self.assertGreater(len(ctx.timeline_5m), 0)

                ctx.now = dt.datetime(2026, 2, 12, 9, 20, 0)
                symbol = f"{csymbol}.SIM"
                out = ctx.data(symbol, cfg.runtime.freq_5m, 3, fields="eob,open,high,low,close,volume")
                self.assertEqual(len(out), 3)
                self.assertLessEqual(pd.to_datetime(out.iloc[-1]["eob"]), pd.Timestamp(ctx.now))


if __name__ == '__main__':
    unittest.main()
