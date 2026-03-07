from __future__ import annotations

import inspect
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from strategy.gm.contract_roll import roll_main_contract
from strategy.gm.orders import PositionSide_Long, PositionSide_Short

try:
    from gm.api import ADJUST_NONE, MODE_BACKTEST, MODE_LIVE, run, schedule  # type: ignore
except Exception:  # pragma: no cover
    ADJUST_NONE = 0
    MODE_BACKTEST = 1
    MODE_LIVE = 2
    run = None
    schedule = None


@dataclass
class _LocalPosition:
    volume: int = 0
    vwap: float = 0.0


class _LocalAccount:
    def __init__(self, context):
        self._ctx = context

    @property
    def cash(self):
        return self._ctx.cash

    def position(self, symbol: str, side):
        key = (symbol, side)
        pos = self._ctx.positions.get(key)
        if pos is None:
            return None
        return {
            "volume": pos.volume,
            "available": pos.volume,
            "available_now": pos.volume,
            "vwap": pos.vwap,
        }


class _LocalContext:
    def __init__(self, cfg):
        self.cfg = cfg
        self.local_simulation = True
        self.now = datetime.now().replace(second=0, microsecond=0)
        self.local_data_root = str(os.getenv("LOCAL_DATA_ROOT", "")).strip()
        self.cash = {
            "nav": 500000.0,
            "balance": 500000.0,
            "available": 500000.0,
        }
        self.positions: dict[tuple[str, int], _LocalPosition] = {}
        self.frames: dict[tuple[str, str], pd.DataFrame] = {}
        self.timeline_5m: list[pd.Timestamp] = []
        self.using_external_data = False
        self._build_frames(cfg)

    @staticmethod
    def _safe_empty_frame() -> pd.DataFrame:
        return pd.DataFrame(columns=["eob", "open", "high", "low", "close", "volume"])

    def _normalize_frame(self, df: pd.DataFrame | None) -> pd.DataFrame:
        if df is None or len(df) == 0:
            return self._safe_empty_frame()
        out = df.copy()
        for col in ["eob", "open", "high", "low", "close", "volume"]:
            if col not in out.columns:
                out[col] = pd.NA
        out = out[["eob", "open", "high", "low", "close", "volume"]].copy()
        out["eob"] = pd.to_datetime(out["eob"], errors="coerce")
        for col in ["open", "high", "low", "close", "volume"]:
            out[col] = pd.to_numeric(out[col], errors="coerce")
        out = out.dropna(subset=["eob", "high", "low", "close"])
        out = out.sort_values("eob").drop_duplicates(subset=["eob"], keep="last").reset_index(drop=True)
        return out

    def _clip_by_backtest_window(self, df: pd.DataFrame) -> pd.DataFrame:
        if len(df) == 0:
            return df
        try:
            start = pd.to_datetime(self.cfg.gm.backtest_start)
            end = pd.to_datetime(self.cfg.gm.backtest_end)
        except Exception:
            return df
        out = df[df["eob"] >= start]
        out = out[out["eob"] <= end]
        return out.reset_index(drop=True)

    def _load_csv_frame(self, root: Path, csymbol: str, freq: str) -> pd.DataFrame:
        file_path = root / csymbol / f"{freq}.csv"
        if not file_path.exists():
            return self._safe_empty_frame()
        try:
            raw = pd.read_csv(file_path)
        except Exception as exc:
            print(f"local_data warning read_failed file={file_path.as_posix()} err={exc}")
            return self._safe_empty_frame()
        out = self._normalize_frame(raw)
        out = self._clip_by_backtest_window(out)
        return out

    def _build_synthetic_symbol(self, csymbol: str, symbol: str) -> None:
        base_now = self.now
        idx_5m = pd.date_range(end=base_now, periods=max(self.cfg.runtime.sub_count_5m, 200), freq="5min")
        idx_1h = pd.date_range(end=base_now, periods=max(self.cfg.runtime.sub_count_1h, 120), freq="1h")

        base_price = 8000.0 if "ag" in csymbol else 7000.0
        df_5m = pd.DataFrame(
            {
                "eob": idx_5m,
                "open": [base_price + i * 0.8 for i in range(len(idx_5m))],
                "high": [base_price + i * 0.8 + 3 for i in range(len(idx_5m))],
                "low": [base_price + i * 0.8 - 3 for i in range(len(idx_5m))],
                "close": [base_price + i * 0.8 + (0.5 if i % 2 else -0.3) for i in range(len(idx_5m))],
                "volume": [1200 + (i % 20) * 40 for i in range(len(idx_5m))],
            }
        )
        df_1h = pd.DataFrame(
            {
                "eob": idx_1h,
                "open": [base_price + i * 2 for i in range(len(idx_1h))],
                "high": [base_price + i * 2 + 8 for i in range(len(idx_1h))],
                "low": [base_price + i * 2 - 8 for i in range(len(idx_1h))],
                "close": [base_price + i * 2 + (1 if i % 2 else -1) for i in range(len(idx_1h))],
                "volume": [5000 + (i % 10) * 100 for i in range(len(idx_1h))],
            }
        )
        self.frames[(symbol, self.cfg.runtime.freq_5m)] = df_5m
        self.frames[(symbol, self.cfg.runtime.freq_1h)] = df_1h

    def _rebuild_timeline(self) -> None:
        points: list[pd.Timestamp] = []
        for csymbol in self.cfg.runtime.symbols:
            symbol = f"{csymbol}.SIM"
            frame = self.frames.get((symbol, self.cfg.runtime.freq_5m))
            if frame is None or len(frame) == 0:
                continue
            points.extend(list(pd.to_datetime(frame["eob"])))
        if not points:
            self.timeline_5m = []
            return
        self.timeline_5m = sorted(pd.Index(points).dropna().unique())
        self.now = pd.to_datetime(self.timeline_5m[0]).to_pydatetime()

    def _build_frames(self, cfg):
        root = Path(self.local_data_root) if self.local_data_root else None
        loaded_csv_symbol_count = 0
        for csymbol in cfg.runtime.symbols:
            symbol = f"{csymbol}.SIM"
            if root is not None:
                df_5m = self._load_csv_frame(root, csymbol, cfg.runtime.freq_5m)
                df_1h = self._load_csv_frame(root, csymbol, cfg.runtime.freq_1h)
                if len(df_5m) > 0 and len(df_1h) > 0:
                    self.frames[(symbol, cfg.runtime.freq_5m)] = df_5m
                    self.frames[(symbol, cfg.runtime.freq_1h)] = df_1h
                    loaded_csv_symbol_count += 1
                    continue
                print(
                    f"local_data warning missing_or_empty csymbol={csymbol} "
                    f"freq5m={cfg.runtime.freq_5m} rows5m={len(df_5m)} "
                    f"freq1h={cfg.runtime.freq_1h} rows1h={len(df_1h)}"
                )
            self._build_synthetic_symbol(csymbol, symbol)

        self.using_external_data = loaded_csv_symbol_count > 0
        if root is not None:
            print(
                f"local_data source={root.as_posix()} "
                f"csv_symbols={loaded_csv_symbol_count}/{len(cfg.runtime.symbols)}"
            )
        self._rebuild_timeline()

    def account(self):
        return _LocalAccount(self)

    def data(self, symbol: str, frequency: str, count: int, fields: str):
        _ = fields
        frame = self.frames.get((symbol, frequency))
        if frame is None:
            return self._safe_empty_frame()
        now_ts = pd.to_datetime(self.now)
        sub = frame[frame["eob"] <= now_ts]
        if len(sub) == 0:
            return self._safe_empty_frame()
        return sub.tail(int(count)).copy()

    def submit_target_volume(self, symbol: str, target_qty: int, side: int):
        key = (symbol, side)
        if target_qty <= 0:
            self.positions.pop(key, None)
            return

        frame = self.data(
            symbol=symbol,
            frequency=self.cfg.runtime.freq_5m,
            count=1,
            fields="eob,open,high,low,close,volume",
        )
        px = float(frame.iloc[-1]["close"]) if frame is not None and len(frame) > 0 else 0.0
        self.positions[key] = _LocalPosition(volume=int(target_qty), vwap=px)

    def build_bars_for_now(self) -> list[SimpleNamespace]:
        now_ts = pd.to_datetime(self.now)
        bars: list[SimpleNamespace] = []
        for csymbol in self.cfg.runtime.symbols:
            symbol = f"{csymbol}.SIM"
            frame = self.frames.get((symbol, self.cfg.runtime.freq_5m))
            if frame is None or len(frame) == 0:
                continue
            if bool((frame["eob"] == now_ts).any()):
                bars.append(SimpleNamespace(symbol=symbol, frequency=self.cfg.runtime.freq_5m))
        return bars


class _GMCallbacks:
    def __init__(self, engine):
        self.engine = engine

    @staticmethod
    def _permission_min_start(now: pd.Timestamp | None = None) -> pd.Timestamp:
        """Free-tier window: latest 180 natural days excluding today."""
        ref = pd.to_datetime(now if now is not None else pd.Timestamp.now())
        if getattr(ref, "tz", None) is not None:
            ref = ref.tz_localize(None)
        today = ref.floor("D")
        return today - timedelta(days=180)

    def _clip_backtest_window_if_needed(self) -> None:
        """自动裁剪回测窗口，确保在权限范围内且不超过max_days"""
        cfg = self.engine.cfg
        if str(cfg.runtime.mode).upper() != "BACKTEST":
            return
        local_data_root = str(os.getenv("LOCAL_DATA_ROOT", "")).strip()
        max_days = int(getattr(cfg.gm, "backtest_max_days", 365) or 365)
        if max_days <= 0:
            max_days = 365
        permission_min_start = None if local_data_root else self._permission_min_start()
        try:
            start = pd.to_datetime(cfg.gm.backtest_start)
            end = pd.to_datetime(cfg.gm.backtest_end)
        except Exception:
            return

        # 确保不早于权限起始日期
        if permission_min_start is not None and start < permission_min_start:
            print(f"gm backtest start clipped from {start} to {permission_min_start} (permission)")
            start = permission_min_start

        if start >= end:
            end = pd.Timestamp.now().floor("min")
            if permission_min_start is None:
                start = end - timedelta(days=max_days)
            else:
                start = max(end - timedelta(days=max_days), permission_min_start)
            cfg.gm.backtest_start = start.strftime("%Y-%m-%d %H:%M:%S")
            cfg.gm.backtest_end = end.strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"gm backtest window reset start={cfg.gm.backtest_start} "
                f"end={cfg.gm.backtest_end} max_days={max_days}"
            )
            return

        span_days = (end - start).total_seconds() / 86400.0
        if span_days <= max_days:
            cfg.gm.backtest_start = start.strftime("%Y-%m-%d %H:%M:%S")
            cfg.gm.backtest_end = end.strftime("%Y-%m-%d %H:%M:%S")
            return

        if permission_min_start is None:
            clipped_start = end - timedelta(days=max_days)
        else:
            clipped_start = max(end - timedelta(days=max_days), permission_min_start)
        cfg.gm.backtest_start = clipped_start.strftime("%Y-%m-%d %H:%M:%S")
        cfg.gm.backtest_end = end.strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"gm backtest window clipped start={cfg.gm.backtest_start} "
            f"end={cfg.gm.backtest_end} max_days={max_days}"
        )

    def init(self, context):
        if not hasattr(context, "now"):
            context.now = datetime.now()
        self.engine.initialize_runtime(context)
        print(
            f"gm init mode={self.engine.cfg.runtime.mode} "
            f"symbols={len(self.engine.cfg.runtime.symbols)} "
            f"freq5m={self.engine.cfg.runtime.freq_5m} freq1h={self.engine.cfg.runtime.freq_1h}"
        )
        roll_main_contract(self.engine, context)
        if schedule is not None:
            try:
                schedule(lambda ctx: roll_main_contract(self.engine, ctx), date_rule="1d", time_rule="09:01:00")
            except Exception:
                pass

    def on_bar(self, context, bars):
        if not hasattr(context, "now"):
            context.now = datetime.now()
        self.engine.bind_context(context)
        self.engine.runtime._on_bar_seq = int(getattr(self.engine.runtime, "_on_bar_seq", 0)) + 1

        today = context.now.strftime("%Y-%m-%d")
        if self.engine.runtime.last_roll_date != today:
            roll_main_contract(self.engine, context)

        # 重试之前订阅失败的合约（避免长时间mapped=0）
        self._retry_failed_subscriptions()

        print(f"gm on_bar now={today} bars={len(bars)} mapped={len(self.engine.runtime.symbol_to_csymbol)}")
        self.engine.process_symbols_by_bars(bars)

    def on_order_status(self, context, order):
        _ = context
        print("order_status", getattr(order, "symbol", None), getattr(order, "status", None))

    def on_execution_report(self, context, execrpt):
        _ = context
        print("execution", getattr(execrpt, "symbol", None), getattr(execrpt, "price", None), getattr(execrpt, "volume", None))

    def on_error(self, context, code, info):
        _ = context
        print(f"ERROR code={code} info={info}")

    def _run_local(self):
        context = _LocalContext(self.engine.cfg)
        self.init(context)

        replayed = 0
        if context.using_external_data and context.timeline_5m:
            for bar_time in context.timeline_5m:
                context.now = pd.to_datetime(bar_time).to_pydatetime()
                bars = context.build_bars_for_now()
                if not bars:
                    continue
                self.on_bar(context, bars)
                replayed += 1
        if replayed == 0:
            bars = []
            for csymbol in self.engine.cfg.runtime.symbols:
                symbol = f"{csymbol}.SIM"
                bars.append(SimpleNamespace(symbol=symbol, frequency=self.engine.cfg.runtime.freq_5m))
            self.on_bar(context, bars)
            replayed = 1
        print(
            f"local run done mode={self.engine.cfg.runtime.mode} run_id={self.engine.cfg.runtime.run_id} "
            f"replayed_5m_bars={replayed} external_data={int(context.using_external_data)}"
        )

    def _supports_callback_kwargs(self) -> bool:
        try:
            sig = inspect.signature(run)
            params = sig.parameters
            needed = {"init", "on_bar", "on_order_status", "on_execution_report", "on_error"}
            return needed.issubset(set(params.keys()))
        except Exception:
            return False

    def _filter_supported_kwargs(self, kwargs: dict) -> dict:
        try:
            sig = inspect.signature(run)
            params = sig.parameters
            if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
                return kwargs
            return {k: v for k, v in kwargs.items() if k in params}
        except Exception:
            return kwargs

    def run_gm(self):
        mode = self.engine.cfg.runtime.mode.upper()
        self._clip_backtest_window_if_needed()

        # Local-safe default: only call gm.api.run when credentials are provided.
        force_local = str(os.getenv("GM_FORCE_LOCAL", "")).strip().lower() in {"1", "true", "yes", "on"}
        local_reasons: list[str] = []
        if force_local:
            local_reasons.append("GM_FORCE_LOCAL is enabled")
        if run is None:
            local_reasons.append("gm.api.run is unavailable (gm package missing in current interpreter)")
        if not self.engine.cfg.gm.token:
            local_reasons.append("GM token is empty")
        if not self.engine.cfg.gm.strategy_id:
            local_reasons.append("GM strategy_id is empty")

        if local_reasons:
            print(f"gm fallback to local simulation: {'; '.join(local_reasons)}")
            self._run_local()
            return

        run_mode = MODE_LIVE if mode == "LIVE" else MODE_BACKTEST
        base_kwargs = {
            "token": self.engine.cfg.gm.token,
            "strategy_id": self.engine.cfg.gm.strategy_id,
            "filename": "main.py",
            "mode": run_mode,
            "backtest_start_time": self.engine.cfg.gm.backtest_start,
            "backtest_end_time": self.engine.cfg.gm.backtest_end,
            "backtest_adjust": ADJUST_NONE,
            "backtest_commission_ratio": max(float(self.engine.cfg.risk.backtest_commission_ratio), 0.0),
            "backtest_slippage_ratio": max(float(self.engine.cfg.risk.backtest_slippage_ratio), 0.0),
            "backtest_match_mode": 0,
        }
        serv_addr = str(getattr(self.engine.cfg.gm, "serv_addr", "") or "").strip()
        if serv_addr:
            base_kwargs["serv_addr"] = serv_addr

        callback_kwargs = {
            "init": self.init,
            "on_bar": self.on_bar,
            "on_order_status": self.on_order_status,
            "on_execution_report": self.on_execution_report,
            "on_error": self.on_error,
        }

        run_kwargs = self._filter_supported_kwargs(dict(base_kwargs))
        if self._supports_callback_kwargs():
            run_kwargs.update(callback_kwargs)
            run_kwargs = self._filter_supported_kwargs(run_kwargs)

        try:
            run(**run_kwargs)
        except Exception as exc:
            if isinstance(exc, TypeError):
                msg = str(exc)
                callback_kw_error = (
                    "unexpected keyword argument 'init'" in msg
                    or "unexpected keyword argument 'on_bar'" in msg
                    or "unexpected keyword argument 'on_order_status'" in msg
                    or "unexpected keyword argument 'on_execution_report'" in msg
                    or "unexpected keyword argument 'on_error'" in msg
                )
                if callback_kw_error:
                    # Older GM SDK expects callbacks by top-level function names
                    # in filename (main.py). Retry without explicit callback kwargs.
                    run(**self._filter_supported_kwargs(dict(base_kwargs)))
                    return
                raise

            msg = str(exc)
            auth_like_error = ("status\": 1000" in msg) or ("token" in msg.lower())
            if auth_like_error:
                print(
                    "gm run auth/config error. Please verify GM_TOKEN and GM_STRATEGY_ID, "
                    "and ensure they match your current account/strategy."
                )
            raise

    def _retry_failed_subscriptions(self) -> None:
        """重试之前订阅失败的合约，避免长时间mapped=0"""
        fail_counts = getattr(self.engine.runtime, "_subscribe_fail_counts", {})
        if not fail_counts:
            return
        bar_seq = int(getattr(self.engine.runtime, "_on_bar_seq", 0))
        if bar_seq % 10 != 1:
            return

        from strategy.gm.contract_roll import _subscribe_symbol

        for symbol, fail_count in list(fail_counts.items()):
            csymbol = next(
                (k for k, st in self.engine.runtime.states_by_csymbol.items() if st.main_symbol == symbol),
                None,
            )
            if csymbol is None:
                fail_counts.pop(symbol, None)
                continue

            ok = _subscribe_symbol(
                symbol,
                self.engine.cfg.runtime.freq_5m,
                self.engine.cfg.runtime.freq_1h,
                self.engine.cfg.runtime.sub_count_5m,
                self.engine.cfg.runtime.sub_count_1h,
            )
            if ok:
                fail_counts.pop(symbol, None)
                print(f"gm retry subscribe ok symbol={symbol} after {fail_count} failures")
            else:
                fail_counts[symbol] = fail_count + 1


def build_gm_callbacks(engine):
    return _GMCallbacks(engine)
