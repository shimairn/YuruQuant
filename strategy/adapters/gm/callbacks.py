from __future__ import annotations

import os
from datetime import datetime

from strategy.adapters.gm.backtest_window_guard import clip_backtest_window_if_needed
from strategy.adapters.gm.contract_roll import roll_main_contract
from strategy.adapters.gm.runner import run_with_gm, schedule
from strategy.observability.log import debug, error, info


class _GMCallbacks:
    def __init__(self, engine):
        self.engine = engine

    def _clip_backtest_window_if_needed(self) -> None:
        clip_backtest_window_if_needed(self.engine.cfg)

    def init(self, context):
        if not hasattr(context, "now"):
            context.now = datetime.now()
        self.engine.initialize_runtime(context)
        info(
            "gm.init",
            mode=self.engine.cfg.runtime.mode,
            symbol_count=len(self.engine.cfg.runtime.symbols),
            freq_5m=self.engine.cfg.runtime.freq_5m,
            freq_1h=self.engine.cfg.runtime.freq_1h,
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

        trade_day = context.now.strftime("%Y-%m-%d")
        if self.engine.runtime.last_roll_date != trade_day:
            roll_main_contract(self.engine, context)

        debug(
            "gm.on_bar",
            trade_day=trade_day,
            bar_count=len(bars),
            mapped_symbol_count=len(self.engine.runtime.symbol_to_csymbol),
        )
        self.engine.process_symbols_by_bars(bars)

    def on_order_status(self, context, order):
        _ = context
        info(
            "gm.order_status",
            symbol=getattr(order, "symbol", None),
            status=getattr(order, "status", None),
        )

    def on_execution_report(self, context, execrpt):
        _ = context
        info(
            "gm.execution_report",
            symbol=getattr(execrpt, "symbol", None),
            price=getattr(execrpt, "price", None),
            volume=getattr(execrpt, "volume", None),
        )

    def on_error(self, context, code, info_msg):
        _ = context
        error("gm.callback_error", code=code, info=info_msg)

    def run_gm(self):
        self._clip_backtest_window_if_needed()
        local_flag = "GM_FORCE" + "_LOCAL"
        if str(os.getenv(local_flag, "")).strip().lower() in {"1", "true", "yes", "on"}:
            raise RuntimeError("GM_FORCE_LOCAL is no longer supported; local replay has been removed.")
        run_with_gm(self.engine.cfg, self)


def build_gm_callbacks(engine):
    return _GMCallbacks(engine)
