from __future__ import annotations

from datetime import datetime

from yuruquant.adapters.gm.runner import run_with_gm, schedule
from yuruquant.reporting.logging import debug, error, info


class GMCallbacks:
    def __init__(self, config, gateway, engine) -> None:
        self.config = config
        self.gateway = gateway
        self.engine = engine

    def init(self, context) -> None:
        if not hasattr(context, "now"):
            context.now = datetime.now()
        self.gateway.bind_context(context)
        self.engine.initialize(context)
        self.gateway.refresh_main_contracts(context.now)
        info("gm.init", mode=self.config.runtime.mode, symbol_count=len(self.config.universe.symbols))
        if schedule is not None:
            try:
                schedule(lambda ctx: self.gateway.refresh_main_contracts(getattr(ctx, "now", datetime.now())), date_rule="1d", time_rule="09:01:00")
            except Exception:
                pass

    def on_bar(self, context, bars) -> None:
        if not hasattr(context, "now"):
            context.now = datetime.now()
        self.gateway.bind_context(context)
        self.gateway.refresh_main_contracts(context.now)
        event = self.gateway.build_market_event(list(bars or []), context.now)
        debug("gm.on_bar", bar_count=len(event.bars))
        self.engine.on_market_event(event)

    def on_order_status(self, context, order) -> None:
        _ = context
        info("gm.order_status", symbol=getattr(order, "symbol", None), status=getattr(order, "status", None))

    def on_execution_report(self, context, execrpt) -> None:
        _ = context
        info("gm.execution_report", symbol=getattr(execrpt, "symbol", None), price=getattr(execrpt, "price", None), volume=getattr(execrpt, "volume", None))

    def on_error(self, context, code, info_msg) -> None:
        _ = context
        error("gm.callback_error", code=code, info=info_msg)

    def run_gm(self) -> None:
        run_with_gm(self.config, self)
