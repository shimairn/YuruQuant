from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from quantframe.app.cli import CLIArgs
from quantframe.app.config import AppConfig, LoadedResources, load_config, load_object, load_resources
from quantframe.platforms import get_platform_factory
from quantframe.reporting import CsvReporter
from quantframe.trend import TrendEngine


@dataclass
class Application:
    config: AppConfig
    resources: LoadedResources
    platform: object
    strategy: object
    engine: TrendEngine

    def initialize(self, context: object | None = None) -> None:
        self.platform.bind_context(context)
        self.platform.initialize()
        self.platform.subscribe(self.resources.universe, self.strategy.decision_frequency, self.strategy.history_bars)

    def on_bar(self, context: object, raw_bars: list[object]) -> None:
        self.platform.bind_context(context)
        bars = self.platform.normalize_bars(raw_bars)
        self.engine.on_bars(bars)

    def on_order_status(self, context: object, order: object) -> None:
        _ = context
        _ = order

    def on_execution_report(self, context: object, execution: object) -> None:
        _ = context
        _ = execution

    def on_error(self, context: object, code: object, info: object) -> None:
        _ = context
        _ = code
        _ = info

    def run(self) -> None:
        self.platform.run(self)


def _apply_cli_overrides(config: AppConfig, args: CLIArgs) -> AppConfig:
    runtime = config.runtime
    platform = config.platform
    gm = platform.gm
    if args.mode:
        runtime = type(runtime)(mode=args.mode, run_id=runtime.run_id)
    if args.run_id:
        runtime = type(runtime)(mode=runtime.mode, run_id=args.run_id)
    elif not runtime.run_id:
        runtime = type(runtime)(mode=runtime.mode, run_id=f"framework_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    if gm is not None:
        gm = type(gm)(
            token=args.token or gm.token,
            strategy_id=args.strategy_id or gm.strategy_id,
            serv_addr=args.serv_addr or gm.serv_addr,
            subscribe_wait_group=gm.subscribe_wait_group,
            wait_group_timeout=gm.wait_group_timeout,
            backtest=gm.backtest,
        )
        platform = type(platform)(name=platform.name, gm=gm)
    return type(config)(
        runtime=runtime,
        platform=platform,
        resources=config.resources,
        strategy=config.strategy,
        reporting=config.reporting,
    )


def build_application(args: CLIArgs) -> Application:
    config = _apply_cli_overrides(load_config(args.config), args)
    resources = load_resources(config)
    platform_factory = get_platform_factory(config.platform.name)
    platform = platform_factory(config)
    strategy_factory = load_object(config.strategy.factory)
    strategy = strategy_factory(config=config, resources=resources)
    reporter = CsvReporter(output_dir=config.reporting.output_dir, enabled=config.reporting.enabled)
    engine = TrendEngine(platform=platform, instruments=resources.universe, strategy=strategy, reporter=reporter)
    return Application(config=config, resources=resources, platform=platform, strategy=strategy, engine=engine)
