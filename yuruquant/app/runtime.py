from __future__ import annotations

import sys
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from yuruquant.app.cli import CLIArgs, safe_parse_args

if TYPE_CHECKING:
    from yuruquant.app.bootstrap import Application


_APP: Any | None = None


@contextmanager
def isolated_argv():
    original_argv = list(sys.argv)
    try:
        sys.argv = [sys.argv[0]]
        yield
    finally:
        sys.argv = original_argv


def ensure_application(args: CLIArgs | None = None):
    global _APP
    if _APP is None:
        runtime_args = args or safe_parse_args()
        with isolated_argv():
            from yuruquant.app.bootstrap import build_application

            _APP = build_application(runtime_args)
    return _APP


def dispatch_callback(name: str, *args):
    app = ensure_application()
    callback = getattr(app.callbacks, name)
    return callback(*args)


def main() -> int:
    app = ensure_application(safe_parse_args())
    from yuruquant.reporting import info, warn

    info(
        'runtime.startup',
        mode=app.config.runtime.mode,
        run_id=app.config.runtime.run_id,
        symbols=len(app.config.universe.symbols),
        entry_frequency=app.config.universe.entry_frequency,
        trend_frequency=app.config.universe.trend_frequency,
    )
    try:
        app.callbacks.run_gm()
    except Exception:
        warn('runtime.run_failed', mode=app.config.runtime.mode, run_id=app.config.runtime.run_id)
        raise
    return 0
