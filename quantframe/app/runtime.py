from __future__ import annotations

import sys
from contextlib import contextmanager

from quantframe.app.bootstrap import Application, build_application
from quantframe.app.cli import CLIArgs, parse_args


_APP: Application | None = None


@contextmanager
def _isolated_argv():
    original = list(sys.argv)
    try:
        sys.argv = [sys.argv[0]]
        yield
    finally:
        sys.argv = original


def ensure_application(args: CLIArgs | None = None) -> Application:
    global _APP
    if _APP is None:
        runtime_args = args or parse_args()
        with _isolated_argv():
            _APP = build_application(runtime_args)
    return _APP


def reset_application() -> None:
    global _APP
    _APP = None


def dispatch_callback(name: str, *args):
    app = ensure_application()
    return getattr(app, name)(*args)


def main(argv: list[str] | None = None) -> int:
    app = ensure_application(parse_args(argv))
    app.run()
    return 0
