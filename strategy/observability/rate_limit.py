from __future__ import annotations

from collections import defaultdict


_COUNTS: dict[str, int] = defaultdict(int)


def should_emit(key: str, every_n: int) -> bool:
    step = max(int(every_n), 1)
    _COUNTS[key] = int(_COUNTS.get(key, 0)) + 1
    idx = _COUNTS[key]
    return idx == 1 or (idx % step) == 0


def reset_counters() -> None:
    _COUNTS.clear()

