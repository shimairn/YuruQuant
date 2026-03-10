from __future__ import annotations

from collections.abc import Callable


PlatformFactory = Callable[[object], object]

_PLATFORM_FACTORIES: dict[str, PlatformFactory] = {}


def register_platform(name: str, factory: PlatformFactory) -> None:
    normalized = str(name).strip().lower()
    if not normalized:
        raise ValueError("platform name must be non-empty")
    _PLATFORM_FACTORIES[normalized] = factory


def get_platform_factory(name: str) -> PlatformFactory:
    normalized = str(name).strip().lower()
    factory = _PLATFORM_FACTORIES.get(normalized)
    if factory is None:
        available = ", ".join(sorted(_PLATFORM_FACTORIES)) or "<none>"
        raise ValueError(f"unsupported platform '{name}'. available: {available}")
    return factory
