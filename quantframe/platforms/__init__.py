from quantframe.platforms.registry import get_platform_factory, register_platform

# Import supported adapters so they self-register.
from quantframe.platforms.gm import adapter as _gm_adapter  # noqa: F401

__all__ = ["get_platform_factory", "register_platform"]
