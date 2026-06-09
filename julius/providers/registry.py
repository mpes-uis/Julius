"""Provider registry — maps provider name string to the correct Provider class."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from julius.providers.base import Provider

_registry: dict[str, type["Provider"]] = {}


def register(cls: type["Provider"]) -> type["Provider"]:
    """Class decorator that registers a Provider subclass."""
    _registry[cls.name] = cls
    return cls


def get_provider(name: str) -> "Provider":
    """Instantiate a registered provider by name. Raises KeyError if not found."""
    if name not in _registry:
        available = ", ".join(sorted(_registry))
        raise KeyError(
            f"Provider '{name}' not found. Available: {available or '(none registered yet)'}"
        )
    return _registry[name]()


def list_providers() -> list[str]:
    """Return sorted list of registered provider names."""
    return sorted(_registry)
