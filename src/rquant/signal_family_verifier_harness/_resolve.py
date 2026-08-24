"""Surface resolution, exactly as ruling O2 froze it.

A `SurfaceId` value *is* `object.__module__ + "." + object.__qualname__` for the callable it
names (`authority.md` L1219-1224). Resolution therefore walks the dotted name as an
importlib module load followed by an attribute chain, requires the result to be callable,
and requires the derived qualname to equal the surface ID character for character. An alias
re-exported under another name, a wrapper, a monkeypatch, a non-callable attribute, or a
module that merely happens to expose the last component all fail that equality.

The generation is imported here and nowhere above, so a harness that resolves nothing
imports nothing.
"""

from __future__ import annotations

import importlib
from typing import Any


class SurfaceResolutionError(ValueError):
    """A frozen surface ID does not name a callable of that exact qualname."""


def _split(surface_id: str) -> tuple[str, tuple[str, ...]]:
    """Split `module.Class.method` into the longest importable module and its attributes."""

    if type(surface_id) is not str or not surface_id:
        raise SurfaceResolutionError("surface id must be a nonempty string")
    parts = surface_id.split(".")
    if len(parts) < 2 or any(not part for part in parts):
        raise SurfaceResolutionError("surface id must be a dotted qualname")
    if parts[0] != "rquant":
        raise SurfaceResolutionError("surface id must name an rquant callable")
    for cut in range(len(parts) - 1, 0, -1):
        module_name = ".".join(parts[:cut])
        try:
            importlib.import_module(module_name)
        except ImportError:
            continue
        return module_name, tuple(parts[cut:])
    raise SurfaceResolutionError(f"no importable module carries surface {surface_id}")


def resolve_surface(surface_id: str) -> Any:
    """Return the exact callable object the frozen surface ID names."""

    module_name, attributes = _split(surface_id)
    resolved: Any = importlib.import_module(module_name)
    for attribute in attributes:
        try:
            resolved = getattr(resolved, attribute)
        except AttributeError as exc:
            raise SurfaceResolutionError(f"surface {surface_id} has no attribute chain") from exc
    if not callable(resolved):
        raise SurfaceResolutionError(f"surface {surface_id} does not resolve to a callable")
    derived = f"{getattr(resolved, '__module__', '')}.{getattr(resolved, '__qualname__', '')}"
    if derived != surface_id:
        raise SurfaceResolutionError(
            f"surface {surface_id} resolves to {derived}; aliases and wrappers reject"
        )
    return resolved


def bound_surface(instance: Any, surface_id: str) -> Any:
    """Bind the frozen surface to one builder-constructed object and prove the identity.

    A bound method carries the underlying function on `__func__`; requiring that function to
    be the very object `resolve_surface` returned is what makes "this instance runs that
    exact code" checkable rather than assumed. Dunder surfaces such as
    `ServingSourceAuthorityReader.__call__` are looked up on the type, because Python's
    special-method lookup does the same.
    """

    resolved = resolve_surface(surface_id)
    attribute = surface_id.rsplit(".", 1)[1]
    if attribute.startswith("__") and attribute.endswith("__"):
        bound = getattr(type(instance), attribute, None)
        if bound is not resolved:
            raise SurfaceResolutionError(
                f"{type(instance).__name__} does not implement {surface_id}"
            )
        return lambda *args, **kwargs: resolved(instance, *args, **kwargs)
    bound = getattr(instance, attribute, None)
    if bound is None or getattr(bound, "__func__", None) is not resolved:
        raise SurfaceResolutionError(
            f"the builder-constructed {type(instance).__name__} is not bound to {surface_id}"
        )
    return bound


__all__ = ["SurfaceResolutionError", "bound_surface", "resolve_surface"]
