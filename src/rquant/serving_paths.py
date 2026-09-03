"""Single source of truth for the Serving root every page process reads.

The publisher writes ``LINUX_PRODUCTION_RUNTIME_ROOT / "serving"``
(``runtime_production_profile`` publishes ``"serving_root": str(root / "serving")``),
i.e. ``/home/lighthouse/rquant/data/runtime/serving``.  Each page used to carry its
own ``os.environ.get("RQUANT_SERVING_ROOT", "data/serving")`` literal, and that
literal named a directory no publisher has ever written -- a page whose unit did
not set the variable rendered blank against a plausible-looking wrong path, and
one consumer (Strategy Lab) has no unit at all, so ``Environment=`` cannot reach it.
Both halves of the resolution live here so the six consumers cannot drift again.
"""

from __future__ import annotations

import os
from collections.abc import Mapping

SERVING_ROOT_ENV_VAR = "RQUANT_SERVING_ROOT"
"""Environment variable the four Streamlit units pin to the publisher directory."""

DEFAULT_SERVING_ROOT = "data/runtime/serving"
"""Publisher directory relative to the deploy root, used when the variable is unset."""


def serving_root_from_env(environ: Mapping[str, str] | None = None) -> str:
    """Return the Serving root this process must read.

    A blank value counts as unset: ``Environment=RQUANT_SERVING_ROOT=`` in a unit
    would otherwise resolve to the working directory instead of failing loudly.
    """

    source = os.environ if environ is None else environ
    value = source.get(SERVING_ROOT_ENV_VAR)
    if value is None or not value.strip():
        return DEFAULT_SERVING_ROOT
    return value


__all__ = [
    "DEFAULT_SERVING_ROOT",
    "SERVING_ROOT_ENV_VAR",
    "serving_root_from_env",
]
