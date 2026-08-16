"""Execute an already-verified executable file descriptor."""

from __future__ import annotations

import os
from collections.abc import Mapping


class DescriptorExecutionError(RuntimeError):
    """Descriptor execution cannot preserve the verified executable identity."""


def exec_verified_descriptor(
    descriptor: int,
    argv: tuple[str, ...],
    environment: Mapping[str, str],
) -> object:
    execve = os.execve
    if execve not in os.supports_fd:
        raise DescriptorExecutionError(
            "formal descriptor execution is unavailable on this platform"
        )
    return execve(descriptor, argv, dict(environment))


__all__ = ["DescriptorExecutionError", "exec_verified_descriptor"]
