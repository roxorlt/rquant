"""Execute an already-verified executable file descriptor."""

from __future__ import annotations

import os
from collections.abc import Mapping


class DescriptorExecutionError(RuntimeError):
    """Descriptor execution cannot preserve the verified executable identity."""


def descriptor_execution_supported() -> bool:
    """Return whether this interpreter can execute an already-open descriptor."""

    return os.execve in os.supports_fd


def exec_verified_descriptor(
    descriptor: int,
    argv: tuple[str, ...],
    environment: Mapping[str, str],
) -> object:
    execve = os.execve
    if not descriptor_execution_supported():
        raise DescriptorExecutionError(
            "formal descriptor execution is unavailable on this platform"
        )
    return execve(descriptor, argv, dict(environment))


__all__ = ["DescriptorExecutionError", "descriptor_execution_supported", "exec_verified_descriptor"]
