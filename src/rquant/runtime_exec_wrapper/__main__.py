"""Argument transport for the runtime wrapper. It accepts one role literal and nothing else.

The unit line is fixed and unit-owned:

    ExecStart=/usr/local/libexec/rquant-runtime-exec.pyz --role strategy_live

There is no second flag, no positional argument, no environment variable and no `%i`
expansion that reaches an authority decision. A template unit may still carry `%i` in its
sandbox directives — those are systemd's own path grants, not values this process reads —
but it never appears on this command line (ruling D-2).
"""

from __future__ import annotations

import os
import re
import sys
from collections.abc import Sequence

from ._verify import (
    PROTECTED_ROLES,
    RuntimeExecError,
    child_argv,
    frozen_bootstrap,
    resolve_launch,
)

USAGE = (
    "usage: rquant-runtime-exec.pyz --role <allowlisted-role> [--instance <label>]"
)
REFUSAL_EXIT_CODE = 78
#: The systemd template label grammar. It is only ever a lookup key into the root-owned
#: per-role instance allowlist, never a path component this process builds with.
_INSTANCE = re.compile(r"[a-z0-9][a-z0-9-]{0,79}")


def parse_role(argv: Sequence[str]) -> tuple[str, str | None]:
    """Accept exactly `--role <literal>` and optionally `--instance <label>`."""

    arguments = list(argv)[1:]
    if len(arguments) not in (2, 4) or arguments[0] != "--role":
        raise RuntimeExecError(USAGE)
    role = arguments[1]
    if role not in PROTECTED_ROLES:
        raise RuntimeExecError("the requested role is not an allowlisted unit-owned literal")
    instance: str | None = None
    if len(arguments) == 4:
        if arguments[2] != "--instance":
            raise RuntimeExecError(USAGE)
        instance = arguments[3]
        if type(instance) is not str or _INSTANCE.fullmatch(instance) is None:
            raise RuntimeExecError("the requested instance label is malformed")
    return role, instance


def main(argv: Sequence[str]) -> int:
    try:
        role, instance = parse_role(argv)
        launch = resolve_launch(
            role,
            instance=instance,
            source_environment=dict(os.environ),
        )
        command = child_argv(launch, frozen_bootstrap())
    except RuntimeExecError as error:
        sys.stderr.write(f"{error}\n")
        return REFUSAL_EXIT_CODE
    try:
        os.chdir(launch["working_directory"])
        os.execve(launch["python_path"], list(command), launch["environment"])
    except OSError as error:
        # M3: a failed chdir or exec must be the documented refusal, not a traceback.
        sys.stderr.write(f"the runtime child could not be executed: {error}\n")
        return REFUSAL_EXIT_CODE


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
