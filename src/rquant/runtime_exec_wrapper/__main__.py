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
import sys
from collections.abc import Sequence

from ._verify import (
    PROTECTED_ROLES,
    RuntimeExecError,
    child_argv,
    frozen_bootstrap,
    resolve_launch,
)

USAGE = "usage: rquant-runtime-exec.pyz --role <allowlisted-role>"
REFUSAL_EXIT_CODE = 78


def parse_role(argv: Sequence[str]) -> str:
    arguments = list(argv)[1:]
    if len(arguments) != 2 or arguments[0] != "--role":
        raise RuntimeExecError(USAGE)
    role = arguments[1]
    if role not in PROTECTED_ROLES:
        raise RuntimeExecError("the requested role is not an allowlisted unit-owned literal")
    return role


def main(argv: Sequence[str]) -> int:
    try:
        role = parse_role(argv)
        launch = resolve_launch(role, source_environment=dict(os.environ))
        command = child_argv(launch, frozen_bootstrap())
    except RuntimeExecError as error:
        sys.stderr.write(f"{error}\n")
        return REFUSAL_EXIT_CODE
    os.chdir(launch["working_directory"])
    os.execve(launch["python_path"], list(command), launch["environment"])


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
