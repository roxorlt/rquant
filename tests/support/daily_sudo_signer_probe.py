#!/usr/bin/env python3
"""Linux-like sudo/NoNewPrivileges probe for the Daily receipt signer boundary."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

FIXED_DAILY_SIGNER_COMMAND = (
    "/usr/bin/sudo",
    "-n",
    "/usr/local/libexec/rquant-daily-receipt-signer",
)


def main(arguments: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-new-privileges", choices=("true", "false"), required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    parsed = parser.parse_args(arguments)
    command = tuple(parsed.command)

    if command != FIXED_DAILY_SIGNER_COMMAND:
        print("sudoers denied Daily signer extra arguments", file=sys.stderr)
        return 2
    if parsed.no_new_privileges == "true":
        print("sudo signer blocked by NoNewPrivileges", file=sys.stderr)
        return 1

    helper = os.environ.get("RQUANT_DAILY_SIGNER_PROBE_HELPER")
    if helper is None:
        print("probe helper is not configured", file=sys.stderr)
        return 3
    helper_path = Path(helper)
    if not helper_path.is_absolute() or not helper_path.is_file():
        print("probe helper is unavailable", file=sys.stderr)
        return 3
    completed = subprocess.run(
        (sys.executable, str(helper_path)),
        input=sys.stdin.buffer.read(),
        check=False,
        capture_output=True,
        timeout=5,
        env={"LANG": "C", "PATH": "/usr/bin:/bin"},
    )
    sys.stdout.buffer.write(completed.stdout)
    sys.stderr.buffer.write(completed.stderr)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
