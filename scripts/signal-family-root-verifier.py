#!/usr/bin/env python3
"""The checkout copy of the root verifier entry point. It refuses to run.

Codex round-2 P1-4: the root verifier must never run from a mutable checkout. This file used
to insert `<checkout>/src` at `sys.path[0]` and import the verifier from it, which put the
whole privileged sequence — the anchored policy open, the deployment lock, the append store —
under the authority of whatever bytes happened to be in a `lighthouse`-writable working tree.

The production entry point is now the fixed root-owned archive built by
`scripts/build-signal-family-verifier-artifact.py`:

    /usr/bin/python3.11 -I -S \
      /usr/local/libexec/rquant-signal-family-verifier-v1.pyz verify

Installing that archive and its content-addressed tree under `/usr/local` is a separate root
infrastructure transaction with its own explicit user authorization (`authority.md`
L1389-1395). This script exists only so that running the old path fails loudly instead of
silently doing the dangerous thing.
"""

from __future__ import annotations

import sys

INSTALLED_ENTRY = "/usr/local/libexec/rquant-signal-family-verifier-v1.pyz"
REFUSAL_EXIT_CODE = 78


def main() -> int:
    sys.stderr.write(
        "the signal-family root verifier refuses to run from a mutable checkout\n"
        f"run the installed root-owned artifact instead:\n"
        f"  /usr/bin/python3.11 -I -S {INSTALLED_ENTRY} <verify|revoke|rollback>\n"
        "build it with scripts/build-signal-family-verifier-artifact.py; installing it is a\n"
        "separately authorized root infrastructure transaction\n"
    )
    return REFUSAL_EXIT_CODE


if __name__ == "__main__":
    raise SystemExit(main())
