"""Apply one single-point mutation to `src/rquant/legacy_shadow_export.py`.

Usage (from the worktree root):
    python3 tp5-baseline/mutations/apply-mutation.py m5-guard-other-only
    .venv/bin/python -m pytest tests/unit/test_legacy_shadow_export.py -q
    git checkout HEAD -- src/rquant/legacy_shadow_export.py
"""

from __future__ import annotations

import sys
from pathlib import Path

SOURCE = Path("src/rquant/legacy_shadow_export.py")

DERIVATION = """    owners = (
        frozenset({0, os.geteuid()})
        if _SESSION_MODE in allowed_modes
        else frozenset({os.geteuid()})
    )
"""
GUARD = "        or observed.st_mode & (stat.S_IWGRP | stat.S_IWOTH)\n"
DERIVATION_KEY = "        if _SESSION_MODE in allowed_modes\n"
OWNER_CLAUSE = "        or observed.st_uid not in owners\n"

MUTATIONS: dict[str, tuple[str, str]] = {
    # id: (exact text to find, replacement)
    "m0-revert-to-base": (
        DERIVATION + "    if (\n        not stat.S_ISDIR(observed.st_mode)\n"
        + OWNER_CLAUSE
        + GUARD,
        "    if (\n        not stat.S_ISDIR(observed.st_mode)\n"
        "        or observed.st_uid != os.geteuid()\n",
    ),
    "m1-drop-0o022": (GUARD, ""),
    "m2-owners-always-euid": (DERIVATION, "    owners = frozenset({os.geteuid()})\n"),
    "m3-owners-always-root-and-euid": (
        DERIVATION,
        "    owners = frozenset({0, os.geteuid()})\n",
    ),
    "m4-derive-from-root-mode": (
        DERIVATION_KEY,
        "        if _ROOT_MODE in allowed_modes\n",
    ),
    # Reviewer additions: each half of the 0o022 guard, on its own.
    "m5-guard-other-only": (GUARD, "        or observed.st_mode & stat.S_IWOTH\n"),
    "m6-guard-group-only": (GUARD, "        or observed.st_mode & stat.S_IWGRP\n"),
}


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in MUTATIONS:
        print(f"usage: {sys.argv[0]} <{' | '.join(MUTATIONS)}>", file=sys.stderr)
        return 2
    mutation = sys.argv[1]
    find, replace = MUTATIONS[mutation]
    text = SOURCE.read_text()
    if text.count(find) != 1:
        print(f"{mutation}: anchor matched {text.count(find)} times, expected 1", file=sys.stderr)
        return 1
    SOURCE.write_text(text.replace(find, replace, 1))
    print(f"applied {mutation}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
