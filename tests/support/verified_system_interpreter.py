"""Materialize a base interpreter that passes the release identity predicates.

`rquant.release_generation._verified_interpreter` requires the deployment system
Python to be a regular file, not a symlink, owned by the calling uid, with a
single link, no group/other write bit, and the owner execute bit. Test fixtures
used to point a synthetic `pyvenv.cfg` at `sys.base_prefix / "bin"`, which hands
the gate whatever interpreter the host happens to run.

On GitHub's `ubuntu-24.04` runner that interpreter is the hosted tool cache
build, which fails two of the six predicates: the image build installs it as
root and `images/ubuntu/scripts/build/configure-system.sh` finishes with
`chmod -R 777 /opt`, so the binary is `uid=0 mode=0777` while the job steps run
as `runner` (uid 1001). Locally (macOS, uv-managed interpreter) the same
fixtures pass, which is why this only ever broke on CI.

The fix is to give the fixtures a base interpreter they own: copy the real base
executable into a test-owned directory (mode 0700, fresh inode, single link) and
symlink the standard library next to it so the copy still starts and reports the
right version/ABI. The production predicate set is untouched.
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

__all__ = ["materialize_system_interpreter", "system_interpreter_names"]

_LINKED_PREFIX_ENTRIES = ("lib", "lib64", "include")


def system_interpreter_names() -> tuple[str, ...]:
    """Return every executable name a venv may resolve against the fake home.

    CPython derives `sys._base_executable` as `<pyvenv home>/<basename of the
    invoked executable>`, so the materialized home must carry each name a
    fixture venv might be launched under.
    """
    version = f"{sys.version_info.major}.{sys.version_info.minor}"
    return ("python", f"python{sys.version_info.major}", f"python{version}")


def materialize_system_interpreter(root: Path) -> Path:
    """Materialize a private copy of the base interpreter under `root`.

    Returns the `bin` directory to record as `home` in a fixture `pyvenv.cfg`.
    """
    source = Path(getattr(sys, "_base_executable", None) or sys.executable).resolve(strict=True)
    base_prefix = Path(sys.base_prefix)
    root.mkdir(parents=True, exist_ok=True)
    root.chmod(0o700)
    bin_directory = root / "bin"
    bin_directory.mkdir(exist_ok=True)
    bin_directory.chmod(0o700)
    for name in system_interpreter_names():
        target = bin_directory / name
        if target.exists():
            continue
        shutil.copy2(source, target)
        target.chmod(0o700)
        observed = target.lstat()
        if observed.st_nlink != 1 or observed.st_uid != os.getuid():
            raise AssertionError(f"materialized interpreter {target} is not a private copy")
    for entry in _LINKED_PREFIX_ENTRIES:
        origin = base_prefix / entry
        link = root / entry
        if origin.exists() and not link.exists():
            link.symlink_to(origin)
    return bin_directory
