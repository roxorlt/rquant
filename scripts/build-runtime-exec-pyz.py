#!/usr/bin/env python3
"""Build `/usr/local/libexec/rquant-runtime-exec.pyz` as a byte-reproducible zipapp.

`authority.md` L146-160 lists this pyz in the Trusted Computing Base with owner `root:root`
and exact mode `0555`, and `runtime_authority.PRODUCTION_RUNTIME_PYZ` has referenced its path
since long before anything was built for it. Codex round-2 P1-3 makes it real: every
protected runtime unit now executes it instead of `.venv/bin/python -m rquant...`.

The artifact is pinned by hash, so it has to be a pure function of its sources. As with
`scripts/build-signal-family-verifier-harness.py`, `zipapp.create_archive` cannot promise
that — it stamps entries with the current mtime — so the archive is written here with a
frozen 1980-01-01 timestamp, sorted entry order, fixed permissions and a fixed deflate
level, and nothing compiled is ever added.

    python scripts/build-runtime-exec-pyz.py --output /tmp/rquant-runtime-exec.pyz

The script prints the artifact's SHA-256, which is the value the production runtime profile
must carry for `runtime_pyz`. It installs nothing: placing the archive under `/usr/local`
is a separately authorized root infrastructure transaction.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import stat
import sys
import zipfile
from collections.abc import Sequence
from pathlib import Path
from typing import Final

PACKAGE_DIRECTORY_NAME: Final[str] = "runtime_exec_wrapper"
ARCHIVE_PACKAGE_NAME: Final[str] = "rquant_runtime_exec_wrapper"

FROZEN_TIMESTAMP: Final[tuple[int, int, int, int, int, int]] = (1980, 1, 1, 0, 0, 0)
FROZEN_EXTERNAL_ATTR: Final[int] = (stat.S_IFREG | 0o444) << 16
FROZEN_COMPRESS_LEVEL: Final[int] = 9
ARTIFACT_MODE: Final[int] = 0o555

BOOTSTRAP_SOURCE: Final[str] = f'''"""Zipapp bootstrap for the fixed root-owned runtime wrapper."""

import sys

from {ARCHIVE_PACKAGE_NAME}.__main__ import main

sys.exit(main(sys.argv))
'''


def package_root(repository_root: Path) -> Path:
    return repository_root / "src" / "rquant" / PACKAGE_DIRECTORY_NAME


def collect_sources(package: Path) -> tuple[tuple[str, bytes], ...]:
    """Every `.py` file of the package, as sorted archive-relative name/content pairs.

    The package is relocated under `rquant_runtime_exec_wrapper/` rather than `rquant/`. The
    wrapper runs under `-I -S` with no site-packages at all, so nothing of the generation is
    on its path to shadow — but keeping the name distinct means a mistake in that argument
    can never turn into the wrapper importing generation code by accident.
    """

    if not package.is_dir():
        raise SystemExit(f"the runtime wrapper package is missing: {package}")
    entries: list[tuple[str, bytes]] = []
    for path in sorted(package.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        if path.is_symlink() or not path.is_file():
            raise SystemExit(f"the wrapper sources must be regular files: {path}")
        relative = path.relative_to(package).as_posix()
        entries.append((f"{ARCHIVE_PACKAGE_NAME}/{relative}", path.read_bytes()))
    names = {name for name, _ in entries}
    for required in ("__main__.py", "_verify.py"):
        if f"{ARCHIVE_PACKAGE_NAME}/{required}" not in names:
            raise SystemExit(f"the runtime wrapper package has no {required}")
    entries.append(("__main__.py", BOOTSTRAP_SOURCE.encode("utf-8")))
    entries.sort(key=lambda entry: entry[0])
    return tuple(entries)


def build_archive_bytes(entries: Sequence[tuple[str, bytes]]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(
        buffer,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=FROZEN_COMPRESS_LEVEL,
    ) as archive:
        for name, payload in entries:
            info = zipfile.ZipInfo(filename=name, date_time=FROZEN_TIMESTAMP)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = FROZEN_EXTERNAL_ATTR
            info.create_system = 3
            archive.writestr(info, payload)
    return buffer.getvalue()


def build_wrapper(repository_root: Path, output: Path) -> str:
    payload = build_archive_bytes(collect_sources(package_root(repository_root)))
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.partial")
    temporary.write_bytes(payload)
    temporary.chmod(ARTIFACT_MODE)
    temporary.replace(output)
    return hashlib.sha256(payload).hexdigest()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--repository-root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    print(build_wrapper(arguments.repository_root.resolve(), arguments.output.resolve()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
