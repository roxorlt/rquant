#!/usr/bin/env python3
"""Build the fixed Phase C verifier harness as a byte-reproducible zipapp.

The external root policy pins the harness by `harness_sha256` (`authority.md` L1408-1412),
so the artifact has to be a pure function of its sources: the same tree must always produce
the same bytes, on any machine, in any order, at any time. `zipapp.create_archive` cannot
promise that — it stamps each entry with the current mtime — so this script writes the
archive itself with a frozen 1980-01-01 timestamp, sorted entry order, fixed permissions,
and a fixed deflate level. Nothing compiled is ever added; `__pycache__` is excluded.

The package is relocated under `rquant_signal_family_verifier_harness/` inside the archive
rather than `rquant/…`. Shipping a `rquant/` tree in the zipapp would put it ahead of the
generation's own `rquant` on `sys.path` and shadow the very production code the child is
supposed to exercise.

    python scripts/build-signal-family-verifier-harness.py \\
        --output /usr/local/libexec/rquant-signal-family-verifier-harness-v1.pyz

The script prints the artifact's SHA-256, which is the value the policy must carry.
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

PACKAGE_DIRECTORY_NAME: Final[str] = "signal_family_verifier_harness"
ARCHIVE_PACKAGE_NAME: Final[str] = "rquant_signal_family_verifier_harness"

#: The zip epoch. Anything derived from "now" would break byte reproducibility.
FROZEN_TIMESTAMP: Final[tuple[int, int, int, int, int, int]] = (1980, 1, 1, 0, 0, 0)
#: 0o444 in the high half of `external_attr`, plus the regular-file type bits.
FROZEN_EXTERNAL_ATTR: Final[int] = (stat.S_IFREG | 0o444) << 16
FROZEN_COMPRESS_LEVEL: Final[int] = 9

BOOTSTRAP_SOURCE: Final[str] = f'''"""Zipapp bootstrap for the fixed Phase C verifier harness."""

import sys

from {ARCHIVE_PACKAGE_NAME}.__main__ import main

sys.exit(main())
'''


def package_root(repository_root: Path) -> Path:
    return repository_root / "src" / "rquant" / PACKAGE_DIRECTORY_NAME


def collect_sources(package: Path) -> tuple[tuple[str, bytes], ...]:
    """Every `.py` file of the package, as sorted archive-relative name/content pairs."""

    if not package.is_dir():
        raise SystemExit(f"harness package is missing: {package}")
    entries: list[tuple[str, bytes]] = []
    for path in sorted(package.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        if path.is_symlink() or not path.is_file():
            raise SystemExit(f"harness sources must be regular files: {path}")
        relative = path.relative_to(package).as_posix()
        entries.append((f"{ARCHIVE_PACKAGE_NAME}/{relative}", path.read_bytes()))
    if not entries:
        raise SystemExit(f"harness package has no sources: {package}")
    names = {name for name, _ in entries}
    if f"{ARCHIVE_PACKAGE_NAME}/__main__.py" not in names:
        raise SystemExit("harness package has no __main__.py")
    entries.append(("__main__.py", BOOTSTRAP_SOURCE.encode("utf-8")))
    entries.sort(key=lambda entry: entry[0])
    return tuple(entries)


def build_archive_bytes(entries: Sequence[tuple[str, bytes]]) -> bytes:
    """Serialize the archive deterministically, in memory, so the caller can hash it."""

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


def build_harness(repository_root: Path, output: Path) -> str:
    payload = build_archive_bytes(collect_sources(package_root(repository_root)))
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.partial")
    temporary.write_bytes(payload)
    temporary.chmod(0o555)
    temporary.replace(output)
    return hashlib.sha256(payload).hexdigest()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="where to write the .pyz artifact",
    )
    parser.add_argument(
        "--repository-root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="repository checkout that owns src/rquant (defaults to this script's checkout)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    digest = build_harness(arguments.repository_root.resolve(), arguments.output.resolve())
    print(digest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
