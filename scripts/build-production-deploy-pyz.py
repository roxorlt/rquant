#!/usr/bin/env python3
"""Build `/usr/local/libexec/rquant-production-deploy.pyz` as a byte-reproducible zipapp.

S1 §1.3.2: the root publication transaction must not be `root` running code out of the
`lighthouse`-writable checkout (`production-interpreter-authority.md` L146-158), so the few
modules it needs are frozen into one root-owned archive. Its runtime surface is the standard
library plus `rquant.strict_json`, `rquant.runtime_authority`, `rquant.runtime_authority_publish`
and the wrapper's `_verify` (for the pre-publication preflight); the build refuses any other
import at the top level of a packaged module.

Reproducibility follows `scripts/build-runtime-exec-pyz.py` exactly: frozen 1980-01-01
timestamps, sorted entries, fixed permissions, fixed deflate level, nothing compiled.

    python scripts/build-production-deploy-pyz.py --repository-root . --output /tmp/deploy.pyz

The script prints the artifact's SHA-256 — the value the production profile carries as
`deploy_pyz.sha256` — and installs nothing.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import io
import stat
import sys
import zipfile
from collections.abc import Sequence
from pathlib import Path
from typing import Final

FROZEN_TIMESTAMP: Final[tuple[int, int, int, int, int, int]] = (1980, 1, 1, 0, 0, 0)
FROZEN_EXTERNAL_ATTR: Final[int] = (stat.S_IFREG | 0o444) << 16
FROZEN_COMPRESS_LEVEL: Final[int] = 9
ARTIFACT_MODE: Final[int] = 0o555

#: Archive name -> checkout-relative source. `rquant/__init__.py` is a written shell, not the
#: checkout's, so nothing of the package's import chain can ride in.
PACKAGED_SOURCES: Final[tuple[tuple[str, str], ...]] = (
    ("rquant/strict_json.py", "src/rquant/strict_json.py"),
    ("rquant/runtime_authority.py", "src/rquant/runtime_authority.py"),
    ("rquant/runtime_authority_publish.py", "src/rquant/runtime_authority_publish.py"),
    ("rquant/runtime_exec_wrapper/__init__.py", "src/rquant/runtime_exec_wrapper/__init__.py"),
    ("rquant/runtime_exec_wrapper/_verify.py", "src/rquant/runtime_exec_wrapper/_verify.py"),
)
PACKAGE_SHELL: Final[str] = '"""Frozen shell of the `rquant` package inside the deploy pyz."""\n'
BOOTSTRAP_SOURCE: Final[str] = '''"""Zipapp bootstrap for the root-owned deploy transaction."""

import sys

from rquant.runtime_authority_publish import main

sys.exit(main(sys.argv[1:]))
'''
#: The only non-stdlib modules a packaged module may import at its top level.
ALLOWED_PACKAGE_IMPORTS: Final[frozenset[str]] = frozenset(
    {
        "rquant",
        "rquant.strict_json",
        "rquant.runtime_authority",
        "rquant.runtime_authority_publish",
        "rquant.runtime_exec_wrapper",
        "rquant.runtime_exec_wrapper._verify",
    }
)


def _top_level_imports(source: str, archive_name: str) -> set[str]:
    try:
        tree = ast.parse(source, filename=archive_name)
    except SyntaxError as error:
        raise SystemExit(f"{archive_name} does not parse: {error}") from error
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                raise SystemExit(f"{archive_name} uses a relative import; the pyz forbids it")
            names.add(node.module or "")
    return names


def assert_stdlib_only(source: str, archive_name: str) -> None:
    stdlib = sys.stdlib_module_names
    for name in sorted(_top_level_imports(source, archive_name)):
        root = name.split(".")[0]
        if root in stdlib or name == "__future__":
            continue
        if name in ALLOWED_PACKAGE_IMPORTS:
            continue
        raise SystemExit(f"{archive_name} imports {name!r}, which the deploy pyz cannot carry")


def collect_sources(repository_root: Path) -> tuple[tuple[str, bytes], ...]:
    entries: list[tuple[str, bytes]] = [
        ("__main__.py", BOOTSTRAP_SOURCE.encode("utf-8")),
        ("rquant/__init__.py", PACKAGE_SHELL.encode("utf-8")),
    ]
    for archive_name, relative in PACKAGED_SOURCES:
        path = repository_root / relative
        if path.is_symlink() or not path.is_file():
            raise SystemExit(f"the deploy pyz source must be a regular file: {path}")
        payload = path.read_bytes()
        assert_stdlib_only(payload.decode("utf-8"), archive_name)
        entries.append((archive_name, payload))
    assert_stdlib_only(BOOTSTRAP_SOURCE, "__main__.py")
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


def build_deploy_pyz(repository_root: Path, output: Path) -> str:
    payload = build_archive_bytes(collect_sources(repository_root))
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
    print(build_deploy_pyz(arguments.repository_root.resolve(), arguments.output.resolve()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
