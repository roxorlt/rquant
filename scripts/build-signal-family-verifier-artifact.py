#!/usr/bin/env python3
"""Build the root-owned, content-addressed signal-family verifier artifact.

Codex round-2 P1-4 forbids the root verifier from importing business code out of a mutable
checkout. The replacement is a pair:

* a venv-shaped tree — `pyvenv.cfg`, `lib/python3.11/site-packages/` holding the verifier's
  exact import closure and its third-party dependencies — installed under
  `/usr/local/lib/rquant-signal-family-verifier/<content-id>/`, root-owned and read-only;
* a fixed entry archive at `/usr/local/libexec/rquant-signal-family-verifier-v1.pyz` that
  carries that tree's canonical manifest frozen inside it.

The content id is the SHA-256 of the manifest's canonical bytes, so the tree names itself
and one flipped byte anywhere under it is a different artifact that the entry refuses.

    python scripts/build-signal-family-verifier-artifact.py --output-root /tmp/staging

Determinism follows `scripts/build-signal-family-verifier-harness.py` exactly — frozen
1980-01-01 zip timestamps, sorted entries, fixed permissions, fixed deflate level, no
`__pycache__` — because both artifacts are pinned by hash and a build that varied would
break the reproducibility argument for either.

This script installs nothing. Placing the tree or the entry under `/usr/local` is a separate
root infrastructure transaction with its own explicit user authorization.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import shutil
import stat
import subprocess
import sys
import zipfile
from collections.abc import Sequence
from pathlib import Path
from typing import Final

REPOSITORY_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT / "src"))

from rquant.signal_family_verifier_entry import _artifact  # noqa: E402

PACKAGE_DIRECTORY_NAME: Final[str] = "signal_family_verifier_entry"
ARCHIVE_PACKAGE_NAME: Final[str] = "rquant_signal_family_verifier_entry"

FROZEN_TIMESTAMP: Final[tuple[int, int, int, int, int, int]] = (1980, 1, 1, 0, 0, 0)
FROZEN_EXTERNAL_ATTR: Final[int] = (stat.S_IFREG | 0o444) << 16
FROZEN_COMPRESS_LEVEL: Final[int] = 9

#: The verifier's own import closure, measured rather than guessed: importing
#: `rquant.signal_family_root_verifier` in a bare interpreter reaches exactly these modules.
#: `--verify-closure` re-measures it and refuses a build whose shipped set has drifted.
DEFAULT_CLOSURE_PROBE: Final[str] = (
    "import json, sys\n"
    "import rquant.signal_family_root_verifier\n"
    "print(json.dumps(sorted(n for n in sys.modules if n.split('.')[0] == 'rquant')))\n"
)
DEFAULT_THIRD_PARTY: Final[tuple[str, ...]] = (
    "annotated_types",
    "pydantic",
    "pydantic_core",
    "typing_extensions",
    "typing_inspection",
)
#: The production host the artifact is installed on. A macOS-built extension can never load
#: there, so the default is the deployment target rather than the build host. The offline
#: suite passes its own platform to exercise the entry archive on a developer machine.
DEFAULT_TARGET_PLATFORM: Final[str] = "linux"
_FOREIGN_PLATFORM_TAGS: Final[dict[str, tuple[str, ...]]] = {
    "linux": ("-darwin", "-win32", "-win_amd64"),
    "darwin": ("-linux-gnu", "-win32", "-win_amd64"),
}
PYVENV_TEMPLATE: Final[str] = (
    "home = {home}\n"
    "include-system-site-packages = false\n"
    "version = {version}\n"
)
_EXCLUDED_DIRECTORY_NAMES: Final[frozenset[str]] = frozenset({"__pycache__"})
_EXCLUDED_SUFFIXES: Final[tuple[str, ...]] = (".pyc", ".pyo", ".pth")
_EXCLUDED_FILE_NAMES: Final[frozenset[str]] = frozenset(
    {"sitecustomize.py", "usercustomize.py"}
)

BOOTSTRAP_SOURCE: Final[str] = f'''"""Zipapp bootstrap for the root-owned verifier entry."""

import sys

from {ARCHIVE_PACKAGE_NAME}.__main__ import main

sys.exit(main(sys.argv))
'''


def measured_closure(repository_root: Path, interpreter: Path) -> tuple[str, ...]:
    """Import the verifier in a bare interpreter and report the `rquant` modules it reached."""

    completed = subprocess.run(
        [str(interpreter), "-c", DEFAULT_CLOSURE_PROBE],
        cwd=str(repository_root),
        check=True,
        capture_output=True,
        text=True,
        env={"PYTHONPATH": str(repository_root / "src"), "PATH": "/usr/bin:/bin"},
    )
    return tuple(json.loads(completed.stdout))


def _copy_module(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)


def _copy_package(source: Path, target: Path) -> None:
    for path in sorted(source.rglob("*")):
        if any(part in _EXCLUDED_DIRECTORY_NAMES for part in path.parts):
            continue
        if path.is_symlink():
            raise SystemExit(f"the artifact source holds a symbolic link: {path}")
        if path.is_dir():
            (target / path.relative_to(source)).mkdir(parents=True, exist_ok=True)
            continue
        if path.suffix in _EXCLUDED_SUFFIXES or path.name in _EXCLUDED_FILE_NAMES:
            continue
        destination = target / path.relative_to(source)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, destination)


class ArtifactBuildError(SystemExit):
    """The build refuses rather than producing a tree that cannot import."""


def assert_source_venv_matches_target(
    *,
    source_venv: Path,
    python_version: str,
    target_platform: str = DEFAULT_TARGET_PLATFORM,
) -> None:
    """Refuse a source venv whose interpreter does not match the declared target.

    The tree carries native extensions — `pydantic_core` ships a compiled `.so` — and the
    installed `pyvenv.cfg` declares the interpreter that is meant to load them. Building
    from a 3.13/macOS venv while declaring 3.11 produced a tree holding
    `_pydantic_core.cpython-313-darwin.so` under `lib/python3.11/site-packages`: unimportable
    by the production interpreter, and a different content id, which is the TCB anchor. The
    build now fails instead of emitting it.
    """

    major_minor = ".".join(python_version.split(".")[:2])
    site = source_venv / f"lib/python{major_minor}/site-packages"
    if not site.is_dir():
        available = sorted(
            path.parent.name for path in source_venv.glob("lib/python3.*/site-packages")
        )
        raise ArtifactBuildError(
            f"the source venv has no {site.relative_to(source_venv)}; "
            f"--python-version says {python_version} but the venv holds {available or 'nothing'}"
        )
    config = source_venv / "pyvenv.cfg"
    if config.is_file():
        declared = {
            key.strip(): value.strip()
            for key, _, value in (
                line.partition("=") for line in config.read_text(encoding="utf-8").splitlines()
            )
            if key.strip()
        }
        observed = declared.get("version") or declared.get("version_info", "")
        if observed and not observed.startswith(major_minor):
            raise ArtifactBuildError(
                f"the source venv declares Python {observed}, "
                f"but --python-version says {python_version}"
            )

    expected_tag = f"cpython-{major_minor.replace('.', '')}"
    for extension in sorted(site.rglob("*.so")):
        name = extension.name
        if "cpython-" not in name:
            continue
        if expected_tag not in name:
            raise ArtifactBuildError(
                f"the source venv holds a native extension built for another interpreter: "
                f"{extension.relative_to(source_venv)} does not carry {expected_tag}"
            )
        foreign = _FOREIGN_PLATFORM_TAGS.get(target_platform, ())
        if any(tag in name for tag in foreign):
            raise ArtifactBuildError(
                f"the source venv holds a native extension for another platform: "
                f"{extension.relative_to(source_venv)} cannot load on a "
                f"{target_platform} host"
            )


def materialize_tree(
    *,
    repository_root: Path,
    source_venv: Path,
    staging: Path,
    rquant_modules: Sequence[str],
    third_party: Sequence[str],
    interpreter_home: str,
    python_version: str,
) -> None:
    """Lay out the venv-shaped tree. Sources only; nothing compiled and nothing generated."""

    site_packages = staging / _artifact.SITE_PACKAGES_RELATIVE
    site_packages.mkdir(parents=True)
    (staging / "pyvenv.cfg").write_text(
        PYVENV_TEMPLATE.format(home=interpreter_home, version=python_version),
        encoding="utf-8",
    )

    package_root = site_packages / "rquant"
    package_root.mkdir()
    for name in sorted(set(rquant_modules)):
        relative = name.removeprefix("rquant")
        if not relative:
            _copy_module(
                repository_root / "src" / "rquant" / "__init__.py",
                package_root / "__init__.py",
            )
            continue
        parts = relative.lstrip(".").split(".")
        candidate = repository_root / "src" / "rquant" / Path(*parts)
        if candidate.is_dir():
            _copy_package(candidate, package_root / Path(*parts))
        else:
            _copy_module(
                candidate.with_suffix(".py"),
                package_root / Path(*parts[:-1]) / f"{parts[-1]}.py",
            )

    source_site = source_venv / _artifact.SITE_PACKAGES_RELATIVE
    if not source_site.is_dir():
        matches = sorted((source_venv / "lib").glob("python3.*/site-packages"))
        if not matches:
            raise SystemExit(f"the source venv has no site-packages: {source_venv}")
        source_site = matches[0]
    for name in sorted(set(third_party)):
        directory = source_site / name
        module = source_site / f"{name}.py"
        if directory.is_dir():
            _copy_package(directory, site_packages / name)
        elif module.is_file():
            _copy_module(module, site_packages / f"{name}.py")
        else:
            raise SystemExit(f"the source venv has no dependency named {name!r}")

    _artifact.freeze_tree_modes(staging)


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


def frozen_manifest_source(content_id: str, manifest_json: bytes) -> str:
    """The generated module the archive carries in place of the checkout placeholder."""

    return (
        '"""The tree manifest frozen into this entry archive at build time."""\n\n'
        "from __future__ import annotations\n\n"
        "from typing import Final\n\n"
        "from ._artifact import TreeEntry, VerifierArtifactError, parse_manifest\n\n"
        f"CONTENT_ID: Final[str] = {content_id!r}\n"
        f"MANIFEST_JSON: Final[bytes] = {manifest_json!r}\n\n\n"
        "def require_frozen_manifest() -> tuple[str, tuple[TreeEntry, ...]]:\n"
        '    """The frozen pair. A built entry always has one."""\n\n'
        "    if not CONTENT_ID or not MANIFEST_JSON:\n"
        "        raise VerifierArtifactError(\n"
        '            "this verifier entry has not been built"\n'
        "        )\n"
        "    return CONTENT_ID, parse_manifest(MANIFEST_JSON)\n"
    )


def collect_entry_sources(
    repository_root: Path,
    *,
    content_id: str,
    manifest_json: bytes,
) -> tuple[tuple[str, bytes], ...]:
    package = repository_root / "src" / "rquant" / PACKAGE_DIRECTORY_NAME
    if not package.is_dir():
        raise SystemExit(f"the verifier entry package is missing: {package}")
    entries: list[tuple[str, bytes]] = []
    for path in sorted(package.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        if path.is_symlink() or not path.is_file():
            raise SystemExit(f"the entry sources must be regular files: {path}")
        relative = path.relative_to(package).as_posix()
        if relative == "_frozen_manifest.py":
            payload = frozen_manifest_source(content_id, manifest_json).encode("utf-8")
        else:
            payload = path.read_bytes()
        entries.append((f"{ARCHIVE_PACKAGE_NAME}/{relative}", payload))
    names = {name for name, _ in entries}
    for required in ("__main__.py", "_artifact.py", "_cli.py", "_frozen_manifest.py"):
        if f"{ARCHIVE_PACKAGE_NAME}/{required}" not in names:
            raise SystemExit(f"the verifier entry package has no {required}")
    entries.append(("__main__.py", BOOTSTRAP_SOURCE.encode("utf-8")))
    entries.sort(key=lambda entry: entry[0])
    return tuple(entries)


def build(arguments: argparse.Namespace) -> dict[str, object]:
    repository_root = arguments.repository_root.resolve()
    output_root = arguments.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    modules = list(arguments.rquant_module)
    if not modules:
        modules = list(measured_closure(repository_root, Path(sys.executable)))
    third_party = list(arguments.third_party) or list(DEFAULT_THIRD_PARTY)

    assert_source_venv_matches_target(
        source_venv=arguments.source_venv.resolve(),
        python_version=arguments.python_version,
        target_platform=arguments.target_platform,
    )
    staging = output_root / ".staging"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    try:
        materialize_tree(
            repository_root=repository_root,
            source_venv=arguments.source_venv.resolve(),
            staging=staging,
            rquant_modules=modules,
            third_party=third_party,
            interpreter_home=arguments.interpreter_home,
            python_version=arguments.python_version,
        )
        manifest = _artifact.build_tree_manifest(staging)
        manifest_json = _artifact.canonical_manifest_bytes(manifest)
        content_id = _artifact.content_id(manifest)
        tree_root = output_root / content_id
        if tree_root.exists():
            _artifact.remove_frozen_tree(tree_root)
        _artifact.relocate_frozen_tree(staging, tree_root)
    finally:
        if staging.exists():
            _artifact.remove_frozen_tree(staging)

    payload = build_archive_bytes(
        collect_entry_sources(
            repository_root,
            content_id=content_id,
            manifest_json=manifest_json,
        )
    )
    entry_path = arguments.entry_output or (output_root / _artifact.ARTIFACT_ENTRY_PATH.name)
    entry_path = Path(entry_path).resolve()
    entry_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = entry_path.with_name(f".{entry_path.name}.partial")
    temporary.write_bytes(payload)
    temporary.chmod(_artifact.ENTRY_FILE_MODE)
    temporary.replace(entry_path)

    plan = _artifact.install_plan(content_id=content_id)
    return {
        "content_id": content_id,
        "manifest_sha256": hashlib.sha256(manifest_json).hexdigest(),
        "manifest_entries": len(manifest),
        "entry_sha256": hashlib.sha256(payload).hexdigest(),
        "entry_path": str(entry_path),
        "tree_root": str(tree_root),
        "install_tree_root": str(plan.tree_root),
        "install_entry_path": str(plan.entry_path),
        "install_owner_uid": plan.owner_uid,
        "install_owner_gid": plan.owner_gid,
        "install_directory_mode": f"{plan.directory_mode:04o}",
        "install_file_mode": f"{plan.file_mode:04o}",
        "install_entry_mode": f"{plan.entry_mode:04o}",
        "rquant_modules": sorted(modules),
        "third_party": sorted(third_party),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--entry-output", type=Path, default=None)
    parser.add_argument("--repository-root", type=Path, default=REPOSITORY_ROOT)
    parser.add_argument("--source-venv", type=Path, default=REPOSITORY_ROOT / ".venv")
    parser.add_argument("--rquant-module", action="append", default=[])
    parser.add_argument("--third-party", action="append", default=[])
    parser.add_argument("--interpreter-home", default="/usr/bin")
    parser.add_argument("--python-version", default="3.11.15")
    parser.add_argument("--target-platform", default=DEFAULT_TARGET_PLATFORM)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    report = build(build_parser().parse_args(argv))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
