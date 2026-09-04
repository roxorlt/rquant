"""`rquant runtime-authority-stage`: lay a runtime authority generation out of a checkout.

The unprivileged half of S1 §1.3. From a clean checkout at a named commit, the frozen
`.venv`, the installed wrapper and deploy pyz and the system interpreter it computes, in
memory, everything the root transaction will install — the closure profile, the generation
tree with its full manifest, the candidate `current.json` — and a `plan.json` that carries
the sha256 of every one of those files. Without `--apply` it prints the plan and writes
nothing at all; with it, the whole staging directory is materialised under a temporary name,
re-hashed against the prediction and renamed into place. The sha256 of `plan.json` is what
the operator carries to the root side by hand.

`--bootstrap-from-checkout` (S1 §9.3) derives the 25 instanced roles' labels and service
manifests from the frozen constants of the checkout instead of a legacy `data/runtime`
generation, which does not exist on a first installation. It never reads or creates that
directory. It imports `rquant.runtime_production_profile` lazily for the two constant tables
and never constructs `rquant.config.Settings`, so it runs in a worktree without `.env`
(acceptance A22).
"""

from __future__ import annotations

import argparse
import os
import re
import secrets
import shutil
import stat
import subprocess
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from rquant import runtime_authority as authority
from rquant.runtime_authority import (
    PRODUCTION_ROLE_POLICY,
    RuntimeAncestorPolicy,
    RuntimeAuthorityError,
    RuntimeFilePolicy,
)
from rquant.runtime_authority_publish import (
    DIRECTORY_MODE,
    EXECUTABLE_MODE,
    FILE_MODE,
    GENERATION_APP_SOURCE,
    GENERATION_CWD,
    GENERATION_MANIFESTS,
    GENERATION_NAME,
    GENERATION_PYTHON,
    GENERATION_PYVENV,
    GENERATION_SITE_PACKAGES,
    PLAN_NAME,
    PLAN_SCHEMA_ID,
    PLAN_SCHEMA_VERSION,
    PROFILE_NAME,
    RECORD_NAME,
    ClosurePolicy,
    DirectoryLinkConvention,
    GenerationLayout,
    RuntimeAuthorityStageError,
    StagedFile,
    _write_all,
    candidate_record,
    detect_directory_link_convention,
    full_manifest_bytes,
    generation_slot,
    materialize_layout,
    plan_bytes,
    predict_manifest_entries,
    profile_document,
    read_previous_record,
    scan_frozen_tree,
    sha256_bytes,
    sha256_file,
    staged_files_for,
)
from rquant.runtime_exec_wrapper import _verify
from rquant.strict_json import StrictJsonError, canonical_json_bytes, strict_json_loads

#: U-11 (coordinator ruling): the two recovery roles share one frozen service id, hence one
#: label and one placeholder manifest. The repository has no other producer for it.
RECOVERY_SERVICE_ID = "recovery.primary.v1"
#: U-10: `page_control`'s label is an orphan literal of its unit file; the service id is a
#: placeholder name for the manifest the service never opens (S1 §9.3, C-12).
PAGE_CONTROL_SERVICE_ID = "page-control.orphan.v1"
PAGE_CONTROL_UNIT = Path("deploy/systemd/rquant-page-control.service")
#: What the generation mirrors out of the checkout (E-1 layout).
CHECKOUT_SOURCE_PATHS = ("src/rquant", "scripts/strict_json.py")
CHECKOUT_CONSUMED_PATHS = (*CHECKOUT_SOURCE_PATHS, "deploy/systemd")
#: S1 §7 B-5a: the stdlib subtrees a profile does not enumerate.
STDLIB_EXCLUDED_DIRECTORIES = frozenset(
    {"__pycache__", "test", "idlelib", "tkinter", "lib2to3", "ensurepip", "site-packages"}
)
_BYTECODE_SUFFIXES = (".pyc", ".pyo")
_FORBIDDEN_BASENAMES = frozenset({"sitecustomize.py", "usercustomize.py"})
_INSTANCE_LITERAL = re.compile(r"svc-[0-9a-f]{64}")
_COMMIT = re.compile(r"[0-9a-f]{40}")
_LEGACY_GENERATION = re.compile(r"[0-9a-f]{64}")
_INTERP = re.compile(r"\[Requesting program interpreter: ([^\]]+)\]")
_INTERPRETER_FACTS = (
    "import json, sys, sysconfig; paths = sysconfig.get_paths(); "
    "print(json.dumps({'version': '%d.%d.%d' % sys.version_info[:3], "
    "'stdlib': paths['stdlib'], 'platstdlib': paths['platstdlib']}))"
)


def _log(message: str) -> None:
    sys.stderr.write(f"{message}\n")
    sys.stderr.flush()


# ---------------------------------------------------------------------------------------
# Interpreter closure (S1 §1.2D, B-5a)
# ---------------------------------------------------------------------------------------


@dataclass(frozen=True)
class InterpreterClosure:
    version: str
    elf_loader: RuntimeFilePolicy
    stdlib: tuple[RuntimeFilePolicy, ...]
    shared_libraries: tuple[RuntimeFilePolicy, ...]


def elf_loader_from_readelf(text: str) -> str:
    """The `PT_INTERP` path out of `readelf -l`: the dynamic loader, and only it."""

    matches = sorted(set(_INTERP.findall(text)))
    if len(matches) != 1:
        raise RuntimeAuthorityStageError(
            f"readelf reports {len(matches)} program interpreters; expected exactly one"
        )
    loader = matches[0].strip()
    if not loader.startswith("/"):
        raise RuntimeAuthorityStageError(f"program interpreter is not absolute: {loader!r}")
    return loader


def resolved_closure_member(path: str) -> str:
    """The real path of a closure member, which is the only path this chain can open (G-2).

    A closure member is opened with `O_NOFOLLOW` on both sides — here and again in the
    wrapper, against the path the profile declares — and `O_NOFOLLOW` constrains the last
    segment. `ldd` prints the name it was asked for, and on RHEL 9 that name is routinely a
    versioned symlink (`libcrypt.so.1 -> libcrypt.so.1.1.0`), so declaring it would make the
    open fail with `ELOOP` for a file that is perfectly legitimate. Resolving here keeps
    `O_NOFOLLOW` doing its one job — refusing a member that was swapped for a symlink after
    the profile was written — while the profile names the file the digest was taken from.
    """

    return os.path.realpath(path)


def shared_libraries_from_ldd(text: str, *, elf_loader: str) -> tuple[str, ...]:
    """The `name => path` lines of `ldd`, resolved, minus the loader (B-5a / M-4 dedup rule).

    `ldd` prints the dynamic loader as its own line without `=>` on most systems and as
    `loader => loader` on some; either way it is `elf_loader` and must not be repeated in
    `shared_libraries`, or `RuntimeClosureProfile` rejects the closure for duplicate paths.
    The comparison is between resolved paths, because the loader is one of the members that
    reaches this closure under a versioned symlink.
    `linux-vdso` has no file and is dropped with every other `=>` line without a path.
    """

    loader = resolved_closure_member(elf_loader)
    paths: set[str] = set()
    for line in text.splitlines():
        if " => " not in line:
            continue
        _name, _separator, remainder = line.partition(" => ")
        candidate = remainder.strip().split(" ")[0]
        if not candidate.startswith("/"):
            continue
        resolved = resolved_closure_member(candidate)
        if resolved == loader:
            continue
        paths.add(resolved)
    return tuple(sorted(paths))


def file_policy(
    path: Path,
    *,
    declared_path: Path | None = None,
    mode: int | None = None,
) -> RuntimeFilePolicy:
    """A closure file's declared policy: real digest, root owner, 0555 or 0444 by exec bit."""

    digest, _size = sha256_file(path)
    if mode is None:
        info = path.lstat()
        mode = EXECUTABLE_MODE if info.st_mode & stat.S_IXUSR else FILE_MODE
    return RuntimeFilePolicy(
        path=path if declared_path is None else declared_path,
        sha256=digest,
        owner_uid=0,
        mode=mode,
    )


def _run(command: Sequence[str], *, label: str) -> str:
    try:
        completed = subprocess.run(
            list(command),
            capture_output=True,
            text=True,
            check=False,
            env={"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin"},
        )
    except OSError as exc:
        raise RuntimeAuthorityStageError(f"{label} could not be executed: {exc}") from exc
    if completed.returncode != 0:
        raise RuntimeAuthorityStageError(
            f"{label} failed with exit {completed.returncode}: {completed.stderr.strip()}"
        )
    return completed.stdout


def interpreter_facts(system_python: Path) -> dict[str, str]:
    output = _run(
        [str(system_python), "-I", "-S", "-c", _INTERPRETER_FACTS],
        label="system interpreter probe",
    )
    try:
        facts = strict_json_loads(output)
    except StrictJsonError as exc:
        raise RuntimeAuthorityStageError(
            f"system interpreter probe output is invalid: {exc}"
        ) from exc
    if type(facts) is not dict or set(facts) != {"version", "stdlib", "platstdlib"}:
        raise RuntimeAuthorityStageError("system interpreter probe output has unexpected fields")
    return {key: str(value) for key, value in facts.items()}


def stdlib_files(directories: Iterable[Path]) -> tuple[Path, ...]:
    """Regular files of the standard library subtrees, minus the B-5a exclusions."""

    found: set[Path] = set()
    for directory in directories:
        if not directory.is_dir():
            raise RuntimeAuthorityStageError(f"standard library directory is missing: {directory}")
        for current, subdirectories, files in os.walk(directory, followlinks=False):
            subdirectories[:] = sorted(
                name for name in subdirectories if name not in STDLIB_EXCLUDED_DIRECTORIES
            )
            for name in files:
                if name.endswith(_BYTECODE_SUFFIXES):
                    continue
                path = Path(current) / name
                if stat.S_ISREG(path.lstat().st_mode):
                    found.add(path)
    return tuple(sorted(found))


def discover_interpreter_closure(system_python: Path) -> InterpreterClosure:
    """`readelf` + `ldd` + a stdlib walk of the real system interpreter (Linux only)."""

    facts = interpreter_facts(system_python)
    readelf = shutil.which("readelf")
    ldd = shutil.which("ldd")
    if readelf is None or ldd is None:
        raise RuntimeAuthorityStageError("readelf and ldd are required to discover the closure")
    loader = resolved_closure_member(
        elf_loader_from_readelf(_run([readelf, "-l", str(system_python)], label="readelf"))
    )
    libraries = shared_libraries_from_ldd(
        _run([ldd, str(system_python)], label="ldd"), elf_loader=loader
    )
    stdlib_roots = {Path(facts["stdlib"]), Path(facts["platstdlib"])}
    return InterpreterClosure(
        version=facts["version"],
        elf_loader=file_policy(Path(loader), mode=EXECUTABLE_MODE),
        stdlib=tuple(file_policy(path) for path in stdlib_files(sorted(stdlib_roots))),
        shared_libraries=tuple(file_policy(Path(path)) for path in libraries),
    )


def ancestor_policies(paths: Iterable[Path]) -> tuple[RuntimeAncestorPolicy, ...]:
    """Every parent directory of every closure path, with its observed owner and mode."""

    parents = sorted({parent for path in paths for parent in Path(path).parents}, key=str)
    policies: list[RuntimeAncestorPolicy] = []
    for parent in parents:
        info = os.stat(parent)
        policies.append(
            RuntimeAncestorPolicy(
                path=parent, owner_uid=info.st_uid, mode=stat.S_IMODE(info.st_mode)
            )
        )
    return tuple(policies)


def pyvenv_config(version: str) -> bytes:
    """The three lines of S1 §1.4 (U-1-R), one space on each side of every `=`."""

    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise RuntimeAuthorityStageError(f"interpreter version is malformed: {version!r}")
    home = authority.PRODUCTION_SYSTEM_PYTHON.parent
    return f"home = {home}\ninclude-system-site-packages = false\nversion = {version}\n".encode()


# ---------------------------------------------------------------------------------------
# Instance labels and service manifests (S1 §9.3)
# ---------------------------------------------------------------------------------------


def instance_label(service_id: str) -> str:
    """`runtime_deployment_bundle._instance_name`, restated here so the stage does not import
    that module; the suite pins the two against each other."""

    return "svc-" + sha256_bytes(service_id.encode("utf-8"))


def read_page_control_instance(unit: Path) -> str:
    """The one `svc-<64 hex>` literal of `rquant-page-control.service` (U-10, A19)."""

    try:
        text = unit.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeAuthorityStageError(f"page-control unit is unreadable: {exc}") from exc
    literals = sorted(set(_INSTANCE_LITERAL.findall(text)))
    if len(literals) != 1:
        raise RuntimeAuthorityStageError(
            f"{unit} carries {len(literals)} instance literals; exactly one is required"
        )
    return literals[0]


@dataclass(frozen=True)
class ServiceSet:
    instances: Mapping[str, tuple[str, ...]]
    service_ids: Mapping[str, str]
    manifests: Mapping[str, bytes]


def _kind_backed_manifest(role: str, service_id: str, commit: str) -> bytes:
    return canonical_json_bytes(
        {
            "schema_version": 2,
            "service_id": service_id,
            "service_kind": role,
            "plane": "live",
            "interval_seconds": 60.0,
            "stale_after_seconds": 900.0,
            "producer_commit": commit,
            "settings": {},
        },
        trailing_newline=True,
    )


def _placeholder_manifest(service_id: str, commit: str) -> bytes:
    return canonical_json_bytes(
        {"service_id": service_id, "producer_commit": commit}, trailing_newline=True
    )


def _orphan_services(
    page_control_unit: Path,
    commit: str,
    instances: dict[str, list[str]],
    service_ids: dict[str, str],
    manifests: dict[str, bytes],
) -> None:
    page_label = read_page_control_instance(page_control_unit)
    instances.setdefault("page_control", []).append(page_label)
    service_ids[page_label] = PAGE_CONTROL_SERVICE_ID
    manifests[page_label] = _placeholder_manifest(PAGE_CONTROL_SERVICE_ID, commit)
    recovery_label = instance_label(RECOVERY_SERVICE_ID)
    for role in ("runtime_recovery", "runtime_recovery_rehearsal"):
        instances.setdefault(role, []).append(recovery_label)
    service_ids[recovery_label] = RECOVERY_SERVICE_ID
    manifests[recovery_label] = _placeholder_manifest(RECOVERY_SERVICE_ID, commit)


def _finish_services(
    instances: dict[str, list[str]],
    service_ids: dict[str, str],
    manifests: dict[str, bytes],
) -> ServiceSet:
    policy = {entry.name: entry for entry in PRODUCTION_ROLE_POLICY}
    for role, labels in instances.items():
        if role not in policy or not policy[role].instanced:
            raise RuntimeAuthorityStageError(f"labels derived for a non-instanced role: {role}")
        if len(set(labels)) != len(labels):
            raise RuntimeAuthorityStageError(f"role {role} received a duplicate label")
    for entry in PRODUCTION_ROLE_POLICY:
        if entry.instanced and not instances.get(entry.name):
            raise RuntimeAuthorityStageError(f"instanced role {entry.name} has no label")
    return ServiceSet(
        instances={role: tuple(sorted(labels)) for role, labels in sorted(instances.items())},
        service_ids=dict(sorted(service_ids.items())),
        manifests=dict(sorted(manifests.items())),
    )


def derive_bootstrap_services(*, page_control_unit: Path, commit: str) -> ServiceSet:
    """Route B: labels and manifests from the checkout's frozen constants (S1 §9.3)."""

    from rquant.runtime_production_profile import (
        _REQUIRED_STRATEGIES,
        _SINGLETON_SERVICE_IDS,
    )

    policy = {entry.name: entry for entry in PRODUCTION_ROLE_POLICY}
    instances: dict[str, list[str]] = {}
    service_ids: dict[str, str] = {}
    manifests: dict[str, bytes] = {}

    def add(role: str, service_id: str) -> None:
        entry = policy.get(role)
        if entry is None or entry.service_kind != role or not entry.instanced:
            raise RuntimeAuthorityStageError(f"service id {service_id} names no kind-backed role")
        label = instance_label(service_id)
        instances.setdefault(role, []).append(label)
        service_ids[label] = service_id
        manifests[label] = _kind_backed_manifest(role, service_id, commit)

    for kind, service_id in _SINGLETON_SERVICE_IDS.items():
        add(kind.value, service_id)
    for strategy in sorted(_REQUIRED_STRATEGIES):
        add("candidate_publisher", f"candidate.{strategy}.v1")
        add("strategy_live", f"strategy.{strategy}.v1")
    _orphan_services(page_control_unit, commit, instances, service_ids, manifests)
    return _finish_services(instances, service_ids, manifests)


def legacy_generation_directory(legacy_root: Path, generation: str) -> Path:
    if generation == "current":
        link = legacy_root / "current"
        try:
            info = link.lstat()
        except OSError as exc:
            raise RuntimeAuthorityStageError(f"legacy current pointer is missing: {exc}") from exc
        if not stat.S_ISLNK(info.st_mode):
            raise RuntimeAuthorityStageError("legacy current pointer is not a symlink")
        target = Path(os.readlink(link))
        if (
            target.is_absolute()
            or len(target.parts) != 2
            or target.parts[0] != "generations"
            or _LEGACY_GENERATION.fullmatch(target.parts[1]) is None
        ):
            raise RuntimeAuthorityStageError(f"legacy current pointer is malformed: {target}")
        return legacy_root / target
    if _LEGACY_GENERATION.fullmatch(generation) is None:
        raise RuntimeAuthorityStageError(f"legacy generation is not a 64-hex id: {generation!r}")
    return legacy_root / "generations" / generation


def legacy_services(
    *,
    legacy_root: Path,
    generation: str,
    page_control_unit: Path,
    commit: str,
) -> ServiceSet:
    """Route A: copy the service manifests of a legacy `data/runtime` generation verbatim."""

    directory = legacy_generation_directory(legacy_root, generation) / "manifests"
    if not directory.is_dir():
        raise RuntimeAuthorityStageError(f"legacy manifests directory is missing: {directory}")
    policy = {entry.name: entry for entry in PRODUCTION_ROLE_POLICY}
    instances: dict[str, list[str]] = {}
    service_ids: dict[str, str] = {}
    manifests: dict[str, bytes] = {}
    for path in sorted(directory.iterdir()):
        if path.suffix != ".json" or _INSTANCE_LITERAL.fullmatch(path.stem) is None:
            raise RuntimeAuthorityStageError(f"legacy manifests directory holds a stranger: {path}")
        if not stat.S_ISREG(path.lstat().st_mode):
            raise RuntimeAuthorityStageError(f"legacy manifest is not a regular file: {path}")
        payload = path.read_bytes()
        try:
            document = strict_json_loads(payload)
        except StrictJsonError as exc:
            raise RuntimeAuthorityStageError(f"legacy manifest {path} is not JSON: {exc}") from exc
        if type(document) is not dict:
            raise RuntimeAuthorityStageError(f"legacy manifest {path} is not an object")
        role = document.get("service_kind")
        service_id = document.get("service_id")
        entry = policy.get(role) if type(role) is str else None
        if entry is None or entry.service_kind != role or type(service_id) is not str:
            raise RuntimeAuthorityStageError(f"legacy manifest {path} names no kind-backed role")
        instances.setdefault(role, []).append(path.stem)
        service_ids[path.stem] = service_id
        manifests[path.stem] = payload
    _orphan_services(page_control_unit, commit, instances, service_ids, manifests)
    return _finish_services(instances, service_ids, manifests)


# ---------------------------------------------------------------------------------------
# Checkout and venv enumeration
# ---------------------------------------------------------------------------------------


def _git(checkout_root: Path, *arguments: str) -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", str(checkout_root), *arguments],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise RuntimeAuthorityStageError(f"git could not be executed: {exc}") from exc
    if completed.returncode != 0:
        raise RuntimeAuthorityStageError(
            f"git {' '.join(arguments)} failed: {completed.stderr.strip()}"
        )
    return completed.stdout


def _under_consumed_paths(relative: str) -> bool:
    return any(
        relative == consumed or relative.startswith(consumed + "/")
        for consumed in CHECKOUT_CONSUMED_PATHS
    )


def checkout_sources(checkout_root: Path, commit: str) -> dict[str, Path]:
    """The tracked files the generation mirrors, from a clean checkout at `commit`.

    Enumeration goes through `git ls-files` rather than the directory, so an untracked file
    cannot ride into the generation; the working tree must carry no tracked change anywhere
    and no untracked entry under the consumed paths, so what is copied is the commit's tree.
    """

    if _COMMIT.fullmatch(commit) is None:
        raise RuntimeAuthorityStageError(f"--commit is not a 40-hex commit sha: {commit!r}")
    checkout_root = Path(os.path.abspath(checkout_root))
    head = _git(checkout_root, "rev-parse", "HEAD").strip()
    if head != commit:
        raise RuntimeAuthorityStageError(f"checkout HEAD {head} is not --commit {commit}")
    for line in _git(checkout_root, "status", "--porcelain", "--untracked-files=all").splitlines():
        if not line:
            continue
        if line.startswith("??") and not _under_consumed_paths(line[3:]):
            continue
        raise RuntimeAuthorityStageError(f"checkout is not clean: {line.strip()!r}")
    listing = _git(checkout_root, "ls-files", "-z", "--", *CHECKOUT_SOURCE_PATHS)
    sources: dict[str, Path] = {}
    for relative in listing.split("\0"):
        if not relative:
            continue
        parts = relative.split("/")
        if "__pycache__" in parts or relative.endswith(_BYTECODE_SUFFIXES):
            continue
        name = parts[-1]
        if name in _FORBIDDEN_BASENAMES or name.endswith(".pth"):
            raise RuntimeAuthorityStageError(f"checkout carries an import hook: {relative}")
        path = checkout_root / relative
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise RuntimeAuthorityStageError(f"checkout source is not a regular file: {relative}")
        sources[relative] = path
    required = {"src/rquant/__init__.py", "scripts/strict_json.py"}
    for entry in PRODUCTION_ROLE_POLICY:
        required.add("src/" + entry.module.replace(".", "/") + ".py")
    missing = sorted(required - set(sources))
    if missing:
        raise RuntimeAuthorityStageError(f"checkout lacks required sources: {', '.join(missing)}")
    return dict(sorted(sources.items()))


def venv_site_packages(venv_source: Path) -> dict[str, Path]:
    """Every regular file of the venv's one `site-packages`, minus bytecode and `.pth`."""

    candidates = sorted((venv_source / "lib").glob("python3.*/site-packages"))
    candidates = [path for path in candidates if path.is_dir() and not path.is_symlink()]
    if len(candidates) != 1:
        raise RuntimeAuthorityStageError(
            f"{venv_source} holds {len(candidates)} site-packages directories; expected one"
        )
    site = candidates[0]
    files: dict[str, Path] = {}
    for current, subdirectories, names in os.walk(site, followlinks=False):
        base = Path(current)
        for name in subdirectories:
            if stat.S_ISLNK((base / name).lstat().st_mode):
                raise RuntimeAuthorityStageError(f"venv holds a symlink: {base / name}")
        subdirectories[:] = sorted(name for name in subdirectories if name != "__pycache__")
        for name in sorted(names):
            path = base / name
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode):
                raise RuntimeAuthorityStageError(f"venv holds a symlink: {path}")
            if not stat.S_ISREG(info.st_mode):
                raise RuntimeAuthorityStageError(f"venv holds a special file: {path}")
            if name.endswith(_BYTECODE_SUFFIXES) or name.endswith(".pth"):
                continue
            if name in _FORBIDDEN_BASENAMES:
                raise RuntimeAuthorityStageError(f"venv holds an import hook: {path}")
            relative = path.relative_to(site).as_posix()
            files[f"{GENERATION_SITE_PACKAGES}/{relative}"] = path
    return dict(sorted(files.items()))


# ---------------------------------------------------------------------------------------
# The stage plan
# ---------------------------------------------------------------------------------------


@dataclass(frozen=True)
class StageOptions:
    checkout_root: Path
    commit: str
    runtime_pyz: Path
    deploy_pyz: Path
    system_python: Path
    venv_source: Path
    staging: Path
    operation_id: str
    bootstrap_from_checkout: bool
    legacy_runtime_root: Path | None = None
    legacy_generation: str = "current"
    page_control_unit: Path = PAGE_CONTROL_UNIT

    @property
    def mode(self) -> str:
        return "bootstrap" if self.bootstrap_from_checkout else "legacy"


@dataclass(frozen=True)
class StagePlan:
    options: StageOptions
    plan: Mapping[str, object]
    plan_payload: bytes
    profile_payload: bytes
    record_payload: bytes
    manifest_payload: bytes
    layout: GenerationLayout
    entries: tuple[Mapping[str, object], ...]
    convention: DirectoryLinkConvention

    @property
    def plan_sha256(self) -> str:
        return sha256_bytes(self.plan_payload)


def _require_regular_file(path: Path, label: str) -> Path:
    try:
        info = path.lstat()
    except OSError as exc:
        raise RuntimeAuthorityStageError(f"{label} is missing: {path}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise RuntimeAuthorityStageError(f"{label} is not a regular file: {path}")
    return path


def collect_services(options: StageOptions) -> ServiceSet:
    unit = options.page_control_unit
    if not unit.is_absolute():
        unit = options.checkout_root / unit
    if options.bootstrap_from_checkout:
        return derive_bootstrap_services(page_control_unit=unit, commit=options.commit)
    if options.legacy_runtime_root is None:
        raise RuntimeAuthorityStageError(
            "either --bootstrap-from-checkout or --legacy-runtime-root is required"
        )
    return legacy_services(
        legacy_root=options.legacy_runtime_root,
        generation=options.legacy_generation,
        page_control_unit=unit,
        commit=options.commit,
    )


def build_stage_plan(options: StageOptions) -> StagePlan:
    """Compute the complete staging in memory; reads everything, writes nothing."""

    try:
        authority._require_operation_id(options.operation_id)
    except RuntimeAuthorityError as exc:
        raise RuntimeAuthorityStageError(str(exc)) from exc
    system_python = Path(os.path.abspath(options.system_python))
    if system_python != authority.PRODUCTION_SYSTEM_PYTHON:
        raise RuntimeAuthorityStageError(
            f"--system-python must be the profile's {authority.PRODUCTION_SYSTEM_PYTHON}"
        )
    _require_regular_file(system_python, "--system-python")
    runtime_pyz = _require_regular_file(Path(os.path.abspath(options.runtime_pyz)), "--runtime-pyz")
    deploy_pyz = _require_regular_file(Path(os.path.abspath(options.deploy_pyz)), "--deploy-pyz")
    if os.path.lexists(options.staging):
        raise RuntimeAuthorityStageError(f"--staging already exists: {options.staging}")

    sources = checkout_sources(options.checkout_root, options.commit)
    services = collect_services(options)
    site_packages = venv_site_packages(Path(os.path.abspath(options.venv_source)))
    _log(
        f"sources {len(sources)} files, site-packages {len(site_packages)} files, "
        f"service manifests {len(services.manifests)}"
    )

    closure = discover_interpreter_closure(system_python)
    system_python_policy = file_policy(
        system_python, declared_path=authority.PRODUCTION_SYSTEM_PYTHON, mode=EXECUTABLE_MODE
    )
    runtime_pyz_policy = file_policy(
        runtime_pyz, declared_path=authority.PRODUCTION_RUNTIME_PYZ, mode=EXECUTABLE_MODE
    )
    deploy_pyz_policy = file_policy(
        deploy_pyz, declared_path=authority.PRODUCTION_DEPLOY_PYZ, mode=EXECUTABLE_MODE
    )
    closure_files = (
        system_python_policy,
        closure.elf_loader,
        *closure.stdlib,
        *closure.shared_libraries,
        deploy_pyz_policy,
        runtime_pyz_policy,
    )
    closure_policy = ClosurePolicy(
        system_python=system_python_policy,
        elf_loader=closure.elf_loader,
        stdlib=closure.stdlib,
        shared_libraries=closure.shared_libraries,
        deploy_pyz=deploy_pyz_policy,
        runtime_pyz=runtime_pyz_policy,
        ancestors=ancestor_policies(item.path for item in closure_files),
    )
    profile_id, profile_payload, profile = profile_document(closure_policy, services.instances)
    _log(
        f"profile {profile_id}: {len(profile.files)} closure files "
        f"(stdlib {len(closure.stdlib)}, shared libraries {len(closure.shared_libraries)}), "
        f"{len(profile.ancestors)} ancestors, {len(profile_payload)} bytes"
    )

    pyvenv = pyvenv_config(closure.version)
    files: dict[str, StagedFile] = {
        GENERATION_PYTHON: StagedFile(EXECUTABLE_MODE, source=system_python),
        GENERATION_PYVENV: StagedFile(FILE_MODE, payload=pyvenv),
    }
    for relative, source in sources.items():
        files[relative] = StagedFile(FILE_MODE, source=source)
    for relative, source in site_packages.items():
        files[relative] = StagedFile(FILE_MODE, source=source)
    for label, payload in services.manifests.items():
        files[f"{GENERATION_MANIFESTS}/{label}.json"] = StagedFile(FILE_MODE, payload=payload)
    layout = GenerationLayout.build(
        files, empty_directories=(GENERATION_CWD, GENERATION_SITE_PACKAGES, GENERATION_MANIFESTS)
    )
    convention = detect_directory_link_convention(options.staging)
    entries = predict_manifest_entries(
        layout, owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID, convention=convention
    )
    manifest_payload = full_manifest_bytes(profile_id, entries)
    generation_id = sha256_bytes(manifest_payload)
    slot = generation_slot(
        generation_id=generation_id, commit=options.commit, profile_id=profile_id
    )
    _validate_generation(layout, entries, manifest_payload, slot, profile, pyvenv)

    previous = read_previous_record(profile)
    record = candidate_record(previous, slot, operation_id=options.operation_id)
    authority._validate_record(record, profile)
    record_payload = authority.canonical_runtime_authority_bytes(record)

    plan: dict[str, object] = {
        "schema_id": PLAN_SCHEMA_ID,
        "schema_version": PLAN_SCHEMA_VERSION,
        "operation_id": options.operation_id,
        "mode": options.mode,
        "producer_commit": options.commit,
        "profile_id": profile_id,
        "generation_id": generation_id,
        "sequence": record.sequence,
        "previous_operation_id": None if previous is None else previous.operation_id,
        "runtime_pyz_sha256": runtime_pyz_policy.sha256,
        "deploy_pyz_sha256": deploy_pyz_policy.sha256,
        "system_python_sha256": system_python_policy.sha256,
        "instance_mapping": {role: list(labels) for role, labels in services.instances.items()},
        "service_manifests": dict(services.service_ids),
        "closure_summary": {
            "interpreter_version": closure.version,
            "elf_loader": str(closure.elf_loader.path),
            "stdlib_files": len(closure.stdlib),
            "shared_libraries": len(closure.shared_libraries),
            "closure_files": len(profile.files),
            "ancestors": len(profile.ancestors),
            "profile_bytes": len(profile_payload),
            "generation_entries": len(entries),
            "generation_bytes": sum(int(entry["size"]) for entry in entries),
            "link_convention": convention,
        },
        "staged_files": staged_files_for(
            entries,
            profile_payload=profile_payload,
            record_payload=record_payload,
            manifest_payload=manifest_payload,
        ),
    }
    return StagePlan(
        options=options,
        plan=plan,
        plan_payload=plan_bytes(plan),
        profile_payload=profile_payload,
        record_payload=record_payload,
        manifest_payload=manifest_payload,
        layout=layout,
        entries=entries,
        convention=convention,
    )


def _validate_generation(
    layout: GenerationLayout,
    entries: Sequence[Mapping[str, object]],
    manifest_payload: bytes,
    slot: authority.RuntimeGenerationSlot,
    profile: authority.RuntimeClosureProfile,
    pyvenv: bytes,
) -> None:
    """Every publish-side check that needs no root-owned tree, run before anything is
    written: manifest schema and budgets, import hooks, pyvenv (both sides' rules), and the
    wrapper's module entry contract for every role module."""

    try:
        authority._validate_generation_manifest(manifest_payload, slot, profile)
    except RuntimeAuthorityError as exc:
        raise RuntimeAuthorityStageError(f"generation manifest would be refused: {exc}") from exc
    import importlib.machinery

    import_roots = (GENERATION_SITE_PACKAGES, GENERATION_APP_SOURCE)
    for entry in entries:
        classification = authority._classify_forbidden_import_path(
            str(entry["path"]),
            entry_type=str(entry["type"]),
            import_roots=import_roots,
            extension_suffixes=tuple(importlib.machinery.EXTENSION_SUFFIXES),
        )
        if classification is not None:
            raise RuntimeAuthorityStageError(
                f"generation would carry an import hook: {entry['path']} ({classification})"
            )
    try:
        authority._validate_pyvenv_config(pyvenv, system_python=profile.system_python.path)
    except RuntimeAuthorityError as exc:
        raise RuntimeAuthorityStageError(f"pyvenv.cfg would be refused: {exc}") from exc
    lines = [line.strip() for line in pyvenv.decode("utf-8").splitlines()]
    if _verify._REQUIRED_PYVENV_LINE not in lines:
        raise RuntimeAuthorityStageError("pyvenv.cfg lacks the wrapper's literal line")
    for entry in PRODUCTION_ROLE_POLICY:
        relative = f"{GENERATION_APP_SOURCE}/{entry.module.replace('.', '/')}.py"
        staged = layout.files.get(relative)
        if staged is None or staged.source is None:
            raise RuntimeAuthorityStageError(f"role module source is not staged: {relative}")
        expects_argv = bool(entry.control_root) or bool(entry.module_arguments)
        try:
            _verify.assert_module_entry_contract(
                staged.source.read_text(encoding="utf-8"), expects_argv=expects_argv
            )
        except _verify.RuntimeExecError as exc:
            raise RuntimeAuthorityStageError(
                f"role {entry.name} module {relative} fails the entry contract: {exc}"
            ) from exc


def apply_stage_plan(stage: StagePlan) -> Path:
    """Materialise under `<staging>.tmp-<op>`, prove it matches the prediction, rename."""

    staging = Path(os.path.abspath(stage.options.staging))
    if os.path.lexists(staging):
        raise RuntimeAuthorityStageError(f"--staging already exists: {staging}")
    staging.parent.mkdir(parents=True, exist_ok=True)
    temporary = staging.with_name(f".{staging.name}.tmp-{stage.options.operation_id}")
    if os.path.lexists(temporary):
        raise RuntimeAuthorityStageError(f"temporary staging already exists: {temporary}")
    temporary.mkdir(mode=0o700)
    try:
        generation = temporary / GENERATION_NAME
        generation.mkdir(mode=0o700)
        materialize_layout(stage.layout, generation)
        observed = scan_frozen_tree(generation, owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID)
        if observed != stage.entries:
            differences = [
                str(entry["path"])
                for entry, expected in zip(observed, stage.entries, strict=False)
                if entry != expected
            ]
            raise RuntimeAuthorityStageError(
                "materialised generation differs from the predicted manifest: "
                f"{', '.join(differences[:5]) or 'entry count'}"
            )
        _write_frozen(generation / authority.GENERATION_MANIFEST_NAME, stage.manifest_payload)
        generation.chmod(DIRECTORY_MODE)
        _write_frozen(temporary / PROFILE_NAME, stage.profile_payload)
        _write_frozen(temporary / RECORD_NAME, stage.record_payload)
        _write_frozen(temporary / PLAN_NAME, stage.plan_payload)
        # Renamed while still writable: Darwin refuses to rename a directory it cannot
        # write, even within one parent. Frozen right after, before anything can read it.
        os.rename(temporary, staging)
    except BaseException:
        _discard(temporary)
        raise
    staging.chmod(DIRECTORY_MODE)
    return staging


def _write_frozen(path: Path, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        _write_all(descriptor, payload)
        os.fchmod(descriptor, FILE_MODE)
    finally:
        os.close(descriptor)


def _discard(root: Path) -> None:
    if not os.path.lexists(root):
        return
    for current, _subdirectories, _files in os.walk(root):
        os.chmod(current, 0o700)
    shutil.rmtree(root, ignore_errors=True)


# ---------------------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rquant runtime-authority-stage",
        description="Stage a runtime authority generation out of a checkout (unprivileged).",
    )
    parser.add_argument("--checkout-root", type=Path, required=True)
    parser.add_argument("--commit", required=True, help="40-hex commit; must equal HEAD")
    parser.add_argument("--runtime-pyz", type=Path, required=True)
    parser.add_argument("--deploy-pyz", type=Path, required=True)
    parser.add_argument("--system-python", type=Path, required=True)
    parser.add_argument("--venv-source", type=Path, required=True)
    parser.add_argument("--staging", type=Path, required=True)
    parser.add_argument("--operation-id", help="32-hex; random when omitted")
    parser.add_argument("--apply", action="store_true", help="write the staging directory")
    parser.add_argument(
        "--bootstrap-from-checkout",
        action="store_true",
        help="derive instance labels and service manifests from the checkout (S1 §9.3)",
    )
    parser.add_argument("--legacy-runtime-root", type=Path)
    parser.add_argument("--legacy-generation", default="current")
    parser.add_argument(
        "--page-control-instance-from-unit",
        type=Path,
        default=PAGE_CONTROL_UNIT,
        help="unit file carrying the page_control instance literal (relative to checkout)",
    )
    return parser


def options_from_arguments(arguments: argparse.Namespace) -> StageOptions:
    if not arguments.bootstrap_from_checkout and arguments.legacy_runtime_root is None:
        raise RuntimeAuthorityStageError(
            "either --bootstrap-from-checkout or --legacy-runtime-root is required"
        )
    return StageOptions(
        checkout_root=Path(os.path.abspath(arguments.checkout_root)),
        commit=arguments.commit,
        runtime_pyz=arguments.runtime_pyz,
        deploy_pyz=arguments.deploy_pyz,
        system_python=arguments.system_python,
        venv_source=arguments.venv_source,
        staging=Path(os.path.abspath(arguments.staging)),
        operation_id=arguments.operation_id or secrets.token_hex(16),
        bootstrap_from_checkout=arguments.bootstrap_from_checkout,
        legacy_runtime_root=arguments.legacy_runtime_root,
        legacy_generation=arguments.legacy_generation,
        page_control_unit=arguments.page_control_instance_from_unit,
    )


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        options = options_from_arguments(arguments)
        stage = build_stage_plan(options)
        if arguments.apply:
            staging = apply_stage_plan(stage)
            _log(f"staged {staging}")
            _log(f"plan.json sha256 {stage.plan_sha256}")
    except RuntimeAuthorityError as error:
        sys.stderr.write(f"refused: {error}\n")
        return 1
    except OSError as error:
        sys.stderr.write(f"failed: {error}\n")
        return 1
    sys.stdout.buffer.write(stage.plan_payload)
    sys.stdout.flush()
    return 0


__all__ = [
    "InterpreterClosure",
    "ServiceSet",
    "StageOptions",
    "StagePlan",
    "ancestor_policies",
    "apply_stage_plan",
    "build_stage_plan",
    "checkout_sources",
    "derive_bootstrap_services",
    "discover_interpreter_closure",
    "elf_loader_from_readelf",
    "resolved_closure_member",
    "instance_label",
    "legacy_services",
    "main",
    "pyvenv_config",
    "read_page_control_instance",
    "shared_libraries_from_ldd",
    "venv_site_packages",
]


if __name__ == "__main__":
    raise SystemExit(main())
