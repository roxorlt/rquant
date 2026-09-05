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
from functools import lru_cache
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
#: The cadence route B gives every derived manifest. Per-role cadence is a property of the
#: published production profile, not of a first installation (#200 is about plane and
#: settings); the health publisher reports the same staleness bound it hands its sources.
MANIFEST_INTERVAL_SECONDS = 60.0
MANIFEST_STALE_AFTER_SECONDS = 900.0
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
    #: The standard library subtrees the walk actually read, in canonical order.
    stdlib_roots: tuple[Path, ...] = ()
    #: Subtrees `sysconfig` named that this host does not have (#198 BLK-1). They hold no
    #: files, so they change nothing about the closure — but the plan states them, because a
    #: silent skip would make the closure's contents unverifiable from the plan alone.
    skipped_stdlib_roots: tuple[Path, ...] = ()


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


def stdlib_directories(facts: Mapping[str, str]) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
    """The stdlib subtrees to walk and the ones this host does not have (#198 BLK-1).

    `stdlib` is the standard library itself; its absence means the probe described an
    interpreter that cannot exist, and it is still refused. `platstdlib` is the
    platform-specific half, and the RHEL family redirects the default scheme's copy into
    `/usr/local/lib64/pythonX.Y` so that `pip install` cannot write into an RPM-owned
    directory — a path the distribution never creates. On 82.156.0.68 (OpenCloudOS 9.2,
    2026-09-05) that absent directory refused the whole staging; Debian maps `platstdlib`
    onto `/usr/lib/pythonX.Y`, which is why the ubuntu CI runners never showed it.

    A directory that does not exist contributes no files, so skipping it leaves the closure
    byte for byte the one the walk would have produced. What it must not do is vanish
    quietly: the skipped roots come back to the caller, reach the staging log, and are
    written into `plan.json`.
    """

    stdlib = Path(facts["stdlib"])
    if not stdlib.is_dir():
        raise RuntimeAuthorityStageError(f"standard library directory is missing: {stdlib}")
    walked = {stdlib}
    skipped: set[Path] = set()
    platstdlib = Path(facts["platstdlib"])
    if platstdlib != stdlib:
        (walked if platstdlib.is_dir() else skipped).add(platstdlib)
    return tuple(sorted(walked)), tuple(sorted(skipped))


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
    walked, skipped = stdlib_directories(facts)
    _log(f"stdlib subtrees {', '.join(str(path) for path in walked)}")
    if skipped:
        _log(
            "stdlib subtrees absent on this host and skipped: "
            f"{', '.join(str(path) for path in skipped)}"
        )
    return InterpreterClosure(
        version=facts["version"],
        elf_loader=file_policy(Path(loader), mode=EXECUTABLE_MODE),
        stdlib=tuple(file_policy(path) for path in stdlib_files(walked)),
        shared_libraries=tuple(file_policy(Path(path)) for path in libraries),
        stdlib_roots=walked,
        skipped_stdlib_roots=skipped,
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


#: The runtime owner root every derived setting addresses. `None` means the frozen
#: `runtime_deployment_profile.LINUX_PRODUCTION_RUNTIME_ROOT`; the suite points this at a
#: temporary directory the same way it points `PRODUCTION_SYSTEM_PYTHON` at a fake
#: interpreter, so a builder can be constructed out of a staged manifest in a test.
PRODUCTION_RUNTIME_ROOT: Path | None = None
#: The v3 cost engine `build_production_runtime_profile` gives the paper broker. It is a
#: frozen policy of this repository, not an installation fact.
PAPER_EXECUTION_COST_SPEC: Mapping[str, object] = {
    "schema_version": 3,
    "cost_engine_version": "rquant-paper-cost-engine-v3",
    "instrument_selectors": [
        {
            "selector_id": "cn-sse-a-share",
            "market": "CN",
            "exchange": "SSE",
            "instrument_class": "EQUITY",
            "security_class": "A_SHARE",
        },
        {
            "selector_id": "cn-szse-a-share",
            "market": "CN",
            "exchange": "SZSE",
            "instrument_class": "EQUITY",
            "security_class": "A_SHARE",
        },
    ],
    "commission_rules": [
        {
            "rule_id": "commission-cn-sse-a-share",
            "selector_id": "cn-sse-a-share",
            "rate_bps": "3",
            "minimum_amount": "5",
            "applies_to": "BOTH",
        },
        {
            "rule_id": "commission-cn-szse-a-share",
            "selector_id": "cn-szse-a-share",
            "rate_bps": "3",
            "minimum_amount": "5",
            "applies_to": "BOTH",
        },
    ],
    "transfer_fee_rules": [
        {
            "rule_id": "transfer-cn-sse-a-share",
            "selector_id": "cn-sse-a-share",
            "rate_bps": "0",
            "minimum_amount": "0",
            "applies_to": "BOTH",
        },
        {
            "rule_id": "transfer-cn-szse-a-share",
            "selector_id": "cn-szse-a-share",
            "rate_bps": "0",
            "minimum_amount": "0",
            "applies_to": "BOTH",
        },
    ],
    "stamp_duty_rules": [
        {
            "rule_id": "stamp-cn-sse-a-share",
            "selector_id": "cn-sse-a-share",
            "rate_bps": "10",
            "minimum_amount": "0",
            "applies_to": "SELL",
        },
        {
            "rule_id": "stamp-cn-szse-a-share",
            "selector_id": "cn-szse-a-share",
            "rate_bps": "10",
            "minimum_amount": "0",
            "applies_to": "SELL",
        },
    ],
    "fee_notional_basis": "EXECUTED_NOTIONAL",
    "assessment_unit": "FILL",
    "slippage": {
        "owner": "shared_cost_engine",
        "buy_bps": "5",
        "sell_bps": "5",
        "price_tick": "0.0001",
        "price_rounding": "HALF_UP",
    },
    "money": {"quantum": "0.01", "rounding": "HALF_UP"},
}


#: The daily receipt trusted keyring, the one file this stage reads outside the checkout and
#: the artifacts it was handed (coordinator ruling, 2026-09-05). `root:root 0444`, produced by
#: `scripts/install-runtime-credential-infra.sh` in B-3 and world-readable, so reading it as
#: the unprivileged staging user is not a privilege gain and adds no writable trust surface.
#: `None` means the frozen
#: `runtime_deployment_profile.PRODUCTION_DAILY_RECEIPT_TRUSTED_KEYRING_PATH` — the same
#: constant `_hydrate_daily_receipt_authority_from_fixed_keyring` reads, not a second literal;
#: the suite points this at a temporary file the way it points the runtime root.
DAILY_RECEIPT_KEYRING_PATH: Path | None = None
_MAX_KEYRING_BYTES = 64 * 1024


def daily_receipt_keyring_path() -> Path:
    """The trusted keyring this stage reads (the module seam above)."""

    if DAILY_RECEIPT_KEYRING_PATH is not None:
        return DAILY_RECEIPT_KEYRING_PATH
    from rquant.runtime_deployment_profile import PRODUCTION_DAILY_RECEIPT_TRUSTED_KEYRING_PATH

    return PRODUCTION_DAILY_RECEIPT_TRUSTED_KEYRING_PATH


def daily_receipt_authority() -> tuple[str, str, dict[str, str]]:
    """The active daily receipt key id, its public key and the retired ones.

    The judgement is stated here rather than inherited, because this is a TCB read: the file
    must exist, be a regular file that is not a symlink and has exactly one link, be owned by
    the authority owner (`PRODUCTION_PROFILE_OWNER_UID`, root in production) and carry no
    group or world write bit. Anything else refuses with the observed facts in the message —
    an absent or unsafe keyring never degrades into an empty authority, because a manifest
    that named no key would start a receipt signer with nothing to verify against.

    The signature over the keyring is not re-checked here. The manifest carries
    `receipt_trusted_keyring_path`, and the daily orchestrator verifies the keyring itself at
    run time; the copy this stage takes is published inside the root transaction and is
    content-addressed with the rest of the generation.
    """

    path = daily_receipt_keyring_path()
    try:
        observed = path.lstat()
    except OSError as exc:
        raise RuntimeAuthorityStageError(
            f"daily receipt trusted keyring is unavailable: {path} ({exc.strerror})"
        ) from exc
    if not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1:
        raise RuntimeAuthorityStageError(
            f"daily receipt trusted keyring is not a single-linked regular file: {path}"
        )
    if observed.st_uid != authority.PRODUCTION_PROFILE_OWNER_UID:
        raise RuntimeAuthorityStageError(
            f"daily receipt trusted keyring owner is unsafe: {path} is owned by uid "
            f"{observed.st_uid}, expected {authority.PRODUCTION_PROFILE_OWNER_UID}"
        )
    mode = stat.S_IMODE(observed.st_mode)
    if mode & 0o022:
        raise RuntimeAuthorityStageError(
            f"daily receipt trusted keyring mode is unsafe: {path} is {mode:04o}, "
            "group and world write are refused"
        )
    if not 0 < observed.st_size <= _MAX_KEYRING_BYTES:
        raise RuntimeAuthorityStageError(
            f"daily receipt trusted keyring size is unsafe: {path} holds {observed.st_size} bytes"
        )
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise RuntimeAuthorityStageError(
            f"daily receipt trusted keyring is unreadable: {path} ({exc.strerror})"
        ) from exc
    try:
        document = strict_json_loads(payload)
    except StrictJsonError as exc:
        raise RuntimeAuthorityStageError(
            f"daily receipt trusted keyring is not canonical JSON: {path} ({exc})"
        ) from exc
    if type(document) is not dict:
        raise RuntimeAuthorityStageError(f"daily receipt trusted keyring is not an object: {path}")
    active_key_id = document.get("active_key_id")
    active_public_key = document.get("active_public_key")
    previous = document.get("previous_public_keys")
    if (
        type(active_key_id) is not str
        or not active_key_id
        or type(active_public_key) is not str
        or not active_public_key
        or type(previous) is not dict
        or any(
            type(key) is not str or not key or type(value) is not str or not value
            for key, value in previous.items()
        )
    ):
        raise RuntimeAuthorityStageError(
            f"daily receipt trusted keyring shape is invalid: {path}"
        )
    return active_key_id, active_public_key, dict(sorted(previous.items()))


def production_runtime_root() -> Path:
    """The runtime owner root the derived settings address (the module seam above)."""

    if PRODUCTION_RUNTIME_ROOT is not None:
        return PRODUCTION_RUNTIME_ROOT
    from rquant.runtime_deployment_profile import LINUX_PRODUCTION_RUNTIME_ROOT

    return LINUX_PRODUCTION_RUNTIME_ROOT


@lru_cache(maxsize=4)
def builtin_strategy_facts(commit: str) -> tuple[tuple[object, ...], Mapping[str, object]]:
    """The three built-in strategies' bindings and static feature schemas, as the checkout
    computes them for itself. `install_production_runtime_prerequisites` refuses a profile
    whose bindings differ from this same plan, so these are derivations, not guesses. The
    cache is keyed by commit because every fingerprint below hashes executable content.
    """

    from rquant.runtime_definition_bootstrap import plan_builtin_definitions
    from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

    strategies = tuple(
        sorted(
            plan_builtin_definitions(producer_commit=commit).strategies,
            key=lambda strategy: strategy.strategy_id,
        )
    )
    evaluators = BuiltinStrategyEvaluatorRegistry(producer_commit=commit)
    schemas = {
        strategy.strategy_id: {
            name: semantic.contract_payload()
            for name, semantic in evaluators.load_definition(
                strategy.strategy_id,
                strategy.strategy_version,
            ).static_feature_schema.items()
        }
        for strategy in strategies
    }
    return strategies, schemas


def bootstrap_settings(commit: str) -> dict[str, dict[str, object]]:
    """The settings of every kind-backed role, by service id (S1 §9.3, #200).

    Route B used to hand every builder an empty settings object, which each of them
    rejected against its own model. What a first installation *can* know is derived here
    with the same expressions `build_production_runtime_profile` uses: paths under the
    runtime owner root, frozen constants of `runtime_deployment_profile`, the operational
    database location `runtime_artifact_terminal_lifecycle` computes from the root, and the
    strategy fingerprints and static feature schemas the checkout computes for itself.

    What a first installation cannot know is left out on purpose: the market calendar
    generation, the sealed candidate documents, the historical minute snapshot, the
    definition registry, the routing policy, the trade calendar, the recovery and retention
    authorities, the artifact location and the signer public keys are all operator facts
    that live outside the runtime owner root (`ProductionRuntimeProfileInputs` refuses an
    immutable input inside it) or under `/etc/rquant`. A placeholder for any of them would
    be a lie the builder cannot detect, so the field is absent and the builder's own model
    names it. The suite pins both halves: what is derived, and what is missing.
    """

    from rquant.runtime_artifact_terminal_lifecycle import (
        artifact_retention_state_root,
        operational_database_path,
        operational_readonly_database_path,
    )
    from rquant.runtime_builder_daily_orchestrator import build_daily_shadow_stage_commands
    from rquant.runtime_deployment_bundle import strategy_live_producer_version
    from rquant.runtime_deployment_profile import (
        PRODUCTION_DAILY_RECEIPT_TRUSTED_KEYRING_PATH,
        PRODUCTION_DAILY_SIGNER_SOCKET_ENDPOINT,
        PRODUCTION_SHADOW_INSTANCE_ID,
        PRODUCTION_SHADOW_SERVICE_ID,
        PRODUCTION_SHADOW_SIGNER_COMMAND,
    )
    from rquant.runtime_production_profile import _control_bucket
    from rquant.runtime_service_entrypoint import RuntimeServiceKind

    root = production_runtime_root()
    (
        receipt_active_key_id,
        receipt_active_public_key,
        receipt_previous_public_keys,
    ) = daily_receipt_authority()
    database = operational_database_path(root)
    research_metadata = operational_readonly_database_path(root)
    reference_registry = root / "authorities" / "reference-slow" / "reference.sqlite3"
    reference_spool = root / "live" / "reference-slow"
    minute_root = root / "live" / "market-minute"
    quote_root = root / "live" / "watchlist-quote"
    auction_root = root / "live" / "auction-match"
    auction_universe_root = root / "authorities" / "auction-universe"
    signal_root = root / "live" / "signal-bus"
    signal_spool = signal_root / "spool"
    feature_root = root / "live" / "features"
    legacy_shadow_root = root.parent / "legacy-shadow"
    research_root = root / "research"
    final_artifacts = research_root / "final-artifacts"
    lab_jobs_path = research_root / "lab_jobs.sqlite3"
    experiment_registry = research_root / "experiment_registry.sqlite3"
    health_authority = root / "control" / "authority-runtime-health"
    reference_cursor_root = (
        root
        / "control"
        / "reference-slow-publishers"
        / instance_label("reference-slow.publisher.v1")
        / "cursors"
    )
    broker_root = root / "live" / "paper-brokers" / instance_label("paper-broker.shadow-main.v1")
    notifier_root = root / "live" / "notifications" / instance_label("notifier.admin.shadow.v1")
    catalog_root = (
        research_root / "artifact-catalogs" / instance_label("artifact-catalog.primary.v1")
    )
    retention_state_root = artifact_retention_state_root(root)

    strategies, schemas = builtin_strategy_facts(commit)

    def candidate_service_id(strategy_id: str) -> str:
        return f"candidate.{strategy_id}.v1"

    def strategy_service_id(strategy_id: str) -> str:
        return f"strategy.{strategy_id}.v1"

    def candidate_root(strategy_id: str) -> Path:
        return root / "live" / "candidates" / instance_label(candidate_service_id(strategy_id))

    def runner_state_path(strategy_id: str) -> Path:
        return (
            root
            / "live"
            / "strategies"
            / instance_label(strategy_service_id(strategy_id))
            / "runner.sqlite3"
        )

    candidate_authorities = [
        {
            "strategy_id": strategy.strategy_id,
            "strategy_version": str(strategy.strategy_version),
            "snapshot_root": str(candidate_root(strategy.strategy_id)),
            "required": True,
            "max_age_seconds": 7 * 24 * 60 * 60,
            "definition_fingerprint": strategy.registration_fingerprint,
            "executable_fingerprint": strategy.executable_fingerprint,
            "candidate_schema_fingerprint": strategy.candidate_schema_fingerprint,
            "static_feature_names": sorted(schemas[strategy.strategy_id]),
            "static_feature_schema": schemas[strategy.strategy_id],
        }
        for strategy in strategies
    ]

    settings: dict[str, dict[str, object]] = {
        "reference-slow.source.v1": {
            "database_path": str(database),
            "spool_root": str(reference_spool),
            "quota_path": str(reference_spool / "quota.sqlite3"),
            "quota_units_per_window": 500,
            "quota_accounting_mode": "transport",
            "quota_cost_per_capture": None,
            "retry_ordinal": 0,
            "pending_recovery_min_age_seconds": 60,
            "revision_lookback_sessions": 5,
            "history_page_size": 64,
            "limits": {
                "snapshot_max_bytes": 8 * 1024**3,
                "snapshot_min_free_bytes": 2 * 1024**3,
                "snapshot_copy_timeout_seconds": 45.0,
                "query_chunk_rows": 512,
                "max_response_rows": 10_000,
                "max_response_bytes": 8 * 1024**2,
            },
            "consumer_cursor_root": str(reference_cursor_root),
            "retention_consumer_id": "reference-slow-publisher",
            "retention_hot_batches": 128,
            "retention_page_size": 32,
            "producer_version": "reference-slow-source-v1",
        },
        "reference-slow.publisher.v1": {
            "spool_root": str(reference_spool),
            "registry_path": str(reference_registry),
            "cursor_root": str(reference_cursor_root),
            "consumer_id": "reference-slow-publisher",
            "page_size": 16,
        },
        "auction-universe.publisher.v1": {
            "database_path": str(database),
            "authority_root": str(auction_universe_root),
        },
        "auction-match.source.v1": {
            "spool_root": str(auction_root),
            "quota_path": str(auction_root / "quota.sqlite3"),
            "quota_units_per_window": 500,
            "quota_cost_per_request": 1,
            "producer_version": "auction-match-source-v1",
            "universe_path": str(auction_universe_root / "current.json"),
            "max_attempts": 3,
        },
        "market-minute.source.v1": {
            "spool_root": str(minute_root),
            "quota_path": str(minute_root / "quota.sqlite3"),
            "quota_units_per_window": 500,
            "quota_cost_per_request": 20,
            "pending_recovery_min_age_seconds": 60,
            "max_codes_per_source_call": 300,
            "producer_version": "market-minute-source-v1",
            "candidate_authorities": candidate_authorities,
        },
        "watchlist-quote.source.v1": {
            "spool_root": str(quote_root),
            "quota_path": str(quote_root / "quota.sqlite3"),
            "quota_units_per_window": 12,
            "quota_cost_per_request": 1,
            "producer_version": "watchlist-quote-source-v1",
            "schema_version": 2,
            "rollout_mode": "candidate",
            "minimum_cadence_seconds": 5.0,
            "request_timeout_seconds": 2.5,
            "failure_threshold": 3,
            "circuit_cooldown_seconds": 30.0,
            "max_backoff_seconds": 60.0,
            "candidate_authorities": candidate_authorities,
        },
        "daily-close.source.v1": {
            "spool_root": str(root / "live" / "daily-close"),
            "quota_path": str(root / "live" / "daily-close" / "quota.sqlite3"),
            "quota_units_per_window": 20,
            "quota_accounting_mode": "transport",
            "quota_cost_per_request": None,
            "pending_recovery_min_age_seconds": 300,
            "producer_version": "daily-close-source-v1",
        },
        "daily.pipeline.orchestrator.shadow.v1": {
            "storage_root": str(research_root / "daily-pipeline"),
            "source_spool_root": str(root / "live" / "daily-close"),
            "deployment_profile_path": str(root / "current" / "deployment-profile.json"),
            "mode": "shadow",
            "service_owner": "daily.pipeline.orchestrator.shadow.v1",
            "stages": [
                "raw_capture",
                "validate_candidate",
                "canonical_publish",
                "screen",
                "pool",
                "summary",
                "serving_refresh",
                "replica_sync",
                "research_ingest",
                "backup",
            ],
            "stage_commands": [
                command.model_dump(mode="json")
                for command in build_daily_shadow_stage_commands(
                    python_executable=Path("/home/lighthouse/rquant/.venv/bin/python"),
                    working_directory=Path("/home/lighthouse/rquant"),
                )
            ],
            "receipt_active_key_id": receipt_active_key_id,
            "receipt_active_public_key_pem": receipt_active_public_key,
            "receipt_previous_public_key_pems": receipt_previous_public_keys,
            "receipt_signer_socket_endpoint": str(PRODUCTION_DAILY_SIGNER_SOCKET_ENDPOINT),
            "receipt_trusted_keyring_path": str(PRODUCTION_DAILY_RECEIPT_TRUSTED_KEYRING_PATH),
            "receipt_signer_timeout_seconds": 5.0,
            "receipt_signer_test_mode": False,
        },
        PRODUCTION_SHADOW_SERVICE_ID: {
            "report_root": str(research_root / "shadow-reports"),
            "legacy_monitor_root": str(legacy_shadow_root / "monitor"),
            "legacy_surge_root": str(legacy_shadow_root / "surge"),
            "isolated_runner_root": str(legacy_shadow_root / "isolated-runners"),
            "signer_command": list(PRODUCTION_SHADOW_SIGNER_COMMAND),
            "report_producer_service_id": PRODUCTION_SHADOW_SERVICE_ID,
            "report_producer_instance_id": PRODUCTION_SHADOW_INSTANCE_ID,
            "signer_timeout_seconds": 5.0,
            "producer_version": "shadow-session-production-v1",
            "match_tolerance_microseconds": 60_000_000,
            "mode": "shadow",
            "strategy_bindings": [
                {
                    "strategy_id": strategy.strategy_id,
                    "strategy_version": strategy.strategy_version,
                    "definition_fingerprint": strategy.registration_fingerprint,
                    "executable_fingerprint": strategy.executable_fingerprint,
                }
                for strategy in strategies
                if strategy.strategy_id in {"n_shape", "growth_board_surge"}
            ],
        },
        "feature.intraday-pit.v1": {
            "raw_spool_root": str(minute_root),
            "feature_spool_root": str(feature_root),
            "limit": 128,
            "consumer_id": "feature-live",
            "feature_config": {
                "lookback_sessions": 20,
                "opening_acceleration_block_minutes": 3,
                "bar_timestamp_semantics": "bar_end",
                "contract_id": "intraday-pit",
                "contract_version": 3,
                "schema_version": 2,
            },
        },
        "signal-router.all-strategies.v1": {
            "signal_bus_path": str(signal_root / "signal_bus.sqlite3"),
            "signal_spool_root": str(signal_spool),
            "sources": [
                {
                    "source_id": strategy_service_id(strategy.strategy_id),
                    "runner_state_path": str(runner_state_path(strategy.strategy_id)),
                    "expected_strategy_registration_fingerprint": (
                        strategy.registration_fingerprint
                    ),
                    "expected_strategy_spec_fingerprint": strategy.strategy_spec_fingerprint,
                    "expected_evaluator_contract_fingerprint": strategy.executable_fingerprint,
                }
                for strategy in strategies
            ],
            "batch_limit": 256,
            "paused": False,
        },
        "notifier.admin.shadow.v1": {
            "signal_spool_root": str(signal_spool),
            "notification_state_path": str(notifier_root / "notification_state.sqlite3"),
            "worker_id": "notifier-admin-shadow",
            "batch_limit": 128,
            "lease_seconds": 30,
            "serving_authority_root": str(notifier_root / "serving-authority"),
            "page_projection_database_path": str(database),
            "page_projection_surge_live_root": str(database.parent / "surge_live"),
            "paused": True,
        },
        "paper-constraint.market.v1": {
            "minute_spool_root": str(minute_root),
            "reference_registry_path": str(reference_registry),
            "authority_root": str(root / "authorities" / "paper-execution"),
            "quote_ttl_seconds": 120,
        },
        "paper-broker.shadow-main.v1": {
            "account_id": "shadow-main",
            "execution_lag_seconds": 60,
            "buy_quantity": 100,
            "reduce_quantity": 100,
            "sell_quantity": 100,
            "signal_spool_root": str(signal_spool),
            "queue_path": str(broker_root / "queue.sqlite3"),
            "consumer_state_path": str(broker_root / "consumer.sqlite3"),
            "broker_path": str(broker_root / "broker.sqlite3"),
            "initial_cash": "100000",
            "execution_cost_spec": dict(PAPER_EXECUTION_COST_SPEC),
            "limit": 128,
            "serving_authority_root": str(broker_root / "serving-authority"),
            "paused": False,
        },
        "lab-jobs.serving.v1": {
            "lab_jobs_path": str(lab_jobs_path),
            "research_metadata_path": str(research_metadata),
            "authority_root": str(research_root / "serving-authorities" / "lab-jobs"),
        },
        "artifact-catalog.primary.v1": {
            "research_root": str(research_root),
            "artifact_root": str(final_artifacts),
            "state_root": str(catalog_root),
            "lab_jobs_path": str(lab_jobs_path),
            "dataset_authority_path": str(research_metadata),
            "experiment_registry_path": str(experiment_registry),
        },
        "artifact-retention.primary.v1": {
            "managed_root": str(final_artifacts),
            "state_root": str(retention_state_root),
            "reference_store_path": str(retention_state_root / "references.sqlite3"),
            "catalog_authority_root": str(retention_state_root / "catalog-authority"),
            "max_recovery_age": "P30D",
            "max_bundle_items": 128,
            "max_bundle_bytes": 8589934592,
            "retention_policy": {
                "hot_min_age": "P7D",
                "warm_min_age": "P30D",
                "cold_min_age": "P90D",
                "minimum_verified_copies": 1,
                "verification_max_age": "P1D",
                "plan_ttl": "PT1H",
                "claim_ttl": "PT10M",
                "rules": [],
            },
            "worker": {
                "batch_items": 16,
                "batch_bytes": 1073741824,
                "max_runtime": "PT60S",
                "lease_ttl": "PT5M",
                "max_attempts": 3,
                "retry_delay": "PT1M",
            },
        },
        "promotions.serving.v1": {
            "experiment_registry_path": str(experiment_registry),
            "experiment_registry_managed_trust_root": str(research_root),
            "authority_root": str(research_root / "serving-authorities" / "promotions"),
        },
        "serving.publisher.v1": {
            "serving_root": str(root / "serving"),
            "schema_version": 3,
            "source_authorities": [
                {"dataset_id": "signals", "root": str(notifier_root / "serving-authority")},
                {"dataset_id": "paper_accounts", "root": str(broker_root / "serving-authority")},
                {"dataset_id": "runtime_health", "root": str(health_authority)},
                {
                    "dataset_id": "lab_jobs",
                    "root": str(research_root / "serving-authorities" / "lab-jobs"),
                },
                {
                    "dataset_id": "promotions",
                    "root": str(research_root / "serving-authorities" / "promotions"),
                },
                {
                    "dataset_id": "reference_slow_authority",
                    "root": str(reference_spool / "serving-authority"),
                },
            ],
        },
    }
    for strategy in strategies:
        settings[candidate_service_id(strategy.strategy_id)] = {
            "strategy_id": strategy.strategy_id,
            "strategy_version": strategy.strategy_version,
            "definition_fingerprint": strategy.registration_fingerprint,
            "executable_fingerprint": strategy.executable_fingerprint,
            "candidate_schema_fingerprint": strategy.candidate_schema_fingerprint,
            "static_feature_schema": schemas[strategy.strategy_id],
            "snapshot_root": str(candidate_root(strategy.strategy_id)),
        }
        service_id = strategy_service_id(strategy.strategy_id)
        settings[service_id] = {
            "feature_spool_root": str(feature_root),
            "runner_state_path": str(runner_state_path(strategy.strategy_id)),
            "strategy_registration_fingerprint": strategy.registration_fingerprint,
            "strategy_spec_fingerprint": strategy.strategy_spec_fingerprint,
            "evaluator_contract_fingerprint": strategy.executable_fingerprint,
            "strategy_executable_fingerprint": strategy.executable_fingerprint,
            "candidate_schema_fingerprint": strategy.candidate_schema_fingerprint,
            "candidate_snapshot_root": str(candidate_root(strategy.strategy_id)),
            "paper_broker_path": str(broker_root / "broker.sqlite3"),
            "paper_account_id": "shadow-main",
            "candidate_max_age_seconds": 7 * 24 * 60 * 60,
            "strategy_id": strategy.strategy_id,
            "strategy_version": strategy.strategy_version,
            "batch_limit": 128,
            "signal_bus_path": str(signal_root / "signal_bus.sqlite3"),
            "producer_instance_id": instance_label(service_id),
            "producer_version": strategy_live_producer_version(
                service_id=service_id,
                strategy_version=strategy.strategy_version,
                producer_commit=commit,
            ),
        }
    #: The health publisher watches every other runtime service, exactly as
    #: `build_production_runtime_profile` composes it: every kind-backed manifest except
    #: its own and the serving publisher it feeds.
    kinds = {
        service_id: kind
        for kind, service_id in _singleton_service_ids().items()
        if kind
        not in (RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER, RuntimeServiceKind.SERVING_PUBLISHER)
    }
    for strategy in strategies:
        kinds[candidate_service_id(strategy.strategy_id)] = RuntimeServiceKind.CANDIDATE_PUBLISHER
        kinds[strategy_service_id(strategy.strategy_id)] = RuntimeServiceKind.STRATEGY_LIVE
    settings["runtime-health.all.v1"] = {
        "authority_root": str(health_authority),
        "sources": [
            {
                "control_root": str(
                    root / "control" / _control_bucket(kind) / instance_label(service_id)
                ),
                "service_id": service_id,
                "plane": role_plane(kind.value),
                "stale_after_seconds": MANIFEST_STALE_AFTER_SECONDS,
                "producer_commit": commit,
            }
            for service_id, kind in sorted(kinds.items())
        ],
    }
    return settings


def _singleton_service_ids() -> Mapping[object, str]:
    from rquant.runtime_production_profile import _SINGLETON_SERVICE_IDS

    return _SINGLETON_SERVICE_IDS


def role_plane(role: str) -> str:
    """The plane `role` runs on, read out of the one table the builders assert against.

    `runtime_deployment_bundle._EXPECTED_PLANE` is that table: `validate_runtime_deployment_
    topology` refuses a profile whose manifest disagrees with it, and every builder repeats
    the same expectation as its own first assertion. Route B used to write `live` into all
    28 manifests (#200), which the seven serving and research builders rejected on sight, so
    this reads the shared table rather than restating it (the `_DISTRIBUTION_OWNED_
    DIRECTORIES` rule of #198).
    """

    from rquant.runtime_deployment_bundle import _EXPECTED_PLANE
    from rquant.runtime_service_entrypoint import RuntimeServiceKind

    try:
        kind = RuntimeServiceKind(role)
    except ValueError as exc:
        raise RuntimeAuthorityStageError(f"role names no runtime service kind: {role}") from exc
    plane = _EXPECTED_PLANE.get(kind)
    if plane is None:
        raise RuntimeAuthorityStageError(f"role has no plane in the shared table: {role}")
    return str(plane.value)


def _kind_backed_manifest(
    role: str,
    service_id: str,
    commit: str,
    settings: Mapping[str, object],
) -> bytes:
    return canonical_json_bytes(
        {
            "schema_version": 2,
            "service_id": service_id,
            "service_kind": role,
            "plane": role_plane(role),
            "interval_seconds": MANIFEST_INTERVAL_SECONDS,
            "stale_after_seconds": MANIFEST_STALE_AFTER_SECONDS,
            "producer_commit": commit,
            "settings": dict(settings),
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

    derived = bootstrap_settings(commit)

    def add(role: str, service_id: str) -> None:
        entry = policy.get(role)
        if entry is None or entry.service_kind != role or not entry.instanced:
            raise RuntimeAuthorityStageError(f"service id {service_id} names no kind-backed role")
        if service_id not in derived:
            raise RuntimeAuthorityStageError(f"service id has no derived settings: {service_id}")
        label = instance_label(service_id)
        instances.setdefault(role, []).append(label)
        service_ids[label] = service_id
        manifests[label] = _kind_backed_manifest(role, service_id, commit, derived[service_id])

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
            "stdlib_roots": [str(path) for path in closure.stdlib_roots],
            "skipped_stdlib_roots": [str(path) for path in closure.skipped_stdlib_roots],
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
