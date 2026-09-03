"""Process entrypoint for one allow-listed isolated runtime service."""

from __future__ import annotations

import argparse
import os
import re
import signal
import stat
import subprocess
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from types import FrameType
from typing import TYPE_CHECKING

from loguru import logger

from rquant.runtime_capabilities import load_systemd_runtime_capabilities
from rquant.runtime_deployment_profile import (
    PRODUCTION_SHADOW_SIGNER_COMMAND,
    load_current_runtime_deployment_profile,
)
from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceRegistry,
    RuntimeServiceStep,
    load_runtime_service_manifest,
    run_runtime_service_manifest,
)

if TYPE_CHECKING:
    from rquant.runtime_artifact_terminal_lifecycle import (
        ProductionArtifactTerminalLifecycle,
    )
    from rquant.runtime_schema_registry import RuntimeSchemaServiceBinding
    from rquant.runtime_service_control import RuntimeStepResult
    from rquant.runtime_shadow_validation import CompletionAttestationSigner

#: The directory a generation keeps its per-instance service manifests in. The wrapper
#: derives `--manifest` as `<generation>/manifests/<instance>.json` and refuses to forward
#: a path the generation's own full manifest does not cover.
AUTHORITY_MANIFEST_DIRECTORY = "manifests"

#: What a role reports while it runs without the legacy runtime root: no schema dual write,
#: and no artifact terminal lifecycle. Route B publishes no `data/runtime/current`, so the
#: first generation runs this way by design — but it has to say so, in the journal and in
#: the heartbeat, rather than degrade in silence.
RUNTIME_ROOT_DEGRADED_REASON = "runtime_root_unavailable"

_ARTIFACT_TERMINAL_OWNER_SERVICE_KINDS = frozenset(
    {
        RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
        RuntimeServiceKind.ARTIFACT_RETENTION,
        RuntimeServiceKind.PROMOTIONS_PUBLISHER,
    }
)


def build_runtime_artifact_terminal_lifecycle_factory(
    runtime_root: Path,
    *,
    service_kind: RuntimeServiceKind,
) -> Callable[[], ProductionArtifactTerminalLifecycle]:
    """Return the service-scoped production terminal composition.

    The factory is lazy and preserves the manifest's capability boundary: no
    publisher can acquire a retention metadata or source-authority writer just
    by starting its process.
    """

    from rquant.runtime_artifact_terminal_lifecycle import (
        build_production_artifact_terminal_lifecycle,
    )

    normalized_root = runtime_root.resolve(strict=False)

    def open_lifecycle() -> ProductionArtifactTerminalLifecycle:
        return build_production_artifact_terminal_lifecycle(
            runtime_root=normalized_root,
            experiment_registry_path=normalized_root / "research" / "experiment_registry.sqlite3",
            service_kind=service_kind,
        )

    return open_lifecycle


def load_runtime_schema_service_bindings(
    runtime_root: Path,
    *,
    manifest: RuntimeServiceManifest,
    generation_id: str,
    observed_at: datetime,
) -> tuple[RuntimeSchemaServiceBinding, ...]:
    from rquant.runtime_deployment_bundle import (
        load_runtime_schema_service_bindings as load_bindings,
    )

    return load_bindings(
        runtime_root,
        manifest=manifest,
        generation_id=generation_id,
        observed_at=observed_at,
    )


def runtime_schema_dual_write_context(
    bindings: tuple[RuntimeSchemaServiceBinding, ...],
) -> AbstractContextManager[None]:
    from rquant.runtime_schema_registry import (
        runtime_schema_dual_write_context as schema_context,
    )

    return schema_context(bindings)


def _absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError("runtime paths must be absolute")
    return path


def _commit_sha(value: str) -> str:
    if re.fullmatch(r"[0-9a-f]{40}", value) is None:
        raise argparse.ArgumentTypeError("expected commit must be a full lowercase Git SHA")
    return value


def _generation_hash(value: str) -> str:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise argparse.ArgumentTypeError("expected generation must be a full lowercase SHA-256")
    return value


def build_builtin_registry(
    *,
    runtime_capabilities: Mapping[str, str],
    artifact_retention_schema_resolver: Callable[[int], str] | None = None,
    artifact_terminal_lifecycle_factory: (
        Callable[[], ProductionArtifactTerminalLifecycle] | None
    ) = None,
    completion_attestation_signer: CompletionAttestationSigner | None = None,
    completion_attestation_active_key_id: str | None = None,
    startup_degraded_reasons: tuple[str, ...] = (),
) -> RuntimeServiceRegistry:
    from rquant.runtime_service_builtin import build_builtin_registry as factory

    kwargs: dict[str, object] = {
        "runtime_capabilities": runtime_capabilities,
        "artifact_retention_schema_resolver": artifact_retention_schema_resolver,
        "artifact_terminal_lifecycle_factory": artifact_terminal_lifecycle_factory,
    }
    if completion_attestation_signer is not None:
        kwargs["completion_attestation_signer"] = completion_attestation_signer
        kwargs["completion_attestation_active_key_id"] = completion_attestation_active_key_id
    registry: RuntimeServiceRegistry = factory(**kwargs)  # type: ignore[arg-type]
    if startup_degraded_reasons:
        registry = _StartupDegradedRegistry(registry, reasons=startup_degraded_reasons)
    return registry


def runtime_root_from_control_root(control_root: Path) -> Path:
    """`<runtime root>/control/<kind directory>/<instance>` -> `<runtime root>`.

    `--control-root` is the root-owned prefix from the profile with the authorised instance
    label appended, so this is arithmetic on a validated value rather than a guess about
    caller-supplied input. The old reader instead looked for the literal `current` inside
    the manifest path; the authority chain has no such component, so it returned `None` and
    every role degraded without saying why.

    `runtime_recovery_service.runtime_root_for` does the same arithmetic, but it also
    insists the parent directory be named `recovery`. That is right for its own two roles
    and wrong for the other twenty-two, whose kind directories are all different
    (`strategies`, `notifiers`, `features`, ...), so the shape check here is the general
    one: three levels of parent, with `control` immediately above the kind directory. A
    path that does not have that shape is an error, never a silent `None`.
    """

    root = Path(control_root)
    parents = root.parents
    if len(parents) < 3 or parents[1].name != "control":
        raise ValueError("runtime control root does not sit under a runtime control tree")
    return parents[2]


def _read_authority_manifest(path: Path) -> bytes:
    """Read a manifest out of the immutable generation, one path component at a time.

    The old-chain reader (`runtime_service_entrypoint._read_owned_manifest`) requires the
    file to be owned by the running user with mode 0600, which is what the lighthouse-owned
    `data/runtime` tree looked like. A generation's copy of the same document is root-owned
    and 0444, and the wrapper already compared its bytes with the root-owned full manifest
    before this process existed — so those two checks cannot be met and are not what is
    being defended here. What is left to defend is the walk: no symlinked component, a
    regular file, owned by root or by this process, and not writable by group or other.
    """

    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    directory_descriptor = -1
    manifest_descriptor = -1
    try:
        directory_descriptor = os.open(path.anchor, directory_flags | no_follow)
        for component in path.parts[1:-1]:
            child_descriptor = os.open(
                component,
                directory_flags | no_follow,
                dir_fd=directory_descriptor,
            )
            os.close(directory_descriptor)
            directory_descriptor = child_descriptor
        manifest_descriptor = os.open(
            path.name,
            os.O_RDONLY | no_follow,
            dir_fd=directory_descriptor,
        )
        observed = os.fstat(manifest_descriptor)
        if not stat.S_ISREG(observed.st_mode):
            raise ValueError("runtime service manifest must be a regular file")
        if observed.st_uid not in {0, os.geteuid()}:
            raise ValueError("runtime service manifest is not owned by root or this runtime")
        if stat.S_IMODE(observed.st_mode) & (stat.S_IWGRP | stat.S_IWOTH):
            raise ValueError("runtime service manifest is writable outside its owner")
        with os.fdopen(manifest_descriptor, "rb", closefd=True) as stream:
            manifest_descriptor = -1
            return stream.read()
    except OSError as exc:
        raise ValueError("runtime service manifest is unavailable or contains a symlink") from exc
    finally:
        if manifest_descriptor >= 0:
            os.close(manifest_descriptor)
        if directory_descriptor >= 0:
            os.close(directory_descriptor)


def load_authority_service_manifest(
    path: Path,
    *,
    expected_commit: str,
    expected_generation: str,
) -> RuntimeServiceManifest:
    """Load the service manifest the wrapper selected out of the current generation.

    The generation binding that the old chain got from following a `current` symlink comes
    from the path itself here: the wrapper only ever derives
    `<generation>/manifests/<instance>.json`, and the generation directory is named by the
    same id it passes as `--expected-generation`.
    """

    resolved = Path(os.path.abspath(path))
    if resolved.parent.name != AUTHORITY_MANIFEST_DIRECTORY:
        raise ValueError("runtime service manifest is outside the generation manifest directory")
    if resolved.parent.parent.name != expected_generation:
        raise ValueError("runtime service manifest generation does not match runtime environment")
    payload = _read_authority_manifest(resolved)
    try:
        manifest = RuntimeServiceManifest.model_validate_json(payload)
    except ValueError as exc:
        if str(exc).startswith("runtime service manifest"):
            raise
        raise ValueError("invalid runtime service manifest") from exc
    if manifest.producer_commit != expected_commit:
        raise ValueError("runtime service manifest commit does not match running code")
    return manifest


class _StartupDegradedStep:
    """Stamp a startup degradation onto whatever the real step reports."""

    def __init__(self, step: RuntimeServiceStep, reasons: tuple[str, ...]) -> None:
        self._step = step
        self._reasons = reasons

    def __call__(self) -> RuntimeStepResult:
        result = self._step()
        merged = tuple(sorted({*result.degraded_reasons, *self._reasons}))
        if merged == tuple(result.degraded_reasons):
            return result
        return result.model_copy(update={"degraded_reasons": merged})

    def close(self) -> None:
        close = getattr(self._step, "close", None)
        if callable(close):
            close()

    def __getattr__(self, name: str) -> object:
        return getattr(self._step, name)


class _StartupDegradedRegistry(RuntimeServiceRegistry):
    """Wrap a builtin registry so every heartbeat carries the startup degradation.

    A degraded reason is what turns the heartbeat's status from `running` into `degraded`,
    which is the only channel `runtime_health_publisher` reads. Logging the degradation and
    then publishing a healthy heartbeat would be the silence this exists to remove.
    """

    def __init__(self, inner: RuntimeServiceRegistry, *, reasons: tuple[str, ...]) -> None:
        super().__init__()
        self._inner = inner
        self._reasons = reasons

    @property
    def startup_degraded_reasons(self) -> tuple[str, ...]:
        return self._reasons

    @property
    def registered_kinds(self) -> tuple[RuntimeServiceKind, ...]:
        return self._inner.registered_kinds

    def open_artifact_terminal_lifecycle(self) -> ProductionArtifactTerminalLifecycle:
        return self._inner.open_artifact_terminal_lifecycle()

    def register(self, kind: RuntimeServiceKind, builder: object) -> None:
        self._inner.register(kind, builder)  # type: ignore[arg-type]

    def build(self, manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        return _StartupDegradedStep(self._inner.build(manifest), self._reasons)


def _runtime_root_from_current_manifest(path: Path) -> Path | None:
    manifest_parts = path.parts
    try:
        current_index = manifest_parts.index("current")
    except ValueError:
        return None
    if manifest_parts[current_index + 1 : current_index + 2] != ("manifests",):
        raise ValueError("runtime manifest current pointer layout is invalid")
    return Path(*manifest_parts[:current_index])


def build_runtime_strategy_completion_attestation_signer(
    runtime_root: Path,
    *,
    manifest: RuntimeServiceManifest,
) -> tuple[CompletionAttestationSigner, str]:
    """Open the profile-bound Shadow completion signer for one strategy service."""

    from rquant.runtime_shadow_validation import (
        Ed25519CompletionAttestationKeyring,
        Ed25519CompletionAttestationSigner,
        SecureShadowSigningClient,
    )

    profile = load_current_runtime_deployment_profile(runtime_root)
    if getattr(profile, "producer_commit", None) != manifest.producer_commit:
        raise ValueError("strategy completion signer profile commit differs from manifest")
    profile_manifests: list[RuntimeServiceManifest] = []
    for item in getattr(profile, "manifests", ()):
        try:
            profile_manifests.append(RuntimeServiceManifest.model_validate(item))
        except ValueError as exc:
            raise ValueError(
                "strategy completion signer profile contains invalid manifests"
            ) from exc
    matches = tuple(item for item in profile_manifests if item.service_id == manifest.service_id)
    if len(matches) != 1 or matches[0].manifest_fingerprint != manifest.manifest_fingerprint:
        raise ValueError("strategy completion signer profile does not bind this manifest")
    shadow = getattr(profile, "shadow", None)
    if shadow is None:
        raise ValueError("strategy completion signer requires the current Shadow signer profile")
    command = tuple(getattr(shadow, "signer_command", ()))
    if command != PRODUCTION_SHADOW_SIGNER_COMMAND:
        raise ValueError("strategy completion signer must use the fixed protected Shadow helper")
    active_key_id = str(getattr(shadow, "completion_active_key_id", "")).strip()
    active_public_key = getattr(shadow, "completion_active_public_key_pem", None)
    previous_raw = getattr(shadow, "completion_previous_public_key_pems", {})
    if not active_key_id or not isinstance(active_public_key, str) or not active_public_key:
        raise ValueError("strategy completion signer profile lacks active Ed25519 credentials")
    if not isinstance(previous_raw, Mapping):
        raise ValueError("strategy completion signer previous keyring is invalid")
    previous_public_keys: dict[str, bytes] = {}
    for key_id, public_key in previous_raw.items():
        if not isinstance(key_id, str) or not isinstance(public_key, str):
            raise ValueError("strategy completion signer previous keyring is invalid")
        previous_public_keys[key_id] = public_key.encode("utf-8")
    try:
        keyring = Ed25519CompletionAttestationKeyring(
            active_key_id=active_key_id,
            active_public_key=active_public_key.encode("utf-8"),
            previous_public_keys=previous_public_keys,
        )
    except ValueError as exc:
        raise ValueError(
            "strategy completion signer profile has invalid Ed25519 public key material"
        ) from exc
    timeout_seconds = getattr(shadow, "timeout_seconds", None)
    if not isinstance(timeout_seconds, int | float) or isinstance(timeout_seconds, bool):
        raise ValueError("strategy completion signer timeout is invalid")
    signer = Ed25519CompletionAttestationSigner(
        key_id=keyring.active_key_id,
        client=SecureShadowSigningClient(
            command=PRODUCTION_SHADOW_SIGNER_COMMAND,
            key_id=keyring.active_key_id,
            timeout_seconds=float(timeout_seconds),
        ),
    )
    return signer, keyring.active_key_id


def _retention_schema_resolver(
    manifest: RuntimeServiceManifest,
) -> Callable[[int], str] | None:
    if manifest.service_kind is not RuntimeServiceKind.ARTIFACT_RETENTION:
        return None
    from rquant.runtime_builder_retention import TrustedDescriptorSchemaResolver

    required = (
        "schema_authority_root",
        "schema_authority_path",
        "schema_authority_sha256",
    )
    missing = [key for key in required if not str(manifest.settings.get(key, "")).strip()]
    if missing:
        raise ValueError(
            "artifact retention manifest lacks trusted schema authority: " + ", ".join(missing)
        )
    return TrustedDescriptorSchemaResolver.from_authority(
        root=Path(str(manifest.settings["schema_authority_root"])),
        path=Path(str(manifest.settings["schema_authority_path"])),
        expected_sha256=str(manifest.settings["schema_authority_sha256"]),
    )


def resolve_checkout_commit(root: Path | None = None) -> str:
    checkout = (root or Path.cwd()).resolve()
    try:
        revision = subprocess.run(
            [
                "/usr/bin/git",
                "-C",
                str(checkout),
                "rev-parse",
                "--verify",
                "HEAD^{commit}",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as exc:
        raise RuntimeError("runtime checkout commit cannot be verified") from exc
    commit = revision.stdout.strip()
    if revision.returncode != 0 or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise RuntimeError("runtime checkout commit cannot be verified")
    try:
        status = subprocess.run(
            [
                "/usr/bin/git",
                "-C",
                str(checkout),
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as exc:
        raise RuntimeError("runtime checkout cleanliness cannot be verified") from exc
    if status.returncode != 0:
        raise RuntimeError("runtime checkout cleanliness cannot be verified")
    if status.stdout.strip():
        raise RuntimeError("runtime checkout source tree must be clean")
    return commit


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one isolated rQuant service")
    parser.add_argument("--manifest", required=True, type=_absolute_path)
    parser.add_argument("--control-root", required=True, type=_absolute_path)
    parser.add_argument("--expected-commit", required=True, type=_commit_sha)
    parser.add_argument("--expected-generation", required=True, type=_generation_hash)
    parser.add_argument(
        "--expected-kind",
        action="append",
        type=RuntimeServiceKind,
        choices=tuple(RuntimeServiceKind),
        help="Reject manifests not admitted by this systemd unit template",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one service step and stop; intended for validation only",
    )
    parser.add_argument(
        "--authority-runtime",
        action="store_true",
        help=(
            "Take the commit, the generation and the runtime root from the root-owned "
            "authority documents instead of a git checkout"
        ),
    )
    return parser


def run(args: argparse.Namespace) -> int:
    authority_runtime = bool(getattr(args, "authority_runtime", False))
    if authority_runtime:
        # No git here, on purpose. The generation's working directory is not a checkout, so
        # `resolve_checkout_commit` could only fail; and the commit it would be asked to
        # confirm already comes from root-owned `current.json`, which the wrapper checked
        # the shape of before forwarding it. Re-deriving it from a repository this process
        # is not allowed to see would be a weaker answer, not a second opinion.
        manifest = load_authority_service_manifest(
            args.manifest,
            expected_commit=args.expected_commit,
            expected_generation=args.expected_generation,
        )
    else:
        actual_commit = resolve_checkout_commit()
        if actual_commit != args.expected_commit:
            raise RuntimeError("runtime checkout commit does not match expected commit")
        manifest = load_runtime_service_manifest(
            args.manifest,
            expected_commit=args.expected_commit,
            expected_generation=args.expected_generation,
        )
    if args.expected_kind is not None and manifest.service_kind not in args.expected_kind:
        raise ValueError("runtime manifest kind is not admitted by this systemd unit")
    instance = args.manifest.stem
    if re.fullmatch(r"svc-[0-9a-f]{64}", instance) is None:
        raise ValueError("runtime manifest filename does not identify a valid service instance")
    runtime_capabilities = load_systemd_runtime_capabilities(
        manifest.service_kind,
        expected_service_id=manifest.service_id,
        expected_instance=instance,
        expected_generation=args.expected_generation,
    )
    retention_schema_resolver = _retention_schema_resolver(manifest)
    artifact_terminal_lifecycle_factory: (
        Callable[[], ProductionArtifactTerminalLifecycle] | None
    ) = None
    completion_attestation_signer: CompletionAttestationSigner | None = None
    completion_attestation_active_key_id: str | None = None
    startup_degraded_reasons: tuple[str, ...] = ()
    if authority_runtime:
        derived_root = runtime_root_from_control_root(args.control_root)
        if derived_root.is_dir():
            runtime_root = derived_root
        else:
            # Route B publishes no legacy runtime root, so the first generation runs
            # without schema dual write and without an artifact terminal lifecycle. That
            # is an accepted degradation, not an accident — but it used to be indicated by
            # nothing at all, which is what made the review call it silent.
            runtime_root = None
            startup_degraded_reasons = (RUNTIME_ROOT_DEGRADED_REASON,)
            logger.warning(
                "runtime root {root} is unavailable: schema dual write and the artifact "
                "terminal lifecycle are disabled for service {service_id} ({kind})",
                root=str(derived_root),
                service_id=manifest.service_id,
                kind=manifest.service_kind.value,
            )
    else:
        runtime_root = _runtime_root_from_current_manifest(args.manifest)
    if runtime_root is None:
        schema_bindings = ()
    else:
        if manifest.service_kind in _ARTIFACT_TERMINAL_OWNER_SERVICE_KINDS:
            artifact_terminal_lifecycle_factory = build_runtime_artifact_terminal_lifecycle_factory(
                runtime_root,
                service_kind=manifest.service_kind,
            )
        schema_bindings = load_runtime_schema_service_bindings(
            runtime_root,
            manifest=manifest,
            generation_id=args.expected_generation,
            observed_at=datetime.now(UTC),
        )
    if manifest.service_kind is RuntimeServiceKind.STRATEGY_LIVE:
        if runtime_root is None:
            raise ValueError("strategy-live runtime must use a current deployment profile")
        (
            completion_attestation_signer,
            completion_attestation_active_key_id,
        ) = build_runtime_strategy_completion_attestation_signer(
            runtime_root,
            manifest=manifest,
        )
    stop_event = Event()

    def request_stop(_signum: int, _frame: FrameType | None) -> None:
        stop_event.set()

    previous_handlers = {
        signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        for signum in previous_handlers:
            signal.signal(signum, request_stop)
        with runtime_schema_dual_write_context(schema_bindings):
            registry_kwargs: dict[str, object] = {
                "runtime_capabilities": runtime_capabilities,
            }
            if startup_degraded_reasons:
                registry_kwargs["startup_degraded_reasons"] = startup_degraded_reasons
            if retention_schema_resolver is not None:
                registry_kwargs["artifact_retention_schema_resolver"] = retention_schema_resolver
            if artifact_terminal_lifecycle_factory is not None:
                registry_kwargs["artifact_terminal_lifecycle_factory"] = (
                    artifact_terminal_lifecycle_factory
                )
            if completion_attestation_signer is not None:
                registry_kwargs["completion_attestation_signer"] = completion_attestation_signer
                registry_kwargs["completion_attestation_active_key_id"] = (
                    completion_attestation_active_key_id
                )
            registry = build_builtin_registry(**registry_kwargs)  # type: ignore[arg-type]
            run_runtime_service_manifest(
                manifest,
                registry=registry,
                control_root=args.control_root,
                stop_event=stop_event,
                max_iterations=1 if args.once else None,
            )
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(build_parser().parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AUTHORITY_MANIFEST_DIRECTORY",
    "RUNTIME_ROOT_DEGRADED_REASON",
    "build_runtime_artifact_terminal_lifecycle_factory",
    "build_runtime_strategy_completion_attestation_signer",
    "build_builtin_registry",
    "build_parser",
    "load_authority_service_manifest",
    "main",
    "resolve_checkout_commit",
    "run",
    "runtime_root_from_control_root",
]
