"""Offline collection and immutable publication of attested runtime code."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import stat
import uuid
from collections.abc import Callable
from contextlib import ExitStack, suppress
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator

from rquant.adapter_manifest import VerifyOnlyEd25519Keyring
from rquant.authority_path_security import (
    AuthorityPathSecurityError,
    SecureRegularFileLease,
    open_secure_regular_file_lease,
)
from rquant.runtime_code_attestation import (
    CodeTrustEvidence,
    RuntimeCodeAttestation,
    RuntimeCodeBundle,
    RuntimeCodeBundleEntry,
    RuntimeCodeGenerationArtifact,
    RuntimeCodeGenerationManifest,
    RuntimeCodePromotionReceipt,
    RuntimeCodePromotionTrust,
    RuntimeCodeTrustError,
    VerifiedRuntimeCodeAttestation,
    build_runtime_code_bundle,
    require_runtime_code_attestation,
    require_runtime_code_promotion_receipt,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.strict_json import (
    StrictJsonError,
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
)

_HASH_PATTERN = r"^[0-9a-f]{64}$"
_MAX_BUNDLE_BYTES = 1024 * 1024 * 1024
_MAX_AUTHORITY_BYTES = 16 * 1024 * 1024
_POINTER_BYTES = 66
_PACKAGE_MODES = frozenset({0o400, 0o440, 0o444, 0o600, 0o640, 0o644})
_COLLECT_MODES = frozenset(
    {0o400, 0o440, 0o444, 0o500, 0o550, 0o555, 0o600, 0o640, 0o644, 0o700, 0o750, 0o755}
)
FaultHook = Callable[[str], None]


class RuntimeCodeGenerationError(RuntimeError):
    """An input or installed runtime generation cannot be trusted."""


def _canonical_relative_path(value: str) -> str:
    if not value or value != value.strip() or not value.isascii():
        raise ValueError("runtime generation path must be canonical ASCII")
    if value.startswith("/") or "\\" in value or "//" in value:
        raise ValueError("runtime generation path must be relative POSIX")
    candidate = PurePosixPath(value)
    if str(candidate) != value or any(part in {"", ".", ".."} for part in candidate.parts):
        raise ValueError("runtime generation path must be canonical")
    if any(part.casefold() == ".git" for part in candidate.parts):
        raise ValueError("runtime generation path cannot contain Git metadata")
    return value


def _canonical_absolute_path(value: Path) -> Path:
    if not value.is_absolute() or Path(os.path.abspath(value)) != value:
        raise ValueError("runtime authority path must be canonical absolute")
    return value


class RuntimeCodeCollectFile(RuntimeContractModel):
    source_path: str
    bundle_path: str
    mode: Literal[292, 365]

    @field_validator("source_path", "bundle_path", mode="before")
    @classmethod
    def validate_path(cls, value: object) -> object:
        if not isinstance(value, str):
            raise ValueError("runtime collection path must be text")
        return _canonical_relative_path(value)


class RuntimeCodeInstallRequest(RuntimeContractModel):
    source_root: Path
    bundle_path: Path
    attestation_path: Path
    certificate_path: Path
    receipt_path: Path
    expected_audience: str = Field(min_length=1, max_length=200)
    expected_installation_id: str = Field(min_length=1, max_length=200)
    expected_target_platform: str = Field(min_length=1, max_length=200)
    now: AwareUtcDatetime

    @field_validator(
        "source_root",
        "bundle_path",
        "attestation_path",
        "certificate_path",
        "receipt_path",
        mode="after",
    )
    @classmethod
    def validate_absolute_path(cls, value: Path) -> Path:
        return _canonical_absolute_path(value)

    @model_validator(mode="after")
    def validate_source_boundary(self) -> Self:
        for path in (
            self.bundle_path,
            self.attestation_path,
            self.certificate_path,
            self.receipt_path,
        ):
            try:
                path.relative_to(self.source_root)
            except ValueError as exc:
                raise ValueError("runtime package path escapes source root") from exc
        return self


class RuntimeCodeInstallReceipt(RuntimeContractModel):
    generation_id: str = Field(pattern=_HASH_PATTERN)
    previous_generation_id: str | None = Field(default=None, pattern=_HASH_PATTERN)
    write_performed: bool
    evidence: CodeTrustEvidence


class LoadedRuntimeCodeGeneration(RuntimeContractModel):
    generation_root: Path
    release_root: Path
    manifest: RuntimeCodeGenerationManifest
    attestation: RuntimeCodeAttestation
    promotion_receipt: RuntimeCodePromotionReceipt
    evidence: CodeTrustEvidence
    material_uid: int = Field(strict=True, ge=0)
    material_gid: int = Field(strict=True, ge=0)


class RuntimeCodeGenerationCapability:
    """Process-local lease proving one verified generation remains selected."""

    def __init__(
        self,
        *,
        loaded: LoadedRuntimeCodeGeneration,
        pointer_lease: SecureRegularFileLease,
        artifact_leases: tuple[SecureRegularFileLease, ...],
        require_authority_paths: Callable[[], None],
        require_exact_tree: Callable[[], None],
        require_current_promotion: Callable[[], None],
        audit_events: tuple[str, ...],
    ) -> None:
        self.loaded = loaded
        self._pointer_lease = pointer_lease
        self._artifact_leases = artifact_leases
        self._require_authority_paths = require_authority_paths
        self._require_exact_tree = require_exact_tree
        self._require_current_promotion = require_current_promotion
        self._audit_events = audit_events
        self._execution_binding_digest: str | None = None
        self._closed = False

    @property
    def evidence(self) -> CodeTrustEvidence:
        return self.loaded.evidence

    @property
    def release_root(self) -> Path:
        return self.loaded.release_root

    @property
    def audit_events(self) -> tuple[str, ...]:
        return self._audit_events

    @property
    def execution_binding_digest(self) -> str | None:
        return self._execution_binding_digest

    def require_live(self) -> None:
        if self._closed:
            raise RuntimeCodeGenerationError("runtime code capability is closed")
        expected_pointer = f"{self.evidence.generation_id}\n".encode("ascii")
        if self._pointer_lease.read_all(max_bytes=_POINTER_BYTES) != expected_pointer:
            raise RuntimeCodeGenerationError("runtime generation selection changed")
        for lease in self._artifact_leases:
            lease.require_unchanged()
        self._require_authority_paths()
        self._require_exact_tree()
        try:
            self._require_current_promotion()
        except RuntimeCodeTrustError as exc:
            raise RuntimeCodeGenerationError(
                "runtime generation promotion is no longer current"
            ) from exc

    def _mark_verified_execution(self, binding_digest: str) -> None:
        """Record a child execution only after its generation receipt was verified."""

        if re.fullmatch(_HASH_PATTERN, binding_digest) is None:
            raise RuntimeCodeGenerationError("runtime execution binding digest is invalid")
        self.require_live()
        if self._execution_binding_digest not in {None, binding_digest}:
            raise RuntimeCodeGenerationError("runtime execution binding changed")
        self._execution_binding_digest = binding_digest
        self._audit_events = tuple(
            "execution-binding-verified" if event == "execution-binding-pending" else event
            for event in self._audit_events
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for lease in reversed(self._artifact_leases):
            lease.close()
        self._pointer_lease.close()

    def __enter__(self) -> RuntimeCodeGenerationCapability:
        self.require_live()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def collect_runtime_code_bundle(
    checkout_root: Path,
    files: tuple[RuntimeCodeCollectFile, ...],
    *,
    expected_uid: int,
    expected_gid: int,
    fault_hook: FaultHook | None = None,
) -> RuntimeCodeBundle:
    """Collect checkout bytes through retained descriptors; Git is not consulted."""

    checkout_root = _canonical_absolute_path(checkout_root)
    if not files:
        raise RuntimeCodeGenerationError("runtime collection cannot be empty")
    ordered = tuple(sorted(files, key=lambda item: item.bundle_path))
    if len({item.bundle_path.casefold() for item in ordered}) != len(ordered):
        raise RuntimeCodeGenerationError("runtime collection contains duplicate paths")
    try:
        with ExitStack() as stack:
            entries: list[RuntimeCodeBundleEntry] = []
            for item in ordered:
                source = checkout_root.joinpath(*PurePosixPath(item.source_path).parts)
                lease = stack.enter_context(
                    open_secure_regular_file_lease(
                        source,
                        trusted_root=checkout_root,
                        expected_uid=expected_uid,
                        expected_gid=expected_gid,
                        allowed_modes=_COLLECT_MODES,
                        max_bytes=256 * 1024 * 1024,
                        min_bytes=0,
                    )
                )
                if fault_hook is not None:
                    fault_hook(f"collector:after-open:{item.source_path}")
                entries.append(
                    RuntimeCodeBundleEntry(
                        path=item.bundle_path,
                        mode=item.mode,
                        content=lease.read_all(max_bytes=256 * 1024 * 1024),
                    )
                )
            return build_runtime_code_bundle(tuple(entries))
    except RuntimeCodeGenerationError:
        raise
    except (AuthorityPathSecurityError, OSError, ValueError) as exc:
        raise RuntimeCodeGenerationError(f"runtime collection changed or is unsafe: {exc}") from exc


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_new_file(path: Path, payload: bytes, mode: int) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0),
        mode,
    )
    try:
        offset = 0
        while offset < len(payload):
            offset += os.write(descriptor, payload[offset:])
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
        observed = os.fstat(descriptor)
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_nlink != 1
            or stat.S_IMODE(observed.st_mode) != mode
        ):
            raise RuntimeCodeGenerationError("published runtime file identity is unsafe")
    finally:
        os.close(descriptor)


def _remove_staging(path: Path) -> None:
    if not path.exists() or path.is_symlink():
        return
    for directory, child_directories, _files in os.walk(path, topdown=False):
        for child in child_directories:
            os.chmod(Path(directory) / child, 0o700, follow_symlinks=False)
        os.chmod(directory, 0o700, follow_symlinks=False)
    shutil.rmtree(path)


def _atomic_pointer(root: Path, name: str, generation_id: str) -> None:
    temporary = root / f".{name}.{uuid.uuid4().hex}.tmp"
    try:
        _write_new_file(temporary, f"{generation_id}\n".encode("ascii"), 0o444)
        os.replace(temporary, root / name)
        _fsync_directory(root)
    finally:
        with suppress(FileNotFoundError):
            temporary.unlink()


def _artifact(path: str, payload: bytes, mode: int) -> RuntimeCodeGenerationArtifact:
    return RuntimeCodeGenerationArtifact(
        path=path,
        mode=mode,
        size=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )


def _materialized_root(files: tuple[RuntimeCodeGenerationArtifact, ...]) -> str:
    return canonical_sha256(
        {
            "contract": "rquant-runtime-code-materialized-tree/v1",
            "files": [file.model_dump(mode="json") for file in files],
        }
    )


def _read_pointer(
    runtime_root: Path,
    *,
    trusted_base: Path,
    name: str,
    expected_uid: int,
    expected_gid: int,
) -> str | None:
    path = runtime_root / name
    try:
        path.lstat()
    except FileNotFoundError:
        return None
    with open_secure_regular_file_lease(
        path,
        trusted_root=trusted_base,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
        allowed_modes=frozenset({0o400, 0o440, 0o444}),
        max_bytes=_POINTER_BYTES,
    ) as lease:
        payload = lease.read_all(max_bytes=_POINTER_BYTES)
    if len(payload) != 65 or not payload.endswith(b"\n"):
        raise RuntimeCodeGenerationError("runtime generation pointer is invalid")
    generation_id = payload[:-1].decode("ascii", errors="strict")
    if len(generation_id) != 64 or any(
        character not in "0123456789abcdef" for character in generation_id
    ):
        raise RuntimeCodeGenerationError("runtime generation pointer is invalid")
    return generation_id


def _read_generation_file(
    path: Path,
    *,
    trusted_base: Path,
    expected_uid: int,
    expected_gid: int,
    max_bytes: int,
    modes: frozenset[int] = frozenset({0o444}),
) -> bytes:
    with open_secure_regular_file_lease(
        path,
        trusted_root=trusted_base,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
        allowed_modes=modes,
        max_bytes=max_bytes,
        min_bytes=0,
    ) as lease:
        return lease.read_all(max_bytes=max_bytes)


def _read_package(
    request: RuntimeCodeInstallRequest,
    *,
    expected_uid: int,
    expected_gid: int,
    fault_hook: FaultHook | None = None,
) -> tuple[bytes, bytes, bytes, bytes]:
    paths = (
        (request.bundle_path, _MAX_BUNDLE_BYTES),
        (request.attestation_path, _MAX_AUTHORITY_BYTES),
        (request.certificate_path, _MAX_AUTHORITY_BYTES),
        (request.receipt_path, _MAX_AUTHORITY_BYTES),
    )
    with ExitStack() as stack:
        leases: list[SecureRegularFileLease] = []
        for path, maximum in paths:
            leases.append(
                stack.enter_context(
                    open_secure_regular_file_lease(
                        path,
                        trusted_root=request.source_root,
                        expected_uid=expected_uid,
                        expected_gid=expected_gid,
                        allowed_modes=_PACKAGE_MODES,
                        max_bytes=maximum,
                        min_bytes=1,
                    )
                )
            )
        if fault_hook is not None:
            fault_hook("installer:package-leased")
        return tuple(
            lease.read_all(max_bytes=maximum)
            for lease, (_path, maximum) in zip(leases, paths, strict=True)
        )  # type: ignore[return-value]


class RuntimeCodeGenerationInstaller:
    def __init__(
        self,
        *,
        runtime_root: Path,
        trusted_base: Path,
        root_keyring: VerifyOnlyEd25519Keyring,
        runtime_keyring: VerifyOnlyEd25519Keyring,
        promotion_trust: RuntimeCodePromotionTrust,
        expected_uid: int,
        expected_gid: int,
        fault_hook: FaultHook | None = None,
    ) -> None:
        self._runtime_root = _canonical_absolute_path(runtime_root)
        self._trusted_base = _canonical_absolute_path(trusted_base)
        self._root_keyring = root_keyring
        self._runtime_keyring = runtime_keyring
        self._promotion_trust = promotion_trust
        self._expected_uid = expected_uid
        self._expected_gid = expected_gid
        self._fault_hook = fault_hook

    def install(self, request: RuntimeCodeInstallRequest) -> RuntimeCodeInstallReceipt:
        try:
            bundle_bytes, attestation_bytes, certificate_bytes, receipt_bytes = _read_package(
                request,
                expected_uid=self._expected_uid,
                expected_gid=self._expected_gid,
                fault_hook=self._fault_hook,
            )
            verified = require_runtime_code_attestation(
                attestation_bytes=attestation_bytes,
                certificate_bytes=certificate_bytes,
                bundle_bytes=bundle_bytes,
                root_keyring=self._root_keyring,
                runtime_keyring=self._runtime_keyring,
                expected_audience=request.expected_audience,
                expected_installation_id=request.expected_installation_id,
                expected_target_platform=request.expected_target_platform,
                now=request.now,
            )
            candidate = strict_model_validate_canonical_json(
                RuntimeCodePromotionReceipt,
                receipt_bytes,
            )
            current_id = _read_pointer(
                self._runtime_root,
                trusted_base=self._trusted_base,
                name="current",
                expected_uid=self._expected_uid,
                expected_gid=self._expected_gid,
            )
            current = None
            if current_id is not None:
                current = require_attested_runtime_generation(
                    runtime_root=self._runtime_root,
                    trusted_base=self._trusted_base,
                    root_keyring=self._root_keyring,
                    runtime_keyring=self._runtime_keyring,
                    promotion_trust=self._promotion_trust,
                    expected_uid=self._expected_uid,
                    expected_gid=self._expected_gid,
                    expected_audience=request.expected_audience,
                    expected_installation_id=request.expected_installation_id,
                    expected_target_platform=request.expected_target_platform,
                    now=request.now,
                    require_current_promotion=False,
                )
            if current is not None and candidate.generation_id == current.evidence.generation_id:
                expected_previous = candidate.previous_receipt_sha256
                minimum_sequence = candidate.promotion_sequence
            elif current is None:
                expected_previous = "0" * 64
                minimum_sequence = 1
            else:
                expected_previous = current.promotion_receipt.receipt_hash
                minimum_sequence = current.promotion_receipt.promotion_sequence + 1
            receipt = require_runtime_code_promotion_receipt(
                receipt_bytes=receipt_bytes,
                trust=self._promotion_trust,
                attestation_sha256=hashlib.sha256(attestation_bytes).hexdigest(),
                bundle_sha256=verified.bundle.bundle_sha256,
                content_root_sha256=verified.bundle.content_root_sha256,
                installation_id=request.expected_installation_id,
                target_platform=request.expected_target_platform,
                minimum_promotion_sequence=minimum_sequence,
                expected_previous_receipt_sha256=expected_previous,
            )
            self._promotion_trust.require_current_receipt(receipt=receipt)
            evidence = _evidence(verified, receipt, attestation_bytes)
            if current is not None and receipt.generation_id == current.evidence.generation_id:
                return RuntimeCodeInstallReceipt(
                    generation_id=receipt.generation_id,
                    previous_generation_id=current.evidence.generation_id,
                    write_performed=False,
                    evidence=evidence,
                )
            self._publish(
                verified=verified,
                receipt=receipt,
                bundle_bytes=bundle_bytes,
                attestation_bytes=attestation_bytes,
                certificate_bytes=certificate_bytes,
                receipt_bytes=receipt_bytes,
            )
            if self._fault_hook is not None:
                self._fault_hook("installer:before-pointer")
            if current_id is not None:
                _atomic_pointer(self._runtime_root, "previous", current_id)
            _atomic_pointer(self._runtime_root, "current", receipt.generation_id)
            return RuntimeCodeInstallReceipt(
                generation_id=receipt.generation_id,
                previous_generation_id=current_id,
                write_performed=True,
                evidence=evidence,
            )
        except RuntimeCodeGenerationError:
            raise
        except (
            AuthorityPathSecurityError,
            RuntimeCodeTrustError,
            StrictJsonError,
            OSError,
            ValueError,
        ) as exc:
            raise RuntimeCodeGenerationError(
                f"runtime code generation installation failed: {exc}"
            ) from exc
        except Exception as exc:
            raise RuntimeCodeGenerationError(
                f"runtime code generation installation crashed: {exc}"
            ) from exc

    def _publish(
        self,
        *,
        verified: VerifiedRuntimeCodeAttestation,
        receipt: RuntimeCodePromotionReceipt,
        bundle_bytes: bytes,
        attestation_bytes: bytes,
        certificate_bytes: bytes,
        receipt_bytes: bytes,
    ) -> None:
        generations = self._runtime_root / "generations"
        generations.mkdir(mode=0o700, exist_ok=True)
        final = generations / receipt.generation_id
        release_artifacts = [
            _artifact(entry.path, entry.content, entry.mode) for entry in verified.bundle.entries
        ]
        authority_payloads = {
            "promotion-receipt.json": receipt_bytes,
            "runtime-code-attestation.json": attestation_bytes,
            "runtime-code-certificate.json": certificate_bytes,
            "runtime-code.bundle": bundle_bytes,
        }
        artifacts = release_artifacts + [
            _artifact(name, payload, 0o444) for name, payload in authority_payloads.items()
        ]
        ordered_release = tuple(sorted(release_artifacts, key=lambda item: item.path))
        manifest = RuntimeCodeGenerationManifest(
            generation_id=receipt.generation_id,
            attestation_sha256=hashlib.sha256(attestation_bytes).hexdigest(),
            receipt_sha256=hashlib.sha256(receipt_bytes).hexdigest(),
            bundle_sha256=hashlib.sha256(bundle_bytes).hexdigest(),
            materialized_tree_root_sha256=_materialized_root(ordered_release),
            artifacts=tuple(sorted(artifacts, key=lambda item: item.path)),
        )
        manifest_bytes = canonical_model_json_bytes(manifest)
        expected_payloads = {
            **authority_payloads,
            **{entry.path: entry.content for entry in verified.bundle.entries},
            "generation-manifest.json": manifest_bytes,
        }
        try:
            final.lstat()
            final_exists = True
        except FileNotFoundError:
            final_exists = False
        if final_exists:
            _require_published_generation(
                final,
                trusted_base=self._trusted_base,
                manifest=manifest,
                manifest_bytes=manifest_bytes,
                expected_payloads=expected_payloads,
                expected_uid=self._expected_uid,
                expected_gid=self._expected_gid,
            )
            return
        for stale in generations.iterdir():
            if stale.name.endswith(".staging") and stale.is_dir() and not stale.is_symlink():
                _remove_staging(stale)
        staging = generations / f".{receipt.generation_id}.{uuid.uuid4().hex}.staging"
        staging.mkdir(mode=0o700)
        try:
            for entry in verified.bundle.entries:
                target = staging.joinpath(*PurePosixPath(entry.path).parts)
                target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                _write_new_file(target, entry.content, entry.mode)
                if self._fault_hook is not None:
                    self._fault_hook(f"installer:after-extract:{entry.path}")
            for name, payload in authority_payloads.items():
                _write_new_file(staging / name, payload, 0o444)
            _write_new_file(staging / "generation-manifest.json", manifest_bytes, 0o444)
            for directory, child_directories, _files in os.walk(staging, topdown=False):
                for child in child_directories:
                    os.chmod(Path(directory) / child, 0o555, follow_symlinks=False)
                _fsync_directory(Path(directory))
            _require_published_generation(
                staging,
                trusted_base=self._trusted_base,
                manifest=manifest,
                manifest_bytes=manifest_bytes,
                expected_payloads=expected_payloads,
                expected_uid=self._expected_uid,
                expected_gid=self._expected_gid,
            )
            _fsync_directory(staging)
            os.replace(staging, final)
            os.chmod(final, 0o555, follow_symlinks=False)
            _fsync_directory(final)
            _fsync_directory(generations)
            _require_published_generation(
                final,
                trusted_base=self._trusted_base,
                manifest=manifest,
                manifest_bytes=manifest_bytes,
                expected_payloads=expected_payloads,
                expected_uid=self._expected_uid,
                expected_gid=self._expected_gid,
            )
        finally:
            if staging.exists():
                _remove_staging(staging)


def _evidence(
    verified: VerifiedRuntimeCodeAttestation,
    receipt: RuntimeCodePromotionReceipt,
    attestation_bytes: bytes,
) -> CodeTrustEvidence:
    return CodeTrustEvidence(
        generation_id=receipt.generation_id,
        attestation_sha256=hashlib.sha256(attestation_bytes).hexdigest(),
        content_root_sha256=verified.attestation.content_root_sha256,
        promotion_sequence=receipt.promotion_sequence,
        provenance_commit=verified.attestation.provenance_commit,
    )


def _walk_generation(
    generation_root: Path,
    *,
    allowed_owner_uids: frozenset[int],
) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
    observed_files: list[Path] = []
    observed_directories: list[Path] = []
    for directory, directories, files in os.walk(generation_root, topdown=True, followlinks=False):
        base = Path(directory)
        base_stat = base.lstat()
        if (
            not stat.S_ISDIR(base_stat.st_mode)
            or stat.S_ISLNK(base_stat.st_mode)
            or base_stat.st_uid not in allowed_owner_uids
            or base_stat.st_mode & 0o022
        ):
            raise RuntimeCodeGenerationError("runtime generation directory is unsafe")
        for name in directories:
            child = base / name
            child_stat = child.lstat()
            if (
                not stat.S_ISDIR(child_stat.st_mode)
                or stat.S_ISLNK(child_stat.st_mode)
                or child_stat.st_uid not in allowed_owner_uids
                or child_stat.st_mode & 0o022
            ):
                raise RuntimeCodeGenerationError("runtime generation contains a special directory")
            observed_directories.append(child.relative_to(generation_root))
        for name in files:
            observed_files.append((base / name).relative_to(generation_root))
    return (
        tuple(sorted(observed_files, key=lambda path: path.as_posix())),
        tuple(sorted(observed_directories, key=lambda path: path.as_posix())),
    )


def _require_exact_generation_tree(
    generation_root: Path,
    manifest: RuntimeCodeGenerationManifest,
    *,
    expected_uid: int,
) -> None:
    expected_paths = {artifact.path for artifact in manifest.artifacts}
    expected_paths.add("generation-manifest.json")
    expected_directories: set[str] = set()
    for path in expected_paths:
        parent = PurePosixPath(path).parent
        while parent != PurePosixPath("."):
            expected_directories.add(parent.as_posix())
            parent = parent.parent
    observed_files, observed_directories = _walk_generation(
        generation_root,
        allowed_owner_uids=frozenset({0, expected_uid}),
    )
    if {path.as_posix() for path in observed_files} != expected_paths or {
        path.as_posix() for path in observed_directories
    } != expected_directories:
        raise RuntimeCodeGenerationError("runtime generation artifact table is incomplete")


def _require_published_generation(
    generation_root: Path,
    *,
    trusted_base: Path,
    manifest: RuntimeCodeGenerationManifest,
    manifest_bytes: bytes,
    expected_payloads: dict[str, bytes],
    expected_uid: int,
    expected_gid: int,
) -> None:
    expected_modes = {artifact.path: artifact.mode for artifact in manifest.artifacts}
    expected_modes["generation-manifest.json"] = 0o444
    expected_paths = set(expected_payloads)
    expected_directories: set[str] = set()
    for path in expected_paths:
        parent = PurePosixPath(path).parent
        while parent != PurePosixPath("."):
            expected_directories.add(parent.as_posix())
            parent = parent.parent
    observed_files, observed_directories = _walk_generation(
        generation_root,
        allowed_owner_uids=frozenset({0, expected_uid}),
    )
    if {path.as_posix() for path in observed_files} != expected_paths or {
        path.as_posix() for path in observed_directories
    } != expected_directories:
        raise RuntimeCodeGenerationError("published runtime generation tree is not exact")
    for path, expected in expected_payloads.items():
        payload = _read_generation_file(
            generation_root.joinpath(*PurePosixPath(path).parts),
            trusted_base=trusted_base,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            max_bytes=max(1, len(expected) + 1),
            modes=frozenset({expected_modes[path]}),
        )
        if payload != expected:
            raise RuntimeCodeGenerationError("published runtime generation bytes changed")
    parsed_manifest = strict_model_validate_canonical_json(
        RuntimeCodeGenerationManifest,
        manifest_bytes,
    )
    if parsed_manifest != manifest:
        raise RuntimeCodeGenerationError("published runtime generation manifest changed")


def require_attested_runtime_generation(
    *,
    runtime_root: Path,
    trusted_base: Path,
    root_keyring: VerifyOnlyEd25519Keyring,
    runtime_keyring: VerifyOnlyEd25519Keyring,
    promotion_trust: RuntimeCodePromotionTrust,
    expected_uid: int,
    expected_gid: int,
    expected_audience: str,
    expected_installation_id: str,
    expected_target_platform: str,
    now: datetime,
    require_current_promotion: bool = True,
) -> LoadedRuntimeCodeGeneration:
    """Load and independently verify the currently selected immutable generation."""

    try:
        runtime_root = _canonical_absolute_path(runtime_root)
        trusted_base = _canonical_absolute_path(trusted_base)
        generation_id = _read_pointer(
            runtime_root,
            trusted_base=trusted_base,
            name="current",
            expected_uid=expected_uid,
            expected_gid=expected_gid,
        )
        if generation_id is None:
            raise RuntimeCodeGenerationError("runtime generation current pointer is missing")
        generation_root = runtime_root / "generations" / generation_id
        manifest_bytes = _read_generation_file(
            generation_root / "generation-manifest.json",
            trusted_base=trusted_base,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            max_bytes=_MAX_AUTHORITY_BYTES,
        )
        manifest = strict_model_validate_canonical_json(
            RuntimeCodeGenerationManifest,
            manifest_bytes,
        )
        if manifest.generation_id != generation_id:
            raise RuntimeCodeGenerationError("runtime generation id does not match pointer")
        _require_exact_generation_tree(
            generation_root,
            manifest,
            expected_uid=expected_uid,
        )
        payloads: dict[str, bytes] = {}
        for artifact in manifest.artifacts:
            payload = _read_generation_file(
                generation_root.joinpath(*PurePosixPath(artifact.path).parts),
                trusted_base=trusted_base,
                expected_uid=expected_uid,
                expected_gid=expected_gid,
                max_bytes=max(1, artifact.size + 1),
                modes=frozenset({artifact.mode}),
            )
            if (
                len(payload) != artifact.size
                or hashlib.sha256(payload).hexdigest() != artifact.sha256
            ):
                raise RuntimeCodeGenerationError(
                    "runtime generation artifact differs from manifest"
                )
            payloads[artifact.path] = payload
        attestation_bytes = payloads["runtime-code-attestation.json"]
        certificate_bytes = payloads["runtime-code-certificate.json"]
        bundle_bytes = payloads["runtime-code.bundle"]
        receipt_bytes = payloads["promotion-receipt.json"]
        if (
            hashlib.sha256(attestation_bytes).hexdigest() != manifest.attestation_sha256
            or hashlib.sha256(receipt_bytes).hexdigest() != manifest.receipt_sha256
            or hashlib.sha256(bundle_bytes).hexdigest() != manifest.bundle_sha256
        ):
            raise RuntimeCodeGenerationError("runtime generation manifest binding is invalid")
        verified = require_runtime_code_attestation(
            attestation_bytes=attestation_bytes,
            certificate_bytes=certificate_bytes,
            bundle_bytes=bundle_bytes,
            root_keyring=root_keyring,
            runtime_keyring=runtime_keyring,
            expected_audience=expected_audience,
            expected_installation_id=expected_installation_id,
            expected_target_platform=expected_target_platform,
            now=now,
        )
        release_artifacts = tuple(
            artifact for artifact in manifest.artifacts if artifact.path.startswith("release/")
        )
        if _materialized_root(release_artifacts) != manifest.materialized_tree_root_sha256 or tuple(
            (artifact.path, artifact.mode, artifact.size, artifact.sha256)
            for artifact in release_artifacts
        ) != tuple(
            (file.path, file.mode, file.size, file.sha256) for file in verified.attestation.files
        ):
            raise RuntimeCodeGenerationError("runtime materialized tree is not attested")
        candidate = strict_model_validate_canonical_json(RuntimeCodePromotionReceipt, receipt_bytes)
        receipt = require_runtime_code_promotion_receipt(
            receipt_bytes=receipt_bytes,
            trust=promotion_trust,
            attestation_sha256=manifest.attestation_sha256,
            bundle_sha256=verified.bundle.bundle_sha256,
            content_root_sha256=verified.bundle.content_root_sha256,
            installation_id=expected_installation_id,
            target_platform=expected_target_platform,
            minimum_promotion_sequence=candidate.promotion_sequence,
            expected_previous_receipt_sha256=candidate.previous_receipt_sha256,
        )
        if require_current_promotion:
            promotion_trust.require_current_receipt(receipt=receipt)
        if receipt.generation_id != generation_id:
            raise RuntimeCodeGenerationError("runtime generation receipt does not match pointer")
        return LoadedRuntimeCodeGeneration(
            generation_root=generation_root,
            release_root=generation_root / "release",
            manifest=manifest,
            attestation=verified.attestation,
            promotion_receipt=receipt,
            evidence=_evidence(verified, receipt, attestation_bytes),
            material_uid=expected_uid,
            material_gid=expected_gid,
        )
    except RuntimeCodeGenerationError:
        raise
    except (
        AuthorityPathSecurityError,
        RuntimeCodeTrustError,
        StrictJsonError,
        KeyError,
        OSError,
        ValueError,
    ) as exc:
        raise RuntimeCodeGenerationError(f"runtime code generation is invalid: {exc}") from exc


def open_attested_runtime_generation(
    *,
    runtime_root: Path,
    trusted_base: Path,
    root_keyring: VerifyOnlyEd25519Keyring,
    runtime_keyring: VerifyOnlyEd25519Keyring,
    promotion_trust: RuntimeCodePromotionTrust,
    expected_uid: int,
    expected_gid: int,
    expected_audience: str,
    expected_installation_id: str,
    expected_target_platform: str,
    now: datetime,
    fault_hook: FaultHook | None = None,
) -> RuntimeCodeGenerationCapability:
    """Verify a formal generation and retain its selected filesystem identities."""

    def reverify() -> LoadedRuntimeCodeGeneration:
        return require_attested_runtime_generation(
            runtime_root=runtime_root,
            trusted_base=trusted_base,
            root_keyring=root_keyring,
            runtime_keyring=runtime_keyring,
            promotion_trust=promotion_trust,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_audience=expected_audience,
            expected_installation_id=expected_installation_id,
            expected_target_platform=expected_target_platform,
            now=now,
        )

    loaded = reverify()
    manifest_bytes = canonical_model_json_bytes(loaded.manifest)

    def require_authority_paths() -> None:
        try:
            selected = _read_pointer(
                runtime_root,
                trusted_base=trusted_base,
                name="current",
                expected_uid=expected_uid,
                expected_gid=expected_gid,
            )
            observed_manifest = _read_generation_file(
                loaded.generation_root / "generation-manifest.json",
                trusted_base=trusted_base,
                expected_uid=expected_uid,
                expected_gid=expected_gid,
                max_bytes=_MAX_AUTHORITY_BYTES,
            )
        except (AuthorityPathSecurityError, OSError) as exc:
            raise RuntimeCodeGenerationError(
                "runtime generation authority path is unsafe"
            ) from exc
        if (
            selected != loaded.evidence.generation_id
            or observed_manifest != manifest_bytes
        ):
            raise RuntimeCodeGenerationError("runtime generation authority path changed")

    def require_exact_tree() -> None:
        _require_exact_generation_tree(
            loaded.generation_root,
            loaded.manifest,
            expected_uid=loaded.material_uid,
        )

    def require_current_promotion() -> None:
        promotion_trust.require_current_receipt(receipt=loaded.promotion_receipt)

    pointer_lease: SecureRegularFileLease | None = None
    artifact_leases: list[SecureRegularFileLease] = []
    try:
        pointer_lease = open_secure_regular_file_lease(
            runtime_root / "current",
            trusted_root=trusted_base,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            allowed_modes=frozenset({0o400, 0o440, 0o444}),
            max_bytes=_POINTER_BYTES,
        )
        expected_pointer = f"{loaded.evidence.generation_id}\n".encode("ascii")
        if pointer_lease.read_all(max_bytes=_POINTER_BYTES) != expected_pointer:
            raise RuntimeCodeGenerationError("runtime generation changed while opening")
        generation_files = [
            ("generation-manifest.json", 0o444, manifest_bytes),
        ]
        for artifact in loaded.manifest.artifacts:
            generation_files.append(
                (
                    artifact.path,
                    artifact.mode,
                    _read_generation_file(
                        loaded.generation_root.joinpath(*PurePosixPath(artifact.path).parts),
                        trusted_base=trusted_base,
                        expected_uid=expected_uid,
                        expected_gid=expected_gid,
                        max_bytes=max(1, artifact.size + 1),
                        modes=frozenset({artifact.mode}),
                    ),
                )
            )
        for path, mode, expected_payload in generation_files:
            lease = open_secure_regular_file_lease(
                loaded.generation_root.joinpath(*PurePosixPath(path).parts),
                trusted_root=trusted_base,
                expected_uid=expected_uid,
                expected_gid=expected_gid,
                allowed_modes=frozenset({mode}),
                max_bytes=max(1, len(expected_payload) + 1),
                min_bytes=0,
            )
            artifact_leases.append(lease)
            if lease.read_all(max_bytes=max(1, len(expected_payload) + 1)) != expected_payload:
                raise RuntimeCodeGenerationError("runtime generation changed while leasing")
        if fault_hook is not None:
            fault_hook("capability:after-verify")
        capability = RuntimeCodeGenerationCapability(
            loaded=loaded,
            pointer_lease=pointer_lease,
            artifact_leases=tuple(artifact_leases),
            require_authority_paths=require_authority_paths,
            require_exact_tree=require_exact_tree,
            require_current_promotion=require_current_promotion,
            audit_events=(
                "pointer-verified",
                "attestation-verified",
                "promotion-current-verified",
                "generation-verified",
                "execution-binding-pending",
            ),
        )
        capability.require_live()
        return capability
    except Exception:
        for lease in reversed(artifact_leases):
            lease.close()
        if pointer_lease is not None:
            pointer_lease.close()
        raise


__all__ = [
    "LoadedRuntimeCodeGeneration",
    "RuntimeCodeCollectFile",
    "RuntimeCodeGenerationError",
    "RuntimeCodeGenerationCapability",
    "RuntimeCodeGenerationInstaller",
    "RuntimeCodeInstallReceipt",
    "RuntimeCodeInstallRequest",
    "collect_runtime_code_bundle",
    "open_attested_runtime_generation",
    "require_attested_runtime_generation",
]
