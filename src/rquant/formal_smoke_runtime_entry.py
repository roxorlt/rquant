"""Generation-internal entry for one attested formal smoke request."""

from __future__ import annotations

import hashlib
import os
import stat
import sys
import time
from pathlib import Path, PurePosixPath

from rquant.authority_path_security import open_secure_regular_file_lease
from rquant.formal_runtime import FORMAL_SMOKE_BOOTSTRAP_SHA256
from rquant.formal_smoke_protocol import (
    FormalSmokeArtifactReceipt,
    FormalSmokeExecutionReceipt,
    FormalSmokeExecutionRequest,
    FormalSmokeReplayPayload,
    FormalSmokeReplayRequest,
    formal_smoke_request_digest,
    formal_smoke_result_digest,
)
from rquant.strict_json import (
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
)

_MAX_REQUEST_BYTES = 8 * 1024 * 1024


class FormalSmokeGenerationEntryError(RuntimeError):
    """The child request is not bound to this attested generation process."""


def _read_private_pipe(descriptor: int) -> bytes:
    observed = os.fstat(descriptor)
    if not stat.S_ISFIFO(observed.st_mode) or observed.st_uid != os.getuid():
        raise FormalSmokeGenerationEntryError("formal smoke request descriptor is not private")
    payload = bytearray()
    while True:
        chunk = os.read(descriptor, min(64 * 1024, _MAX_REQUEST_BYTES + 1 - len(payload)))
        if not chunk:
            break
        payload.extend(chunk)
        if len(payload) > _MAX_REQUEST_BYTES:
            raise FormalSmokeGenerationEntryError("formal smoke request exceeds the limit")
    if not payload:
        raise FormalSmokeGenerationEntryError("formal smoke request is missing")
    return bytes(payload)


def _write_private_pipe(descriptor: int, payload: bytes) -> None:
    observed = os.fstat(descriptor)
    if not stat.S_ISFIFO(observed.st_mode) or observed.st_uid != os.getuid():
        raise FormalSmokeGenerationEntryError("formal smoke receipt descriptor is not private")
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise FormalSmokeGenerationEntryError("formal smoke receipt write failed")
        view = view[written:]


def _require_private_directory(path: Path, *, mode: int | None = None) -> None:
    try:
        observed = path.lstat()
        physical = path.resolve(strict=True)
    except OSError as exc:
        raise FormalSmokeGenerationEntryError(
            "formal smoke artifact directory is unavailable"
        ) from exc
    if (
        physical != path
        or not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_mode & 0o022
        or (mode is not None and stat.S_IMODE(observed.st_mode) != mode)
    ):
        raise FormalSmokeGenerationEntryError("formal smoke artifact directory is unsafe")


def _verify_attested_file(request: FormalSmokeExecutionRequest, path: Path) -> None:
    identity = request.execution_identity
    try:
        relative = path.relative_to(identity.generation_root).as_posix()
    except ValueError as exc:
        raise FormalSmokeGenerationEntryError(
            "formal smoke imported code escaped the generation"
        ) from exc
    descriptor = {file.path: file for file in identity.code_files}.get(relative)
    if descriptor is None:
        raise FormalSmokeGenerationEntryError("formal smoke imported code is not attested")
    with open_secure_regular_file_lease(
        path,
        trusted_root=identity.generation_root,
        expected_uid=identity.material_uid,
        expected_gid=identity.material_gid,
        allowed_modes=frozenset({descriptor.mode}),
        max_bytes=max(1, descriptor.size + 1),
    ) as lease:
        payload = lease.read_all(max_bytes=max(1, descriptor.size + 1))
    if len(payload) != descriptor.size or hashlib.sha256(payload).hexdigest() != descriptor.sha256:
        raise FormalSmokeGenerationEntryError("formal smoke imported code digest is invalid")


def _verify_loaded_generation_modules(request: FormalSmokeExecutionRequest) -> None:
    roots = tuple(
        request.execution_identity.generation_root.joinpath(*PurePosixPath(root).parts)
        for root in request.execution_identity.import_roots
    )
    required = {
        "rquant.formal_smoke_protocol",
        "rquant.formal_smoke_runtime_entry",
    }
    seen: set[str] = set()
    for name, module in tuple(sys.modules.items()):
        if name != "rquant" and not name.startswith("rquant."):
            continue
        raw_path = getattr(module, "__file__", None)
        if raw_path is None:
            continue
        try:
            path = Path(raw_path).resolve(strict=True)
        except OSError as exc:
            raise FormalSmokeGenerationEntryError(
                "formal smoke imported module is unavailable"
            ) from exc
        if not any(path.is_relative_to(root) for root in roots):
            raise FormalSmokeGenerationEntryError(
                "formal smoke imported rQuant module escaped attested import roots"
            )
        _verify_attested_file(request, path)
        seen.add(name)
    if not required.issubset(seen):
        raise FormalSmokeGenerationEntryError("formal smoke generation entry is not attested")


def _validate_execution_context(request: FormalSmokeExecutionRequest) -> None:
    identity = request.execution_identity
    expected_interpreter = identity.generation_root.joinpath(
        *PurePosixPath(identity.interpreter.path).parts
    )
    expected_working_directory = identity.generation_root.joinpath(
        *PurePosixPath(identity.working_directory).parts
    )
    expected_import_roots = tuple(
        str(identity.generation_root.joinpath(*PurePosixPath(root).parts))
        for root in identity.import_roots
    )
    if identity.bootstrap_sha256 != FORMAL_SMOKE_BOOTSTRAP_SHA256:
        raise FormalSmokeGenerationEntryError("formal smoke bootstrap identity is invalid")
    if Path(sys.executable) != expected_interpreter:
        raise FormalSmokeGenerationEntryError("formal smoke interpreter identity is invalid")
    if Path.cwd() != expected_working_directory:
        raise FormalSmokeGenerationEntryError("formal smoke working directory is invalid")
    if tuple(sys.path[: len(expected_import_roots)]) != expected_import_roots:
        raise FormalSmokeGenerationEntryError("formal smoke import roots are not active")
    _require_private_directory(request.artifact_root)
    _require_private_directory(request.staging_root, mode=0o700)
    _verify_loaded_generation_modules(request)


def _artifact_receipt(
    request: FormalSmokeExecutionRequest,
    *,
    kind: str,
    final_path: Path,
) -> FormalSmokeArtifactReceipt:
    try:
        relative = final_path.relative_to(request.artifact_root)
    except ValueError as exc:
        raise FormalSmokeGenerationEntryError("formal smoke result path escaped output") from exc
    staged = request.staging_root / relative
    with open_secure_regular_file_lease(
        staged,
        trusted_root=request.staging_root,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        allowed_modes=frozenset({0o600, 0o640, 0o644}),
        max_bytes=64 * 1024 * 1024,
    ) as lease:
        payload = lease.read_all(max_bytes=64 * 1024 * 1024)
    return FormalSmokeArtifactReceipt(
        kind=kind,
        relative_path=relative.as_posix(),
        size=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )


def run_formal_smoke_generation_entry(*, request_fd: int, receipt_fd: int) -> int:
    """Validate, execute, persist, and receipt one request inside generation A."""

    request = strict_model_validate_canonical_json(
        FormalSmokeExecutionRequest,
        _read_private_pipe(request_fd),
    )
    _validate_execution_context(request)

    from rquant.formal_runtime_composition import open_formal_runtime_capability
    from rquant.formal_smoke_execution import _execution_identity
    from rquant.formal_smoke_replay import run_formal_smoke_replay

    reference = request.bootstrap_reference
    capability = open_formal_runtime_capability(
        configuration_path=reference.configuration_path,
        trusted_base=reference.trusted_base,
        expected_authority_uid=reference.expected_authority_uid,
        expected_authority_gid=reference.expected_authority_gid,
        startup_deadline_monotonic=time.monotonic() + 30,
    )
    try:
        capability.require_live()
        if (
            capability.evidence != request.code_trust_evidence
            or _execution_identity(capability) != request.execution_identity
        ):
            raise FormalSmokeGenerationEntryError(
                "formal smoke child capability does not match request"
            )
        business_request = FormalSmokeReplayRequest(
            strategy=request.strategy,
            start_date=request.start_date,
            end_date=request.end_date,
            audit_run_id=request.audit_run_id,
            dataset_snapshot_id=request.dataset_snapshot_id,
            dataset_binding_hash=request.dataset_binding_hash,
            code_commit=request.code_commit,
            runtime_capability=capability,
        )
        result = run_formal_smoke_replay(
            business_request,
            base_dir=request.artifact_root,
            staging_base_dir=request.staging_root,
        )
        capability.require_live()
        _verify_loaded_generation_modules(request)
        payload = FormalSmokeReplayPayload.model_validate(
            result.model_dump(mode="python", exclude={"json_path", "markdown_path"})
        )
        artifacts = (
            _artifact_receipt(
                request,
                kind="json",
                final_path=result.json_path,
            ),
            _artifact_receipt(
                request,
                kind="markdown",
                final_path=result.markdown_path,
            ),
        )
        receipt = FormalSmokeExecutionReceipt(
            code_trust_evidence=request.code_trust_evidence,
            request_digest=formal_smoke_request_digest(request),
            execution_identity=request.execution_identity,
            result=payload,
            artifacts=artifacts,
            result_digest=formal_smoke_result_digest(payload, artifacts),
        )
        capability.require_live()
        _write_private_pipe(receipt_fd, canonical_model_json_bytes(receipt))
        return 0
    finally:
        capability.close()


__all__ = [
    "FormalSmokeGenerationEntryError",
    "run_formal_smoke_generation_entry",
]
