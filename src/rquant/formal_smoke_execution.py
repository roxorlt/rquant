"""Outer verifier and descriptor launcher for formal smoke execution."""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from collections.abc import Callable, Mapping
from contextlib import suppress
from datetime import date
from pathlib import Path, PurePosixPath

from pydantic import Field, ValidationError

from rquant.authority_path_security import (
    AuthorityPathSecurityError,
    SecureRegularFileLease,
    open_secure_regular_file_lease,
)
from rquant.formal_runtime import (
    FORMAL_SMOKE_BOOTSTRAP_SHA256,
    FormalRuntimeError,
    FormalRuntimeSession,
    bind_formal_smoke_runtime,
    exec_formal_smoke_child,
)
from rquant.formal_smoke_protocol import (
    FormalSmokeArtifactReceipt,
    FormalSmokeAttestedReplayResult,
    FormalSmokeBootstrapReference,
    FormalSmokeExecutionIdentity,
    FormalSmokeExecutionReceipt,
    FormalSmokeExecutionRequest,
    FormalSmokeStrategy,
    formal_smoke_receipt_digest,
    formal_smoke_request_digest,
)
from rquant.runtime_code_generation import (
    RuntimeCodeGenerationCapability,
    RuntimeCodeGenerationError,
)
from rquant.runtime_contracts import RuntimeContractModel
from rquant.strict_json import (
    StrictJsonError,
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
)

_MAX_RECEIPT_BYTES = 8 * 1024 * 1024


class FormalSmokeExecutionError(RuntimeError):
    """The attested generation did not produce a verifiable formal smoke result."""


class FormalSmokeChildProcessResult(RuntimeContractModel):
    exit_code: int = Field(strict=True, ge=0, le=255)
    receipt_bytes: bytes = Field(max_length=_MAX_RECEIPT_BYTES)


FormalSmokeExchange = Callable[
    [FormalRuntimeSession, bytes],
    FormalSmokeChildProcessResult,
]


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise FormalSmokeExecutionError("formal smoke request write failed")
        view = view[written:]


def _read_bounded(descriptor: int) -> bytes:
    payload = bytearray()
    while True:
        chunk = os.read(descriptor, min(64 * 1024, _MAX_RECEIPT_BYTES + 1 - len(payload)))
        if not chunk:
            return bytes(payload)
        payload.extend(chunk)
        if len(payload) > _MAX_RECEIPT_BYTES:
            raise FormalSmokeExecutionError("formal smoke receipt exceeds the limit")


def _wait_exit_code(process_id: int) -> int:
    while True:
        try:
            waited, status = os.waitpid(process_id, 0)
            break
        except InterruptedError:
            continue
    if waited != process_id or not os.WIFEXITED(status):
        return 255
    return os.WEXITSTATUS(status)


def _exchange_formal_smoke_child(
    session: FormalRuntimeSession,
    request_bytes: bytes,
) -> FormalSmokeChildProcessResult:
    request_read, request_write = os.pipe()
    receipt_read, receipt_write = os.pipe()
    try:
        process_id = os.fork()
    except BaseException:
        for descriptor in (request_read, request_write, receipt_read, receipt_write):
            os.close(descriptor)
        raise
    if process_id == 0:  # pragma: no cover - exact execution is asserted by Linux FD tests
        try:
            os.close(request_write)
            os.close(receipt_read)
            exec_formal_smoke_child(
                session,
                request_descriptor=request_read,
                receipt_descriptor=receipt_write,
            )
        except BaseException:
            os._exit(126)
        os._exit(0)

    os.close(request_read)
    os.close(receipt_write)
    receipt = b""
    try:
        _write_all(request_write, request_bytes)
        os.close(request_write)
        request_write = -1
        receipt = _read_bounded(receipt_read)
    finally:
        for descriptor in (request_write, receipt_read):
            with suppress(OSError):
                os.close(descriptor)
        exit_code = _wait_exit_code(process_id)
    return FormalSmokeChildProcessResult(exit_code=exit_code, receipt_bytes=receipt)


def _require_output_root(output_dir: Path) -> Path:
    output = Path(output_dir)
    if not output.is_absolute() or output != Path(os.path.abspath(output)):
        raise FormalSmokeExecutionError("formal smoke output directory must be canonical absolute")
    if not output.exists():
        output.mkdir(mode=0o700, parents=True)
    try:
        observed = output.lstat()
        physical = output.resolve(strict=True)
    except OSError as exc:
        raise FormalSmokeExecutionError("formal smoke output directory is unavailable") from exc
    if (
        physical != output
        or not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_mode & 0o022
    ):
        raise FormalSmokeExecutionError("formal smoke output directory is unsafe")
    return output


def _execution_identity(
    capability: RuntimeCodeGenerationCapability,
) -> FormalSmokeExecutionIdentity:
    capability.require_live()
    loaded = capability.loaded
    spec = loaded.attestation.execution_spec
    files = tuple(sorted(loaded.attestation.files, key=lambda item: item.path))
    by_path = {file.path: file for file in files}
    try:
        launcher = by_path[spec.launcher_path]
        interpreter = by_path[spec.interpreter_path]
    except KeyError as exc:  # signed attestation validation should already reject this
        raise FormalSmokeExecutionError("formal smoke execution file is missing") from exc
    return FormalSmokeExecutionIdentity(
        generation_id=capability.evidence.generation_id,
        generation_root=loaded.generation_root,
        material_uid=loaded.material_uid,
        material_gid=loaded.material_gid,
        launcher=launcher,
        interpreter=interpreter,
        working_directory=spec.working_directory,
        import_roots=spec.import_roots,
        python_abi=spec.python_abi,
        bootstrap_sha256=FORMAL_SMOKE_BOOTSTRAP_SHA256,
        code_files=files,
    )


def _validate_receipt(
    request: FormalSmokeExecutionRequest,
    process_result: FormalSmokeChildProcessResult,
) -> FormalSmokeExecutionReceipt:
    if process_result.exit_code != 0:
        raise FormalSmokeExecutionError(
            f"formal smoke child failed with exit code {process_result.exit_code}"
        )
    if not process_result.receipt_bytes:
        raise FormalSmokeExecutionError("formal smoke child returned no receipt")
    try:
        receipt = strict_model_validate_canonical_json(
            FormalSmokeExecutionReceipt,
            process_result.receipt_bytes,
        )
    except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
        raise FormalSmokeExecutionError("formal smoke receipt is invalid") from exc
    if receipt.request_digest != formal_smoke_request_digest(request):
        raise FormalSmokeExecutionError("formal smoke receipt request digest mismatch")
    if receipt.code_trust_evidence != request.code_trust_evidence:
        raise FormalSmokeExecutionError("formal smoke receipt evidence mismatch")
    if receipt.execution_identity != request.execution_identity:
        raise FormalSmokeExecutionError("formal smoke receipt execution identity mismatch")
    expected_result = {
        "strategy": request.strategy,
        "audit_run_id": request.audit_run_id,
        "dataset_snapshot_id": request.dataset_snapshot_id,
        "dataset_binding_hash": request.dataset_binding_hash,
        "code_commit": request.code_commit,
        "missing_evidence": (),
    }
    for field_name, expected in expected_result.items():
        if getattr(receipt.result, field_name) != expected:
            raise FormalSmokeExecutionError(f"formal smoke receipt {field_name} mismatch")
    return receipt


def _mark_verified_execution(
    capability: RuntimeCodeGenerationCapability,
    binding_digest: str,
) -> None:
    capability._mark_verified_execution(binding_digest)


def _require_live_session(session: FormalRuntimeSession) -> None:
    session.require_live()


def _verify_staged_artifacts(
    request: FormalSmokeExecutionRequest,
    receipt: FormalSmokeExecutionReceipt,
) -> tuple[tuple[Path, SecureRegularFileLease], ...]:
    verified: list[tuple[Path, SecureRegularFileLease]] = []
    try:
        for artifact in receipt.artifacts:
            relative = PurePosixPath(artifact.relative_path)
            staged = request.staging_root.joinpath(*relative.parts)
            lease: SecureRegularFileLease | None = None
            try:
                lease = open_secure_regular_file_lease(
                    staged,
                    trusted_root=request.staging_root,
                    expected_uid=os.getuid(),
                    expected_gid=os.getgid(),
                    allowed_modes=frozenset({0o600, 0o640, 0o644}),
                    max_bytes=max(1, artifact.size + 1),
                )
                payload = lease.read_all(max_bytes=max(1, artifact.size + 1))
                if (
                    len(payload) != artifact.size
                    or hashlib.sha256(payload).hexdigest() != artifact.sha256
                ):
                    raise FormalSmokeExecutionError("formal smoke artifact digest mismatch")
                verified.append((staged, lease))
                lease = None
            finally:
                if lease is not None:
                    lease.close()
        return tuple(verified)
    except BaseException:
        for _path, opened in reversed(verified):
            opened.close()
        raise


def _publish_artifacts(
    request: FormalSmokeExecutionRequest,
    receipt: FormalSmokeExecutionReceipt,
    staged: tuple[tuple[Path, SecureRegularFileLease], ...],
) -> tuple[Path, Path]:
    destination_dir = request.artifact_root / "strategy_lab_runs"
    destination_dir.mkdir(mode=0o755, exist_ok=True)
    observed = destination_dir.lstat()
    if (
        destination_dir.resolve(strict=True) != destination_dir
        or not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_mode & 0o022
    ):
        raise FormalSmokeExecutionError("formal smoke publication directory is unsafe")
    destinations = tuple(
        request.artifact_root.joinpath(*PurePosixPath(artifact.relative_path).parts)
        for artifact in receipt.artifacts
    )
    published: list[Path] = []
    try:
        for (source, source_lease), destination, artifact in zip(
            staged,
            destinations,
            receipt.artifacts,
            strict=True,
        ):
            source_lease.require_unchanged()
            source_descriptor = source_lease.fileno()
            source_identity = os.fstat(source_descriptor)
            os.link(source, destination, follow_symlinks=False)
            published.append(destination)
            _verify_linked_artifact(
                destination,
                source_descriptor,
                source_identity,
                artifact,
            )
            _require_linked_source(source, source_descriptor, source_identity)
        for source, _lease in staged:
            source.unlink()
    except BaseException:
        for destination in reversed(published):
            destination.unlink(missing_ok=True)
        raise
    finally:
        for _source, lease in reversed(staged):
            lease.close()
    return destinations[0], destinations[1]


def _verify_linked_artifact(
    destination: Path,
    source_descriptor: int,
    source_identity: os.stat_result,
    artifact: FormalSmokeArtifactReceipt,
) -> None:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(destination, flags)
    try:
        source = os.fstat(source_descriptor)
        linked = os.fstat(descriptor)
        if (
            not stat.S_ISREG(linked.st_mode)
            or stat.S_IMODE(linked.st_mode) not in {0o600, 0o640, 0o644}
            or linked.st_uid != os.getuid()
            or linked.st_gid != os.getgid()
            or linked.st_nlink != 2
            or (linked.st_dev, linked.st_ino) != (source_identity.st_dev, source_identity.st_ino)
            or (source.st_dev, source.st_ino) != (source_identity.st_dev, source_identity.st_ino)
            or linked.st_size != artifact.size
        ):
            raise FormalSmokeExecutionError("formal smoke published artifact identity mismatch")
        payload = bytearray()
        while len(payload) <= artifact.size:
            chunk = os.read(descriptor, min(64 * 1024, artifact.size + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        if len(payload) != artifact.size or hashlib.sha256(payload).hexdigest() != artifact.sha256:
            raise FormalSmokeExecutionError("formal smoke published artifact digest mismatch")
    finally:
        os.close(descriptor)


def _require_linked_source(
    source: Path,
    descriptor: int,
    expected: os.stat_result,
) -> None:
    named = os.stat(source, follow_symlinks=False)
    opened = os.fstat(descriptor)
    expected_stable = (
        expected.st_dev,
        expected.st_ino,
        expected.st_mode,
        expected.st_uid,
        expected.st_gid,
        expected.st_size,
        expected.st_mtime_ns,
    )
    for observed in (named, opened):
        if (
            observed.st_dev,
            observed.st_ino,
            observed.st_mode,
            observed.st_uid,
            observed.st_gid,
            observed.st_size,
            observed.st_mtime_ns,
        ) != expected_stable or observed.st_nlink != 2:
            raise FormalSmokeExecutionError("formal smoke staged artifact changed during publish")


def _run_attested_formal_smoke(
    capability: RuntimeCodeGenerationCapability,
    *,
    strategy: FormalSmokeStrategy,
    start_date: date,
    end_date: date,
    audit_run_id: str,
    dataset_snapshot_id: str,
    dataset_binding_hash: str,
    output_dir: Path,
    bootstrap_reference: FormalSmokeBootstrapReference,
    environment_source: Mapping[str, str],
    exchange: FormalSmokeExchange = _exchange_formal_smoke_child,
) -> FormalSmokeAttestedReplayResult:
    """Launch generation A and accept only its fully bound canonical receipt."""

    session: FormalRuntimeSession | None = None
    try:
        if not isinstance(capability, RuntimeCodeGenerationCapability):
            raise FormalSmokeExecutionError(
                "formal smoke requires an attested generation capability"
            )
        capability.require_live()
        output = _require_output_root(output_dir)
        with tempfile.TemporaryDirectory(prefix=".formal-smoke-", dir=output) as raw_staging:
            staging = Path(raw_staging)
            staging.chmod(0o700)
            request = FormalSmokeExecutionRequest(
                strategy=strategy,
                start_date=start_date,
                end_date=end_date,
                audit_run_id=audit_run_id,
                dataset_snapshot_id=dataset_snapshot_id,
                dataset_binding_hash=dataset_binding_hash,
                code_commit=capability.evidence.provenance_commit,
                code_trust_evidence=capability.evidence,
                execution_identity=_execution_identity(capability),
                bootstrap_reference=bootstrap_reference,
                artifact_root=output,
                staging_root=staging,
            )
            session = bind_formal_smoke_runtime(
                capability,
                environment_source=environment_source,
            )
            process_result = exchange(session, canonical_model_json_bytes(request))
            receipt = _validate_receipt(request, process_result)
            _require_live_session(session)
            staged = _verify_staged_artifacts(request, receipt)
            _require_live_session(session)
            binding_digest = formal_smoke_receipt_digest(receipt)
            result_values = receipt.result.model_dump(mode="python")
            expected_paths = tuple(
                output.joinpath(*PurePosixPath(artifact.relative_path).parts)
                for artifact in receipt.artifacts
            )
            accepted = FormalSmokeAttestedReplayResult(
                **result_values,
                json_path=expected_paths[0],
                markdown_path=expected_paths[1],
                execution_receipt=receipt,
                execution_receipt_digest=binding_digest,
            )
            json_path, markdown_path = _publish_artifacts(request, receipt, staged)
            try:
                _require_live_session(session)
                _mark_verified_execution(capability, binding_digest)
            except BaseException:
                markdown_path.unlink(missing_ok=True)
                json_path.unlink(missing_ok=True)
                raise
            return accepted
    except FormalSmokeExecutionError:
        raise
    except (
        AuthorityPathSecurityError,
        FormalRuntimeError,
        OSError,
        RuntimeCodeGenerationError,
        StrictJsonError,
        TypeError,
        ValidationError,
        ValueError,
    ) as exc:
        raise FormalSmokeExecutionError("formal smoke attested execution failed") from exc
    finally:
        if session is not None:
            session.close()
        else:
            capability.close()


def run_attested_formal_smoke(
    capability: RuntimeCodeGenerationCapability,
    *,
    strategy: FormalSmokeStrategy,
    start_date: date,
    end_date: date,
    audit_run_id: str,
    dataset_snapshot_id: str,
    dataset_binding_hash: str,
    output_dir: Path,
    bootstrap_reference: FormalSmokeBootstrapReference,
    environment_source: Mapping[str, str],
) -> FormalSmokeAttestedReplayResult:
    """Launch only through the built-in verified descriptor transport."""

    return _run_attested_formal_smoke(
        capability,
        strategy=strategy,
        start_date=start_date,
        end_date=end_date,
        audit_run_id=audit_run_id,
        dataset_snapshot_id=dataset_snapshot_id,
        dataset_binding_hash=dataset_binding_hash,
        output_dir=output_dir,
        bootstrap_reference=bootstrap_reference,
        environment_source=environment_source,
        exchange=_exchange_formal_smoke_child,
    )


__all__ = [
    "FormalSmokeChildProcessResult",
    "FormalSmokeExecutionError",
    "run_attested_formal_smoke",
]
