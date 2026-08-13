"""Reviewed command adapters for the daily-close DAG control plane.

The manifest is intentionally small: it declares which bounded executable is
allowed to own each stage.  The executable receives a durable idempotency key
and must use the fenced child helper to atomically publish a signed
``ExternalStageReceipt``.  The parent never treats an exit code as success; it
commits only a fully bound receipt it can verify after a restart.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import stat
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal, Self
from uuid import uuid4

from pydantic import Field, field_validator, model_validator

from rquant.daily_pipeline_ledger import (
    DailyPipelineLedger,
    DailyPipelineLedgerError,
    DailyPipelineMode,
    DailyPipelineStorageBinding,
    DailyPipelineStorageProfile,
    DailyStageEffectIntent,
    StageResult,
)
from rquant.daily_pipeline_orchestrator import (
    DailyStageExecutionContext,
    DailyStageHealth,
    DailyStageProcessSpec,
)
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.strict_json import (
    StrictJsonError,
    canonical_model_json_bytes,
    strict_canonical_json_loads,
)

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
StageId = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")]
_MAX_RECEIPT_BYTES = 64 * 1024
_KEY_ID_PATTERN = r"^[A-Za-z0-9._-]{1,128}$"


class DailyPipelineCommandManifestError(RuntimeError):
    """A reviewed stage command or its receipt fails closed."""


@dataclass(frozen=True)
class DailyExternalReceiptKey:
    """HMAC capability held by approved stage children and receipt verifiers."""

    key_id: str
    secret: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if re.fullmatch(_KEY_ID_PATTERN, self.key_id) is None:
            raise ValueError("daily external receipt key id is invalid")
        if not isinstance(self.secret, bytes) or not 32 <= len(self.secret) <= 4_096:
            raise ValueError("daily external receipt secret must be 32..4096 bytes")


DailyExternalReceiptKeyProvider = Callable[[str], DailyExternalReceiptKey | None]


class ExternalStageReceipt(RuntimeContractModel):
    """Signed proof that one fenced child committed one prepared effect."""

    contract: Literal["daily-external-stage-receipt/v2"] = "daily-external-stage-receipt/v2"
    receipt_id: Sha256
    mode: DailyPipelineMode
    run_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    stage_id: StageId
    idempotency_key: Sha256
    fencing_token: int = Field(ge=1)
    lease_expiry: AwareUtcDatetime
    source_generation_id: Sha256
    source_content_hash: Sha256
    command_manifest_hash: Sha256
    effect_id: Sha256
    result: StageResult
    issued_at: AwareUtcDatetime
    key_id: str = Field(pattern=_KEY_ID_PATTERN)
    signature: Sha256

    @model_validator(mode="after")
    def bind_receipt(self) -> Self:
        object.__setattr__(self, "lease_expiry", normalize_aware_utc(self.lease_expiry))
        object.__setattr__(self, "issued_at", normalize_aware_utc(self.issued_at))
        if self.issued_at >= self.lease_expiry:
            raise ValueError("external stage receipt was issued outside its lease")
        expected = canonical_sha256(
            self.model_dump(mode="python", exclude={"receipt_id", "signature"})
        )
        if not hmac.compare_digest(self.receipt_id, expected):
            raise ValueError("external stage receipt id does not match canonical content")
        return self

    @classmethod
    def signed(
        cls,
        *,
        run_id: str,
        mode: DailyPipelineMode,
        stage_id: str,
        idempotency_key: str,
        fencing_token: int,
        lease_expiry: datetime,
        source_generation_id: str,
        source_content_hash: str,
        command_manifest_hash: str,
        effect_id: str,
        result: StageResult,
        issued_at: datetime,
        signing_key: DailyExternalReceiptKey,
    ) -> ExternalStageReceipt:
        payload = {
            "contract": "daily-external-stage-receipt/v2",
            "mode": mode,
            "run_id": run_id,
            "stage_id": stage_id,
            "idempotency_key": idempotency_key,
            "fencing_token": fencing_token,
            "lease_expiry": normalize_aware_utc(lease_expiry),
            "source_generation_id": source_generation_id,
            "source_content_hash": source_content_hash,
            "command_manifest_hash": command_manifest_hash,
            "effect_id": effect_id,
            "result": StageResult.model_validate(result),
            "issued_at": normalize_aware_utc(issued_at),
            "key_id": signing_key.key_id,
        }
        receipt_id = canonical_sha256(payload)
        signature = hmac.new(
            signing_key.secret,
            receipt_id.encode("ascii"),
            hashlib.sha256,
        ).hexdigest()
        return cls(receipt_id=receipt_id, signature=signature, **payload)

    def verify(self, key_provider: DailyExternalReceiptKeyProvider) -> None:
        key = key_provider(self.key_id)
        if key is None or key.key_id != self.key_id:
            raise DailyPipelineCommandManifestError(
                "external stage receipt signature key is not trusted"
            )
        expected = hmac.new(
            key.secret,
            self.receipt_id.encode("ascii"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(self.signature, expected):
            raise DailyPipelineCommandManifestError("external stage receipt signature is invalid")


def _absolute_normalized(path: Path, *, label: str) -> Path:
    candidate = Path(path)
    normalized = Path(os.path.normpath(os.fspath(candidate)))
    if not candidate.is_absolute() or candidate != normalized:
        raise ValueError(f"{label} must be an absolute normalized path")
    return candidate


def _safe_read(
    storage_profile: DailyPipelineStorageProfile,
    *,
    leaf: Literal["receipts", "control"],
    path: Path,
    label: str,
    max_bytes: int,
) -> bytes:
    profile = DailyPipelineStorageProfile.model_validate(storage_profile)
    expected_root = (
        profile.receipt_root if leaf == "receipts" else profile.command_manifest_path.parent
    )
    if path.parent != expected_root:
        raise DailyPipelineCommandManifestError(f"{label} escapes its storage profile")
    try:
        with DailyPipelineStorageBinding.open(profile, leaf=leaf) as binding:
            return _safe_read_bound(
                binding,
                path.name,
                label=label,
                max_bytes=max_bytes,
            )
    except DailyPipelineLedgerError as exc:
        raise DailyPipelineCommandManifestError(f"{label} storage binding is unsafe") from exc


def _safe_read_bound(
    binding: DailyPipelineStorageBinding,
    name: str,
    *,
    label: str,
    max_bytes: int,
) -> bytes:
    descriptor = -1
    try:
        binding.assert_current()
        before = os.stat(name, dir_fd=binding.leaf_fd, follow_symlinks=False)
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_size > max_bytes
        ):
            raise DailyPipelineCommandManifestError(f"{label} is unsafe")
        descriptor = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=binding.leaf_fd,
        )
        opened = os.fstat(descriptor)
        current = os.stat(name, dir_fd=binding.leaf_fd, follow_symlinks=False)
        if (before.st_dev, before.st_ino, before.st_size) != (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
        ) or (current.st_dev, current.st_ino, current.st_size) != (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
        ):
            raise DailyPipelineCommandManifestError(f"{label} changed while opening")
        payload = os.read(descriptor, max_bytes + 1)
        if len(payload) > max_bytes:
            raise DailyPipelineCommandManifestError(f"{label} exceeds size limit")
        after = os.fstat(descriptor)
        final = os.stat(name, dir_fd=binding.leaf_fd, follow_symlinks=False)
        if (after.st_dev, after.st_ino, after.st_size) != (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
        ) or (final.st_dev, final.st_ino, final.st_size) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
        ):
            raise DailyPipelineCommandManifestError(f"{label} changed while reading")
        binding.assert_current()
        return payload
    except FileNotFoundError:
        raise
    except DailyPipelineCommandManifestError:
        raise
    except OSError as exc:
        raise DailyPipelineCommandManifestError(f"{label} cannot be read safely") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _receipt_key_from_environment(key_id: str) -> DailyExternalReceiptKey | None:
    configured_id = os.environ.get("RQUANT_DAILY_EXTERNAL_RECEIPT_KEY_ID", "").strip()
    secret = os.environ.get("RQUANT_DAILY_EXTERNAL_RECEIPT_SECRET", "").encode("utf-8")
    if not configured_id and not secret:
        return None
    if configured_id != key_id or not secret:
        return None
    return DailyExternalReceiptKey(key_id=configured_id, secret=secret)


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise DailyPipelineCommandManifestError(
            f"external stage receipt environment is missing {name}"
        )
    return value


def _write_receipt_once(
    storage_profile: DailyPipelineStorageProfile,
    path: Path,
    payload: bytes,
) -> None:
    if len(payload) > _MAX_RECEIPT_BYTES:
        raise DailyPipelineCommandManifestError("external stage receipt exceeds size limit")
    profile = DailyPipelineStorageProfile.model_validate(storage_profile)
    if path.parent != profile.receipt_root:
        raise DailyPipelineCommandManifestError(
            "external stage receipt escapes its storage profile"
        )
    try:
        binding = DailyPipelineStorageBinding.open(profile, leaf="receipts")
    except DailyPipelineLedgerError as exc:
        raise DailyPipelineCommandManifestError(
            "external stage receipt storage binding is unsafe"
        ) from exc
    directory_fd = binding.leaf_fd
    temporary = f".{path.name}.{uuid4().hex}.tmp"
    descriptor = -1
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=directory_fd,
        )
        os.write(descriptor, payload)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, path.name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        except FileExistsError:
            current = _safe_read_bound(
                binding,
                path.name,
                label="external stage receipt",
                max_bytes=_MAX_RECEIPT_BYTES,
            )
            if current != payload:
                raise DailyPipelineCommandManifestError(
                    "external stage receipt conflicts with an existing receipt"
                ) from None
        os.unlink(temporary, dir_fd=directory_fd)
        os.fsync(directory_fd)
        binding.assert_current()
    except DailyPipelineLedgerError as exc:
        raise DailyPipelineCommandManifestError(
            "external stage receipt storage binding changed"
        ) from exc
    except DailyPipelineCommandManifestError:
        raise
    except OSError as exc:
        raise DailyPipelineCommandManifestError(
            "external stage receipt publication failed"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        with suppress(FileNotFoundError):
            os.unlink(temporary, dir_fd=directory_fd)
        binding.close()


def publish_external_stage_receipt_from_environment(
    result: StageResult,
    *,
    issued_at: datetime,
) -> ExternalStageReceipt:
    """Child-only commit helper that fences, signs, and publishes atomically."""
    run_id = _required_environment("RQUANT_DAILY_RUN_ID")
    try:
        mode = DailyPipelineMode(_required_environment("RQUANT_DAILY_RUN_MODE"))
    except ValueError as exc:
        raise DailyPipelineCommandManifestError(
            "external stage receipt run mode is invalid"
        ) from exc
    stage_id = _required_environment("RQUANT_DAILY_STAGE_ID")
    idempotency_key = _required_environment("RQUANT_DAILY_EFFECT_IDEMPOTENCY_KEY")
    effect_id = _required_environment("RQUANT_DAILY_EFFECT_ID")
    command_manifest_hash = _required_environment("RQUANT_DAILY_COMMAND_MANIFEST_HASH")
    locator = _absolute_normalized(
        Path(_required_environment("RQUANT_DAILY_EFFECT_RECEIPT_LOCATOR")),
        label="external stage receipt",
    )
    storage_profile = DailyPipelineStorageProfile.create(
        root=_absolute_normalized(
            Path(_required_environment("RQUANT_DAILY_STORAGE_ROOT")),
            label="daily pipeline storage root",
        ),
        mode=mode,
        profile_hash=_required_environment("RQUANT_DAILY_PROFILE_HASH"),
    )
    if storage_profile.namespace_id != _required_environment("RQUANT_DAILY_STORAGE_NAMESPACE_ID"):
        raise DailyPipelineCommandManifestError("external stage receipt storage namespace is stale")
    ledger_path = _absolute_normalized(
        Path(_required_environment("RQUANT_DAILY_LEDGER_PATH")),
        label="daily pipeline ledger",
    )
    if ledger_path != storage_profile.state_path:
        raise DailyPipelineCommandManifestError(
            "external stage receipt ledger path is outside its storage profile"
        )
    ledger = DailyPipelineLedger(
        storage_profile=storage_profile,
        service_owner=_required_environment("RQUANT_DAILY_SERVICE_OWNER"),
    )
    try:
        fencing_token = int(_required_environment("RQUANT_DAILY_FENCING_TOKEN"))
    except ValueError as exc:
        raise DailyPipelineCommandManifestError(
            "external stage receipt fencing token is invalid"
        ) from exc
    key_id = _required_environment("RQUANT_DAILY_EXTERNAL_RECEIPT_KEY_ID")
    key = _receipt_key_from_environment(key_id)
    if key is None:
        raise DailyPipelineCommandManifestError("external stage receipt signing key is unavailable")
    observed = normalize_aware_utc(issued_at)
    with ledger.hold_external_effect_fence(
        run_id=run_id,
        stage_id=stage_id,
        idempotency_key=idempotency_key,
        effect_id=effect_id,
        command_manifest_hash=command_manifest_hash,
        fencing_token=fencing_token,
        checked_at=observed,
    ) as fence:
        receipt = ExternalStageReceipt.signed(
            run_id=fence.run_id,
            mode=fence.mode,
            stage_id=fence.stage_id,
            idempotency_key=fence.idempotency_key,
            fencing_token=fence.fencing_token,
            lease_expiry=fence.lease_expiry,
            source_generation_id=fence.source_generation_id,
            source_content_hash=fence.source_content_hash,
            command_manifest_hash=fence.command_manifest_hash,
            effect_id=fence.effect_id,
            result=StageResult.model_validate(result),
            issued_at=observed,
            signing_key=key,
        )
        _write_receipt_once(storage_profile, locator, canonical_model_json_bytes(receipt))
        return receipt


class DailyPipelineStageCommand(RuntimeContractModel):
    stage_id: StageId
    adapter_identity: str = Field(min_length=1, max_length=256)
    argv: tuple[str, ...] = Field(min_length=1, max_length=128)
    environment: dict[str, str] = Field(default_factory=dict, max_length=64)
    working_directory: str | None = Field(default=None, min_length=1, max_length=4_096)
    receipt_root: Path
    receipt_key_id: str = Field(pattern=_KEY_ID_PATTERN)
    estimated_memory_mb: int = Field(default=64, ge=1, le=1_048_576)
    estimated_io_bytes: int = Field(default=0, ge=0, le=1 << 50)

    @field_validator("argv")
    @classmethod
    def validate_argv(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not item for item in values):
            raise ValueError("daily stage command argv contains an empty item")
        return values

    @field_validator("receipt_root")
    @classmethod
    def validate_receipt_root(cls, value: Path) -> Path:
        return _absolute_normalized(value, label="daily stage receipt root")


class DailyPipelineCommandManifest(RuntimeContractModel):
    contract: Literal["daily-pipeline-command-manifest/v3"] = "daily-pipeline-command-manifest/v3"
    mode: DailyPipelineMode
    storage_profile: DailyPipelineStorageProfile
    stages: tuple[DailyPipelineStageCommand, ...] = Field(min_length=1, max_length=64)
    manifest_hash: Sha256 | None = None

    @model_validator(mode="after")
    def bind_manifest_identity(self) -> Self:
        identifiers = tuple(stage.stage_id for stage in self.stages)
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("daily pipeline command stages must be unique")
        if self.mode is not self.storage_profile.mode:
            raise ValueError("daily command manifest mode does not match its storage profile")
        if any(stage.receipt_root != self.storage_profile.receipt_root for stage in self.stages):
            raise ValueError("daily command manifest receipt root is outside its storage profile")
        expected = canonical_sha256(self.model_dump(mode="json", exclude={"manifest_hash"}))
        if self.manifest_hash is None:
            object.__setattr__(self, "manifest_hash", expected)
        elif self.manifest_hash != expected:
            raise ValueError(
                "daily pipeline command manifest hash does not match canonical content"
            )
        return self

    def adapter_for(
        self,
        stage_id: str,
        *,
        trusted_receipt_key_provider: DailyExternalReceiptKeyProvider = (
            _receipt_key_from_environment
        ),
    ) -> DailyCommandStageAdapter:
        stage = next((item for item in self.stages if item.stage_id == stage_id), None)
        if stage is None:
            raise DailyPipelineCommandManifestError(f"daily command stage is missing: {stage_id}")
        return DailyCommandStageAdapter(
            stage,
            mode=self.mode,
            storage_profile=self.storage_profile,
            command_manifest_hash=self.manifest_hash,
            trusted_receipt_key_provider=trusted_receipt_key_provider,
        )


class DailyCommandStageAdapter:
    """Adapter that only launches the reviewed command declared in one manifest."""

    def __init__(
        self,
        command: DailyPipelineStageCommand,
        *,
        mode: DailyPipelineMode,
        storage_profile: DailyPipelineStorageProfile,
        command_manifest_hash: str,
        trusted_receipt_key_provider: DailyExternalReceiptKeyProvider,
    ) -> None:
        self._command = DailyPipelineStageCommand.model_validate(command)
        self._mode = DailyPipelineMode(mode)
        self._storage_profile = DailyPipelineStorageProfile.model_validate(storage_profile)
        self._command_manifest_hash = command_manifest_hash
        self._trusted_receipt_key_provider = trusted_receipt_key_provider
        self.stage_id = self._command.stage_id

    def health(self, _context: DailyStageExecutionContext, /) -> DailyStageHealth:
        return DailyStageHealth(
            ready=True,
            detail="reviewed_command_manifest",
            estimated_memory_mb=self._command.estimated_memory_mb,
            estimated_io_bytes=self._command.estimated_io_bytes,
        )

    def prepare(self, context: DailyStageExecutionContext, /) -> DailyStageEffectIntent:
        key = canonical_sha256(
            {
                "contract": "daily-stage-idempotency-key/v3",
                "mode": context.run.spec.mode,
                "run_id": context.run.run_id,
                "stage_id": context.attempt.stage_id,
                "input_identity": context.run.input_identity,
                "command_manifest_hash": context.run.spec.command_manifest_hash,
            }
        )
        if context.run.spec.command_manifest_hash != self._command_manifest_hash:
            raise DailyPipelineCommandManifestError(
                "daily stage command manifest does not match the immutable run"
            )
        if (
            context.run.spec.mode is not self._mode
            or context.run.spec.profile_hash != self._storage_profile.profile_hash
        ):
            raise DailyPipelineCommandManifestError(
                "daily stage command storage profile does not match the immutable run"
            )
        effect_id = canonical_sha256(
            {
                "contract": "daily-stage-effect-locator/v3",
                "mode": self._mode,
                "idempotency_key": key,
                "adapter_identity": self._command.adapter_identity,
                "command_manifest_hash": self._command_manifest_hash,
                "receipt_root": str(self._command.receipt_root),
            }
        )
        receipt = self._command.receipt_root / f"{effect_id}.json"
        return DailyStageEffectIntent(
            mode=self._mode,
            idempotency_key=key,
            command_manifest_hash=self._command_manifest_hash,
            adapter_identity=self._command.adapter_identity,
            receipt_locator=str(receipt),
        )

    def command(
        self,
        _context: DailyStageExecutionContext,
        _effect: DailyStageEffectIntent,
        /,
    ) -> DailyStageProcessSpec:
        return DailyStageProcessSpec(
            argv=self._command.argv,
            environment={
                **self._command.environment,
                "RQUANT_DAILY_EXTERNAL_RECEIPT_KEY_ID": self._command.receipt_key_id,
            },
            working_directory=self._command.working_directory,
        )

    def reconcile(
        self,
        _context: DailyStageExecutionContext,
        effect: DailyStageEffectIntent,
        /,
    ) -> StageResult | None:
        locator = _absolute_normalized(Path(effect.receipt_locator), label="daily stage receipt")
        try:
            locator.relative_to(self._command.receipt_root)
        except ValueError as exc:
            raise DailyPipelineCommandManifestError(
                "daily stage receipt escapes its reviewed root"
            ) from exc
        try:
            payload = _safe_read(
                self._storage_profile,
                leaf="receipts",
                path=locator,
                label="daily stage receipt",
                max_bytes=_MAX_RECEIPT_BYTES,
            )
        except FileNotFoundError:
            return None
        try:
            decoded = strict_canonical_json_loads(payload)
            receipt = ExternalStageReceipt.model_validate(decoded)
        except (StrictJsonError, TypeError, ValueError) as exc:
            raise DailyPipelineCommandManifestError("daily stage receipt is invalid") from exc
        receipt.verify(self._trusted_receipt_key_provider)
        expected = (
            _context.run.spec.mode,
            _context.run.run_id,
            _context.attempt.stage_id,
            effect.idempotency_key,
            _context.run.spec.source_generation_id,
            _context.run.spec.source_content_hash,
            _context.run.spec.command_manifest_hash,
            effect.effect_id,
            self._command.receipt_key_id,
        )
        observed = (
            receipt.mode,
            receipt.run_id,
            receipt.stage_id,
            receipt.idempotency_key,
            receipt.source_generation_id,
            receipt.source_content_hash,
            receipt.command_manifest_hash,
            receipt.effect_id,
            receipt.key_id,
        )
        if observed != expected:
            raise DailyPipelineCommandManifestError(
                "daily stage receipt identity does not match the prepared effect"
            )
        if receipt.fencing_token > _context.attempt.fencing_token:
            raise DailyPipelineCommandManifestError(
                "daily stage receipt fencing token is from a foreign writer"
            )
        if (
            receipt.fencing_token == _context.attempt.fencing_token
            and receipt.lease_expiry != _context.lease.expires_at
        ):
            raise DailyPipelineCommandManifestError(
                "daily stage receipt lease expiry does not match the active fence"
            )
        return receipt.result


def load_daily_pipeline_command_manifest(
    path: Path,
    *,
    expected_storage_profile: DailyPipelineStorageProfile,
) -> DailyPipelineCommandManifest:
    """Load a canonical, owner-private manifest without following symlinks."""
    candidate = _absolute_normalized(path, label="daily pipeline command manifest")
    profile = DailyPipelineStorageProfile.model_validate(expected_storage_profile)
    if candidate != profile.command_manifest_path:
        raise DailyPipelineCommandManifestError(
            "daily pipeline command manifest storage profile mismatch"
        )
    try:
        payload = _safe_read(
            profile,
            leaf="control",
            path=candidate,
            label="daily pipeline command manifest",
            max_bytes=_MAX_RECEIPT_BYTES,
        )
        manifest = DailyPipelineCommandManifest.model_validate(strict_canonical_json_loads(payload))
        if manifest.storage_profile != profile or manifest.mode is not profile.mode:
            raise DailyPipelineCommandManifestError(
                "daily pipeline command manifest storage profile mismatch"
            )
        return manifest
    except (StrictJsonError, TypeError, ValueError) as exc:
        raise DailyPipelineCommandManifestError(
            "daily pipeline command manifest is invalid"
        ) from exc


__all__ = [
    "DailyCommandStageAdapter",
    "DailyExternalReceiptKey",
    "DailyExternalReceiptKeyProvider",
    "DailyPipelineCommandManifest",
    "DailyPipelineCommandManifestError",
    "DailyPipelineStageCommand",
    "ExternalStageReceipt",
    "load_daily_pipeline_command_manifest",
    "publish_external_stage_receipt_from_environment",
]
