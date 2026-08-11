"""Immutable production exports consumed by the legacy Shadow reader."""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import re
import stat
import sys
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal, Protocol
from zoneinfo import ZoneInfo

from pydantic import Field, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.runtime_shadow_sources import (
    LegacyMonitorEvent,
    LegacySurgeEvent,
    ShadowRunnerSignalSource,
    legacy_records_raw_input_id,
    runner_source_raw_input_id,
)
from rquant.runtime_shadow_validation import (
    SecureShadowSigningClient,
    ShadowSourceCompletionReceipt,
    _ed25519_signing_payload,
    _verify_ed25519_signature,
    shadow_session_boundaries,
)
from rquant.signal_router_runtime import RunnerSignalBatch
from rquant.strict_json import (
    StrictJsonError,
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_canonical_json_loads,
    strict_json_loads,
)

_CONTRACT = "legacy-shadow-export/v2"
_RECORD_CONTRACT = "legacy-shadow-export-record/v2"
_MAX_EXPORT_BYTES = 128 * 1024 * 1024
_MAX_RECORDS = 100_000
_MAX_RECORD_BYTES = 4 * 1024 * 1024
_CST = ZoneInfo("Asia/Shanghai")
_FILE_MODE = 0o444
_ROOT_MODE = 0o700
_SESSION_MODE = 0o555
_PUBLISH_WINDOW = timedelta(minutes=5)
_SHADOW_STRATEGY_IDS = frozenset({"n_shape", "growth_board_surge"})
_READ_CHUNK_BYTES = 1024 * 1024
_RECOVERY_MARKER_FILENAME = "recovery-marker.json"
_RECOVERY_MARKER_NAMESPACE = "rquant-legacy-shadow-recovery-marker"
_FINALIZATION_RECEIPT_FILENAME = "finalization-receipt.json"
_FINALIZATION_RECEIPT_NAMESPACE = "rquant-legacy-shadow-finalization-receipt"
_RECOVERY_WALL_MONOTONIC_TOLERANCE = timedelta(seconds=5)
_LINUX_LOCAL_FILESYSTEMS = frozenset({"ext2", "ext3", "ext4", "xfs", "btrfs"})
_POSIX_DIR_FD_SUPPORTED = all(
    function in os.supports_dir_fd for function in (os.open, os.stat, os.rename)
)


class LegacyShadowExportError(RuntimeError):
    """A legacy shadow export cannot be made safe for comparison."""


class LegacyShadowExportConflictError(LegacyShadowExportError):
    """An immutable session already exists with different evidence."""


class LegacyShadowExportUnavailableError(LegacyShadowExportError):
    """A required completed export is absent, partial, or invalid."""


class LegacyShadowRecoveryCaptureBinding(RuntimeContractModel):
    contract: Literal["rquant-legacy-shadow-recovery-capture-request/v1"] = (
        "rquant-legacy-shadow-recovery-capture-request/v1"
    )
    trade_date: date
    source_id: str = Field(min_length=1)
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    producer_version: str = Field(min_length=1)
    staging_name: str = Field(pattern=r"^\.staging-[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9a-f]{32}$")


class LegacyShadowRecoveryResumeBinding(RuntimeContractModel):
    contract: Literal["rquant-legacy-shadow-recovery-resume-request/v1"] = (
        "rquant-legacy-shadow-recovery-resume-request/v1"
    )
    trade_date: date
    source_id: str = Field(min_length=1)
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    staging_name: str = Field(pattern=r"^\.staging-[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9a-f]{32}$")


class LegacyShadowRecoveryCaptureClaims(RuntimeContractModel):
    contract: Literal["legacy-shadow-recovery-capture-claims/v1"] = (
        "legacy-shadow-recovery-capture-claims/v1"
    )
    trade_date: date
    source_id: str = Field(min_length=1)
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    producer_version: str = Field(min_length=1)
    staging_name: str = Field(pattern=r"^\.staging-[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9a-f]{32}$")
    captured_at: AwareUtcDatetime
    captured_monotonic_ns: int = Field(ge=0)
    boot_id: str = Field(pattern=r"^[0-9a-f-]{36}$")
    clock_source: Literal["CLOCK_BOOTTIME", "CLOCK_MONOTONIC", "test-monotonic"]


class LegacyShadowRecoveryCapture(RuntimeContractModel):
    contract: Literal["legacy-shadow-recovery-capture/v1"] = "legacy-shadow-recovery-capture/v1"
    key_id: str = Field(min_length=1, max_length=128)
    signature_algorithm: Literal["test-hmac-sha256", "ed25519"]
    claims: LegacyShadowRecoveryCaptureClaims
    signed_claims_base64: str = Field(min_length=1, max_length=2 * 1024 * 1024)
    signature: str = Field(min_length=1, max_length=16_384)

    @model_validator(mode="after")
    def validate_signed_claims(self) -> LegacyShadowRecoveryCapture:
        try:
            payload = base64.b64decode(self.signed_claims_base64, validate=True)
            claims = LegacyShadowRecoveryCaptureClaims.model_validate(
                strict_canonical_json_loads(payload)
            )
        except (StrictJsonError, TypeError, ValueError) as exc:
            raise ValueError("legacy shadow recovery signed capture is invalid") from exc
        if claims != self.claims:
            raise ValueError("legacy shadow recovery signed capture claims differ")
        return self


class LegacyShadowRecoveryMarkerDraft(RuntimeContractModel):
    contract: Literal["legacy-shadow-recovery-marker-draft/v2"] = (
        "legacy-shadow-recovery-marker-draft/v2"
    )
    trade_date: date
    source_id: str = Field(min_length=1)
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    producer_version: str = Field(min_length=1)
    input_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    staging_name: str = Field(pattern=r"^\.staging-[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9a-f]{32}$")
    batch_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    surge_collection_proof_id: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    runner_manifest_binding_id: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )


class LegacyShadowRecoveryMarkerClaims(RuntimeContractModel):
    contract: Literal["legacy-shadow-recovery-marker-claims/v3"] = (
        "legacy-shadow-recovery-marker-claims/v3"
    )
    trade_date: date
    source_id: str = Field(min_length=1)
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    producer_version: str = Field(min_length=1)
    input_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    staging_name: str = Field(pattern=r"^\.staging-[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9a-f]{32}$")
    batch_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    directory_device: int = Field(ge=0)
    directory_inode: int = Field(ge=1)
    artifact_digests: Mapping[str, str]
    surge_collection_proof_id: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    runner_manifest_binding_id: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    captured_at: AwareUtcDatetime
    produced_at: AwareUtcDatetime
    boot_id: str = Field(pattern=r"^[0-9a-f-]{36}$")
    captured_monotonic_ns: int = Field(ge=0)
    produced_monotonic_ns: int = Field(ge=0)
    clock_source: Literal["CLOCK_BOOTTIME", "CLOCK_MONOTONIC", "test-monotonic"]

    @model_validator(mode="after")
    def validate_trusted_completion(self) -> LegacyShadowRecoveryMarkerClaims:
        if (
            self.produced_at < self.captured_at
            or self.produced_monotonic_ns < self.captured_monotonic_ns
        ):
            raise ValueError("legacy shadow recovery marker clock is invalid")
        records_filename = (
            "events.json"
            if self.source_id == "legacy-monitor-events"
            else "events.jsonl"
            if self.source_id == "legacy-surge-jsonl"
            else "completed-batch.json"
        )
        expected_artifacts = {
            records_filename,
            "records.jsonl",
            "completion.json",
            "manifest.json",
        }
        if set(self.artifact_digests) != expected_artifacts or any(
            re.fullmatch(r"[0-9a-f]{64}", digest) is None
            for digest in self.artifact_digests.values()
        ):
            raise ValueError("legacy shadow recovery artifact digests are invalid")
        return self


class LegacyShadowRecoveryMarker(RuntimeContractModel):
    contract: Literal["legacy-shadow-recovery-marker/v1"] = "legacy-shadow-recovery-marker/v1"
    marker_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    key_id: str = Field(min_length=1, max_length=128)
    signature_algorithm: Literal["test-hmac-sha256", "ed25519"]
    claims: LegacyShadowRecoveryMarkerClaims
    signature: str = Field(min_length=1, max_length=16_384)

    @model_validator(mode="after")
    def validate_identity(self) -> LegacyShadowRecoveryMarker:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"marker_id"}))
        if self.marker_id != expected:
            raise ValueError("legacy shadow recovery marker identity mismatch")
        return self


class LegacyShadowFinalizationClaims(RuntimeContractModel):
    contract: Literal["legacy-shadow-finalization-claims/v1"] = (
        "legacy-shadow-finalization-claims/v1"
    )
    trade_date: date
    source_id: str = Field(min_length=1)
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    producer_version: str = Field(min_length=1)
    staging_name: str = Field(pattern=r"^\.staging-[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9a-f]{32}$")
    transaction_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    capture_token_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    marker_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    marker_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    batch_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    directory_device: int = Field(ge=0)
    directory_inode: int = Field(ge=1)
    artifact_digests: Mapping[str, str]
    finalized_at: AwareUtcDatetime
    finalized_monotonic_ns: int = Field(ge=0)
    boot_id: str = Field(pattern=r"^[0-9a-f-]{36}$")
    clock_source: Literal["CLOCK_BOOTTIME", "CLOCK_MONOTONIC", "test-monotonic"]

    @model_validator(mode="after")
    def validate_artifact_digests(self) -> LegacyShadowFinalizationClaims:
        records_filename = (
            "events.json"
            if self.source_id == "legacy-monitor-events"
            else "events.jsonl"
            if self.source_id == "legacy-surge-jsonl"
            else "completed-batch.json"
        )
        expected_artifacts = {
            records_filename,
            "records.jsonl",
            "completion.json",
            "manifest.json",
        }
        if set(self.artifact_digests) != expected_artifacts or any(
            re.fullmatch(r"[0-9a-f]{64}", digest) is None
            for digest in self.artifact_digests.values()
        ):
            raise ValueError("legacy shadow finalization artifact digests are invalid")
        return self


class LegacyShadowFinalizationReceipt(RuntimeContractModel):
    contract: Literal["legacy-shadow-finalization-receipt/v1"] = (
        "legacy-shadow-finalization-receipt/v1"
    )
    receipt_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    key_id: str = Field(min_length=1, max_length=128)
    signature_algorithm: Literal["test-hmac-sha256", "ed25519"]
    claims: LegacyShadowFinalizationClaims
    signature: str = Field(min_length=1, max_length=16_384)

    @model_validator(mode="after")
    def validate_identity(self) -> LegacyShadowFinalizationReceipt:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"receipt_id"}))
        if self.receipt_id != expected:
            raise ValueError("legacy shadow finalization receipt identity mismatch")
        return self


class LegacyShadowRecoverySigner(Protocol):
    def capture(
        self,
        binding: LegacyShadowRecoveryCaptureBinding,
    ) -> LegacyShadowRecoveryCapture: ...

    def issue(
        self,
        draft: LegacyShadowRecoveryMarkerDraft,
        capture: LegacyShadowRecoveryCapture,
        *,
        staging_root: Path,
    ) -> LegacyShadowRecoveryMarker: ...

    def resume(
        self,
        binding: LegacyShadowRecoveryResumeBinding,
        *,
        staging_root: Path,
    ) -> LegacyShadowRecoveryMarker: ...


class LegacyShadowRecoveryVerifier(Protocol):
    def verify(self, marker: LegacyShadowRecoveryMarker) -> bool: ...

    def verify_finalization(
        self,
        receipt: LegacyShadowFinalizationReceipt,
    ) -> bool: ...


def _recovery_marker_payload(claims: LegacyShadowRecoveryMarkerClaims) -> bytes:
    return canonical_json_bytes(
        {
            "contract": "rquant-legacy-shadow-recovery-signing/v3",
            "namespace": _RECOVERY_MARKER_NAMESPACE,
            "claims": claims.model_dump(mode="json"),
        }
    )


def _recovery_capture_payload(claims: LegacyShadowRecoveryCaptureClaims) -> bytes:
    return canonical_model_json_bytes(claims)


def _finalization_receipt_payload(claims: LegacyShadowFinalizationClaims) -> bytes:
    return canonical_json_bytes(
        {
            "contract": "rquant-legacy-shadow-finalization-signing/v1",
            "namespace": _FINALIZATION_RECEIPT_NAMESPACE,
            "claims": claims.model_dump(mode="json"),
        }
    )


class HmacLegacyShadowRecoveryAuthority:
    """Explicit test authority. Production construction never accepts this type."""

    def __init__(
        self,
        *,
        key_id: str,
        secret: bytes,
        wall_clock: Callable[[], datetime] | None = None,
        monotonic_ns: Callable[[], int] | None = None,
        boot_id: Callable[[], str] | None = None,
    ) -> None:
        if not key_id or not secret:
            raise ValueError("legacy shadow test recovery authority is incomplete")
        self.key_id = key_id
        self._secret = bytes(secret)
        self._wall_clock = wall_clock or (lambda: datetime.now(UTC))
        self._monotonic_ns = monotonic_ns or time.monotonic_ns
        self._boot_id = boot_id or (lambda: "00000000-0000-4000-8000-000000000001")
        self._consumed_capture_tokens: set[str] = set()

    def capture(
        self,
        binding: LegacyShadowRecoveryCaptureBinding,
    ) -> LegacyShadowRecoveryCapture:
        verified = LegacyShadowRecoveryCaptureBinding.model_validate(binding)
        captured_at = normalize_aware_utc(self._wall_clock())
        if not _in_publish_window(
            trade_date=verified.trade_date,
            captured_at=captured_at,
        ):
            raise ValueError("legacy shadow recovery capture is outside the publish window")
        claims = LegacyShadowRecoveryCaptureClaims(
            **verified.model_dump(mode="python", exclude={"contract"}),
            captured_at=captured_at,
            captured_monotonic_ns=self._monotonic_ns(),
            boot_id=self._boot_id(),
            clock_source="test-monotonic",
        )
        signature = hmac.new(
            self._secret,
            _recovery_capture_payload(claims),
            hashlib.sha256,
        ).hexdigest()
        signed_payload = _recovery_capture_payload(claims)
        return LegacyShadowRecoveryCapture(
            key_id=self.key_id,
            signature_algorithm="test-hmac-sha256",
            claims=claims,
            signed_claims_base64=base64.b64encode(signed_payload).decode("ascii"),
            signature=signature,
        )

    def issue(
        self,
        draft: LegacyShadowRecoveryMarkerDraft,
        capture: LegacyShadowRecoveryCapture,
        *,
        staging_root: Path,
    ) -> LegacyShadowRecoveryMarker:
        verified_draft = LegacyShadowRecoveryMarkerDraft.model_validate(draft)
        verified_capture = LegacyShadowRecoveryCapture.model_validate(capture)
        expected_capture_signature = hmac.new(
            self._secret,
            base64.b64decode(verified_capture.signed_claims_base64, validate=True),
            hashlib.sha256,
        ).hexdigest()
        if (
            verified_capture.key_id != self.key_id
            or verified_capture.signature_algorithm != "test-hmac-sha256"
            or not hmac.compare_digest(
                verified_capture.signature,
                expected_capture_signature,
            )
        ):
            raise ValueError("legacy shadow recovery capture signature is invalid")
        capture_token_id = canonical_sha256(verified_capture.model_dump(mode="python"))
        if capture_token_id in self._consumed_capture_tokens:
            raise ValueError("legacy shadow recovery capture token was already consumed")
        for field_name in (
            "trade_date",
            "source_id",
            "producer_commit",
            "producer_version",
            "staging_name",
        ):
            if getattr(verified_draft, field_name) != getattr(
                verified_capture.claims,
                field_name,
            ):
                raise ValueError("legacy shadow recovery capture binding mismatch")
        produced_at = normalize_aware_utc(self._wall_clock())
        produced_monotonic_ns = self._monotonic_ns()
        if not _in_publish_window(
            trade_date=verified_draft.trade_date,
            captured_at=produced_at,
        ):
            raise ValueError("legacy shadow recovery completion is outside the publish window")
        if (
            self._boot_id() != verified_capture.claims.boot_id
            or produced_at < verified_capture.claims.captured_at
            or produced_monotonic_ns < verified_capture.claims.captured_monotonic_ns
        ):
            raise ValueError("legacy shadow recovery clock rollback detected")
        wall_elapsed = produced_at - verified_capture.claims.captured_at
        monotonic_elapsed = timedelta(
            microseconds=(produced_monotonic_ns - verified_capture.claims.captured_monotonic_ns)
            // 1_000
        )
        if abs(wall_elapsed - monotonic_elapsed) > _RECOVERY_WALL_MONOTONIC_TOLERANCE:
            raise ValueError("legacy shadow recovery clock rollback detected")
        staging_root_descriptor = _open_directory_fd(
            staging_root,
            label="legacy shadow test staging root",
        )
        staging_descriptor = -1
        try:
            staging_descriptor = _open_child_directory_at(
                staging_root_descriptor,
                verified_draft.staging_name,
                label="legacy shadow test staging",
                allowed_modes=frozenset({_ROOT_MODE}),
            )
            directory_stat, artifact_digests, observed_batch_digest = (
                _inspect_complete_marker_artifacts_at(
                    staging_descriptor,
                    source_id=verified_draft.source_id,
                )
            )
            if observed_batch_digest != verified_draft.batch_digest:
                raise ValueError("legacy shadow recovery batch digest mismatch")
        finally:
            if staging_descriptor >= 0:
                os.close(staging_descriptor)
            os.close(staging_root_descriptor)
        verified = LegacyShadowRecoveryMarkerClaims(
            **verified_draft.model_dump(mode="python", exclude={"contract"}),
            directory_device=directory_stat.st_dev,
            directory_inode=directory_stat.st_ino,
            artifact_digests=artifact_digests,
            captured_at=verified_capture.claims.captured_at,
            produced_at=produced_at,
            boot_id=verified_capture.claims.boot_id,
            captured_monotonic_ns=verified_capture.claims.captured_monotonic_ns,
            produced_monotonic_ns=produced_monotonic_ns,
            clock_source="test-monotonic",
        )
        signature = hmac.new(
            self._secret,
            _recovery_marker_payload(verified),
            hashlib.sha256,
        ).hexdigest()
        values = {
            "contract": "legacy-shadow-recovery-marker/v1",
            "key_id": self.key_id,
            "signature_algorithm": "test-hmac-sha256",
            "claims": verified,
            "signature": signature,
        }
        marker = LegacyShadowRecoveryMarker(
            marker_id=canonical_sha256(values),
            **values,
        )
        marker_payload = canonical_model_json_bytes(marker)
        staging_root_descriptor = _open_directory_fd(
            staging_root,
            label="legacy shadow test staging root",
        )
        staging_descriptor = -1
        try:
            staging_descriptor = _open_child_directory_at(
                staging_root_descriptor,
                verified_draft.staging_name,
                label="legacy shadow test staging",
                allowed_modes=frozenset({_ROOT_MODE}),
            )
            rebound = os.fstat(staging_descriptor)
            if (rebound.st_dev, rebound.st_ino) != (
                directory_stat.st_dev,
                directory_stat.st_ino,
            ):
                raise ValueError("legacy shadow recovery staging changed before signing")
            _write_new_file_at(
                staging_descriptor,
                _RECOVERY_MARKER_FILENAME,
                marker_payload,
            )
            os.fsync(staging_descriptor)
            os.fsync(staging_root_descriptor)
            finalized_at = normalize_aware_utc(self._wall_clock())
            finalized_monotonic_ns = self._monotonic_ns()
            finalized_boot_id = self._boot_id()
            if not _in_publish_window(
                trade_date=verified_draft.trade_date,
                captured_at=finalized_at,
            ):
                raise ValueError(
                    "legacy shadow recovery finalization is outside the publish window"
                )
            if (
                finalized_boot_id != verified.boot_id
                or finalized_at < verified.produced_at
                or finalized_monotonic_ns < verified.produced_monotonic_ns
            ):
                raise ValueError("legacy shadow recovery finalization clock rollback detected")
            finalization_wall_elapsed = finalized_at - verified.produced_at
            finalization_monotonic_elapsed = timedelta(
                microseconds=(finalized_monotonic_ns - verified.produced_monotonic_ns) // 1_000
            )
            if (
                abs(finalization_wall_elapsed - finalization_monotonic_elapsed)
                > _RECOVERY_WALL_MONOTONIC_TOLERANCE
            ):
                raise ValueError("legacy shadow recovery finalization clock rollback detected")
            transaction_id = canonical_sha256(
                {
                    "contract": "legacy-shadow-recovery-transaction-identity/v1",
                    "capture_token_id": capture_token_id,
                }
            )
            finalization_claims = LegacyShadowFinalizationClaims(
                trade_date=verified.trade_date,
                source_id=verified.source_id,
                producer_commit=verified.producer_commit,
                producer_version=verified.producer_version,
                staging_name=verified.staging_name,
                transaction_id=transaction_id,
                capture_token_id=capture_token_id,
                marker_id=marker.marker_id,
                marker_sha256=hashlib.sha256(marker_payload).hexdigest(),
                batch_digest=verified.batch_digest,
                directory_device=verified.directory_device,
                directory_inode=verified.directory_inode,
                artifact_digests=verified.artifact_digests,
                finalized_at=finalized_at,
                finalized_monotonic_ns=finalized_monotonic_ns,
                boot_id=finalized_boot_id,
                clock_source="test-monotonic",
            )
            finalization_signature = hmac.new(
                self._secret,
                _finalization_receipt_payload(finalization_claims),
                hashlib.sha256,
            ).hexdigest()
            finalization_values = {
                "contract": "legacy-shadow-finalization-receipt/v1",
                "key_id": self.key_id,
                "signature_algorithm": "test-hmac-sha256",
                "claims": finalization_claims,
                "signature": finalization_signature,
            }
            finalization_receipt = LegacyShadowFinalizationReceipt(
                receipt_id=canonical_sha256(finalization_values),
                **finalization_values,
            )
            _write_new_file_at(
                staging_descriptor,
                _FINALIZATION_RECEIPT_FILENAME,
                canonical_model_json_bytes(finalization_receipt),
            )
            os.fsync(staging_descriptor)
            os.fsync(staging_root_descriptor)
            durable_at = normalize_aware_utc(self._wall_clock())
            durable_monotonic_ns = self._monotonic_ns()
            durable_boot_id = self._boot_id()
            try:
                if not _in_publish_window(
                    trade_date=verified_draft.trade_date,
                    captured_at=durable_at,
                ):
                    raise ValueError(
                        "legacy shadow recovery finalization is outside the publish window"
                    )
                if (
                    durable_boot_id != finalized_boot_id
                    or durable_at < finalized_at
                    or durable_monotonic_ns < finalized_monotonic_ns
                ):
                    raise ValueError("legacy shadow recovery finalization clock rollback detected")
                durable_wall_elapsed = durable_at - finalized_at
                durable_monotonic_elapsed = timedelta(
                    microseconds=(durable_monotonic_ns - finalized_monotonic_ns) // 1_000
                )
                if (
                    abs(durable_wall_elapsed - durable_monotonic_elapsed)
                    > _RECOVERY_WALL_MONOTONIC_TOLERANCE
                ):
                    raise ValueError("legacy shadow recovery finalization clock rollback detected")
            except ValueError:
                os.unlink(
                    _FINALIZATION_RECEIPT_FILENAME,
                    dir_fd=staging_descriptor,
                )
                os.fsync(staging_descriptor)
                raise
            self._consumed_capture_tokens.add(capture_token_id)
        finally:
            if staging_descriptor >= 0:
                os.close(staging_descriptor)
            os.close(staging_root_descriptor)
        return marker

    def resume(
        self,
        binding: LegacyShadowRecoveryResumeBinding,
        *,
        staging_root: Path,
    ) -> LegacyShadowRecoveryMarker:
        verified_binding = LegacyShadowRecoveryResumeBinding.model_validate(binding)
        root_descriptor = _open_directory_fd(
            staging_root,
            label="legacy shadow test staging root",
        )
        staging_descriptor = -1
        try:
            staging_descriptor = _open_child_directory_at(
                root_descriptor,
                verified_binding.staging_name,
                label="legacy shadow test recovery staging",
                allowed_modes=frozenset({_ROOT_MODE, _SESSION_MODE}),
            )
            marker = _load_recovery_marker_at(staging_descriptor, verifier=self)
            finalization = _load_finalization_receipt_at(
                staging_descriptor,
                verifier=self,
            )
            _verify_recovery_marker_batch_at(staging_descriptor, marker)
            _verify_finalization_receipt_batch_at(
                staging_descriptor,
                marker=marker,
                receipt=finalization,
            )
        finally:
            if staging_descriptor >= 0:
                os.close(staging_descriptor)
            os.close(root_descriptor)
        if (
            marker.claims.trade_date != verified_binding.trade_date
            or marker.claims.source_id != verified_binding.source_id
            or marker.claims.producer_commit != verified_binding.producer_commit
            or marker.claims.staging_name != verified_binding.staging_name
        ):
            raise ValueError("legacy shadow recovery resume binding mismatch")
        return marker

    def verify(self, marker: LegacyShadowRecoveryMarker) -> bool:
        try:
            verified = LegacyShadowRecoveryMarker.model_validate(marker)
        except (TypeError, ValueError):
            return False
        expected = hmac.new(
            self._secret,
            _recovery_marker_payload(verified.claims),
            hashlib.sha256,
        ).hexdigest()
        return (
            verified.key_id == self.key_id
            and verified.signature_algorithm == "test-hmac-sha256"
            and hmac.compare_digest(verified.signature, expected)
        )

    def verify_finalization(
        self,
        receipt: LegacyShadowFinalizationReceipt,
    ) -> bool:
        try:
            verified = LegacyShadowFinalizationReceipt.model_validate(receipt)
        except (TypeError, ValueError):
            return False
        expected = hmac.new(
            self._secret,
            _finalization_receipt_payload(verified.claims),
            hashlib.sha256,
        ).hexdigest()
        return (
            verified.key_id == self.key_id
            and verified.signature_algorithm == "test-hmac-sha256"
            and hmac.compare_digest(verified.signature, expected)
        )


class Ed25519LegacyShadowRecoverySigner:
    def __init__(self, *, key_id: str, client: SecureShadowSigningClient) -> None:
        if not key_id:
            raise ValueError("legacy shadow recovery signer key id is empty")
        self.key_id = key_id
        self._client = client

    def capture(
        self,
        binding: LegacyShadowRecoveryCaptureBinding,
    ) -> LegacyShadowRecoveryCapture:
        verified = LegacyShadowRecoveryCaptureBinding.model_validate(binding)
        payload, signature = self._client.capture_legacy_recovery(
            payload=canonical_model_json_bytes(verified),
        )
        try:
            claims = LegacyShadowRecoveryCaptureClaims.model_validate(
                strict_canonical_json_loads(payload)
            )
        except (StrictJsonError, ValueError) as exc:
            raise ValueError("legacy shadow recovery capture response is invalid") from exc
        for field_name in (
            "trade_date",
            "source_id",
            "producer_commit",
            "producer_version",
            "staging_name",
        ):
            if getattr(claims, field_name) != getattr(verified, field_name):
                raise ValueError("legacy shadow recovery capture response does not bind request")
        if len(base64.b64decode(signature, validate=True)) != 64:
            raise ValueError("legacy shadow recovery capture signature is invalid")
        return LegacyShadowRecoveryCapture(
            key_id=self.key_id,
            signature_algorithm="ed25519",
            claims=claims,
            signed_claims_base64=base64.b64encode(payload).decode("ascii"),
            signature=signature,
        )

    def issue(
        self,
        draft: LegacyShadowRecoveryMarkerDraft,
        capture: LegacyShadowRecoveryCapture,
        *,
        staging_root: Path,
    ) -> LegacyShadowRecoveryMarker:
        verified_draft = LegacyShadowRecoveryMarkerDraft.model_validate(draft)
        verified_capture = LegacyShadowRecoveryCapture.model_validate(capture)
        request_payload = canonical_json_bytes(
            {
                "contract": "rquant-legacy-shadow-recovery-sign-request/v2",
                "capture": {
                    "contract": "legacy-shadow-recovery-capture/v1",
                    "key_id": verified_capture.key_id,
                    "signature_algorithm": verified_capture.signature_algorithm,
                    "claims_base64": verified_capture.signed_claims_base64,
                    "signature": verified_capture.signature,
                },
                "claims": verified_draft.model_dump(mode="json"),
            }
        )
        signed_payload, signature = self._client.sign_legacy_recovery(
            payload=request_payload,
        )
        return self._validated_persisted_marker(
            signed_payload=signed_payload,
            signature=signature,
            staging_root=staging_root,
            staging_name=verified_draft.staging_name,
        )

    def resume(
        self,
        binding: LegacyShadowRecoveryResumeBinding,
        *,
        staging_root: Path,
    ) -> LegacyShadowRecoveryMarker:
        verified_binding = LegacyShadowRecoveryResumeBinding.model_validate(binding)
        signed_payload, signature = self._client.resume_legacy_recovery(
            payload=canonical_model_json_bytes(verified_binding),
        )
        marker = self._validated_persisted_marker(
            signed_payload=signed_payload,
            signature=signature,
            staging_root=staging_root,
            staging_name=verified_binding.staging_name,
        )
        if (
            marker.claims.trade_date != verified_binding.trade_date
            or marker.claims.source_id != verified_binding.source_id
            or marker.claims.producer_commit != verified_binding.producer_commit
            or marker.claims.staging_name != verified_binding.staging_name
        ):
            raise ValueError("legacy shadow recovery resume response does not bind request")
        return marker

    def _validated_persisted_marker(
        self,
        *,
        signed_payload: bytes,
        signature: str,
        staging_root: Path,
        staging_name: str,
    ) -> LegacyShadowRecoveryMarker:
        try:
            document = strict_canonical_json_loads(signed_payload)
            if (
                not isinstance(document, Mapping)
                or document.get("contract") != "rquant-legacy-shadow-recovery-signing/v3"
                or document.get("namespace") != _RECOVERY_MARKER_NAMESPACE
            ):
                raise ValueError("unexpected recovery signing payload")
            verified = LegacyShadowRecoveryMarkerClaims.model_validate(document.get("claims"))
        except (StrictJsonError, TypeError, ValueError) as exc:
            raise ValueError("legacy shadow recovery signing response is invalid") from exc
        if signed_payload != _recovery_marker_payload(verified):
            raise ValueError("legacy shadow recovery signing payload is not canonical")
        try:
            valid_length = len(base64.b64decode(signature, validate=True)) == 64
        except (TypeError, ValueError):
            valid_length = False
        if not valid_length:
            raise ValueError("legacy shadow recovery signer returned an invalid signature")
        values = {
            "contract": "legacy-shadow-recovery-marker/v1",
            "key_id": self.key_id,
            "signature_algorithm": "ed25519",
            "claims": verified,
            "signature": signature,
        }
        marker = LegacyShadowRecoveryMarker(
            marker_id=canonical_sha256(values),
            **values,
        )
        root_descriptor = _open_directory_fd(
            staging_root,
            label="legacy shadow staging root",
        )
        staging_descriptor = -1
        try:
            staging_descriptor = _open_child_directory_at(
                root_descriptor,
                staging_name,
                label="legacy shadow signed staging",
                allowed_modes=frozenset({_SESSION_MODE}),
            )
            persisted = LegacyShadowRecoveryMarker.model_validate(
                strict_canonical_json_loads(
                    _read_regular_at(
                        staging_descriptor,
                        _RECOVERY_MARKER_FILENAME,
                        label="legacy shadow signed recovery marker",
                    )
                )
            )
        except (StrictJsonError, ValueError) as exc:
            raise ValueError("legacy shadow recovery marker was not safely published") from exc
        finally:
            if staging_descriptor >= 0:
                os.close(staging_descriptor)
            os.close(root_descriptor)
        if persisted != marker:
            raise ValueError("legacy shadow recovery marker response differs from staging")
        return marker


class Ed25519LegacyShadowRecoveryKeyring:
    def __init__(
        self,
        *,
        active_key_id: str,
        active_public_key: bytes,
        previous_public_keys: Mapping[str, bytes] | None = None,
    ) -> None:
        keys = {active_key_id: active_public_key, **dict(previous_public_keys or {})}
        if not active_key_id or any(not key_id or not value for key_id, value in keys.items()):
            raise ValueError("legacy shadow recovery keyring is incomplete")
        if len(keys) != 1 + len(dict(previous_public_keys or {})):
            raise ValueError("legacy shadow recovery active key is duplicated")
        self.active_key_id = active_key_id
        self._keys = keys

    def verify(self, marker: LegacyShadowRecoveryMarker) -> bool:
        try:
            verified = LegacyShadowRecoveryMarker.model_validate(marker)
        except (TypeError, ValueError):
            return False
        public_key = self._keys.get(verified.key_id)
        if public_key is None or verified.signature_algorithm != "ed25519":
            return False
        return _verify_ed25519_signature(
            public_key=public_key,
            payload=_ed25519_signing_payload(
                namespace=_RECOVERY_MARKER_NAMESPACE,
                payload=_recovery_marker_payload(verified.claims),
            ),
            signature=verified.signature,
        )

    def verify_finalization(
        self,
        receipt: LegacyShadowFinalizationReceipt,
    ) -> bool:
        try:
            verified = LegacyShadowFinalizationReceipt.model_validate(receipt)
        except (TypeError, ValueError):
            return False
        public_key = self._keys.get(verified.key_id)
        if public_key is None or verified.signature_algorithm != "ed25519":
            return False
        return _verify_ed25519_signature(
            public_key=public_key,
            payload=_ed25519_signing_payload(
                namespace=_FINALIZATION_RECEIPT_NAMESPACE,
                payload=_finalization_receipt_payload(verified.claims),
            ),
            signature=verified.signature,
        )


class LegacyShadowFilesystemPolicy(RuntimeContractModel):
    mode: Literal["linux-production", "test-only-local-posix"]


def legacy_shadow_test_filesystem_policy() -> LegacyShadowFilesystemPolicy:
    return LegacyShadowFilesystemPolicy(mode="test-only-local-posix")


def _signed_session_modes(
    policy: LegacyShadowFilesystemPolicy,
) -> frozenset[int]:
    return frozenset({_ROOT_MODE} if policy.mode == "test-only-local-posix" else {_SESSION_MODE})


@dataclass(frozen=True)
class LegacyShadowTestDependencies:
    wall_clock: Callable[[], datetime]
    monotonic_ns: Callable[[], int]
    boot_id: Callable[[], str]
    recovery_signer: LegacyShadowRecoverySigner
    recovery_verifier: LegacyShadowRecoveryVerifier
    filesystem_policy: LegacyShadowFilesystemPolicy

    def __post_init__(self) -> None:
        if self.filesystem_policy.mode != "test-only-local-posix":
            raise ValueError("legacy shadow test dependencies require the test filesystem policy")


@dataclass(frozen=True)
class _ProductionLegacyShadowDependencies:
    wall_clock: Callable[[], datetime]
    monotonic_ns: Callable[[], int]
    boot_id: Callable[[], str]
    recovery_signer: LegacyShadowRecoverySigner
    recovery_verifier: LegacyShadowRecoveryVerifier
    filesystem_policy: LegacyShadowFilesystemPolicy

    def __post_init__(self) -> None:
        if self.filesystem_policy.mode != "linux-production":
            raise ValueError("production legacy shadow dependencies reject test overrides")


_LegacyShadowDependencies = LegacyShadowTestDependencies | _ProductionLegacyShadowDependencies


class LegacyShadowFilesystemContract(RuntimeContractModel):
    contract: Literal["legacy-shadow-filesystem/v1"] = "legacy-shadow-filesystem/v1"
    filesystem: Literal["local-posix"] = "local-posix"
    atomic_rename_same_filesystem: Literal[True] = True
    parent_dir_fd_nofollow: Literal[True] = True
    remote_filesystems_supported: Literal[False] = False


LEGACY_SHADOW_FILESYSTEM_CONTRACT = LegacyShadowFilesystemContract()


class LegacyShadowExportRecord(RuntimeContractModel):
    contract: Literal["legacy-shadow-export-record/v2"] = _RECORD_CONTRACT
    record_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    record_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_id: str = Field(min_length=1)
    trade_date: date
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    captured_at: AwareUtcDatetime
    as_of: AwareUtcDatetime
    sequence: int = Field(ge=1)
    payload: Mapping[str, object]

    @model_validator(mode="after")
    def validate_identity(self) -> LegacyShadowExportRecord:
        identity = {
            "contract": self.contract,
            "source_id": self.source_id,
            "trade_date": self.trade_date,
            "producer_commit": self.producer_commit,
            "captured_at": self.captured_at,
            "as_of": self.as_of,
            "sequence": self.sequence,
            "payload": dict(self.payload),
        }
        expected = canonical_sha256(identity)
        if self.record_id != expected or self.record_sha256 != expected:
            raise ValueError("legacy shadow export record identity mismatch")
        return self


class LegacySurgeCollectionProof(RuntimeContractModel):
    contract: Literal["legacy-surge-collection-proof/v2"] = "legacy-surge-collection-proof/v2"
    proof_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    trade_date: date
    started_at: AwareUtcDatetime
    first_success_at: AwareUtcDatetime
    last_success_at: AwareUtcDatetime
    successful_snapshots: int = Field(ge=1)
    nonempty_successful_snapshots: int = Field(ge=1)
    empty_successful_snapshots: Literal[0] = 0
    failed_snapshots: int = Field(ge=0)
    maximum_active_gap_seconds: int = Field(ge=0)
    maximum_consecutive_misses: int = Field(ge=0)
    ending_consecutive_misses: int = Field(ge=0)
    source_routes: tuple[Literal["tushare_rt"], ...] = Field(min_length=1)
    market_universe_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    market_universe_expected_count: int = Field(ge=1)
    minimum_market_coverage_count: int = Field(ge=1)
    minimum_market_coverage_bps: int = Field(ge=9_800, le=10_000)
    source_health: Literal["healthy", "recovered"] = "healthy"

    @model_validator(mode="after")
    def validate_complete_coverage(self) -> LegacySurgeCollectionProof:
        local_started = self.started_at.astimezone(_CST)
        local_first = self.first_success_at.astimezone(_CST)
        local_last = self.last_success_at.astimezone(_CST)
        if any(
            observed.date() != self.trade_date
            for observed in (local_started, local_first, local_last)
        ):
            raise ValueError("surge collection proof trade date mismatch")
        if local_started.time() > datetime.min.replace(hour=9, minute=30).time():
            raise ValueError("surge collection proof started after the open boundary")
        if local_first.time() > datetime.min.replace(hour=9, minute=31).time():
            raise ValueError("surge collection proof lacks open coverage")
        if local_last.time() < datetime.min.replace(hour=14, minute=59).time():
            raise ValueError("surge collection proof lacks close coverage")
        if self.first_success_at > self.last_success_at:
            raise ValueError("surge collection proof success range is invalid")
        if self.nonempty_successful_snapshots != self.successful_snapshots:
            raise ValueError("surge successful snapshots must all be nonempty")
        if self.source_routes != ("tushare_rt",):
            raise ValueError("surge collection proof route is not trusted")
        expected_coverage_bps = (
            self.minimum_market_coverage_count * 10_000 // self.market_universe_expected_count
        )
        if (
            self.minimum_market_coverage_count > self.market_universe_expected_count
            or self.minimum_market_coverage_bps != expected_coverage_bps
        ):
            raise ValueError("surge market coverage proof is inconsistent")
        if self.maximum_active_gap_seconds > 125:
            raise ValueError("surge collection proof has an unsafe snapshot gap")
        if self.maximum_consecutive_misses > 1 or self.ending_consecutive_misses != 0:
            raise ValueError("surge collection proof has an unrecovered provider miss")
        if self.source_health != ("recovered" if self.failed_snapshots else "healthy"):
            raise ValueError("surge collection proof source health is inconsistent")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"proof_id"}))
        if self.proof_id != expected:
            raise ValueError("surge collection proof identity mismatch")
        return self

    @classmethod
    def create(cls, **values: object) -> LegacySurgeCollectionProof:
        identity = canonical_sha256({"contract": "legacy-surge-collection-proof/v2", **values})
        return cls(proof_id=identity, **values)


class LegacyShadowRunnerManifestBinding(RuntimeContractModel):
    contract: Literal["legacy-shadow-runner-manifest-binding/v1"] = (
        "legacy-shadow-runner-manifest-binding/v1"
    )
    binding_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    strategy_id: str = Field(min_length=1)
    strategy_version: int = Field(ge=1)
    producer_manifest_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    producer_service_id: str = Field(min_length=1)
    producer_instance_id: str = Field(min_length=1)
    producer_version: str = Field(min_length=1)
    strategy_registration_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    strategy_spec_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    evaluator_contract_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    executable_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_identity(self) -> LegacyShadowRunnerManifestBinding:
        if self.producer_service_id != f"strategy.{self.strategy_id}.v1":
            raise ValueError("runner manifest service does not bind its strategy")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"binding_id"}))
        if self.binding_id != expected:
            raise ValueError("runner manifest binding identity mismatch")
        return self

    @classmethod
    def create(cls, **values: object) -> LegacyShadowRunnerManifestBinding:
        identity = {
            "contract": "legacy-shadow-runner-manifest-binding/v1",
            **values,
        }
        return cls(binding_id=canonical_sha256(identity), **values)


class LegacyShadowExportManifest(RuntimeContractModel):
    contract: Literal["legacy-shadow-export/v2"] = _CONTRACT
    batch_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_id: str = Field(min_length=1)
    trade_date: date
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    producer_version: str = Field(min_length=1)
    captured_at: AwareUtcDatetime
    as_of: AwareUtcDatetime
    records_filename: Literal["events.json", "events.jsonl", "completed-batch.json"]
    records_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    records_count: int = Field(ge=0, le=_MAX_RECORDS)
    record_envelopes_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    completion_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    completion_receipt_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    input_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    surge_collection_proof: LegacySurgeCollectionProof | None = None
    runner_manifest_binding: LegacyShadowRunnerManifestBinding | None = None
    accepted: Literal[True] = True

    @model_validator(mode="after")
    def validate_identity(self) -> LegacyShadowExportManifest:
        expected = canonical_sha256(
            {
                "contract": self.contract,
                "source_id": self.source_id,
                "trade_date": self.trade_date,
                "producer_commit": self.producer_commit,
                "producer_version": self.producer_version,
                "captured_at": self.captured_at,
                "as_of": self.as_of,
                "records_filename": self.records_filename,
                "records_sha256": self.records_sha256,
                "records_count": self.records_count,
                "record_envelopes_sha256": self.record_envelopes_sha256,
                "completion_sha256": self.completion_sha256,
                "completion_receipt_id": self.completion_receipt_id,
                "input_identity": self.input_identity,
                "surge_collection_proof": self.surge_collection_proof,
                "runner_manifest_binding": self.runner_manifest_binding,
                "accepted": self.accepted,
            }
        )
        if self.batch_id != expected:
            raise ValueError("legacy shadow export batch identity mismatch")
        if (self.source_id == "legacy-surge-jsonl") != (self.surge_collection_proof is not None):
            raise ValueError("legacy surge export requires one complete collection proof")
        if (
            self.surge_collection_proof is not None
            and self.surge_collection_proof.trade_date != self.trade_date
        ):
            raise ValueError("legacy surge collection proof trade date mismatch")
        if (self.records_filename == "completed-batch.json") != (
            self.runner_manifest_binding is not None
        ):
            raise ValueError("isolated export requires one runner manifest binding")
        return self


@dataclass(frozen=True)
class AcceptedLegacyShadowExport:
    root: Path
    session_path: Path
    manifest: LegacyShadowExportManifest
    records: tuple[dict[str, object], ...]
    records_path: Path
    completion_receipt: ShadowSourceCompletionReceipt
    completed_batch: RunnerSignalBatch | None = None


ExportFaultHook = Callable[[str], None]


def _absolute_path(path: Path, *, label: str) -> Path:
    candidate = Path(path)
    normalized = Path(os.path.abspath(candidate))
    if not candidate.is_absolute() or candidate != normalized:
        raise ValueError(f"{label} must be absolute and normalized")
    return candidate


def _decode_mountinfo_path(value: str) -> str:
    return (
        value.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
    )


def _linux_mount_filesystem(path: Path) -> str:
    try:
        lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise LegacyShadowExportError("legacy shadow production mount type is unavailable") from exc
    candidate = str(path)
    matches: list[tuple[int, str]] = []
    for line in lines:
        left, separator, right = line.partition(" - ")
        fields = left.split()
        right_fields = right.split()
        if not separator or len(fields) < 5 or not right_fields:
            continue
        mount_point = _decode_mountinfo_path(fields[4])
        if candidate == mount_point or candidate.startswith(mount_point.rstrip("/") + "/"):
            matches.append((len(mount_point), right_fields[0].lower()))
    if not matches:
        raise LegacyShadowExportError("legacy shadow production mount type is unknown")
    return max(matches)[1]


def _validate_mount_policy(path: Path, policy: LegacyShadowFilesystemPolicy) -> None:
    if policy.mode == "test-only-local-posix":
        return
    if sys.platform != "linux":
        raise LegacyShadowExportError(
            "legacy shadow production filesystem requires Linux mount verification"
        )
    filesystem = _linux_mount_filesystem(path)
    if filesystem not in _LINUX_LOCAL_FILESYSTEMS:
        raise LegacyShadowExportError(
            f"legacy shadow production filesystem is not an approved local mount: {filesystem}"
        )


def _ensure_directory_tree(path: Path) -> None:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    descriptor = os.open("/", flags)
    try:
        for component in path.parts[1:]:
            try:
                child = os.open(component, flags, dir_fd=descriptor)
            except FileNotFoundError:
                os.mkdir(component, _ROOT_MODE, dir_fd=descriptor)
                os.fsync(descriptor)
                child = os.open(component, flags, dir_fd=descriptor)
            observed = os.fstat(child)
            if not stat.S_ISDIR(observed.st_mode):
                os.close(child)
                raise LegacyShadowExportError("legacy shadow directory tree is unsafe")
            os.close(descriptor)
            descriptor = child
    except BaseException:
        os.close(descriptor)
        raise
    else:
        os.close(descriptor)


def _ensure_private_root(
    path: Path,
    *,
    filesystem_policy: LegacyShadowFilesystemPolicy,
) -> Path:
    root = _absolute_path(path, label="legacy shadow export root")
    validate_legacy_shadow_filesystem_contract(root, policy=filesystem_policy)
    _ensure_directory_tree(root)
    descriptor = _open_directory_fd(root, label="legacy shadow export root")
    observed = os.fstat(descriptor)
    if not stat.S_ISDIR(observed.st_mode) or observed.st_uid not in {0, os.geteuid()}:
        os.close(descriptor)
        raise LegacyShadowExportError("legacy shadow export root is unsafe")
    try:
        os.fchmod(descriptor, _ROOT_MODE)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _validate_mount_policy(root, filesystem_policy)
    return root


def validate_legacy_shadow_filesystem_contract(
    path: Path,
    *,
    policy: LegacyShadowFilesystemPolicy,
) -> LegacyShadowFilesystemContract:
    """Preflight the required primitives; deployment must provide a local POSIX filesystem."""

    root = _absolute_path(path, label="legacy shadow filesystem root")
    required_flags = ("O_DIRECTORY", "O_NOFOLLOW", "O_NONBLOCK")
    if (
        os.name != "posix"
        or any(not hasattr(os, name) for name in required_flags)
        or not _POSIX_DIR_FD_SUPPORTED
    ):
        raise LegacyShadowExportError(
            "legacy shadow export requires local POSIX dir_fd and atomic rename semantics"
        )
    probe = root
    while not os.path.lexists(probe):
        if probe == probe.parent:
            raise LegacyShadowExportError("legacy shadow filesystem root is unavailable")
        probe = probe.parent
    _validate_mount_policy(probe, policy)
    try:
        observed = probe.lstat()
    except OSError as exc:
        raise LegacyShadowExportError("legacy shadow filesystem root is unavailable") from exc
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid not in {0, os.geteuid()}
    ):
        raise LegacyShadowExportError("legacy shadow filesystem root is unsafe")
    return LEGACY_SHADOW_FILESYSTEM_CONTRACT


def _open_directory_fd(path: Path, *, label: str) -> int:
    normalized = _absolute_path(path, label=label)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    descriptor = os.open("/", flags)
    try:
        for component in normalized.parts[1:]:
            child = os.open(component, flags, dir_fd=descriptor)
            observed = os.fstat(child)
            if not stat.S_ISDIR(observed.st_mode):
                os.close(child)
                raise LegacyShadowExportError(f"{label} is unsafe")
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


@dataclass(frozen=True)
class _BoundRegularReader:
    descriptor: int
    parent_descriptor: int
    filename: str
    observed: os.stat_result


@contextmanager
def _open_bound_regular(
    path: Path,
    *,
    label: str,
    maximum_bytes: int,
    required_mode: int | None,
    unavailable_error: type[LegacyShadowExportError],
    missing_ok: bool = False,
) -> Iterator[_BoundRegularReader | None]:
    candidate = _absolute_path(path, label=label)
    parent_descriptor = -1
    descriptor = -1
    try:
        parent_descriptor = _open_directory_fd(candidate.parent, label=f"{label} parent")
        try:
            descriptor = os.open(
                candidate.name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                dir_fd=parent_descriptor,
            )
        except FileNotFoundError:
            if missing_ok:
                yield None
                return
            raise
        observed = os.fstat(descriptor)
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
            or observed.st_size > maximum_bytes
            or (required_mode is not None and stat.S_IMODE(observed.st_mode) != required_mode)
        ):
            raise unavailable_error(f"{label} is unsafe or exceeds its budget")
        yield _BoundRegularReader(
            descriptor=descriptor,
            parent_descriptor=parent_descriptor,
            filename=candidate.name,
            observed=observed,
        )
        completed = os.fstat(descriptor)
        rebound = os.stat(candidate.name, dir_fd=parent_descriptor, follow_symlinks=False)
        identity = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_nlink")
        if any(getattr(completed, name) != getattr(observed, name) for name in identity) or any(
            getattr(rebound, name) != getattr(observed, name) for name in identity
        ):
            raise unavailable_error(f"{label} changed while read")
    except unavailable_error:
        raise
    except LegacyShadowExportError as exc:
        raise unavailable_error(f"{label} is unavailable or unsafe") from exc
    except OSError as exc:
        raise unavailable_error(f"{label} is unavailable or unsafe") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if parent_descriptor >= 0:
            os.close(parent_descriptor)


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise OSError("legacy shadow export write made no progress")
        offset += written


def _finish_stream_file(descriptor: int) -> None:
    os.fchmod(descriptor, _FILE_MODE)
    os.fsync(descriptor)


def _write_new_file_at(directory_descriptor: int, filename: str, payload: bytes) -> None:
    descriptor = os.open(
        filename,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o600,
        dir_fd=directory_descriptor,
    )
    try:
        _write_all(descriptor, payload)
        os.fchmod(descriptor, _FILE_MODE)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _open_new_file_at(directory_descriptor: int, filename: str) -> int:
    return os.open(
        filename,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o600,
        dir_fd=directory_descriptor,
    )


def _open_child_directory_at(
    parent_descriptor: int,
    name: str,
    *,
    label: str,
    allowed_modes: frozenset[int],
) -> int:
    if not name or name in {".", ".."} or "/" in name or "\\" in name:
        raise LegacyShadowExportUnavailableError(f"{label} name is unsafe")
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            dir_fd=parent_descriptor,
        )
    except OSError as exc:
        raise LegacyShadowExportUnavailableError(f"{label} is unavailable") from exc
    observed = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) not in allowed_modes
    ):
        os.close(descriptor)
        raise LegacyShadowExportUnavailableError(f"{label} is unsafe")
    return descriptor


def _read_regular_at(
    directory_descriptor: int,
    filename: str,
    *,
    label: str,
    maximum_bytes: int = _MAX_EXPORT_BYTES,
    required_mode: int | None = _FILE_MODE,
) -> bytes:
    descriptor = -1
    try:
        descriptor = os.open(
            filename,
            os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
            dir_fd=directory_descriptor,
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid not in {0, os.geteuid()}
            or before.st_nlink != 1
            or before.st_size > maximum_bytes
            or (required_mode is not None and stat.S_IMODE(before.st_mode) != required_mode)
        ):
            raise LegacyShadowExportUnavailableError(f"{label} is unsafe or oversized")
        payload = bytearray()
        while len(payload) <= maximum_bytes:
            chunk = os.read(
                descriptor,
                min(_READ_CHUNK_BYTES, maximum_bytes + 1 - len(payload)),
            )
            if not chunk:
                break
            payload.extend(chunk)
        if len(payload) > maximum_bytes:
            raise LegacyShadowExportUnavailableError(f"{label} exceeds its budget")
        after = os.fstat(descriptor)
        rebound = os.stat(filename, dir_fd=directory_descriptor, follow_symlinks=False)
        identity = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_nlink")
        if any(getattr(before, key) != getattr(after, key) for key in identity) or any(
            getattr(before, key) != getattr(rebound, key) for key in identity
        ):
            raise LegacyShadowExportUnavailableError(f"{label} changed while read")
        return bytes(payload)
    except LegacyShadowExportUnavailableError:
        raise
    except OSError as exc:
        raise LegacyShadowExportUnavailableError(f"{label} is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _batch_digest_at(directory_descriptor: int, filenames: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for filename in sorted(filenames):
        payload = _read_regular_at(
            directory_descriptor,
            filename,
            label=f"legacy shadow batch file {filename}",
        )
        digest.update(len(filename).to_bytes(4, "big"))
        digest.update(filename.encode("ascii"))
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(hashlib.sha256(payload).digest())
    return digest.hexdigest()


def _marker_artifact_names(source_id: str) -> frozenset[str]:
    records_filename = (
        "events.json"
        if source_id == "legacy-monitor-events"
        else "events.jsonl"
        if source_id == "legacy-surge-jsonl"
        else "completed-batch.json"
    )
    return frozenset(
        {
            records_filename,
            "records.jsonl",
            "completion.json",
            "manifest.json",
        }
    )


def _inspect_complete_marker_artifacts_at(
    directory_descriptor: int,
    *,
    source_id: str,
) -> tuple[os.stat_result, dict[str, str], str]:
    directory_before = os.fstat(directory_descriptor)
    filenames = _marker_artifact_names(source_id)
    if set(os.listdir(directory_descriptor)) != set(filenames):
        raise LegacyShadowExportError("legacy shadow staging is incomplete before signing")
    artifact_digests: dict[str, str] = {}
    for filename in sorted(filenames):
        payload = _read_regular_at(
            directory_descriptor,
            filename,
            label=f"legacy shadow staging artifact {filename}",
        )
        artifact_digests[filename] = hashlib.sha256(payload).hexdigest()
    batch_digest = _batch_digest_at(directory_descriptor, filenames)
    directory_after = os.fstat(directory_descriptor)
    if (directory_before.st_dev, directory_before.st_ino) != (
        directory_after.st_dev,
        directory_after.st_ino,
    ):
        raise LegacyShadowExportError("legacy shadow staging changed while inspected")
    return directory_before, artifact_digests, batch_digest


def _completion_payload(receipt: ShadowSourceCompletionReceipt) -> bytes:
    return canonical_model_json_bytes(receipt)


def _manifest(
    *,
    source_id: str,
    trade_date: date,
    producer_commit: str,
    producer_version: str,
    captured_at: datetime,
    as_of: datetime,
    records_filename: Literal["events.json", "events.jsonl", "completed-batch.json"],
    records_sha256: str,
    records_count: int,
    record_envelopes_sha256: str,
    completion_payload: bytes,
    receipt: ShadowSourceCompletionReceipt,
    surge_collection_proof: LegacySurgeCollectionProof | None = None,
    runner_manifest_binding: LegacyShadowRunnerManifestBinding | None = None,
) -> LegacyShadowExportManifest:
    values = {
        "contract": _CONTRACT,
        "source_id": source_id,
        "trade_date": trade_date,
        "producer_commit": producer_commit,
        "producer_version": producer_version,
        "captured_at": captured_at,
        "as_of": as_of,
        "records_filename": records_filename,
        "records_sha256": records_sha256,
        "records_count": records_count,
        "record_envelopes_sha256": record_envelopes_sha256,
        "completion_sha256": hashlib.sha256(completion_payload).hexdigest(),
        "completion_receipt_id": str(receipt.receipt_id),
        "input_identity": receipt.input_identity,
        "surge_collection_proof": surge_collection_proof,
        "runner_manifest_binding": runner_manifest_binding,
        "accepted": True,
    }
    return LegacyShadowExportManifest(batch_id=canonical_sha256(values), **values)


def _issue_recovery_marker(
    *,
    trade_date: date,
    source_id: str,
    producer_commit: str,
    producer_version: str,
    input_identity: str,
    staging_name: str,
    batch_digest: str,
    surge_collection_proof_id: str | None,
    runner_manifest_binding_id: str | None,
    capture: LegacyShadowRecoveryCapture,
    staging_root: Path,
    signer: LegacyShadowRecoverySigner,
    verifier: LegacyShadowRecoveryVerifier,
) -> LegacyShadowRecoveryMarker:
    try:
        draft = LegacyShadowRecoveryMarkerDraft(
            trade_date=trade_date,
            source_id=source_id,
            producer_commit=producer_commit,
            producer_version=producer_version,
            input_identity=input_identity,
            staging_name=staging_name,
            batch_digest=batch_digest,
            surge_collection_proof_id=surge_collection_proof_id,
            runner_manifest_binding_id=runner_manifest_binding_id,
        )
        marker = signer.issue(draft, capture, staging_root=staging_root)
    except (TypeError, ValueError, RuntimeError) as exc:
        raise LegacyShadowExportError("legacy shadow recovery marker signing failed") from exc
    if not verifier.verify(marker):
        raise LegacyShadowExportError("legacy shadow recovery marker signature is invalid")
    root_descriptor = _open_directory_fd(
        staging_root,
        label="legacy shadow staging root",
    )
    staging_descriptor = -1
    try:
        staging_descriptor = _open_child_directory_at(
            root_descriptor,
            staging_name,
            label="legacy shadow finalized staging",
            allowed_modes=frozenset({_ROOT_MODE, _SESSION_MODE}),
        )
        finalization = _load_finalization_receipt_at(
            staging_descriptor,
            verifier=verifier,
        )
        _verify_recovery_marker_batch_at(staging_descriptor, marker)
        _verify_finalization_receipt_batch_at(
            staging_descriptor,
            marker=marker,
            receipt=finalization,
        )
    except LegacyShadowExportUnavailableError as exc:
        raise LegacyShadowExportError("legacy shadow finalization receipt is unavailable") from exc
    finally:
        if staging_descriptor >= 0:
            os.close(staging_descriptor)
        os.close(root_descriptor)
    return marker


def _capture_recovery_marker_start(
    *,
    trade_date: date,
    source_id: str,
    producer_commit: str,
    producer_version: str,
    staging_name: str,
    signer: LegacyShadowRecoverySigner,
) -> LegacyShadowRecoveryCapture:
    try:
        return signer.capture(
            LegacyShadowRecoveryCaptureBinding(
                trade_date=trade_date,
                source_id=source_id,
                producer_commit=producer_commit,
                producer_version=producer_version,
                staging_name=staging_name,
            )
        )
    except (TypeError, ValueError, RuntimeError) as exc:
        raise LegacyShadowExportError("legacy shadow recovery capture failed") from exc


def _validate_recovery_clock(
    marker: LegacyShadowRecoveryMarker,
    *,
    dependencies: _LegacyShadowDependencies,
) -> None:
    now = normalize_aware_utc(dependencies.wall_clock())
    monotonic_now = dependencies.monotonic_ns()
    if marker.claims.boot_id != dependencies.boot_id():
        raise LegacyShadowExportError("legacy shadow recovery marker boot id differs")
    if now < marker.claims.produced_at or monotonic_now < marker.claims.produced_monotonic_ns:
        raise LegacyShadowExportError("legacy shadow recovery clock rollback detected")
    wall_elapsed = now - marker.claims.produced_at
    monotonic_elapsed = timedelta(
        microseconds=(monotonic_now - marker.claims.produced_monotonic_ns) // 1_000
    )
    if abs(wall_elapsed - monotonic_elapsed) > _RECOVERY_WALL_MONOTONIC_TOLERANCE:
        raise LegacyShadowExportError("legacy shadow recovery clock rollback detected")


def _load_recovery_marker_at(
    session_descriptor: int,
    *,
    verifier: LegacyShadowRecoveryVerifier,
) -> LegacyShadowRecoveryMarker:
    try:
        marker = LegacyShadowRecoveryMarker.model_validate(
            strict_canonical_json_loads(
                _read_regular_at(
                    session_descriptor,
                    _RECOVERY_MARKER_FILENAME,
                    label="legacy shadow recovery marker",
                )
            )
        )
    except (StrictJsonError, ValueError) as exc:
        raise LegacyShadowExportUnavailableError(
            "legacy shadow recovery marker is invalid"
        ) from exc
    try:
        signature_valid = verifier.verify(marker)
    except Exception as exc:
        raise LegacyShadowExportUnavailableError(
            "legacy shadow recovery marker verification is unavailable"
        ) from exc
    if not signature_valid:
        raise LegacyShadowExportUnavailableError(
            "legacy shadow recovery marker signature is invalid"
        )
    return marker


def _load_finalization_receipt_at(
    session_descriptor: int,
    *,
    verifier: LegacyShadowRecoveryVerifier,
) -> LegacyShadowFinalizationReceipt:
    try:
        receipt = LegacyShadowFinalizationReceipt.model_validate(
            strict_canonical_json_loads(
                _read_regular_at(
                    session_descriptor,
                    _FINALIZATION_RECEIPT_FILENAME,
                    label="legacy shadow finalization receipt",
                )
            )
        )
    except (StrictJsonError, ValueError) as exc:
        raise LegacyShadowExportUnavailableError(
            "legacy shadow finalization receipt is invalid"
        ) from exc
    try:
        signature_valid = verifier.verify_finalization(receipt)
    except Exception as exc:
        raise LegacyShadowExportUnavailableError(
            "legacy shadow finalization receipt verification is unavailable"
        ) from exc
    if not signature_valid:
        raise LegacyShadowExportUnavailableError(
            "legacy shadow finalization receipt signature is invalid"
        )
    return receipt


def _verify_recovery_marker_batch_at(
    session_descriptor: int,
    marker: LegacyShadowRecoveryMarker,
) -> None:
    observed_directory = os.fstat(session_descriptor)
    if (observed_directory.st_dev, observed_directory.st_ino) != (
        marker.claims.directory_device,
        marker.claims.directory_inode,
    ):
        raise LegacyShadowExportUnavailableError(
            "legacy shadow recovery marker directory identity mismatch"
        )
    filenames = _marker_artifact_names(marker.claims.source_id)
    observed_digests = {
        filename: hashlib.sha256(
            _read_regular_at(
                session_descriptor,
                filename,
                label=f"legacy shadow signed artifact {filename}",
            )
        ).hexdigest()
        for filename in sorted(filenames)
    }
    if dict(marker.claims.artifact_digests) != observed_digests:
        raise LegacyShadowExportUnavailableError(
            "legacy shadow recovery marker artifact digest mismatch"
        )
    if _batch_digest_at(session_descriptor, filenames) != marker.claims.batch_digest:
        raise LegacyShadowExportUnavailableError(
            "legacy shadow recovery marker batch digest mismatch"
        )


def _verify_finalization_receipt_batch_at(
    session_descriptor: int,
    *,
    marker: LegacyShadowRecoveryMarker,
    receipt: LegacyShadowFinalizationReceipt,
) -> None:
    claims = receipt.claims
    marker_payload = _read_regular_at(
        session_descriptor,
        _RECOVERY_MARKER_FILENAME,
        label="legacy shadow recovery marker",
    )
    observed_directory = os.fstat(session_descriptor)
    wall_elapsed = claims.finalized_at - marker.claims.produced_at
    monotonic_elapsed = timedelta(
        microseconds=(claims.finalized_monotonic_ns - marker.claims.produced_monotonic_ns) // 1_000
    )
    if (
        receipt.key_id != marker.key_id
        or receipt.signature_algorithm != marker.signature_algorithm
        or claims.trade_date != marker.claims.trade_date
        or claims.source_id != marker.claims.source_id
        or claims.producer_commit != marker.claims.producer_commit
        or claims.producer_version != marker.claims.producer_version
        or claims.staging_name != marker.claims.staging_name
        or claims.marker_id != marker.marker_id
        or claims.marker_sha256 != hashlib.sha256(marker_payload).hexdigest()
        or claims.batch_digest != marker.claims.batch_digest
        or claims.directory_device != marker.claims.directory_device
        or claims.directory_inode != marker.claims.directory_inode
        or (claims.directory_device, claims.directory_inode)
        != (observed_directory.st_dev, observed_directory.st_ino)
        or dict(claims.artifact_digests) != dict(marker.claims.artifact_digests)
        or claims.boot_id != marker.claims.boot_id
        or claims.clock_source != marker.claims.clock_source
        or claims.finalized_at < marker.claims.produced_at
        or claims.finalized_monotonic_ns < marker.claims.produced_monotonic_ns
        or abs(wall_elapsed - monotonic_elapsed) > _RECOVERY_WALL_MONOTONIC_TOLERANCE
        or not _in_publish_window(
            trade_date=claims.trade_date,
            captured_at=claims.finalized_at,
        )
    ):
        raise LegacyShadowExportUnavailableError(
            "legacy shadow finalization receipt binding is invalid"
        )


def _validate_receipt(
    receipt: ShadowSourceCompletionReceipt,
    *,
    source: Literal["legacy", "isolated"],
    source_id: str,
    trade_date: date,
    input_identity: str,
    producer_commit: str,
    export_produced_at: datetime,
    as_of: datetime,
) -> ShadowSourceCompletionReceipt:
    verified = ShadowSourceCompletionReceipt.model_validate(receipt)
    _session_open, session_close = shadow_session_boundaries(trade_date)
    if (
        verified.evidence_origin != "production"
        or verified.source != source
        or verified.source_id != source_id
        or verified.trade_date != trade_date
        or verified.session_close_at != session_close
        or verified.complete_through < session_close
        or verified.input_identity != input_identity
        or verified.producer_commit != producer_commit
        or verified.complete_through != as_of
        or verified.produced_at > export_produced_at
    ):
        raise LegacyShadowExportError("legacy shadow completion receipt does not bind export")
    return verified


def _in_publish_window(*, trade_date: date, captured_at: datetime) -> bool:
    _session_open, session_close = shadow_session_boundaries(trade_date)
    return session_close <= captured_at <= session_close + _PUBLISH_WINDOW


def recover_legacy_shadow_export(
    *,
    root: Path,
    trade_date: date,
    expected_source_id: str,
    expected_commit: str,
    dependencies: _LegacyShadowDependencies,
) -> Path:
    """Promote only a previously signed, complete staging batch.

    This entry has no source rows/path parameters by design. It cannot recreate content.
    """

    export_root = _absolute_path(root, label="legacy shadow export root")
    validate_legacy_shadow_filesystem_contract(
        export_root,
        policy=dependencies.filesystem_policy,
    )
    try:
        root_descriptor = _open_directory_fd(
            export_root,
            label="legacy shadow export root",
        )
    except (OSError, LegacyShadowExportError) as exc:
        raise LegacyShadowExportUnavailableError(
            "legacy shadow recovery root is unavailable"
        ) from exc
    session_name = trade_date.isoformat()
    try:
        names = tuple(os.listdir(root_descriptor))
        if session_name in names:
            accepted = _load_accepted_legacy_shadow_session(
                export_root=export_root,
                session=export_root / session_name,
                trade_date=trade_date,
                expected_source_id=expected_source_id,
                expected_commit=expected_commit,
                allowed_modes=_signed_session_modes(dependencies.filesystem_policy),
                recovery_verifier=dependencies.recovery_verifier,
            )
            return accepted.session_path
        candidates = tuple(
            sorted(name for name in names if name.startswith(f".staging-{trade_date.isoformat()}-"))
        )
        if len(candidates) != 1:
            message = (
                "legacy shadow recovery staging batch is unavailable"
                if not candidates
                else "legacy shadow recovery has conflicting staging batches"
            )
            raise LegacyShadowExportUnavailableError(message)
        staging_name = candidates[0]
        try:
            dependencies.recovery_signer.resume(
                LegacyShadowRecoveryResumeBinding(
                    trade_date=trade_date,
                    source_id=expected_source_id,
                    producer_commit=expected_commit,
                    staging_name=staging_name,
                ),
                staging_root=export_root,
            )
        except Exception as exc:
            raise LegacyShadowExportUnavailableError(
                "legacy shadow recovery transaction is unavailable"
            ) from exc
        staging_descriptor = _open_child_directory_at(
            root_descriptor,
            staging_name,
            label="legacy shadow recovery staging",
            allowed_modes=_signed_session_modes(dependencies.filesystem_policy),
        )
        try:
            marker = _load_recovery_marker_at(
                staging_descriptor,
                verifier=dependencies.recovery_verifier,
            )
            finalization = _load_finalization_receipt_at(
                staging_descriptor,
                verifier=dependencies.recovery_verifier,
            )
            _verify_recovery_marker_batch_at(staging_descriptor, marker)
            _verify_finalization_receipt_batch_at(
                staging_descriptor,
                marker=marker,
                receipt=finalization,
            )
        finally:
            os.close(staging_descriptor)
        if marker.claims.staging_name != staging_name:
            raise LegacyShadowExportError("legacy shadow recovery staging name differs")
        if marker.claims.source_id != expected_source_id:
            raise LegacyShadowExportError("legacy shadow recovery source differs")
        if marker.claims.producer_commit != expected_commit:
            raise LegacyShadowExportError("legacy shadow recovery producer commit differs")
        _validate_recovery_clock(marker, dependencies=dependencies)
        _load_accepted_legacy_shadow_session(
            export_root=export_root,
            session=export_root / staging_name,
            trade_date=trade_date,
            expected_source_id=expected_source_id,
            expected_commit=expected_commit,
            allowed_modes=_signed_session_modes(dependencies.filesystem_policy),
            recovery_verifier=dependencies.recovery_verifier,
        )
        try:
            os.rename(
                staging_name,
                session_name,
                src_dir_fd=root_descriptor,
                dst_dir_fd=root_descriptor,
            )
        except FileExistsError:
            raise LegacyShadowExportConflictError(
                "legacy shadow recovery conflicts with an existing session"
            ) from None
        os.fsync(root_descriptor)
        return export_root / session_name
    finally:
        os.close(root_descriptor)


@dataclass(frozen=True)
class _PreparedLegacyRecord:
    payload: dict[str, object]
    encoded: bytes


@dataclass(frozen=True)
class LegacyMonitorCaptureSpool:
    path: Path
    trade_date: date
    records_count: int
    content_sha256: str
    content_bytes: int


def prepare_legacy_monitor_spool(
    *,
    root: Path,
    trade_date: date,
    rows: Iterable[Mapping[str, object]],
    filesystem_policy: LegacyShadowFilesystemPolicy,
) -> LegacyMonitorCaptureSpool:
    """Page validated monitor rows into one bounded file while DuckDB is still open."""

    export_root = _ensure_private_root(root, filesystem_policy=filesystem_policy)
    root_descriptor = _open_directory_fd(export_root, label="legacy monitor spool root")
    filename = f".capture-{trade_date.isoformat()}-{uuid.uuid4().hex}.jsonl"
    descriptor = _open_new_file_at(root_descriptor, filename)
    digest = hashlib.sha256()
    count = 0
    content_bytes = 0
    _session_open, session_close = shadow_session_boundaries(trade_date)
    try:
        for prepared in _monitor_records(
            rows,
            trade_date=trade_date,
            captured_at=session_close,
        ):
            count += 1
            line = prepared.encoded + b"\n"
            if content_bytes + len(line) > _MAX_EXPORT_BYTES:
                raise LegacyShadowExportError("legacy monitor spool exceeds byte budget")
            _write_all(descriptor, line)
            digest.update(line)
            content_bytes += len(line)
        os.fchmod(descriptor, 0o600)
        os.fsync(descriptor)
        os.fsync(root_descriptor)
        return LegacyMonitorCaptureSpool(
            path=export_root / filename,
            trade_date=trade_date,
            records_count=count,
            content_sha256=digest.hexdigest(),
            content_bytes=content_bytes,
        )
    except BaseException:
        try:
            os.unlink(filename, dir_fd=root_descriptor)
            os.fsync(root_descriptor)
        except FileNotFoundError:
            pass
        raise
    finally:
        os.close(descriptor)
        os.close(root_descriptor)


def prepare_legacy_monitor_production_spool(
    *,
    data_dir: Path,
    trade_date: date,
    rows: Iterable[Mapping[str, object]],
) -> LegacyMonitorCaptureSpool:
    return prepare_legacy_monitor_spool(
        root=_production_export_root(data_dir, source="monitor"),
        trade_date=trade_date,
        rows=rows,
        filesystem_policy=LegacyShadowFilesystemPolicy(mode="linux-production"),
    )


def _monitor_spool_records(spool: LegacyMonitorCaptureSpool) -> Iterator[_PreparedLegacyRecord]:
    verified = LegacyMonitorCaptureSpool(
        path=_absolute_path(spool.path, label="legacy monitor capture spool"),
        trade_date=spool.trade_date,
        records_count=spool.records_count,
        content_sha256=spool.content_sha256,
        content_bytes=spool.content_bytes,
    )
    with _open_bound_regular(
        verified.path,
        label="legacy monitor capture spool",
        maximum_bytes=_MAX_EXPORT_BYTES,
        required_mode=0o600,
        unavailable_error=LegacyShadowExportError,
    ) as source:
        assert source is not None
        if source.observed.st_size != verified.content_bytes:
            raise LegacyShadowExportError("legacy monitor capture spool size mismatch")
        digest = hashlib.sha256()
        buffer = bytearray()
        count = 0
        completed = False
        while not completed:
            chunk = os.read(source.descriptor, _READ_CHUNK_BYTES)
            if chunk:
                digest.update(chunk)
                buffer.extend(chunk)
            else:
                completed = True
            while True:
                newline = buffer.find(b"\n")
                if newline < 0:
                    if completed and buffer:
                        raise LegacyShadowExportError(
                            "legacy monitor capture spool has a partial record"
                        )
                    break
                raw_line = bytes(buffer[:newline])
                del buffer[: newline + 1]
                count += 1
                if count > verified.records_count or len(raw_line) > _MAX_RECORD_BYTES:
                    raise LegacyShadowExportError(
                        "legacy monitor capture spool exceeds its record budget"
                    )
                try:
                    event = LegacyMonitorEvent.model_validate(strict_canonical_json_loads(raw_line))
                except (StrictJsonError, ValueError) as exc:
                    raise LegacyShadowExportError(
                        "legacy monitor capture spool record is invalid"
                    ) from exc
                if event.trade_date != verified.trade_date:
                    raise LegacyShadowExportError(
                        "legacy monitor capture spool trade date mismatch"
                    )
                payload = event.model_dump(mode="json")
                yield _PreparedLegacyRecord(payload=payload, encoded=canonical_json_bytes(payload))
        if count != verified.records_count or digest.hexdigest() != verified.content_sha256:
            raise LegacyShadowExportError("legacy monitor capture spool identity mismatch")


def _discard_monitor_spool(spool: LegacyMonitorCaptureSpool) -> None:
    parent_descriptor = _open_directory_fd(
        spool.path.parent,
        label="legacy monitor capture spool parent",
    )
    try:
        os.unlink(spool.path.name, dir_fd=parent_descriptor)
        os.fsync(parent_descriptor)
    except FileNotFoundError:
        pass
    finally:
        os.close(parent_descriptor)


def _monitor_records(
    rows: Iterable[Mapping[str, object]],
    *,
    trade_date: date,
    captured_at: datetime,
) -> Iterator[_PreparedLegacyRecord]:
    latest_event: datetime | None = None
    raw_bytes = 0
    for count, row in enumerate(rows, start=1):
        if count > _MAX_RECORDS:
            raise LegacyShadowExportError("legacy monitor record count exceeds budget")
        event = LegacyMonitorEvent.model_validate(row)
        if event.trade_date != trade_date:
            raise ValueError("legacy monitor row trade_date does not match export")
        event_at = event.trigger_time.replace(tzinfo=_CST).astimezone(UTC)
        latest_event = event_at if latest_event is None else max(latest_event, event_at)
        if captured_at < latest_event:
            raise ValueError("legacy monitor export captured_at precedes an event")
        payload = event.model_dump(mode="json")
        encoded = canonical_json_bytes(payload)
        if len(encoded) > _MAX_RECORD_BYTES:
            raise LegacyShadowExportError("legacy monitor record exceeds byte budget")
        raw_bytes += len(encoded)
        if raw_bytes > _MAX_EXPORT_BYTES:
            raise LegacyShadowExportError("legacy monitor total byte budget exceeded")
        yield _PreparedLegacyRecord(payload=payload, encoded=encoded)


def _surge_records(path: Path) -> Iterator[_PreparedLegacyRecord]:
    with _open_bound_regular(
        path,
        label="legacy surge source",
        maximum_bytes=_MAX_EXPORT_BYTES,
        required_mode=None,
        unavailable_error=LegacyShadowExportError,
        missing_ok=True,
    ) as source:
        if source is None:
            return
        buffer = bytearray()
        record_count = 0
        canonical_bytes = 0
        completed = False
        while not completed:
            chunk = os.read(source.descriptor, _READ_CHUNK_BYTES)
            if chunk:
                buffer.extend(chunk)
                if len(buffer) > _MAX_RECORD_BYTES and b"\n" not in buffer[: _MAX_RECORD_BYTES + 1]:
                    raise LegacyShadowExportError("legacy surge record exceeds byte budget")
            else:
                completed = True
            while True:
                newline = buffer.find(b"\n")
                if newline < 0:
                    if not completed:
                        break
                    raw_line = bytes(buffer)
                    buffer.clear()
                else:
                    raw_line = bytes(buffer[:newline])
                    del buffer[: newline + 1]
                if raw_line:
                    record_count += 1
                    if record_count > _MAX_RECORDS:
                        raise LegacyShadowExportError("legacy surge record count exceeds budget")
                    if len(raw_line) > _MAX_RECORD_BYTES:
                        raise LegacyShadowExportError("legacy surge record exceeds byte budget")
                    try:
                        event = LegacySurgeEvent.model_validate(strict_json_loads(raw_line))
                    except (StrictJsonError, ValueError) as exc:
                        raise LegacyShadowExportError(
                            "legacy surge source record is invalid"
                        ) from exc
                    payload = event.model_dump(mode="json")
                    encoded = canonical_json_bytes(payload)
                    canonical_bytes += len(encoded) + 1
                    if canonical_bytes > _MAX_EXPORT_BYTES:
                        raise LegacyShadowExportError("legacy surge total byte budget exceeded")
                    yield _PreparedLegacyRecord(payload=payload, encoded=encoded)
                if newline < 0:
                    break


def _legacy_stream_input_identity(
    records: Iterable[_PreparedLegacyRecord],
    *,
    source_id: str,
    trade_date: date,
    format: Literal["json-array", "jsonl"],
) -> str:
    digest = hashlib.sha256()
    record_count = 0
    raw_bytes = 0
    for record in records:
        if format == "json-array":
            digest.update(len(record.encoded).to_bytes(8, "big"))
            digest.update(record.encoded)
            raw_bytes += len(record.encoded)
        else:
            line = record.encoded + b"\n"
            digest.update(line)
            raw_bytes += len(line)
        record_count += 1
    if format == "json-array":
        contract = "shadow-legacy-record-stream/v2"
        descriptor: object = {"source_id": source_id, "trade_date": trade_date}
    else:
        contract = "shadow-legacy-surge-raw-input/v2"
        descriptor = {
            "source_id": source_id,
            "trade_date": trade_date,
            "format": "jsonl",
        }
    return canonical_sha256(
        {
            "contract": contract,
            "descriptor": descriptor,
            "records_sha256": digest.hexdigest(),
            "record_count": record_count,
            "raw_bytes": raw_bytes,
        }
    )


def _discard_directory_at(root_descriptor: int, name: str) -> None:
    if not name.startswith((".building-", ".staging-")):
        return
    try:
        descriptor = _open_child_directory_at(
            root_descriptor,
            name,
            label="legacy shadow unpublished directory",
            allowed_modes=frozenset({_ROOT_MODE}),
        )
    except LegacyShadowExportUnavailableError:
        return
    try:
        filenames = tuple(os.listdir(descriptor))
        if _RECOVERY_MARKER_FILENAME in filenames:
            return
        for filename in filenames:
            os.unlink(filename, dir_fd=descriptor)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.rmdir(name, dir_fd=root_descriptor)
    os.fsync(root_descriptor)


def _publish_legacy_stream_secure(
    *,
    root: Path,
    trade_date: date,
    source_id: Literal["legacy-monitor-events", "legacy-surge-jsonl"],
    producer_commit: str,
    producer_version: str,
    as_of: datetime,
    records_filename: Literal["events.json", "events.jsonl"],
    record_factory: Callable[[datetime], Iterable[_PreparedLegacyRecord]],
    surge_collection_proof: LegacySurgeCollectionProof | None,
    dependencies: _LegacyShadowDependencies,
    fault_hook: ExportFaultHook | None,
) -> Path:
    export_root = _ensure_private_root(
        root,
        filesystem_policy=dependencies.filesystem_policy,
    )
    format: Literal["json-array", "jsonl"] = (
        "json-array" if records_filename == "events.json" else "jsonl"
    )
    root_descriptor = _open_directory_fd(export_root, label="legacy shadow export root")
    session_name = trade_date.isoformat()
    names = tuple(os.listdir(root_descriptor))
    if session_name in names:
        os.close(root_descriptor)
        accepted = load_accepted_legacy_shadow_export(
            root=export_root,
            trade_date=trade_date,
            expected_source_id=source_id,
            expected_commit=producer_commit,
            recovery_verifier=dependencies.recovery_verifier,
            filesystem_policy=dependencies.filesystem_policy,
        )
        candidate_identity = _legacy_stream_input_identity(
            record_factory(accepted.manifest.captured_at),
            source_id=source_id,
            trade_date=trade_date,
            format=format,
        )
        if (
            accepted.manifest.input_identity != candidate_identity
            or accepted.manifest.producer_version != producer_version
        ):
            raise LegacyShadowExportConflictError(
                "legacy shadow immutable session conflicts with new evidence"
            )
        return accepted.session_path
    if any(name.startswith(f".staging-{trade_date.isoformat()}-") for name in names):
        os.close(root_descriptor)
        return recover_legacy_shadow_export(
            root=export_root,
            trade_date=trade_date,
            expected_source_id=source_id,
            expected_commit=producer_commit,
            dependencies=dependencies,
        )
    nonce = uuid.uuid4().hex
    building_name = f".building-{trade_date.isoformat()}-{nonce}"
    staging_name = f".staging-{trade_date.isoformat()}-{nonce}"
    os.mkdir(building_name, _ROOT_MODE, dir_fd=root_descriptor)
    os.fsync(root_descriptor)
    building_descriptor = _open_child_directory_at(
        root_descriptor,
        building_name,
        label="legacy shadow building directory",
        allowed_modes=frozenset({_ROOT_MODE}),
    )
    raw_descriptor = -1
    envelope_descriptor = -1
    try:
        capture = _capture_recovery_marker_start(
            trade_date=trade_date,
            source_id=source_id,
            producer_commit=producer_commit,
            producer_version=producer_version,
            staging_name=staging_name,
            signer=dependencies.recovery_signer,
        )
        captured_at = capture.claims.captured_at
        records = record_factory(captured_at)
        raw_descriptor = _open_new_file_at(building_descriptor, records_filename)
        envelope_descriptor = _open_new_file_at(building_descriptor, "records.jsonl")
        raw_digest = hashlib.sha256()
        envelope_digest = hashlib.sha256()
        input_digest = hashlib.sha256()
        raw_input_bytes = 0
        total_bytes = 0
        record_count = 0
        prefix = b"[" if format == "json-array" else b""
        if prefix:
            _write_all(raw_descriptor, prefix)
            raw_digest.update(prefix)
            total_bytes += len(prefix)
        for record in records:
            record_count += 1
            if record_count > _MAX_RECORDS:
                raise LegacyShadowExportError("legacy shadow record count exceeds budget")
            if format == "json-array":
                raw_chunk = (b"," if record_count > 1 else b"") + record.encoded
                input_digest.update(len(record.encoded).to_bytes(8, "big"))
                input_digest.update(record.encoded)
                raw_input_bytes += len(record.encoded)
            else:
                raw_chunk = record.encoded + b"\n"
                input_digest.update(raw_chunk)
                raw_input_bytes += len(raw_chunk)
            identity = {
                "contract": _RECORD_CONTRACT,
                "source_id": source_id,
                "trade_date": trade_date,
                "producer_commit": producer_commit,
                "captured_at": captured_at,
                "as_of": as_of,
                "sequence": record_count,
                "payload": record.payload,
            }
            record_digest = canonical_sha256(identity)
            envelope = LegacyShadowExportRecord(
                record_id=record_digest,
                record_sha256=record_digest,
                source_id=source_id,
                trade_date=trade_date,
                producer_commit=producer_commit,
                captured_at=captured_at,
                as_of=as_of,
                sequence=record_count,
                payload=record.payload,
            )
            envelope_chunk = canonical_model_json_bytes(envelope) + b"\n"
            if total_bytes + len(raw_chunk) + len(envelope_chunk) > _MAX_EXPORT_BYTES:
                raise LegacyShadowExportError("legacy shadow total byte budget exceeded")
            _write_all(raw_descriptor, raw_chunk)
            _write_all(envelope_descriptor, envelope_chunk)
            raw_digest.update(raw_chunk)
            envelope_digest.update(envelope_chunk)
            total_bytes += len(raw_chunk) + len(envelope_chunk)
        suffix = b"]" if format == "json-array" else b""
        if total_bytes + len(suffix) > _MAX_EXPORT_BYTES:
            raise LegacyShadowExportError("legacy shadow total byte budget exceeded")
        if suffix:
            _write_all(raw_descriptor, suffix)
            raw_digest.update(suffix)
            total_bytes += len(suffix)
        _finish_stream_file(raw_descriptor)
        _finish_stream_file(envelope_descriptor)
        os.close(raw_descriptor)
        raw_descriptor = -1
        os.close(envelope_descriptor)
        envelope_descriptor = -1

        identity_contract = (
            "shadow-legacy-record-stream/v2"
            if format == "json-array"
            else "shadow-legacy-surge-raw-input/v2"
        )
        descriptor: object = (
            {"source_id": source_id, "trade_date": trade_date}
            if format == "json-array"
            else {
                "source_id": source_id,
                "trade_date": trade_date,
                "format": "jsonl",
            }
        )
        input_identity = canonical_sha256(
            {
                "contract": identity_contract,
                "descriptor": descriptor,
                "records_sha256": input_digest.hexdigest(),
                "record_count": record_count,
                "raw_bytes": raw_input_bytes,
            }
        )
        receipt = ShadowSourceCompletionReceipt(
            evidence_origin="production",
            source="legacy",
            source_id=source_id,
            trade_date=trade_date,
            session_close_at=as_of,
            complete_through=as_of,
            input_identity=input_identity,
            produced_at=captured_at,
            producer_commit=producer_commit,
            producer_version=producer_version,
        )
        completion_payload = _completion_payload(receipt)
        manifest = _manifest(
            source_id=source_id,
            trade_date=trade_date,
            producer_commit=producer_commit,
            producer_version=producer_version,
            captured_at=captured_at,
            as_of=as_of,
            records_filename=records_filename,
            records_sha256=raw_digest.hexdigest(),
            records_count=record_count,
            record_envelopes_sha256=envelope_digest.hexdigest(),
            completion_payload=completion_payload,
            receipt=receipt,
            surge_collection_proof=surge_collection_proof,
        )
        manifest_payload = canonical_model_json_bytes(manifest)
        if total_bytes + len(completion_payload) + len(manifest_payload) > _MAX_EXPORT_BYTES:
            raise LegacyShadowExportError("legacy shadow total byte budget exceeded")
        _write_new_file_at(building_descriptor, "completion.json", completion_payload)
        _write_new_file_at(building_descriptor, "manifest.json", manifest_payload)
        os.fsync(building_descriptor)
        os.close(building_descriptor)
        building_descriptor = -1
        os.rename(
            building_name,
            staging_name,
            src_dir_fd=root_descriptor,
            dst_dir_fd=root_descriptor,
        )
        os.fsync(root_descriptor)
        staging_descriptor = _open_child_directory_at(
            root_descriptor,
            staging_name,
            label="legacy shadow complete staging",
            allowed_modes=frozenset({_ROOT_MODE}),
        )
        try:
            batch_digest = _batch_digest_at(
                staging_descriptor,
                _marker_artifact_names(source_id),
            )
        finally:
            os.close(staging_descriptor)
        marker = _issue_recovery_marker(
            trade_date=trade_date,
            source_id=source_id,
            producer_commit=producer_commit,
            producer_version=producer_version,
            input_identity=input_identity,
            staging_name=staging_name,
            batch_digest=batch_digest,
            surge_collection_proof_id=(
                None if surge_collection_proof is None else surge_collection_proof.proof_id
            ),
            runner_manifest_binding_id=None,
            capture=capture,
            staging_root=export_root,
            signer=dependencies.recovery_signer,
            verifier=dependencies.recovery_verifier,
        )
        staging_descriptor = _open_child_directory_at(
            root_descriptor,
            staging_name,
            label="legacy shadow signed staging",
            allowed_modes=_signed_session_modes(dependencies.filesystem_policy),
        )
        try:
            _verify_recovery_marker_batch_at(staging_descriptor, marker)
        finally:
            os.close(staging_descriptor)
        if fault_hook is not None:
            fault_hook("before_publish")
        os.rename(
            staging_name,
            session_name,
            src_dir_fd=root_descriptor,
            dst_dir_fd=root_descriptor,
        )
        os.fsync(root_descriptor)
        if fault_hook is not None:
            fault_hook("after_publish")
        return export_root / session_name
    except BaseException:
        if building_descriptor >= 0:
            os.close(building_descriptor)
            building_descriptor = -1
        _discard_directory_at(root_descriptor, building_name)
        _discard_directory_at(root_descriptor, staging_name)
        raise
    finally:
        if raw_descriptor >= 0:
            os.close(raw_descriptor)
        if envelope_descriptor >= 0:
            os.close(envelope_descriptor)
        if building_descriptor >= 0:
            os.close(building_descriptor)
        os.close(root_descriptor)


def _legacy_export_times(
    *,
    trade_date: date,
    dependencies: _LegacyShadowDependencies,
) -> tuple[datetime, int, str, datetime]:
    captured_at = normalize_aware_utc(dependencies.wall_clock())
    captured_monotonic_ns = dependencies.monotonic_ns()
    captured_boot_id = dependencies.boot_id()
    _session_open, session_close = shadow_session_boundaries(trade_date)
    return captured_at, captured_monotonic_ns, captured_boot_id, session_close


def _production_export_root(data_dir: Path, *, source: str) -> Path:
    root = _absolute_path(data_dir, label="legacy shadow data directory")
    return root / "legacy-shadow" / source


def _production_export_commit(environment: Mapping[str, str] | None) -> str:
    values = os.environ if environment is None else environment
    candidate = values.get("RQUANT_CODE_COMMIT", "").strip().lower()
    if len(candidate) != 40 or any(character not in "0123456789abcdef" for character in candidate):
        raise LegacyShadowExportError("RQUANT_CODE_COMMIT must be a full lowercase commit SHA")
    return candidate


def _linux_boot_id() -> str:
    descriptor = -1
    try:
        descriptor = os.open(
            "/proc/sys/kernel/random/boot_id",
            os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
        )
        observed = os.fstat(descriptor)
        if not stat.S_ISREG(observed.st_mode) or observed.st_size > 128:
            raise LegacyShadowExportError("Linux boot id source is unsafe")
        value = os.read(descriptor, 129).decode("ascii").strip().lower()
    except (OSError, UnicodeDecodeError) as exc:
        raise LegacyShadowExportError("Linux boot id is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise LegacyShadowExportError("Linux boot id is invalid") from exc
    return str(parsed)


def _production_export_dependencies(
    data_dir: Path,
    *,
    environment: Mapping[str, str] | None,
) -> _ProductionLegacyShadowDependencies:
    """Build fixed production dependencies; there is no clock or filesystem override."""

    if sys.platform != "linux":
        raise LegacyShadowExportError("legacy shadow production export requires Linux")
    from rquant.runtime_builder_shadow import ShadowSessionSettings
    from rquant.runtime_deployment_profile import load_current_runtime_deployment_profile
    from rquant.runtime_service_entrypoint import RuntimeServiceKind

    normalized_data_dir = _absolute_path(data_dir, label="legacy shadow data directory")
    profile = load_current_runtime_deployment_profile(normalized_data_dir / "runtime")
    expected_commit = _production_export_commit(environment)
    if profile.producer_commit != expected_commit:
        raise LegacyShadowExportError(
            "runtime deployment profile commit differs from legacy shadow exporter"
        )
    manifests = tuple(
        manifest
        for manifest in profile.manifests
        if manifest.service_kind is RuntimeServiceKind.SHADOW_SESSION
    )
    if len(manifests) != 1:
        raise LegacyShadowExportError(
            "runtime deployment profile lacks one Shadow signing authority"
        )
    settings = ShadowSessionSettings.model_validate(dict(manifests[0].settings))
    client = SecureShadowSigningClient(
        command=settings.signer_command,
        key_id=settings.report_active_key_id,
        timeout_seconds=settings.signer_timeout_seconds,
    )
    verifier = Ed25519LegacyShadowRecoveryKeyring(
        active_key_id=settings.report_active_key_id,
        active_public_key=settings.report_active_public_key_pem.encode("utf-8"),
        previous_public_keys={
            key_id: value.encode("utf-8")
            for key_id, value in settings.report_previous_public_key_pems.items()
        },
    )
    return _ProductionLegacyShadowDependencies(
        wall_clock=lambda: datetime.now(UTC),
        monotonic_ns=time.monotonic_ns,
        boot_id=_linux_boot_id,
        recovery_signer=Ed25519LegacyShadowRecoverySigner(
            key_id=settings.report_active_key_id,
            client=client,
        ),
        recovery_verifier=verifier,
        filesystem_policy=LegacyShadowFilesystemPolicy(mode="linux-production"),
    )


def publish_legacy_monitor_export(
    *,
    root: Path,
    trade_date: date,
    rows: Iterable[Mapping[str, object]],
    producer_commit: str,
    producer_version: str,
    dependencies: LegacyShadowTestDependencies,
    fault_hook: ExportFaultHook | None = None,
) -> Path:
    """Atomically publish one closed legacy-monitor session without touching DuckDB."""

    captured_at, _captured_monotonic_ns, _captured_boot_id, as_of = _legacy_export_times(
        trade_date=trade_date,
        dependencies=dependencies,
    )
    if not _in_publish_window(trade_date=trade_date, captured_at=captured_at):
        return recover_legacy_shadow_export(
            root=root,
            trade_date=trade_date,
            expected_source_id="legacy-monitor-events",
            expected_commit=producer_commit,
            dependencies=dependencies,
        )
    return _publish_legacy_stream_secure(
        root=root,
        trade_date=trade_date,
        source_id="legacy-monitor-events",
        producer_commit=producer_commit,
        producer_version=producer_version,
        as_of=as_of,
        records_filename="events.json",
        record_factory=lambda trusted_captured_at: _monitor_records(
            rows,
            trade_date=trade_date,
            captured_at=trusted_captured_at,
        ),
        surge_collection_proof=None,
        dependencies=dependencies,
        fault_hook=fault_hook,
    )


def publish_legacy_monitor_production_export(
    *,
    data_dir: Path,
    trade_date: date,
    spool: LegacyMonitorCaptureSpool,
    environment: Mapping[str, str] | None = None,
) -> Path:
    """Publish the old monitor's closed authoritative rows to its shadow-only root."""

    dependencies = _production_export_dependencies(data_dir, environment=environment)
    root = _production_export_root(data_dir, source="monitor")
    if spool.trade_date != trade_date or spool.path.parent != root:
        raise LegacyShadowExportError("legacy monitor capture spool binding mismatch")
    captured_at, _captured_monotonic_ns, _captured_boot_id, as_of = _legacy_export_times(
        trade_date=trade_date,
        dependencies=dependencies,
    )
    if not _in_publish_window(trade_date=trade_date, captured_at=captured_at):
        return recover_legacy_shadow_export(
            root=root,
            trade_date=trade_date,
            expected_source_id="legacy-monitor-events",
            expected_commit=_production_export_commit(environment),
            dependencies=dependencies,
        )
    published = _publish_legacy_stream_secure(
        root=root,
        trade_date=trade_date,
        source_id="legacy-monitor-events",
        producer_commit=_production_export_commit(environment),
        producer_version="legacy-monitor-shadow-export/v1",
        as_of=as_of,
        records_filename="events.json",
        record_factory=lambda _trusted_captured_at: _monitor_spool_records(spool),
        surge_collection_proof=None,
        dependencies=dependencies,
        fault_hook=None,
    )
    _discard_monitor_spool(spool)
    return published


def _surge_records_payload(records: tuple[dict[str, object], ...]) -> bytes:
    return b"".join(canonical_json_bytes(record) + b"\n" for record in records)


def _surge_input_identity(*, records_payload: bytes, trade_date: date) -> str:
    digest = hashlib.sha256()
    lines = records_payload.splitlines(keepends=True)
    for raw_line in lines:
        digest.update(raw_line)
    return canonical_sha256(
        {
            "contract": "shadow-legacy-surge-raw-input/v2",
            "descriptor": {
                "source_id": "legacy-surge-jsonl",
                "trade_date": trade_date,
                "format": "jsonl",
            },
            "records_sha256": digest.hexdigest(),
            "record_count": len(lines),
            "raw_bytes": len(records_payload),
        }
    )


def publish_legacy_surge_export(
    *,
    root: Path,
    trade_date: date,
    events_path: Path,
    producer_commit: str,
    producer_version: str,
    collection_proof: LegacySurgeCollectionProof,
    dependencies: LegacyShadowTestDependencies,
    fault_hook: ExportFaultHook | None = None,
) -> Path:
    """Copy the closed surge-watch JSONL into the immutable Shadow contract."""

    captured_at, _captured_monotonic_ns, _captured_boot_id, as_of = _legacy_export_times(
        trade_date=trade_date,
        dependencies=dependencies,
    )
    if not _in_publish_window(trade_date=trade_date, captured_at=captured_at):
        return recover_legacy_shadow_export(
            root=root,
            trade_date=trade_date,
            expected_source_id="legacy-surge-jsonl",
            expected_commit=producer_commit,
            dependencies=dependencies,
        )
    return _publish_legacy_stream_secure(
        root=root,
        trade_date=trade_date,
        source_id="legacy-surge-jsonl",
        producer_commit=producer_commit,
        producer_version=producer_version,
        as_of=as_of,
        records_filename="events.jsonl",
        record_factory=lambda _trusted_captured_at: _surge_records(events_path),
        surge_collection_proof=LegacySurgeCollectionProof.model_validate(collection_proof),
        dependencies=dependencies,
        fault_hook=fault_hook,
    )


def publish_legacy_surge_production_export(
    *,
    data_dir: Path,
    trade_date: date,
    events_path: Path,
    collection_proof: LegacySurgeCollectionProof,
    environment: Mapping[str, str] | None = None,
) -> Path:
    """Publish the old surge-watch's closed authoritative JSONL to shadow only."""

    dependencies = _production_export_dependencies(data_dir, environment=environment)
    root = _production_export_root(data_dir, source="surge")
    captured_at, _captured_monotonic_ns, _captured_boot_id, as_of = _legacy_export_times(
        trade_date=trade_date,
        dependencies=dependencies,
    )
    if not _in_publish_window(trade_date=trade_date, captured_at=captured_at):
        return recover_legacy_shadow_export(
            root=root,
            trade_date=trade_date,
            expected_source_id="legacy-surge-jsonl",
            expected_commit=_production_export_commit(environment),
            dependencies=dependencies,
        )
    return _publish_legacy_stream_secure(
        root=root,
        trade_date=trade_date,
        source_id="legacy-surge-jsonl",
        producer_commit=_production_export_commit(environment),
        producer_version="legacy-surge-shadow-export/v1",
        as_of=as_of,
        records_filename="events.jsonl",
        record_factory=lambda _trusted_captured_at: _surge_records(events_path),
        surge_collection_proof=LegacySurgeCollectionProof.model_validate(collection_proof),
        dependencies=dependencies,
        fault_hook=None,
    )


def _completed_runner_batch(
    source: ShadowRunnerSignalSource,
    *,
    trade_date: date,
    receipt: ShadowSourceCompletionReceipt,
) -> RunnerSignalBatch:
    if receipt.high_watermark is None:
        raise LegacyShadowExportError("isolated runner completion has no high watermark")
    cursor = 0
    descriptor = None
    next_sequence: int | None = None
    records = []
    raw_bytes = 0
    while True:
        batch = source.read_completed_batch(
            trade_date=trade_date,
            after_sequence=cursor,
            limit=1_000,
        )
        if batch.after_sequence != cursor or batch.limit != 1_000:
            raise LegacyShadowExportError("isolated runner did not honor export read bounds")
        observed_descriptor = batch.snapshot.descriptor
        if descriptor is None:
            descriptor = observed_descriptor
            next_sequence = descriptor.first_sequence
            if descriptor.high_watermark != receipt.high_watermark:
                raise LegacyShadowExportError("isolated runner watermark differs from completion")
        elif observed_descriptor != descriptor:
            raise LegacyShadowExportError("isolated runner snapshot changed while exported")
        for record in batch.records:
            if (
                next_sequence is None
                or record.sequence != next_sequence
                or record.sequence > receipt.high_watermark
            ):
                raise LegacyShadowExportError("isolated runner completion sequence is invalid")
            if len(records) >= _MAX_RECORDS:
                raise LegacyShadowExportError("isolated runner record count exceeds budget")
            encoded = canonical_model_json_bytes(record)
            if len(encoded) > _MAX_RECORD_BYTES:
                raise LegacyShadowExportError("isolated runner record exceeds byte budget")
            raw_bytes += len(encoded)
            if raw_bytes > _MAX_EXPORT_BYTES:
                raise LegacyShadowExportError("isolated runner total byte budget exceeded")
            records.append(record)
            cursor = record.sequence
            next_sequence += 1
        if cursor == receipt.high_watermark:
            break
        if not batch.records:
            raise LegacyShadowExportError("isolated runner completion is partial")
    if descriptor is None:
        raise LegacyShadowExportError("isolated runner completion has no snapshot")
    return RunnerSignalBatch(
        snapshot=batch.snapshot,
        after_sequence=0,
        limit=max(1, len(records)),
        records=tuple(records),
    )


def _publish_isolated_secure(
    *,
    root: Path,
    trade_date: date,
    source_id: str,
    producer_commit: str,
    producer_version: str,
    as_of: datetime,
    records_payload: bytes,
    records: Iterable[object],
    records_count: int,
    input_identity: str,
    completion_receipt: ShadowSourceCompletionReceipt,
    runner_manifest_binding: LegacyShadowRunnerManifestBinding,
    dependencies: _LegacyShadowDependencies,
    fault_hook: ExportFaultHook | None,
) -> Path:
    export_root = _ensure_private_root(
        root,
        filesystem_policy=dependencies.filesystem_policy,
    )
    if len(records_payload) > _MAX_EXPORT_BYTES or records_count > _MAX_RECORDS:
        raise LegacyShadowExportError("isolated runner export exceeds its budget")
    root_descriptor = _open_directory_fd(export_root, label="isolated shadow export root")
    session_name = trade_date.isoformat()
    names = tuple(os.listdir(root_descriptor))
    if session_name in names:
        os.close(root_descriptor)
        accepted = load_accepted_legacy_shadow_export(
            root=export_root,
            trade_date=trade_date,
            expected_source_id=source_id,
            expected_commit=producer_commit,
            recovery_verifier=dependencies.recovery_verifier,
            filesystem_policy=dependencies.filesystem_policy,
        )
        if (
            accepted.manifest.input_identity != input_identity
            or accepted.manifest.producer_version != producer_version
        ):
            raise LegacyShadowExportConflictError(
                "isolated shadow immutable session conflicts with new evidence"
            )
        return accepted.session_path
    if any(name.startswith(f".staging-{trade_date.isoformat()}-") for name in names):
        os.close(root_descriptor)
        return recover_legacy_shadow_export(
            root=export_root,
            trade_date=trade_date,
            expected_source_id=source_id,
            expected_commit=producer_commit,
            dependencies=dependencies,
        )
    nonce = uuid.uuid4().hex
    building_name = f".building-{trade_date.isoformat()}-{nonce}"
    staging_name = f".staging-{trade_date.isoformat()}-{nonce}"
    os.mkdir(building_name, _ROOT_MODE, dir_fd=root_descriptor)
    os.fsync(root_descriptor)
    building_descriptor = _open_child_directory_at(
        root_descriptor,
        building_name,
        label="isolated shadow building directory",
        allowed_modes=frozenset({_ROOT_MODE}),
    )
    envelope_descriptor = -1
    try:
        capture = _capture_recovery_marker_start(
            trade_date=trade_date,
            source_id=source_id,
            producer_commit=producer_commit,
            producer_version=producer_version,
            staging_name=staging_name,
            signer=dependencies.recovery_signer,
        )
        captured_at = capture.claims.captured_at
        receipt = _validate_receipt(
            completion_receipt,
            source="isolated",
            source_id=source_id,
            trade_date=trade_date,
            input_identity=input_identity,
            producer_commit=producer_commit,
            export_produced_at=captured_at,
            as_of=as_of,
        )
        _write_new_file_at(building_descriptor, "completed-batch.json", records_payload)
        envelope_descriptor = _open_new_file_at(building_descriptor, "records.jsonl")
        envelope_digest = hashlib.sha256()
        total_bytes = len(records_payload)
        observed_count = 0
        for sequence, record in enumerate(records, start=1):
            if sequence > records_count:
                raise LegacyShadowExportError("isolated runner record count changed")
            payload = (
                record.model_dump(mode="json")
                if isinstance(record, RuntimeContractModel)
                else dict(record)
                if isinstance(record, Mapping)
                else None
            )
            if payload is None:
                raise LegacyShadowExportError("isolated runner record is invalid")
            identity = {
                "contract": _RECORD_CONTRACT,
                "source_id": source_id,
                "trade_date": trade_date,
                "producer_commit": producer_commit,
                "captured_at": captured_at,
                "as_of": as_of,
                "sequence": sequence,
                "payload": payload,
            }
            digest = canonical_sha256(identity)
            chunk = (
                canonical_model_json_bytes(
                    LegacyShadowExportRecord(
                        record_id=digest,
                        record_sha256=digest,
                        source_id=source_id,
                        trade_date=trade_date,
                        producer_commit=producer_commit,
                        captured_at=captured_at,
                        as_of=as_of,
                        sequence=sequence,
                        payload=payload,
                    )
                )
                + b"\n"
            )
            if len(chunk) > _MAX_RECORD_BYTES or total_bytes + len(chunk) > _MAX_EXPORT_BYTES:
                raise LegacyShadowExportError("isolated runner export exceeds its budget")
            _write_all(envelope_descriptor, chunk)
            envelope_digest.update(chunk)
            total_bytes += len(chunk)
            observed_count = sequence
        if observed_count != records_count:
            raise LegacyShadowExportError("isolated runner record count changed")
        _finish_stream_file(envelope_descriptor)
        os.close(envelope_descriptor)
        envelope_descriptor = -1
        completion_payload = _completion_payload(receipt)
        manifest = _manifest(
            source_id=source_id,
            trade_date=trade_date,
            producer_commit=producer_commit,
            producer_version=producer_version,
            captured_at=captured_at,
            as_of=as_of,
            records_filename="completed-batch.json",
            records_sha256=hashlib.sha256(records_payload).hexdigest(),
            records_count=records_count,
            record_envelopes_sha256=envelope_digest.hexdigest(),
            completion_payload=completion_payload,
            receipt=receipt,
            runner_manifest_binding=runner_manifest_binding,
        )
        manifest_payload = canonical_model_json_bytes(manifest)
        if total_bytes + len(completion_payload) + len(manifest_payload) > _MAX_EXPORT_BYTES:
            raise LegacyShadowExportError("isolated runner export exceeds its budget")
        _write_new_file_at(building_descriptor, "completion.json", completion_payload)
        _write_new_file_at(building_descriptor, "manifest.json", manifest_payload)
        os.fsync(building_descriptor)
        os.close(building_descriptor)
        building_descriptor = -1
        os.rename(
            building_name,
            staging_name,
            src_dir_fd=root_descriptor,
            dst_dir_fd=root_descriptor,
        )
        os.fsync(root_descriptor)
        staging_descriptor = _open_child_directory_at(
            root_descriptor,
            staging_name,
            label="isolated shadow complete staging",
            allowed_modes=frozenset({_ROOT_MODE}),
        )
        try:
            batch_digest = _batch_digest_at(
                staging_descriptor,
                _marker_artifact_names(source_id),
            )
        finally:
            os.close(staging_descriptor)
        marker = _issue_recovery_marker(
            trade_date=trade_date,
            source_id=source_id,
            producer_commit=producer_commit,
            producer_version=producer_version,
            input_identity=input_identity,
            staging_name=staging_name,
            batch_digest=batch_digest,
            surge_collection_proof_id=None,
            runner_manifest_binding_id=runner_manifest_binding.binding_id,
            capture=capture,
            staging_root=export_root,
            signer=dependencies.recovery_signer,
            verifier=dependencies.recovery_verifier,
        )
        _validate_receipt(
            receipt,
            source="isolated",
            source_id=source_id,
            trade_date=trade_date,
            input_identity=input_identity,
            producer_commit=producer_commit,
            export_produced_at=marker.claims.produced_at,
            as_of=as_of,
        )
        staging_descriptor = _open_child_directory_at(
            root_descriptor,
            staging_name,
            label="isolated shadow signed staging",
            allowed_modes=_signed_session_modes(dependencies.filesystem_policy),
        )
        try:
            _verify_recovery_marker_batch_at(staging_descriptor, marker)
        finally:
            os.close(staging_descriptor)
        if fault_hook is not None:
            fault_hook("before_publish")
        os.rename(
            staging_name,
            session_name,
            src_dir_fd=root_descriptor,
            dst_dir_fd=root_descriptor,
        )
        os.fsync(root_descriptor)
        if fault_hook is not None:
            fault_hook("after_publish")
        return export_root / session_name
    except BaseException:
        if building_descriptor >= 0:
            os.close(building_descriptor)
            building_descriptor = -1
        _discard_directory_at(root_descriptor, building_name)
        _discard_directory_at(root_descriptor, staging_name)
        raise
    finally:
        if envelope_descriptor >= 0:
            os.close(envelope_descriptor)
        if building_descriptor >= 0:
            os.close(building_descriptor)
        os.close(root_descriptor)


def publish_isolated_runner_export(
    *,
    root: Path,
    strategy_id: str,
    trade_date: date,
    source: ShadowRunnerSignalSource,
    expected_commit: str,
    dependencies: LegacyShadowTestDependencies,
    fault_hook: ExportFaultHook | None = None,
) -> Path:
    """Mirror an already completed isolated runner without re-signing its evidence."""

    if not strategy_id or any(part in strategy_id for part in ("/", "\\", "..")):
        raise ValueError("isolated runner strategy_id is invalid")
    return _publish_isolated_runner_export(
        root=root,
        strategy_id=strategy_id,
        trade_date=trade_date,
        source=source,
        expected_commit=expected_commit,
        expected_runner_binding=None,
        dependencies=dependencies,
        fault_hook=fault_hook,
    )


def _publish_isolated_runner_export(
    *,
    root: Path,
    strategy_id: str,
    trade_date: date,
    source: ShadowRunnerSignalSource,
    expected_commit: str,
    expected_runner_binding: LegacyShadowRunnerManifestBinding | None,
    dependencies: _LegacyShadowDependencies,
    fault_hook: ExportFaultHook | None = None,
) -> Path:
    if not strategy_id or any(part in strategy_id for part in ("/", "\\", "..")):
        raise ValueError("isolated runner strategy_id is invalid")
    export_root = _absolute_path(root, label="isolated runner export root") / strategy_id
    captured_at, _captured_monotonic_ns, _captured_boot_id, as_of = _legacy_export_times(
        trade_date=trade_date,
        dependencies=dependencies,
    )
    if not _in_publish_window(trade_date=trade_date, captured_at=captured_at):
        return recover_legacy_shadow_export(
            root=export_root,
            trade_date=trade_date,
            expected_source_id=f"strategy.{strategy_id}.v1",
            expected_commit=expected_commit,
            dependencies=dependencies,
        )
    receipt = ShadowSourceCompletionReceipt.model_validate(
        source.read_completion_receipt(trade_date=trade_date)
    )
    if (
        receipt.evidence_origin != "production"
        or receipt.source != "isolated"
        or receipt.trade_date != trade_date
        or receipt.producer_commit != expected_commit
        or receipt.completion_attestation is None
    ):
        raise LegacyShadowExportError("isolated runner completion receipt is not accepted")
    attestation = receipt.completion_attestation
    if attestation is None:  # pragma: no cover - guarded by the receipt check above
        raise LegacyShadowExportError("isolated runner completion attestation is absent")
    observed_runner_binding = LegacyShadowRunnerManifestBinding.create(
        strategy_id=attestation.claims.strategy_id,
        strategy_version=attestation.claims.strategy_version,
        producer_manifest_fingerprint=(attestation.claims.producer_manifest_fingerprint),
        producer_commit=attestation.claims.producer_commit,
        producer_service_id=attestation.claims.producer_service_id,
        producer_instance_id=attestation.claims.producer_instance_id,
        producer_version=attestation.claims.producer_version,
        strategy_registration_fingerprint=(attestation.claims.strategy_registration_fingerprint),
        strategy_spec_fingerprint=attestation.claims.strategy_spec_fingerprint,
        evaluator_contract_fingerprint=attestation.claims.executable_fingerprint,
        executable_fingerprint=attestation.claims.executable_fingerprint,
    )
    runner_binding = expected_runner_binding or observed_runner_binding
    if observed_runner_binding != runner_binding:
        raise LegacyShadowExportError(
            "isolated runner completion differs from its production manifest"
        )
    batch = _completed_runner_batch(source, trade_date=trade_date, receipt=receipt)
    input_identity = runner_source_raw_input_id(
        batch.snapshot.descriptor,
        batch.records,
        trade_date=trade_date,
    )
    if input_identity != receipt.input_identity:
        raise LegacyShadowExportError("isolated runner completion input identity mismatch")
    return _publish_isolated_secure(
        root=export_root,
        trade_date=trade_date,
        source_id=receipt.source_id,
        producer_commit=receipt.producer_commit,
        producer_version=receipt.producer_version,
        as_of=as_of,
        records_payload=canonical_model_json_bytes(batch),
        records=batch.records,
        records_count=len(batch.records),
        input_identity=input_identity,
        completion_receipt=receipt,
        runner_manifest_binding=runner_binding,
        dependencies=dependencies,
        fault_hook=fault_hook,
    )


@dataclass(frozen=True)
class _ProductionRunnerAuthority:
    binding: LegacyShadowRunnerManifestBinding
    runner_state_path: Path


def _production_runner_authorities(
    profile: object,
    *,
    expected_commit: str,
) -> Mapping[str, _ProductionRunnerAuthority]:
    from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest

    if getattr(profile, "producer_commit", None) != expected_commit:
        raise LegacyShadowExportError(
            "runtime deployment profile commit differs from isolated exporter"
        )
    manifests = tuple(
        manifest
        for manifest in getattr(profile, "manifests", ())
        if isinstance(manifest, RuntimeServiceManifest)
        and manifest.service_kind is RuntimeServiceKind.STRATEGY_LIVE
        and str(manifest.settings.get("strategy_id", "")) in _SHADOW_STRATEGY_IDS
    )
    if len(manifests) != len(_SHADOW_STRATEGY_IDS):
        raise LegacyShadowExportError(
            "runtime deployment profile must contain exactly two shadow runner manifests"
        )
    authorities: dict[str, _ProductionRunnerAuthority] = {}
    for manifest in manifests:
        settings = manifest.settings
        strategy_id = str(settings.get("strategy_id", ""))
        if strategy_id in authorities:
            raise LegacyShadowExportError("shadow runner manifest strategy is duplicated")
        try:
            runner_state_path = _absolute_path(
                Path(str(settings["runner_state_path"])),
                label="isolated runner state path",
            )
            binding = LegacyShadowRunnerManifestBinding.create(
                strategy_id=strategy_id,
                strategy_version=int(settings["strategy_version"]),
                producer_manifest_fingerprint=manifest.manifest_fingerprint,
                producer_commit=manifest.producer_commit,
                producer_service_id=manifest.service_id,
                producer_instance_id=str(settings["producer_instance_id"]),
                producer_version=str(settings["producer_version"]),
                strategy_registration_fingerprint=str(
                    settings["strategy_registration_fingerprint"]
                ),
                strategy_spec_fingerprint=str(settings["strategy_spec_fingerprint"]),
                evaluator_contract_fingerprint=str(settings["evaluator_contract_fingerprint"]),
                executable_fingerprint=str(settings["strategy_executable_fingerprint"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise LegacyShadowExportError(
                "runtime deployment profile runner manifest is incomplete"
            ) from exc
        authorities[strategy_id] = _ProductionRunnerAuthority(
            binding=binding,
            runner_state_path=runner_state_path,
        )
    if set(authorities) != _SHADOW_STRATEGY_IDS:
        raise LegacyShadowExportError(
            "runtime deployment profile shadow runner manifests are incomplete"
        )
    return authorities


def publish_isolated_runner_production_exports(
    *,
    data_dir: Path,
    trade_date: date,
    sources: Mapping[str, ShadowRunnerSignalSource],
    environment: Mapping[str, str] | None = None,
) -> Mapping[str, Path]:
    """Publish the two required isolated outputs into the shadow-only fan-in root."""

    if set(sources) != _SHADOW_STRATEGY_IDS:
        raise LegacyShadowExportError(
            "isolated runner fan-in must contain exactly the shadow strategy bindings"
        )
    expected_commit = _production_export_commit(environment)
    normalized_data_dir = _absolute_path(data_dir, label="legacy shadow data directory")
    from rquant.runtime_deployment_profile import load_current_runtime_deployment_profile

    profile = load_current_runtime_deployment_profile(normalized_data_dir / "runtime")
    authorities = _production_runner_authorities(
        profile,
        expected_commit=expected_commit,
    )
    return _publish_authorized_isolated_runner_exports(
        data_dir=normalized_data_dir,
        trade_date=trade_date,
        sources=sources,
        expected_commit=expected_commit,
        authorities=authorities,
        environment=environment,
    )


def _publish_authorized_isolated_runner_exports(
    *,
    data_dir: Path,
    trade_date: date,
    sources: Mapping[str, ShadowRunnerSignalSource],
    expected_commit: str,
    authorities: Mapping[str, _ProductionRunnerAuthority],
    environment: Mapping[str, str] | None,
) -> Mapping[str, Path]:
    if set(sources) != _SHADOW_STRATEGY_IDS or set(authorities) != _SHADOW_STRATEGY_IDS:
        raise LegacyShadowExportError(
            "isolated runner fan-in must contain exactly the shadow strategy bindings"
        )
    root = _production_export_root(data_dir, source="isolated-runners")
    dependencies = _production_export_dependencies(data_dir, environment=environment)
    return {
        strategy_id: _publish_isolated_runner_export(
            root=root,
            strategy_id=strategy_id,
            trade_date=trade_date,
            source=sources[strategy_id],
            expected_commit=expected_commit,
            expected_runner_binding=authorities[strategy_id].binding,
            dependencies=dependencies,
        )
        for strategy_id in sorted(_SHADOW_STRATEGY_IDS)
    }


def fan_in_production_isolated_runner_exports(
    *,
    data_dir: Path,
    trade_date: date,
    environment: Mapping[str, str] | None = None,
) -> Mapping[str, Path]:
    """Read profile-authorized runner spools and write no runtime authority state."""

    expected_commit = _production_export_commit(environment)
    normalized_data_dir = _absolute_path(data_dir, label="legacy shadow data directory")
    from rquant.runtime_deployment_profile import load_current_runtime_deployment_profile
    from rquant.signal_router_runtime import ReadonlyStrategyRunnerSignalSource

    profile = load_current_runtime_deployment_profile(normalized_data_dir / "runtime")
    authorities = _production_runner_authorities(
        profile,
        expected_commit=expected_commit,
    )
    sources: dict[str, ShadowRunnerSignalSource] = {}
    for strategy_id, authority in authorities.items():
        binding = authority.binding
        source = ReadonlyStrategyRunnerSignalSource(
            source_id=binding.producer_service_id,
            path=authority.runner_state_path,
            expected_strategy_spec_fingerprint=binding.strategy_spec_fingerprint,
            expected_evaluator_contract_fingerprint=(binding.evaluator_contract_fingerprint),
        )
        observed_strategy, observed_version, observed_spec = source.strategy_identity()
        if (
            observed_strategy != strategy_id
            or observed_version != binding.strategy_version
            or observed_spec != binding.strategy_spec_fingerprint
        ):
            raise LegacyShadowExportError(
                "isolated runner state identity differs from production manifest"
            )
        sources[strategy_id] = source
    return _publish_authorized_isolated_runner_exports(
        data_dir=normalized_data_dir,
        trade_date=trade_date,
        sources=sources,
        expected_commit=expected_commit,
        authorities=authorities,
        environment=environment,
    )


def recover_production_legacy_shadow_exports(
    *,
    data_dir: Path,
    trade_date: date,
    source: Literal["monitor", "surge", "isolated-runners"],
    environment: Mapping[str, str] | None = None,
) -> Mapping[str, Path]:
    """Controlled late-start entry. It can only verify/promote existing signed staging."""

    dependencies = _production_export_dependencies(data_dir, environment=environment)
    expected_commit = _production_export_commit(environment)
    if source == "monitor":
        return {
            source: recover_legacy_shadow_export(
                root=_production_export_root(data_dir, source=source),
                trade_date=trade_date,
                expected_source_id="legacy-monitor-events",
                expected_commit=expected_commit,
                dependencies=dependencies,
            )
        }
    if source == "surge":
        return {
            source: recover_legacy_shadow_export(
                root=_production_export_root(data_dir, source=source),
                trade_date=trade_date,
                expected_source_id="legacy-surge-jsonl",
                expected_commit=expected_commit,
                dependencies=dependencies,
            )
        }
    root = _production_export_root(data_dir, source="isolated-runners")
    return {
        strategy_id: recover_legacy_shadow_export(
            root=root / strategy_id,
            trade_date=trade_date,
            expected_source_id=f"strategy.{strategy_id}.v1",
            expected_commit=expected_commit,
            dependencies=dependencies,
        )
        for strategy_id in sorted(_SHADOW_STRATEGY_IDS)
    }


def _parse_manifest(payload: bytes) -> LegacyShadowExportManifest:
    try:
        return LegacyShadowExportManifest.model_validate(strict_canonical_json_loads(payload))
    except (StrictJsonError, ValueError) as exc:
        raise LegacyShadowExportUnavailableError("legacy shadow manifest is invalid") from exc


def _load_records_payload(
    payload: bytes,
    *,
    manifest: LegacyShadowExportManifest,
) -> tuple[dict[str, object], ...]:
    if hashlib.sha256(payload).hexdigest() != manifest.records_sha256:
        raise LegacyShadowExportUnavailableError("legacy shadow records digest mismatch")
    if manifest.records_filename == "events.json":
        try:
            decoded = strict_canonical_json_loads(payload)
        except StrictJsonError as exc:
            raise LegacyShadowExportUnavailableError("legacy shadow records are invalid") from exc
        if not isinstance(decoded, list):
            raise LegacyShadowExportUnavailableError("legacy shadow records contract is invalid")
        decoded_records = decoded
        model = LegacyMonitorEvent
    elif manifest.records_filename == "events.jsonl":
        try:
            decoded_records = [
                strict_canonical_json_loads(line) for line in payload.splitlines() if line
            ]
        except StrictJsonError as exc:
            raise LegacyShadowExportUnavailableError("legacy shadow records are invalid") from exc
        model = LegacySurgeEvent
    else:
        raise LegacyShadowExportUnavailableError("legacy shadow records contract is unsupported")
    if len(decoded_records) != manifest.records_count or any(
        not isinstance(item, dict) for item in decoded_records
    ):
        raise LegacyShadowExportUnavailableError("legacy shadow records count is invalid")
    records: list[dict[str, object]] = []
    for item in decoded_records:
        try:
            event = model.model_validate(item)
        except ValueError as exc:
            raise LegacyShadowExportUnavailableError(
                "legacy shadow source record is invalid"
            ) from exc
        if isinstance(event, LegacyMonitorEvent) and event.trade_date != manifest.trade_date:
            raise LegacyShadowExportUnavailableError(
                "legacy shadow monitor record trade_date mismatch"
            )
        records.append(dict(item))
    return tuple(records)


def _load_isolated_payload(
    payload: bytes,
    *,
    manifest: LegacyShadowExportManifest,
) -> tuple[RunnerSignalBatch, tuple[dict[str, object], ...]]:
    if hashlib.sha256(payload).hexdigest() != manifest.records_sha256:
        raise LegacyShadowExportUnavailableError("legacy shadow records digest mismatch")
    try:
        batch = RunnerSignalBatch.model_validate(strict_canonical_json_loads(payload))
    except (StrictJsonError, ValueError) as exc:
        raise LegacyShadowExportUnavailableError("isolated shadow batch is invalid") from exc
    records = tuple(record.model_dump(mode="json") for record in batch.records)
    if len(records) != manifest.records_count:
        raise LegacyShadowExportUnavailableError("isolated shadow records count is invalid")
    return batch, records


def _verify_record_envelopes_payload(
    payload: bytes,
    *,
    manifest: LegacyShadowExportManifest,
    records: tuple[dict[str, object], ...],
) -> None:
    if hashlib.sha256(payload).hexdigest() != manifest.record_envelopes_sha256:
        raise LegacyShadowExportUnavailableError("legacy shadow record envelopes digest mismatch")
    lines = payload.splitlines()
    if len(lines) != len(records):
        raise LegacyShadowExportUnavailableError("legacy shadow record envelopes count is invalid")
    for sequence, (line, record) in enumerate(zip(lines, records, strict=True), start=1):
        try:
            envelope = LegacyShadowExportRecord.model_validate(strict_canonical_json_loads(line))
        except (StrictJsonError, ValueError) as exc:
            raise LegacyShadowExportUnavailableError(
                "legacy shadow record envelope is invalid"
            ) from exc
        if (
            envelope.source_id != manifest.source_id
            or envelope.trade_date != manifest.trade_date
            or envelope.producer_commit != manifest.producer_commit
            or envelope.captured_at != manifest.captured_at
            or envelope.as_of != manifest.as_of
            or envelope.sequence != sequence
            or dict(envelope.payload) != record
        ):
            raise LegacyShadowExportUnavailableError("legacy shadow record envelope mismatch")


def _load_accepted_legacy_shadow_session(
    *,
    export_root: Path,
    session: Path,
    trade_date: date,
    expected_source_id: str | None,
    expected_commit: str,
    allowed_modes: frozenset[int],
    recovery_verifier: LegacyShadowRecoveryVerifier,
) -> AcceptedLegacyShadowExport:
    if (
        session.parent != export_root or session.name != trade_date.isoformat()
    ) and not session.name.startswith(f".staging-{trade_date.isoformat()}-"):
        raise LegacyShadowExportUnavailableError("legacy shadow session path is invalid")
    root_descriptor = _open_directory_fd(export_root, label="legacy shadow export root")
    session_descriptor = -1
    try:
        session_descriptor = _open_child_directory_at(
            root_descriptor,
            session.name,
            label="legacy shadow session",
            allowed_modes=allowed_modes,
        )
        marker = _load_recovery_marker_at(
            session_descriptor,
            verifier=recovery_verifier,
        )
        finalization = _load_finalization_receipt_at(
            session_descriptor,
            verifier=recovery_verifier,
        )
        _verify_recovery_marker_batch_at(session_descriptor, marker)
        _verify_finalization_receipt_batch_at(
            session_descriptor,
            marker=marker,
            receipt=finalization,
        )
        manifest = _parse_manifest(
            _read_regular_at(
                session_descriptor,
                "manifest.json",
                label="legacy shadow manifest",
            )
        )
        expected_children = {
            manifest.records_filename,
            "records.jsonl",
            "completion.json",
            "manifest.json",
            _RECOVERY_MARKER_FILENAME,
            _FINALIZATION_RECEIPT_FILENAME,
        }
        observed_children = set(os.listdir(session_descriptor))
        if (
            (expected_source_id is not None and manifest.source_id != expected_source_id)
            or manifest.trade_date != trade_date
            or observed_children != expected_children
        ):
            raise LegacyShadowExportUnavailableError(
                "legacy shadow manifest source identity mismatch"
            )
        if manifest.producer_commit != expected_commit:
            raise LegacyShadowExportUnavailableError("legacy shadow producer commit mismatch")
        records_payload = _read_regular_at(
            session_descriptor,
            manifest.records_filename,
            label="legacy shadow records",
        )
        batch: RunnerSignalBatch | None = None
        if manifest.records_filename == "completed-batch.json":
            batch, records = _load_isolated_payload(records_payload, manifest=manifest)
            receipt_source = "isolated"
        else:
            records = _load_records_payload(records_payload, manifest=manifest)
            receipt_source = "legacy"
        _verify_record_envelopes_payload(
            _read_regular_at(
                session_descriptor,
                "records.jsonl",
                label="legacy shadow record envelopes",
            ),
            manifest=manifest,
            records=records,
        )
        completion_payload = _read_regular_at(
            session_descriptor,
            "completion.json",
            label="legacy shadow completion receipt",
        )
    except OSError as exc:
        raise LegacyShadowExportUnavailableError(
            "legacy shadow export batch is unavailable"
        ) from exc
    finally:
        if session_descriptor >= 0:
            os.close(session_descriptor)
        os.close(root_descriptor)
    if hashlib.sha256(completion_payload).hexdigest() != manifest.completion_sha256:
        raise LegacyShadowExportUnavailableError("legacy shadow completion digest mismatch")
    try:
        receipt = ShadowSourceCompletionReceipt.model_validate(
            strict_canonical_json_loads(completion_payload)
        )
    except (StrictJsonError, ValueError) as exc:
        raise LegacyShadowExportUnavailableError(
            "legacy shadow completion receipt is invalid"
        ) from exc
    source_id = manifest.source_id
    if manifest.records_filename == "events.json":
        input_identity = legacy_records_raw_input_id(
            records,
            source_id=source_id,
            trade_date=trade_date,
        )
    elif manifest.records_filename == "events.jsonl":
        input_identity = _surge_input_identity(
            records_payload=_surge_records_payload(records),
            trade_date=trade_date,
        )
    else:
        assert batch is not None
        input_identity = runner_source_raw_input_id(
            batch.snapshot.descriptor,
            batch.records,
            trade_date=trade_date,
        )
    try:
        _validate_receipt(
            receipt,
            source=receipt_source,
            source_id=source_id,
            trade_date=trade_date,
            input_identity=input_identity,
            producer_commit=expected_commit,
            export_produced_at=marker.claims.produced_at,
            as_of=manifest.as_of,
        )
    except LegacyShadowExportError as exc:
        raise LegacyShadowExportUnavailableError(
            "legacy shadow completion binding is invalid"
        ) from exc
    _session_open, session_close = shadow_session_boundaries(trade_date)
    if (
        manifest.input_identity != input_identity
        or manifest.completion_receipt_id != str(receipt.receipt_id)
        or manifest.as_of != session_close
        or not _in_publish_window(
            trade_date=trade_date,
            captured_at=manifest.captured_at,
        )
        or marker.claims.trade_date != manifest.trade_date
        or marker.claims.source_id != manifest.source_id
        or marker.claims.producer_commit != manifest.producer_commit
        or marker.claims.producer_version != manifest.producer_version
        or marker.claims.input_identity != manifest.input_identity
        or marker.claims.captured_at != manifest.captured_at
        or not _in_publish_window(
            trade_date=trade_date,
            captured_at=marker.claims.produced_at,
        )
        or marker.claims.surge_collection_proof_id
        != (
            None
            if manifest.surge_collection_proof is None
            else manifest.surge_collection_proof.proof_id
        )
        or marker.claims.runner_manifest_binding_id
        != (
            None
            if manifest.runner_manifest_binding is None
            else manifest.runner_manifest_binding.binding_id
        )
        or marker.claims.staging_name != session.name
        and not session.name == trade_date.isoformat()
    ):
        raise LegacyShadowExportUnavailableError("legacy shadow completion binding mismatch")
    if manifest.runner_manifest_binding is not None:
        attestation = receipt.completion_attestation
        binding = manifest.runner_manifest_binding
        if (
            attestation is None
            or attestation.claims.producer_manifest_fingerprint
            != binding.producer_manifest_fingerprint
            or attestation.claims.producer_commit != binding.producer_commit
            or attestation.claims.producer_service_id != binding.producer_service_id
            or attestation.claims.producer_instance_id != binding.producer_instance_id
            or attestation.claims.producer_version != binding.producer_version
            or attestation.claims.strategy_id != binding.strategy_id
            or attestation.claims.strategy_version != binding.strategy_version
            or attestation.claims.strategy_registration_fingerprint
            != binding.strategy_registration_fingerprint
            or attestation.claims.strategy_spec_fingerprint != binding.strategy_spec_fingerprint
            or attestation.claims.executable_fingerprint != binding.executable_fingerprint
        ):
            raise LegacyShadowExportUnavailableError(
                "isolated runner completion differs from exported manifest binding"
            )
    return AcceptedLegacyShadowExport(
        root=export_root,
        session_path=session,
        manifest=manifest,
        records=records,
        records_path=session / manifest.records_filename,
        completion_receipt=receipt,
        completed_batch=batch,
    )


def load_accepted_legacy_shadow_export(
    *,
    root: Path,
    trade_date: date,
    expected_source_id: str | None,
    expected_commit: str,
    recovery_verifier: LegacyShadowRecoveryVerifier,
    filesystem_policy: LegacyShadowFilesystemPolicy,
) -> AcceptedLegacyShadowExport:
    """Load only an immutable, complete, receipt-bound monitor export."""

    export_root = _absolute_path(root, label="legacy shadow export root")
    validate_legacy_shadow_filesystem_contract(export_root, policy=filesystem_policy)
    return _load_accepted_legacy_shadow_session(
        export_root=export_root,
        session=export_root / trade_date.isoformat(),
        trade_date=trade_date,
        expected_source_id=expected_source_id,
        expected_commit=expected_commit,
        allowed_modes=_signed_session_modes(filesystem_policy),
        recovery_verifier=recovery_verifier,
    )


__all__ = [
    "AcceptedLegacyShadowExport",
    "LEGACY_SHADOW_FILESYSTEM_CONTRACT",
    "LegacyShadowExportConflictError",
    "LegacyShadowExportError",
    "LegacyShadowExportManifest",
    "LegacyShadowExportRecord",
    "LegacyShadowExportUnavailableError",
    "LegacyShadowFinalizationClaims",
    "LegacyShadowFinalizationReceipt",
    "LegacyShadowFilesystemPolicy",
    "LegacyShadowRecoveryMarker",
    "LegacyShadowTestDependencies",
    "LegacySurgeCollectionProof",
    "HmacLegacyShadowRecoveryAuthority",
    "LegacyMonitorCaptureSpool",
    "LegacyShadowFilesystemContract",
    "load_accepted_legacy_shadow_export",
    "fan_in_production_isolated_runner_exports",
    "legacy_shadow_test_filesystem_policy",
    "prepare_legacy_monitor_production_spool",
    "prepare_legacy_monitor_spool",
    "publish_legacy_monitor_export",
    "publish_legacy_monitor_production_export",
    "publish_legacy_surge_export",
    "publish_legacy_surge_production_export",
    "publish_isolated_runner_export",
    "publish_isolated_runner_production_exports",
    "recover_legacy_shadow_export",
    "recover_production_legacy_shadow_exports",
    "validate_legacy_shadow_filesystem_contract",
]
