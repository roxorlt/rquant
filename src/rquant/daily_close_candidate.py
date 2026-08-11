"""Signed, immutable, content-addressed daily-close candidates."""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import json
import os
import stat
from collections.abc import Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Literal, Protocol, Self

from pydantic import Field, StringConstraints, model_validator

from rquant.daily_close_gateway import (
    DAILY_CLOSE_DATASETS,
    DailyCloseFacts,
    DailyCloseGateway,
    SuspensionStatusFact,
)
from rquant.daily_close_validation import VerifiedDailyCloseBatch
from rquant.daily_pipeline_ledger import DailyStageAttempt
from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchRecord, LiveBatchSpool
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)

Sha256Hex = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class DailyCloseCandidateError(RuntimeError):
    """Candidate evidence is stale, incomplete, unsigned, or corrupt."""


class DailyCandidateSigner(Protocol):
    key_id: str

    def sign(self, payload: bytes) -> str: ...

    def verify(self, payload: bytes, signature: str) -> bool: ...


class DailyCloseCandidateFence(Protocol):
    def assert_current(self, checked_at: datetime, /) -> None: ...

    def assert_source(
        self,
        source_generation_id: str,
        source_content_hash: str,
        /,
    ) -> None: ...

    def assert_input(self, input_identity: str, /) -> None: ...


class DailyCloseCandidateFenceGuard(Protocol):
    """Holds the Shard B stage fence through every candidate write boundary."""

    def __call__(
        self,
        attempt: DailyStageAttempt,
        checked_at: datetime,
        /,
    ) -> AbstractContextManager[DailyCloseCandidateFence]: ...


class DailyCandidateHmacSigner:
    def __init__(self, *, key_id: str, secret: bytes) -> None:
        if not key_id or any(character.isspace() for character in key_id):
            raise ValueError("daily candidate key_id is invalid")
        if len(secret) < 32:
            raise ValueError("daily candidate HMAC secret must be at least 32 bytes")
        self.key_id = key_id
        self._secret = bytes(secret)

    def sign(self, payload: bytes) -> str:
        return hmac.new(self._secret, payload, hashlib.sha256).hexdigest()

    def verify(self, payload: bytes, signature: str) -> bool:
        return hmac.compare_digest(self.sign(payload), signature)


class DailyCloseCandidateDiff(RuntimeContractModel):
    previous_raw_content_sha256: Sha256Hex | None = None
    current_raw_content_sha256: Sha256Hex
    changed_datasets: tuple[str, ...] = ()
    changed_row_count: int = Field(ge=0)


class DailyCloseCandidateManifest(RuntimeContractModel):
    contract: Literal["daily-close-candidate/v1"] = "daily-close-candidate/v1"
    generation_id: Sha256Hex | None = None
    trade_date: date
    revision: int = Field(ge=1)
    parent_generation_id: Sha256Hex | None = None
    source_generation_id: Sha256Hex
    source_sequence: int = Field(ge=0)
    source_batch_id: Sha256Hex
    source_request_id: Sha256Hex
    envelope_sha256: Sha256Hex
    source_payload_sha256: Sha256Hex
    raw_content_sha256: Sha256Hex
    calendar_generation_id: Sha256Hex
    calendar_producer_commit: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
    calendar_content_sha256: Sha256Hex
    calendar_as_of: AwareUtcDatetime
    minute_content_sha256: Sha256Hex | None = None
    validation_sha256: Sha256Hex
    facts_file_sha256: Sha256Hex
    available_at: AwareUtcDatetime
    diff: DailyCloseCandidateDiff
    signature_key_id: NonEmptyStr
    signature: Sha256Hex

    @model_validator(mode="after")
    def bind_generation(self) -> Self:
        expected = canonical_sha256(
            self.model_dump(mode="python", exclude={"generation_id", "signature"})
        )
        if self.generation_id is None:
            object.__setattr__(self, "generation_id", expected)
        elif self.generation_id != expected:
            raise ValueError("candidate generation id does not match its content")
        if (self.revision == 1) != (self.parent_generation_id is None):
            raise ValueError("candidate revision parent is inconsistent")
        if self.calendar_generation_id != self.calendar_content_sha256:
            raise ValueError("candidate calendar generation changed")
        return self

    def signing_payload(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))


class DailyCloseCandidatePointer(RuntimeContractModel):
    contract: Literal["daily-close-candidate-current/v1"] = "daily-close-candidate-current/v1"
    pointer_id: Sha256Hex | None = None
    trade_date: date
    generation_id: Sha256Hex
    revision: int = Field(ge=1)
    source_generation_id: Sha256Hex
    source_sequence: int = Field(ge=0)
    source_batch_id: Sha256Hex
    signature_key_id: NonEmptyStr
    signature: Sha256Hex

    @model_validator(mode="after")
    def bind_pointer(self) -> Self:
        expected = canonical_sha256(
            self.model_dump(mode="python", exclude={"pointer_id", "signature"})
        )
        if self.pointer_id is None:
            object.__setattr__(self, "pointer_id", expected)
        elif self.pointer_id != expected:
            raise ValueError("candidate current pointer identity changed")
        return self

    def signing_payload(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))


class DailyCloseCandidate(RuntimeContractModel):
    generation_id: Sha256Hex
    path: Path
    manifest: DailyCloseCandidateManifest
    facts: DailyCloseFacts

    @model_validator(mode="after")
    def validate_binding(self) -> Self:
        if self.generation_id != self.manifest.generation_id:
            raise ValueError("candidate handle generation changed")
        if self.facts.identity_sha256 != self.manifest.raw_content_sha256:
            raise ValueError("candidate handle facts changed")
        return self


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


class DailyCloseCandidateStore:
    def __init__(
        self,
        root: Path,
        *,
        signer: DailyCandidateSigner | None = None,
        trusted_verifiers: Mapping[str, DailyCandidateSigner] | None = None,
    ) -> None:
        self.root = Path(root)
        self.signer = signer
        verifiers = dict(trusted_verifiers or {})
        if signer is not None:
            verifiers.setdefault(signer.key_id, signer)
        self._verifiers = verifiers
        self.generations_root = self.root / "generations"
        self.current_root = self.root / "current"
        self._publish_lock_path = self.root / ".candidate-publish.lock"
        for path in (self.root, self.generations_root, self.current_root):
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
            path.chmod(0o700)
            self._require_private_directory(path)

    def publish(
        self,
        verified: VerifiedDailyCloseBatch,
        *,
        spool: LiveBatchSpool,
        attempt: DailyStageAttempt,
        published_at: datetime,
        fence_guard: DailyCloseCandidateFenceGuard,
    ) -> DailyCloseCandidate:
        if self.signer is None:
            raise DailyCloseCandidateError("candidate publisher has no signer")
        bound = VerifiedDailyCloseBatch.model_validate(verified)
        verified_attempt = DailyStageAttempt.model_validate(attempt)
        checked_at = normalize_aware_utc(published_at)
        if not verified_attempt.claimed_at <= checked_at < verified_attempt.lease_expires_at:
            raise DailyCloseCandidateError("candidate stage fence is stale")
        try:
            with self._publish_lock(), fence_guard(verified_attempt, checked_at) as fence:
                return self._publish_fenced(
                    bound,
                    spool=spool,
                    fence=fence,
                    checked_at=checked_at,
                )
        except DailyCloseCandidateError:
            raise
        except Exception as exc:
            raise DailyCloseCandidateError("candidate stage fence verification failed") from exc

    def _publish_fenced(
        self,
        bound: VerifiedDailyCloseBatch,
        *,
        spool: LiveBatchSpool,
        fence: DailyCloseCandidateFence,
        checked_at: datetime,
    ) -> DailyCloseCandidate:
        try:
            fence.assert_source(bound.source_generation_id, bound.raw_content_sha256)
        except DailyCloseCandidateError:
            raise
        except Exception as exc:
            raise DailyCloseCandidateError(
                "candidate stage source identity verification failed"
            ) from exc
        fence.assert_current(checked_at)
        source_record = self._assert_source_current(bound, spool=spool)
        previous = self._load_current_optional(bound.trade_date)
        if previous is None:
            if bound.revision != 1:
                raise DailyCloseCandidateError("candidate revision has no retained parent")
        elif source_record.envelope.batch_id == previous.manifest.source_batch_id:
            if bound.raw_content_sha256 != previous.manifest.raw_content_sha256:
                raise DailyCloseCandidateError("candidate replay content conflicts")
            return previous
        else:
            if not (
                bound.source_generation_id == previous.manifest.source_generation_id
                and bound.revision == previous.manifest.revision + 1
                and source_record.envelope.revises_batch_id == previous.manifest.source_batch_id
            ):
                raise DailyCloseCandidateError("candidate revision chain is not contiguous")

        facts_bytes = _canonical_json_bytes(bound.facts.model_dump(mode="json"))
        facts_hash = hashlib.sha256(facts_bytes).hexdigest()
        diff = self._diff(None if previous is None else previous.facts, bound.facts)
        unsigned = DailyCloseCandidateManifest(
            trade_date=bound.trade_date,
            revision=bound.revision,
            parent_generation_id=None if previous is None else previous.generation_id,
            source_generation_id=bound.source_generation_id,
            source_sequence=bound.source_sequence,
            source_batch_id=bound.source_batch_id,
            source_request_id=bound.source_request_id,
            envelope_sha256=bound.envelope_sha256,
            source_payload_sha256=bound.payload_sha256,
            raw_content_sha256=bound.raw_content_sha256,
            calendar_generation_id=bound.calendar_generation_id,
            calendar_producer_commit=bound.calendar_producer_commit,
            calendar_content_sha256=bound.calendar_content_sha256,
            calendar_as_of=bound.calendar_as_of,
            minute_content_sha256=bound.minute_content_sha256,
            validation_sha256=bound.validation_sha256,
            facts_file_sha256=facts_hash,
            available_at=bound.available_at,
            diff=diff,
            signature_key_id=self.signer.key_id,
            signature="0" * 64,
        )
        manifest = unsigned.model_copy(
            update={"signature": self.signer.sign(unsigned.signing_payload())}
        )
        generation_path = self.generations_root / str(manifest.generation_id)
        fence.assert_current(checked_at)
        self._persist_generation(
            generation_path,
            manifest=manifest,
            facts_bytes=facts_bytes,
        )
        candidate = self.load_generation(str(manifest.generation_id))
        fence.assert_current(checked_at)
        self._assert_source_current(bound, spool=spool)
        pointer = self._signed_pointer(candidate.manifest)
        self._write_current(pointer)
        return self.load_current(bound.trade_date)

    def load_generation(self, generation_id: str) -> DailyCloseCandidate:
        if len(generation_id) != 64 or any(
            character not in "0123456789abcdef" for character in generation_id
        ):
            raise DailyCloseCandidateError("candidate generation id is invalid")
        path = self.generations_root / generation_id
        try:
            self._require_private_directory(path)
            manifest_bytes = self._secure_read(path / "manifest.json", label="manifest")
            facts_bytes = self._secure_read(path / "facts.json", label="facts")
            manifest = DailyCloseCandidateManifest.model_validate_json(manifest_bytes)
            facts = DailyCloseFacts.model_validate_json(facts_bytes)
        except DailyCloseCandidateError:
            raise
        except Exception as exc:
            raise DailyCloseCandidateError("candidate manifest or facts is invalid") from exc
        if manifest.generation_id != generation_id:
            raise DailyCloseCandidateError("candidate manifest path identity changed")
        if hashlib.sha256(facts_bytes).hexdigest() != manifest.facts_file_sha256:
            raise DailyCloseCandidateError("candidate facts file hash changed")
        if facts.identity_sha256 != manifest.raw_content_sha256:
            raise DailyCloseCandidateError("candidate facts content identity changed")
        self._verify_signature(manifest)
        return DailyCloseCandidate(
            generation_id=generation_id,
            path=path,
            manifest=manifest,
            facts=facts,
        )

    def load_current(self, trade_date: date) -> DailyCloseCandidate:
        candidate = self._load_current_optional(trade_date)
        if candidate is None:
            raise DailyCloseCandidateError("canonical candidate current is missing")
        return candidate

    def _load_current_optional(self, trade_date: date) -> DailyCloseCandidate | None:
        path = self.current_root / f"{trade_date.isoformat()}.json"
        if not path.exists():
            return None
        try:
            pointer = DailyCloseCandidatePointer.model_validate_json(
                self._secure_read(path, label="current pointer")
            )
        except DailyCloseCandidateError:
            raise
        except Exception as exc:
            raise DailyCloseCandidateError("candidate current pointer is invalid") from exc
        if pointer.trade_date != trade_date:
            raise DailyCloseCandidateError("candidate current trade_date changed")
        self._verify_signature(pointer)
        candidate = self.load_generation(pointer.generation_id)
        manifest = candidate.manifest
        if not (
            pointer.revision == manifest.revision
            and pointer.source_generation_id == manifest.source_generation_id
            and pointer.source_sequence == manifest.source_sequence
            and pointer.source_batch_id == manifest.source_batch_id
        ):
            raise DailyCloseCandidateError("candidate current binding changed")
        return candidate

    def _persist_generation(
        self,
        path: Path,
        *,
        manifest: DailyCloseCandidateManifest,
        facts_bytes: bytes,
    ) -> None:
        path.mkdir(mode=0o700, parents=False, exist_ok=True)
        path.chmod(0o700)
        self._require_private_directory(path)
        facts_path = path / "facts.json"
        manifest_path = path / "manifest.json"
        manifest_bytes = _canonical_json_bytes(manifest.model_dump(mode="json"))
        self._write_immutable_or_verify(facts_path, facts_bytes, label="facts")
        self._write_immutable_or_verify(manifest_path, manifest_bytes, label="manifest")

    def _write_immutable_or_verify(self, path: Path, payload: bytes, *, label: str) -> None:
        if path.exists():
            if self._secure_read(path, label=label) != payload:
                raise DailyCloseCandidateError(f"candidate {label} conflicts with immutable data")
            return
        LiveBatchSpool._atomic_write(path, payload)
        if self._secure_read(path, label=label) != payload:
            raise DailyCloseCandidateError(f"candidate {label} write is not durable")

    def _write_current(self, pointer: DailyCloseCandidatePointer) -> None:
        path = self.current_root / f"{pointer.trade_date.isoformat()}.json"
        LiveBatchSpool._atomic_write(
            path,
            _canonical_json_bytes(pointer.model_dump(mode="json")),
        )

    def _signed_pointer(
        self,
        manifest: DailyCloseCandidateManifest,
    ) -> DailyCloseCandidatePointer:
        assert self.signer is not None
        unsigned = DailyCloseCandidatePointer(
            trade_date=manifest.trade_date,
            generation_id=str(manifest.generation_id),
            revision=manifest.revision,
            source_generation_id=manifest.source_generation_id,
            source_sequence=manifest.source_sequence,
            source_batch_id=manifest.source_batch_id,
            signature_key_id=self.signer.key_id,
            signature="0" * 64,
        )
        return unsigned.model_copy(
            update={"signature": self.signer.sign(unsigned.signing_payload())}
        )

    def _verify_signature(
        self,
        document: DailyCloseCandidateManifest | DailyCloseCandidatePointer,
    ) -> None:
        verifier = self._verifiers.get(document.signature_key_id)
        if verifier is None or not verifier.verify(document.signing_payload(), document.signature):
            raise DailyCloseCandidateError("candidate signature is not trusted")

    @staticmethod
    def _assert_source_current(
        verified: VerifiedDailyCloseBatch,
        *,
        spool: LiveBatchSpool,
    ) -> LiveBatchRecord:
        descriptor = spool.source_descriptor(LiveChannel.DAILY_CLOSE)
        current = spool.current(LiveChannel.DAILY_CLOSE)
        if current is None or not (
            descriptor.generation_id == verified.source_generation_id
            and current.source_generation_id == verified.source_generation_id
            and current.sequence == verified.source_sequence
            and current.batch_id == verified.source_batch_id
            and current.content_sha256 == verified.payload_sha256
            and current.revision == verified.revision
        ):
            raise DailyCloseCandidateError("verified source current has advanced")
        records = spool.list_after(
            LiveChannel.DAILY_CLOSE,
            sequence=verified.source_sequence - 1,
        )
        record = next(
            (item for item in records if item.envelope.sequence == verified.source_sequence),
            None,
        )
        if record is None or not (
            record.envelope.identity_sha256 == verified.envelope_sha256
            and record.envelope.quality_status is BatchQualityStatus.PUBLISHED
        ):
            raise DailyCloseCandidateError("verified source current evidence changed")
        payload = DailyCloseGateway.decode_payload(spool.read_payload(record))
        if not (
            payload.content_sha256 == verified.raw_content_sha256
            and payload.facts == verified.facts
        ):
            raise DailyCloseCandidateError("verified source current payload changed")
        return record

    @staticmethod
    def _diff(
        previous: DailyCloseFacts | None,
        current: DailyCloseFacts,
    ) -> DailyCloseCandidateDiff:
        changed: list[str] = []
        changed_rows = 0
        for dataset in DAILY_CLOSE_DATASETS:
            current_rows = DailyCloseCandidateStore._row_identities(current.rows(dataset))
            previous_rows = (
                {}
                if previous is None
                else DailyCloseCandidateStore._row_identities(previous.rows(dataset))
            )
            differing_keys = {
                *current_rows.keys(),
                *previous_rows.keys(),
            }
            count = sum(current_rows.get(key) != previous_rows.get(key) for key in differing_keys)
            if count:
                changed.append(dataset.value)
                changed_rows += count
        return DailyCloseCandidateDiff(
            previous_raw_content_sha256=(None if previous is None else previous.identity_sha256),
            current_raw_content_sha256=current.identity_sha256,
            changed_datasets=tuple(changed),
            changed_row_count=changed_rows,
        )

    @staticmethod
    def _row_identities(rows: Sequence[RuntimeContractModel]) -> dict[tuple[object, ...], str]:
        identities: dict[tuple[object, ...], str] = {}
        for row in rows:
            key: tuple[object, ...] = (row.ts_code, row.trade_date)
            if isinstance(row, SuspensionStatusFact):
                key += (row.suspend_type, row.suspend_timing)
            identities[key] = canonical_sha256(row)
        return identities

    @contextmanager
    def _publish_lock(self) -> Iterator[None]:
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        if nofollow == 0:
            raise DailyCloseCandidateError("candidate publish lock requires O_NOFOLLOW")
        descriptor = -1
        locked = False
        try:
            descriptor = os.open(
                self._publish_lock_path,
                os.O_RDWR | os.O_CREAT | nofollow | getattr(os, "O_CLOEXEC", 0),
                0o600,
            )
            opened = os.fstat(descriptor)
            linked = self._publish_lock_path.lstat()
            if not (
                stat.S_ISREG(opened.st_mode)
                and opened.st_uid == os.geteuid()
                and opened.st_nlink == 1
                and stat.S_IMODE(opened.st_mode) == 0o600
                and (opened.st_dev, opened.st_ino) == (linked.st_dev, linked.st_ino)
            ):
                raise DailyCloseCandidateError("candidate publish lock is unsafe")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            locked = True
            yield
        except DailyCloseCandidateError:
            raise
        except OSError as exc:
            raise DailyCloseCandidateError("candidate publish lock is unavailable") from exc
        finally:
            if descriptor >= 0:
                if locked:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)

    @staticmethod
    def _require_private_directory(path: Path) -> None:
        try:
            observed = path.lstat()
        except OSError as exc:
            raise DailyCloseCandidateError("candidate directory is unavailable") from exc
        if not (
            stat.S_ISDIR(observed.st_mode)
            and observed.st_uid == os.geteuid()
            and stat.S_IMODE(observed.st_mode) == 0o700
        ):
            raise DailyCloseCandidateError("candidate directory is unsafe")

    @staticmethod
    def _secure_read(path: Path, *, label: str, max_bytes: int = 128 * 1024 * 1024) -> bytes:
        descriptor = -1
        try:
            descriptor = os.open(
                path,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
            )
            before = os.fstat(descriptor)
            if not (
                stat.S_ISREG(before.st_mode)
                and before.st_uid == os.geteuid()
                and before.st_nlink == 1
                and stat.S_IMODE(before.st_mode) == 0o600
                and before.st_size <= max_bytes
            ):
                raise DailyCloseCandidateError(f"candidate {label} is unsafe")
            chunks: list[bytes] = []
            remaining = max_bytes + 1
            while remaining > 0:
                chunk = os.read(descriptor, min(1024 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            if remaining <= 0 and os.read(descriptor, 1):
                raise DailyCloseCandidateError(f"candidate {label} is too large")
            after = os.fstat(descriptor)
            linked = path.lstat()
            if (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
            ) != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
            ) or (before.st_dev, before.st_ino) != (linked.st_dev, linked.st_ino):
                raise DailyCloseCandidateError(f"candidate {label} changed while reading")
            return b"".join(chunks)
        except DailyCloseCandidateError:
            raise
        except OSError as exc:
            raise DailyCloseCandidateError(f"candidate {label} is unavailable") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)


__all__ = [
    "DailyCandidateHmacSigner",
    "DailyCandidateSigner",
    "DailyCloseCandidate",
    "DailyCloseCandidateDiff",
    "DailyCloseCandidateError",
    "DailyCloseCandidateFence",
    "DailyCloseCandidateFenceGuard",
    "DailyCloseCandidateManifest",
    "DailyCloseCandidatePointer",
    "DailyCloseCandidateStore",
]
