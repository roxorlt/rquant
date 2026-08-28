"""Fenced downstream screen and Pool 2 stages for a canonical daily-close receipt."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import stat
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, suppress
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Literal, Protocol, Self, TypeVar

import duckdb
from pydantic import Field, StringConstraints, field_validator, model_validator

from rquant.daily_canonical_publisher import DailyCanonicalPublishReceipt
from rquant.daily_close_candidate import DailyCloseCandidateFence, DailyCloseCandidateFenceGuard
from rquant.daily_ledger_fence import DailyLedgerFenceGuard
from rquant.daily_pipeline_ledger import (
    DailyPipelineLedger,
    DailyPipelineLedgerError,
    DailyStageAttempt,
    DailyWriterLease,
    StageResult,
)
from rquant.pipeline import (
    DailyPoolPipelineResult,
    DailyScreenPipelineResult,
    run_daily_pool_stage,
    run_daily_screen_stage,
)
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.storage.duckdb import DuckDBStore

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
ArtifactT = TypeVar("ArtifactT", bound="_DownstreamArtifact")


class DailyDownstreamStageError(RuntimeError):
    """A downstream stage observed stale, conflicting, or unsafe evidence."""


class DailyDownstreamCanonicalVerifier(Protocol):
    def __call__(
        self,
        store: DuckDBStore,
        receipt: DailyCanonicalPublishReceipt,
        checked_at: datetime,
        /,
    ) -> None: ...


class _DownstreamArtifact(RuntimeContractModel):
    artifact_id: Sha256 | None = None
    canonical_receipt_id: Sha256
    canonical_generation_id: Sha256
    trade_date: date
    stage_result: StageResult
    created_at: AwareUtcDatetime

    @model_validator(mode="after")
    def bind_artifact_identity(self) -> Self:
        expected = canonical_sha256(
            self.model_dump(mode="python", exclude={"artifact_id", "created_at"})
        )
        if self.artifact_id is None:
            object.__setattr__(self, "artifact_id", expected)
        elif self.artifact_id != expected:
            raise ValueError("downstream artifact identity changed")
        return self


class DailyScreenStageArtifact(_DownstreamArtifact):
    contract: Literal["daily-screen-stage/v1"] = "daily-screen-stage/v1"
    preset_hits: Mapping[str, int] = Field(default_factory=dict)
    errors: tuple[str, ...] = ()

    @field_validator("preset_hits")
    @classmethod
    def validate_preset_hits(cls, values: Mapping[str, int]) -> Mapping[str, int]:
        normalized = {str(key): int(value) for key, value in sorted(values.items())}
        if any(not key or value < -1 for key, value in normalized.items()):
            raise ValueError("screen preset hits are invalid")
        return normalized

    @field_validator("errors")
    @classmethod
    def validate_errors(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if len(values) != len(set(values)) or any(not value for value in values):
            raise ValueError("screen errors must be unique non-empty values")
        return tuple(sorted(values))


class DailyPoolStageArtifact(_DownstreamArtifact):
    contract: Literal["daily-pool-stage/v1"] = "daily-pool-stage/v1"
    pool2_added: int = Field(ge=0)
    pool2_exited: int = Field(ge=0)
    pool2_active_count: int = Field(default=0, ge=0)
    errors: tuple[str, ...] = ()

    @field_validator("errors")
    @classmethod
    def validate_errors(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if len(values) != len(set(values)) or any(not value for value in values):
            raise ValueError("pool errors must be unique non-empty values")
        return tuple(sorted(values))


class DailyDownstreamArtifactStore:
    """Immutable stage artifacts used to recover after DuckDB/ledger crash boundaries."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._assert_directory(self.root)
        self._lock_path = self.root / ".daily-downstream-writer.lock"

    def persist_screen(self, artifact: DailyScreenStageArtifact) -> DailyScreenStageArtifact:
        return self._persist("screen", DailyScreenStageArtifact.model_validate(artifact))

    def persist_pool(self, artifact: DailyPoolStageArtifact) -> DailyPoolStageArtifact:
        return self._persist("pool", DailyPoolStageArtifact.model_validate(artifact))

    def load_screen(self, canonical_receipt_id: str) -> DailyScreenStageArtifact | None:
        return self._load("screen", canonical_receipt_id, DailyScreenStageArtifact)

    def load_pool(self, canonical_receipt_id: str) -> DailyPoolStageArtifact | None:
        return self._load("pool", canonical_receipt_id, DailyPoolStageArtifact)

    @contextmanager
    def writer_lock(self) -> Iterator[None]:
        descriptor = -1
        try:
            descriptor = os.open(
                self._lock_path,
                os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            opened = os.fstat(descriptor)
            linked = self._lock_path.lstat()
            if not (
                stat.S_ISREG(opened.st_mode)
                and stat.S_IMODE(opened.st_mode) == 0o600
                and opened.st_nlink == 1
                and (opened.st_dev, opened.st_ino) == (linked.st_dev, linked.st_ino)
            ):
                raise DailyDownstreamStageError("daily downstream writer lock is unsafe")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            if descriptor >= 0:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    os.close(descriptor)

    def _persist(self, stage: str, artifact: ArtifactT) -> ArtifactT:
        path = self._artifact_path(stage, artifact.canonical_receipt_id)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._assert_directory(path.parent)
        payload = _json_bytes(artifact)
        if path.exists():
            existing = self._read(path, type(artifact))
            if existing.model_dump(mode="python", exclude={"created_at"}) != artifact.model_dump(
                mode="python", exclude={"created_at"}
            ):
                raise DailyDownstreamStageError(
                    "downstream artifact replay conflicts with immutable content"
                )
            return existing
        temporary = path.with_name(f".{path.name}.{os.getpid()}.{artifact.artifact_id}.tmp")
        descriptor = -1
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            os.write(descriptor, payload)
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            try:
                os.link(temporary, path)
            except FileExistsError as exc:
                existing = self._read(path, type(artifact))
                if existing.model_dump(
                    mode="python", exclude={"created_at"}
                ) != artifact.model_dump(mode="python", exclude={"created_at"}):
                    raise DailyDownstreamStageError(
                        "downstream artifact replay conflicts with immutable content"
                    ) from exc
                return existing
            os.unlink(temporary)
            return artifact
        except OSError as exc:
            raise DailyDownstreamStageError("unable to persist downstream stage artifact") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            with suppress(FileNotFoundError):
                temporary.unlink()

    def _load(
        self,
        stage: str,
        canonical_receipt_id: str,
        model: type[ArtifactT],
    ) -> ArtifactT | None:
        if len(canonical_receipt_id) != 64:
            raise ValueError("canonical_receipt_id must be a sha256 digest")
        path = self._artifact_path(stage, canonical_receipt_id)
        return None if not path.exists() else self._read(path, model)

    def _artifact_path(self, stage: str, canonical_receipt_id: str) -> Path:
        if stage not in {"screen", "pool"}:
            raise ValueError("unsupported downstream stage")
        if len(canonical_receipt_id) != 64:
            raise ValueError("canonical_receipt_id must be a sha256 digest")
        return self.root / canonical_receipt_id / f"{stage}.json"

    @staticmethod
    def _assert_directory(path: Path) -> None:
        observed = path.lstat()
        if not stat.S_ISDIR(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
            raise DailyDownstreamStageError("downstream artifact directory is unsafe")

    @staticmethod
    def _read(path: Path, model: type[ArtifactT]) -> ArtifactT:
        observed = path.lstat()
        if (
            not stat.S_ISREG(observed.st_mode)
            or stat.S_ISLNK(observed.st_mode)
            or observed.st_nlink != 1
        ):
            raise DailyDownstreamStageError("downstream artifact file is unsafe")
        try:
            payload = path.read_bytes()
            return model.model_validate_json(payload)
        except (OSError, ValueError) as exc:
            raise DailyDownstreamStageError("downstream artifact is corrupt") from exc


class _DailyDownstreamStage:
    expected_stage_id: str

    def __init__(
        self,
        *,
        writer_factory: Callable[[], DuckDBStore],
        artifact_store: DailyDownstreamArtifactStore,
        ledger_fence_verifier: DailyCloseCandidateFenceGuard,
        clock: Callable[[], datetime],
        canonical_verifier: DailyDownstreamCanonicalVerifier | None = None,
    ) -> None:
        self._writer_factory = writer_factory
        self._artifacts = artifact_store
        self._ledger_fence_verifier = ledger_fence_verifier
        self._clock = clock
        self._canonical_verifier = canonical_verifier or assert_current_canonical_receipt

    @classmethod
    def from_ledger(
        cls,
        *,
        writer_factory: Callable[[], DuckDBStore],
        artifact_store: DailyDownstreamArtifactStore,
        ledger: DailyPipelineLedger,
        lease: DailyWriterLease,
        clock: Callable[[], datetime],
        canonical_verifier: DailyDownstreamCanonicalVerifier | None = None,
    ) -> Self:
        """Construct a downstream production stage with the formal ledger fence."""
        return cls(
            writer_factory=writer_factory,
            artifact_store=artifact_store,
            ledger_fence_verifier=DailyLedgerFenceGuard(ledger=ledger, lease=lease),
            clock=clock,
            canonical_verifier=canonical_verifier,
        )

    def _now(self) -> datetime:
        try:
            return normalize_aware_utc(self._clock())
        except Exception as exc:
            raise DailyDownstreamStageError("downstream stage clock is invalid") from exc

    @contextmanager
    def _fence(
        self,
        canonical: DailyCanonicalPublishReceipt,
        *,
        attempt: DailyStageAttempt,
        ledger_input_identity: str,
    ) -> Iterator[DailyCloseCandidateFence]:
        if attempt.stage_id != self.expected_stage_id:
            raise DailyDownstreamStageError("daily downstream ledger stage does not match")
        now = self._now()
        if not attempt.claimed_at <= now < attempt.lease_expires_at:
            raise DailyDownstreamStageError("daily downstream stage lease is invalid")
        try:
            context = self._ledger_fence_verifier(attempt, now)
            with context as fence:
                self._assert_fence(fence, canonical, ledger_input_identity)
                yield fence
        except DailyPipelineLedgerError as exc:
            raise DailyDownstreamStageError("daily downstream ledger fence rejected stage") from exc

    def _assert_boundary(
        self,
        store: DuckDBStore,
        canonical: DailyCanonicalPublishReceipt,
        fence: DailyCloseCandidateFence,
        ledger_input_identity: str,
    ) -> None:
        self._assert_fence(fence, canonical, ledger_input_identity)
        self._canonical_verifier(store, canonical, self._now())

    def _assert_fence(
        self,
        fence: DailyCloseCandidateFence,
        canonical: DailyCanonicalPublishReceipt,
        ledger_input_identity: str,
    ) -> None:
        now = self._now()
        fence.assert_current(now)
        fence.assert_source(canonical.source_generation_id, canonical.raw_content_sha256)
        fence.assert_input(ledger_input_identity)

    @contextmanager
    def _writer(self) -> Iterator[DuckDBStore]:
        try:
            with self._writer_factory() as store:
                yield store
        except duckdb.IOException as exc:
            if "lock" in str(exc).casefold():
                raise DailyDownstreamStageError(
                    "downstream DuckDB writer lock is unavailable"
                ) from exc
            raise


class DailyScreenStage(_DailyDownstreamStage):
    expected_stage_id = "screen"

    def run(
        self,
        canonical: DailyCanonicalPublishReceipt,
        *,
        attempt: DailyStageAttempt,
        ledger_input_identity: str,
        preset_names: list[str] | None = None,
    ) -> DailyScreenStageArtifact:
        canonical = DailyCanonicalPublishReceipt.model_validate(canonical)
        with (
            self._artifacts.writer_lock(),
            self._fence(
                canonical,
                attempt=attempt,
                ledger_input_identity=ledger_input_identity,
            ) as fence,
            self._writer() as store,
        ):
            existing = self._artifacts.load_screen(canonical.receipt_id)
            self._assert_boundary(store, canonical, fence, ledger_input_identity)
            if existing is not None:
                return existing
            committed = False
            try:
                store._conn.execute("BEGIN")
                self._assert_boundary(store, canonical, fence, ledger_input_identity)
                output = run_daily_screen_stage(
                    canonical.trade_date.isoformat(),
                    preset_names=preset_names,
                    store=store,
                )
                self._assert_boundary(store, canonical, fence, ledger_input_identity)
                store._conn.execute("COMMIT")
                committed = True
            finally:
                if not committed:
                    with suppress(duckdb.Error):
                        store._conn.execute("ROLLBACK")
            artifact = _screen_artifact(canonical, output, self._now())
            self._assert_boundary(store, canonical, fence, ledger_input_identity)
            return self._artifacts.persist_screen(artifact)


class DailyPoolStage(_DailyDownstreamStage):
    expected_stage_id = "pool"

    def run(
        self,
        canonical: DailyCanonicalPublishReceipt,
        *,
        screen_result: DailyScreenStageArtifact,
        attempt: DailyStageAttempt,
        ledger_input_identity: str,
    ) -> DailyPoolStageArtifact:
        canonical = DailyCanonicalPublishReceipt.model_validate(canonical)
        screen_result = DailyScreenStageArtifact.model_validate(screen_result)
        if not (
            screen_result.canonical_receipt_id == canonical.receipt_id
            and screen_result.canonical_generation_id == canonical.generation_id
            and screen_result.trade_date == canonical.trade_date
        ):
            raise DailyDownstreamStageError("pool stage screen artifact canonical binding changed")
        with (
            self._artifacts.writer_lock(),
            self._fence(
                canonical,
                attempt=attempt,
                ledger_input_identity=ledger_input_identity,
            ) as fence,
            self._writer() as store,
        ):
            persisted_screen = self._artifacts.load_screen(canonical.receipt_id)
            if persisted_screen != screen_result:
                raise DailyDownstreamStageError("pool stage screen artifact is missing or changed")
            existing = self._artifacts.load_pool(canonical.receipt_id)
            self._assert_boundary(store, canonical, fence, ledger_input_identity)
            if existing is not None:
                return existing
            committed = False
            try:
                store._conn.execute("BEGIN")
                self._assert_boundary(store, canonical, fence, ledger_input_identity)
                output = run_daily_pool_stage(canonical.trade_date.isoformat(), store=store)
                self._assert_boundary(store, canonical, fence, ledger_input_identity)
                store._conn.execute("COMMIT")
                committed = True
            finally:
                if not committed:
                    with suppress(duckdb.Error):
                        store._conn.execute("ROLLBACK")
            artifact = _pool_artifact(canonical, output, self._now())
            self._assert_boundary(store, canonical, fence, ledger_input_identity)
            return self._artifacts.persist_pool(artifact)


def _screen_artifact(
    canonical: DailyCanonicalPublishReceipt,
    output: DailyScreenPipelineResult,
    created_at: datetime,
) -> DailyScreenStageArtifact:
    payload = {"preset_hits": dict(output.preset_hits), "errors": output.errors}
    return DailyScreenStageArtifact(
        canonical_receipt_id=canonical.receipt_id,
        canonical_generation_id=canonical.generation_id,
        trade_date=canonical.trade_date,
        stage_result=StageResult(
            content_hash=canonical_sha256(payload),
            evidence_hash=canonical_sha256(
                {"canonical_receipt_id": canonical.receipt_id, "payload": payload}
            ),
        ),
        created_at=created_at,
        preset_hits=payload["preset_hits"],
        errors=output.errors,
    )


def _pool_artifact(
    canonical: DailyCanonicalPublishReceipt,
    output: DailyPoolPipelineResult,
    created_at: datetime,
) -> DailyPoolStageArtifact:
    payload = {
        "pool2_added": output.pool2_added,
        "pool2_exited": output.pool2_exited,
        "pool2_active_count": output.pool2_active_count,
        "errors": output.errors,
    }
    return DailyPoolStageArtifact(
        canonical_receipt_id=canonical.receipt_id,
        canonical_generation_id=canonical.generation_id,
        trade_date=canonical.trade_date,
        stage_result=StageResult(
            content_hash=canonical_sha256(payload),
            evidence_hash=canonical_sha256(
                {"canonical_receipt_id": canonical.receipt_id, "payload": payload}
            ),
        ),
        created_at=created_at,
        pool2_added=output.pool2_added,
        pool2_exited=output.pool2_exited,
        pool2_active_count=output.pool2_active_count,
        errors=output.errors,
    )


def assert_current_canonical_receipt(
    store: DuckDBStore,
    receipt: DailyCanonicalPublishReceipt,
    checked_at: datetime,
) -> None:
    if receipt.available_at > checked_at:
        raise DailyDownstreamStageError("canonical publication is not available at stage time")
    publication_receipt_id = (
        receipt.receipt_id
        if receipt.publication_mode == "committed"
        else receipt.recovery_of_receipt_id
    )
    if publication_receipt_id is None:
        raise DailyDownstreamStageError("canonical publication receipt binding is missing")
    try:
        publication = store._conn.execute(
            """
            SELECT generation_id, trade_date, db_content_sha256, canonical_receipt_id, is_current
            FROM daily_canonical_publication
            WHERE generation_id = ?
            """,
            [receipt.generation_id],
        ).fetchone()
        row = store._conn.execute(
            """
            SELECT receipt_id, generation_id, payload_sha256, payload_json
            FROM daily_canonical_publish_receipt
            WHERE receipt_id = ?
            """,
            [receipt.receipt_id],
        ).fetchone()
    except duckdb.Error as exc:
        raise DailyDownstreamStageError("canonical publication authority is unavailable") from exc
    if publication is None or row is None:
        raise DailyDownstreamStageError("canonical publication receipt is missing")
    try:
        stored = DailyCanonicalPublishReceipt.model_validate_json(str(row[3]))
    except ValueError as exc:
        raise DailyDownstreamStageError("canonical publication receipt is corrupt") from exc
    payload_bytes = str(row[3]).encode("utf-8")
    if not (
        stored == receipt
        and row[0] == receipt.receipt_id
        and row[1] == receipt.generation_id
        and row[2] == hashlib.sha256(payload_bytes).hexdigest()
        and publication[0] == receipt.generation_id
        and publication[1] == receipt.trade_date
        and publication[2] == receipt.db_content_sha256
        and publication[3] == publication_receipt_id
        and bool(publication[4])
    ):
        raise DailyDownstreamStageError("canonical publication authority binding changed")


def _json_bytes(model: RuntimeContractModel) -> bytes:
    return json.dumps(
        model.model_dump(mode="json"),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
