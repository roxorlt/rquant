"""Runtime builder for immutable built-in strategy candidate inputs."""

from __future__ import annotations

import os
import re
import stat
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime, time
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Literal, Protocol, TypeAlias
from zoneinfo import ZoneInfo

from pydantic import (
    Field,
    TypeAdapter,
    ValidationError,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.auction_gap_candidate_input import (
    AuctionGapCandidateInputError,
    assemble_auction_gap_candidate_batch,
)
from rquant.live_contracts import BatchQualityStatus
from rquant.live_spool import LiveBatchSpool
from rquant.reference_data_registry import ReadonlyReferenceRegistry
from rquant.runtime_contracts import RuntimeContractModel, normalize_aware_utc
from rquant.runtime_market_session import load_market_calendar_authority
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceStep,
)
from rquant.strategy_candidate_publish_service import (
    AuctionGapCandidateBatch,
    CandidatePublishBatch,
    GrowthBoardCandidateBatch,
    NShapeCandidateBatch,
    publish_candidate_batch,
)
from rquant.strategy_candidate_snapshot import (
    StrategyCandidateStaticFeatureSemantic,
    strategy_candidate_schema_fingerprint,
)
from rquant.strict_json import (
    StrictJsonError,
    canonical_json_bytes,
    strict_canonical_json_loads,
)

CandidateStrategyId: TypeAlias = Literal[
    "n_shape",
    "growth_board_surge",
    "auction_gap",
]

_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_MAX_CANDIDATE_INPUT_BYTES = 16 * 1024 * 1024
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_AUCTION_INPUT_START = time(9, 26)
_AUCTION_INPUT_END = time(9, 30)


class CandidatePublisherRuntimeSettings(RuntimeContractModel):
    strategy_id: CandidateStrategyId
    strategy_version: Literal[1] = 1
    definition_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    executable_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    candidate_schema_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    static_feature_schema: Mapping[str, StrategyCandidateStaticFeatureSemantic]
    input_mode: Literal["sealed_document", "auction_live"] = "sealed_document"
    candidate_input_path: Path | None = None
    auction_spool_root: Path | None = None
    daily_database_path: Path | None = None
    reference_registry_path: Path | None = None
    calendar_path: Path | None = None
    calendar_expected_commit: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{40}$",
    )
    calendar_content_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    snapshot_root: Path

    @field_validator("strategy_version", mode="before")
    @classmethod
    def require_strict_strategy_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("strategy_version must be the strict integer 1")
        return value

    @field_validator("static_feature_schema")
    @classmethod
    def freeze_static_feature_schema(
        cls,
        value: Mapping[str, StrategyCandidateStaticFeatureSemantic],
    ) -> Mapping[str, StrategyCandidateStaticFeatureSemantic]:
        if not value:
            raise ValueError("candidate static feature schema cannot be empty")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("static_feature_schema")
    def serialize_static_feature_schema(
        self,
        value: Mapping[str, StrategyCandidateStaticFeatureSemantic],
    ) -> dict[str, dict[str, str]]:
        return {name: semantic.model_dump(mode="json") for name, semantic in value.items()}

    @field_validator(
        "candidate_input_path",
        "auction_spool_root",
        "daily_database_path",
        "reference_registry_path",
        "calendar_path",
        "snapshot_root",
    )
    @classmethod
    def require_normalized_absolute_path(cls, value: Path | None) -> Path | None:
        if value is None:
            return None
        if not value.is_absolute() or value != Path(os.path.abspath(value)):
            raise ValueError("candidate runtime paths must be absolute and normalized")
        return value

    @model_validator(mode="after")
    def validate_input_mode(self) -> CandidatePublisherRuntimeSettings:
        if self.candidate_schema_fingerprint != strategy_candidate_schema_fingerprint(
            strategy_id=self.strategy_id,
            strategy_version=str(self.strategy_version),
            static_feature_schema=self.static_feature_schema,
        ):
            raise ValueError("candidate schema fingerprint does not match static schema")
        live_paths = (
            self.auction_spool_root,
            self.daily_database_path,
            self.reference_registry_path,
            self.calendar_path,
            self.calendar_expected_commit,
            self.calendar_content_sha256,
        )
        if self.input_mode == "sealed_document":
            if self.candidate_input_path is None:
                raise ValueError("candidate_input_path is required for sealed_document")
            if any(path is not None for path in live_paths):
                raise ValueError("auction live paths are forbidden for sealed_document")
            return self
        if self.strategy_id != "auction_gap":
            raise ValueError("auction_live input mode is only valid for auction_gap")
        if self.candidate_input_path is not None:
            raise ValueError("candidate_input_path is forbidden for auction_live")
        if any(path is None for path in live_paths):
            raise ValueError("all auction live input paths are required")
        return self


class CandidateInputLoader(Protocol):
    def __call__(
        self,
        path: Path,
        *,
        strategy_id: CandidateStrategyId,
        expected_commit: str,
    ) -> CandidatePublishBatch: ...


class AuctionCandidateInputLoader(Protocol):
    def __call__(
        self,
        *,
        auction_spool_root: Path,
        daily_database_path: Path,
        reference_registry_path: Path,
        calendar_path: Path,
        calendar_expected_commit: str,
        calendar_content_sha256: str,
        trade_date: date,
        observed_at: datetime,
        producer_commit: str,
    ) -> CandidatePublishBatch: ...


class _CandidateInputDocumentBase(RuntimeContractModel):
    schema_version: Literal[1] = 1

    @field_validator("schema_version", mode="before")
    @classmethod
    def require_strict_schema_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("schema_version must be the strict integer 1")
        return value


class NShapeCandidateInputDocument(_CandidateInputDocumentBase):
    batch_kind: Literal["n_shape"] = "n_shape"
    batch: NShapeCandidateBatch


class GrowthBoardCandidateInputDocument(_CandidateInputDocumentBase):
    batch_kind: Literal["growth_board_surge"] = "growth_board_surge"
    batch: GrowthBoardCandidateBatch


class AuctionGapCandidateInputDocument(_CandidateInputDocumentBase):
    batch_kind: Literal["auction_gap"] = "auction_gap"
    batch: AuctionGapCandidateBatch


CandidateInputDocument: TypeAlias = Annotated[
    NShapeCandidateInputDocument
    | GrowthBoardCandidateInputDocument
    | AuctionGapCandidateInputDocument,
    Field(discriminator="batch_kind"),
]

_DOCUMENT_ADAPTER = TypeAdapter(CandidateInputDocument)


def _document_for_batch(batch: CandidatePublishBatch) -> CandidateInputDocument:
    if isinstance(batch, NShapeCandidateBatch):
        return NShapeCandidateInputDocument(batch=NShapeCandidateBatch.model_validate(batch))
    if isinstance(batch, GrowthBoardCandidateBatch):
        return GrowthBoardCandidateInputDocument(
            batch=GrowthBoardCandidateBatch.model_validate(batch)
        )
    if isinstance(batch, AuctionGapCandidateBatch):
        return AuctionGapCandidateInputDocument(
            batch=AuctionGapCandidateBatch.model_validate(batch)
        )
    raise TypeError("batch must be a typed candidate publish batch")


def serialize_candidate_input(batch: CandidatePublishBatch) -> bytes:
    document = _document_for_batch(batch)
    return canonical_json_bytes(document.model_dump(mode="json"))


def _require_normalized_path(path: Path) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
        raise ValueError("candidate input path must be absolute and normalized")
    return candidate


def _same_identity(left: os.stat_result, right: os.stat_result) -> bool:
    return (
        left.st_dev,
        left.st_ino,
        left.st_mode,
        left.st_uid,
        left.st_nlink,
    ) == (
        right.st_dev,
        right.st_ino,
        right.st_mode,
        right.st_uid,
        right.st_nlink,
    )


def _same_version(left: os.stat_result, right: os.stat_result) -> bool:
    return _same_identity(left, right) and (
        left.st_size,
        left.st_mtime_ns,
        left.st_ctime_ns,
    ) == (
        right.st_size,
        right.st_mtime_ns,
        right.st_ctime_ns,
    )


def _open_parent_without_symlinks(path: Path) -> int:
    descriptor = os.open(path.anchor, _DIRECTORY_FLAGS)
    try:
        for component in path.parts[1:-1]:
            before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            if stat.S_ISLNK(before.st_mode):
                raise ValueError("candidate input path contains a symlink")
            child = os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptor)
            opened = os.fstat(child)
            current = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            if (
                not stat.S_ISDIR(opened.st_mode)
                or not _same_identity(before, opened)
                or not _same_identity(opened, current)
            ):
                os.close(child)
                raise ValueError("candidate input parent identity changed")
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _read_candidate_input(path: Path) -> bytes:
    parent_descriptor = -1
    descriptor = -1
    try:
        parent_descriptor = _open_parent_without_symlinks(path)
        before = os.stat(path.name, dir_fd=parent_descriptor, follow_symlinks=False)
        if stat.S_ISLNK(before.st_mode):
            raise ValueError("candidate input cannot be a symlink")
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("candidate input must be a regular file")
        if before.st_uid != os.getuid():
            raise ValueError("candidate input must be owned by the current uid")
        if before.st_nlink != 1:
            raise ValueError("candidate input hardlink count must be one")
        if stat.S_IMODE(before.st_mode) != 0o600:
            raise ValueError("candidate input permissions must be 0600")
        if before.st_size <= 0 or before.st_size > _MAX_CANDIDATE_INPUT_BYTES:
            raise ValueError("candidate input size is unsafe")

        descriptor = os.open(path.name, _FILE_FLAGS, dir_fd=parent_descriptor)
        opened = os.fstat(descriptor)
        current = os.stat(path.name, dir_fd=parent_descriptor, follow_symlinks=False)
        if not _same_identity(before, opened) or not _same_identity(opened, current):
            raise ValueError("candidate input identity changed")

        chunks: list[bytes] = []
        remaining = _MAX_CANDIDATE_INPUT_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        after = os.fstat(descriptor)
        active = os.stat(path.name, dir_fd=parent_descriptor, follow_symlinks=False)
        if not _same_version(opened, after) or not _same_version(after, active):
            raise ValueError("candidate input changed while being read")
        if len(payload) != opened.st_size or len(payload) > _MAX_CANDIDATE_INPUT_BYTES:
            raise ValueError("candidate input size changed while being read")
        return payload
    except OSError as exc:
        raise ValueError("candidate input is unavailable or contains a symlink") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if parent_descriptor >= 0:
            os.close(parent_descriptor)


def _batch_strategy_id(batch: CandidatePublishBatch) -> CandidateStrategyId:
    if isinstance(batch, NShapeCandidateBatch):
        return "n_shape"
    if isinstance(batch, GrowthBoardCandidateBatch):
        return "growth_board_surge"
    if isinstance(batch, AuctionGapCandidateBatch):
        return "auction_gap"
    raise TypeError("candidate input loader must return a typed candidate publish batch")


def _validate_loaded_batch(
    batch: CandidatePublishBatch,
    *,
    strategy_id: CandidateStrategyId,
    expected_commit: str,
) -> CandidatePublishBatch:
    observed_strategy = _batch_strategy_id(batch)
    if observed_strategy != strategy_id:
        raise ValueError("candidate input batch kind does not match strategy settings")
    if batch.authority.producer_commit != expected_commit:
        raise ValueError("candidate input producer commit does not match running code")
    return batch


def _restore_document_enums(decoded: object) -> object:
    if not isinstance(decoded, dict):
        return decoded
    batch = decoded.get("batch")
    if not isinstance(batch, dict):
        return decoded
    authority = batch.get("authority")
    if not isinstance(authority, dict):
        return decoded
    quality = authority.get("quality_status")
    if not isinstance(quality, str):
        return decoded
    restored_authority = {**authority, "quality_status": BatchQualityStatus(quality)}
    return {**decoded, "batch": {**batch, "authority": restored_authority}}


def load_candidate_input(
    path: Path,
    *,
    strategy_id: CandidateStrategyId,
    expected_commit: str,
) -> CandidatePublishBatch:
    candidate = _require_normalized_path(path)
    if _COMMIT_PATTERN.fullmatch(expected_commit) is None:
        raise ValueError("expected commit must be a full lowercase Git SHA")
    payload = _read_candidate_input(candidate)
    try:
        decoded = strict_canonical_json_loads(payload)
        document = _DOCUMENT_ADAPTER.validate_python(_restore_document_enums(decoded))
    except (StrictJsonError, ValidationError, TypeError, ValueError) as exc:
        raise ValueError("candidate input batch is invalid or not canonical typed JSON") from exc
    if document.batch_kind != strategy_id:
        raise ValueError("candidate input batch kind does not match strategy settings")
    return _validate_loaded_batch(
        document.batch,
        strategy_id=strategy_id,
        expected_commit=expected_commit,
    )


def load_live_auction_candidate_input(
    *,
    auction_spool_root: Path,
    daily_database_path: Path,
    reference_registry_path: Path,
    calendar_path: Path,
    calendar_expected_commit: str,
    calendar_content_sha256: str,
    trade_date: date,
    observed_at: datetime,
    producer_commit: str,
) -> CandidatePublishBatch:
    calendar = load_market_calendar_authority(
        calendar_path,
        expected_commit=calendar_expected_commit,
    )
    if calendar.content_sha256 != calendar_content_sha256:
        raise ValueError("auction candidate calendar content identity mismatch")
    return assemble_auction_gap_candidate_batch(
        auction_spool=LiveBatchSpool(auction_spool_root),
        daily_database_path=daily_database_path,
        reference_registry=ReadonlyReferenceRegistry(reference_registry_path),
        calendar=calendar,
        trade_date=trade_date,
        observed_at=observed_at,
        producer_commit=producer_commit,
    )


def _utc_now() -> datetime:
    return datetime.now(UTC)


def candidate_publisher_builder(
    *,
    candidate_input_loader: CandidateInputLoader | None = None,
    auction_input_loader: AuctionCandidateInputLoader | None = None,
    clock: Callable[[], datetime] = _utc_now,
) -> RuntimeServiceBuilder:
    loader: CandidateInputLoader = candidate_input_loader or load_candidate_input
    live_auction_loader = auction_input_loader or load_live_auction_candidate_input

    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.CANDIDATE_PUBLISHER:
            raise ValueError("runtime service kind must be candidate_publisher")
        if manifest.plane is not RuntimeServicePlane.LIVE:
            raise ValueError("candidate publisher must run on the live plane")
        settings = CandidatePublisherRuntimeSettings.model_validate(dict(manifest.settings))

        def step() -> RuntimeStepResult:
            if settings.input_mode == "auction_live":
                observed_at = normalize_aware_utc(clock())
                local = observed_at.astimezone(_SHANGHAI)
                local_time = local.timetz().replace(tzinfo=None)
                if not _AUCTION_INPUT_START <= local_time <= _AUCTION_INPUT_END:
                    return RuntimeStepResult()
                if (
                    settings.auction_spool_root is None
                    or settings.daily_database_path is None
                    or settings.reference_registry_path is None
                    or settings.calendar_path is None
                    or settings.calendar_expected_commit is None
                    or settings.calendar_content_sha256 is None
                ):
                    raise RuntimeError("validated auction live paths disappeared")
                try:
                    loaded = live_auction_loader(
                        auction_spool_root=settings.auction_spool_root,
                        daily_database_path=settings.daily_database_path,
                        reference_registry_path=settings.reference_registry_path,
                        calendar_path=settings.calendar_path,
                        calendar_expected_commit=settings.calendar_expected_commit,
                        calendar_content_sha256=settings.calendar_content_sha256,
                        trade_date=local.date(),
                        observed_at=observed_at,
                        producer_commit=manifest.producer_commit,
                    )
                except AuctionGapCandidateInputError:
                    return RuntimeStepResult(degraded_reasons=("auction_gap_input_unavailable",))
            else:
                if settings.candidate_input_path is None:
                    raise RuntimeError("validated candidate_input_path disappeared")
                loaded = loader(
                    settings.candidate_input_path,
                    strategy_id=settings.strategy_id,
                    expected_commit=manifest.producer_commit,
                )
            batch = _validate_loaded_batch(
                loaded,
                strategy_id=settings.strategy_id,
                expected_commit=manifest.producer_commit,
            )
            summary = publish_candidate_batch(
                snapshot_root=settings.snapshot_root,
                expected_commit=manifest.producer_commit,
                batch=batch,
                definition_fingerprint=settings.definition_fingerprint,
                executable_fingerprint=settings.executable_fingerprint,
                candidate_schema_fingerprint=settings.candidate_schema_fingerprint,
                static_feature_schema=settings.static_feature_schema,
            )
            return RuntimeStepResult(
                output_sequence=summary.snapshot_sequence,
                processed_count=summary.candidate_count,
                backlog_count=0,
                source_generations={
                    "candidate_input": summary.authority_snapshot_id,
                    "strategy_candidate": summary.snapshot_content_sha256,
                },
            )

        return step

    return build


__all__ = [
    "AuctionCandidateInputLoader",
    "CandidateInputLoader",
    "CandidateInputDocument",
    "CandidatePublisherRuntimeSettings",
    "CandidateStrategyId",
    "candidate_publisher_builder",
    "load_candidate_input",
    "load_live_auction_candidate_input",
    "serialize_candidate_input",
]
