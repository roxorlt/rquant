"""Bounded display projection sealed beside a complete factor research artifact."""

from __future__ import annotations

import hashlib
import os
import re
import secrets
import stat
from datetime import date
from math import fsum
from pathlib import Path
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from rquant.factor.decay import DecayPeriodStatus
from rquant.factor.definition import FactorDefinition
from rquant.factor.evaluate import CorrelationResult, FiniteFloat
from rquant.factor.portfolio import FactorPortfolioDay
from rquant.factor.result import (
    FactorDayCoverage,
    FactorResearchDay,
    HoldingSessions,
    ResearchDayStatus,
    ResearchPortfolioStatus,
    ResearchSummaryStatus,
    ReturnPriceBasis,
)
from rquant.factor.result_artifact import (
    FactorResearchArtifactV1,
    _cleanup_owned_temporary,
    _file_identity,
    _open_private_root,
    _require_named_regular,
    _require_same_root,
    _root_path,
    _write_all,
)
from rquant.factor.summary import FactorICSummary
from rquant.private_fs import rename_noreplace_at
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

MAX_FACTOR_DISPLAY_ARTIFACT_BYTES = 8 * 1024 * 1024
_MAX_DAYS = 1024
_PREFIX = "factor-display-v1-"
_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")
_SHA256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
_READ_FLAGS = os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC


def _digest(payload: object) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


class FactorDisplayICPoint(BaseModel):
    model_config = _IMMUTABLE

    decision_date: date
    normal_ic: CorrelationResult | None
    rank_ic: CorrelationResult | None
    normal_ic_cumulative_sum: FiniteFloat | None
    rank_ic_cumulative_sum: FiniteFloat | None


class FactorDisplayDecayPeriod(BaseModel):
    model_config = _IMMUTABLE

    lag: int = Field(ge=1, le=10)
    status: DecayPeriodStatus
    source_day_count: int = Field(ge=0)
    valid_pair_count: int = Field(ge=0)
    ic_summary: FactorICSummary | None


class FactorDisplayCoverageDay(BaseModel):
    model_config = _IMMUTABLE

    decision_date: date
    status: ResearchDayStatus
    coverage: FactorDayCoverage


class FactorDisplayArtifactV1(BaseModel):
    """A view of validated research, never a tradeable backtest NAV."""

    model_config = _IMMUTABLE

    schema_version: Literal[1]
    full_artifact_sha256: _SHA256
    result_sha256: _SHA256
    input_sha256: _SHA256
    definition: FactorDefinition
    definition_content_sha256: _SHA256
    factor_id: str
    factor_version: int
    code_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    source_sha256: _SHA256
    snapshot_id: str
    binding_hash: _SHA256
    snapshot_as_of_time: AwareDatetime
    source_mode: Literal["historical_retrospective"]
    source_read_boundary: Literal["single_snapshot_transaction"]
    visibility_basis: Literal["retrospective_adapter_assumption"]
    return_price_basis: ReturnPriceBasis
    holding_sessions: HoldingSessions
    as_of: AwareDatetime
    summary_status: ResearchSummaryStatus
    ic_summary: FactorICSummary | None
    ic_cumulative_kind: Literal["sum_of_valid_daily_ic"]
    ic_points: tuple[FactorDisplayICPoint, ...] = Field(min_length=1, max_length=_MAX_DAYS)
    decay_periods: tuple[FactorDisplayDecayPeriod, ...] = Field(min_length=10, max_length=10)
    portfolio_status: ResearchPortfolioStatus
    portfolio_days: tuple[FactorPortfolioDay, ...] = Field(max_length=_MAX_DAYS)
    coverage_days: tuple[FactorDisplayCoverageDay, ...] = Field(min_length=1, max_length=_MAX_DAYS)
    content_sha256: _SHA256

    @model_validator(mode="after")
    def _verified_content(self) -> FactorDisplayArtifactV1:
        if self.definition_content_sha256 != _digest(self.definition.model_dump(mode="json")):
            raise ValueError("display factor definition digest differs")
        if (
            self.factor_id != self.definition.factor_id
            or self.factor_version != self.definition.version
        ):
            raise ValueError("display factor identity differs from its definition")
        if self.content_sha256 != _digest(self.model_dump(mode="json", exclude={"content_sha256"})):
            raise ValueError("factor display artifact content digest differs")
        return self


class FactorDisplayArtifactReceipt(BaseModel):
    model_config = _IMMUTABLE

    sha256: _SHA256
    filename: str
    byte_count: int = Field(gt=0, le=MAX_FACTOR_DISPLAY_ARTIFACT_BYTES, strict=True)

    @model_validator(mode="after")
    def _matching_name(self) -> FactorDisplayArtifactReceipt:
        if self.filename != f"{_PREFIX}{self.sha256}.json":
            raise ValueError("factor display filename differs from its digest")
        return self


def _ic_point(
    day: FactorResearchDay, normal_values: list[float], rank_values: list[float]
) -> FactorDisplayICPoint:
    normal = day.evaluation.normal_ic if day.evaluation is not None else None
    rank = day.evaluation.rank_ic if day.evaluation is not None else None
    normal_sum = None
    rank_sum = None
    if normal is not None and normal.value is not None:
        normal_values.append(normal.value)
        normal_sum = fsum(normal_values)
    if rank is not None and rank.value is not None:
        rank_values.append(rank.value)
        rank_sum = fsum(rank_values)
    return FactorDisplayICPoint(
        decision_date=day.decision_date,
        normal_ic=normal,
        rank_ic=rank,
        normal_ic_cumulative_sum=normal_sum,
        rank_ic_cumulative_sum=rank_sum,
    )


def project_factor_display_artifact(full: FactorResearchArtifactV1) -> FactorDisplayArtifactV1:
    """Project one verified original; cumulative IC means a sum of valid correlations."""
    checked = FactorResearchArtifactV1.model_validate(full)
    result = checked.research.result
    receipt = checked.research.receipt
    if not 0 < len(result.days) <= _MAX_DAYS:
        raise ValueError("factor display exceeds the 1024-day budget")
    normal_values: list[float] = []
    rank_values: list[float] = []
    portfolio_days = (
        result.portfolio_diagnostics.days if result.portfolio_diagnostics is not None else ()
    )
    unsigned = {
        "schema_version": 1,
        "full_artifact_sha256": checked.content_sha256,
        "result_sha256": result.sha256,
        "input_sha256": result.input_sha256,
        "definition": result.definition,
        "definition_content_sha256": _digest(result.definition.model_dump(mode="json")),
        "factor_id": result.factor_id,
        "factor_version": result.factor_version,
        "code_revision": checked.code_revision,
        "source_sha256": receipt.source_sha256,
        "snapshot_id": receipt.snapshot_id,
        "binding_hash": receipt.binding_hash,
        "snapshot_as_of_time": receipt.snapshot_as_of_time,
        "source_mode": receipt.source_mode,
        "source_read_boundary": receipt.source_read_boundary,
        "visibility_basis": receipt.visibility_basis,
        "return_price_basis": result.return_price_basis,
        "holding_sessions": result.holding_sessions,
        "as_of": result.as_of,
        "summary_status": result.summary_status,
        "ic_summary": result.ic_summary,
        "ic_cumulative_kind": "sum_of_valid_daily_ic",
        "ic_points": tuple(_ic_point(day, normal_values, rank_values) for day in result.days),
        "decay_periods": tuple(
            FactorDisplayDecayPeriod(
                lag=period.lag,
                status=period.status,
                source_day_count=period.source_day_count,
                valid_pair_count=period.valid_pair_count,
                ic_summary=period.ic_summary,
            )
            for period in result.ic_decay.periods
        ),
        "portfolio_status": result.portfolio_status,
        "portfolio_days": portfolio_days,
        "coverage_days": tuple(
            FactorDisplayCoverageDay(
                decision_date=day.decision_date,
                status=day.status,
                coverage=day.coverage,
            )
            for day in result.days
        ),
    }
    unchecked = FactorDisplayArtifactV1.model_construct(content_sha256="0" * 64, **unsigned)
    return FactorDisplayArtifactV1(
        content_sha256=_digest(unchecked.model_dump(mode="json", exclude={"content_sha256"})),
        **unsigned,
    )


def _artifact_bytes(full: FactorResearchArtifactV1) -> tuple[FactorDisplayArtifactV1, bytes]:
    artifact = project_factor_display_artifact(full)
    data = canonical_json_bytes(artifact.model_dump(mode="json", round_trip=True))
    if not 0 < len(data) <= MAX_FACTOR_DISPLAY_ARTIFACT_BYTES:
        raise ValueError("factor display artifact exceeds 8 MiB")
    return artifact, data


def _parse_artifact(data: bytes, expected_sha256: str) -> FactorDisplayArtifactV1:
    if not 0 < len(data) <= MAX_FACTOR_DISPLAY_ARTIFACT_BYTES:
        raise ValueError("factor display artifact exceeds 8 MiB or is empty")
    strict_canonical_json_loads(data)
    artifact = FactorDisplayArtifactV1.model_validate_json(data, strict=False)
    if artifact.content_sha256 != expected_sha256 or data != canonical_json_bytes(
        artifact.model_dump(mode="json", round_trip=True)
    ):
        raise ValueError("factor display artifact has a wrong name or noncanonical content")
    return artifact


def _read_verified(
    root_fd: int, name: str, expected_sha256: str
) -> tuple[FactorDisplayArtifactV1, bytes, int, os.stat_result]:
    descriptor = os.open(name, _READ_FLAGS, dir_fd=root_fd)
    try:
        before = _require_named_regular(root_fd, name, descriptor)
        if not 0 < before.st_size <= MAX_FACTOR_DISPLAY_ARTIFACT_BYTES:
            raise ValueError("factor display artifact exceeds 8 MiB or is empty")
        data = bytearray()
        while len(data) <= MAX_FACTOR_DISPLAY_ARTIFACT_BYTES:
            chunk = os.read(
                descriptor, min(1024 * 1024, MAX_FACTOR_DISPLAY_ARTIFACT_BYTES + 1 - len(data))
            )
            if not chunk:
                break
            data.extend(chunk)
        if len(data) != before.st_size or len(data) > MAX_FACTOR_DISPLAY_ARTIFACT_BYTES:
            raise ValueError("factor display artifact changed during read")
        artifact = _parse_artifact(bytes(data), expected_sha256)
        after = _require_named_regular(root_fd, name, descriptor)
        if _file_identity(after) != _file_identity(before):
            raise ValueError("factor display artifact changed during read")
        return artifact, bytes(data), descriptor, after
    except BaseException:
        os.close(descriptor)
        raise


def publish_factor_display_artifact(
    full: FactorResearchArtifactV1, root: Path
) -> FactorDisplayArtifactReceipt:
    """Seal a compact file without replacing any prior content identity."""
    artifact, data = _artifact_bytes(full)
    root = _root_path(root)
    name = f"{_PREFIX}{artifact.content_sha256}.json"
    root_fd = _open_private_root(root)
    temporary_name: str | None = None
    temporary_identity: tuple[int, int] | None = None
    try:
        _require_same_root(root, root_fd)
        temporary_name = f".factor-display-{secrets.token_hex(16)}.tmp"
        temporary_fd = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=root_fd,
        )
        try:
            temporary = os.fstat(temporary_fd)
            temporary_identity = temporary.st_dev, temporary.st_ino
            os.fchmod(temporary_fd, 0o600)
            temporary = os.fstat(temporary_fd)
            if not stat.S_ISREG(temporary.st_mode) or temporary.st_nlink != 1:
                raise ValueError("factor display temporary is not a single-link file")
            _write_all(temporary_fd, data)
            os.fsync(temporary_fd)
        finally:
            os.close(temporary_fd)
        _, read_back, temporary_read_fd, _ = _read_verified(
            root_fd, temporary_name, artifact.content_sha256
        )
        try:
            if read_back != data:
                raise ValueError("factor display temporary differs from request")
        finally:
            os.close(temporary_read_fd)
        _require_same_root(root, root_fd)
        try:
            rename_noreplace_at(root_fd, temporary_name, root_fd, name)
        except FileExistsError:
            _cleanup_owned_temporary(root_fd, temporary_name, temporary_identity)
            temporary_name = None
        else:
            temporary_name = None
        stored, stored_bytes, final_fd, verified_stat = _read_verified(
            root_fd, name, artifact.content_sha256
        )
        try:
            if stored != artifact or stored_bytes != data:
                raise ValueError("existing factor display artifact differs from requested content")
            os.fsync(final_fd)
            current = _require_named_regular(root_fd, name, final_fd)
            if _file_identity(current) != _file_identity(verified_stat):
                raise ValueError("factor display artifact changed before directory sync")
            os.fsync(root_fd)
            if _file_identity(_require_named_regular(root_fd, name, final_fd)) != _file_identity(
                verified_stat
            ):
                raise ValueError("factor display artifact changed after directory sync")
            _require_same_root(root, root_fd)
        finally:
            os.close(final_fd)
        return FactorDisplayArtifactReceipt(
            sha256=artifact.content_sha256, filename=name, byte_count=len(data)
        )
    finally:
        if temporary_name is not None and temporary_identity is not None:
            _cleanup_owned_temporary(root_fd, temporary_name, temporary_identity)
        os.close(root_fd)


def _load_factor_display_artifact_with_identity(
    root: Path, sha256: str
) -> tuple[FactorDisplayArtifactV1, tuple[int, int], tuple[int, ...]]:
    """Read one verified compact view and retain its physical generation."""
    if re.fullmatch(r"[0-9a-f]{64}", sha256) is None:
        raise ValueError("factor display identity must be a lowercase SHA-256")
    root = _root_path(root)
    root_fd = _open_private_root(root)
    name = f"{_PREFIX}{sha256}.json"
    try:
        artifact, _, descriptor, verified_stat = _read_verified(root_fd, name, sha256)
        try:
            _require_same_root(root, root_fd)
            current = _require_named_regular(root_fd, name, descriptor)
            if _file_identity(current) != _file_identity(verified_stat):
                raise ValueError("factor display artifact changed after read")
            opened = os.fstat(root_fd)
            return artifact, (opened.st_dev, opened.st_ino), _file_identity(verified_stat)
        finally:
            os.close(descriptor)
    finally:
        os.close(root_fd)


def load_factor_display_artifact(root: Path, sha256: str) -> FactorDisplayArtifactV1:
    """Read a digest-derived direct child of the private physical root."""
    artifact, _, _ = _load_factor_display_artifact_with_identity(root, sha256)
    return artifact
