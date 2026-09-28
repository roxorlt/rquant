"""Read-only, bounded Serving view of sealed formula market tasks and results."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import stat
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc
from rquant.screen.formula_market_jobs import (
    FormulaMarketJobReceipt,
    FormulaMarketJobRequest,
    FormulaMarketJobResult,
    JobErrorCode,
    JobStatus,
)
from rquant.serving_read_models import ServingProjectionInput, ServingProjectionPayload
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

FORMULA_MARKET_PROJECTION_TABLES = frozenset(
    {"formula_market_job_state", "formula_market_job", "research_artifact_index"}
)
MAX_FORMULA_MARKET_JOBS = 100
_MAX_STORED_JOBS = 4096
_MAX_STATE_BYTES = 64 * 1024 * 1024
_MAX_REQUEST_BYTES = 32 * 1024
_MAX_RESULT_BYTES = 2 * 1024 * 1024
_MAX_TOTAL_RESULT_BYTES = 32 * 1024 * 1024
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_DIR_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)


class FormulaMarketJobStateRow(RuntimeContractModel):
    status_key: Literal["current"] = "current"
    availability: Literal["unavailable", "empty", "ready"]
    total_task_count: int = Field(ge=0, le=_MAX_STORED_JOBS, strict=True)
    retained_task_count: int = Field(ge=0, le=MAX_FORMULA_MARKET_JOBS, strict=True)
    has_older_tasks: bool

    @model_validator(mode="after")
    def validate_counts(self) -> FormulaMarketJobStateRow:
        if (
            (self.availability == "unavailable" and self.total_task_count != 0)
            or (self.availability == "empty" and self.total_task_count != 0)
            or (self.availability == "ready" and self.total_task_count == 0)
            or self.retained_task_count != min(self.total_task_count, MAX_FORMULA_MARKET_JOBS)
            or self.has_older_tasks != (self.total_task_count > MAX_FORMULA_MARKET_JOBS)
        ):
            raise ValueError("formula task availability and counts disagree")
        return self


class FormulaMarketJobRow(RuntimeContractModel):
    rank: int = Field(ge=0, lt=MAX_FORMULA_MARKET_JOBS, strict=True)
    task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    status: JobStatus
    attempts: int = Field(ge=0, strict=True)
    created_at: AwareUtcDatetime
    updated_at: AwareUtcDatetime
    error_code: JobErrorCode | None = None
    result_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    formula_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    formula: str = Field(min_length=1, max_length=4096)
    trade_date: date
    decision_at: AwareUtcDatetime
    universe_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    projection_identity: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_row(self) -> FormulaMarketJobRow:
        if self.created_at > self.updated_at:
            raise ValueError("formula task timestamp order is invalid")
        if (self.status == "succeeded") != (self.result_sha256 is not None):
            raise ValueError("formula task result reference disagrees with status")
        if (self.status == "failed") != (self.error_code is not None):
            raise ValueError("formula task error disagrees with status")
        if self.formula_sha256 != hashlib.sha256(self.formula.encode("utf-8")).hexdigest():
            raise ValueError("formula task formula digest is invalid")
        return self


class FormulaMarketArtifactIndexRow(RuntimeContractModel):
    artifact_type: Literal["formula_market_result"] = "formula_market_result"
    artifact_version: Literal[1] = 1
    rank: int = Field(ge=0, lt=MAX_FORMULA_MARKET_JOBS, strict=True)
    task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    relative_path: str
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    byte_count: int = Field(gt=0, le=_MAX_RESULT_BYTES, strict=True)
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    formula_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    universe_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    projection_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    trade_date: date
    decision_at: AwareUtcDatetime
    market_total: int = Field(ge=0, strict=True)
    listed_count: int = Field(ge=0, strict=True)
    paused_count: int = Field(ge=0, strict=True)
    match_count: int = Field(ge=0, strict=True)
    no_match_count: int = Field(ge=0, strict=True)
    unknown_count: int = Field(ge=0, strict=True)

    @model_validator(mode="after")
    def validate_index(self) -> FormulaMarketArtifactIndexRow:
        expected = f"formula-market-v1-{self.task_id}-{self.content_sha256}.json"
        if self.relative_path != expected:
            raise ValueError("formula result relative path is not controlled")
        if (
            self.listed_count + self.paused_count != self.market_total
            or self.match_count + self.no_match_count + self.unknown_count != self.market_total
        ):
            raise ValueError("formula result index counts do not conserve the market")
        return self


class FormulaMarketJobSnapshot(RuntimeContractModel):
    state: FormulaMarketJobStateRow
    jobs: tuple[FormulaMarketJobRow, ...] = Field(
        default_factory=tuple, max_length=MAX_FORMULA_MARKET_JOBS
    )
    artifacts: tuple[FormulaMarketArtifactIndexRow, ...] = Field(
        default_factory=tuple, max_length=MAX_FORMULA_MARKET_JOBS
    )
    available_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_snapshot(self) -> FormulaMarketJobSnapshot:
        _validate_rows(self.state, self.jobs, self.artifacts, self.available_at)
        return self


@dataclass(frozen=True)
class _Task:
    receipt: FormulaMarketJobReceipt
    request: FormulaMarketJobRequest
    request_sha256: str


def _validate_rows(
    state: FormulaMarketJobStateRow,
    jobs: tuple[FormulaMarketJobRow, ...],
    artifacts: tuple[FormulaMarketArtifactIndexRow, ...],
    available_at: datetime,
) -> None:
    if len(jobs) != state.retained_task_count:
        raise ValueError("formula task state and retained rows disagree")
    if [row.rank for row in jobs] != list(range(len(jobs))):
        raise ValueError("formula task rank order is invalid")
    if len({row.task_id for row in jobs}) != len(jobs):
        raise ValueError("formula task rows repeat a task")
    if any(row.created_at > available_at or row.updated_at > available_at for row in jobs):
        raise ValueError("formula task is newer than its Serving availability")
    if any(jobs[index].created_at < jobs[index + 1].created_at for index in range(len(jobs) - 1)):
        raise ValueError("formula task rows are not newest first")
    successes = {row.task_id: row for row in jobs if row.status == "succeeded"}
    if len(artifacts) != len(successes) or [row.rank for row in artifacts] != sorted(
        row.rank for row in artifacts
    ):
        raise ValueError("formula success index is incomplete or unordered")
    seen: set[str] = set()
    for artifact in artifacts:
        job = successes.get(artifact.task_id)
        if job is None or artifact.task_id in seen:
            raise ValueError("formula result references no unique successful task")
        seen.add(artifact.task_id)
        if (
            artifact.rank != job.rank
            or artifact.content_sha256 != job.result_sha256
            or artifact.request_sha256 != job.request_sha256
            or artifact.formula_sha256 != job.formula_sha256
            or artifact.universe_identity != job.universe_identity
            or artifact.projection_identity != job.projection_identity
            or artifact.trade_date != job.trade_date
            or artifact.decision_at != job.decision_at
        ):
            raise ValueError("formula result index and task row disagree")


def validate_formula_market_projections(
    projections: dict[str, ServingProjectionPayload | ServingProjectionInput],
) -> None:
    present = FORMULA_MARKET_PROJECTION_TABLES & projections.keys()
    if not present:
        return
    if present != FORMULA_MARKET_PROJECTION_TABLES:
        raise ValueError("formula task Serving projections are incomplete")
    selected = {name: projections[name] for name in FORMULA_MARKET_PROJECTION_TABLES}
    bound = tuple(item for item in selected.values() if isinstance(item, ServingProjectionInput))
    if bound and (
        len(bound) != len(selected) or len({item.owner_generation_id for item in bound}) != 1
    ):
        raise ValueError("formula task projections mix Serving generations")
    if len({item.available_at for item in selected.values()}) != 1:
        raise ValueError("formula task and result index times disagree")
    state_rows = selected["formula_market_job_state"].rows
    if len(state_rows) != 1:
        raise ValueError("formula task state row is missing")
    state = FormulaMarketJobStateRow.model_validate(dict(state_rows[0]))
    jobs = tuple(
        FormulaMarketJobRow.model_validate(dict(row)) for row in selected["formula_market_job"].rows
    )
    artifacts = tuple(
        FormulaMarketArtifactIndexRow.model_validate(dict(row))
        for row in selected["research_artifact_index"].rows
    )
    _validate_rows(state, jobs, artifacts, selected["formula_market_job_state"].available_at)


def project_formula_market_job(
    snapshot: FormulaMarketJobSnapshot,
) -> tuple[ServingProjectionPayload, ...]:
    return (
        ServingProjectionPayload(
            table_name="formula_market_job_state",
            available_at=snapshot.available_at,
            rows=(snapshot.state.model_dump(mode="json"),),
        ),
        ServingProjectionPayload(
            table_name="formula_market_job",
            available_at=snapshot.available_at,
            rows=tuple(row.model_dump(mode="json") for row in snapshot.jobs),
        ),
        ServingProjectionPayload(
            table_name="research_artifact_index",
            available_at=snapshot.available_at,
            rows=tuple(row.model_dump(mode="json") for row in snapshot.artifacts),
        ),
    )


def _identity(found: os.stat_result) -> tuple[int, int, int, int, int, int, int, int]:
    return (
        found.st_dev,
        found.st_ino,
        found.st_mode,
        found.st_uid,
        found.st_nlink,
        found.st_size,
        found.st_mtime_ns,
        found.st_ctime_ns,
    )


def _read_result_bytes(root: Path, name: str) -> tuple[bytes, datetime]:
    if not root.is_absolute() or root != root.resolve(strict=False):
        raise ValueError("formula result root must be absolute and canonical")
    before_dir = os.lstat(root)
    if (
        not stat.S_ISDIR(before_dir.st_mode)
        or before_dir.st_uid != os.geteuid()
        or stat.S_IMODE(before_dir.st_mode) != 0o700
    ):
        raise ValueError("formula result root is unsafe")
    directory = os.open(root, _DIR_FLAGS)
    try:
        if _identity(os.fstat(directory)) != _identity(before_dir):
            raise ValueError("formula result root changed while opening")
        before = os.stat(name, dir_fd=directory, follow_symlinks=False)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_nlink != 1
            or not 0 < before.st_size <= _MAX_RESULT_BYTES
        ):
            raise ValueError("formula result file is unsafe")
        handle = os.open(name, _READ_FLAGS, dir_fd=directory)
        try:
            if _identity(os.fstat(handle)) != _identity(before):
                raise ValueError("formula result changed while opening")
            remaining = before.st_size
            chunks: list[bytes] = []
            while remaining:
                chunk = os.read(handle, min(remaining, 1024 * 1024))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            after = os.fstat(handle)
            current = os.stat(name, dir_fd=directory, follow_symlinks=False)
            if (
                remaining
                or _identity(after) != _identity(before)
                or _identity(current) != _identity(before)
                or _identity(os.fstat(directory)) != _identity(before_dir)
            ):
                raise ValueError("formula result changed while reading")
            return b"".join(chunks), datetime.fromtimestamp(before.st_ctime_ns / 1e9, tz=UTC)
        finally:
            os.close(handle)
    finally:
        os.close(directory)


def _parse_result(
    root: Path,
    *,
    task_id: str,
    digest: str,
    request_sha256: str,
    formula_sha256: str,
    universe_identity: str,
    projection_identity: str,
    trade_date: date,
    decision_at: datetime,
    expected_bytes: int | None = None,
) -> tuple[FormulaMarketJobResult, int, datetime]:
    if (
        len(task_id) != 32
        or any(char not in "0123456789abcdef" for char in task_id)
        or len(digest) != 64
        or any(char not in "0123456789abcdef" for char in digest)
    ):
        raise ValueError("formula result task or content identity is invalid")
    name = f"formula-market-v1-{task_id}-{digest}.json"
    payload, published_at = _read_result_bytes(root, name)
    if expected_bytes is not None and len(payload) != expected_bytes:
        raise ValueError("formula result byte count changed")
    result = FormulaMarketJobResult.model_validate(strict_canonical_json_loads(payload))
    if (
        canonical_json_bytes(result.model_dump(mode="json")) != payload
        or result.task_id != task_id
        or result.content_sha256 != digest
        or result.request_sha256 != request_sha256
        or result.formula_sha256 != formula_sha256
        or result.summary.universe_identity != universe_identity
        or result.summary.projection_identity != projection_identity
        or result.summary.trade_date != trade_date
        or result.summary.decision_at != normalize_aware_utc(decision_at)
    ):
        raise ValueError("formula result and task source identity disagree")
    return result, len(payload), published_at


def read_formula_market_result(
    artifact_root: Path, index: FormulaMarketArtifactIndexRow
) -> FormulaMarketJobResult:
    """Verify a Serving reference against a configured private root, without opening task state."""
    index = FormulaMarketArtifactIndexRow.model_validate(index)
    result, _, _ = _parse_result(
        artifact_root,
        task_id=index.task_id,
        digest=index.content_sha256,
        request_sha256=index.request_sha256,
        formula_sha256=index.formula_sha256,
        universe_identity=index.universe_identity,
        projection_identity=index.projection_identity,
        trade_date=index.trade_date,
        decision_at=index.decision_at,
        expected_bytes=index.byte_count,
    )
    summary = result.summary
    if (
        summary.market_total != index.market_total
        or summary.listed_count != index.listed_count
        or summary.paused_count != index.paused_count
        or summary.match_count != index.match_count
        or summary.no_match_count != index.no_match_count
        or summary.unknown_count != index.unknown_count
    ):
        raise ValueError("formula result and Serving index counts disagree")
    return result


def _sidecars(path: Path) -> tuple[tuple[int, int] | None, tuple[int, int] | None, int]:
    identities: list[tuple[int, int] | None] = []
    wal_ctime_ns = 0
    for suffix in ("-wal", "-shm"):
        try:
            info = os.lstat(Path(f"{path}{suffix}"))
        except FileNotFoundError:
            identities.append(None)
            continue
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
            raise ValueError("formula task SQLite sidecar is unsafe")
        if suffix == "-wal":
            if info.st_size > _MAX_STATE_BYTES:
                raise ValueError("formula task WAL exceeds read bound")
            wal_ctime_ns = info.st_ctime_ns
        identities.append((info.st_dev, info.st_ino))
    if (identities[0] is None) != (identities[1] is None):
        raise ValueError("formula task SQLite sidecars are incomplete")
    return identities[0], identities[1], wal_ctime_ns


def _read_request(row: sqlite3.Row) -> FormulaMarketJobRequest:
    raw = row["request_json"]
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > _MAX_REQUEST_BYTES:
        raise ValueError("formula task request exceeds read bound")
    payload = raw.encode("utf-8")
    request = FormulaMarketJobRequest.model_validate(strict_canonical_json_loads(payload))
    if (
        canonical_json_bytes(request.model_dump(mode="json")) != payload
        or hashlib.sha256(payload).hexdigest() != row["request_sha256"]
        or request.idempotency_key != row["idempotency_key"]
    ):
        raise ValueError("formula task request digest is invalid")
    return request


def _read_task(row: sqlite3.Row) -> _Task:
    receipt = FormulaMarketJobReceipt(
        task_id=row["task_id"],
        status=row["status"],
        attempts=row["attempts"],
        created_at=datetime.fromisoformat(row["created_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
        result_sha256=row["result_sha256"],
        error_code=row["error_code"],
    )
    return _Task(receipt, _read_request(row), row["request_sha256"])


def _read_snapshot_rows(
    connection: sqlite3.Connection,
    *,
    artifact_root: Path,
    observed: datetime,
) -> tuple[
    int, tuple[FormulaMarketJobRow, ...], tuple[FormulaMarketArtifactIndexRow, ...], datetime
]:
    count = connection.execute("SELECT COUNT(*) FROM formula_market_job").fetchone()[0]
    if type(count) is not int or not 0 <= count <= _MAX_STORED_JOBS:
        raise ValueError("formula task store exceeds read bound")
    stored = connection.execute(
        "SELECT * FROM formula_market_job ORDER BY created_at DESC, rowid DESC LIMIT ?",
        (MAX_FORMULA_MARKET_JOBS,),
    ).fetchall()
    jobs: list[FormulaMarketJobRow] = []
    artifacts: list[FormulaMarketArtifactIndexRow] = []
    latest_available = _EPOCH
    result_bytes = 0
    for rank, row in enumerate(stored):
        task = _read_task(row)
        receipt, request = task.receipt, task.request
        if receipt.updated_at > observed or receipt.created_at > receipt.updated_at:
            raise ValueError("formula task timestamp is not available")
        job = FormulaMarketJobRow(
            rank=rank,
            task_id=receipt.task_id,
            status=receipt.status,
            attempts=receipt.attempts,
            created_at=receipt.created_at,
            updated_at=receipt.updated_at,
            error_code=receipt.error_code,
            result_sha256=receipt.result_sha256,
            request_sha256=task.request_sha256,
            formula_sha256=hashlib.sha256(request.formula.encode("utf-8")).hexdigest(),
            formula=request.formula,
            trade_date=request.trade_date,
            decision_at=request.decision_at,
            universe_identity=request.expected_universe_sha256,
            projection_identity=request.expected_projection_identity,
        )
        jobs.append(job)
        latest_available = max(latest_available, receipt.updated_at)
        if receipt.status != "succeeded":
            continue
        assert receipt.result_sha256 is not None
        result, size, published_at = _parse_result(
            artifact_root,
            task_id=receipt.task_id,
            digest=receipt.result_sha256,
            request_sha256=task.request_sha256,
            formula_sha256=job.formula_sha256,
            universe_identity=job.universe_identity,
            projection_identity=job.projection_identity,
            trade_date=job.trade_date,
            decision_at=job.decision_at,
        )
        result_bytes += size
        if result_bytes > _MAX_TOTAL_RESULT_BYTES:
            raise ValueError("formula result index exceeds read bound")
        latest_available = max(latest_available, published_at)
        summary = result.summary
        artifacts.append(
            FormulaMarketArtifactIndexRow(
                rank=rank,
                task_id=receipt.task_id,
                relative_path=f"formula-market-v1-{receipt.task_id}-{receipt.result_sha256}.json",
                content_sha256=receipt.result_sha256,
                byte_count=size,
                request_sha256=task.request_sha256,
                formula_sha256=job.formula_sha256,
                universe_identity=job.universe_identity,
                projection_identity=job.projection_identity,
                trade_date=job.trade_date,
                decision_at=job.decision_at,
                market_total=summary.market_total,
                listed_count=summary.listed_count,
                paused_count=summary.paused_count,
                match_count=summary.match_count,
                no_match_count=summary.no_match_count,
                unknown_count=summary.unknown_count,
            )
        )
    return count, tuple(jobs), tuple(artifacts), latest_available


def read_formula_market_job_snapshot(
    state_path: Path | None,
    artifact_root: Path | None,
    *,
    observed_at: datetime,
) -> FormulaMarketJobSnapshot:
    """Read one SQLite snapshot and authenticate every indexed immutable result."""
    observed = normalize_aware_utc(observed_at)
    unavailable = FormulaMarketJobSnapshot(
        state=FormulaMarketJobStateRow(
            availability="unavailable",
            total_task_count=0,
            retained_task_count=0,
            has_older_tasks=False,
        ),
        available_at=_EPOCH,
    )
    if state_path is None:
        return unavailable
    if artifact_root is None:
        raise ValueError("formula task state requires a result root")
    if not state_path.is_absolute() or state_path != state_path.resolve(strict=False):
        raise ValueError("formula task state path must be absolute and canonical")
    try:
        before = os.lstat(state_path)
    except FileNotFoundError:
        if _sidecars(state_path)[:2] != (None, None):
            raise ValueError("formula task state is missing while sidecars exist") from None
        return unavailable
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_nlink != 1
        or before.st_size > _MAX_STATE_BYTES
    ):
        raise ValueError("formula task state file is unsafe")
    sidecars = _sidecars(state_path)
    connection: sqlite3.Connection | None = None
    try:
        uri = f"{state_path.as_uri()}?mode=ro"
        if sidecars[:2] == (None, None):
            uri += "&immutable=1"
        connection = sqlite3.connect(uri, uri=True, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("BEGIN")
        count, jobs, artifacts, row_available = _read_snapshot_rows(
            connection, artifact_root=artifact_root, observed=observed
        )
        connection.rollback()
    finally:
        if connection is not None:
            connection.close()
    after = os.lstat(state_path)
    if (
        _identity(before)[:2] != _identity(after)[:2]
        or _sidecars(state_path)[:2] != sidecars[:2]
        or (sidecars[:2] == (None, None) and before.st_ctime_ns != after.st_ctime_ns)
    ):
        raise ValueError("formula task state rotated while reading")
    available_at = max(
        row_available,
        datetime.fromtimestamp(max(before.st_ctime_ns, sidecars[2]) / 1e9, tz=UTC),
    )
    if available_at > observed:
        raise ValueError("formula task source is newer than observation")
    return FormulaMarketJobSnapshot(
        state=FormulaMarketJobStateRow(
            availability="empty" if count == 0 else "ready",
            total_task_count=count,
            retained_task_count=len(jobs),
            has_older_tasks=count > len(jobs),
        ),
        jobs=jobs,
        artifacts=artifacts,
        available_at=available_at,
    )
