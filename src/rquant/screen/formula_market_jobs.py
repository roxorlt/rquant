"""Durable, single-active formula runs over two pinned read-only sources."""

from __future__ import annotations

import hashlib
import os
import re
import secrets
import sqlite3
import stat
import threading
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import Field, field_validator, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.screen.formula_history_projection import (
    FormulaProjectionBudgetError,
    FormulaProjectionUnavailableError,
)
from rquant.screen.formula_market_run import (
    FormulaMarketRunBudgetError,
    FormulaMarketRunSummary,
    FormulaMarketRunTimeoutError,
    run_formula_market,
)
from rquant.screen.formula_market_universe import FormulaMarketUniverseError
from rquant.screen.tdx.evaluate import (
    EvaluationRejectedError,
    FormulaEvaluationInput,
    compile_formula,
    evaluate_compiled_formula,
)
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

_TASK_ID = re.compile(r"^[0-9a-f]{32}$")
_TOKEN = re.compile(r"^[0-9a-f]{32}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_KEY = re.compile(r"^[A-Za-z0-9_-]{16,64}$")
_MAX_JOBS = 4096
_MAX_ARTIFACT_BYTES = 2 * 1024 * 1024
_MAX_ATTEMPTS = 3
_DIR_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
_WRITE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)

JobStatus = Literal["queued", "running", "succeeded", "failed"]
JobErrorCode = Literal[
    "source_changed",
    "formula_rejected",
    "timeout",
    "capacity",
    "artifact_invalid",
    "lease_exhausted",
    "internal_error",
]


class FormulaMarketArtifactUnavailableError(RuntimeError):
    """A stored success has no authentic, complete and bound result."""


class FormulaMarketJobActiveError(ValueError):
    """A different formula market task still owns the single active slot."""


class _FormulaMarketSourceRootMismatchError(ValueError):
    """The claimed request names sources outside this worker's trusted configuration."""


class FormulaMarketJobRequest(RuntimeContractModel):
    """Trusted caller input; roots and source identities never come from the browser."""

    idempotency_key: str = Field(pattern=r"^[A-Za-z0-9_-]{16,64}$")
    formula: str = Field(min_length=1, max_length=4096)
    trade_date: date
    decision_at: AwareUtcDatetime
    universe_root: Path
    projection_root: Path
    expected_universe_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_projection_identity: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("universe_root", "projection_root")
    @classmethod
    def require_absolute_root(cls, value: Path) -> Path:
        path = Path(value)
        if (
            not path.is_absolute()
            or path != Path(os.path.normpath(path))
            or path != path.resolve(strict=False)
        ):
            raise ValueError("formula source root must be an absolute canonical path")
        return path

    @model_validator(mode="after")
    def validate_formula_and_date(self) -> FormulaMarketJobRequest:
        compiled = compile_formula(self.formula)
        evaluate_compiled_formula(
            FormulaEvaluationInput(
                formula=self.formula,
                decision_date=self.trade_date,
                decision_at=self.decision_at,
                stocks=(),
            ),
            compiled,
        )
        return self


class FormulaMarketJobReceipt(RuntimeContractModel):
    task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    status: JobStatus
    attempts: int = Field(ge=0, strict=True)
    created_at: AwareUtcDatetime
    updated_at: AwareUtcDatetime
    result_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    error_code: JobErrorCode | None = None

    @model_validator(mode="after")
    def validate_status(self) -> FormulaMarketJobReceipt:
        if (self.status == "succeeded") != (self.result_sha256 is not None):
            raise ValueError("only a succeeded formula task can name a result")
        if (self.status == "failed") != (self.error_code is not None):
            raise ValueError("only a failed formula task can carry an error")
        return self


class FormulaMarketJobResult(RuntimeContractModel):
    schema_version: Literal[1] = 1
    task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    formula_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    summary: FormulaMarketRunSummary
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_result(self) -> FormulaMarketJobResult:
        summary = self.summary
        for moment in (
            summary.decision_at,
            summary.universe_completed_at,
            summary.projection_updated_at,
        ):
            if moment.isoformat() != normalize_aware_utc(moment).isoformat():
                raise ValueError("formula result times must be canonical UTC")
        counts = (
            summary.market_total,
            summary.listed_count,
            summary.paused_count,
            summary.match_count,
            summary.no_match_count,
            summary.unknown_count,
            *summary.unknown_reasons.values(),
        )
        if any(type(count) is not int or count < 0 for count in counts):
            raise ValueError("formula result contains invalid counts")
        if (
            summary.listed_count + summary.paused_count != summary.market_total
            or summary.match_count + summary.no_match_count + summary.unknown_count
            != summary.market_total
            or sum(summary.unknown_reasons.values()) != summary.unknown_count
            or len(summary.match_codes) != summary.match_count
            or tuple(sorted(set(summary.match_codes))) != summary.match_codes
        ):
            raise ValueError("formula result counts do not conserve the market")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_sha256"}))
        if self.content_sha256 != expected:
            raise ValueError("formula result content digest is invalid")
        return self

    @classmethod
    def create(
        cls,
        *,
        task_id: str,
        request_sha256: str,
        formula_sha256: str,
        summary: FormulaMarketRunSummary,
    ) -> FormulaMarketJobResult:
        summary = FormulaMarketRunSummary.model_validate(
            {
                **summary.model_dump(mode="python"),
                "decision_at": normalize_aware_utc(summary.decision_at),
                "universe_completed_at": normalize_aware_utc(summary.universe_completed_at),
                "projection_updated_at": normalize_aware_utc(summary.projection_updated_at),
            }
        )
        content = {
            "schema_version": 1,
            "task_id": task_id,
            "request_sha256": request_sha256,
            "formula_sha256": formula_sha256,
            "summary": summary,
        }
        return cls(**content, content_sha256=canonical_sha256(content))


@dataclass(frozen=True)
class _Claim:
    task_id: str
    token: str
    attempts: int
    request: FormulaMarketJobRequest
    request_sha256: str
    previous_token: str | None


def _now(clock: Callable[[], datetime]) -> datetime:
    return normalize_aware_utc(clock())


def _request_bytes(request: FormulaMarketJobRequest) -> bytes:
    return canonical_json_bytes(request.model_dump(mode="json"))


def _private_directory(path: Path) -> None:
    if not path.is_absolute() or path != Path(os.path.normpath(path)):
        raise ValueError("formula task paths must be absolute and normalized")
    path.mkdir(mode=0o700, exist_ok=True)
    info = path.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise ValueError("formula task directory must be private and owned")


def _file_identity(info: os.stat_result) -> tuple[int, int, int, int, int, int, int, int]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_uid,
        info.st_nlink,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def _read_file(directory: int, name: str) -> bytes:
    before = os.stat(name, dir_fd=directory, follow_symlinks=False)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_nlink != 1
        or not 0 < before.st_size <= _MAX_ARTIFACT_BYTES
    ):
        raise FormulaMarketArtifactUnavailableError("formula result file is unsafe")
    handle = os.open(name, _READ_FLAGS, dir_fd=directory)
    try:
        if _file_identity(os.fstat(handle)) != _file_identity(before):
            raise FormulaMarketArtifactUnavailableError("formula result changed while opening")
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
            or _file_identity(after) != _file_identity(before)
            or _file_identity(current) != _file_identity(before)
        ):
            raise FormulaMarketArtifactUnavailableError("formula result changed while reading")
        return b"".join(chunks)
    finally:
        os.close(handle)


def _write_file(directory: int, name: str, payload: bytes) -> None:
    handle = os.open(name, _WRITE_FLAGS, 0o600, dir_fd=directory)
    try:
        os.fchmod(handle, 0o600)
        view = memoryview(payload)
        while view:
            count = os.write(handle, view)
            if count <= 0:
                raise OSError("formula result write made no progress")
            view = view[count:]
        os.fsync(handle)
    finally:
        os.close(handle)


class FormulaMarketJobStore:
    """SQLite admission, state and immutable result binding for one active run."""

    def __init__(
        self,
        *,
        state_path: Path,
        artifact_directory: Path,
        clock: Callable[[], datetime] | None = None,
        lease_seconds: int = 180,
    ) -> None:
        if type(lease_seconds) is not int or not 3 <= lease_seconds <= 3600:
            raise ValueError("formula task lease must be between 3 and 3600 seconds")
        if not state_path.is_absolute() or state_path != Path(os.path.normpath(state_path)):
            raise ValueError("formula task state path must be absolute and normalized")
        self.state_path = state_path
        self.artifact_directory = artifact_directory
        self.clock = clock or (lambda: datetime.now(UTC))
        self.lease_seconds = lease_seconds
        _private_directory(state_path.parent)
        _private_directory(artifact_directory)
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            with self._transaction_on(connection):
                connection.execute(
                    """CREATE TABLE IF NOT EXISTS formula_market_job (
                        task_id TEXT PRIMARY KEY,
                        idempotency_key TEXT NOT NULL UNIQUE,
                        request_json TEXT NOT NULL,
                        request_sha256 TEXT NOT NULL,
                        status TEXT NOT NULL CHECK(status IN
                            ('queued','running','succeeded','failed')),
                        attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
                        lease_token TEXT,
                        lease_until TEXT,
                        result_sha256 TEXT,
                        error_code TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )"""
                )
                connection.execute(
                    """CREATE UNIQUE INDEX IF NOT EXISTS formula_market_one_active
                    ON formula_market_job((1)) WHERE status IN ('queued','running')"""
                )
        os.chmod(state_path, 0o600)

    def _connect(self) -> sqlite3.Connection:
        try:
            info = self.state_path.lstat()
        except FileNotFoundError:
            pass
        else:
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
                raise ValueError("formula task state file is unsafe")
        connection = sqlite3.connect(self.state_path, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @staticmethod
    @contextmanager
    def _transaction_on(connection: sqlite3.Connection) -> Iterator[None]:
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with closing(self._connect()) as connection, self._transaction_on(connection):
            yield connection

    @staticmethod
    def _request_from_row(row: sqlite3.Row) -> FormulaMarketJobRequest:
        payload = row["request_json"].encode("utf-8")
        request = FormulaMarketJobRequest.model_validate(strict_canonical_json_loads(payload))
        if (
            _request_bytes(request) != payload
            or hashlib.sha256(payload).hexdigest() != row["request_sha256"]
            or request.idempotency_key != row["idempotency_key"]
        ):
            raise ValueError("stored formula task request is invalid")
        return request

    @staticmethod
    def _receipt(row: sqlite3.Row) -> FormulaMarketJobReceipt:
        return FormulaMarketJobReceipt(
            task_id=row["task_id"],
            status=row["status"],
            attempts=row["attempts"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            result_sha256=row["result_sha256"],
            error_code=row["error_code"],
        )

    def submit(self, request: FormulaMarketJobRequest) -> FormulaMarketJobReceipt:
        request = FormulaMarketJobRequest.model_validate(request)
        payload = _request_bytes(request)
        digest = hashlib.sha256(payload).hexdigest()
        now = _now(self.clock).isoformat()
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM formula_market_job WHERE idempotency_key = ?",
                (request.idempotency_key,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["request_json"] != payload.decode()
                    or existing["request_sha256"] != digest
                ):
                    raise ValueError("idempotency key already binds a different formula request")
                task_id = existing["task_id"]
            else:
                count = connection.execute("SELECT COUNT(*) FROM formula_market_job").fetchone()[0]
                if count >= _MAX_JOBS:
                    raise ValueError("formula task capacity reached")
                if (
                    connection.execute(
                        "SELECT 1 FROM formula_market_job "
                        "WHERE status IN ('queued','running') LIMIT 1"
                    ).fetchone()
                    is not None
                ):
                    raise FormulaMarketJobActiveError("another formula task is active")
                task_id = uuid4().hex
                connection.execute(
                    """INSERT INTO formula_market_job
                    (task_id,idempotency_key,request_json,request_sha256,status,created_at,updated_at)
                    VALUES (?, ?, ?, ?, 'queued', ?, ?)""",
                    (task_id, request.idempotency_key, payload.decode(), digest, now, now),
                )
        return self.status(task_id)

    def admission_by_key(
        self,
        idempotency_key: str,
    ) -> tuple[FormulaMarketJobRequest, str] | None:
        """Recover an admitted task without needing its eventual result artifact."""
        if _KEY.fullmatch(idempotency_key) is None:
            raise ValueError("invalid formula task idempotency key")
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM formula_market_job WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        if row is None:
            return None
        request = self._request_from_row(row)
        task_id = row["task_id"]
        if _TASK_ID.fullmatch(task_id) is None:
            raise ValueError("stored formula task id is invalid")
        return request, task_id

    def _row(self, task_id: str) -> sqlite3.Row:
        if _TASK_ID.fullmatch(task_id) is None:
            raise ValueError("invalid formula task id")
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM formula_market_job WHERE task_id = ?", (task_id,)
            ).fetchone()
        if row is None:
            raise KeyError(task_id)
        return row

    def status(self, task_id: str) -> FormulaMarketJobReceipt:
        row = self._row(task_id)
        receipt = self._receipt(row)
        request = self._request_from_row(row)
        if receipt.status == "succeeded":
            self._load_result(receipt, request, row["request_sha256"])
        return receipt

    def latest(self) -> FormulaMarketJobReceipt | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT task_id FROM formula_market_job "
                "ORDER BY created_at DESC, rowid DESC LIMIT 1"
            ).fetchone()
        return None if row is None else self.status(row["task_id"])

    @staticmethod
    def _artifact_name(task_id: str, digest: str) -> str:
        if _TASK_ID.fullmatch(task_id) is None or _SHA256.fullmatch(digest) is None:
            raise FormulaMarketArtifactUnavailableError("formula result identity is invalid")
        return f"formula-market-v1-{task_id}-{digest}.json"

    def _load_result(
        self,
        receipt: FormulaMarketJobReceipt,
        request: FormulaMarketJobRequest,
        request_sha256: str,
    ) -> FormulaMarketJobResult:
        if receipt.result_sha256 is None or _SHA256.fullmatch(request_sha256) is None:
            raise FormulaMarketArtifactUnavailableError("formula result reference is invalid")
        name = self._artifact_name(receipt.task_id, receipt.result_sha256)
        try:
            _private_directory(self.artifact_directory)
            directory = os.open(self.artifact_directory, _DIR_FLAGS)
            try:
                payload = _read_file(directory, name)
            finally:
                os.close(directory)
            result = FormulaMarketJobResult.model_validate(strict_canonical_json_loads(payload))
            if (
                canonical_json_bytes(result.model_dump(mode="json")) != payload
                or result.task_id != receipt.task_id
                or result.content_sha256 != receipt.result_sha256
                or result.request_sha256 != request_sha256
                or result.formula_sha256 != hashlib.sha256(request.formula.encode()).hexdigest()
                or result.summary.trade_date != request.trade_date
                or result.summary.decision_at != request.decision_at
                or result.summary.universe_identity != request.expected_universe_sha256
                or result.summary.projection_identity != request.expected_projection_identity
            ):
                raise ValueError("formula result and request disagree")
            return result
        except (OSError, ValueError) as exc:
            raise FormulaMarketArtifactUnavailableError(
                "stored formula result cannot be verified"
            ) from exc

    def read_result(self, task_id: str) -> FormulaMarketJobResult:
        row = self._row(task_id)
        receipt = self._receipt(row)
        if receipt.status != "succeeded":
            raise ValueError("formula task has not succeeded")
        try:
            request = self._request_from_row(row)
        except ValueError as exc:
            raise FormulaMarketArtifactUnavailableError(
                "stored formula request is invalid"
            ) from exc
        return self._load_result(receipt, request, row["request_sha256"])

    def _claim(self) -> _Claim | None:
        now = _now(self.clock)
        with self._transaction() as connection:
            row = connection.execute(
                """SELECT * FROM formula_market_job WHERE status = 'queued'
                OR (status = 'running' AND lease_until <= ?)
                ORDER BY created_at, rowid LIMIT 1""",
                (now.isoformat(),),
            ).fetchone()
            if row is None:
                return None
            request = self._request_from_row(row)
            if row["attempts"] >= _MAX_ATTEMPTS:
                connection.execute(
                    """UPDATE formula_market_job SET status='failed', error_code='lease_exhausted',
                    lease_token=NULL, lease_until=NULL, updated_at=? WHERE task_id=?""",
                    (now.isoformat(), row["task_id"]),
                )
                return None
            token = secrets.token_hex(16)
            connection.execute(
                """UPDATE formula_market_job SET status='running', attempts=attempts+1,
                lease_token=?, lease_until=?, updated_at=? WHERE task_id=?""",
                (
                    token,
                    (now + timedelta(seconds=self.lease_seconds)).isoformat(),
                    now.isoformat(),
                    row["task_id"],
                ),
            )
            return _Claim(
                task_id=row["task_id"],
                token=token,
                attempts=row["attempts"] + 1,
                request=request,
                request_sha256=row["request_sha256"],
                previous_token=row["lease_token"] if row["status"] == "running" else None,
            )

    def _renew(self, claim: _Claim) -> bool:
        now = _now(self.clock)
        with self._transaction() as connection:
            changed = connection.execute(
                """UPDATE formula_market_job SET lease_until=?, updated_at=?
                WHERE task_id=? AND status='running' AND lease_token=? AND lease_until>?""",
                (
                    (now + timedelta(seconds=self.lease_seconds)).isoformat(),
                    now.isoformat(),
                    claim.task_id,
                    claim.token,
                    now.isoformat(),
                ),
            ).rowcount
            return changed == 1

    def _require_lease(self, claim: _Claim) -> None:
        now = _now(self.clock).isoformat()
        with closing(self._connect()) as connection:
            row = connection.execute(
                """SELECT 1 FROM formula_market_job WHERE task_id=? AND status='running'
                AND lease_token=? AND lease_until>?""",
                (claim.task_id, claim.token, now),
            ).fetchone()
        if row is None:
            raise RuntimeError("formula task lease was lost")

    def _discard_previous_stage(self, claim: _Claim) -> None:
        token = claim.previous_token
        if token is None:
            return
        if _TOKEN.fullmatch(token) is None:
            raise FormulaMarketArtifactUnavailableError("previous formula task token is invalid")
        stage = f".{claim.task_id}.{token}.stage"
        directory = os.open(self.artifact_directory, _DIR_FLAGS)
        try:
            self._require_lease(claim)
            try:
                info = os.stat(stage, dir_fd=directory, follow_symlinks=False)
            except FileNotFoundError:
                return
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_nlink not in (1, 2)
                or info.st_size > _MAX_ARTIFACT_BYTES
            ):
                raise FormulaMarketArtifactUnavailableError(
                    "previous formula result stage is unsafe"
                )
            os.unlink(stage, dir_fd=directory)
            os.fsync(directory)
        finally:
            os.close(directory)

    def _publish_result(self, claim: _Claim, result: FormulaMarketJobResult) -> None:
        payload = canonical_json_bytes(result.model_dump(mode="json"))
        if not 0 < len(payload) <= _MAX_ARTIFACT_BYTES:
            raise FormulaMarketArtifactUnavailableError("formula result exceeds artifact budget")
        name = self._artifact_name(claim.task_id, result.content_sha256)
        stage = f".{claim.task_id}.{claim.token}.stage"
        _private_directory(self.artifact_directory)
        directory = os.open(self.artifact_directory, _DIR_FLAGS)
        try:
            self._require_lease(claim)
            try:
                existing = _read_file(directory, name)
            except FileNotFoundError:
                pass
            else:
                if existing != payload:
                    raise FormulaMarketArtifactUnavailableError("formula result identity conflicts")
                return
            try:
                _write_file(directory, stage, payload)
                self._require_lease(claim)
                try:
                    os.link(
                        stage,
                        name,
                        src_dir_fd=directory,
                        dst_dir_fd=directory,
                        follow_symlinks=False,
                    )
                except FileExistsError:
                    existing = _read_file(directory, name)
                    if existing != payload:
                        raise FormulaMarketArtifactUnavailableError(
                            "formula result identity conflicts"
                        ) from None
                os.unlink(stage, dir_fd=directory)
                os.fsync(directory)
                if _read_file(directory, name) != payload:
                    raise FormulaMarketArtifactUnavailableError(
                        "formula result changed after publish"
                    )
            finally:
                with suppress(FileNotFoundError):
                    os.unlink(stage, dir_fd=directory)
        finally:
            os.close(directory)

    def _finish_success(
        self, claim: _Claim, result: FormulaMarketJobResult
    ) -> FormulaMarketJobReceipt:
        now = _now(self.clock).isoformat()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM formula_market_job WHERE task_id=?", (claim.task_id,)
            ).fetchone()
            if row is None:
                raise RuntimeError("formula task lease was lost")
            request = self._request_from_row(row)
            self._load_result(
                FormulaMarketJobReceipt(
                    task_id=claim.task_id,
                    status="succeeded",
                    attempts=claim.attempts,
                    created_at=datetime.fromisoformat(row["created_at"]),
                    updated_at=datetime.fromisoformat(row["updated_at"]),
                    result_sha256=result.content_sha256,
                ),
                request,
                claim.request_sha256,
            )
            changed = connection.execute(
                """UPDATE formula_market_job SET status='succeeded', result_sha256=?,
                lease_token=NULL, lease_until=NULL, updated_at=? WHERE task_id=? AND
                status='running' AND lease_token=? AND lease_until>?""",
                (result.content_sha256, now, claim.task_id, claim.token, now),
            ).rowcount
            if changed != 1:
                raise RuntimeError("formula task lease was lost")
        return self.status(claim.task_id)

    def _finish_failure(self, claim: _Claim, error_code: JobErrorCode) -> FormulaMarketJobReceipt:
        now = _now(self.clock).isoformat()
        with self._transaction() as connection:
            changed = connection.execute(
                """UPDATE formula_market_job SET status='failed', error_code=?,
                lease_token=NULL, lease_until=NULL, updated_at=? WHERE task_id=? AND
                status='running' AND lease_token=? AND lease_until>?""",
                (error_code, now, claim.task_id, claim.token, now),
            ).rowcount
            if changed != 1:
                raise RuntimeError("formula task lease was lost")
        return self.status(claim.task_id)


def _error_code(exc: Exception) -> JobErrorCode:
    if isinstance(exc, FormulaMarketRunTimeoutError):
        return "timeout"
    if isinstance(exc, (FormulaMarketRunBudgetError, FormulaProjectionBudgetError)):
        return "capacity"
    if isinstance(exc, EvaluationRejectedError):
        return "formula_rejected" if exc.code == "formula" else "source_changed"
    if isinstance(
        exc,
        (
            _FormulaMarketSourceRootMismatchError,
            FormulaMarketUniverseError,
            FormulaProjectionUnavailableError,
            OSError,
        ),
    ):
        return "source_changed"
    if isinstance(exc, FormulaMarketArtifactUnavailableError):
        return "artifact_invalid"
    return "internal_error"


class FormulaMarketJobWorker:
    """One bounded claim with a renewable lease and fenced completion."""

    def __init__(
        self,
        store: FormulaMarketJobStore,
        *,
        trusted_source_roots: tuple[Path, Path] | None = None,
    ) -> None:
        self.store = store
        self.trusted_source_roots = trusted_source_roots

    def run_one(self) -> FormulaMarketJobReceipt | None:
        claim = self.store._claim()
        if claim is None:
            return None
        stopped = threading.Event()
        lost = threading.Event()

        def heartbeat() -> None:
            while not stopped.wait(self.store.lease_seconds / 3):
                try:
                    if not self.store._renew(claim):
                        lost.set()
                        return
                except Exception:
                    lost.set()
                    return

        renewer = threading.Thread(target=heartbeat, name="formula-market-lease", daemon=True)
        renewer.start()
        try:
            try:
                request = claim.request
                if (
                    self.trusted_source_roots is not None
                    and (
                        request.universe_root,
                        request.projection_root,
                    )
                    != self.trusted_source_roots
                ):
                    raise _FormulaMarketSourceRootMismatchError("formula task source roots changed")
                self.store._discard_previous_stage(claim)
                summary = run_formula_market(
                    request.universe_root,
                    request.projection_root,
                    request.formula,
                    request.trade_date,
                    request.decision_at,
                    expected_universe_sha256=request.expected_universe_sha256,
                    expected_projection_identity=request.expected_projection_identity,
                )
                result = FormulaMarketJobResult.create(
                    task_id=claim.task_id,
                    request_sha256=claim.request_sha256,
                    formula_sha256=hashlib.sha256(request.formula.encode()).hexdigest(),
                    summary=summary,
                )
                if lost.is_set():
                    raise RuntimeError("formula task lease was lost")
                self.store._publish_result(claim, result)
                return self.store._finish_success(claim, result)
            except Exception as exc:
                if lost.is_set():
                    raise RuntimeError("formula task lease was lost") from exc
                return self.store._finish_failure(claim, _error_code(exc))
        finally:
            stopped.set()
            renewer.join(timeout=self.store.lease_seconds / 3 + 1)
