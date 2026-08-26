"""Transactional single-writer publication of verified daily-close candidates."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Literal, Protocol, Self

import duckdb
import pandas as pd
from pydantic import Field, StringConstraints, model_validator

from rquant.daily_close_candidate import (
    DailyCloseCandidate,
    DailyCloseCandidateFence,
    DailyCloseCandidateFenceGuard,
    DailyCloseCandidateStore,
)
from rquant.daily_close_gateway import DailyCloseGateway
from rquant.daily_pipeline_ledger import (
    DailyPipelineLedgerError,
    DailyStageAttempt,
    DailyStageReceipt,
    StageResult,
)
from rquant.ingest import (
    DailyIngestMaterialization,
    apply_daily_materialization_in_transaction,
    derive_daily_materialization_indicators,
)
from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.security_status import SecurityStatusDaily
from rquant.storage.duckdb import DuckDBStore
from rquant.suspension import normalize_suspend_d_snapshot

Sha256Hex = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class DailyCanonicalPublishError(RuntimeError):
    """Canonical publication evidence is stale, conflicting, or corrupt."""


class DailyCanonicalPublishBusyError(DailyCanonicalPublishError):
    """The one permitted canonical writer cannot currently acquire its lock."""


class DailyCanonicalLedgerFenceVerifier(DailyCloseCandidateFenceGuard, Protocol):
    """Holds the authoritative stage fence through canonical persistence."""


class CanonicalTableWatermark(RuntimeContractModel):
    table_name: str = Field(min_length=1)
    trade_date: date
    row_count: int = Field(ge=0)
    content_sha256: Sha256Hex


class CanonicalDatabaseIdentity(RuntimeContractModel):
    """The one physical DuckDB generation that may receive a daily publication."""

    canonical_path: str = Field(min_length=1)
    device: int = Field(ge=0)
    inode: int = Field(ge=0)
    generation_id: Sha256Hex | None = None

    @model_validator(mode="after")
    def bind_generation(self) -> Self:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"generation_id"}))
        if self.generation_id is None:
            object.__setattr__(self, "generation_id", expected)
        elif self.generation_id != expected:
            raise ValueError("canonical database identity generation changed")
        return self


class DailyCanonicalPublishReceipt(RuntimeContractModel):
    contract: Literal["daily-canonical-publish-receipt/v1"] = "daily-canonical-publish-receipt/v1"
    receipt_id: Sha256Hex | None = None
    generation_id: Sha256Hex
    trade_date: date
    revision: int = Field(ge=1)
    source_generation_id: Sha256Hex
    source_sequence: int = Field(ge=0)
    source_batch_id: Sha256Hex
    raw_content_sha256: Sha256Hex
    calendar_generation_id: Sha256Hex
    calendar_producer_commit: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
    calendar_content_sha256: Sha256Hex
    calendar_as_of: AwareUtcDatetime
    database_identity: CanonicalDatabaseIdentity
    available_at: AwareUtcDatetime
    committed_at: AwareUtcDatetime
    publication_mode: Literal["committed", "recovered"] = "committed"
    recovery_of_receipt_id: Sha256Hex | None = None
    db_content_sha256: Sha256Hex
    watermarks: tuple[CanonicalTableWatermark, ...]
    ledger_fencing_token: int = Field(ge=1)
    stage_result: StageResult
    expected_ledger_receipt: DailyStageReceipt

    @model_validator(mode="after")
    def bind_receipt(self) -> Self:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"receipt_id"}))
        if self.receipt_id is None:
            object.__setattr__(self, "receipt_id", expected)
        elif self.receipt_id != expected:
            raise ValueError("canonical publish receipt identity changed")
        if self.stage_result != self.expected_ledger_receipt.result:
            raise ValueError("canonical receipt ledger result changed")
        if self.calendar_generation_id != self.calendar_content_sha256:
            raise ValueError("canonical receipt calendar generation changed")
        if self.publication_mode == "committed" and self.recovery_of_receipt_id is not None:
            raise ValueError("committed canonical receipt cannot recover another receipt")
        if self.publication_mode == "recovered" and self.recovery_of_receipt_id is None:
            raise ValueError("recovered canonical receipt requires its committed receipt")
        return self


_PUBLICATION_DDL = """
CREATE TABLE IF NOT EXISTS daily_canonical_publication (
    generation_id          VARCHAR PRIMARY KEY,
    trade_date             DATE        NOT NULL,
    revision               INTEGER     NOT NULL,
    parent_generation_id   VARCHAR,
    source_generation_id   VARCHAR     NOT NULL,
    source_sequence        BIGINT      NOT NULL,
    source_batch_id        VARCHAR     NOT NULL,
    raw_content_sha256     VARCHAR     NOT NULL,
    available_at           TIMESTAMPTZ NOT NULL,
    committed_at           TIMESTAMPTZ NOT NULL,
    db_content_sha256      VARCHAR     NOT NULL,
    watermarks_json        JSON        NOT NULL,
    canonical_receipt_id   VARCHAR     NOT NULL,
    database_identity_json JSON        NOT NULL,
    is_current             BOOLEAN     NOT NULL,
    UNIQUE (trade_date, revision),
    UNIQUE (trade_date, source_generation_id, source_sequence)
);
"""

_RECEIPT_DDL = """
CREATE TABLE IF NOT EXISTS daily_canonical_publish_receipt (
    receipt_id             VARCHAR PRIMARY KEY,
    generation_id          VARCHAR     NOT NULL,
    ledger_run_id          VARCHAR     NOT NULL,
    ledger_stage_id        VARCHAR     NOT NULL,
    ledger_attempt_number  INTEGER     NOT NULL,
    ledger_fencing_token   BIGINT      NOT NULL,
    ledger_receipt_id      VARCHAR     NOT NULL,
    ledger_input_identity  VARCHAR     NOT NULL,
    db_content_sha256      VARCHAR     NOT NULL,
    watermarks_sha256      VARCHAR     NOT NULL,
    publication_receipt_id VARCHAR     NOT NULL,
    payload_sha256         VARCHAR     NOT NULL,
    payload_json           JSON        NOT NULL,
    UNIQUE (ledger_run_id, ledger_stage_id, ledger_attempt_number)
);
"""

_TABLE_QUERIES: tuple[tuple[str, str], ...] = (
    (
        "daily_bar",
        "SELECT ts_code, trade_date, open, high, low, close, pre_close, change, "
        "pct_chg, vol, amount FROM daily_bar WHERE trade_date = ? ORDER BY ts_code",
    ),
    (
        "stock_status_daily",
        "SELECT ts_code, trade_date, name, is_st, name_source, st_source, available_at, "
        "ingested_at, conflict_reason FROM stock_status_daily WHERE trade_date = ? "
        "ORDER BY ts_code",
    ),
    (
        "stock_suspend_event",
        "SELECT source, ts_code, trade_date, suspend_type, suspend_timing, session_scope, "
        "available_at, ingested_at FROM stock_suspend_event WHERE trade_date = ? "
        "ORDER BY source, ts_code, suspend_type, suspend_timing",
    ),
    (
        "stock_suspend_coverage",
        "SELECT source, trade_date, coverage_state, row_count, snapshot_hash, queried_at "
        "FROM stock_suspend_coverage WHERE trade_date = ? ORDER BY source",
    ),
    (
        "adj_factor",
        "SELECT ts_code, trade_date, adj_factor FROM adj_factor WHERE trade_date = ? "
        "ORDER BY ts_code",
    ),
    (
        "daily_basic",
        "SELECT ts_code, trade_date, turnover_rate, volume_ratio, total_mv, circ_mv "
        "FROM daily_basic WHERE trade_date = ? ORDER BY ts_code",
    ),
    (
        "index_daily_bar",
        "SELECT ts_code, trade_date, open, high, low, close, pre_close, change, "
        "pct_chg, vol, amount FROM index_daily_bar WHERE trade_date = ? ORDER BY ts_code",
    ),
    (
        "daily_indicator",
        "SELECT * FROM daily_indicator WHERE trade_date = ? ORDER BY ts_code",
    ),
    (
        "daily_state",
        "SELECT * FROM daily_state WHERE trade_date = ? ORDER BY ts_code",
    ),
)


class DailyCanonicalPublisher:
    def __init__(
        self,
        *,
        candidate_store: DailyCloseCandidateStore,
        raw_spool: LiveBatchSpool,
        indicator_reader_factory: Callable[[], DuckDBStore],
        writer_factory: Callable[[], DuckDBStore],
        ledger_fence_verifier: DailyCanonicalLedgerFenceVerifier,
        clock: Callable[[], datetime],
    ) -> None:
        self.candidate_store = candidate_store
        self._raw_spool = raw_spool
        self._indicator_reader_factory = indicator_reader_factory
        self._writer_factory = writer_factory
        self._ledger_fence_verifier = ledger_fence_verifier
        self._clock = clock
        self._lock_path = self.candidate_store.root / ".canonical-publish.lock"

    def publish(
        self,
        generation_id: str,
        *,
        attempt: DailyStageAttempt,
        ledger_input_identity: str,
        committed_at: datetime,
    ) -> DailyCanonicalPublishReceipt:
        verified_attempt = DailyStageAttempt.model_validate(attempt)
        del committed_at
        checked_at = self._now()
        if verified_attempt.stage_id != "canonical_publish":
            raise DailyCanonicalPublishError("ledger attempt is not canonical_publish")
        if not (verified_attempt.claimed_at <= checked_at < verified_attempt.lease_expires_at):
            raise DailyCanonicalPublishError("ledger attempt lease is not valid at commit")
        if len(ledger_input_identity) != 64 or any(
            character not in "0123456789abcdef" for character in ledger_input_identity
        ):
            raise ValueError("ledger_input_identity must be a sha256 hex digest")
        with self._publish_lock():
            try:
                fence_context = self._ledger_fence_verifier(verified_attempt, checked_at)
            except Exception as exc:
                raise DailyCanonicalPublishError("ledger fence verification failed") from exc
            try:
                with fence_context as fence:
                    return self._publish_fenced(
                        generation_id,
                        attempt=verified_attempt,
                        ledger_input_identity=ledger_input_identity,
                        fence=fence,
                    )
            except DailyPipelineLedgerError as exc:
                raise DailyCanonicalPublishError("ledger fence verification failed") from exc
            except TypeError as exc:
                raise DailyCanonicalPublishError("ledger fence guard is invalid") from exc

    def _publish_fenced(
        self,
        generation_id: str,
        *,
        attempt: DailyStageAttempt,
        ledger_input_identity: str,
        fence: DailyCloseCandidateFence,
    ) -> DailyCanonicalPublishReceipt:
        candidate = self.candidate_store.load_generation(generation_id)
        self._assert_boundary(
            candidate,
            fence=fence,
            ledger_input_identity=ledger_input_identity,
        )
        current = self.candidate_store.load_current(candidate.manifest.trade_date)
        if current.generation_id != candidate.generation_id:
            raise DailyCanonicalPublishError("canonical publisher accepts only candidate current")
        materialization = self._materialization(candidate)
        reader_identity: CanonicalDatabaseIdentity | None = None

        def reader_factory() -> DuckDBStore:
            nonlocal reader_identity
            reader = self._indicator_reader_factory()
            identity = self._database_identity(reader)
            if reader_identity is not None and reader_identity != identity:
                reader.__exit__()
                raise DailyCanonicalPublishError("indicator reader database identity changed")
            reader_identity = identity
            return reader

        try:
            target_indicators = derive_daily_materialization_indicators(
                materialization,
                indicator_reader_factory=reader_factory,
            )
        except duckdb.IOException as exc:
            if "lock" in str(exc).casefold():
                raise DailyCanonicalPublishBusyError("DuckDB reader lock is unavailable") from exc
            raise
        if reader_identity is None:
            raise DailyCanonicalPublishError("indicator reader database identity is missing")
        self._assert_boundary(
            candidate,
            fence=fence,
            ledger_input_identity=ledger_input_identity,
        )
        try:
            writer_context = self._writer_factory()
        except duckdb.IOException as exc:
            raise DailyCanonicalPublishBusyError("DuckDB writer lock is unavailable") from exc
        try:
            with writer_context as writer:
                writer_identity = self._database_identity(writer)
                if writer_identity != reader_identity:
                    raise DailyCanonicalPublishError(
                        "canonical reader and writer database identity differ"
                    )
                return self._commit_or_recover(
                    writer,
                    candidate=candidate,
                    materialization=materialization,
                    target_indicators=target_indicators,
                    attempt=attempt,
                    ledger_input_identity=ledger_input_identity,
                    fence=fence,
                    database_identity=writer_identity,
                )
        except duckdb.IOException as exc:
            if "lock" in str(exc).casefold():
                raise DailyCanonicalPublishBusyError("DuckDB writer lock is unavailable") from exc
            raise

    def _commit_or_recover(
        self,
        writer: DuckDBStore,
        *,
        candidate: DailyCloseCandidate,
        materialization: DailyIngestMaterialization,
        target_indicators: pd.DataFrame,
        attempt: DailyStageAttempt,
        ledger_input_identity: str,
        fence: DailyCloseCandidateFence,
        database_identity: CanonicalDatabaseIdentity,
    ) -> DailyCanonicalPublishReceipt:
        transaction_open = False
        try:
            writer._conn.execute("BEGIN")
            transaction_open = True
            self._initialize_metadata(writer)
            self._assert_boundary(
                candidate,
                fence=fence,
                ledger_input_identity=ledger_input_identity,
            )
            duplicate = self._publication_row(writer, candidate.generation_id)
            if duplicate is not None:
                receipt = self._recover_in_transaction(
                    writer,
                    candidate=candidate,
                    publication=duplicate,
                    attempt=attempt,
                    ledger_input_identity=ledger_input_identity,
                    database_identity=database_identity,
                )
                self._assert_boundary(
                    candidate,
                    fence=fence,
                    ledger_input_identity=ledger_input_identity,
                )
                writer._conn.execute("COMMIT")
                transaction_open = False
                return receipt
            current = writer._conn.execute(
                """
                SELECT generation_id, revision
                FROM daily_canonical_publication
                WHERE trade_date = ? AND is_current = TRUE
                """,
                [candidate.manifest.trade_date],
            ).fetchone()
            if current is None:
                if candidate.manifest.parent_generation_id is not None:
                    raise DailyCanonicalPublishError("canonical revision has no database parent")
            elif not (
                str(current[0]) == candidate.manifest.parent_generation_id
                and int(current[1]) + 1 == candidate.manifest.revision
            ):
                raise DailyCanonicalPublishError("canonical database revision chain changed")

            apply_daily_materialization_in_transaction(
                writer,
                materialization,
                target_indicators=target_indicators,
                replace_trade_date=True,
                include_market_sentiment=False,
            )
            self._assert_boundary(
                candidate,
                fence=fence,
                ledger_input_identity=ledger_input_identity,
            )
            watermarks = self._collect_watermarks(writer, candidate.manifest.trade_date)
            db_content_sha256 = canonical_sha256(watermarks)
            receipt = self._receipt(
                candidate,
                watermarks=watermarks,
                db_content_sha256=db_content_sha256,
                attempt=attempt,
                ledger_input_identity=ledger_input_identity,
                committed_at=self._now(),
                database_identity=database_identity,
            )
            writer._conn.execute(
                "UPDATE daily_canonical_publication SET is_current = FALSE "
                "WHERE trade_date = ? AND is_current = TRUE",
                [candidate.manifest.trade_date],
            )
            writer._conn.execute(
                """
                INSERT INTO daily_canonical_publication(
                    generation_id, trade_date, revision, parent_generation_id,
                    source_generation_id, source_sequence, source_batch_id,
                    raw_content_sha256, available_at, committed_at,
                    db_content_sha256, watermarks_json, canonical_receipt_id,
                    database_identity_json, is_current
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                """,
                [
                    candidate.generation_id,
                    candidate.manifest.trade_date,
                    candidate.manifest.revision,
                    candidate.manifest.parent_generation_id,
                    candidate.manifest.source_generation_id,
                    candidate.manifest.source_sequence,
                    candidate.manifest.source_batch_id,
                    candidate.manifest.raw_content_sha256,
                    candidate.manifest.available_at,
                    receipt.committed_at,
                    db_content_sha256,
                    self._model_json(watermarks),
                    receipt.receipt_id,
                    self._model_json(database_identity),
                ],
            )
            self._insert_receipt(writer, receipt)
            self._assert_boundary(
                candidate,
                fence=fence,
                ledger_input_identity=ledger_input_identity,
            )
            writer._conn.execute("COMMIT")
            transaction_open = False
            return receipt
        except BaseException as error:
            if transaction_open:
                try:
                    writer._conn.execute("ROLLBACK")
                except Exception as rollback_error:
                    error.add_note(f"canonical publication rollback failed: {rollback_error}")
            raise

    def _recover_in_transaction(
        self,
        writer: DuckDBStore,
        *,
        candidate: DailyCloseCandidate,
        publication: tuple[object, ...],
        attempt: DailyStageAttempt,
        ledger_input_identity: str,
        database_identity: CanonicalDatabaseIdentity,
    ) -> DailyCanonicalPublishReceipt:
        if not (
            publication[0] == candidate.generation_id
            and publication[1] == candidate.manifest.trade_date
            and int(publication[2]) == candidate.manifest.revision
            and publication[3] == candidate.manifest.raw_content_sha256
            and publication[8] == self._model_json(database_identity)
        ):
            raise DailyCanonicalPublishError("committed canonical identity changed")
        try:
            stored_watermarks = tuple(
                CanonicalTableWatermark.model_validate(item)
                for item in json.loads(str(publication[5]))
            )
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise DailyCanonicalPublishError("committed canonical watermarks are corrupt") from exc
        if str(publication[4]) != canonical_sha256(stored_watermarks):
            raise DailyCanonicalPublishError("committed canonical database content changed")
        observed_watermarks = self._collect_watermarks(writer, candidate.manifest.trade_date)
        if observed_watermarks != stored_watermarks:
            raise DailyCanonicalPublishError("committed canonical database content changed")
        canonical_receipt_id = publication[6]
        if not isinstance(canonical_receipt_id, str):
            raise DailyCanonicalPublishError("committed canonical receipt is missing")
        stored = self._receipt_for_attempt(
            writer,
            attempt=attempt,
            candidate=candidate,
            ledger_input_identity=ledger_input_identity,
            publication_receipt_id=canonical_receipt_id,
            database_identity=database_identity,
            watermarks=stored_watermarks,
        )
        if stored is not None:
            return stored
        receipt = self._receipt(
            candidate,
            watermarks=stored_watermarks,
            db_content_sha256=canonical_sha256(stored_watermarks),
            attempt=attempt,
            ledger_input_identity=ledger_input_identity,
            committed_at=self._now(),
            database_identity=database_identity,
            publication_mode="recovered",
            recovery_of_receipt_id=canonical_receipt_id,
        )
        self._insert_receipt(writer, receipt)
        return receipt

    @staticmethod
    def _publication_row(
        store: DuckDBStore,
        generation_id: str,
    ) -> tuple[object, ...] | None:
        return store._conn.execute(
            """
            SELECT generation_id, trade_date, revision, raw_content_sha256,
                   db_content_sha256, watermarks_json, canonical_receipt_id, is_current,
                   database_identity_json
            FROM daily_canonical_publication WHERE generation_id = ?
            """,
            [generation_id],
        ).fetchone()

    @staticmethod
    def _initialize_metadata(writer: DuckDBStore) -> None:
        writer._conn.execute(_PUBLICATION_DDL)
        writer._conn.execute(_RECEIPT_DDL)
        for statement in (
            "ALTER TABLE daily_canonical_publication "
            "ADD COLUMN IF NOT EXISTS database_identity_json JSON",
            "ALTER TABLE daily_canonical_publish_receipt "
            "ADD COLUMN IF NOT EXISTS ledger_input_identity VARCHAR",
            "ALTER TABLE daily_canonical_publish_receipt "
            "ADD COLUMN IF NOT EXISTS db_content_sha256 VARCHAR",
            "ALTER TABLE daily_canonical_publish_receipt "
            "ADD COLUMN IF NOT EXISTS watermarks_sha256 VARCHAR",
            "ALTER TABLE daily_canonical_publish_receipt "
            "ADD COLUMN IF NOT EXISTS publication_receipt_id VARCHAR",
            "ALTER TABLE daily_canonical_publish_receipt "
            "ADD COLUMN IF NOT EXISTS payload_sha256 VARCHAR",
        ):
            writer._conn.execute(statement)

    def _assert_boundary(
        self,
        candidate: DailyCloseCandidate,
        *,
        fence: DailyCloseCandidateFence,
        ledger_input_identity: str,
    ) -> None:
        checked_at = self._now()
        fence.assert_current(checked_at)
        fence.assert_source(
            candidate.manifest.source_generation_id,
            candidate.manifest.raw_content_sha256,
        )
        fence.assert_input(ledger_input_identity)
        self._assert_raw_authoritative_current(candidate)

    def _assert_raw_authoritative_current(self, candidate: DailyCloseCandidate) -> None:
        manifest = candidate.manifest
        descriptor = self._raw_spool.source_descriptor(LiveChannel.DAILY_CLOSE)
        current = self._raw_spool.current(LiveChannel.DAILY_CLOSE)
        if current is None or not (
            current.source_generation_id == manifest.source_generation_id
            and current.sequence == manifest.source_sequence
            and current.batch_id == manifest.source_batch_id
            and current.content_sha256 == manifest.source_payload_sha256
            and current.revision == manifest.revision
            and current.quality_status is BatchQualityStatus.PUBLISHED
            and descriptor.generation_id == manifest.source_generation_id
        ):
            raise DailyCanonicalPublishError("raw authoritative current changed")
        records = self._raw_spool.list_after(
            LiveChannel.DAILY_CLOSE,
            sequence=manifest.source_sequence - 1,
        )
        record = next(
            (item for item in records if item.envelope.sequence == manifest.source_sequence),
            None,
        )
        if record is None or not (
            record.envelope.identity_sha256 == manifest.envelope_sha256
            and record.envelope.batch_id == manifest.source_batch_id
            and record.envelope.content_sha256 == manifest.source_payload_sha256
            and record.envelope.source_request_id == manifest.source_request_id
            and record.envelope.revision == manifest.revision
            and record.envelope.quality_status is BatchQualityStatus.PUBLISHED
        ):
            raise DailyCanonicalPublishError("raw authoritative immutable record changed")
        try:
            raw = DailyCloseGateway.decode_payload(self._raw_spool.read_payload(record))
        except Exception as exc:
            raise DailyCanonicalPublishError("raw authoritative payload is unavailable") from exc
        if not (
            raw.content_sha256 == manifest.raw_content_sha256
            and raw.source_request_id == manifest.source_request_id
            and raw.revision == manifest.revision
            and raw.available_at == manifest.available_at
        ):
            raise DailyCanonicalPublishError("raw authoritative payload binding changed")

    def _now(self) -> datetime:
        try:
            return normalize_aware_utc(self._clock())
        except Exception as exc:
            raise DailyCanonicalPublishError("canonical publisher clock is invalid") from exc

    @staticmethod
    def _database_identity(store: DuckDBStore) -> CanonicalDatabaseIdentity:
        path = Path(store.path)
        canonical_path = Path(os.path.abspath(path))
        if not path.is_absolute() or path != canonical_path:
            raise DailyCanonicalPublishError("canonical database path is not normalized")
        try:
            observed = path.lstat()
        except OSError as exc:
            raise DailyCanonicalPublishError("canonical database identity is unavailable") from exc
        if not stat.S_ISREG(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
            raise DailyCanonicalPublishError("canonical database path is unsafe")
        return CanonicalDatabaseIdentity(
            canonical_path=str(canonical_path),
            device=observed.st_dev,
            inode=observed.st_ino,
        )

    @staticmethod
    def _materialization(candidate: DailyCloseCandidate) -> DailyIngestMaterialization:
        available_at = candidate.manifest.available_at
        statuses = tuple(
            SecurityStatusDaily(
                ts_code=row.ts_code,
                trade_date=row.trade_date,
                name=row.name,
                is_st=row.is_st,
                name_source="daily_close",
                st_source="daily_close",
                available_at=available_at,
                ingested_at=available_at,
            )
            for row in candidate.facts.security_status
        )
        suspension_frame = pd.DataFrame.from_records(
            (
                {
                    "ts_code": row.ts_code,
                    "trade_date": row.trade_date,
                    "suspend_timing": row.suspend_timing,
                    "suspend_type": row.suspend_type,
                }
                for row in candidate.facts.suspension_status
            ),
            columns=("ts_code", "trade_date", "suspend_timing", "suspend_type"),
        )
        suspension = normalize_suspend_d_snapshot(
            suspension_frame,
            trade_date=candidate.manifest.trade_date,
            queried_at=available_at,
            source="daily_close",
        )
        return DailyIngestMaterialization(
            trade_date=candidate.manifest.trade_date,
            available_at=available_at,
            facts=candidate.facts,
            security_status=statuses,
            suspension=suspension,
        )

    @classmethod
    def _collect_watermarks(
        cls,
        store: DuckDBStore,
        trade_date: date,
    ) -> tuple[CanonicalTableWatermark, ...]:
        watermarks: list[CanonicalTableWatermark] = []
        for table_name, query in _TABLE_QUERIES:
            cursor = store._conn.execute(query, [trade_date])
            columns = tuple(item[0] for item in cursor.description)
            rows = cursor.fetchall()
            watermarks.append(
                CanonicalTableWatermark(
                    table_name=table_name,
                    trade_date=trade_date,
                    row_count=len(rows),
                    content_sha256=canonical_sha256({"columns": columns, "rows": rows}),
                )
            )
        return tuple(watermarks)

    @staticmethod
    def _receipt(
        candidate: DailyCloseCandidate,
        *,
        watermarks: tuple[CanonicalTableWatermark, ...],
        db_content_sha256: str,
        attempt: DailyStageAttempt,
        ledger_input_identity: str,
        committed_at: datetime,
        database_identity: CanonicalDatabaseIdentity,
        publication_mode: Literal["committed", "recovered"] = "committed",
        recovery_of_receipt_id: str | None = None,
    ) -> DailyCanonicalPublishReceipt:
        evidence_hash = canonical_sha256(
            {
                "generation_id": candidate.generation_id,
                "available_at": candidate.manifest.available_at,
                "watermarks": watermarks,
            }
        )
        result = StageResult(
            content_hash=db_content_sha256,
            evidence_hash=evidence_hash,
        )
        expected_ledger_receipt = DailyStageReceipt(
            mode=attempt.mode,
            run_id=attempt.run_id,
            stage_id=attempt.stage_id,
            attempt_number=attempt.attempt_number,
            input_identity=ledger_input_identity,
            result=result,
            prepared_at=committed_at,
        )
        return DailyCanonicalPublishReceipt(
            generation_id=candidate.generation_id,
            trade_date=candidate.manifest.trade_date,
            revision=candidate.manifest.revision,
            source_generation_id=candidate.manifest.source_generation_id,
            source_sequence=candidate.manifest.source_sequence,
            source_batch_id=candidate.manifest.source_batch_id,
            raw_content_sha256=candidate.manifest.raw_content_sha256,
            calendar_generation_id=candidate.manifest.calendar_generation_id,
            calendar_producer_commit=candidate.manifest.calendar_producer_commit,
            calendar_content_sha256=candidate.manifest.calendar_content_sha256,
            calendar_as_of=candidate.manifest.calendar_as_of,
            database_identity=database_identity,
            available_at=candidate.manifest.available_at,
            committed_at=committed_at,
            publication_mode=publication_mode,
            recovery_of_receipt_id=recovery_of_receipt_id,
            db_content_sha256=db_content_sha256,
            watermarks=watermarks,
            ledger_fencing_token=attempt.fencing_token,
            stage_result=result,
            expected_ledger_receipt=expected_ledger_receipt,
        )

    @classmethod
    def _insert_receipt(
        cls,
        writer: DuckDBStore,
        receipt: DailyCanonicalPublishReceipt,
    ) -> None:
        expected = receipt.expected_ledger_receipt
        payload_json = cls._model_json(receipt)
        publication_receipt_id = (
            receipt.receipt_id
            if receipt.publication_mode == "committed"
            else receipt.recovery_of_receipt_id
        )
        if publication_receipt_id is None:
            raise DailyCanonicalPublishError("canonical receipt publication binding is missing")
        writer._conn.execute(
            """
            INSERT INTO daily_canonical_publish_receipt(
                receipt_id, generation_id, ledger_run_id, ledger_stage_id,
                ledger_attempt_number, ledger_fencing_token,
                ledger_receipt_id, ledger_input_identity, db_content_sha256,
                watermarks_sha256, publication_receipt_id, payload_sha256, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                receipt.receipt_id,
                receipt.generation_id,
                expected.run_id,
                expected.stage_id,
                expected.attempt_number,
                receipt.ledger_fencing_token,
                expected.receipt_id,
                expected.input_identity,
                receipt.db_content_sha256,
                canonical_sha256(receipt.watermarks),
                publication_receipt_id,
                hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
                payload_json,
            ],
        )

    @classmethod
    def _receipt_for_attempt(
        cls,
        writer: DuckDBStore,
        *,
        attempt: DailyStageAttempt,
        candidate: DailyCloseCandidate,
        ledger_input_identity: str,
        publication_receipt_id: str,
        database_identity: CanonicalDatabaseIdentity,
        watermarks: tuple[CanonicalTableWatermark, ...],
    ) -> DailyCanonicalPublishReceipt | None:
        row = writer._conn.execute(
            """
            SELECT receipt_id, generation_id, ledger_run_id, ledger_stage_id,
                   ledger_attempt_number, ledger_fencing_token, ledger_receipt_id,
                   ledger_input_identity, db_content_sha256, watermarks_sha256,
                   publication_receipt_id, payload_sha256, payload_json
            FROM daily_canonical_publish_receipt
            WHERE ledger_run_id = ? AND ledger_stage_id = ?
              AND ledger_attempt_number = ?
            """,
            [attempt.run_id, attempt.stage_id, attempt.attempt_number],
        ).fetchone()
        if row is None:
            return None
        try:
            payload_json = str(row[12])
            receipt = DailyCanonicalPublishReceipt.model_validate_json(payload_json)
        except ValueError as exc:
            raise DailyCanonicalPublishError("stored canonical receipt is corrupt") from exc
        expected_publication_receipt_id = (
            receipt.receipt_id
            if receipt.publication_mode == "committed"
            else receipt.recovery_of_receipt_id
        )
        if not (
            receipt.receipt_id == row[0]
            and receipt.generation_id == row[1] == candidate.generation_id
            and receipt.expected_ledger_receipt.run_id == row[2] == attempt.run_id
            and receipt.expected_ledger_receipt.stage_id == row[3] == attempt.stage_id
            and receipt.expected_ledger_receipt.attempt_number
            == int(row[4])
            == attempt.attempt_number
            and receipt.ledger_fencing_token == int(row[5])
            and receipt.expected_ledger_receipt.receipt_id == row[6]
            and receipt.expected_ledger_receipt.input_identity == row[7] == ledger_input_identity
            and receipt.db_content_sha256 == row[8]
            and canonical_sha256(receipt.watermarks) == row[9]
            and expected_publication_receipt_id == row[10] == publication_receipt_id
            and hashlib.sha256(payload_json.encode("utf-8")).hexdigest() == row[11]
            and receipt.stage_result.content_hash == receipt.db_content_sha256
            and receipt.watermarks == watermarks
            and receipt.database_identity == database_identity
            and receipt.trade_date == candidate.manifest.trade_date
            and receipt.revision == candidate.manifest.revision
            and receipt.source_generation_id == candidate.manifest.source_generation_id
            and receipt.source_sequence == candidate.manifest.source_sequence
            and receipt.source_batch_id == candidate.manifest.source_batch_id
            and receipt.raw_content_sha256 == candidate.manifest.raw_content_sha256
            and receipt.calendar_generation_id == candidate.manifest.calendar_generation_id
            and receipt.calendar_producer_commit == candidate.manifest.calendar_producer_commit
            and receipt.calendar_content_sha256 == candidate.manifest.calendar_content_sha256
            and receipt.calendar_as_of == candidate.manifest.calendar_as_of
        ):
            raise DailyCanonicalPublishError("stored canonical receipt binding changed")
        # The fence is an ordering check, not an equality one. A replacement
        # writer adopts the running attempt (DailyPipelineLedger.recover leaves
        # it in place) and therefore reaches this receipt under the same attempt
        # number with a strictly newer token; that is the newer owner and it is
        # entitled to the stored receipt. A stored token newer than the caller's
        # means the caller is the stale writer and must not publish.
        if receipt.ledger_fencing_token > attempt.fencing_token:
            raise DailyCanonicalPublishError(
                "stored canonical receipt was written by a newer fencing token"
            )
        return receipt

    @staticmethod
    def _model_json(value: RuntimeContractModel | tuple[CanonicalTableWatermark, ...]) -> str:
        if isinstance(value, tuple):
            payload: object = [item.model_dump(mode="json") for item in value]
        else:
            payload = value.model_dump(mode="json")
        return json.dumps(
            payload,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    @contextmanager
    def _publish_lock(self) -> Iterator[None]:
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        if nofollow == 0:
            raise DailyCanonicalPublishError("canonical writer lock requires O_NOFOLLOW")
        descriptor = -1
        locked = False
        try:
            descriptor = os.open(
                self._lock_path,
                os.O_RDWR | os.O_CREAT | nofollow | getattr(os, "O_CLOEXEC", 0),
                0o600,
            )
            opened = os.fstat(descriptor)
            linked = self._lock_path.lstat()
            if not (
                stat.S_ISREG(opened.st_mode)
                and opened.st_uid == os.geteuid()
                and opened.st_nlink == 1
                and stat.S_IMODE(opened.st_mode) == 0o600
                and (opened.st_dev, opened.st_ino) == (linked.st_dev, linked.st_ino)
            ):
                raise DailyCanonicalPublishError("canonical writer lock is unsafe")
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise DailyCanonicalPublishBusyError("canonical writer lock is busy") from exc
            locked = True
            yield
        except DailyCanonicalPublishError:
            raise
        except OSError as exc:
            raise DailyCanonicalPublishError("canonical writer lock is unavailable") from exc
        finally:
            if descriptor >= 0:
                if locked:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)


__all__ = [
    "CanonicalTableWatermark",
    "DailyCanonicalPublishBusyError",
    "DailyCanonicalPublishError",
    "DailyCanonicalLedgerFenceVerifier",
    "DailyCanonicalPublishReceipt",
    "DailyCanonicalPublisher",
]
