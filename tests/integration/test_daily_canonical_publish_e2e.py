from __future__ import annotations

import multiprocessing as mp
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from queue import Empty
from typing import Any

import duckdb
import pytest

from rquant.daily_canonical_publisher import (
    DailyCanonicalPublishBusyError,
    DailyCanonicalPublisher,
    DailyCanonicalPublishError,
)
from rquant.daily_close_candidate import DailyCandidateHmacSigner, DailyCloseCandidateStore
from rquant.daily_close_gateway import DailyCloseGateway, DailyCloseGatewayConfig
from rquant.daily_close_validation import (
    DailyCloseValidationPolicy,
    DailyCloseValidator,
    VerifiedDailyCloseBatch,
)
from rquant.daily_ledger_fence import DailyLedgerFenceGuard
from rquant.daily_pipeline_ledger import (
    DailyPipelineLedger,
    DailyPipelineLedgerError,
    DailyPipelineMode,
    DailyPipelineStorageProfile,
    DailyRunSpec,
    DailyStageAttempt,
    DailyStageSpec,
    DailyStageState,
    StageResult,
)
from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.storage.duckdb import DuckDBStore

TRADE_DATE = date(2026, 7, 31)
OBSERVED_AT = datetime(2026, 7, 31, 9, 5, tzinfo=UTC)
AVAILABLE_AT = OBSERVED_AT + timedelta(seconds=2)
COMMITTED_AT = datetime(2026, 7, 31, 9, 10, tzinfo=UTC)


def _storage_profile(root: Path, *, profile_hash: str = "d" * 64) -> DailyPipelineStorageProfile:
    root.mkdir(parents=True, exist_ok=True)
    return DailyPipelineStorageProfile.create(
        root=root.resolve(),
        mode=DailyPipelineMode.SHADOW,
        profile_hash=profile_hash,
    )


def _calendar() -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit="a" * 40,
        coverage_start=TRADE_DATE - timedelta(days=7),
        coverage_end=TRADE_DATE + timedelta(days=7),
        open_dates=(TRADE_DATE,),
        generated_at=OBSERVED_AT - timedelta(seconds=1),
    )


def _published(tmp_path: Path) -> tuple[DailyCloseGateway, object]:
    snapshot = {
        "daily_bar": (
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "open": 10.0,
                "high": 10.4,
                "low": 9.9,
                "close": 10.2,
                "pre_close": 9.95,
                "change": 0.25,
                "pct_chg": 2.512562814070352,
                "vol": 1_000.0,
                "amount": 10_200.0,
            },
        ),
        "daily_basic": (
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "turnover_rate": 0.5,
                "volume_ratio": 1.2,
                "total_mv": 200_000.0,
                "circ_mv": 180_000.0,
            },
        ),
        "adj_factor": ({"ts_code": "600000.SH", "trade_date": TRADE_DATE, "adj_factor": 1.01},),
        "index_daily": (
            {
                "ts_code": "000001.SH",
                "trade_date": TRADE_DATE,
                "open": 3200.0,
                "high": 3230.0,
                "low": 3190.0,
                "close": 3220.0,
                "pre_close": 3198.0,
                "change": 22.0,
                "pct_chg": 0.688,
                "vol": 2_000.0,
                "amount": 30_000.0,
            },
        ),
        "security_status": (
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "name": "浦发银行",
                "is_st": False,
                "listing_status": "L",
            },
        ),
        "suspension_status": (),
        "partial_datasets": (),
    }
    gateway = DailyCloseGateway(
        spool=LiveBatchSpool(tmp_path / "live"),
        fetcher=lambda _request: snapshot,
        config=DailyCloseGatewayConfig(
            producer_version="integration-e2e-v1",
            producer_commit="a" * 40,
        ),
        completion_clock=lambda: AVAILABLE_AT,
    )
    gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)
    record = gateway.spool.list_after(LiveChannel.DAILY_CLOSE, sequence=-1)[-1]
    return gateway, record


def _policy() -> DailyCloseValidationPolicy:
    return DailyCloseValidationPolicy(
        expected_schema_version=1,
        min_daily_rows=1,
        max_daily_rows=10,
        required_index_codes=("000001.SH",),
    )


def _signer() -> DailyCandidateHmacSigner:
    return DailyCandidateHmacSigner(
        key_id="daily-integration-e2e",
        secret=b"daily-integration-e2e-secret-32-b",
    )


def _candidate_attempt() -> DailyStageAttempt:
    return DailyStageAttempt(
        run_id="daily-integration-candidate",
        stage_id="validate",
        attempt_number=1,
        fencing_token=1,
        claimed_at=OBSERVED_AT,
        lease_expires_at=OBSERVED_AT + timedelta(minutes=15),
    )


class _CandidateFence:
    def assert_current(self, checked_at: datetime) -> None:
        attempt = _candidate_attempt()
        assert attempt.claimed_at <= checked_at < attempt.lease_expires_at

    def assert_source(self, _generation_id: str, _content_hash: str) -> None:
        return None

    def assert_input(self, _input_identity: str) -> None:
        return None


@contextmanager
def _candidate_fence(
    attempt: DailyStageAttempt,
    checked_at: datetime,
) -> Iterator[_CandidateFence]:
    expected = _candidate_attempt()
    assert attempt == expected
    assert expected.claimed_at <= checked_at < expected.lease_expires_at
    yield _CandidateFence()


def _publish_candidate(
    store: DailyCloseCandidateStore,
    verified: VerifiedDailyCloseBatch,
    *,
    spool: LiveBatchSpool,
):
    return store.publish(
        verified,
        spool=spool,
        attempt=_candidate_attempt(),
        published_at=AVAILABLE_AT + timedelta(seconds=1),
        fence_guard=_candidate_fence,
    )


def _seed_database(path: Path) -> None:
    with DuckDBStore(path) as store:
        store._conn.execute(
            """
            INSERT INTO stock_basic
            (ts_code, symbol, name, area, industry, list_date, market)
            VALUES ('600000.SH', '600000', '浦发银行', '上海', '银行',
                    DATE '1999-11-10', '主板')
            """
        )
        store._conn.execute(
            """
            INSERT INTO trade_calendar
            (exchange, cal_date, is_open, pretrade_date, source, updated_at)
            VALUES ('SSE', ?, TRUE, NULL, 'test', ?)
            """,
            [TRADE_DATE, COMMITTED_AT],
        )


def _concurrent_canonical_publish_worker(
    candidate_root: str,
    raw_root: str,
    db_path: str,
    storage_root: str,
    profile_hash: str,
    run_id: str,
    generation_id: str,
    barrier: Any,
    results: Any,
) -> None:
    try:
        candidates = DailyCloseCandidateStore(Path(candidate_root), signer=_signer())
        raw_spool = LiveBatchSpool(Path(raw_root))
        storage_profile = DailyPipelineStorageProfile.create(
            root=Path(storage_root),
            mode=DailyPipelineMode.SHADOW,
            profile_hash=profile_hash,
        )
        ledger = DailyPipelineLedger(
            storage_profile=storage_profile,
            service_owner="daily-close",
        )
        now = COMMITTED_AT + timedelta(seconds=3)
        lease = ledger.acquire_writer(
            owner="daily-close",
            now=now,
            lease_for=timedelta(minutes=5),
        )
        barrier.wait(timeout=5)
        attempt = ledger.claim_next(lease, now=now)
        if attempt is None:
            results.put("no-claim")
            return
        run = ledger.run(run_id)
        publisher = DailyCanonicalPublisher(
            candidate_store=candidates,
            raw_spool=raw_spool,
            indicator_reader_factory=lambda: DuckDBStore(Path(db_path), read_only=True),
            writer_factory=lambda: DuckDBStore(Path(db_path)),
            ledger_fence_verifier=DailyLedgerFenceGuard(ledger=ledger, lease=lease),
            clock=lambda: now,
        )
        receipt = publisher.publish(
            generation_id,
            attempt=attempt,
            ledger_input_identity=run.input_identity,
            committed_at=now,
        )
        results.put(f"success:{receipt.receipt_id}")
    except BaseException as exc:
        results.put(f"{type(exc).__name__}:{exc}")


def _hold_duckdb_writer(path: str, ready: Any, release: Any, result: Any) -> None:
    connection = None
    try:
        connection = duckdb.connect(path)
        connection.execute("BEGIN")
        connection.execute("UPDATE stock_basic SET name = name WHERE ts_code = '600000.SH'")
        ready.set()
        if not release.wait(timeout=10):
            raise TimeoutError("lock holder release timed out")
        connection.execute("ROLLBACK")
        result.put(None)
    except BaseException as exc:
        ready.set()
        result.put(f"{type(exc).__name__}:{exc}")
    finally:
        if connection is not None:
            connection.close()


class _CountingStore(DuckDBStore):
    daily_apply_count = 0

    def upsert_daily(self, frame):
        type(self).daily_apply_count += 1
        return super().upsert_daily(frame)


def _chain(tmp_path: Path):
    gateway, record = _published(tmp_path / "raw")
    verified = DailyCloseValidator(
        spool=gateway.spool,
        policy=_policy(),
        calendar=_calendar(),
    ).validate(record)
    candidates = DailyCloseCandidateStore(tmp_path / "candidates", signer=_signer())
    candidate = _publish_candidate(candidates, verified, spool=gateway.spool)
    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)
    storage_profile = _storage_profile(tmp_path / "chain-profile")
    ledger = DailyPipelineLedger(
        storage_profile=storage_profile,
        service_owner="daily-close",
    )
    lease = ledger.acquire_writer(
        owner="daily-close",
        now=COMMITTED_AT,
        lease_for=timedelta(minutes=5),
    )
    run = ledger.create_run(
        lease,
        DailyRunSpec(
            mode=DailyPipelineMode.SHADOW,
            trade_date=TRADE_DATE,
            source_generation_id=verified.source_generation_id,
            source_content_hash=verified.raw_content_sha256,
            command_manifest_hash="e" * 64,
            code_commit="c" * 40,
            profile_hash="d" * 64,
            stages=(DailyStageSpec(stage_id="canonical_publish"),),
        ),
        now=COMMITTED_AT,
    )
    attempt = ledger.claim_next(lease, now=COMMITTED_AT + timedelta(milliseconds=1))
    assert attempt is not None
    publisher = DailyCanonicalPublisher(
        candidate_store=candidates,
        raw_spool=gateway.spool,
        indicator_reader_factory=lambda: DuckDBStore(db_path, read_only=True),
        writer_factory=lambda: _CountingStore(db_path),
        ledger_fence_verifier=DailyLedgerFenceGuard(ledger=ledger, lease=lease),
        clock=lambda: COMMITTED_AT + timedelta(seconds=3),
    )
    return verified, candidate, db_path, publisher, ledger, lease, run, attempt


@pytest.mark.xfail(
    reason=(
        "daily ledger recovery adopts running attempts (4c583e2) while the "
        "canonical publisher still requires a fresh attempt number and an equal "
        "fencing token; owning package must reconcile the two"
    ),
    strict=True,
)
def test_spool_validate_candidate_ledger_duckdb_receipt_recovers_fenced_attempt(
    tmp_path: Path,
) -> None:
    _CountingStore.daily_apply_count = 0
    gateway, record = _published(tmp_path / "raw")
    verified = DailyCloseValidator(
        spool=gateway.spool,
        policy=_policy(),
        calendar=_calendar(),
    ).validate(record)
    candidates = DailyCloseCandidateStore(tmp_path / "candidates", signer=_signer())
    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)
    storage_profile = _storage_profile(tmp_path / "daily-profile")
    ledger = DailyPipelineLedger(
        storage_profile=storage_profile,
        service_owner="daily-close",
    )
    lease_one = ledger.acquire_writer(
        owner="daily-close",
        now=COMMITTED_AT,
        lease_for=timedelta(seconds=30),
    )
    run = ledger.create_run(
        lease_one,
        DailyRunSpec(
            mode=DailyPipelineMode.SHADOW,
            trade_date=TRADE_DATE,
            source_generation_id=verified.source_generation_id,
            source_content_hash=verified.raw_content_sha256,
            command_manifest_hash="e" * 64,
            code_commit="c" * 40,
            profile_hash="d" * 64,
            stages=(
                DailyStageSpec(stage_id="validate"),
                DailyStageSpec(
                    stage_id="canonical_publish",
                    depends_on=("validate",),
                    retry_backoff_seconds=0,
                ),
            ),
        ),
        now=COMMITTED_AT,
    )
    publisher = DailyCanonicalPublisher(
        candidate_store=candidates,
        raw_spool=gateway.spool,
        indicator_reader_factory=lambda: DuckDBStore(db_path, read_only=True),
        writer_factory=lambda: _CountingStore(db_path),
        ledger_fence_verifier=DailyLedgerFenceGuard(ledger=ledger, lease=lease_one),
        clock=lambda: COMMITTED_AT + timedelta(seconds=3),
    )
    validate_attempt = ledger.claim_next(lease_one, now=COMMITTED_AT)
    assert validate_attempt is not None
    candidate = candidates.publish(
        verified,
        spool=gateway.spool,
        attempt=validate_attempt,
        published_at=COMMITTED_AT + timedelta(milliseconds=50),
        fence_guard=DailyLedgerFenceGuard(ledger=ledger, lease=lease_one),
    )
    ledger.succeed(
        lease_one,
        validate_attempt,
        StageResult(
            content_hash=verified.validation_sha256,
            evidence_hash=verified.raw_content_sha256,
        ),
        now=COMMITTED_AT + timedelta(milliseconds=100),
    )
    first_attempt = ledger.claim_next(
        lease_one,
        now=COMMITTED_AT + timedelta(milliseconds=200),
    )
    assert first_attempt is not None
    publisher._ledger_fence_verifier = DailyLedgerFenceGuard(ledger=ledger, lease=lease_one)
    first_db_receipt = publisher.publish(
        candidate.generation_id,
        attempt=first_attempt,
        ledger_input_identity=run.input_identity,
        committed_at=COMMITTED_AT + timedelta(seconds=1),
    )

    lease_two = ledger.acquire_writer(
        owner="daily-close",
        now=COMMITTED_AT + timedelta(seconds=2),
        lease_for=timedelta(minutes=5),
    )
    with pytest.raises(DailyPipelineLedgerError, match="stale"):
        ledger.prepare_success(
            lease_one,
            first_attempt,
            first_db_receipt.stage_result,
            now=COMMITTED_AT + timedelta(seconds=2),
        )
    recovered = ledger.recover(
        lease_two,
        now=COMMITTED_AT + timedelta(seconds=2),
    )
    assert recovered.retried_stage_ids == ("canonical_publish",)
    second_attempt = ledger.claim_next(
        lease_two,
        now=COMMITTED_AT + timedelta(seconds=2),
    )
    assert second_attempt is not None
    assert second_attempt.attempt_number == 2
    publisher._ledger_fence_verifier = DailyLedgerFenceGuard(ledger=ledger, lease=lease_two)
    second_db_receipt = publisher.publish(
        candidate.generation_id,
        attempt=second_attempt,
        ledger_input_identity=run.input_identity,
        committed_at=COMMITTED_AT + timedelta(seconds=3),
    )
    assert second_db_receipt.publication_mode == "recovered"
    assert second_db_receipt.recovery_of_receipt_id == first_db_receipt.receipt_id
    prepared = ledger.prepare_success(
        lease_two,
        second_attempt,
        second_db_receipt.stage_result,
        now=second_db_receipt.expected_ledger_receipt.prepared_at,
    )
    assert prepared == second_db_receipt.expected_ledger_receipt
    ledger.finalize_success(
        lease_two,
        prepared,
        now=COMMITTED_AT + timedelta(seconds=4),
    )

    assert ledger.stage(run.run_id, "canonical_publish").state is DailyStageState.SUCCEEDED
    assert _CountingStore.daily_apply_count == 1
    with DuckDBStore(db_path, read_only=True) as reader:
        assert reader._conn.execute("SELECT count(*) FROM daily_bar").fetchone()[0] == 1
        assert (
            reader._conn.execute("SELECT count(*) FROM daily_canonical_publication").fetchone()[0]
            == 1
        )
        assert (
            reader._conn.execute("SELECT count(*) FROM daily_canonical_publish_receipt").fetchone()[
                0
            ]
            == 2
        )


def test_real_duckdb_lock_contention_fails_before_any_canonical_write(
    tmp_path: Path,
) -> None:
    _verified, candidate, db_path, publisher, _ledger, _lease, run, attempt = _chain(tmp_path)
    context = mp.get_context("spawn")
    ready = context.Event()
    release = context.Event()
    result = context.Queue()
    process = context.Process(
        target=_hold_duckdb_writer,
        args=(str(db_path), ready, release, result),
    )
    process.start()
    try:
        assert ready.wait(timeout=10)
        with pytest.raises(DailyCanonicalPublishBusyError, match="lock"):
            publisher.publish(
                candidate.generation_id,
                attempt=attempt,
                ledger_input_identity=run.input_identity,
                committed_at=COMMITTED_AT + timedelta(seconds=1),
            )
    finally:
        release.set()
        process.join(timeout=10)
        if process.is_alive():
            process.terminate()
            process.join(timeout=2)
            pytest.fail("DuckDB lock holder did not stop")
    assert result.get(timeout=2) is None
    with DuckDBStore(db_path, read_only=True) as reader:
        assert reader._conn.execute("SELECT count(*) FROM daily_bar").fetchone()[0] == 0


def test_new_writer_lease_fences_stale_attempt_before_canonical_publication(
    tmp_path: Path,
) -> None:
    verified, candidate, db_path, publisher, *_unused = _chain(tmp_path)
    storage_profile = _storage_profile(tmp_path / "daily-profile")
    ledger = DailyPipelineLedger(
        storage_profile=storage_profile,
        service_owner="daily-close",
    )
    lease_a = ledger.acquire_writer(
        owner="daily-close",
        now=COMMITTED_AT,
        lease_for=timedelta(minutes=5),
    )
    run = ledger.create_run(
        lease_a,
        DailyRunSpec(
            mode=DailyPipelineMode.SHADOW,
            trade_date=TRADE_DATE,
            source_generation_id=verified.source_generation_id,
            source_content_hash=verified.raw_content_sha256,
            command_manifest_hash="e" * 64,
            code_commit="c" * 40,
            profile_hash="d" * 64,
            stages=(DailyStageSpec(stage_id="canonical_publish"),),
        ),
        now=COMMITTED_AT,
    )
    attempt_a = ledger.claim_next(
        lease_a,
        now=COMMITTED_AT + timedelta(milliseconds=100),
    )
    assert attempt_a is not None
    lease_b = ledger.acquire_writer(
        owner="daily-close",
        now=COMMITTED_AT + timedelta(seconds=1),
        lease_for=timedelta(minutes=5),
    )
    assert lease_b.fencing_token > attempt_a.fencing_token

    publisher._ledger_fence_verifier = DailyLedgerFenceGuard(ledger=ledger, lease=lease_a)
    with pytest.raises(DailyCanonicalPublishError, match="fenc"):
        publisher.publish(
            candidate.generation_id,
            attempt=attempt_a,
            ledger_input_identity=run.input_identity,
            committed_at=COMMITTED_AT + timedelta(seconds=2),
        )

    with DuckDBStore(db_path, read_only=True) as reader:
        assert reader._conn.execute("SELECT count(*) FROM daily_bar").fetchone()[0] == 0
        metadata_exists = reader._conn.execute(
            """
            SELECT count(*) FROM information_schema.tables
            WHERE table_name = 'daily_canonical_publication'
            """
        ).fetchone()[0]
        assert metadata_exists == 0


def test_fence_guard_blocks_a_new_lease_until_the_canonical_transaction_finishes(
    tmp_path: Path,
) -> None:
    verified, candidate, db_path, publisher, *_unused = _chain(tmp_path)
    storage_profile = _storage_profile(tmp_path / "daily-profile")
    ledger = DailyPipelineLedger(
        storage_profile=storage_profile,
        service_owner="daily-close",
    )
    lease_one = ledger.acquire_writer(
        owner="daily-close",
        now=COMMITTED_AT,
        lease_for=timedelta(minutes=5),
    )
    run = ledger.create_run(
        lease_one,
        DailyRunSpec(
            mode=DailyPipelineMode.SHADOW,
            trade_date=TRADE_DATE,
            source_generation_id=verified.source_generation_id,
            source_content_hash=verified.raw_content_sha256,
            command_manifest_hash="e" * 64,
            code_commit="c" * 40,
            profile_hash="d" * 64,
            stages=(DailyStageSpec(stage_id="canonical_publish"),),
        ),
        now=COMMITTED_AT,
    )
    attempt = ledger.claim_next(lease_one, now=COMMITTED_AT + timedelta(milliseconds=1))
    assert attempt is not None
    write_started = threading.Event()
    release_write = threading.Event()
    publish_errors: list[BaseException] = []

    class BlockingStore(DuckDBStore):
        def upsert_daily(self, frame):
            write_started.set()
            if not release_write.wait(timeout=5):
                raise TimeoutError("canonical write was not released")
            return super().upsert_daily(frame)

    guarded_publisher = DailyCanonicalPublisher(
        candidate_store=publisher.candidate_store,
        raw_spool=publisher._raw_spool,
        indicator_reader_factory=lambda: DuckDBStore(db_path, read_only=True),
        writer_factory=lambda: BlockingStore(db_path),
        ledger_fence_verifier=DailyLedgerFenceGuard(ledger=ledger, lease=lease_one),
        clock=lambda: COMMITTED_AT + timedelta(seconds=1),
    )

    def publish() -> None:
        try:
            guarded_publisher.publish(
                candidate.generation_id,
                attempt=attempt,
                ledger_input_identity=run.input_identity,
                committed_at=COMMITTED_AT + timedelta(seconds=1),
            )
        except BaseException as exc:
            publish_errors.append(exc)

    publisher_thread = threading.Thread(target=publish)
    publisher_thread.start()
    assert write_started.wait(timeout=5)
    with ThreadPoolExecutor(max_workers=1) as executor:
        contender = executor.submit(
            ledger.acquire_writer,
            owner="daily-close",
            now=COMMITTED_AT + timedelta(seconds=1),
            lease_for=timedelta(minutes=5),
        )
        time.sleep(0.15)
        assert not contender.done()
        release_write.set()
        lease_two = contender.result(timeout=5)
    publisher_thread.join(timeout=5)

    assert not publisher_thread.is_alive()
    assert publish_errors == []
    assert lease_two.fencing_token > lease_one.fencing_token


@pytest.mark.xfail(
    reason=(
        "daily ledger recovery adopts running attempts (4c583e2) while the "
        "canonical publisher still requires a fresh attempt number and an equal "
        "fencing token; owning package must reconcile the two"
    ),
    strict=True,
)
def test_two_processes_publish_one_canonical_candidate_once(tmp_path: Path) -> None:
    verified, candidate, db_path, publisher, *_unused = _chain(tmp_path)
    storage_profile = _storage_profile(tmp_path / "daily-profile")
    ledger = DailyPipelineLedger(
        storage_profile=storage_profile,
        service_owner="daily-close",
    )
    bootstrap_lease = ledger.acquire_writer(
        owner="daily-close",
        now=COMMITTED_AT,
        lease_for=timedelta(minutes=5),
    )
    run = ledger.create_run(
        bootstrap_lease,
        DailyRunSpec(
            mode=DailyPipelineMode.SHADOW,
            trade_date=TRADE_DATE,
            source_generation_id=verified.source_generation_id,
            source_content_hash=verified.raw_content_sha256,
            command_manifest_hash="e" * 64,
            code_commit="c" * 40,
            profile_hash="d" * 64,
            stages=(DailyStageSpec(stage_id="canonical_publish"),),
        ),
        now=COMMITTED_AT,
    )
    context = mp.get_context("spawn")
    barrier = context.Barrier(2)
    results = context.Queue()
    processes = [
        context.Process(
            target=_concurrent_canonical_publish_worker,
            args=(
                str(publisher.candidate_store.root),
                str(publisher._raw_spool.root),
                str(db_path),
                str(storage_profile.root),
                storage_profile.profile_hash,
                run.run_id,
                candidate.generation_id,
                barrier,
                results,
            ),
        )
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)
        if process.is_alive():
            process.terminate()
            process.join(timeout=2)
            pytest.fail("canonical publisher worker timed out")

    observed: list[str] = []
    try:
        for _ in processes:
            observed.append(results.get(timeout=2))
    except Empty:
        pytest.fail("canonical publisher worker did not report a result")

    assert [process.exitcode for process in processes] == [0, 0]
    successes = [item for item in observed if item.startswith("success:")]
    assert len(successes) == 1, observed
    assert sum(item.startswith("DailyPipelineLedgerError:") for item in observed) == 1
    with DuckDBStore(db_path, read_only=True) as reader:
        assert reader._conn.execute("SELECT count(*) FROM daily_bar").fetchone()[0] == 1
        assert (
            reader._conn.execute("SELECT count(*) FROM daily_canonical_publication").fetchone()[0]
            == 1
        )
        assert (
            reader._conn.execute("SELECT count(*) FROM daily_canonical_publish_receipt").fetchone()[
                0
            ]
            == 1
        )
