from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from rquant.daily_canonical_publisher import (
    DailyCanonicalPublishBusyError,
    DailyCanonicalPublisher,
    DailyCanonicalPublishError,
)
from rquant.daily_close_candidate import DailyCloseCandidateStore
from rquant.daily_close_validation import DailyCloseValidator
from rquant.daily_pipeline_ledger import DailyPipelineLedgerError, DailyStageAttempt
from rquant.live_contracts import LiveChannel
from rquant.storage.duckdb import DuckDBStore
from tests.unit.test_daily_close_candidate import _publish_candidate, _signer
from tests.unit.test_daily_close_validation import (
    OBSERVED_AT,
    TRADE_DATE,
    _calendar,
    _policy,
    _published,
    _snapshot,
)

COMMITTED_AT = datetime(2026, 7, 31, 9, 10, tzinfo=UTC)
LEDGER_INPUT = "e" * 64


class _CurrentFence:
    def assert_current(self, checked_at: datetime) -> None:
        assert _attempt().claimed_at <= checked_at < _attempt().lease_expires_at

    def assert_source(
        self,
        _source_generation_id: str,
        _source_content_hash: str,
    ) -> None:
        return None

    def assert_input(self, _input_identity: str) -> None:
        return None


def _attempt(number: int = 1, fence: int = 1) -> DailyStageAttempt:
    return DailyStageAttempt(
        run_id="daily-canonical-test",
        stage_id="canonical_publish",
        attempt_number=number,
        fencing_token=fence,
        claimed_at=COMMITTED_AT - timedelta(seconds=1),
        lease_expires_at=COMMITTED_AT + timedelta(minutes=5),
    )


@contextmanager
def _assert_test_ledger_fence(
    attempt: DailyStageAttempt,
    checked_at: datetime,
) -> Iterator[_CurrentFence]:
    if not attempt.claimed_at <= checked_at < attempt.lease_expires_at:
        raise DailyPipelineLedgerError("test ledger attempt is stale")
    yield _CurrentFence()


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


def _candidate(
    tmp_path: Path,
    *,
    snapshots: list[dict[str, object]] | None = None,
):
    gateway, record = _published(tmp_path / "source", snapshots)
    verified = DailyCloseValidator(
        spool=gateway.spool,
        policy=_policy(),
        calendar=_calendar(),
    ).validate(record)
    store = DailyCloseCandidateStore(tmp_path / "candidates", signer=_signer())
    candidate = _publish_candidate(store, verified, spool=gateway.spool)
    return gateway, store, candidate


def _publisher(
    store: DailyCloseCandidateStore,
    db_path: Path,
    raw_spool,
    *,
    writer_factory=None,
    ledger_fence_verifier: Callable[[DailyStageAttempt, datetime], Iterator[object]] = (
        _assert_test_ledger_fence
    ),
) -> DailyCanonicalPublisher:
    return DailyCanonicalPublisher(
        candidate_store=store,
        raw_spool=raw_spool,
        indicator_reader_factory=lambda: DuckDBStore(db_path, read_only=True),
        writer_factory=writer_factory or (lambda: DuckDBStore(db_path)),
        ledger_fence_verifier=ledger_fence_verifier,
        clock=lambda: COMMITTED_AT,
    )


def test_publisher_uses_only_verified_current_candidate_and_preserves_pit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway, candidate_store, candidate = _candidate(tmp_path)
    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)

    def remote_forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("canonical publisher requested remote data")

    monkeypatch.setattr("rquant.ingest.ts.pro_api", remote_forbidden)
    receipt = _publisher(candidate_store, db_path, gateway.spool).publish(
        candidate.generation_id,
        attempt=_attempt(),
        ledger_input_identity=LEDGER_INPUT,
        committed_at=COMMITTED_AT,
    )

    assert receipt.generation_id == candidate.generation_id
    assert receipt.available_at == candidate.manifest.available_at
    assert receipt.expected_ledger_receipt.run_id == "daily-canonical-test"
    assert receipt.expected_ledger_receipt.result == receipt.stage_result
    assert receipt.expected_ledger_receipt.prepared_at == COMMITTED_AT
    assert receipt.calendar_generation_id == candidate.manifest.calendar_generation_id
    assert receipt.calendar_producer_commit == candidate.manifest.calendar_producer_commit
    assert receipt.calendar_content_sha256 == candidate.manifest.calendar_content_sha256
    assert receipt.calendar_as_of == candidate.manifest.calendar_as_of
    with DuckDBStore(db_path, read_only=True) as reader:
        bar = reader._conn.execute(
            "SELECT close FROM daily_bar WHERE ts_code = '600000.SH' AND trade_date = ?",
            [TRADE_DATE],
        ).fetchone()
        status = reader._conn.execute(
            "SELECT is_st, available_at FROM stock_status_daily WHERE ts_code = '600000.SH'",
        ).fetchone()
        publication = reader._conn.execute(
            """
            SELECT generation_id, available_at, db_content_sha256,
                   canonical_receipt_id
            FROM daily_canonical_publication
            WHERE trade_date = ? AND is_current = TRUE
            """,
            [TRADE_DATE],
        ).fetchone()
    assert bar == (10.2,)
    assert status == (False, candidate.manifest.available_at)
    assert publication == (
        candidate.generation_id,
        candidate.manifest.available_at,
        receipt.db_content_sha256,
        receipt.receipt_id,
    )


def test_publisher_rechecks_raw_authoritative_current_after_business_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway, candidate_store, candidate = _candidate(
        tmp_path,
        snapshots=[_snapshot(), _snapshot(close=10.3)],
    )
    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)
    publisher = DailyCanonicalPublisher(
        candidate_store=candidate_store,
        raw_spool=gateway.spool,
        indicator_reader_factory=lambda: DuckDBStore(db_path, read_only=True),
        writer_factory=lambda: DuckDBStore(db_path),
        ledger_fence_verifier=_assert_test_ledger_fence,
        clock=lambda: COMMITTED_AT,
    )
    apply_original = __import__(
        "rquant.daily_canonical_publisher", fromlist=["apply_daily_materialization_in_transaction"]
    ).apply_daily_materialization_in_transaction

    def apply_then_revise(*args: object, **kwargs: object) -> None:
        apply_original(*args, **kwargs)
        gateway.capture_once(
            trade_date=TRADE_DATE,
            observed_at=OBSERVED_AT + timedelta(seconds=1),
            refresh=True,
        )

    monkeypatch.setattr(
        "rquant.daily_canonical_publisher.apply_daily_materialization_in_transaction",
        apply_then_revise,
    )

    with pytest.raises(DailyCanonicalPublishError, match="raw authoritative current"):
        publisher.publish(
            candidate.generation_id,
            attempt=_attempt(),
            ledger_input_identity=LEDGER_INPUT,
            committed_at=COMMITTED_AT,
        )

    with DuckDBStore(db_path, read_only=True) as reader:
        assert reader._conn.execute("SELECT count(*) FROM daily_bar").fetchone()[0] == 0


def test_publisher_rolls_back_when_raw_revision_arrives_at_transaction_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway, candidate_store, candidate = _candidate(
        tmp_path,
        snapshots=[_snapshot(), _snapshot(close=10.3)],
    )
    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)
    publisher = _publisher(candidate_store, db_path, gateway.spool)
    initialize_original = publisher._initialize_metadata

    def initialize_then_revise(writer: DuckDBStore) -> None:
        initialize_original(writer)
        gateway.capture_once(
            trade_date=TRADE_DATE,
            observed_at=OBSERVED_AT + timedelta(seconds=1),
            refresh=True,
        )

    monkeypatch.setattr(publisher, "_initialize_metadata", initialize_then_revise)
    with pytest.raises(DailyCanonicalPublishError, match="raw authoritative current"):
        publisher.publish(
            candidate.generation_id,
            attempt=_attempt(),
            ledger_input_identity=LEDGER_INPUT,
            committed_at=COMMITTED_AT,
        )
    with DuckDBStore(db_path, read_only=True) as reader:
        assert reader._conn.execute("SELECT count(*) FROM daily_bar").fetchone()[0] == 0


def test_publisher_rolls_back_when_raw_revision_arrives_before_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway, candidate_store, candidate = _candidate(
        tmp_path,
        snapshots=[_snapshot(), _snapshot(close=10.3)],
    )
    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)
    publisher = _publisher(candidate_store, db_path, gateway.spool)
    insert_original = publisher._insert_receipt

    def insert_then_revise(writer: DuckDBStore, receipt: object) -> None:
        insert_original(writer, receipt)
        gateway.capture_once(
            trade_date=TRADE_DATE,
            observed_at=OBSERVED_AT + timedelta(seconds=1),
            refresh=True,
        )

    monkeypatch.setattr(publisher, "_insert_receipt", insert_then_revise)
    with pytest.raises(DailyCanonicalPublishError, match="raw authoritative current"):
        publisher.publish(
            candidate.generation_id,
            attempt=_attempt(),
            ledger_input_identity=LEDGER_INPUT,
            committed_at=COMMITTED_AT,
        )
    with DuckDBStore(db_path, read_only=True) as reader:
        assert reader._conn.execute("SELECT count(*) FROM daily_bar").fetchone()[0] == 0


def test_publisher_uses_injected_clock_at_every_fence_boundary(tmp_path: Path) -> None:
    gateway, candidate_store, candidate = _candidate(tmp_path)
    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)
    expired = False
    attempt = DailyStageAttempt(
        run_id="clock-boundary",
        stage_id="canonical_publish",
        attempt_number=1,
        fencing_token=1,
        claimed_at=COMMITTED_AT - timedelta(seconds=1),
        lease_expires_at=COMMITTED_AT + timedelta(seconds=1),
    )

    class Fence:
        def assert_current(self, checked_at: datetime) -> None:
            if not attempt.claimed_at <= checked_at < attempt.lease_expires_at:
                raise DailyPipelineLedgerError("daily stage fencing token is stale")

        def assert_source(self, _generation: str, _content_hash: str) -> None:
            return None

        def assert_input(self, _input_identity: str) -> None:
            return None

    @contextmanager
    def fence_guard(_attempt_value: DailyStageAttempt, _checked_at: datetime) -> Iterator[Fence]:
        yield Fence()

    class ExpiringStore(DuckDBStore):
        def upsert_daily(self, frame):
            nonlocal expired
            result = super().upsert_daily(frame)
            expired = True
            return result

    publisher = DailyCanonicalPublisher(
        candidate_store=candidate_store,
        raw_spool=gateway.spool,
        indicator_reader_factory=lambda: DuckDBStore(db_path, read_only=True),
        writer_factory=lambda: ExpiringStore(db_path),
        ledger_fence_verifier=fence_guard,
        clock=lambda: COMMITTED_AT + timedelta(seconds=2) if expired else COMMITTED_AT,
    )
    with pytest.raises(DailyCanonicalPublishError, match="fenc"):
        publisher.publish(
            candidate.generation_id,
            attempt=attempt,
            ledger_input_identity=LEDGER_INPUT,
            committed_at=COMMITTED_AT,
        )
    with DuckDBStore(db_path, read_only=True) as reader:
        assert reader._conn.execute("SELECT count(*) FROM daily_bar").fetchone()[0] == 0


def test_publisher_rejects_a_complete_receipt_from_another_ledger_attempt(
    tmp_path: Path,
) -> None:
    gateway, candidate_store, candidate = _candidate(tmp_path)
    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)
    publisher = _publisher(candidate_store, db_path, gateway.spool)
    alpha = DailyStageAttempt(
        run_id="alpha-run",
        stage_id="canonical_publish",
        attempt_number=1,
        fencing_token=1,
        claimed_at=COMMITTED_AT - timedelta(seconds=1),
        lease_expires_at=COMMITTED_AT + timedelta(minutes=5),
    )
    beta = alpha.model_copy(update={"run_id": "beta-run"})
    publisher.publish(
        candidate.generation_id,
        attempt=alpha,
        ledger_input_identity=LEDGER_INPUT,
        committed_at=COMMITTED_AT,
    )
    publisher.publish(
        candidate.generation_id,
        attempt=beta,
        ledger_input_identity=LEDGER_INPUT,
        committed_at=COMMITTED_AT,
    )
    with DuckDBStore(db_path) as writer:
        beta_payload = writer._conn.execute(
            "SELECT payload_json FROM daily_canonical_publish_receipt WHERE ledger_run_id = ?",
            [beta.run_id],
        ).fetchone()[0]
        writer._conn.execute(
            "UPDATE daily_canonical_publish_receipt SET payload_json = ? WHERE ledger_run_id = ?",
            [beta_payload, alpha.run_id],
        )
    with pytest.raises(DailyCanonicalPublishError, match="binding"):
        publisher.publish(
            candidate.generation_id,
            attempt=alpha,
            ledger_input_identity=LEDGER_INPUT,
            committed_at=COMMITTED_AT,
        )


def test_publisher_requires_reader_and_writer_to_open_the_same_database_generation(
    tmp_path: Path,
) -> None:
    gateway, candidate_store, candidate = _candidate(tmp_path)
    reader_path = tmp_path / "reader.duckdb"
    writer_path = tmp_path / "writer.duckdb"
    _seed_database(reader_path)
    _seed_database(writer_path)
    publisher = DailyCanonicalPublisher(
        candidate_store=candidate_store,
        raw_spool=gateway.spool,
        indicator_reader_factory=lambda: DuckDBStore(reader_path, read_only=True),
        writer_factory=lambda: DuckDBStore(writer_path),
        ledger_fence_verifier=_assert_test_ledger_fence,
        clock=lambda: COMMITTED_AT,
    )
    with pytest.raises(DailyCanonicalPublishError, match="database identity differ"):
        publisher.publish(
            candidate.generation_id,
            attempt=_attempt(),
            ledger_input_identity=LEDGER_INPUT,
            committed_at=COMMITTED_AT,
        )
    with DuckDBStore(writer_path, read_only=True) as reader:
        assert reader._conn.execute("SELECT count(*) FROM daily_bar").fetchone()[0] == 0


def test_same_attempt_recovery_returns_receipt_without_reapplying_business_rows(
    tmp_path: Path,
) -> None:
    gateway, candidate_store, candidate = _candidate(tmp_path)
    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)

    class CountingStore(DuckDBStore):
        apply_count = 0

        def upsert_daily(self, frame):
            type(self).apply_count += 1
            return super().upsert_daily(frame)

    publisher = _publisher(
        candidate_store,
        db_path,
        gateway.spool,
        writer_factory=lambda: CountingStore(db_path),
    )
    first = publisher.publish(
        candidate.generation_id,
        attempt=_attempt(),
        ledger_input_identity=LEDGER_INPUT,
        committed_at=COMMITTED_AT,
    )
    recovered = publisher.publish(
        candidate.generation_id,
        attempt=_attempt(),
        ledger_input_identity=LEDGER_INPUT,
        committed_at=COMMITTED_AT,
    )

    assert recovered == first
    assert CountingStore.apply_count == 1


def test_revision_replaces_current_and_keeps_publication_history(tmp_path: Path) -> None:
    gateway, candidate_store, first = _candidate(
        tmp_path,
        snapshots=[_snapshot(), _snapshot(close=10.3)],
    )
    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)
    publisher = _publisher(candidate_store, db_path, gateway.spool)
    first_receipt = publisher.publish(
        first.generation_id,
        attempt=_attempt(),
        ledger_input_identity=LEDGER_INPUT,
        committed_at=COMMITTED_AT,
    )
    gateway.capture_once(
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT + timedelta(seconds=1),
        refresh=True,
    )
    record = gateway.spool.list_after(
        LiveChannel.DAILY_CLOSE,
        sequence=first.manifest.source_sequence,
    )[0]
    verified = DailyCloseValidator(
        spool=gateway.spool,
        policy=_policy(),
        calendar=_calendar(),
    ).validate(record)
    revised = _publish_candidate(candidate_store, verified, spool=gateway.spool)
    revised_receipt = publisher.publish(
        revised.generation_id,
        attempt=_attempt(number=2, fence=2),
        ledger_input_identity=LEDGER_INPUT,
        committed_at=COMMITTED_AT + timedelta(seconds=1),
    )

    with DuckDBStore(db_path, read_only=True) as reader:
        close = reader._conn.execute(
            "SELECT close FROM daily_bar WHERE trade_date = ?", [TRADE_DATE]
        ).fetchone()[0]
        history = reader._conn.execute(
            """
            SELECT generation_id, is_current
            FROM daily_canonical_publication
            WHERE trade_date = ? ORDER BY revision
            """,
            [TRADE_DATE],
        ).fetchall()
    assert close == pytest.approx(10.3)
    assert history == [
        (first.generation_id, False),
        (revised.generation_id, True),
    ]
    assert revised_receipt.db_content_sha256 != first_receipt.db_content_sha256


def test_historical_generation_recovery_returns_its_durable_receipt(
    tmp_path: Path,
) -> None:
    gateway, candidate_store, first = _candidate(
        tmp_path,
        snapshots=[_snapshot(), _snapshot(close=10.3)],
    )
    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)

    class CountingStore(DuckDBStore):
        apply_count = 0

        def upsert_daily(self, frame):
            type(self).apply_count += 1
            return super().upsert_daily(frame)

    publisher = _publisher(
        candidate_store,
        db_path,
        gateway.spool,
        writer_factory=lambda: CountingStore(db_path),
    )
    first_attempt = _attempt()
    first_receipt = publisher.publish(
        first.generation_id,
        attempt=first_attempt,
        ledger_input_identity=LEDGER_INPUT,
        committed_at=COMMITTED_AT,
    )

    gateway.capture_once(
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT + timedelta(seconds=1),
        refresh=True,
    )
    record = gateway.spool.list_after(
        LiveChannel.DAILY_CLOSE,
        sequence=first.manifest.source_sequence,
    )[0]
    verified = DailyCloseValidator(
        spool=gateway.spool,
        policy=_policy(),
        calendar=_calendar(),
    ).validate(record)
    revised = _publish_candidate(candidate_store, verified, spool=gateway.spool)
    publisher.publish(
        revised.generation_id,
        attempt=_attempt(number=2, fence=2),
        ledger_input_identity=LEDGER_INPUT,
        committed_at=COMMITTED_AT + timedelta(seconds=1),
    )
    assert candidate_store.load_current(TRADE_DATE).generation_id == revised.generation_id
    with DuckDBStore(db_path, read_only=True) as reader:
        assert reader._conn.execute(
            "SELECT is_current FROM daily_canonical_publication WHERE generation_id = ?",
            [first.generation_id],
        ).fetchone() == (False,)

    with pytest.raises(DailyCanonicalPublishError, match="raw authoritative current"):
        publisher.publish(
            first.generation_id,
            attempt=first_attempt,
            ledger_input_identity=LEDGER_INPUT,
            committed_at=COMMITTED_AT,
        )

    assert first_receipt.generation_id == first.generation_id
    assert CountingStore.apply_count == 2


def test_transaction_failure_and_lock_contention_publish_nothing(tmp_path: Path) -> None:
    gateway, candidate_store, candidate = _candidate(tmp_path)
    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)

    class FailingStore(DuckDBStore):
        def upsert_daily_basic(self, frame):
            del frame
            raise RuntimeError("daily basic failed")

    with pytest.raises(RuntimeError, match="daily basic failed"):
        _publisher(
            candidate_store,
            db_path,
            gateway.spool,
            writer_factory=lambda: FailingStore(db_path),
        ).publish(
            candidate.generation_id,
            attempt=_attempt(),
            ledger_input_identity=LEDGER_INPUT,
            committed_at=COMMITTED_AT,
        )
    with DuckDBStore(db_path, read_only=True) as reader:
        assert reader._conn.execute("SELECT count(*) FROM daily_bar").fetchone()[0] == 0
        table = reader._conn.execute(
            """
            SELECT count(*) FROM information_schema.tables
            WHERE table_name = 'daily_canonical_publication'
            """
        ).fetchone()[0]
        if table:
            assert (
                reader._conn.execute("SELECT count(*) FROM daily_canonical_publication").fetchone()[
                    0
                ]
                == 0
            )

    def locked_writer():
        raise duckdb.IOException("Could not set lock on file")

    with pytest.raises(DailyCanonicalPublishBusyError, match="lock"):
        _publisher(candidate_store, db_path, gateway.spool, writer_factory=locked_writer).publish(
            candidate.generation_id,
            attempt=_attempt(),
            ledger_input_identity=LEDGER_INPUT,
            committed_at=COMMITTED_AT,
        )


def test_publisher_rejects_noncurrent_candidate(tmp_path: Path) -> None:
    gateway, candidate_store, original = _candidate(
        tmp_path,
        snapshots=[_snapshot(), _snapshot(close=10.3)],
    )
    gateway.capture_once(
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT + timedelta(seconds=1),
        refresh=True,
    )
    record = gateway.spool.list_after(
        LiveChannel.DAILY_CLOSE,
        sequence=original.manifest.source_sequence,
    )[0]
    revised = DailyCloseValidator(
        spool=gateway.spool,
        policy=_policy(),
        calendar=_calendar(),
    ).validate(record)
    _publish_candidate(candidate_store, revised, spool=gateway.spool)
    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)

    with pytest.raises(DailyCanonicalPublishError, match="current"):
        _publisher(candidate_store, db_path, gateway.spool).publish(
            original.generation_id,
            attempt=_attempt(),
            ledger_input_identity=LEDGER_INPUT,
            committed_at=COMMITTED_AT,
        )


def test_ledger_fence_is_checked_under_lock_before_candidate_or_database_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway, candidate_store, candidate = _candidate(tmp_path)
    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)
    publisher = _publisher(candidate_store, db_path, gateway.spool)
    lock_held = False

    @contextmanager
    def observed_lock() -> Iterator[None]:
        nonlocal lock_held
        lock_held = True
        try:
            yield
        finally:
            lock_held = False

    def reject_stale(_attempt: DailyStageAttempt, _checked_at: datetime) -> None:
        assert lock_held
        raise DailyPipelineLedgerError("daily stage fencing token is stale")

    def unexpected_candidate_read(_generation_id: str) -> object:
        raise AssertionError("candidate read happened before ledger fence verification")

    monkeypatch.setattr(publisher, "_publish_lock", observed_lock)
    monkeypatch.setattr(
        publisher,
        "_ledger_fence_verifier",
        reject_stale,
        raising=False,
    )
    monkeypatch.setattr(candidate_store, "load_generation", unexpected_candidate_read)

    with pytest.raises(DailyCanonicalPublishError, match="fenc"):
        publisher.publish(
            candidate.generation_id,
            attempt=_attempt(),
            ledger_input_identity=LEDGER_INPUT,
            committed_at=COMMITTED_AT,
        )


def test_publisher_holds_stage_fence_through_the_duckdb_commit_boundary(tmp_path: Path) -> None:
    gateway, candidate_store, candidate = _candidate(tmp_path)
    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)
    active_fences = 0

    @contextmanager
    def tracked_fence(
        _attempt: DailyStageAttempt,
        _checked_at: datetime,
    ) -> Iterator[_CurrentFence]:
        nonlocal active_fences
        active_fences += 1
        try:
            yield _CurrentFence()
        finally:
            active_fences -= 1

    class FenceCheckingStore(DuckDBStore):
        def upsert_daily(self, frame):
            assert active_fences == 1
            return super().upsert_daily(frame)

    receipt = _publisher(
        candidate_store,
        db_path,
        gateway.spool,
        writer_factory=lambda: FenceCheckingStore(db_path),
        ledger_fence_verifier=tracked_fence,
    ).publish(
        candidate.generation_id,
        attempt=_attempt(),
        ledger_input_identity=LEDGER_INPUT,
        committed_at=COMMITTED_AT,
    )

    assert receipt.generation_id == candidate.generation_id
    assert active_fences == 0


def test_publisher_rolls_back_when_the_fence_turns_stale_before_commit(tmp_path: Path) -> None:
    gateway, candidate_store, candidate = _candidate(tmp_path)
    db_path = tmp_path / "canonical.duckdb"
    _seed_database(db_path)
    stale = False

    class Fence:
        def assert_current(self, _checked_at: datetime) -> None:
            if stale:
                raise DailyPipelineLedgerError("daily stage fencing token is stale")

        def assert_source(
            self,
            _source_generation_id: str,
            _source_content_hash: str,
        ) -> None:
            return None

        def assert_input(self, _input_identity: str) -> None:
            return None

    @contextmanager
    def fence_guard(
        _attempt: DailyStageAttempt,
        _checked_at: datetime,
    ) -> Iterator[Fence]:
        yield Fence()

    class StalingStore(DuckDBStore):
        def upsert_daily(self, frame):
            nonlocal stale
            result = super().upsert_daily(frame)
            stale = True
            return result

    publisher = _publisher(
        candidate_store,
        db_path,
        gateway.spool,
        writer_factory=lambda: StalingStore(db_path),
        ledger_fence_verifier=fence_guard,
    )
    with pytest.raises(DailyCanonicalPublishError, match="fenc"):
        publisher.publish(
            candidate.generation_id,
            attempt=_attempt(),
            ledger_input_identity=LEDGER_INPUT,
            committed_at=COMMITTED_AT,
        )

    with DuckDBStore(db_path, read_only=True) as reader:
        assert reader._conn.execute("SELECT count(*) FROM daily_bar").fetchone()[0] == 0
