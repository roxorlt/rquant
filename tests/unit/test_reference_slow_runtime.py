from __future__ import annotations

import base64
import hashlib
import os
import sqlite3
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
from multiprocessing import get_context
from pathlib import Path
from threading import Event

import pytest

import rquant.runtime_service_builtin as runtime_service_builtin
from rquant.live_contracts import (
    BatchEnvelope,
    BatchQualityStatus,
    ConsumerCursor,
    LiveChannel,
)
from rquant.live_spool import LiveBatchSpool, LiveSpoolIntegrityError
from rquant.reference_data_registry import (
    ReferenceDataset,
    ReferenceDataUnavailableError,
    ReferencePublicationAuthenticator,
    ReferenceRegistry,
)
from rquant.reference_slow_publisher import (
    ReferenceDailyFact,
    ReferenceSecurityFact,
    ReferenceSlowSourceSnapshot,
)
from rquant.reference_slow_runtime import (
    ReferenceSlowRuntimeError,
    capture_reference_slow_batch,
    publish_reference_slow_batches,
)
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_service_builtin import (
    ReferenceSlowQuotaCapture,
    ReferenceSlowSourceSettings,
    build_builtin_registry,
    reference_slow_source_builder,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_serving_authority import ServingSourceAuthorityReader
from rquant.runtime_serving_snapshot import (
    REFERENCE_SLOW_AUTHORITY_DATASET_ID,
    ReferenceSlowPayload,
)
from rquant.source_quota_store import (
    SourceQuotaAttemptOutcome,
    SourceQuotaConflictError,
    SourceQuotaStore,
)
from rquant.source_quota_transport import QuotaBoundTransportObserver
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

COMMIT = "a" * 40
TARGET_DATE = date(2026, 7, 31)
PRIOR_DATE = date(2026, 7, 30)
OBSERVED_AT = datetime(2026, 7, 31, 1, 20, tzinfo=UTC)
PUBLISHED_AT = datetime(2026, 7, 31, 1, 25, tzinfo=UTC)
SAFE_PUBLISHED_AT = PUBLISHED_AT - timedelta(seconds=2)


@pytest.fixture(autouse=True)
def _reference_publication_credential(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "reference-publication-hmac.json"
    path.write_bytes(
        canonical_json_bytes(
            {
                "key_id": "test-reference-v1",
                "secret_hex": b"reference-publication-test-secret-0001".hex(),
            }
        )
    )
    path.chmod(0o600)
    monkeypatch.setenv("RQ_REFERENCE_PUBLICATION_HMAC_FILE", str(path))
    private_key = tmp_path / "reference-source-ed25519"
    subprocess.run(
        ("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(private_key)),
        check=True,
    )
    private_key.chmod(0o600)
    monkeypatch.setenv("RQ_REFERENCE_SOURCE_SIGNING_KEY_ID", "source-test-v1")
    monkeypatch.setenv(
        "RQ_REFERENCE_SOURCE_PRIVATE_KEY",
        private_key.read_text(encoding="ascii"),
    )
    monkeypatch.setenv(
        "RQ_REFERENCE_SOURCE_PUBLIC_KEY",
        private_key.with_suffix(".pub").read_text(encoding="ascii").strip(),
    )


def _runtime_capabilities() -> dict[str, str]:
    return {
        "RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID": "test-reference-v1",
        "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX": (
            b"reference-publication-test-secret-0001".hex()
        ),
        "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID": os.environ["RQ_REFERENCE_SOURCE_SIGNING_KEY_ID"],
        "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64": base64.b64encode(
            os.environ["RQ_REFERENCE_SOURCE_PRIVATE_KEY"].encode("ascii")
        ).decode("ascii"),
        "RQ_REFERENCE_SOURCE_PUBLIC_KEY": os.environ["RQ_REFERENCE_SOURCE_PUBLIC_KEY"],
    }


def test_reference_source_quota_cannot_understate_six_external_calls(
    tmp_path: Path,
) -> None:
    calendar = _calendar()
    with pytest.raises(ValueError, match="quota_cost_per_capture"):
        ReferenceSlowSourceSettings(
            database_path=(tmp_path / "daily.duckdb").resolve(),
            calendar_path=(tmp_path / "calendar.json").resolve(),
            calendar_expected_commit=calendar.producer_commit,
            calendar_content_sha256=calendar.content_sha256,
            spool_root=(tmp_path / "spool").resolve(),
            quota_path=(tmp_path / "quota.sqlite3").resolve(),
            quota_units_per_window=500,
            quota_cost_per_capture=5,
            producer_version="reference-source-v2",
        )

    settings = ReferenceSlowSourceSettings(
        database_path=(tmp_path / "daily.duckdb").resolve(),
        calendar_path=(tmp_path / "calendar.json").resolve(),
        calendar_expected_commit=calendar.producer_commit,
        calendar_content_sha256=calendar.content_sha256,
        spool_root=(tmp_path / "spool").resolve(),
        quota_path=(tmp_path / "quota.sqlite3").resolve(),
        quota_units_per_window=500,
        producer_version="reference-source-v2",
    )
    assert settings.quota_cost_per_capture == 6


def test_reference_kill_is_durable_and_same_request_cannot_refetch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = _calendar()
    settings = ReferenceSlowSourceSettings(
        database_path=(tmp_path / "daily.duckdb").resolve(),
        calendar_path=(tmp_path / "calendar.json").resolve(),
        calendar_expected_commit=calendar.producer_commit,
        calendar_content_sha256=calendar.content_sha256,
        spool_root=(tmp_path / "spool").resolve(),
        quota_path=(tmp_path / "quota.sqlite3").resolve(),
        quota_units_per_window=6,
        pending_recovery_min_age_seconds=60,
        producer_version="reference-source-v2",
    )
    calls = 0

    def kill(**_kwargs: object) -> None:
        nonlocal calls
        calls += 1
        raise KeyboardInterrupt()

    monkeypatch.setattr(
        "rquant.reference_slow_source.capture_reference_slow_source_snapshot",
        kill,
    )
    quota = SourceQuotaStore(
        settings.quota_path,
        boot_id="boot-a",
        monotonic_ns=lambda: 0,
    )
    kwargs = {
        "settings": settings,
        "quota_store": quota,
        "adapter": object(),
        "calendar": calendar,
        "target_trade_date": TARGET_DATE,
        "observed_at": OBSERVED_AT,
        "completion_clock": lambda: OBSERVED_AT,
        "producer_commit": COMMIT,
    }

    with pytest.raises(KeyboardInterrupt):
        runtime_service_builtin._capture_reference_with_quota(**kwargs)  # type: ignore[arg-type]
    kwargs["quota_store"] = SourceQuotaStore(
        settings.quota_path,
        boot_id="boot-a",
        monotonic_ns=lambda: 61_000_000_000,
    )
    kwargs["observed_at"] = OBSERVED_AT + timedelta(seconds=10)
    with pytest.raises(SourceQuotaConflictError, match="already exists"):
        runtime_service_builtin._capture_reference_with_quota(**kwargs)  # type: ignore[arg-type]

    assert calls == 1
    (attempt,) = quota.list_attempts(source=settings.source)
    assert attempt.dispatched_at is not None
    assert attempt.outcome is SourceQuotaAttemptOutcome.UNKNOWN
    assert quota.remaining(settings.source, now=OBSERVED_AT) == 0


def test_reference_explicit_revision_can_retry_same_trade_date(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = _calendar()
    settings = ReferenceSlowSourceSettings(
        database_path=(tmp_path / "daily.duckdb").resolve(),
        calendar_path=(tmp_path / "calendar.json").resolve(),
        calendar_expected_commit=calendar.producer_commit,
        calendar_content_sha256=calendar.content_sha256,
        spool_root=(tmp_path / "spool").resolve(),
        quota_path=(tmp_path / "quota.sqlite3").resolve(),
        quota_units_per_window=12,
        producer_version="reference-source-v2",
    )
    calls = 0

    def capture(**_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("first revision failed")
        return object()

    monkeypatch.setattr(
        "rquant.reference_slow_source.capture_reference_slow_source_snapshot",
        capture,
    )
    quota = SourceQuotaStore(settings.quota_path)
    kwargs = {
        "settings": settings,
        "quota_store": quota,
        "adapter": object(),
        "calendar": calendar,
        "target_trade_date": TARGET_DATE,
        "observed_at": OBSERVED_AT,
        "completion_clock": lambda: OBSERVED_AT,
        "producer_commit": COMMIT,
    }

    with pytest.raises(TimeoutError):
        runtime_service_builtin._capture_reference_with_quota(**kwargs)  # type: ignore[arg-type]
    with pytest.raises(SourceQuotaConflictError, match="already exists"):
        runtime_service_builtin._capture_reference_with_quota(**kwargs)  # type: ignore[arg-type]
    result = runtime_service_builtin._capture_reference_with_quota(  # type: ignore[arg-type]
        **kwargs,
        retry_ordinal=1,
    )

    assert result is not None
    assert calls == 2
    assert {attempt.outcome for attempt in quota.list_attempts(source=settings.source)} == {
        SourceQuotaAttemptOutcome.FAILURE,
        SourceQuotaAttemptOutcome.SUCCESS,
    }


def test_reference_transport_receipt_counts_each_real_call_and_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = _calendar()
    settings = ReferenceSlowSourceSettings(
        database_path=(tmp_path / "daily.duckdb").resolve(),
        calendar_path=(tmp_path / "calendar.json").resolve(),
        calendar_expected_commit=calendar.producer_commit,
        calendar_content_sha256=calendar.content_sha256,
        spool_root=(tmp_path / "spool").resolve(),
        quota_path=(tmp_path / "quota.sqlite3").resolve(),
        quota_units_per_window=20,
        quota_accounting_mode="transport",
        quota_cost_per_capture=None,
        producer_version="reference-source-v2",
    )
    quota = SourceQuotaStore(settings.quota_path)
    observer = QuotaBoundTransportObserver(
        store=quota,
        source=settings.source,
        quota_units_per_window=settings.quota_units_per_window,
        window_kind="minute",
        clock=lambda: OBSERVED_AT,
    )
    transport_calls: list[str] = []

    def transport(api_name: str, *, fails: bool = False) -> None:
        transport_calls.append(api_name)
        if fails:
            raise TimeoutError("retry")

    def capture(**_kwargs: object) -> ReferenceSlowSourceSnapshot:
        observer.observe("stock_st", lambda: transport("stock_st"))
        for status in ("L", "D", "P"):
            observer.observe("stock_basic", lambda value=status: transport(f"stock_basic:{value}"))
        try:
            observer.observe("adj_factor", lambda: transport("adj_factor", fails=True))
        except TimeoutError:
            observer.observe("adj_factor", lambda: transport("adj_factor"))
        observer.observe("suspend_d", lambda: transport("suspend_d"))
        return _snapshot()

    monkeypatch.setattr(
        "rquant.reference_slow_source.capture_reference_slow_source_snapshot",
        capture,
    )
    result = runtime_service_builtin._capture_reference_with_quota(
        settings=settings,
        quota_store=quota,
        adapter=object(),  # type: ignore[arg-type]
        calendar=calendar,
        target_trade_date=TARGET_DATE,
        observed_at=OBSERVED_AT,
        completion_clock=lambda: OBSERVED_AT,
        producer_commit=COMMIT,
        transport_observer=observer,
    )

    assert isinstance(result, ReferenceSlowQuotaCapture)
    assert result.source_usage.actual_call_count == 7
    assert len(result.source_usage.call_receipts) == 7
    assert len(transport_calls) == 7
    assert quota.remaining(settings.source, now=OBSERVED_AT) == 13


def test_reference_restart_aggregates_a_killed_last_call_as_unknown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = _calendar()
    settings = ReferenceSlowSourceSettings(
        database_path=(tmp_path / "daily.duckdb").resolve(),
        calendar_path=(tmp_path / "calendar.json").resolve(),
        calendar_expected_commit=calendar.producer_commit,
        calendar_content_sha256=calendar.content_sha256,
        spool_root=(tmp_path / "spool").resolve(),
        quota_path=(tmp_path / "quota.sqlite3").resolve(),
        quota_units_per_window=20,
        quota_accounting_mode="transport",
        quota_cost_per_capture=None,
        producer_version="reference-source-v2",
    )
    old_store = SourceQuotaStore(
        settings.quota_path,
        boot_id="boot-old",
        monotonic_ns=lambda: 0,
    )
    old_observer = QuotaBoundTransportObserver(
        store=old_store,
        source=settings.source,
        quota_units_per_window=settings.quota_units_per_window,
        window_kind="minute",
        clock=lambda: OBSERVED_AT,
    )

    def kill_last(**_kwargs: object) -> ReferenceSlowSourceSnapshot:
        for api_name in (
            "stock_st",
            "stock_basic",
            "stock_basic",
            "stock_basic",
            "adj_factor",
        ):
            old_observer.observe(api_name, lambda: None)
        old_observer.observe(
            "suspend_d",
            lambda: (_ for _ in ()).throw(KeyboardInterrupt()),
        )
        raise AssertionError("unreachable")

    monkeypatch.setattr(
        "rquant.reference_slow_source.capture_reference_slow_source_snapshot",
        kill_last,
    )
    with pytest.raises(KeyboardInterrupt):
        runtime_service_builtin._capture_reference_with_quota(
            settings=settings,
            quota_store=old_store,
            adapter=object(),  # type: ignore[arg-type]
            calendar=calendar,
            target_trade_date=TARGET_DATE,
            observed_at=OBSERVED_AT,
            completion_clock=lambda: OBSERVED_AT,
            producer_commit=COMMIT,
            transport_observer=old_observer,
        )

    external_calls = 0

    def forbidden(**_kwargs: object) -> ReferenceSlowSourceSnapshot:
        nonlocal external_calls
        external_calls += 1
        return _snapshot()

    monkeypatch.setattr(
        "rquant.reference_slow_source.capture_reference_slow_source_snapshot",
        forbidden,
    )
    restarted_store = SourceQuotaStore(
        settings.quota_path,
        boot_id="boot-new",
        monotonic_ns=lambda: 1,
    )
    restarted_observer = QuotaBoundTransportObserver(
        store=restarted_store,
        source=settings.source,
        quota_units_per_window=settings.quota_units_per_window,
        window_kind="minute",
        clock=lambda: OBSERVED_AT + timedelta(seconds=10),
    )
    with pytest.raises(SourceQuotaConflictError, match="unknown"):
        runtime_service_builtin._capture_reference_with_quota(
            settings=settings,
            quota_store=restarted_store,
            adapter=object(),  # type: ignore[arg-type]
            calendar=calendar,
            target_trade_date=TARGET_DATE,
            observed_at=OBSERVED_AT + timedelta(seconds=10),
            completion_clock=lambda: OBSERVED_AT + timedelta(seconds=10),
            producer_commit=COMMIT,
            transport_observer=restarted_observer,
        )

    assert external_calls == 0


def test_reference_source_retention_cannot_archive_revision_history(
    tmp_path: Path,
) -> None:
    calendar = _calendar()

    with pytest.raises(ValueError, match="retention_hot_batches"):
        ReferenceSlowSourceSettings(
            database_path=(tmp_path / "daily.duckdb").resolve(),
            calendar_path=(tmp_path / "calendar.json").resolve(),
            calendar_expected_commit=calendar.producer_commit,
            calendar_content_sha256=calendar.content_sha256,
            spool_root=(tmp_path / "spool").resolve(),
            quota_path=(tmp_path / "quota.sqlite3").resolve(),
            quota_units_per_window=500,
            history_page_size=64,
            retention_hot_batches=32,
            producer_version="reference-source-v2",
        )


def _calendar() -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=date(2026, 7, 29),
        coverage_end=date(2026, 8, 3),
        open_dates=(date(2026, 7, 29), PRIOR_DATE, TARGET_DATE, date(2026, 8, 3)),
        generated_at=datetime(2026, 7, 28, tzinfo=UTC),
    )


def _snapshot(
    *,
    captured_at: datetime = OBSERVED_AT,
    target_trade_date: date = TARGET_DATE,
    prior_trade_date: date = PRIOR_DATE,
    name: str = "成长样本",
) -> ReferenceSlowSourceSnapshot:
    calendar = _calendar()
    return ReferenceSlowSourceSnapshot.create(
        target_trade_date=target_trade_date,
        captured_at=captured_at,
        producer_commit=COMMIT,
        source_snapshot_ids={
            "daily": "1" * 64,
            "security": hashlib.sha256(name.encode()).hexdigest(),
            "suspension": "3" * 64,
            "calendar": calendar.content_sha256,
        },
        daily_facts=(
            ReferenceDailyFact(
                ts_code="300001.SZ",
                trade_date=prior_trade_date,
                close_raw=20.0,
                prior_adj_factor=1.0,
                adj_factor=2.0,
            ),
        ),
        security_facts=(
            ReferenceSecurityFact(
                ts_code="300001.SZ",
                name=name,
                is_st=False,
                list_date=date(2020, 1, 2),
                market="创业板",
            ),
        ),
    )


def _exit_after_receipt_intent_fsync(
    spool_root: str,
    cursor_root: str,
    registry_path: str,
) -> None:
    spool = LiveBatchSpool(
        Path(spool_root),
        cursor_root=Path(cursor_root),
        source_read_only=True,
    )
    registry = ReferenceRegistry(Path(registry_path))
    before = datetime(2026, 7, 31, 1, 24, 59, tzinfo=UTC)
    late = datetime(2026, 7, 31, 1, 25, 0, 1000, tzinfo=UTC)
    crossed = False
    original_clear = spool._clear_completion_receipt_intent

    def phase_clock() -> datetime:
        # Deterministic phase clock: every checkpoint before the completion-intent
        # cleanup observes `before`, so no amount of fsync or CPU stall can expire
        # the publication early. Only `crossing_cleanup` moves it past 09:25.
        return late if crossed else before

    def exit_late_after_durable_unlink(publication_id: str) -> None:
        nonlocal crossed
        original_clear(publication_id)
        crossed = True
        os._exit(86)

    spool._clear_completion_receipt_intent = exit_late_after_durable_unlink  # type: ignore[method-assign]
    publish_reference_slow_batches(
        spool=spool,
        registry=registry,
        calendar=_calendar(),
        consumer_id="reference-publisher",
        observed_at=before,
        producer_commit=COMMIT,
        completion_clock=phase_clock,
    )
    os._exit(87)


def test_source_batch_is_once_per_trade_date_and_publisher_replays_idempotently(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "spool")
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    cursor_root = tmp_path / "publisher-state" / "cursors"
    calls = 0

    def load_snapshot() -> ReferenceSlowSourceSnapshot:
        nonlocal calls
        calls += 1
        return _snapshot()

    captured = capture_reference_slow_batch(
        spool=spool,
        calendar=_calendar(),
        observed_at=OBSERVED_AT,
        producer_commit=COMMIT,
        producer_version="test-v1",
        snapshot_loader=load_snapshot,
        completion_clock=lambda: OBSERVED_AT.replace(minute=24),
    )
    repeated = capture_reference_slow_batch(
        spool=spool,
        calendar=_calendar(),
        observed_at=OBSERVED_AT,
        producer_commit=COMMIT,
        producer_version="test-v1",
        snapshot_loader=load_snapshot,
        completion_clock=lambda: OBSERVED_AT.replace(minute=24),
    )

    assert captured.processed_count == 1
    assert repeated.processed_count == 0
    assert calls == 1
    current = spool.current(LiveChannel.REFERENCE_SLOW)
    assert current is not None
    assert current.sequence == 0

    def source_files() -> tuple[Path, ...]:
        return tuple(
            sorted(
                relative
                for path in spool.root.rglob("*")
                if path.is_file()
                and not (relative := path.relative_to(spool.root)).is_relative_to(
                    "serving-authority"
                )
            )
        )

    source_files_before_publish = source_files()

    consumer_spool = LiveBatchSpool(
        spool.root,
        cursor_root=cursor_root,
        source_read_only=True,
    )
    published = publish_reference_slow_batches(
        spool=consumer_spool,
        registry=registry,
        calendar=_calendar(),
        consumer_id="reference-publisher",
        observed_at=SAFE_PUBLISHED_AT,
        producer_commit=COMMIT,
        completion_clock=lambda: SAFE_PUBLISHED_AT,
    )
    replayed = publish_reference_slow_batches(
        spool=consumer_spool,
        registry=registry,
        calendar=_calendar(),
        consumer_id="reference-publisher",
        observed_at=SAFE_PUBLISHED_AT,
        producer_commit=COMMIT,
        completion_clock=lambda: SAFE_PUBLISHED_AT,
    )

    assert published.processed_count == 1
    assert published.input_sequence == 0
    assert replayed.processed_count == 0
    cursor = consumer_spool.load_cursor(
        "reference-publisher",
        LiveChannel.REFERENCE_SLOW,
    )
    assert cursor is not None
    assert cursor.last_sequence == 0
    assert source_files() == source_files_before_publish
    assert registry.current_manifest().row_count == 6
    authority = ServingSourceAuthorityReader(
        root=spool.root / "serving-authority",
        expected_producer_commit=COMMIT,
        expected_dataset_id=REFERENCE_SLOW_AUTHORITY_DATASET_ID,
        expected_payload_kind="reference_slow",
    )(PUBLISHED_AT)
    assert isinstance(authority.payload, ReferenceSlowPayload)
    assert authority.payload.reference_generation_id == registry.current_pointer().generation_id
    assert {item.table_name for item in authority.payload.projections} >= {
        "stock_basic",
        "daily_bar",
        "nl_screen_universe",
    }


def test_reference_publisher_reads_only_one_bounded_cursor_page(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer = LiveBatchSpool(tmp_path / "spool")
    capture_reference_slow_batch(
        spool=producer,
        calendar=_calendar(),
        observed_at=OBSERVED_AT,
        producer_commit=COMMIT,
        producer_version="test-v1",
        snapshot_loader=lambda: _snapshot(),
        completion_clock=lambda: OBSERVED_AT.replace(minute=24),
    )
    consumer = LiveBatchSpool(
        producer.root,
        cursor_root=tmp_path / "publisher-state" / "cursors",
        source_read_only=True,
    )
    original_list_after = consumer.list_after
    limits: list[int | None] = []

    def bounded_list_after(
        channel: LiveChannel,
        *,
        sequence: int,
        limit: int | None = None,
    ):
        limits.append(limit)
        return original_list_after(channel, sequence=sequence, limit=limit)

    monkeypatch.setattr(consumer, "list_after", bounded_list_after)

    result = publish_reference_slow_batches(
        spool=consumer,
        registry=ReferenceRegistry(tmp_path / "reference.sqlite3"),
        calendar=_calendar(),
        consumer_id="reference-publisher",
        observed_at=SAFE_PUBLISHED_AT,
        producer_commit=COMMIT,
        completion_clock=lambda: SAFE_PUBLISHED_AT,
        page_size=1,
    )

    assert result.processed_count == 1
    assert limits == [1]


def test_source_revision_scan_publishes_changed_same_day_content_once(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "spool")
    capture_reference_slow_batch(
        spool=spool,
        calendar=_calendar(),
        observed_at=OBSERVED_AT,
        producer_commit=COMMIT,
        producer_version="test-v1",
        snapshot_loader=lambda: _snapshot(),
        completion_clock=lambda: OBSERVED_AT.replace(minute=23),
    )
    scan_started = OBSERVED_AT.replace(minute=24)
    calls: list[date] = []

    def load_revision(target: date) -> ReferenceSlowSourceSnapshot:
        calls.append(target)
        return _snapshot(
            captured_at=scan_started + timedelta(seconds=5),
            name="成长样本修订",
        )

    revised = capture_reference_slow_batch(
        spool=spool,
        calendar=_calendar(),
        observed_at=scan_started,
        producer_commit=COMMIT,
        producer_version="test-v1",
        snapshot_loader=lambda: pytest.fail("initial loader must not run"),
        revision_snapshot_loader=load_revision,
        revision_lookback_sessions=1,
        completion_clock=lambda: scan_started + timedelta(seconds=10),
    )
    replay = capture_reference_slow_batch(
        spool=spool,
        calendar=_calendar(),
        observed_at=scan_started + timedelta(seconds=20),
        producer_commit=COMMIT,
        producer_version="test-v1",
        snapshot_loader=lambda: pytest.fail("initial loader must not run"),
        revision_snapshot_loader=load_revision,
        revision_lookback_sessions=1,
        completion_clock=lambda: scan_started + timedelta(seconds=25),
    )

    records = spool.list_after(LiveChannel.REFERENCE_SLOW, sequence=-1)
    assert revised.processed_count == 1
    assert replay.processed_count == 0
    assert calls == [TARGET_DATE]
    assert [record.envelope.revision for record in records] == [1, 2]


def test_source_revision_scan_cursor_advances_through_bounded_history(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "spool")
    capture_reference_slow_batch(
        spool=spool,
        calendar=_calendar(),
        observed_at=OBSERVED_AT,
        producer_commit=COMMIT,
        producer_version="test-v1",
        snapshot_loader=lambda: _snapshot(),
        completion_clock=lambda: OBSERVED_AT.replace(minute=23),
    )
    scan_started = OBSERVED_AT.replace(minute=24)
    calls: list[date] = []

    def load_revision(target: date) -> ReferenceSlowSourceSnapshot:
        calls.append(target)
        if target == TARGET_DATE:
            return _snapshot(captured_at=scan_started + timedelta(seconds=2))
        return _snapshot(
            captured_at=scan_started + timedelta(seconds=12),
            target_trade_date=PRIOR_DATE,
            prior_trade_date=date(2026, 7, 29),
            name="历史修订",
        )

    first = capture_reference_slow_batch(
        spool=spool,
        calendar=_calendar(),
        observed_at=scan_started,
        producer_commit=COMMIT,
        producer_version="test-v1",
        snapshot_loader=lambda: pytest.fail("initial loader must not run"),
        revision_snapshot_loader=load_revision,
        revision_lookback_sessions=2,
        completion_clock=lambda: scan_started + timedelta(seconds=5),
    )
    second = capture_reference_slow_batch(
        spool=spool,
        calendar=_calendar(),
        observed_at=scan_started + timedelta(seconds=10),
        producer_commit=COMMIT,
        producer_version="test-v1",
        snapshot_loader=lambda: pytest.fail("initial loader must not run"),
        revision_snapshot_loader=load_revision,
        revision_lookback_sessions=2,
        completion_clock=lambda: scan_started + timedelta(seconds=15),
    )

    assert first.processed_count == 0
    assert second.processed_count == 1
    assert calls == [TARGET_DATE, PRIOR_DATE]


def test_source_batch_distinguishes_evidence_completion_from_atomic_availability(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "spool")
    completed_at = OBSERVED_AT.replace(minute=24, second=30)
    snapshot = _snapshot(captured_at=completed_at)
    clock_values = iter(
        (
            completed_at.replace(second=43),
            completed_at.replace(second=44),
            completed_at.replace(second=45),
            completed_at.replace(second=46),
            completed_at.replace(second=47),
        )
    )

    result = capture_reference_slow_batch(
        spool=spool,
        calendar=_calendar(),
        observed_at=OBSERVED_AT,
        producer_commit=COMMIT,
        producer_version="test-v1",
        snapshot_loader=lambda: snapshot,
        completion_clock=lambda: next(clock_values),
    )

    assert result.processed_count == 1
    current = spool.current(LiveChannel.REFERENCE_SLOW)
    assert current is not None
    records = spool.list_after(LiveChannel.REFERENCE_SLOW, sequence=-1)
    assert records[0].envelope.source_time == completed_at
    assert records[0].envelope.available_at == completed_at.replace(second=48)
    assert records[0].envelope.received_at == records[0].envelope.available_at


def _registry_publication_counts(registry: ReferenceRegistry) -> tuple[int, int, int]:
    with closing(sqlite3.connect(registry.path)) as connection:
        return tuple(
            int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in (
                "reference_record",
                "reference_generation",
                "reference_current",
            )
        )


def _captured_consumer_spool(tmp_path: Path) -> tuple[LiveBatchSpool, Path]:
    producer = LiveBatchSpool(tmp_path / "spool")
    completed_at = OBSERVED_AT.replace(minute=24, second=30)
    capture_reference_slow_batch(
        spool=producer,
        calendar=_calendar(),
        observed_at=OBSERVED_AT,
        producer_commit=COMMIT,
        producer_version="test-v1",
        snapshot_loader=lambda: _snapshot(captured_at=completed_at),
        completion_clock=lambda: completed_at.replace(second=45),
    )
    cursor_root = tmp_path / "publisher-state" / "cursors"
    return (
        LiveBatchSpool(producer.root, cursor_root=cursor_root, source_read_only=True),
        cursor_root,
    )


def test_publisher_fails_closed_when_start_is_one_millisecond_after_cutoff(
    tmp_path: Path,
) -> None:
    spool, _cursor_root = _captured_consumer_spool(tmp_path)
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    late = datetime(2026, 7, 31, 1, 25, 0, 1000, tzinfo=UTC)

    with pytest.raises(ReferenceSlowRuntimeError, match="started after 09:25"):
        publish_reference_slow_batches(
            spool=spool,
            registry=registry,
            calendar=_calendar(),
            consumer_id="reference-publisher",
            observed_at=late,
            producer_commit=COMMIT,
            completion_clock=lambda: late,
        )

    assert _registry_publication_counts(registry) == (0, 0, 0)
    assert spool.load_cursor("reference-publisher", LiveChannel.REFERENCE_SLOW) is None
    with pytest.raises(ReferenceDataUnavailableError):
        registry.current_pointer()


def test_publisher_compensates_registry_current_and_cursor_when_commit_crosses_cutoff(
    tmp_path: Path,
) -> None:
    spool, _cursor_root = _captured_consumer_spool(tmp_path)
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    before = datetime(2026, 7, 31, 1, 24, 59, tzinfo=UTC)
    late = datetime(2026, 7, 31, 1, 25, 0, 1000, tzinfo=UTC)
    clock_values = iter((before, before, before, late))

    with pytest.raises(ReferenceSlowRuntimeError, match="completed after 09:25"):
        publish_reference_slow_batches(
            spool=spool,
            registry=registry,
            calendar=_calendar(),
            consumer_id="reference-publisher",
            observed_at=before,
            producer_commit=COMMIT,
            completion_clock=lambda: next(clock_values),
        )

    assert _registry_publication_counts(registry) == (0, 0, 0)
    assert spool.load_cursor("reference-publisher", LiveChannel.REFERENCE_SLOW) is None
    with pytest.raises(ReferenceDataUnavailableError):
        registry.current_pointer()


def test_publisher_compensates_registry_when_cursor_write_crosses_cutoff(
    tmp_path: Path,
) -> None:
    spool, _cursor_root = _captured_consumer_spool(tmp_path)
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    before = datetime(2026, 7, 31, 1, 24, 59, tzinfo=UTC)
    late = datetime(2026, 7, 31, 1, 25, 0, 1000, tzinfo=UTC)
    clock_values = iter((before, before, before, before, before, late))

    with pytest.raises(ReferenceSlowRuntimeError, match="completed after 09:25"):
        publish_reference_slow_batches(
            spool=spool,
            registry=registry,
            calendar=_calendar(),
            consumer_id="reference-publisher",
            observed_at=before,
            producer_commit=COMMIT,
            completion_clock=lambda: next(clock_values),
        )

    assert _registry_publication_counts(registry) == (0, 0, 0)
    assert spool.load_cursor("reference-publisher", LiveChannel.REFERENCE_SLOW) is None


def test_delayed_final_intent_cleanup_crossing_cutoff_rolls_back_before_lock_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool, _cursor_root = _captured_consumer_spool(tmp_path)
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    before = datetime(2026, 7, 31, 1, 24, 59, tzinfo=UTC)
    late = datetime(2026, 7, 31, 1, 25, 0, 1000, tzinfo=UTC)
    crossed = False
    cleared = False
    original_clear = spool._clear_completion_receipt_intent

    def phase_clock() -> datetime:
        # Deterministic phase clock: every checkpoint before the completion-intent
        # cleanup observes `before`, so no amount of fsync or CPU stall can expire
        # the publication early. Only `crossing_cleanup` moves it past 09:25.
        return late if crossed else before

    def crossing_cleanup(publication_id: str) -> None:
        nonlocal crossed, cleared
        original_clear(publication_id)
        cleared = True
        crossed = True

    monkeypatch.setattr(spool, "_clear_completion_receipt_intent", crossing_cleanup)

    with pytest.raises(ReferenceSlowRuntimeError, match="completed after 09:25") as excinfo:
        publish_reference_slow_batches(
            spool=spool,
            registry=registry,
            calendar=_calendar(),
            consumer_id="reference-publisher",
            observed_at=before,
            producer_commit=COMMIT,
            completion_clock=phase_clock,
        )

    assert cleared, "the deadline expired before the completion-intent cleanup was reached"
    cause = excinfo.value.__cause__
    assert isinstance(cause, LiveSpoolIntegrityError)
    assert str(cause) == "completion receipt completed after deadline"
    assert _registry_publication_counts(registry) == (0, 0, 0)
    assert spool.load_cursor("reference-publisher", LiveChannel.REFERENCE_SLOW) is None
    evidence_files = tuple(spool.completion_evidence_root.glob("*.json"))
    assert len(evidence_files) == 1
    evidence = strict_canonical_json_loads(evidence_files[0].read_bytes())
    assert isinstance(evidence, dict)
    assert evidence["outcome"] == "rolled_back_deadline"
    assert "secret" not in evidence_files[0].read_text(encoding="utf-8")
    authenticator = ReferencePublicationAuthenticator.from_environment()
    assert authenticator is not None
    authentication_mac = evidence["authentication_mac"]
    assert isinstance(authentication_mac, str)
    assert authenticator.verify(
        {
            "contract": "reference-publication-durable-evidence/v1",
            **{key: value for key, value in evidence.items() if key != "authentication_mac"},
        },
        authentication_mac,
    )


def test_real_exit_after_late_receipt_intent_unlink_recovers_by_rolling_back(
    tmp_path: Path,
) -> None:
    spool, cursor_root = _captured_consumer_spool(tmp_path)
    registry_path = tmp_path / "reference.sqlite3"
    ReferenceRegistry(registry_path)
    process = get_context("fork").Process(
        target=_exit_after_receipt_intent_fsync,
        args=(str(spool.root), str(cursor_root), str(registry_path)),
    )

    process.start()
    process.join(timeout=5.0)
    if process.is_alive():
        process.terminate()
        process.join(timeout=1.0)
        pytest.fail("deadline attack child did not exit")
    assert process.exitcode == 86

    reopened_registry = ReferenceRegistry(registry_path)
    reopened_spool = LiveBatchSpool(
        spool.root,
        cursor_root=cursor_root,
        source_read_only=True,
    )
    # The crash left a durable completion receipt behind but no durable evidence,
    # so recovery must refuse the receipt and roll the publication back.
    assert tuple(reopened_spool.completion_receipt_root.glob("*.json")) != ()
    assert tuple(reopened_spool.completion_evidence_root.glob("*.json")) == ()
    assert _registry_publication_counts(reopened_registry) == (0, 0, 0)
    assert reopened_spool.load_cursor("reference-publisher", LiveChannel.REFERENCE_SLOW) is None


def test_integer_monotonic_deadline_rolls_back_when_wall_clock_freezes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool, _cursor_root = _captured_consumer_spool(tmp_path)
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    before = datetime(2026, 7, 31, 1, 24, 59, tzinfo=UTC)
    monkeypatch.setattr("rquant.reference_slow_runtime.monotonic", lambda: 1.0)
    monkeypatch.setattr("rquant.live_spool.time.monotonic", lambda: 2.000001)

    with pytest.raises(ReferenceSlowRuntimeError, match="completed after 09:25"):
        publish_reference_slow_batches(
            spool=spool,
            registry=registry,
            calendar=_calendar(),
            consumer_id="reference-publisher",
            observed_at=before,
            producer_commit=COMMIT,
            completion_clock=lambda: before,
        )

    assert _registry_publication_counts(registry) == (0, 0, 0)
    assert spool.load_cursor("reference-publisher", LiveChannel.REFERENCE_SLOW) is None


def test_success_persists_authenticated_final_durable_completion_evidence(
    tmp_path: Path,
) -> None:
    spool, _cursor_root = _captured_consumer_spool(tmp_path)
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    before = datetime(2026, 7, 31, 1, 24, 59, tzinfo=UTC)

    result = publish_reference_slow_batches(
        spool=spool,
        registry=registry,
        calendar=_calendar(),
        consumer_id="reference-publisher",
        observed_at=before,
        producer_commit=COMMIT,
        completion_clock=lambda: before,
    )

    assert result.processed_count == 1
    evidence_files = tuple(spool.completion_evidence_root.glob("*.json"))
    assert len(evidence_files) == 1
    evidence = strict_canonical_json_loads(evidence_files[0].read_bytes())
    assert isinstance(evidence, dict)
    assert evidence["outcome"] == "committed"
    authenticator = ReferencePublicationAuthenticator.from_environment()
    assert authenticator is not None
    authentication_mac = evidence["authentication_mac"]
    assert isinstance(authentication_mac, str)
    assert authenticator.verify(
        {
            "contract": "reference-publication-durable-evidence/v1",
            **{key: value for key, value in evidence.items() if key != "authentication_mac"},
        },
        authentication_mac,
    )


def test_registry_reader_cannot_observe_generation_before_cursor_finalizes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool, _cursor_root = _captured_consumer_spool(tmp_path)
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    started_at = PUBLISHED_AT - timedelta(seconds=1)
    entered_cursor_finalize = Event()
    release_cursor_finalize = Event()
    original_complete = spool.complete_cursor_publication

    def block_cursor_finalize(consumer_id: str, channel: LiveChannel) -> None:
        entered_cursor_finalize.set()
        assert release_cursor_finalize.wait(timeout=2.0)
        original_complete(consumer_id, channel)

    monkeypatch.setattr(spool, "complete_cursor_publication", block_cursor_finalize)
    with ThreadPoolExecutor(max_workers=2) as executor:
        publisher = executor.submit(
            publish_reference_slow_batches,
            spool=spool,
            registry=registry,
            calendar=_calendar(),
            consumer_id="reference-publisher",
            observed_at=started_at,
            producer_commit=COMMIT,
            completion_clock=lambda: started_at,
        )
        assert entered_cursor_finalize.wait(timeout=2.0)
        reader = executor.submit(registry.current_pointer)
        time.sleep(0.05)
        assert not reader.done()
        release_cursor_finalize.set()
        assert publisher.result(timeout=2.0).processed_count == 1
        assert reader.result(timeout=2.0).generation_id == registry.current_pointer().generation_id


def test_publisher_exact_cutoff_uses_monotonic_deadline_when_wall_clock_freezes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool, _cursor_root = _captured_consumer_spool(tmp_path)
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    original_clear = spool._clear_completion_receipt_intent

    def delayed_cleanup(publication_id: str) -> None:
        original_clear(publication_id)
        time.sleep(0.15)

    monkeypatch.setattr(spool, "_clear_completion_receipt_intent", delayed_cleanup)

    with pytest.raises(ReferenceSlowRuntimeError, match="completed after 09:25"):
        publish_reference_slow_batches(
            spool=spool,
            registry=registry,
            calendar=_calendar(),
            consumer_id="reference-publisher",
            observed_at=PUBLISHED_AT,
            producer_commit=COMMIT,
            completion_clock=lambda: PUBLISHED_AT,
        )

    assert _registry_publication_counts(registry) == (0, 0, 0)
    assert spool.load_cursor("reference-publisher", LiveChannel.REFERENCE_SLOW) is None


def test_source_exact_cutoff_uses_monotonic_deadline_when_wall_clock_freezes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = LiveBatchSpool(tmp_path / "spool")
    original_clear = spool._clear_publication_intent

    def delayed_cleanup(channel: LiveChannel) -> None:
        original_clear(channel)
        time.sleep(0.15)

    monkeypatch.setattr(spool, "_clear_publication_intent", delayed_cleanup)

    with pytest.raises(ReferenceSlowRuntimeError, match="atomic publication"):
        capture_reference_slow_batch(
            spool=spool,
            calendar=_calendar(),
            observed_at=PUBLISHED_AT,
            producer_commit=COMMIT,
            producer_version="test-v1",
            snapshot_loader=lambda: _snapshot(captured_at=PUBLISHED_AT),
            completion_clock=lambda: PUBLISHED_AT,
        )

    assert spool.current(LiveChannel.REFERENCE_SLOW) is None


def test_restart_rolls_back_system_exit_after_registry_commit(tmp_path: Path) -> None:
    spool, cursor_root = _captured_consumer_spool(tmp_path)
    registry_path = tmp_path / "reference.sqlite3"
    registry = ReferenceRegistry(registry_path)
    calls = 0

    def exit_after_registry_commit() -> datetime:
        nonlocal calls
        calls += 1
        if calls < 4:
            return SAFE_PUBLISHED_AT
        raise SystemExit("injected exit after registry commit")

    with pytest.raises(SystemExit, match="injected exit"):
        publish_reference_slow_batches(
            spool=spool,
            registry=registry,
            calendar=_calendar(),
            consumer_id="reference-publisher",
            observed_at=SAFE_PUBLISHED_AT,
            producer_commit=COMMIT,
            completion_clock=exit_after_registry_commit,
        )

    reopened_registry = ReferenceRegistry(registry_path)
    reopened_spool = LiveBatchSpool(
        spool.root,
        cursor_root=cursor_root,
        source_read_only=True,
    )
    assert _registry_publication_counts(reopened_registry) == (0, 0, 0)
    assert reopened_spool.load_cursor("reference-publisher", LiveChannel.REFERENCE_SLOW) is None


def test_restart_rolls_back_system_exit_after_cursor_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool, cursor_root = _captured_consumer_spool(tmp_path)
    registry_path = tmp_path / "reference.sqlite3"
    registry = ReferenceRegistry(registry_path)
    cursor_path = spool._cursor_path("reference-publisher", LiveChannel.REFERENCE_SLOW)
    original_atomic_write = LiveBatchSpool._atomic_write

    def exit_after_cursor_replace(path: Path, payload: bytes) -> None:
        original_atomic_write(path, payload)
        if path == cursor_path:
            raise SystemExit("injected exit after cursor replace")

    monkeypatch.setattr(
        LiveBatchSpool,
        "_atomic_write",
        staticmethod(exit_after_cursor_replace),
    )
    with pytest.raises(SystemExit, match="injected exit"):
        publish_reference_slow_batches(
            spool=spool,
            registry=registry,
            calendar=_calendar(),
            consumer_id="reference-publisher",
            observed_at=SAFE_PUBLISHED_AT,
            producer_commit=COMMIT,
            completion_clock=lambda: SAFE_PUBLISHED_AT,
        )
    monkeypatch.setattr(LiveBatchSpool, "_atomic_write", staticmethod(original_atomic_write))

    reopened_registry = ReferenceRegistry(registry_path)
    reopened_spool = LiveBatchSpool(
        spool.root,
        cursor_root=cursor_root,
        source_read_only=True,
    )
    assert _registry_publication_counts(reopened_registry) == (0, 0, 0)
    assert reopened_spool.load_cursor("reference-publisher", LiveChannel.REFERENCE_SLOW) is None


def test_restart_preserves_registry_and_cursor_after_receipt_cleanup_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool, cursor_root = _captured_consumer_spool(tmp_path)
    registry_path = tmp_path / "reference.sqlite3"
    registry = ReferenceRegistry(registry_path)

    def exit_between_intent_cleanup(
        consumer_id: str,
        channel: LiveChannel,
    ) -> None:
        del consumer_id, channel
        raise SystemExit("injected exit between intent cleanup")

    monkeypatch.setattr(
        spool,
        "complete_cursor_publication",
        exit_between_intent_cleanup,
    )

    with pytest.raises(SystemExit, match="between intent cleanup"):
        publish_reference_slow_batches(
            spool=spool,
            registry=registry,
            calendar=_calendar(),
            consumer_id="reference-publisher",
            observed_at=SAFE_PUBLISHED_AT,
            producer_commit=COMMIT,
            completion_clock=lambda: SAFE_PUBLISHED_AT,
        )

    reopened_registry = ReferenceRegistry(registry_path)
    reopened_spool = LiveBatchSpool(
        spool.root,
        cursor_root=cursor_root,
        source_read_only=True,
    )
    assert _registry_publication_counts(reopened_registry) == (6, 1, 1)
    cursor = reopened_spool.load_cursor(
        "reference-publisher",
        LiveChannel.REFERENCE_SLOW,
    )
    assert cursor is not None
    assert cursor.last_sequence == 0


def test_source_batch_fails_closed_when_atomic_publication_finishes_after_cutoff(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "spool")
    completed_at = OBSERVED_AT.replace(minute=24, second=30)
    late = datetime(2026, 7, 31, 1, 25, 0, 1000, tzinfo=UTC)
    clock_values = iter((OBSERVED_AT.replace(minute=24, second=59), late))

    with pytest.raises(ReferenceSlowRuntimeError, match="atomic publication"):
        capture_reference_slow_batch(
            spool=spool,
            calendar=_calendar(),
            observed_at=OBSERVED_AT,
            producer_commit=COMMIT,
            producer_version="test-v1",
            snapshot_loader=lambda: _snapshot(captured_at=completed_at),
            completion_clock=lambda: next(clock_values),
        )

    assert spool.current(LiveChannel.REFERENCE_SLOW) is None
    assert tuple(spool._channel_dir(LiveChannel.REFERENCE_SLOW).iterdir()) == ()
    assert not spool._intent_path(LiveChannel.REFERENCE_SLOW).exists()


def test_builtin_source_and_publisher_have_separate_writers_and_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar_path = tmp_path / "calendar.json"
    calendar = _calendar()
    calendar_path.write_bytes(canonical_json_bytes(calendar.model_dump(mode="json")))
    calendar_path.chmod(0o600)
    spool_root = (tmp_path / "spool").resolve()
    registry_path = (tmp_path / "authority" / "reference.sqlite3").resolve()
    cursor_root = (tmp_path / "publisher-state" / "cursors").resolve()
    source = RuntimeServiceManifest(
        service_id="source.reference-slow",
        service_kind=RuntimeServiceKind.REFERENCE_SLOW_SOURCE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=30,
        stale_after_seconds=120,
        producer_commit=COMMIT,
        settings={
            "database_path": str((tmp_path / "operational.duckdb").resolve()),
            "calendar_path": str(calendar_path.resolve()),
            "calendar_expected_commit": calendar.producer_commit,
            "calendar_content_sha256": calendar.content_sha256,
            "spool_root": str(spool_root),
            "quota_path": str((spool_root / "quota.sqlite3").resolve()),
            "quota_units_per_window": 500,
            "quota_cost_per_capture": 6,
            "producer_version": "reference-source-v1",
        },
    )
    publisher = RuntimeServiceManifest(
        service_id="publisher.reference-slow",
        service_kind=RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=5,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings={
            "calendar_path": str(calendar_path.resolve()),
            "calendar_expected_commit": calendar.producer_commit,
            "calendar_content_sha256": calendar.content_sha256,
            "spool_root": str(spool_root),
            "registry_path": str(registry_path),
            "cursor_root": str(cursor_root),
            "consumer_id": "reference-slow-publisher",
        },
    )
    monkeypatch.setattr(
        "rquant.reference_slow_source.capture_reference_slow_source_snapshot",
        lambda **_kwargs: _snapshot(),
    )
    monkeypatch.setattr("rquant.reference_slow_runtime._utc_now", lambda: SAFE_PUBLISHED_AT)
    clock_values = iter(
        (
            OBSERVED_AT,
            OBSERVED_AT.replace(minute=24),
            OBSERVED_AT.replace(minute=24, second=1),
            OBSERVED_AT.replace(minute=24, second=2),
            OBSERVED_AT.replace(minute=24, second=3),
            OBSERVED_AT.replace(minute=24, second=4),
            SAFE_PUBLISHED_AT,
            SAFE_PUBLISHED_AT,
            SAFE_PUBLISHED_AT,
            SAFE_PUBLISHED_AT,
            SAFE_PUBLISHED_AT,
            SAFE_PUBLISHED_AT,
            SAFE_PUBLISHED_AT,
            SAFE_PUBLISHED_AT,
            SAFE_PUBLISHED_AT,
            SAFE_PUBLISHED_AT,
        )
    )
    runtime = build_builtin_registry(
        reference_adapter_factory=lambda: object(),  # type: ignore[arg-type]
        adapter_factory=lambda: object(),  # type: ignore[arg-type]
        universe_loader=lambda: ("600000.SH",),
        clock=lambda: next(clock_values, SAFE_PUBLISHED_AT),
        runtime_capabilities=_runtime_capabilities(),
    )

    source_result = runtime.build(source)()

    assert source_result.processed_count == 1
    assert not registry_path.exists()

    publisher_result = runtime.build(publisher)()

    assert publisher_result.processed_count == 1
    assert registry_path.is_file()
    assert ReferenceRegistry(registry_path).current_manifest().row_count == 6


def test_production_source_builder_retires_only_committed_consumer_history(
    tmp_path: Path,
) -> None:
    calendar = _calendar()
    calendar_path = (tmp_path / "calendar.json").resolve()
    calendar_path.write_bytes(canonical_json_bytes(calendar.model_dump(mode="json")))
    calendar_path.chmod(0o600)
    spool_root = (tmp_path / "spool").resolve()
    cursor_root = (tmp_path / "publisher-state" / "cursors").resolve()
    producer = LiveBatchSpool(spool_root)
    envelopes: list[BatchEnvelope] = []
    for sequence in range(8):
        payload = f"reference-retention-{sequence}".encode()
        available_at = OBSERVED_AT + timedelta(seconds=10 + sequence)
        envelope = BatchEnvelope(
            schema_version=1,
            channel=LiveChannel.REFERENCE_SLOW,
            dataset_id="reference_slow_source",
            source="rquant.reference_slow_source",
            source_request_id=f"request-{sequence}",
            batch_id=f"batch-{sequence}",
            sequence=sequence,
            revision=1,
            event_time_start=OBSERVED_AT,
            event_time_end=OBSERVED_AT,
            source_time=OBSERVED_AT,
            received_at=available_at,
            available_at=available_at,
            row_count=1,
            content_sha256=hashlib.sha256(payload).hexdigest(),
            quality_status=BatchQualityStatus.PUBLISHED,
            producer_version="reference-source-v1",
            producer_commit=COMMIT,
        )
        producer.publish(
            envelope,
            payload,
            completion_clock=lambda available_at=available_at: available_at,
            not_after=OBSERVED_AT + timedelta(minutes=5),
        )
        envelopes.append(envelope)
    descriptor = producer.source_descriptor(LiveChannel.REFERENCE_SLOW)
    cursor_parent = cursor_root.parent
    cursor_parent.mkdir(mode=0o700, parents=True)
    manifest = RuntimeServiceManifest(
        service_id="source.reference-slow",
        service_kind=RuntimeServiceKind.REFERENCE_SLOW_SOURCE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=30,
        stale_after_seconds=120,
        producer_commit=COMMIT,
        settings={
            "database_path": str((tmp_path / "operational.duckdb").resolve()),
            "calendar_path": str(calendar_path),
            "calendar_expected_commit": calendar.producer_commit,
            "calendar_content_sha256": calendar.content_sha256,
            "spool_root": str(spool_root),
            "quota_path": str((spool_root / "quota.sqlite3").resolve()),
            "quota_units_per_window": 500,
            "history_page_size": 5,
            "consumer_cursor_root": str(cursor_root),
            "retention_consumer_id": "reference-slow-publisher",
            "retention_hot_batches": 5,
            "retention_page_size": 16,
            "producer_version": "reference-source-v1",
        },
    )
    after_capture_window = datetime(2026, 7, 31, 1, 26, tzinfo=UTC)

    cursor_parent.chmod(0o500)
    try:
        source_step = reference_slow_source_builder(
            adapter_factory=lambda: object(),  # type: ignore[arg-type]
            clock=lambda: after_capture_window,
            runtime_capabilities=_runtime_capabilities(),
        )(manifest)
        first_result = source_step()
    finally:
        cursor_parent.chmod(0o700)

    assert first_result.processed_count == 0
    assert not cursor_root.exists()
    assert not producer.reference_archive_paths(0)[0].exists()

    publisher = LiveBatchSpool(spool_root, cursor_root=cursor_root)
    publisher.commit_cursor(
        ConsumerCursor(
            consumer_id="reference-slow-publisher",
            channel=LiveChannel.REFERENCE_SLOW,
            source_generation_id=descriptor.generation_id,
            last_sequence=2,
            last_batch_id=envelopes[2].batch_id,
            last_content_sha256=envelopes[2].content_sha256,
            updated_at=OBSERVED_AT + timedelta(minutes=1),
        )
    )

    result = source_step()

    assert result.processed_count == 0
    assert all(path.exists() for path in producer.reference_archive_paths(2))
    assert producer._manifest_path(LiveChannel.REFERENCE_SLOW, 3).exists()
    assert producer.current(LiveChannel.REFERENCE_SLOW) is not None


def test_production_builder_discovers_bounded_revisions_with_pit_availability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar_path = tmp_path / "calendar.json"
    calendar = _calendar()
    calendar_path.write_bytes(canonical_json_bytes(calendar.model_dump(mode="json")))
    calendar_path.chmod(0o600)
    spool_root = (tmp_path / "spool").resolve()
    registry_path = (tmp_path / "authority" / "reference.sqlite3").resolve()
    cursor_root = (tmp_path / "publisher-state" / "cursors").resolve()
    source = RuntimeServiceManifest(
        service_id="source.reference-slow",
        service_kind=RuntimeServiceKind.REFERENCE_SLOW_SOURCE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=30,
        stale_after_seconds=120,
        producer_commit=COMMIT,
        settings={
            "database_path": str((tmp_path / "operational.duckdb").resolve()),
            "calendar_path": str(calendar_path.resolve()),
            "calendar_expected_commit": calendar.producer_commit,
            "calendar_content_sha256": calendar.content_sha256,
            "spool_root": str(spool_root),
            "quota_path": str((spool_root / "quota.sqlite3").resolve()),
            "quota_units_per_window": 500,
            "quota_cost_per_capture": 6,
            "revision_lookback_sessions": 2,
            "producer_version": "reference-source-v1",
        },
    )
    publisher = RuntimeServiceManifest(
        service_id="publisher.reference-slow",
        service_kind=RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=5,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings={
            "calendar_path": str(calendar_path.resolve()),
            "calendar_expected_commit": calendar.producer_commit,
            "calendar_content_sha256": calendar.content_sha256,
            "spool_root": str(spool_root),
            "registry_path": str(registry_path),
            "cursor_root": str(cursor_root),
            "consumer_id": "reference-slow-publisher",
        },
    )
    now = [OBSERVED_AT]
    captured_targets: list[date] = []
    today_capture_count = 0

    def capture_snapshot(**kwargs: object) -> ReferenceSlowSourceSnapshot:
        nonlocal today_capture_count
        target = kwargs["target_trade_date"]
        assert isinstance(target, date)
        captured_targets.append(target)
        if target == TARGET_DATE:
            today_capture_count += 1
            return _snapshot(
                captured_at=now[0],
                name="当日初始" if today_capture_count == 1 else "当日修订",
            )
        return _snapshot(
            captured_at=now[0],
            target_trade_date=PRIOR_DATE,
            prior_trade_date=date(2026, 7, 29),
            name="历史修订",
        )

    monkeypatch.setattr(
        "rquant.reference_slow_source.capture_reference_slow_source_snapshot",
        capture_snapshot,
    )
    runtime = build_builtin_registry(
        reference_adapter_factory=lambda: object(),  # type: ignore[arg-type]
        adapter_factory=lambda: object(),  # type: ignore[arg-type]
        universe_loader=lambda: ("300001.SZ",),
        clock=lambda: now[0],
        runtime_capabilities=_runtime_capabilities(),
    )
    source_step = runtime.build(source)

    initial = source_step()
    now[0] = OBSERVED_AT.replace(minute=24)
    same_day_revision = source_step()
    now[0] += timedelta(seconds=10)
    historical_revision = source_step()

    assert [initial.processed_count, same_day_revision.processed_count] == [1, 1]
    assert historical_revision.processed_count == 1
    assert captured_targets == [TARGET_DATE, TARGET_DATE, PRIOR_DATE]
    source_records = LiveBatchSpool(spool_root).list_after(
        LiveChannel.REFERENCE_SLOW,
        sequence=-1,
    )
    assert [record.envelope.revision for record in source_records] == [1, 2, 1]

    publisher_started = OBSERVED_AT.replace(minute=24, second=20)
    now[0] = publisher_started
    published = runtime.build(publisher)()

    assert published.processed_count == 3
    registry = ReferenceRegistry(registry_path)
    historical_effective_from = datetime(2026, 7, 29, 16, 0, tzinfo=UTC)
    historical_records = tuple(
        record
        for record in registry.records(
            dataset_id=ReferenceDataset.ST_STATUS,
            key="300001.SZ",
        )
        if record.effective_from == historical_effective_from
    )
    assert len(historical_records) == 1
    assert historical_records[0].payload["name"] == "历史修订"
    assert historical_records[0].first_available_at == publisher_started + timedelta(seconds=5)
