"""The 2026-09-24 reference-slow window: heartbeat positions (#298) and publication (#297).

The first production day with the capture fixed (#293, v0.33.20) the source sealed batch 0 at
09:21, the publisher then refused every attempt, and the source crashed once after 09:25:

* #298 -- the first source round after 09:25 returned the default `input_sequence=-1` after
  the in-window rounds had reported 0; `RuntimeServiceControl.record_success` raised
  `input sequence cannot regress`, the unit exited 1 and `OnFailure` pushed the owner.
* #297 -- the publisher promised visibility at `prepared_at + 5 s` and the registry refused a
  commit that ended later; under the live slice's CPU quota the ~33k-record commit does not
  fit in 5 s, and the refusal was reported as "completed after 09:25".

Every case below drives the real spool, registry, serving authority and service control.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import closing
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
import pytest

import rquant.reference_slow_runtime as reference_slow_runtime
from rquant.auction_gap_candidate_input import assemble_auction_gap_candidate_batch
from rquant.auction_match_gateway import AuctionMatchGateway, AuctionMatchGatewayConfig
from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.reference_data_registry import (
    ReadonlyReferenceRegistry,
    ReferenceDataset,
    ReferenceDataUnavailableError,
    ReferencePublicationDeadlineError,
    ReferencePublicationVisibilityError,
    ReferenceRecord,
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
from rquant.runtime_service_builtin import build_builtin_registry
from rquant.runtime_service_control import (
    RuntimeServiceControl,
    RuntimeServicePlane,
    RuntimeServiceSpec,
    RuntimeStepResult,
)
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityReader,
    ServingSourceAuthorityUnavailableError,
)
from rquant.runtime_serving_snapshot import REFERENCE_SLOW_AUTHORITY_DATASET_ID
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_reference_slow_runtime import (
    COMMIT,
    PRIOR_DATE,
    TARGET_DATE,
    _reference_publication_credential,  # noqa: F401 - autouse credential fixture
    _runtime_capabilities,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
NEXT_DATE = date(2026, 8, 3)
#: the five sessions before TARGET_DATE are what `auction_gap_candidate_input` reads volumes for
OPEN_DATES = (
    date(2026, 7, 24),
    date(2026, 7, 27),
    date(2026, 7, 28),
    date(2026, 7, 29),
    PRIOR_DATE,
    TARGET_DATE,
    NEXT_DATE,
    date(2026, 8, 4),
    date(2026, 8, 5),
    date(2026, 8, 6),
)


def at(hour: int, minute: int, second: int = 0, *, day: date = TARGET_DATE) -> datetime:
    """Asia/Shanghai wall time as UTC -- both roles read local wall time."""

    return datetime.combine(day, time(hour, minute, second), tzinfo=SHANGHAI).astimezone(UTC)


class _Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def _calendar() -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=OPEN_DATES[0],
        coverage_end=OPEN_DATES[-1],
        open_dates=OPEN_DATES,
        generated_at=datetime(2026, 7, 23, tzinfo=UTC),
    )


def _snapshot(
    *,
    captured_at: datetime,
    target_trade_date: date = TARGET_DATE,
    prior_trade_date: date = PRIOR_DATE,
    codes: tuple[str, ...] = ("300001.SZ",),
    producer_commit: str = COMMIT,
) -> ReferenceSlowSourceSnapshot:
    return ReferenceSlowSourceSnapshot.create(
        target_trade_date=target_trade_date,
        captured_at=captured_at,
        producer_commit=producer_commit,
        source_snapshot_ids={
            "daily": "1" * 64,
            "security": "2" * 64,
            "suspension": "3" * 64,
            "calendar": _calendar().content_sha256,
        },
        daily_facts=tuple(
            ReferenceDailyFact(
                ts_code=code,
                trade_date=prior_trade_date,
                close_raw=10.0,
                prior_adj_factor=1.0,
                adj_factor=1.0,
            )
            for code in codes
        ),
        security_facts=tuple(
            ReferenceSecurityFact(
                ts_code=code,
                name="成长样本",
                is_st=False,
                list_date=date(2020, 1, 2),
                market="创业板",
            )
            for code in codes
        ),
    )


def _control(root: Path, service_id: str, clock: _Clock) -> RuntimeServiceControl:
    return RuntimeServiceControl(
        root,
        spec=RuntimeServiceSpec(
            service_id=service_id,
            plane=RuntimeServicePlane.LIVE,
            stale_after=timedelta(minutes=3),
            producer_commit=COMMIT,
        ),
        clock=clock,
    )


def _manifests(tmp_path: Path) -> tuple[RuntimeServiceManifest, RuntimeServiceManifest, Path]:
    calendar = _calendar()
    calendar_path = (tmp_path / "calendar.json").resolve()
    calendar_path.write_bytes(canonical_json_bytes(calendar.model_dump(mode="json")))
    calendar_path.chmod(0o600)
    spool_root = (tmp_path / "live" / "reference-slow").resolve()
    registry_path = (tmp_path / "authorities" / "reference-slow" / "reference.sqlite3").resolve()
    cursor_root = (tmp_path / "control" / "reference-slow-publishers" / "cursors").resolve()
    source = RuntimeServiceManifest(
        service_id="reference-slow.source.v1",
        service_kind=RuntimeServiceKind.REFERENCE_SLOW_SOURCE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=30,
        stale_after_seconds=180,
        producer_commit=COMMIT,
        settings={
            "database_path": str((tmp_path / "rquant_ro.duckdb").resolve()),
            "calendar_path": str(calendar_path),
            "calendar_expected_commit": calendar.producer_commit,
            "calendar_content_sha256": calendar.content_sha256,
            "spool_root": str(spool_root),
            "quota_path": str(spool_root / "quota.sqlite3"),
            "quota_units_per_window": 500,
            "quota_cost_per_capture": 6,
            "revision_lookback_sessions": 1,
            "producer_version": "reference-slow-source-v1",
        },
    )
    publisher = RuntimeServiceManifest(
        service_id="reference-slow.publisher.v1",
        service_kind=RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=5,
        stale_after_seconds=180,
        producer_commit=COMMIT,
        settings={
            "calendar_path": str(calendar_path),
            "calendar_expected_commit": calendar.producer_commit,
            "calendar_content_sha256": calendar.content_sha256,
            "spool_root": str(spool_root),
            "registry_path": str(registry_path),
            "cursor_root": str(cursor_root),
            "consumer_id": "reference-slow-publisher",
        },
    )
    return source, publisher, registry_path


def _session_snapshot(captured_at: datetime) -> ReferenceSlowSourceSnapshot:
    """Whatever session the clock is on, captured the way the production source captures it."""

    day = captured_at.astimezone(SHANGHAI).date()
    if day == NEXT_DATE:
        return _snapshot(
            captured_at=captured_at, target_trade_date=NEXT_DATE, prior_trade_date=TARGET_DATE
        )
    return _snapshot(captured_at=captured_at, target_trade_date=day, prior_trade_date=PRIOR_DATE)


def test_source_and_publisher_heartbeats_never_regress_across_the_window_and_the_next_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#298: one real `RuntimeServiceControl` per role, walked through two sessions.

    09:19 (nothing yet) -> 09:21 (capture, batch 0) -> 09:24 -> 09:26 -> next session 09:19
    -> 09:21 (batch 1) -> 09:26. Before the fix the 09:26 source round returned -1 after 0
    and `record_success` raised `input sequence cannot regress` -- the 09-24 exit and push.
    """

    clock = _Clock(at(9, 19))
    monkeypatch.setattr(
        "rquant.reference_slow_source.capture_reference_slow_source_snapshot",
        lambda **kwargs: _session_snapshot(kwargs["captured_at"]),
    )
    source_manifest, publisher_manifest, _registry_path = _manifests(tmp_path)
    runtime = build_builtin_registry(
        reference_adapter_factory=lambda: object(),  # type: ignore[arg-type]
        adapter_factory=lambda: object(),  # type: ignore[arg-type]
        universe_loader=lambda: ("300001.SZ",),
        clock=clock,
        runtime_capabilities=_runtime_capabilities(),
    )
    source_step = runtime.build(source_manifest)
    publisher_step = runtime.build(publisher_manifest)
    source_control = _control(tmp_path / "control", "reference-slow.source.v1", clock)
    publisher_control = _control(tmp_path / "control", "reference-slow.publisher.v1", clock)
    source_control.start()
    publisher_control.start()

    source_positions: list[tuple[str, int, int]] = []
    publisher_positions: list[tuple[str, int, int]] = []
    publisher_refusals: list[str] = []

    def source_round(moment: datetime) -> None:
        clock.now = moment
        result = source_step()
        heartbeat = source_control.record_success(result)
        source_positions.append(
            (
                moment.astimezone(SHANGHAI).strftime("%m-%d %H:%M:%S"),
                heartbeat.input_sequence,
                heartbeat.output_sequence,
            )
        )

    def publisher_round(moment: datetime) -> None:
        clock.now = moment
        try:
            result = publisher_step()
        except Exception as error:  # noqa: BLE001 - the loop records it the same way
            publisher_control.record_failure(error)
            publisher_refusals.append(str(error))
            return
        heartbeat = publisher_control.record_success(result)
        publisher_positions.append(
            (
                moment.astimezone(SHANGHAI).strftime("%m-%d %H:%M:%S"),
                heartbeat.input_sequence,
                heartbeat.output_sequence,
            )
        )

    source_round(at(9, 19))
    publisher_round(at(9, 19, 30))
    source_round(at(9, 21))
    publisher_round(at(9, 22))
    source_round(at(9, 24))
    publisher_round(at(9, 24, 30))
    source_round(at(9, 26))
    publisher_round(at(9, 26))
    source_round(at(15, 0))
    source_round(at(9, 19, day=NEXT_DATE))
    publisher_round(at(9, 19, 30, day=NEXT_DATE))
    source_round(at(9, 21, day=NEXT_DATE))
    publisher_round(at(9, 22, day=NEXT_DATE))
    source_round(at(9, 26, day=NEXT_DATE))

    assert source_positions == [
        ("07-31 09:19:00", -1, -1),
        ("07-31 09:21:00", 0, 0),
        ("07-31 09:24:00", 0, 0),
        ("07-31 09:26:00", 0, 0),
        ("07-31 15:00:00", 0, 0),
        ("08-03 09:19:00", 0, 0),
        ("08-03 09:21:00", 1, 1),
        ("08-03 09:26:00", 1, 1),
    ]
    assert publisher_positions == [
        ("07-31 09:19:30", -1, -1),
        ("07-31 09:22:00", 0, 0),
        ("07-31 09:24:30", 0, 0),
        ("08-03 09:19:30", 0, 0),
        ("08-03 09:22:00", 1, 1),
    ]
    assert publisher_refusals == ["reference slow publisher started after 09:25"]
    source_control.stop(reason="test complete")
    publisher_control.stop(reason="test complete")


def test_an_idle_capture_round_reports_the_spool_position_without_rehashing_payloads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The idle rounds run every 30 s all day; `current()` re-hashes every retained payload.

    Up to 128 retained batches of ~3 MB each would be read and hashed 2,800 times a day, so an
    idle round reads the committed pointer only. It also keeps `reference_slow` out of its
    generations, which the builder reads as "today's batch is sealed" (#293).
    """

    spool = LiveBatchSpool(tmp_path / "spool")
    capture_reference_slow_batch(
        spool=spool,
        calendar=_calendar(),
        observed_at=at(9, 20),
        producer_commit=COMMIT,
        producer_version="test-v1",
        snapshot_loader=lambda: _snapshot(captured_at=at(9, 20, 30)),
        completion_clock=lambda: at(9, 20, 40),
    )

    def refuse_rehash(**_kwargs: object) -> object:
        raise AssertionError("an idle round must not re-validate the retained batches")

    monkeypatch.setattr(spool, "_validate_immutable_prefix", refuse_rehash)
    calendar: MarketCalendarAuthority = _calendar()
    for moment in (at(9, 19), at(9, 26), at(9, 0, day=date(2026, 8, 1))):
        result = capture_reference_slow_batch(
            spool=spool,
            calendar=calendar,
            observed_at=moment,
            producer_commit=COMMIT,
            producer_version="test-v1",
            snapshot_loader=lambda: pytest.fail("an idle round must not capture"),
            completion_clock=lambda moment=moment: moment,
        )
        assert result == RuntimeStepResult(
            input_sequence=0,
            output_sequence=0,
            source_generations={"market_calendar": calendar.content_sha256},
        )


def test_an_idle_capture_round_on_an_empty_spool_still_reports_minus_one(tmp_path: Path) -> None:
    result = capture_reference_slow_batch(
        spool=LiveBatchSpool(tmp_path / "spool"),
        calendar=_calendar(),
        observed_at=at(9, 26),
        producer_commit=COMMIT,
        producer_version="test-v1",
        snapshot_loader=lambda: pytest.fail("an idle round must not capture"),
        completion_clock=lambda: at(9, 26),
    )

    assert (result.input_sequence, result.output_sequence) == (-1, -1)


def test_an_idle_round_with_a_pending_intent_recovers_before_it_reports(tmp_path: Path) -> None:
    """A half-committed pointer is never reported: the pending intent is recovered first."""

    spool = LiveBatchSpool(tmp_path / "spool")
    original_atomic_write = LiveBatchSpool._atomic_write
    crashed = False

    def crash_after_current(path: Path, payload: bytes) -> None:
        nonlocal crashed
        original_atomic_write(path, payload)
        if path.parent.name == "current" and not crashed:
            crashed = True
            raise OSError("injected crash after the current pointer")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(LiveBatchSpool, "_atomic_write", staticmethod(crash_after_current))
        with pytest.raises(OSError, match="injected crash"):
            capture_reference_slow_batch(
                spool=spool,
                calendar=_calendar(),
                observed_at=at(9, 20),
                producer_commit=COMMIT,
                producer_version="test-v1",
                snapshot_loader=lambda: _snapshot(captured_at=at(9, 20, 30)),
                completion_clock=lambda: at(9, 20, 40),
            )
    assert spool._intent_path(LiveChannel.REFERENCE_SLOW).exists()
    assert spool._current_path(LiveChannel.REFERENCE_SLOW).exists()

    restarted = LiveBatchSpool(tmp_path / "spool")
    assert restarted.current_sequence(LiveChannel.REFERENCE_SLOW) is None
    assert not restarted._intent_path(LiveChannel.REFERENCE_SLOW).exists()


# ---------------------------------------------------------------------------------------
# #297: the only publication deadline is 09:25
# ---------------------------------------------------------------------------------------


def _registry(tmp_path: Path) -> ReferenceRegistry:
    return ReferenceRegistry(tmp_path / "authorities" / "reference.sqlite3")


def _consumer(spool: LiveBatchSpool, tmp_path: Path) -> LiveBatchSpool:
    return LiveBatchSpool(
        spool.root,
        cursor_root=tmp_path / "publisher-state" / "cursors",
        source_read_only=True,
    )


def _sealed_spool(tmp_path: Path, *, sealed_at: datetime | None = None) -> LiveBatchSpool:
    """Today's batch, sealed the way 2026-09-24's was: captured 09:20:30, prepared 09:21:00."""

    prepared = sealed_at or at(9, 21)
    spool = LiveBatchSpool(tmp_path / "spool")
    capture_reference_slow_batch(
        spool=spool,
        calendar=_calendar(),
        observed_at=prepared - timedelta(seconds=60),
        producer_commit=COMMIT,
        producer_version="test-v1",
        snapshot_loader=lambda: _snapshot(captured_at=prepared - timedelta(seconds=30)),
        completion_clock=lambda: prepared,
    )
    return spool


def _slow_registry_commit(
    monkeypatch: pytest.MonkeyPatch,
    registry: ReferenceRegistry,
    clock: _Clock,
    seconds: float,
) -> None:
    """The registry's stage commit takes `seconds` of wall time (the host's throttled slice)."""

    original = registry.append_many_and_publish_before

    def slow(records: object, *, completion_clock: object, **kwargs: object) -> object:
        readings = 0

        def inside() -> datetime:
            nonlocal readings
            readings += 1
            if readings == 2:
                #: the reading after `COMMIT`: this is where the 09-24 commit ran long
                clock.now += timedelta(seconds=seconds)
            return completion_clock()  # type: ignore[operator]

        return original(records, completion_clock=inside, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(registry, "append_many_and_publish_before", slow)


def _authority(spool: LiveBatchSpool, as_of: datetime) -> object:
    return ServingSourceAuthorityReader(
        root=spool.root / "serving-authority",
        expected_producer_commit=COMMIT,
        expected_dataset_id=REFERENCE_SLOW_AUTHORITY_DATASET_ID,
        expected_payload_kind="reference_slow",
    )(as_of)


@pytest.mark.parametrize("commit_seconds", [6, 60])
def test_a_slow_registry_commit_before_the_cutoff_publishes_visible_at_0925(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    commit_seconds: int,
) -> None:
    """09-24 09:22:52 and ~09:24:24: the commit outran `prepared_at + 5 s`, not 09:25."""

    spool = _sealed_spool(tmp_path)
    registry = _registry(tmp_path)
    clock = _Clock(at(9, 22))
    _slow_registry_commit(monkeypatch, registry, clock, commit_seconds)

    result = publish_reference_slow_batches(
        spool=_consumer(spool, tmp_path),
        registry=registry,
        calendar=_calendar(),
        consumer_id="reference-slow-publisher",
        observed_at=clock.now,
        producer_commit=COMMIT,
        completion_clock=clock,
    )

    assert result.processed_count == 1
    assert clock.now == at(9, 22) + timedelta(seconds=commit_seconds)
    decision = at(9, 25)
    manifest = registry.current_manifest()
    pointer = registry.current_pointer()
    assert manifest.published_at == decision
    assert pointer.switched_at == decision
    for dataset in ReferenceDataset:
        (record,) = registry.records(dataset_id=dataset, key="300001.SZ")
        assert record.first_available_at == decision
    with closing(sqlite3.connect(registry.path)) as connection:
        ((completed_at, visible_at),) = connection.execute(
            "SELECT completed_at, visible_at FROM reference_publication_receipt"
        ).fetchall()
    assert datetime.fromisoformat(completed_at) <= datetime.fromisoformat(visible_at)
    assert datetime.fromisoformat(visible_at) == decision
    #: the serving authority says the same instant, and nobody sees today's generation early
    today = _authority(spool, decision)
    assert today.payload.reference_generation_id == manifest.generation_id
    assert today.published_at == decision
    with pytest.raises(ServingSourceAuthorityUnavailableError, match="not yet available"):
        _authority(spool, decision - timedelta(microseconds=1))
    with pytest.raises(ReferenceDataUnavailableError, match="not available at decision_time"):
        registry.as_of(
            dataset_id=ReferenceDataset.PRICE_LIMIT_REGIME,
            key="300001.SZ",
            event_time=decision,
            decision_time=decision - timedelta(microseconds=1),
        )


def test_a_commit_that_ends_after_0925_still_refuses_and_rolls_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = _sealed_spool(tmp_path)
    registry = _registry(tmp_path)
    consumer = _consumer(spool, tmp_path)
    clock = _Clock(at(9, 24, 30))
    _slow_registry_commit(monkeypatch, registry, clock, 40)

    with pytest.raises(ReferenceSlowRuntimeError) as refused:
        publish_reference_slow_batches(
            spool=consumer,
            registry=registry,
            calendar=_calendar(),
            consumer_id="reference-slow-publisher",
            observed_at=clock.now,
            producer_commit=COMMIT,
            completion_clock=clock,
        )

    assert str(refused.value) == "reference slow publisher completed after 09:25"
    assert consumer.load_cursor("reference-slow-publisher", LiveChannel.REFERENCE_SLOW) is None
    with pytest.raises(ReferenceDataUnavailableError, match="missing"):
        registry.current_pointer()


def test_a_start_after_0925_still_refuses(tmp_path: Path) -> None:
    spool = _sealed_spool(tmp_path)
    late = at(9, 25) + timedelta(milliseconds=1)

    with pytest.raises(ReferenceSlowRuntimeError, match="started after 09:25"):
        publish_reference_slow_batches(
            spool=_consumer(spool, tmp_path),
            registry=_registry(tmp_path),
            calendar=_calendar(),
            consumer_id="reference-slow-publisher",
            observed_at=late,
            producer_commit=COMMIT,
            completion_clock=lambda: late,
        )


def test_a_missed_visibility_instant_and_a_missed_cutoff_read_differently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The registry still refuses a commit that outruns the visibility it was handed.

    The publisher no longer hands it an instant before 09:25, but a caller that does must
    hear which bound it crossed: on 09-24 the guard miss was reported as "after 09:25".
    """

    registry = _registry(tmp_path)
    record = ReferenceRecord(
        dataset_id=ReferenceDataset.ST_STATUS,
        key="300001.SZ",
        effective_from=datetime(2026, 7, 30, 16, tzinfo=UTC),
        revision=1,
        source="test.reference",
        first_available_at=at(9, 22, 5),
        payload={"is_st": False, "name": "成长样本"},
    )
    ticks = iter((at(9, 22), at(9, 22, 6)))
    with pytest.raises(ReferencePublicationVisibilityError) as visibility:
        registry.append_many_and_publish_before(
            (record,),
            published_at=at(9, 22, 5),
            completion_clock=lambda: next(ticks),
            not_after=at(9, 25),
        )
    assert str(visibility.value) == "publication completed after its promised visibility instant"
    late = iter((at(9, 24, 59), at(9, 25, 1)))
    with pytest.raises(ReferencePublicationDeadlineError) as deadline:
        registry.append_many_and_publish_before(
            (record,),
            published_at=at(9, 22, 5),
            completion_clock=lambda: next(late),
            not_after=at(9, 25),
        )
    assert not isinstance(deadline.value, ReferencePublicationVisibilityError)
    assert str(deadline.value) == "publication completed after deadline"

    #: and the publisher's heartbeat carries two different texts for the two
    spool = _sealed_spool(tmp_path)
    for error, text in (
        (
            ReferencePublicationVisibilityError("guard"),
            "reference slow publisher commit ended after its promised visibility instant "
            "(before 09:25)",
        ),
        (
            ReferencePublicationDeadlineError("cutoff"),
            "reference slow publisher completed after 09:25",
        ),
    ):
        publisher_registry = ReferenceRegistry(tmp_path / f"{type(error).__name__}.sqlite3")

        def refuse(*_args: object, error: Exception = error, **_kwargs: object) -> object:
            raise error

        monkeypatch.setattr(publisher_registry, "append_many_and_publish_before", refuse)
        with pytest.raises(ReferenceSlowRuntimeError) as refused:
            publish_reference_slow_batches(
                spool=LiveBatchSpool(
                    spool.root,
                    cursor_root=tmp_path / type(error).__name__ / "cursors",
                    source_read_only=True,
                ),
                registry=publisher_registry,
                calendar=_calendar(),
                consumer_id="reference-slow-publisher",
                observed_at=at(9, 22),
                producer_commit=COMMIT,
                completion_clock=lambda: at(9, 22),
            )
        assert str(refused.value) == text


def test_rounds_between_the_commit_and_0925_recognise_the_authority_without_rebuilding_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Today's authority is visible from 09:25; the recovery branch must still recognise it.

    Read at `started`, a 09:23 round would see yesterday's authority, decide the authority
    lagged the registry, and rebuild today's result from the 3 MB payload every five seconds.
    """

    spool = _sealed_spool(tmp_path)
    registry = _registry(tmp_path)
    consumer = _consumer(spool, tmp_path)
    publish = publish_reference_slow_batches
    common = {
        "spool": consumer,
        "registry": registry,
        "calendar": _calendar(),
        "consumer_id": "reference-slow-publisher",
        "producer_commit": COMMIT,
    }
    first = publish(observed_at=at(9, 22), completion_clock=lambda: at(9, 22), **common)
    rebuilt: list[object] = []
    original_build = reference_slow_runtime.build_reference_slow_serving_result

    def counted_build(**kwargs: object) -> object:
        rebuilt.append(kwargs)
        return original_build(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        reference_slow_runtime, "build_reference_slow_serving_result", counted_build
    )

    later = [
        publish(observed_at=moment, completion_clock=lambda moment=moment: moment, **common)
        for moment in (at(9, 23, 30), at(9, 24, 50))
    ]

    assert rebuilt == []
    assert {item.source_generations["reference_slow_authority"] for item in later} == {
        first.source_generations["reference_slow_authority"]
    }
    assert [(item.input_sequence, item.processed_count) for item in later] == [(0, 0), (0, 0)]


def test_the_second_session_authority_is_not_refused_as_a_rollback(tmp_path: Path) -> None:
    """Record lineage revisions restart at 1 every session; the authority numbers generations.

    Before, the second session's authority reused sequence 1 and was refused ("different
    generation at the current sequence is a rollback"); only a later round's recovery branch
    published it, and never when that round started after 09:25.
    """

    spool = _sealed_spool(tmp_path)
    registry = _registry(tmp_path)
    consumer = _consumer(spool, tmp_path)
    common = {
        "spool": consumer,
        "registry": registry,
        "calendar": _calendar(),
        "consumer_id": "reference-slow-publisher",
        "producer_commit": COMMIT,
    }
    publish_reference_slow_batches(
        observed_at=at(9, 22), completion_clock=lambda: at(9, 22), **common
    )
    capture_reference_slow_batch(
        spool=spool,
        calendar=_calendar(),
        observed_at=at(9, 20, day=NEXT_DATE),
        producer_commit=COMMIT,
        producer_version="test-v1",
        snapshot_loader=lambda: _snapshot(
            captured_at=at(9, 20, 30, day=NEXT_DATE),
            target_trade_date=NEXT_DATE,
            prior_trade_date=TARGET_DATE,
        ),
        completion_clock=lambda: at(9, 21, day=NEXT_DATE),
    )

    second = publish_reference_slow_batches(
        observed_at=at(9, 22, day=NEXT_DATE),
        completion_clock=lambda: at(9, 22, day=NEXT_DATE),
        **common,
    )

    assert second.processed_count == 1
    authority = _authority(spool, at(9, 25, day=NEXT_DATE))
    assert authority.payload.reference_generation_id == registry.current_manifest().generation_id
    assert authority.sequence == 2
    assert _authority(spool, at(9, 30)).sequence == 1


def test_the_auction_gap_input_at_0929_accepts_a_generation_committed_slowly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """What 09-24 lost: `auction_gap_candidate_input` refused every row for want of it."""

    spool = _sealed_spool(tmp_path)
    registry = _registry(tmp_path)
    clock = _Clock(at(9, 22))
    _slow_registry_commit(monkeypatch, registry, clock, 60)
    publish_reference_slow_batches(
        spool=_consumer(spool, tmp_path),
        registry=registry,
        calendar=_calendar(),
        consumer_id="reference-slow-publisher",
        observed_at=clock.now,
        producer_commit=COMMIT,
        completion_clock=clock,
    )
    auction_spool = LiveBatchSpool(tmp_path / "auction-spool")
    frame = pd.DataFrame(
        [
            {
                "ts_code": "300001.SZ",
                "trade_date": TARGET_DATE,
                "price": 10.5,
                "vol": 20_000.0,
                "amount": 210_000.0,
                "pre_close": 10.0,
                "turnover_rate": 0.2,
                "volume_ratio": 9.9,
            }
        ]
    )
    capture = AuctionMatchGateway(
        spool=auction_spool,
        fetcher=lambda _trade_date: frame,
        config=AuctionMatchGatewayConfig(
            producer_version="auction-match-v1",
            producer_commit=COMMIT,
            min_coverage_ratio=1.0,
        ),
    ).capture_once(trade_date=TARGET_DATE, received_at=at(9, 29), expected_codes=("300001.SZ",))
    assert capture.published is True
    daily = tmp_path / "rquant_ro.duckdb"
    with duckdb.connect(str(daily)) as connection:
        connection.execute("CREATE TABLE daily_bar(ts_code VARCHAR, trade_date DATE, vol DOUBLE)")
        connection.executemany(
            "INSERT INTO daily_bar VALUES (?, ?, ?)",
            [("300001.SZ", day, 1_000.0) for day in OPEN_DATES[:5]],
        )
    daily.chmod(0o600)
    replica_time = at(8, 0).timestamp()
    os.utime(daily, (replica_time, replica_time))

    batch = assemble_auction_gap_candidate_batch(
        auction_spool=auction_spool,
        daily_database_path=daily,
        reference_registry=ReadonlyReferenceRegistry(registry.path),
        calendar=_calendar(),
        trade_date=TARGET_DATE,
        observed_at=at(9, 29, 30),
        producer_commit=COMMIT,
    )

    (fact,) = batch.facts
    assert fact.ts_code == "300001.SZ"
    assert fact.is_listed is True
    assert fact.limit_up_price_session_raw == 12.0
    assert fact.available_at == at(9, 29)
    assert batch.authority.captured_at == at(9, 29)


# ---------------------------------------------------------------------------------------
# #297, source side: the batch write gets 30 s, and a miss says which bound it crossed
# ---------------------------------------------------------------------------------------


def _capture_with_write_taking(
    spool: LiveBatchSpool,
    clock: _Clock,
    seconds: float,
    monkeypatch: pytest.MonkeyPatch,
) -> RuntimeStepResult:
    original = spool.publish

    def slow_publish(*args: object, **kwargs: object) -> object:
        clock.now += timedelta(seconds=seconds)
        return original(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(spool, "publish", slow_publish)
    prepared = clock.now
    return capture_reference_slow_batch(
        spool=spool,
        calendar=_calendar(),
        observed_at=prepared - timedelta(seconds=60),
        producer_commit=COMMIT,
        producer_version="test-v1",
        snapshot_loader=lambda: _snapshot(captured_at=prepared - timedelta(seconds=30)),
        completion_clock=clock,
    )


def test_a_source_batch_write_of_twenty_seconds_is_sealed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = LiveBatchSpool(tmp_path / "spool")
    clock = _Clock(at(9, 21))

    result = _capture_with_write_taking(spool, clock, 20, monkeypatch)

    assert result.processed_count == 1
    (record,) = spool.list_after(LiveChannel.REFERENCE_SLOW, sequence=-1)
    assert record.envelope.available_at == at(9, 21, 30)


def test_a_source_batch_write_past_its_guard_or_the_cutoff_reads_differently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    guard_spool = LiveBatchSpool(tmp_path / "guard")
    with pytest.raises(ReferenceSlowRuntimeError) as guard:
        _capture_with_write_taking(guard_spool, _Clock(at(9, 21)), 31, monkeypatch)
    assert str(guard.value) == (
        "reference slow atomic publication ended after its promised visibility instant "
        "(before 09:25)"
    )
    assert guard_spool.current(LiveChannel.REFERENCE_SLOW) is None

    cutoff_spool = LiveBatchSpool(tmp_path / "cutoff")
    with pytest.raises(ReferenceSlowRuntimeError) as cutoff:
        _capture_with_write_taking(cutoff_spool, _Clock(at(9, 24, 50)), 11, monkeypatch)
    assert str(cutoff.value) == "reference slow atomic publication completed after 09:25"
    assert cutoff_spool.current(LiveChannel.REFERENCE_SLOW) is None


# ---------------------------------------------------------------------------------------
# A batch whose session ended unpublished must not hold the cursor; a batch not yet visible
# ends the round instead of failing it
# ---------------------------------------------------------------------------------------


def _capture_session(
    spool: LiveBatchSpool,
    day: date = NEXT_DATE,
    *,
    producer_commit: str = COMMIT,
) -> None:
    prior = max(item for item in OPEN_DATES if item < day)
    capture_reference_slow_batch(
        spool=spool,
        calendar=_calendar(),
        observed_at=at(9, 20, day=day),
        producer_commit=producer_commit,
        producer_version="test-v2",
        snapshot_loader=lambda: _snapshot(
            captured_at=at(9, 20, 30, day=day),
            target_trade_date=day,
            prior_trade_date=prior,
            producer_commit=producer_commit,
        ),
        completion_clock=lambda: at(9, 21, day=day),
    )


def test_a_batch_whose_session_ended_unpublished_does_not_hold_the_next_session(
    tmp_path: Path,
) -> None:
    """2026-09-24's batch 0 was sealed and never published.

    On 09-25 the publisher lists it first; publishing it is refused ("source evidence must
    complete on its discovery session", and after tonight's release its producer_commit is
    refused before that), so every round would have raised on it and batch 1 would never
    have been published. It is passed over and named on the heartbeat instead.
    """

    spool = _sealed_spool(tmp_path)
    registry = _registry(tmp_path)
    consumer = _consumer(spool, tmp_path)
    _capture_session(spool)
    common = {
        "spool": consumer,
        "registry": registry,
        "calendar": _calendar(),
        "consumer_id": "reference-slow-publisher",
        "producer_commit": COMMIT,
    }

    result = publish_reference_slow_batches(
        observed_at=at(9, 22, day=NEXT_DATE),
        completion_clock=lambda: at(9, 22, day=NEXT_DATE),
        **common,
    )

    assert result.processed_count == 1
    assert (result.input_sequence, result.output_sequence) == (1, 1)
    assert result.degraded_reasons == ("expired_source_batch:0",)
    cursor = consumer.load_cursor("reference-slow-publisher", LiveChannel.REFERENCE_SLOW)
    assert cursor is not None and cursor.last_sequence == 1
    (record,) = registry.records(dataset_id=ReferenceDataset.ST_STATUS, key="300001.SZ")
    assert record.effective_from == datetime(2026, 8, 2, 16, tzinfo=UTC)
    #: and the round after that is clean: the cursor is past the expired batch
    after = publish_reference_slow_batches(
        observed_at=at(9, 23, day=NEXT_DATE),
        completion_clock=lambda: at(9, 23, day=NEXT_DATE),
        **common,
    )
    assert after.degraded_reasons == ()
    assert after.input_sequence == 1


def test_an_expired_batch_from_the_previous_release_is_passed_over_before_its_commit_check(
    tmp_path: Path,
) -> None:
    """Tonight's release changes `producer_commit`; 09-24's batch carries v0.33.20's."""

    spool = LiveBatchSpool(tmp_path / "spool")
    _capture_session(spool, TARGET_DATE, producer_commit="b" * 40)
    _capture_session(spool)

    result = publish_reference_slow_batches(
        spool=_consumer(spool, tmp_path),
        registry=_registry(tmp_path),
        calendar=_calendar(),
        consumer_id="reference-slow-publisher",
        observed_at=at(9, 22, day=NEXT_DATE),
        producer_commit=COMMIT,
        completion_clock=lambda: at(9, 22, day=NEXT_DATE),
    )

    assert result.processed_count == 1
    assert result.degraded_reasons == ("expired_source_batch:0",)


def test_more_expired_batches_than_one_page_cannot_stall_the_cursor(tmp_path: Path) -> None:
    spool = _sealed_spool(tmp_path)
    registry = _registry(tmp_path)
    consumer = _consumer(spool, tmp_path)
    common = {
        "spool": consumer,
        "registry": registry,
        "calendar": _calendar(),
        "consumer_id": "reference-slow-publisher",
        "producer_commit": COMMIT,
        "page_size": 1,
    }
    publish_reference_slow_batches(
        observed_at=at(9, 22), completion_clock=lambda: at(9, 22), **common
    )
    for day in (NEXT_DATE, date(2026, 8, 4), date(2026, 8, 5)):
        _capture_session(spool, day)

    result = publish_reference_slow_batches(
        observed_at=at(9, 22, day=date(2026, 8, 5)),
        completion_clock=lambda: at(9, 22, day=date(2026, 8, 5)),
        **common,
    )

    assert result.processed_count == 1
    assert (result.input_sequence, result.output_sequence) == (3, 3)
    assert result.degraded_reasons == ("expired_source_batch:1", "expired_source_batch:2")


def test_a_batch_not_yet_visible_ends_the_round_without_failing_it(tmp_path: Path) -> None:
    """09-24 09:21:26: the round that listed the batch before it was visible raised."""

    spool = _sealed_spool(tmp_path)
    registry = _registry(tmp_path)
    consumer = _consumer(spool, tmp_path)
    common = {
        "spool": consumer,
        "registry": registry,
        "calendar": _calendar(),
        "consumer_id": "reference-slow-publisher",
        "producer_commit": COMMIT,
    }
    publish_reference_slow_batches(
        observed_at=at(9, 22), completion_clock=lambda: at(9, 22), **common
    )
    _capture_session(spool)
    parsed: list[int] = []
    original_snapshot = reference_slow_runtime._record_snapshot

    def counted_snapshot(spool_arg: LiveBatchSpool, record: object) -> object:
        parsed.append(record.envelope.sequence)  # type: ignore[attr-defined]
        return original_snapshot(spool_arg, record)  # type: ignore[arg-type]

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(reference_slow_runtime, "_record_snapshot", counted_snapshot)
        early = publish_reference_slow_batches(
            observed_at=at(9, 21, 10, day=NEXT_DATE),
            completion_clock=lambda: at(9, 21, 10, day=NEXT_DATE),
            **common,
        )

    assert parsed == []
    assert (early.processed_count, early.input_sequence, early.degraded_reasons) == (0, 0, ())
    visible = publish_reference_slow_batches(
        observed_at=at(9, 21, 30, day=NEXT_DATE),
        completion_clock=lambda: at(9, 21, 30, day=NEXT_DATE),
        **common,
    )
    assert (visible.processed_count, visible.input_sequence) == (1, 1)


def test_a_published_batch_reaches_the_authority_when_the_next_one_is_not_yet_visible(
    tmp_path: Path,
) -> None:
    spool = _sealed_spool(tmp_path)
    capture_reference_slow_batch(
        spool=spool,
        calendar=_calendar(),
        observed_at=at(9, 24),
        producer_commit=COMMIT,
        producer_version="test-v1",
        snapshot_loader=lambda: pytest.fail("today's batch is already sealed"),
        revision_snapshot_loader=lambda _target: _snapshot(
            captured_at=at(9, 24, 5),
            codes=("300001.SZ", "600000.SH"),
        ),
        revision_lookback_sessions=1,
        completion_clock=lambda: at(9, 24, 10),
    )
    records = spool.list_after(LiveChannel.REFERENCE_SLOW, sequence=-1)
    assert [record.envelope.available_at for record in records] == [at(9, 21, 30), at(9, 24, 40)]
    registry = _registry(tmp_path)

    result = publish_reference_slow_batches(
        spool=_consumer(spool, tmp_path),
        registry=registry,
        calendar=_calendar(),
        consumer_id="reference-slow-publisher",
        observed_at=at(9, 24, 20),
        producer_commit=COMMIT,
        completion_clock=lambda: at(9, 24, 20),
    )

    assert (result.processed_count, result.input_sequence) == (1, 0)
    authority = _authority(spool, at(9, 25))
    assert authority.payload.reference_generation_id == registry.current_manifest().generation_id
