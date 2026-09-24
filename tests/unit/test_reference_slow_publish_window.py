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

from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.reference_slow_publisher import (
    ReferenceDailyFact,
    ReferenceSecurityFact,
    ReferenceSlowSourceSnapshot,
)
from rquant.reference_slow_runtime import (
    capture_reference_slow_batch,
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
) -> ReferenceSlowSourceSnapshot:
    return ReferenceSlowSourceSnapshot.create(
        target_trade_date=target_trade_date,
        captured_at=captured_at,
        producer_commit=COMMIT,
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
