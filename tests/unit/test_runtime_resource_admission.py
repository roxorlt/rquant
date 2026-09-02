from __future__ import annotations

import multiprocessing.reduction
import os
import sqlite3
import threading
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant.resource_admission import TradingSession

_RESERVATION_NOW = datetime(2026, 8, 3, 8, 0, tzinfo=UTC)


def _reservation_identity(worker_id: str, attempt_digit: int):
    from rquant.resource_admission import ResourceReservationIdentity

    return ResourceReservationIdentity(
        job_id=f"00000000-0000-0000-0000-{attempt_digit:012d}",
        run_id=f"{attempt_digit:x}" * 64,
        shard_id=f"10000000-0000-0000-0000-{attempt_digit:012d}",
        attempt_id=f"20000000-0000-0000-0000-{attempt_digit:012d}",
        claim_generation=1,
        scheduler_fencing_token=1,
        worker_id=worker_id,
    )


def _reservation_request(identity: object):
    from rquant.research_run_spec import ResourceClass
    from rquant.resource_admission import AdmissionRequest

    return AdmissionRequest(
        job_id=str(identity.job_id),
        resource_class=ResourceClass.STANDARD,
        expected_memory_bytes=2 * 1024**3,
        expected_disk_bytes=1,
        expected_quota_units=0,
        expected_duration_ms=1_000,
        source=None,
        preemptible=True,
        read_only=True,
        deadline=_RESERVATION_NOW + timedelta(hours=1),
    )


def _reservation_policy():
    from rquant.resource_admission import AdmissionPolicy

    return AdmissionPolicy(
        allow_live_session=True,
        max_live_shard_duration_ms=5_000,
        max_snapshot_age_seconds=5,
        max_live_backlog_age_seconds=10,
        max_live_p95_latency_seconds=5,
        min_available_memory_bytes=0,
        min_available_disk_bytes=0,
        max_io_pressure_pct=100,
        max_cpu_load_pct=100,
        max_expected_memory_bytes=8 * 1024**3,
        max_expected_disk_bytes=8 * 1024**3,
        max_expected_quota_units=0,
        retry_delay_seconds=1,
    )


def _reservation_snapshot(observed_at: datetime = _RESERVATION_NOW):
    from rquant.resource_admission import ResourceSnapshot

    return ResourceSnapshot(
        observed_at=observed_at,
        session=TradingSession.POST_MARKET,
        live_backlog_age_seconds=0,
        live_p95_latency_seconds=0,
        available_memory_bytes=3 * 1024**3,
        available_disk_bytes=20 * 1024**3,
        io_pressure_pct=0,
        cpu_load_pct=0,
        source_quota_remaining=0,
        live_healthy=True,
    )


def _compete_for_resource_reservation(
    database_path: str,
    worker_id: str,
    attempt_digit: int,
    barrier: object,
    outcomes: object,
) -> None:
    import traceback
    from pathlib import Path

    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    try:
        identity = _reservation_identity(worker_id, attempt_digit)
        store = SQLiteResourceReservationStore(
            Path(database_path),
            clock=lambda: _RESERVATION_NOW,
        )
        barrier.wait(timeout=5)
        result = store.reserve(
            identity=identity,
            request=_reservation_request(identity),
            policy=_reservation_policy(),
            snapshot_provider=_reservation_snapshot,
            lease_seconds=30,
        )
    except BaseException as exc:  # noqa: BLE001 - carried back to the parent
        # An exit code alone tells the parent nothing, and the child's
        # traceback never reaches JUnit; without this the capacity fence
        # assertions read as an unattributable flake.
        outcomes.put((worker_id, f"raised {type(exc).__name__}", False, traceback.format_exc()))
        raise
    outcomes.put((worker_id, result.decision.outcome.value, result.lease is not None, None))


def _reserve_resource_then_crash(database_path: str, marker_path: str) -> None:
    from pathlib import Path

    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    identity = _reservation_identity("worker-crashed", 3)
    result = SQLiteResourceReservationStore(
        Path(database_path),
        clock=lambda: _RESERVATION_NOW,
    ).reserve(
        identity=identity,
        request=_reservation_request(identity),
        policy=_reservation_policy(),
        snapshot_provider=_reservation_snapshot,
        lease_seconds=5,
    )
    assert result.lease is not None
    Path(marker_path).write_text(result.lease.lease_id, encoding="ascii")
    os._exit(17)


class _FixedProbe:
    def __init__(
        self,
        *,
        memory: int = 12 * 1024**3,
        disk: int = 120 * 1024**3,
        cpu: float = 21.5,
        io: float = 7.25,
    ) -> None:
        self.memory = memory
        self.disk = disk
        self.cpu = cpu
        self.io = io
        self.paths: list[Path] = []

    def available_memory_bytes(self) -> int:
        return self.memory

    def available_disk_bytes(self, path: Path) -> int:
        self.paths.append(path)
        return self.disk

    def cpu_load_pct(self) -> float:
        return self.cpu

    def io_pressure_pct(self) -> float:
        return self.io


class _FixedLiveSloProbe:
    def __init__(
        self,
        *,
        backlog: float = 1.25,
        p95: float = 0.4,
        healthy: bool = True,
    ) -> None:
        self.backlog = backlog
        self.p95 = p95
        self.healthy = healthy
        self.calls: list[datetime] = []

    def __call__(self, observed_at: datetime):
        from rquant.runtime_resource_admission import LiveSloEvidence

        self.calls.append(observed_at)
        return LiveSloEvidence(
            observed_at=observed_at - timedelta(seconds=1),
            live_backlog_age_seconds=self.backlog,
            live_p95_latency_seconds=self.p95,
            live_healthy=self.healthy,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("live_backlog_age_microseconds", True),
        ("live_backlog_age_microseconds", "1000"),
        ("live_backlog_age_microseconds", 1 << 63),
        ("live_p95_latency_microseconds", True),
        ("live_p95_latency_microseconds", "1000"),
        ("live_p95_latency_microseconds", 1 << 63),
    ],
)
def test_live_slo_evidence_rejects_noncanonical_or_unbounded_integer(
    field: str,
    value: object,
) -> None:
    from pydantic import ValidationError

    from rquant.runtime_resource_admission import LiveSloEvidence

    payload: dict[str, object] = {
        "observed_at": _RESERVATION_NOW,
        "live_backlog_age_microseconds": 0,
        "live_p95_latency_microseconds": 0,
        "live_healthy": True,
    }
    payload[field] = value

    with pytest.raises(ValidationError):
        LiveSloEvidence.model_validate(payload)


@pytest.mark.parametrize(
    ("observed_at", "expected"),
    [
        (datetime(2026, 8, 3, 1, 14, 59, tzinfo=UTC), TradingSession.CLOSED),
        (datetime(2026, 8, 3, 1, 15, tzinfo=UTC), TradingSession.PRE_MARKET),
        (datetime(2026, 8, 3, 1, 30, tzinfo=UTC), TradingSession.MORNING),
        (datetime(2026, 8, 3, 3, 30, tzinfo=UTC), TradingSession.LUNCH),
        (datetime(2026, 8, 3, 5, 0, tzinfo=UTC), TradingSession.AFTERNOON),
        (datetime(2026, 8, 3, 7, 10, tzinfo=UTC), TradingSession.POST_MARKET),
        (datetime(2026, 8, 1, 2, 0, tzinfo=UTC), TradingSession.CLOSED),
    ],
)
def test_trading_session_uses_explicit_shanghai_boundaries(
    observed_at: datetime,
    expected: TradingSession,
) -> None:
    from rquant.runtime_resource_admission import trading_session_at

    assert trading_session_at(observed_at) is expected


def test_snapshot_provider_reads_one_coherent_local_observation(tmp_path: Path) -> None:
    from rquant.runtime_resource_admission import LocalResourceSnapshotProvider

    observed_at = datetime(2026, 8, 3, 8, 0, tzinfo=UTC)
    probe = _FixedProbe()
    slo = _FixedLiveSloProbe()
    provider = LocalResourceSnapshotProvider(
        disk_path=tmp_path,
        clock=lambda: observed_at,
        probe=probe,
        live_slo_probe=slo,
        session_resolver=lambda _observed_at: TradingSession.POST_MARKET,
    )

    snapshot = provider()

    assert snapshot.observed_at == observed_at
    assert snapshot.session is TradingSession.POST_MARKET
    assert snapshot.available_memory_bytes == 12 * 1024**3
    assert snapshot.available_disk_bytes == 120 * 1024**3
    assert snapshot.cpu_load_pct == 21.5
    assert snapshot.io_pressure_pct == 7.25
    assert snapshot.live_healthy is True
    assert snapshot.live_slo_applicable is False
    assert snapshot.live_backlog_age_seconds == 0
    assert snapshot.live_p95_latency_seconds == 0
    assert snapshot.source_quota_remaining == 0
    assert probe.paths == [tmp_path.resolve()]
    assert slo.calls == []


def test_snapshot_provider_uses_real_live_slo_evidence_during_lunch(
    tmp_path: Path,
) -> None:
    from rquant.runtime_resource_admission import LocalResourceSnapshotProvider

    slo = _FixedLiveSloProbe(backlog=2.5, p95=0.8, healthy=True)
    observed_at = datetime(2026, 8, 3, 4, 0, tzinfo=UTC)
    provider = LocalResourceSnapshotProvider(
        disk_path=tmp_path,
        clock=lambda: observed_at,
        probe=_FixedProbe(),
        live_slo_probe=slo,
        session_resolver=lambda _observed_at: TradingSession.LUNCH,
    )

    snapshot = provider()

    assert snapshot.session is TradingSession.LUNCH
    assert snapshot.live_slo_applicable is True
    assert snapshot.live_healthy is True
    assert snapshot.live_backlog_age_seconds == 2.5
    assert snapshot.live_p95_latency_seconds == 0.8
    assert slo.calls == [observed_at]


def test_authoritative_calendar_closes_a_weekday_holiday() -> None:
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.runtime_resource_admission import RuntimeTradeCalendarSessionResolver

    authority = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit="a" * 40,
        coverage_start=date(2026, 8, 3),
        coverage_end=date(2026, 8, 4),
        open_dates=(date(2026, 8, 4),),
        generated_at=datetime(2026, 8, 2, 0, 0, tzinfo=UTC),
    )

    session = RuntimeTradeCalendarSessionResolver(authority)(datetime(2026, 8, 3, 2, 0, tzinfo=UTC))

    assert session is TradingSession.CLOSED


def test_closed_weekday_snapshot_does_not_read_live_authority(tmp_path: Path) -> None:
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.runtime_resource_admission import (
        LocalResourceSnapshotProvider,
        RuntimeTradeCalendarSessionResolver,
    )

    observed_at = datetime(2026, 8, 3, 2, 0, tzinfo=UTC)
    authority = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit="a" * 40,
        coverage_start=date(2026, 8, 3),
        coverage_end=date(2026, 8, 3),
        open_dates=(),
        generated_at=datetime(2026, 8, 2, 0, 0, tzinfo=UTC),
    )
    slo = _FixedLiveSloProbe()
    snapshot = LocalResourceSnapshotProvider(
        disk_path=tmp_path,
        clock=lambda: observed_at,
        probe=_FixedProbe(),
        live_slo_probe=slo,
        session_resolver=RuntimeTradeCalendarSessionResolver(authority),
    )()

    assert snapshot.session is TradingSession.CLOSED
    assert snapshot.live_slo_applicable is False
    assert slo.calls == []


@pytest.mark.parametrize(
    ("method", "message"),
    [
        ("available_memory_bytes", "available memory probe failed"),
        ("available_disk_bytes", "available disk probe failed"),
        ("cpu_load_pct", "CPU load probe failed"),
        ("io_pressure_pct", "I/O pressure probe failed"),
    ],
)
def test_snapshot_provider_fails_closed_when_any_probe_fails(
    tmp_path: Path,
    method: str,
    message: str,
) -> None:
    from rquant.runtime_resource_admission import (
        LocalResourceSnapshotProvider,
        RuntimeResourceAdmissionError,
    )

    probe = _FixedProbe()

    def fail(*_args: object) -> int:
        raise OSError("unavailable")

    setattr(probe, method, fail)
    provider = LocalResourceSnapshotProvider(
        disk_path=tmp_path,
        clock=lambda: datetime(2026, 8, 3, 8, 0, tzinfo=UTC),
        probe=probe,
        live_slo_probe=_FixedLiveSloProbe(),
        session_resolver=lambda _observed_at: TradingSession.POST_MARKET,
    )

    with pytest.raises(RuntimeResourceAdmissionError, match=message):
        provider()


@pytest.mark.parametrize(
    ("attribute", "value"),
    [
        ("memory", True),
        ("memory", "1024"),
        ("memory", 1 << 63),
        ("cpu", True),
        ("cpu", "21.5"),
        ("io", True),
        ("io", "7.25"),
    ],
)
def test_snapshot_provider_rejects_noncanonical_probe_values(
    tmp_path: Path,
    attribute: str,
    value: object,
) -> None:
    from rquant.runtime_resource_admission import (
        LocalResourceSnapshotProvider,
        RuntimeResourceAdmissionError,
    )

    probe = _FixedProbe()
    setattr(probe, attribute, value)
    provider = LocalResourceSnapshotProvider(
        disk_path=tmp_path,
        clock=lambda: datetime(2026, 8, 3, 8, 0, tzinfo=UTC),
        probe=probe,
        live_slo_probe=_FixedLiveSloProbe(),
        session_resolver=lambda _observed_at: TradingSession.POST_MARKET,
    )

    with pytest.raises(RuntimeResourceAdmissionError, match="validation"):
        provider()


def test_snapshot_provider_fails_closed_when_live_slo_probe_fails(tmp_path: Path) -> None:
    from rquant.runtime_resource_admission import (
        LocalResourceSnapshotProvider,
        RuntimeResourceAdmissionError,
    )

    def fail(_observed_at: datetime):
        raise OSError("authority unavailable")

    provider = LocalResourceSnapshotProvider(
        disk_path=tmp_path,
        clock=lambda: datetime(2026, 8, 3, 4, 0, tzinfo=UTC),
        probe=_FixedProbe(),
        live_slo_probe=fail,
        session_resolver=lambda _observed_at: TradingSession.LUNCH,
    )

    with pytest.raises(RuntimeResourceAdmissionError, match="live SLO probe failed"):
        provider()


def test_runtime_health_authority_reports_missing_p95_instead_of_fabricating_zero(
    tmp_path: Path,
) -> None:
    from rquant.runtime_contracts import canonical_sha256
    from rquant.runtime_resource_admission import (
        RuntimeHealthAuthorityLiveSloProbe,
        RuntimeResourceAdmissionError,
    )
    from rquant.runtime_service_control import (
        RuntimeServiceHealth,
        RuntimeServiceHeartbeat,
        RuntimeServicePlane,
        RuntimeServiceStatus,
    )
    from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
    from rquant.runtime_serving_snapshot import RuntimeHealthPayload, SourceReadResult
    from rquant.serving_contracts import FreshnessStatus

    now = datetime(2026, 8, 3, 4, 0, tzinfo=UTC)
    values: dict[str, object] = {
        "dataset_id": "runtime_health",
        "sequence": 1,
        "event_time": now - timedelta(seconds=2),
        "published_at": now - timedelta(seconds=1),
        "status": FreshnessStatus.FRESH,
        "reason": None,
        "payload": RuntimeHealthPayload(
            runtime_services=(
                RuntimeServiceHealth(
                    service_id="feature-live",
                    plane=RuntimeServicePlane.LIVE,
                    status=RuntimeServiceStatus.RUNNING,
                    stale=False,
                    observed_at=now - timedelta(seconds=1),
                    heartbeat=RuntimeServiceHeartbeat(
                        service_id="feature-live",
                        spec_fingerprint="b" * 64,
                        run_id="c" * 64,
                        generation=1,
                        status=RuntimeServiceStatus.RUNNING,
                        started_at=now - timedelta(minutes=2),
                        heartbeat_at=now - timedelta(seconds=1),
                        last_success_at=now - timedelta(seconds=2),
                    ),
                ),
            )
        ),
    }
    values["generation_id"] = canonical_sha256(values)
    root = tmp_path / "authority"
    commit = "a" * 40
    ServingSourceAuthorityPublisher(
        root=root,
        producer_commit=commit,
        dataset_id="runtime_health",
        payload_kind="runtime_health",
        clock=lambda: now - timedelta(seconds=1),
    ).publish(SourceReadResult.model_validate(values))

    probe = RuntimeHealthAuthorityLiveSloProbe(
        authority_root=root,
        expected_producer_commit=commit,
    )

    with pytest.raises(RuntimeResourceAdmissionError, match="p95"):
        probe(now)


def test_runtime_health_authority_aggregates_live_service_slo(tmp_path: Path) -> None:
    from rquant.runtime_contracts import canonical_sha256
    from rquant.runtime_resource_admission import RuntimeHealthAuthorityLiveSloProbe
    from rquant.runtime_service_control import (
        RuntimeServiceHealth,
        RuntimeServiceHeartbeat,
        RuntimeServicePlane,
        RuntimeServiceStatus,
    )
    from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
    from rquant.runtime_serving_snapshot import RuntimeHealthPayload, SourceReadResult
    from rquant.serving_contracts import FreshnessStatus

    now = datetime(2026, 8, 3, 4, 0, tzinfo=UTC)

    def health(service_id: str, *, success_age: int, durations: tuple[float, ...]):
        ordered = tuple(sorted(durations))
        rank = max(1, int(len(ordered) * 0.95 + 0.999999))
        heartbeat = RuntimeServiceHeartbeat(
            service_id=service_id,
            spec_fingerprint="b" * 64,
            run_id=("c" if service_id == "feature-live" else "d") * 64,
            generation=1,
            status=RuntimeServiceStatus.RUNNING,
            started_at=now - timedelta(minutes=2),
            heartbeat_at=now - timedelta(seconds=1),
            last_success_at=now - timedelta(seconds=success_age),
            recent_step_durations_seconds=durations,
            last_step_duration_seconds=durations[-1],
            p95_step_duration_seconds=ordered[rank - 1],
        )
        return RuntimeServiceHealth(
            service_id=service_id,
            plane=RuntimeServicePlane.LIVE,
            status=RuntimeServiceStatus.RUNNING,
            stale=False,
            observed_at=now - timedelta(seconds=1),
            heartbeat=heartbeat,
        )

    payload = RuntimeHealthPayload(
        runtime_services=(
            health("feature-live", success_age=5, durations=(0.2, 0.7)),
            health("router-live", success_age=3, durations=(0.1, 0.4)),
        )
    )
    values: dict[str, object] = {
        "dataset_id": "runtime_health",
        "sequence": 2,
        "event_time": now - timedelta(seconds=1),
        "published_at": now - timedelta(seconds=1),
        "status": FreshnessStatus.FRESH,
        "reason": None,
        "payload": payload,
    }
    values["generation_id"] = canonical_sha256(values)
    root = tmp_path / "authority"
    commit = "a" * 40
    ServingSourceAuthorityPublisher(
        root=root,
        producer_commit=commit,
        dataset_id="runtime_health",
        payload_kind="runtime_health",
        clock=lambda: now - timedelta(seconds=1),
    ).publish(SourceReadResult.model_validate(values))

    evidence = RuntimeHealthAuthorityLiveSloProbe(
        authority_root=root,
        expected_producer_commit=commit,
    )(now)

    assert evidence.live_backlog_age_seconds == 5
    assert evidence.live_p95_latency_seconds == 0.7
    assert evidence.live_healthy is True


def test_runtime_health_authority_accepts_equivalent_microsecond_slo_values(
    tmp_path: Path,
) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeHealthAuthorityLiveSloProbe,
        RuntimeHealthAuthorityWatermark,
    )
    from rquant.runtime_service_control import (
        RuntimeServiceHealth,
        RuntimeServiceHeartbeat,
        RuntimeServicePlane,
        RuntimeServiceStatus,
    )
    from rquant.runtime_serving_snapshot import RuntimeHealthPayload
    from rquant.serving_contracts import FreshnessStatus

    observed_at = datetime(2026, 8, 3, 4, 0, tzinfo=UTC)
    heartbeat = RuntimeServiceHeartbeat(
        service_id="feature-live",
        spec_fingerprint="b" * 64,
        run_id="c" * 64,
        generation=1,
        status=RuntimeServiceStatus.RUNNING,
        started_at=observed_at - timedelta(minutes=2),
        heartbeat_at=observed_at,
        last_success_at=observed_at - timedelta(microseconds=180),
        recent_step_durations_seconds=(0.2,),
        last_step_duration_seconds=0.2,
        p95_step_duration_seconds=0.2,
    )
    service = RuntimeServiceHealth(
        service_id="feature-live",
        plane=RuntimeServicePlane.LIVE,
        status=RuntimeServiceStatus.RUNNING,
        stale=False,
        observed_at=observed_at,
        heartbeat=heartbeat,
    )
    payload = RuntimeHealthPayload.model_construct(
        runtime_services=(service,),
        live_backlog_age_seconds=0.00017999999999999998,
        live_p95_latency_seconds=0.2,
        live_healthy=True,
    )
    probe = RuntimeHealthAuthorityLiveSloProbe(
        authority_root=tmp_path / "authority",
        expected_producer_commit="a" * 40,
    )

    class FixedReader:
        def __call__(self, _observed_at: datetime) -> SimpleNamespace:
            return SimpleNamespace(
                payload=payload,
                published_at=observed_at,
                status=FreshnessStatus.FRESH,
            )

        def export_watermark(self) -> RuntimeHealthAuthorityWatermark:
            return RuntimeHealthAuthorityWatermark(
                as_of=observed_at,
                pointer_published_at=observed_at,
                sequence=1,
                generation_id="d" * 64,
            )

    probe.reader = FixedReader()

    evidence = probe(observed_at)

    assert evidence.live_backlog_age_seconds == pytest.approx(0.00018)


def test_runtime_health_authority_rejects_observation_before_accepted_watermark(
    tmp_path: Path,
) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeHealthAuthorityLiveSloProbe,
        RuntimeHealthAuthorityWatermark,
        RuntimeResourceAdmissionError,
    )

    watermark_time = datetime(2026, 8, 3, 4, 0, tzinfo=UTC)
    probe = RuntimeHealthAuthorityLiveSloProbe(
        authority_root=tmp_path / "authority",
        expected_producer_commit="a" * 40,
        watermark=RuntimeHealthAuthorityWatermark(
            as_of=watermark_time,
            pointer_published_at=watermark_time,
            sequence=3,
            generation_id="b" * 64,
        ),
    )
    reader_called = False

    class ForbiddenReader:
        def __call__(self, _observed_at: datetime) -> object:
            nonlocal reader_called
            reader_called = True
            raise AssertionError("older observation reached authority reader")

    probe.reader = ForbiddenReader()

    with pytest.raises(RuntimeResourceAdmissionError, match="older.*watermark"):
        probe(watermark_time - timedelta(microseconds=1))

    assert reader_called is False


def test_resource_percentage_above_one_hundred_fails_closed() -> None:
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        _bounded_pct,
    )

    with pytest.raises(RuntimeResourceAdmissionError, match="percentage"):
        _bounded_pct(100.000001)


def test_linux_meminfo_parser_reads_mem_available_bytes() -> None:
    from rquant.runtime_resource_admission import _parse_linux_meminfo_available_bytes

    payload = "MemTotal:       16384000 kB\nMemAvailable:    1234567 kB\n"

    assert _parse_linux_meminfo_available_bytes(payload) == 1_234_567 * 1024


@pytest.mark.parametrize("value", ("0", "01", "-1", "9223372036854775808", "9" * 100))
def test_linux_meminfo_parser_rejects_invalid_or_unbounded_capacity(value: str) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        _parse_linux_meminfo_available_bytes,
    )

    with pytest.raises(RuntimeResourceAdmissionError, match="MemAvailable"):
        _parse_linux_meminfo_available_bytes(f"MemAvailable: {value} kB\n")


def test_linux_io_psi_parser_reads_some_avg10_percentage() -> None:
    from rquant.runtime_resource_admission import _parse_linux_io_pressure_pct

    payload = (
        "some avg10=7.25 avg60=4.00 avg300=1.00 total=1234\n"
        "full avg10=2.00 avg60=1.00 avg300=0.50 total=456\n"
    )

    assert _parse_linux_io_pressure_pct(payload) == 7.25


@pytest.mark.parametrize("value", ("-0.01", "nan", "inf", "100.0001", "9" * 100))
def test_linux_io_psi_parser_rejects_invalid_or_out_of_range_values(value: str) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        _parse_linux_io_pressure_pct,
    )

    with pytest.raises(RuntimeResourceAdmissionError, match="I/O PSI"):
        _parse_linux_io_pressure_pct(f"some avg10={value} avg60=0.00 avg300=0.00 total=0\n")


def test_darwin_vm_stat_parser_sums_available_pages() -> None:
    from rquant.runtime_resource_admission import _parse_darwin_vm_stat_available_bytes

    payload = """Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                               10.
Pages active:                             99.
Pages inactive:                           20.
Pages speculative:                         3.
"""

    assert _parse_darwin_vm_stat_available_bytes(payload) == 33 * 16_384


@pytest.mark.parametrize(
    "payload",
    (
        """Mach Virtual Memory Statistics: (page size of 0 bytes)
Pages free: 1.
Pages inactive: 1.
Pages speculative: 1.
""",
        """Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free: -1.
Pages inactive: 1.
Pages speculative: 1.
""",
        """Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free: 0.
Pages inactive: 0.
Pages speculative: 0.
""",
        "Mach Virtual Memory Statistics: (page size of "
        + "9" * 100
        + " bytes)\nPages free: 1.\nPages inactive: 1.\nPages speculative: 1.\n",
        """Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free: 9223372036854775808.
Pages inactive: 1.
Pages speculative: 1.
""",
        """Mach Virtual Memory Statistics: (page size of 1073741824 bytes)
Pages free: 9223372036854775808.
Pages inactive: 1.
Pages speculative: 1.
""",
    ),
)
def test_darwin_vm_stat_parser_rejects_invalid_or_unbounded_capacity(payload: str) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        _parse_darwin_vm_stat_available_bytes,
    )

    with pytest.raises(RuntimeResourceAdmissionError, match="vm_stat"):
        _parse_darwin_vm_stat_available_bytes(payload)


def test_system_resource_probe_platform_smoke(tmp_path: Path) -> None:
    from rquant.runtime_resource_admission import SystemResourceProbe

    probe = SystemResourceProbe()

    assert probe.available_memory_bytes() > 0
    assert probe.available_disk_bytes(tmp_path) > 0
    assert 0 <= probe.cpu_load_pct() <= 100
    assert 0 <= probe.io_pressure_pct() <= 100


@pytest.mark.parametrize(
    ("status", "stale"),
    [
        ("running", True),
        ("degraded", False),
    ],
)
def test_runtime_health_authority_marks_stale_or_non_running_live_service_unhealthy(
    tmp_path: Path,
    status: str,
    stale: bool,
) -> None:
    from rquant.runtime_contracts import canonical_sha256
    from rquant.runtime_resource_admission import RuntimeHealthAuthorityLiveSloProbe
    from rquant.runtime_service_control import (
        RuntimeServiceHealth,
        RuntimeServiceHeartbeat,
        RuntimeServicePlane,
        RuntimeServiceStatus,
    )
    from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
    from rquant.runtime_serving_snapshot import RuntimeHealthPayload, SourceReadResult
    from rquant.serving_contracts import FreshnessStatus

    now = datetime(2026, 8, 3, 4, 0, tzinfo=UTC)
    service_status = RuntimeServiceStatus(status)
    heartbeat = RuntimeServiceHeartbeat(
        service_id="feature-live",
        spec_fingerprint="b" * 64,
        run_id="c" * 64,
        generation=1,
        status=service_status,
        started_at=now - timedelta(minutes=2),
        heartbeat_at=now - timedelta(seconds=1),
        last_success_at=now - timedelta(seconds=2),
        recent_step_durations_seconds=(0.2,),
        last_step_duration_seconds=0.2,
        p95_step_duration_seconds=0.2,
    )
    payload = RuntimeHealthPayload(
        runtime_services=(
            RuntimeServiceHealth(
                service_id="feature-live",
                plane=RuntimeServicePlane.LIVE,
                status=service_status,
                stale=stale,
                observed_at=now - timedelta(seconds=1),
                heartbeat=heartbeat,
            ),
        )
    )
    values: dict[str, object] = {
        "dataset_id": "runtime_health",
        "sequence": 3,
        "event_time": now - timedelta(seconds=1),
        "published_at": now - timedelta(seconds=1),
        "status": FreshnessStatus.DEGRADED,
        "reason": "live service unhealthy",
        "payload": payload,
    }
    values["generation_id"] = canonical_sha256(values)
    root = tmp_path / "authority"
    commit = "a" * 40
    ServingSourceAuthorityPublisher(
        root=root,
        producer_commit=commit,
        dataset_id="runtime_health",
        payload_kind="runtime_health",
        clock=lambda: now - timedelta(seconds=1),
    ).publish(SourceReadResult.model_validate(values))

    evidence = RuntimeHealthAuthorityLiveSloProbe(
        authority_root=root,
        expected_producer_commit=commit,
    )(now)

    assert evidence.live_healthy is False


def test_runtime_health_authority_rejects_missing_last_success(tmp_path: Path) -> None:
    from rquant.runtime_contracts import canonical_sha256
    from rquant.runtime_resource_admission import (
        RuntimeHealthAuthorityLiveSloProbe,
        RuntimeResourceAdmissionError,
    )
    from rquant.runtime_service_control import (
        RuntimeServiceHealth,
        RuntimeServiceHeartbeat,
        RuntimeServicePlane,
        RuntimeServiceStatus,
    )
    from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
    from rquant.runtime_serving_snapshot import RuntimeHealthPayload, SourceReadResult
    from rquant.serving_contracts import FreshnessStatus

    now = datetime(2026, 8, 3, 4, 0, tzinfo=UTC)
    heartbeat = RuntimeServiceHeartbeat(
        service_id="feature-live",
        spec_fingerprint="b" * 64,
        run_id="c" * 64,
        generation=1,
        status=RuntimeServiceStatus.RUNNING,
        started_at=now - timedelta(minutes=2),
        heartbeat_at=now - timedelta(seconds=1),
        recent_step_durations_seconds=(0.2,),
        last_step_duration_seconds=0.2,
        p95_step_duration_seconds=0.2,
    )
    payload = RuntimeHealthPayload(
        runtime_services=(
            RuntimeServiceHealth(
                service_id="feature-live",
                plane=RuntimeServicePlane.LIVE,
                status=RuntimeServiceStatus.RUNNING,
                stale=False,
                observed_at=now - timedelta(seconds=1),
                heartbeat=heartbeat,
            ),
        )
    )
    values: dict[str, object] = {
        "dataset_id": "runtime_health",
        "sequence": 4,
        "event_time": now - timedelta(seconds=1),
        "published_at": now - timedelta(seconds=1),
        "status": FreshnessStatus.FRESH,
        "reason": None,
        "payload": payload,
    }
    values["generation_id"] = canonical_sha256(values)
    root = tmp_path / "authority"
    commit = "a" * 40
    ServingSourceAuthorityPublisher(
        root=root,
        producer_commit=commit,
        dataset_id="runtime_health",
        payload_kind="runtime_health",
        clock=lambda: now - timedelta(seconds=1),
    ).publish(SourceReadResult.model_validate(values))

    probe = RuntimeHealthAuthorityLiveSloProbe(
        authority_root=root,
        expected_producer_commit=commit,
    )

    with pytest.raises(RuntimeResourceAdmissionError, match="last_success"):
        probe(now)


def test_versioned_policy_blocks_live_research_and_fits_slice_cap() -> None:
    from rquant.runtime_resource_admission import (
        LAB_RESOURCE_POLICY_V1,
        admission_policy_for_version,
    )

    first = admission_policy_for_version(LAB_RESOURCE_POLICY_V1)
    second = admission_policy_for_version(LAB_RESOURCE_POLICY_V1)

    assert first == second
    assert first.allow_live_session is False
    assert first.min_available_memory_bytes >= 2 * 1024**3
    assert first.min_available_disk_bytes > 0
    assert first.max_expected_memory_bytes == 768 * 1024**2
    assert first.max_expected_disk_bytes >= 64 * 1024**3


def test_versioned_policy_defers_all_live_research() -> None:
    from rquant.research_run_spec import ResourceClass
    from rquant.resource_admission import (
        AdmissionOutcome,
        AdmissionRequest,
        ResourceSnapshot,
        evaluate_admission,
    )
    from rquant.runtime_resource_admission import (
        LAB_RESOURCE_POLICY_V1,
        admission_policy_for_version,
    )

    observed_at = datetime(2026, 8, 3, 4, 0, tzinfo=UTC)
    decision = evaluate_admission(
        AdmissionRequest(
            job_id="heavy-job",
            resource_class=ResourceClass.HEAVY,
            expected_memory_bytes=1,
            expected_disk_bytes=1,
            expected_quota_units=0,
            preemptible=False,
            read_only=True,
            deadline=observed_at + timedelta(hours=1),
        ),
        ResourceSnapshot(
            observed_at=observed_at,
            session=TradingSession.LUNCH,
            live_backlog_age_seconds=1,
            live_p95_latency_seconds=0.5,
            available_memory_bytes=100 * 1024**3,
            available_disk_bytes=100 * 1024**3,
            io_pressure_pct=1,
            cpu_load_pct=1,
            source_quota_remaining=0,
            live_healthy=True,
        ),
        admission_policy_for_version(LAB_RESOURCE_POLICY_V1),
    )

    assert decision.outcome is AdmissionOutcome.DEFERRED
    assert "live_session_blocked" in decision.reason_codes


@pytest.mark.parametrize("version", [None, "", "disabled", "lab-resource-v999"])
def test_runtime_bindings_fail_closed_without_known_policy(
    tmp_path: Path,
    version: str | None,
) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        build_runtime_resource_admission,
    )

    with pytest.raises(RuntimeResourceAdmissionError, match="policy version"):
        build_runtime_resource_admission(
            app_env="prod",
            disk_path=tmp_path,
            configured_policy_version=version,
            legacy_opt_out=False,
            probe=_FixedProbe(),
            live_slo_probe=_FixedLiveSloProbe(),
        )


def test_runtime_bindings_require_live_slo_probe(tmp_path: Path) -> None:
    from rquant.runtime_resource_admission import (
        LAB_RESOURCE_POLICY_V1,
        RuntimeResourceAdmissionError,
        build_runtime_resource_admission,
    )

    with pytest.raises(RuntimeResourceAdmissionError, match="live SLO"):
        build_runtime_resource_admission(
            app_env="prod",
            disk_path=tmp_path,
            configured_policy_version=LAB_RESOURCE_POLICY_V1,
            legacy_opt_out=False,
            probe=_FixedProbe(),
            live_slo_probe=None,
        )


def test_runtime_bindings_enable_required_admission(tmp_path: Path) -> None:
    from rquant.runtime_resource_admission import (
        LAB_RESOURCE_POLICY_V1,
        StaticAdmissionPolicyProvider,
        build_runtime_resource_admission,
    )

    bindings = build_runtime_resource_admission(
        app_env="prod",
        disk_path=tmp_path,
        configured_policy_version=LAB_RESOURCE_POLICY_V1,
        legacy_opt_out=False,
        probe=_FixedProbe(),
        live_slo_probe=_FixedLiveSloProbe(),
        session_resolver=lambda observed_at: TradingSession.POST_MARKET,
        clock=lambda: datetime(2026, 8, 3, 8, 0, tzinfo=UTC),
    )

    assert bindings.require_resource_admission is True
    assert bindings.resource_snapshot_provider is not None
    assert isinstance(bindings.admission_policy_provider, StaticAdmissionPolicyProvider)
    multiprocessing.reduction.ForkingPickler.dumps(bindings.admission_policy_provider)
    assert bindings.admission_policy_provider(object()).allow_live_session is False


def test_real_authority_provider_builds_a_spawn_serializable_probe_payload(
    tmp_path: Path,
) -> None:
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.runtime_resource_admission import (
        LocalResourceSnapshotProvider,
        RuntimeHealthAuthorityLiveSloProbe,
        RuntimeTradeCalendarSessionResolver,
    )

    authority = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit="a" * 40,
        coverage_start=date(2026, 8, 3),
        coverage_end=date(2026, 8, 3),
        open_dates=(date(2026, 8, 3),),
        generated_at=datetime(2026, 8, 2, tzinfo=UTC),
    )
    provider = LocalResourceSnapshotProvider(
        disk_path=tmp_path,
        probe=_FixedProbe(),
        live_slo_probe=RuntimeHealthAuthorityLiveSloProbe(
            authority_root=tmp_path / "authority",
            expected_producer_commit="a" * 40,
        ),
        session_resolver=RuntimeTradeCalendarSessionResolver(authority),
    )

    payload = provider.spawn_probe_provider()

    multiprocessing.reduction.ForkingPickler.dumps(payload)


def test_spawned_authority_watermark_survives_across_parent_probe_calls(
    tmp_path: Path,
) -> None:
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.runtime_resource_admission import (
        LocalResourceSnapshotProvider,
        RuntimeHealthAuthorityLiveSloProbe,
        RuntimeHealthAuthorityWatermark,
        RuntimeResourceAdmissionError,
        RuntimeTradeCalendarSessionResolver,
    )

    observed_at = datetime(2026, 8, 3, 4, 0, tzinfo=UTC)
    authority = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit="a" * 40,
        coverage_start=observed_at.date(),
        coverage_end=observed_at.date(),
        open_dates=(observed_at.date(),),
        generated_at=observed_at - timedelta(days=1),
    )
    provider = LocalResourceSnapshotProvider(
        disk_path=tmp_path,
        probe=_FixedProbe(),
        live_slo_probe=RuntimeHealthAuthorityLiveSloProbe(
            authority_root=tmp_path / "authority",
            expected_producer_commit="a" * 40,
        ),
        session_resolver=RuntimeTradeCalendarSessionResolver(authority),
    )
    accepted = RuntimeHealthAuthorityWatermark(
        as_of=observed_at,
        pointer_published_at=observed_at - timedelta(seconds=1),
        sequence=2,
        generation_id="b" * 64,
    )
    provider.accept_probe_state(accepted)
    provider.accept_probe_state(
        RuntimeHealthAuthorityWatermark(
            as_of=observed_at - timedelta(seconds=1),
            pointer_published_at=observed_at - timedelta(seconds=2),
            sequence=1,
            generation_id="c" * 64,
        )
    )

    next_payload = provider.spawn_probe_provider()

    assert next_payload.authority_watermark == accepted
    with pytest.raises(RuntimeResourceAdmissionError, match="sequence rollback"):
        provider.accept_probe_state(
            RuntimeHealthAuthorityWatermark(
                as_of=observed_at + timedelta(seconds=1),
                pointer_published_at=observed_at,
                sequence=1,
                generation_id="c" * 64,
            )
        )


def test_runtime_bindings_require_authoritative_session_resolver(tmp_path: Path) -> None:
    from rquant.runtime_resource_admission import (
        LAB_RESOURCE_POLICY_V1,
        RuntimeResourceAdmissionError,
        build_runtime_resource_admission,
    )

    with pytest.raises(RuntimeResourceAdmissionError, match="trade calendar"):
        build_runtime_resource_admission(
            app_env="prod",
            disk_path=tmp_path,
            configured_policy_version=LAB_RESOURCE_POLICY_V1,
            legacy_opt_out=False,
            probe=_FixedProbe(),
            live_slo_probe=_FixedLiveSloProbe(),
            session_resolver=None,
        )


def test_explicit_legacy_opt_out_is_dev_only(tmp_path: Path) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        build_runtime_resource_admission,
    )

    disabled = build_runtime_resource_admission(
        app_env="dev",
        disk_path=tmp_path,
        configured_policy_version=None,
        legacy_opt_out=True,
        probe=_FixedProbe(),
        live_slo_probe=None,
    )
    assert disabled.require_resource_admission is False
    assert disabled.resource_snapshot_provider is None
    assert disabled.admission_policy_provider is None

    with pytest.raises(RuntimeResourceAdmissionError, match="production"):
        build_runtime_resource_admission(
            app_env="prod",
            disk_path=tmp_path,
            configured_policy_version=None,
            legacy_opt_out=True,
            probe=_FixedProbe(),
            live_slo_probe=None,
        )


def test_cross_process_resource_reservation_admits_exactly_one_last_capacity_contender(
    tmp_path: Path,
) -> None:
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    outcomes = context.Queue()
    database_path = str(tmp_path / "resource-reservations.sqlite3")
    processes = tuple(
        context.Process(
            target=_compete_for_resource_reservation,
            args=(database_path, f"worker-{index}", index, barrier, outcomes),
        )
        for index in (1, 2)
    )

    started_processes: list[object] = []
    try:
        for process in processes:
            process.start()
            started_processes.append(process)
        for process in processes:
            process.join(10)

        results = sorted(outcomes.get(timeout=5) for _ in processes)
        # The contender's own traceback first, so a refused contender never
        # reaches the fence assertions as a bare `[1, 0] == [0, 0]`.
        assert not [result[3] for result in results if result[3] is not None], "\n".join(
            result[3] for result in results if result[3] is not None
        )
        assert [process.exitcode for process in processes] == [0, 0], results
        assert sorted(result[1] for result in results) == ["admitted", "deferred"]
        assert sum(result[2] for result in results) == 1
    finally:
        for process in started_processes:
            if process.is_alive():
                process.terminate()
        for process in started_processes:
            process.join(1)
        for process in started_processes:
            if process.is_alive():
                process.kill()
        for process in started_processes:
            process.join(1)
        outcomes.close()
        outcomes.join_thread()


def test_resource_reservation_store_opens_behind_an_exclusive_writer(
    tmp_path: Path,
) -> None:
    """Opening the store must wait out a concurrent writer, not die on it.

    The connection preamble reads the schema (`PRAGMA synchronous` needs it),
    and the only thing guarding that read is the 5ms poll `busy_timeout` the
    store uses so its own lock waits stay cancellable.  Two CI processes that
    opened the same database while one of them was committing the schema both
    died with `database is locked`.
    """
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    database_path = tmp_path / "resource-reservations.sqlite3"
    SQLiteResourceReservationStore(database_path, clock=lambda: _RESERVATION_NOW)

    blocker = sqlite3.connect(database_path, isolation_level=None, check_same_thread=False)
    released = threading.Event()
    try:
        blocker.execute("PRAGMA busy_timeout = 0")
        blocker.execute("BEGIN EXCLUSIVE")

        def release_the_writer() -> None:
            # Far longer than the 5ms the preamble used to be limited to, and
            # far inside the one-second bound initialisation now waits under.
            time.sleep(0.2)
            blocker.execute("COMMIT")
            released.set()

        releaser = threading.Thread(target=release_the_writer)
        releaser.start()
        try:
            store = SQLiteResourceReservationStore(
                database_path,
                clock=lambda: _RESERVATION_NOW,
            )
        finally:
            releaser.join(30)
        assert not releaser.is_alive()
        assert released.is_set()
        assert store.path == database_path.resolve()
    finally:
        blocker.close()


def test_abandoned_resource_reservation_expires_and_is_recovered(
    tmp_path: Path,
) -> None:
    from rquant.resource_admission import AdmissionOutcome
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    database_path = tmp_path / "resource-reservations.sqlite3"
    marker_path = tmp_path / "crashed-lease.txt"
    crashed = multiprocessing.get_context("spawn").Process(
        target=_reserve_resource_then_crash,
        args=(str(database_path), str(marker_path)),
    )
    crashed.start()
    crashed.join(10)

    assert crashed.exitcode == 17
    assert len(marker_path.read_text(encoding="ascii")) == 64
    before_expiry = SQLiteResourceReservationStore(
        database_path,
        clock=lambda: _RESERVATION_NOW + timedelta(seconds=4),
    )
    assert len(before_expiry.active_leases()) == 1

    recovered_at = _RESERVATION_NOW + timedelta(seconds=6)
    replacement = _reservation_identity("worker-replacement", 4)
    store = SQLiteResourceReservationStore(database_path, clock=lambda: recovered_at)
    second = store.reserve(
        identity=replacement,
        request=_reservation_request(replacement),
        policy=_reservation_policy(),
        snapshot_provider=lambda: _reservation_snapshot(recovered_at),
        lease_seconds=5,
    )

    assert second.decision.outcome is AdmissionOutcome.ADMITTED
    assert tuple(lease.identity for lease in store.active_leases()) == (replacement,)


def test_resource_reservation_release_requires_exact_attempt_and_worker_identity(
    tmp_path: Path,
) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        SQLiteResourceReservationStore,
    )

    identity = _reservation_identity("worker-owner", 5)
    store = SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: _RESERVATION_NOW,
    )
    result = store.reserve(
        identity=identity,
        request=_reservation_request(identity),
        policy=_reservation_policy(),
        snapshot_provider=_reservation_snapshot,
        lease_seconds=30,
    )
    assert result.lease is not None

    with pytest.raises(RuntimeResourceAdmissionError, match="identity"):
        store.release(
            result.lease,
            identity=_reservation_identity("worker-other", 6),
        )

    assert store.active_leases() == (result.lease,)
    assert store.release(result.lease, identity=identity) is True
    assert store.active_leases() == ()


def test_resource_reservation_recheck_atomically_renews_exact_active_lease(
    tmp_path: Path,
) -> None:
    from rquant.resource_admission import AdmissionOutcome
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    current = [_RESERVATION_NOW]
    identity = _reservation_identity("worker-owner", 7)
    store = SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: current[0],
    )
    first = store.reserve(
        identity=identity,
        request=_reservation_request(identity),
        policy=_reservation_policy(),
        snapshot_provider=lambda: _reservation_snapshot(current[0]),
        lease_seconds=5,
    )
    assert first.lease is not None
    current[0] += timedelta(seconds=4)

    renewed = store.recheck(
        lease=first.lease,
        identity=identity,
        request=_reservation_request(identity),
        policy=_reservation_policy(),
        snapshot_provider=lambda: _reservation_snapshot(current[0]),
        lease_seconds=5,
    )

    assert renewed.decision.outcome is AdmissionOutcome.ADMITTED
    assert renewed.lease is not None
    assert renewed.lease.lease_id == first.lease.lease_id
    assert renewed.lease.expires_at == current[0] + timedelta(seconds=5)
    current[0] += timedelta(seconds=2)
    assert store.active_leases() == (renewed.lease,)


def _create_unattested_resource_reservation_schema(path: Path) -> str:
    table_sql = """
        CREATE TABLE resource_reservation (
            lease_id TEXT PRIMARY KEY,
            job_id TEXT,
            run_id TEXT,
            shard_id TEXT,
            attempt_id TEXT,
            claim_generation INTEGER,
            scheduler_fencing_token INTEGER,
            worker_id TEXT,
            request_hash TEXT,
            expected_memory_bytes INTEGER,
            expected_disk_bytes INTEGER,
            expected_quota_units INTEGER,
            granted_at TEXT,
            expires_at TEXT
        )
    """
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA application_id = 1381065281")
        connection.execute("PRAGMA user_version = 1")
        connection.execute(table_sql)
        connection.execute(
            """
            CREATE TRIGGER resource_reservation_zero_memory
            AFTER INSERT ON resource_reservation
            BEGIN
                UPDATE resource_reservation
                SET expected_memory_bytes = 0
                WHERE lease_id = NEW.lease_id;
            END
            """
        )
    return table_sql


def test_resource_store_creates_exact_versioned_strict_schema(tmp_path: Path) -> None:
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    database_path = tmp_path / "resource-reservations.sqlite3"
    SQLiteResourceReservationStore(database_path, clock=lambda: _RESERVATION_NOW)

    with sqlite3.connect(database_path) as connection:
        application_id = connection.execute("PRAGMA application_id").fetchone()[0]
        user_version = connection.execute("PRAGMA user_version").fetchone()[0]
        table = connection.execute(
            "SELECT type, ncol, wr, strict FROM pragma_table_list WHERE name = ?",
            ("resource_reservation",),
        ).fetchone()
        foreign_keys = connection.execute(
            "SELECT * FROM pragma_foreign_key_list(?)",
            ("resource_reservation",),
        ).fetchall()
        objects = connection.execute(
            """
            SELECT type, name FROM sqlite_schema
            WHERE name NOT LIKE 'sqlite_%'
            ORDER BY type, name
            """
        ).fetchall()

    assert application_id == 1_381_065_281
    assert user_version == 2
    assert table == ("table", 15, 1, 1)
    assert foreign_keys == []
    assert objects == [
        ("index", "resource_reservation_expiry_v2_idx"),
        ("table", "resource_reservation"),
        ("table", "resource_reservation_authority"),
    ]


def test_resource_store_rejects_faked_version_with_loose_schema_without_rebuilding(
    tmp_path: Path,
) -> None:
    from rquant.resource_admission import AdmissionOutcome
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        SQLiteResourceReservationStore,
    )

    database_path = tmp_path / "resource-reservations.sqlite3"
    original_sql = _create_unattested_resource_reservation_schema(database_path)

    with pytest.raises(RuntimeResourceAdmissionError, match="schema"):
        store = SQLiteResourceReservationStore(database_path, clock=lambda: _RESERVATION_NOW)
        first_identity = _reservation_identity("worker-first", 8)
        second_identity = _reservation_identity("worker-second", 9)
        first = store.reserve(
            identity=first_identity,
            request=_reservation_request(first_identity),
            policy=_reservation_policy(),
            snapshot_provider=_reservation_snapshot,
            lease_seconds=30,
        )
        second = store.reserve(
            identity=second_identity,
            request=_reservation_request(second_identity),
            policy=_reservation_policy(),
            snapshot_provider=_reservation_snapshot,
            lease_seconds=30,
        )
        assert first.decision.outcome is AdmissionOutcome.ADMITTED
        assert second.decision.outcome is AdmissionOutcome.ADMITTED

    with sqlite3.connect(database_path) as connection:
        persisted_sql = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?",
            ("resource_reservation",),
        ).fetchone()[0]
        trigger_count = connection.execute(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'trigger'"
        ).fetchone()[0]
    assert " ".join(persisted_sql.split()) == " ".join(original_sql.split())
    assert trigger_count == 1


@pytest.mark.parametrize(
    "ddl",
    (
        "CREATE TABLE resource_reservation_shadow (lease_id TEXT)",
        "CREATE VIEW resource_reservation_view AS SELECT * FROM resource_reservation",
        """
        CREATE TRIGGER resource_reservation_noop
        AFTER INSERT ON resource_reservation BEGIN SELECT 1; END
        """,
    ),
)
def test_resource_store_attests_schema_on_every_critical_transaction(
    tmp_path: Path,
    ddl: str,
) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        SQLiteResourceReservationStore,
    )

    database_path = tmp_path / "resource-reservations.sqlite3"
    store = SQLiteResourceReservationStore(database_path, clock=lambda: _RESERVATION_NOW)
    with sqlite3.connect(database_path) as connection:
        connection.execute(ddl)

    with pytest.raises(RuntimeResourceAdmissionError, match="schema"):
        store.active_leases()


def test_resource_store_verifies_exact_inserted_lease_in_same_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        SQLiteResourceReservationStore,
    )

    database_path = tmp_path / "resource-reservations.sqlite3"
    store = SQLiteResourceReservationStore(database_path, clock=lambda: _RESERVATION_NOW)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TRIGGER resource_reservation_zero_memory
            AFTER INSERT ON resource_reservation
            BEGIN
                UPDATE resource_reservation
                SET expected_memory_bytes = 0
                WHERE lease_id = NEW.lease_id;
            END
            """
        )
    monkeypatch.setattr(store, "_attest_schema", lambda _connection: None, raising=False)
    identity = _reservation_identity("worker-owner", 10)

    with pytest.raises(RuntimeResourceAdmissionError, match="insert"):
        store.reserve(
            identity=identity,
            request=_reservation_request(identity),
            policy=_reservation_policy(),
            snapshot_provider=_reservation_snapshot,
            lease_seconds=30,
        )


def test_crashed_resource_lease_keeps_full_ttl_beyond_job_deadline(tmp_path: Path) -> None:
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    current = [_RESERVATION_NOW]
    identity = _reservation_identity("worker-owner", 11)
    request = _reservation_request(identity).model_copy(
        update={"deadline": _RESERVATION_NOW + timedelta(seconds=2)}
    )
    store = SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: current[0],
    )
    admitted = store.reserve(
        identity=identity,
        request=request,
        policy=_reservation_policy(),
        snapshot_provider=lambda: _reservation_snapshot(current[0]),
        lease_seconds=5,
    )
    assert admitted.lease is not None
    assert admitted.lease.expires_at == _RESERVATION_NOW + timedelta(seconds=5)

    current[0] += timedelta(seconds=3)

    assert store.active_leases() == (admitted.lease,)


@pytest.mark.parametrize("value", (0, 1, "true", "false"))
def test_live_slo_boolean_contract_is_strict(value: object) -> None:
    from pydantic import ValidationError

    from rquant.runtime_resource_admission import LiveSloEvidence

    with pytest.raises(ValidationError, match="live_healthy"):
        LiveSloEvidence(
            observed_at=_RESERVATION_NOW,
            live_backlog_age_microseconds=0,
            live_p95_latency_microseconds=0,
            live_healthy=value,
        )


@pytest.mark.parametrize("value", (True, "10.0"))
def test_runtime_percentage_boundary_rejects_noncanonical_values(value: object) -> None:
    from rquant.runtime_resource_admission import RuntimeResourceAdmissionError, _bounded_pct

    with pytest.raises(RuntimeResourceAdmissionError, match="percentage"):
        _bounded_pct(value)


def test_runtime_builder_rejects_noncanonical_legacy_opt_out(tmp_path: Path) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        build_runtime_resource_admission,
    )

    with pytest.raises(RuntimeResourceAdmissionError, match="legacy_opt_out"):
        build_runtime_resource_admission(
            app_env="dev",
            disk_path=tmp_path,
            configured_policy_version=None,
            legacy_opt_out="false",
            probe=_FixedProbe(),
            live_slo_probe=None,
        )


def test_reservation_lock_wait_is_bounded_before_request_deadline(tmp_path: Path) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        SQLiteResourceReservationStore,
    )

    database_path = tmp_path / "resource-reservations.sqlite3"
    store = SQLiteResourceReservationStore(database_path)
    identity = _reservation_identity("worker-lock-wait", 12)
    request = _reservation_request(identity).model_copy(
        update={
            "expected_duration_ms": 1,
            "deadline": datetime.now(UTC) + timedelta(milliseconds=100),
        }
    )
    holder = sqlite3.connect(database_path, isolation_level=None, check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE")
    released = threading.Event()

    def release_external_lock() -> None:
        time.sleep(0.35)
        holder.rollback()
        released.set()

    unlocker = threading.Thread(target=release_external_lock)
    unlocker.start()
    started = time.monotonic()
    try:
        with pytest.raises(RuntimeResourceAdmissionError, match="lock|timeout|deadline"):
            store.reserve(
                identity=identity,
                request=request,
                policy=_reservation_policy(),
                snapshot_provider=lambda: _reservation_snapshot(datetime.now(UTC)),
                lease_seconds=5,
            )
        elapsed = time.monotonic() - started
    finally:
        if not released.is_set():
            holder.rollback()
        unlocker.join(timeout=1)
        holder.close()

    assert not unlocker.is_alive()
    assert elapsed < 0.25
    assert store.active_leases() == ()


def test_reservation_lock_wait_observes_cancellation_without_leaking_waiter(
    tmp_path: Path,
) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        SQLiteResourceReservationStore,
    )

    database_path = tmp_path / "resource-reservations.sqlite3"
    store = SQLiteResourceReservationStore(
        database_path,
        clock=lambda: datetime(2026, 8, 3, 8, 0, tzinfo=UTC),
    )
    identity = _reservation_identity("worker-lock-cancel", 13)
    holder = sqlite3.connect(database_path, isolation_level=None, check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE")
    cancelled = threading.Event()
    cancel_timer = threading.Timer(0.05, cancelled.set)
    cancel_timer.start()
    started = time.monotonic()
    try:
        with pytest.raises(RuntimeResourceAdmissionError, match="cancel"):
            store.reserve(
                identity=identity,
                request=_reservation_request(identity),
                policy=_reservation_policy(),
                snapshot_provider=_reservation_snapshot,
                lease_seconds=5,
                lock_wait_timeout_seconds=0.2,
                stop_requested=cancelled.is_set,
            )
        elapsed = time.monotonic() - started
    finally:
        holder.rollback()
        holder.close()
        cancel_timer.join(timeout=1)

    assert not cancel_timer.is_alive()
    assert elapsed < 0.25
    assert store.active_leases() == ()


_LEGACY_RESERVATION_LOCK_WAIT_BUDGET_SECONDS = 0.05
"""The flat budget `reserve()` gave a contender before issue #159.

Every test below pins the reservation write lock open past this value on
purpose: a legitimate contender that dies inside that window is the defect
under repair, not an artefact of the test.
"""

_RESERVATION_COMMIT_WINDOW_SECONDS = 3 * _LEGACY_RESERVATION_LOCK_WAIT_BUDGET_SECONDS
"""How long the winner stays inside `commit()` once the loser starts spinning.

Measured from the loser's first busy retry, so the window is fixed by the
contention itself rather than by a timer racing the test.
"""


class _CommitWindowConnection:
    """Holds the winner's write lock open across `commit()` - the fsync window."""

    def __init__(
        self,
        connection: sqlite3.Connection,
        *,
        entered: threading.Event,
        may_commit: threading.Event,
        window_seconds: list[float],
    ) -> None:
        self._connection = connection
        self._entered = entered
        self._may_commit = may_commit
        self._window_seconds = window_seconds

    def __getattr__(self, name: str) -> object:
        return getattr(self._connection, name)

    def __enter__(self) -> _CommitWindowConnection:
        self._connection.__enter__()
        return self

    def __exit__(self, *exc_info: object) -> object:
        return self._connection.__exit__(*exc_info)

    def commit(self) -> None:
        started = time.monotonic()
        self._entered.set()
        if not self._may_commit.wait(timeout=30):
            raise AssertionError("the winner's commit window was never released")
        self._connection.commit()
        self._window_seconds.append(time.monotonic() - started)


def _pin_the_commit_window(
    monkeypatch: pytest.MonkeyPatch,
    store: object,
    *,
    entered: threading.Event,
    may_commit: threading.Event,
) -> list[float]:
    window_seconds: list[float] = []
    original_connect = store._connect

    def gated_connect() -> _CommitWindowConnection:
        return _CommitWindowConnection(
            original_connect(),
            entered=entered,
            may_commit=may_commit,
            window_seconds=window_seconds,
        )

    monkeypatch.setattr(store, "_connect", gated_connect)
    return window_seconds


def _release_the_window_after_real_contention(
    monkeypatch: pytest.MonkeyPatch,
    retries: list[float],
    release: threading.Event,
    *,
    hold_seconds: float = _RESERVATION_COMMIT_WINDOW_SECONDS,
) -> None:
    """Record the contender's busy retries and free the winner once it has spun.

    The contender runs on the test's own thread, so the store's poll loop is the
    only thing that can advance the hold - no sleeping timer is involved.
    """

    import rquant.runtime_resource_admission as admission_module

    real_time = admission_module.system_time
    contender = threading.current_thread()
    contention_started: list[float] = []

    def counted_sleep(seconds: float) -> None:
        if threading.current_thread() is contender:
            now = real_time.monotonic()
            if not contention_started:
                contention_started.append(now)
            retries.append(seconds)
            if now - contention_started[0] >= hold_seconds:
                release.set()
        real_time.sleep(seconds)

    monkeypatch.setattr(
        admission_module,
        "system_time",
        SimpleNamespace(monotonic=real_time.monotonic, sleep=counted_sleep),
    )


def test_reservation_lock_wait_outlasts_a_winner_commit_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A legitimate loser must outlast the winner's commit, not be refused by it.

    The winner is pinned inside `connection.commit()` - the real fsync window,
    with the write lock held - while the loser burns busy retries against it.
    Before issue #159 the loser's budget was a flat 50ms, so it died there with
    `resource reservation lock wait timeout` and the capacity fence below was
    never exercised at all.
    """
    from rquant.resource_admission import AdmissionOutcome
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    database_path = tmp_path / "resource-reservations.sqlite3"
    winner_store = SQLiteResourceReservationStore(database_path, clock=lambda: _RESERVATION_NOW)
    loser_store = SQLiteResourceReservationStore(database_path, clock=lambda: _RESERVATION_NOW)
    reader_store = SQLiteResourceReservationStore(database_path, clock=lambda: _RESERVATION_NOW)
    winner_identity = _reservation_identity("worker-commit-winner", 8)
    loser_identity = _reservation_identity("worker-commit-loser", 9)

    entered_commit = threading.Event()
    may_commit = threading.Event()
    window_seconds = _pin_the_commit_window(
        monkeypatch,
        winner_store,
        entered=entered_commit,
        may_commit=may_commit,
    )
    loser_retries: list[float] = []
    _release_the_window_after_real_contention(monkeypatch, loser_retries, may_commit)

    winner_results: list[object] = []
    winner_failures: list[BaseException] = []

    def hold_the_write_lock() -> None:
        try:
            winner_results.append(
                winner_store.reserve(
                    identity=winner_identity,
                    request=_reservation_request(winner_identity),
                    policy=_reservation_policy(),
                    snapshot_provider=_reservation_snapshot,
                    lease_seconds=30,
                )
            )
        except BaseException as exc:  # noqa: BLE001 - surfaced through the assertions
            winner_failures.append(exc)
            may_commit.set()

    winner = threading.Thread(target=hold_the_write_lock)
    winner.start()
    loser_failure: BaseException | None = None
    loser_result: object | None = None
    try:
        assert entered_commit.wait(timeout=30)
        try:
            loser_result = loser_store.reserve(
                identity=loser_identity,
                request=_reservation_request(loser_identity),
                policy=_reservation_policy(),
                snapshot_provider=_reservation_snapshot,
                lease_seconds=30,
            )
        except BaseException as exc:  # noqa: BLE001 - surfaced through the assertions
            loser_failure = exc
    finally:
        may_commit.set()
        winner.join(timeout=30)

    assert not winner.is_alive()
    assert not winner_failures, f"the winner failed to commit: {winner_failures!r}"
    assert loser_failure is None, (
        "a legitimate contender was refused inside the winner's commit window: "
        f"{type(loser_failure).__name__}: {loser_failure}"
    )
    assert len(loser_retries) >= 5
    assert window_seconds
    assert window_seconds[0] > _LEGACY_RESERVATION_LOCK_WAIT_BUDGET_SECONDS
    assert winner_results[0].decision.outcome is AdmissionOutcome.ADMITTED
    assert winner_results[0].lease is not None
    assert loser_result is not None
    assert loser_result.decision.outcome is AdmissionOutcome.DEFERRED
    assert loser_result.lease is None
    assert tuple(lease.identity for lease in reader_store.active_leases()) == (winner_identity,)


def test_reservation_lock_wait_budget_tracks_the_store_bound_and_caller_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.runtime_resource_admission import (
        _MAX_RESOURCE_LOCK_WAIT_SECONDS,
        SQLiteResourceReservationStore,
    )

    store = SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: _RESERVATION_NOW,
    )
    observed: list[float] = []
    original_begin = store._begin_immediate

    def recording_begin(
        connection: object,
        *,
        lock_wait_timeout_seconds: object,
        stop_requested: object,
    ) -> None:
        observed.append(lock_wait_timeout_seconds)
        original_begin(
            connection,
            lock_wait_timeout_seconds=lock_wait_timeout_seconds,
            stop_requested=stop_requested,
        )

    monkeypatch.setattr(store, "_begin_immediate", recording_begin)

    unbounded = _reservation_identity("worker-budget-unbounded", 10)
    store.reserve(
        identity=unbounded,
        request=_reservation_request(unbounded),
        policy=_reservation_policy(),
        snapshot_provider=_reservation_snapshot,
        lease_seconds=30,
    )
    near_deadline = _reservation_identity("worker-budget-near-deadline", 11)
    store.reserve(
        identity=near_deadline,
        request=_reservation_request(near_deadline).model_copy(
            update={
                "expected_duration_ms": 1,
                "deadline": _RESERVATION_NOW + timedelta(milliseconds=20),
            }
        ),
        policy=_reservation_policy(),
        snapshot_provider=_reservation_snapshot,
        lease_seconds=30,
    )
    configured = _reservation_identity("worker-budget-configured", 12)
    store.reserve(
        identity=configured,
        request=_reservation_request(configured),
        policy=_reservation_policy(),
        snapshot_provider=_reservation_snapshot,
        lease_seconds=30,
        lock_wait_timeout_seconds=0.2,
    )

    assert observed[0] == _MAX_RESOURCE_LOCK_WAIT_SECONDS
    assert observed[0] > _LEGACY_RESERVATION_LOCK_WAIT_BUDGET_SECONDS
    assert observed[1] == pytest.approx(0.02)
    assert observed[2] == pytest.approx(0.2)


def test_reservation_lock_wait_cancels_inside_a_winner_commit_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A wider budget must not cost stop responsiveness.

    The loser waits behind a pinned commit window with a full budget available
    and still has to abandon the wait on the very next poll after the stop
    authority flips, well inside the budget it was given.
    """
    from rquant.runtime_resource_admission import (
        _MAX_RESOURCE_LOCK_WAIT_SECONDS,
        RuntimeResourceAdmissionError,
        SQLiteResourceReservationStore,
    )

    database_path = tmp_path / "resource-reservations.sqlite3"
    winner_store = SQLiteResourceReservationStore(database_path, clock=lambda: _RESERVATION_NOW)
    loser_store = SQLiteResourceReservationStore(database_path, clock=lambda: _RESERVATION_NOW)
    reader_store = SQLiteResourceReservationStore(database_path, clock=lambda: _RESERVATION_NOW)
    winner_identity = _reservation_identity("worker-cancel-winner", 14)
    loser_identity = _reservation_identity("worker-cancel-loser", 15)

    entered_commit = threading.Event()
    may_commit = threading.Event()
    _pin_the_commit_window(
        monkeypatch,
        winner_store,
        entered=entered_commit,
        may_commit=may_commit,
    )
    loser_retries: list[float] = []
    stop = threading.Event()
    _release_the_window_after_real_contention(monkeypatch, loser_retries, stop)

    winner_results: list[object] = []

    def hold_the_write_lock() -> None:
        try:
            winner_results.append(
                winner_store.reserve(
                    identity=winner_identity,
                    request=_reservation_request(winner_identity),
                    policy=_reservation_policy(),
                    snapshot_provider=_reservation_snapshot,
                    lease_seconds=30,
                )
            )
        except BaseException:  # noqa: BLE001 - surfaced through the assertions
            may_commit.set()

    winner = threading.Thread(target=hold_the_write_lock)
    winner.start()
    retries_at_stop = 0
    started = 0.0
    elapsed = 0.0
    try:
        assert entered_commit.wait(timeout=30)
        started = time.monotonic()
        with pytest.raises(RuntimeResourceAdmissionError, match="cancel") as refusal:
            loser_store.reserve(
                identity=loser_identity,
                request=_reservation_request(loser_identity),
                policy=_reservation_policy(),
                snapshot_provider=_reservation_snapshot,
                lease_seconds=30,
                stop_requested=stop.is_set,
            )
        elapsed = time.monotonic() - started
        retries_at_stop = len(loser_retries)
    finally:
        may_commit.set()
        winner.join(timeout=30)

    assert not winner.is_alive()
    assert refusal.value is not None
    assert stop.is_set()
    assert len(loser_retries) >= 5
    assert len(loser_retries) - retries_at_stop <= 1
    assert elapsed < _MAX_RESOURCE_LOCK_WAIT_SECONDS
    assert winner_results[0].lease is not None
    assert tuple(lease.identity for lease in reader_store.active_leases()) == (winner_identity,)


def test_reservation_refusals_separate_transient_contention_from_broken_contracts(
    tmp_path: Path,
) -> None:
    """Contention, cancellation and a broken contract are three different answers.

    Only the first is worth retrying, and issue #159 came from a caller that
    could not tell them apart because they all arrived as the same base class.
    """
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionCancelledError,
        RuntimeResourceAdmissionError,
        RuntimeResourceAdmissionLockWaitTimeoutError,
        RuntimeResourceAdmissionTransientError,
        SQLiteResourceReservationStore,
    )

    assert issubclass(RuntimeResourceAdmissionTransientError, RuntimeResourceAdmissionError)
    assert issubclass(
        RuntimeResourceAdmissionLockWaitTimeoutError,
        RuntimeResourceAdmissionTransientError,
    )
    assert issubclass(RuntimeResourceAdmissionCancelledError, RuntimeResourceAdmissionError)
    assert not issubclass(
        RuntimeResourceAdmissionCancelledError,
        RuntimeResourceAdmissionTransientError,
    )

    database_path = tmp_path / "resource-reservations.sqlite3"
    store = SQLiteResourceReservationStore(database_path, clock=lambda: _RESERVATION_NOW)
    identity = _reservation_identity("worker-typed-refusal", 6)
    stopped = threading.Event()
    stopped.set()
    holder = sqlite3.connect(database_path, isolation_level=None, check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(RuntimeResourceAdmissionLockWaitTimeoutError) as contended:
            store.reserve(
                identity=identity,
                request=_reservation_request(identity),
                policy=_reservation_policy(),
                snapshot_provider=_reservation_snapshot,
                lease_seconds=30,
                lock_wait_timeout_seconds=_LEGACY_RESERVATION_LOCK_WAIT_BUDGET_SECONDS,
            )
        with pytest.raises(RuntimeResourceAdmissionCancelledError) as cancelled:
            store.reserve(
                identity=identity,
                request=_reservation_request(identity),
                policy=_reservation_policy(),
                snapshot_provider=_reservation_snapshot,
                lease_seconds=30,
                stop_requested=stopped.is_set,
            )
    finally:
        holder.rollback()
        holder.close()

    assert str(contended.value) == "resource reservation lock wait timeout"
    assert isinstance(contended.value, RuntimeResourceAdmissionTransientError)
    assert not isinstance(cancelled.value, RuntimeResourceAdmissionTransientError)

    mismatched = _reservation_identity("worker-typed-mismatch", 7)
    with pytest.raises(RuntimeResourceAdmissionError) as contract:
        store.reserve(
            identity=identity,
            request=_reservation_request(mismatched),
            policy=_reservation_policy(),
            snapshot_provider=_reservation_snapshot,
            lease_seconds=30,
        )

    assert not isinstance(contract.value, RuntimeResourceAdmissionTransientError)
    assert store.active_leases() == ()


def test_reserve_retry_returns_same_authoritative_lease_and_rejects_conflict(
    tmp_path: Path,
) -> None:
    from rquant.resource_admission import AdmissionOutcome
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        SQLiteResourceReservationStore,
    )

    identity = _reservation_identity("worker-idempotent-reserve", 13)
    request = _reservation_request(identity)
    store = SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: _RESERVATION_NOW,
    )
    first = store.reserve(
        identity=identity,
        request=request,
        policy=_reservation_policy(),
        snapshot_provider=_reservation_snapshot,
        lease_seconds=30,
    )

    retried = store.reserve(
        identity=identity,
        request=request,
        policy=_reservation_policy(),
        snapshot_provider=_reservation_snapshot,
        lease_seconds=30,
    )

    assert retried.decision.outcome is AdmissionOutcome.ADMITTED
    assert retried.lease == first.lease
    conflicting_request = request.model_copy(update={"expected_disk_bytes": 2})
    with pytest.raises(RuntimeResourceAdmissionError, match="conflict"):
        store.reserve(
            identity=identity,
            request=conflicting_request,
            policy=_reservation_policy(),
            snapshot_provider=_reservation_snapshot,
            lease_seconds=30,
        )


def test_renew_response_loss_retry_returns_current_receipt_without_reextending(
    tmp_path: Path,
) -> None:
    from rquant.resource_admission import AdmissionOutcome
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    current = [_RESERVATION_NOW]
    identity = _reservation_identity("worker-idempotent-renew", 14)
    request = _reservation_request(identity)
    store = SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: current[0],
    )
    first = store.reserve(
        identity=identity,
        request=request,
        policy=_reservation_policy(),
        snapshot_provider=lambda: _reservation_snapshot(current[0]),
        lease_seconds=5,
    )
    assert first.lease is not None
    current[0] += timedelta(seconds=2)
    committed = store.recheck(
        lease=first.lease,
        identity=identity,
        request=request,
        policy=_reservation_policy(),
        snapshot_provider=lambda: _reservation_snapshot(current[0]),
        lease_seconds=5,
    )
    assert committed.lease is not None

    retried = store.recheck(
        lease=first.lease,
        identity=identity,
        request=request,
        policy=_reservation_policy(),
        snapshot_provider=lambda: _reservation_snapshot(current[0]),
        lease_seconds=5,
    )

    assert retried.decision.outcome is AdmissionOutcome.ADMITTED
    assert retried.lease == committed.lease
    assert store.active_leases() == (committed.lease,)

    current[0] += timedelta(seconds=1)
    advanced = store.recheck(
        lease=committed.lease,
        identity=identity,
        request=request,
        policy=_reservation_policy(),
        snapshot_provider=lambda: _reservation_snapshot(current[0]),
        lease_seconds=5,
    )
    assert advanced.lease is not None
    with pytest.raises(RuntimeError, match="changed"):
        store.recheck(
            lease=first.lease,
            identity=identity,
            request=request,
            policy=_reservation_policy(),
            snapshot_provider=lambda: _reservation_snapshot(current[0]),
            lease_seconds=5,
        )
    wrong_fence = identity.model_copy(update={"scheduler_fencing_token": 2})
    with pytest.raises(RuntimeError, match="identity"):
        store.recheck(
            lease=advanced.lease,
            identity=wrong_fence,
            request=request,
            policy=_reservation_policy(),
            snapshot_provider=lambda: _reservation_snapshot(current[0]),
            lease_seconds=5,
        )


def test_recheck_cannot_revive_lease_that_expires_during_probe(tmp_path: Path) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        SQLiteResourceReservationStore,
    )

    current = [_RESERVATION_NOW]
    identity = _reservation_identity("worker-expiring-probe", 15)
    request = _reservation_request(identity)
    store = SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: current[0],
    )
    first = store.reserve(
        identity=identity,
        request=request,
        policy=_reservation_policy(),
        snapshot_provider=lambda: _reservation_snapshot(current[0]),
        lease_seconds=5,
    )
    assert first.lease is not None
    current[0] += timedelta(seconds=4)

    def slow_probe():
        current[0] += timedelta(seconds=2)
        return _reservation_snapshot(current[0])

    with pytest.raises(RuntimeResourceAdmissionError, match="expired"):
        store.recheck(
            lease=first.lease,
            identity=identity,
            request=request,
            policy=_reservation_policy(),
            snapshot_provider=slow_probe,
            lease_seconds=5,
        )

    assert store.active_leases() == ()


def test_4097th_active_reservation_is_never_inserted(tmp_path: Path) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        SQLiteResourceReservationStore,
    )

    database_path = tmp_path / "resource-reservations.sqlite3"
    store = SQLiteResourceReservationStore(database_path, clock=lambda: _RESERVATION_NOW)
    granted_at = _RESERVATION_NOW.isoformat(timespec="microseconds")
    expires_at = (_RESERVATION_NOW + timedelta(minutes=1)).isoformat(timespec="microseconds")
    with sqlite3.connect(database_path) as connection:
        columns = [
            "lease_id",
            "job_id",
            "run_id",
            "shard_id",
            "attempt_id",
            "claim_generation",
            "scheduler_fencing_token",
            "worker_id",
            "request_hash",
            "expected_memory_bytes",
            "expected_disk_bytes",
            "expected_quota_units",
            "granted_at",
            "expires_at",
        ]
        table_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(resource_reservation)")
        }
        if "last_renewal_operation_id" in table_columns:
            columns.append("last_renewal_operation_id")
        placeholders = ", ".join("?" for _ in columns)
        rows = []
        for index in range(1, 4_097):
            values: list[object] = [
                f"{index:064x}",
                f"00000000-0000-0000-0000-{index:012d}",
                f"{index:064x}",
                f"10000000-0000-0000-0000-{index:012d}",
                f"20000000-0000-0000-0000-{index:012d}",
                1,
                1,
                f"seed-{index}",
                "a" * 64,
                0,
                0,
                0,
                granted_at,
                expires_at,
            ]
            if "last_renewal_operation_id" in table_columns:
                values.append(f"{index:064x}")
            rows.append(tuple(values))
        connection.executemany(
            f"INSERT INTO resource_reservation ({', '.join(columns)}) VALUES ({placeholders})",
            rows,
        )

    identity = _reservation_identity("worker-over-capacity", 12)
    with pytest.raises(RuntimeResourceAdmissionError, match="budget|capacity"):
        store.reserve(
            identity=identity,
            request=_reservation_request(identity),
            policy=_reservation_policy(),
            snapshot_provider=_reservation_snapshot,
            lease_seconds=5,
        )

    with sqlite3.connect(database_path) as connection:
        count = connection.execute("SELECT COUNT(*) FROM resource_reservation").fetchone()[0]
    assert count == 4_096


def test_persisted_authority_watermark_rejects_utc_clock_rollback(tmp_path: Path) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        SQLiteResourceReservationStore,
    )

    current = [_RESERVATION_NOW]
    identity = _reservation_identity("worker-clock-watermark", 13)
    request = _reservation_request(identity)
    store = SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: current[0],
    )
    first = store.reserve(
        identity=identity,
        request=request,
        policy=_reservation_policy(),
        snapshot_provider=lambda: _reservation_snapshot(current[0]),
        lease_seconds=5,
    )
    assert first.lease is not None
    current[0] += timedelta(seconds=2)
    assert store.active_leases() == (first.lease,)
    current[0] -= timedelta(seconds=1)

    with pytest.raises(RuntimeResourceAdmissionError, match="rollback"):
        store.recheck(
            lease=first.lease,
            identity=identity,
            request=request,
            policy=_reservation_policy(),
            snapshot_provider=lambda: _reservation_snapshot(current[0]),
            lease_seconds=5,
        )

    with sqlite3.connect(store.path) as connection:
        expires_at = connection.execute(
            "SELECT expires_at FROM resource_reservation WHERE lease_id = ?",
            (first.lease.lease_id,),
        ).fetchone()[0]
    assert expires_at == first.lease.expires_at.isoformat(timespec="microseconds")


def test_persisted_snapshot_watermark_rejects_observation_time_rollback(
    tmp_path: Path,
) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        SQLiteResourceReservationStore,
    )

    current = [_RESERVATION_NOW]
    identity = _reservation_identity("worker-snapshot-watermark", 14)
    request = _reservation_request(identity)
    store = SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: current[0],
    )
    first = store.reserve(
        identity=identity,
        request=request,
        policy=_reservation_policy(),
        snapshot_provider=lambda: _reservation_snapshot(current[0]),
        lease_seconds=5,
    )
    assert first.lease is not None
    current[0] += timedelta(seconds=1)

    with pytest.raises(RuntimeResourceAdmissionError, match="snapshot clock rollback"):
        store.recheck(
            lease=first.lease,
            identity=identity,
            request=request,
            policy=_reservation_policy(),
            snapshot_provider=lambda: _reservation_snapshot(
                _RESERVATION_NOW - timedelta(seconds=1)
            ),
            lease_seconds=5,
        )

    assert store.active_leases() == (first.lease,)


def _sqlite_failure(message: str, errorcode: int) -> sqlite3.OperationalError:
    """A SQLite failure whose wording and error code are chosen independently.

    `sqlite3` only attaches `sqlite_errorcode` to exceptions it raises itself,
    and the attribute is writable on a hand-built instance on both 3.11 and
    3.12 - so the two can be varied one at a time.  That separation is the
    whole point: the classifier must follow the code, and a message that says
    `database is locked` while carrying `SQLITE_ERROR` must not be retried.
    """

    failure = sqlite3.OperationalError(message)
    failure.sqlite_errorcode = errorcode
    return failure


class _ScriptedBeginConnection:
    """Answers `BEGIN IMMEDIATE` with a scripted failure, then succeeds."""

    def __init__(self, failure: BaseException, *, failures: int | None) -> None:
        self._failure = failure
        self._remaining = failures
        self.attempts = 0
        self.rollbacks = 0

    def execute(self, statement: str) -> object:
        assert statement == "BEGIN IMMEDIATE"
        self.attempts += 1
        if self._remaining is None or self._remaining > 0:
            if self._remaining is not None:
                self._remaining -= 1
            raise self._failure
        return None

    def rollback(self) -> None:
        self.rollbacks += 1


class _StepClock:
    """A monotonic clock that only advances when the store sleeps.

    The retry loops are then fully determined by their own budget arithmetic:
    no wall-clock race decides how many attempts a test observes.
    """

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _install_step_clock(monkeypatch: pytest.MonkeyPatch) -> _StepClock:
    import rquant.runtime_resource_admission as admission_module

    clock = _StepClock()
    monkeypatch.setattr(
        admission_module,
        "system_time",
        SimpleNamespace(monotonic=clock.monotonic, sleep=clock.sleep),
    )
    return clock


def _contention_store(tmp_path: Path):
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    return SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: _RESERVATION_NOW,
    )


def test_sqlite_contention_is_classified_by_error_code_and_never_by_wording(
    tmp_path: Path,
) -> None:
    """The classifier reads `sqlite_errorcode`; the message is not an interface.

    SQLite's English wording is free to change between builds, so a text match
    on `locked`/`busy` both misses real contention worded differently and
    promotes permanent faults - which really do say `database is locked` in
    some paths - into an unbounded retry.  Extended codes classify with their
    primary code, and anything that carries no structured code at all fails
    closed onto "not contention", because retrying a permanent fault burns the
    caller's whole budget for nothing.
    """
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        RuntimeResourceAdmissionTransientError,
        _reservation_failure,
    )

    del tmp_path

    contended = (
        _sqlite_failure("totally unrelated wording", sqlite3.SQLITE_BUSY),
        _sqlite_failure("totally unrelated wording", sqlite3.SQLITE_LOCKED),
        _sqlite_failure("nothing here says the b-word", sqlite3.SQLITE_BUSY_SNAPSHOT),
        _sqlite_failure("nothing here says the l-word", sqlite3.SQLITE_BUSY_RECOVERY),
        _sqlite_failure("still nothing", sqlite3.SQLITE_LOCKED_SHAREDCACHE),
    )
    for failure in contended:
        classified = _reservation_failure("resource reservation store failed", failure)
        assert isinstance(classified, RuntimeResourceAdmissionTransientError), failure
        assert str(classified) == "resource reservation store failed"

    permanent = (
        _sqlite_failure("database is locked", sqlite3.SQLITE_ERROR),
        _sqlite_failure("the database file is busy", sqlite3.SQLITE_CORRUPT),
        _sqlite_failure("database is locked", sqlite3.SQLITE_READONLY),
        sqlite3.OperationalError("database is locked"),
        OSError("database is locked"),
    )
    for failure in permanent:
        classified = _reservation_failure("resource reservation store failed", failure)
        assert isinstance(classified, RuntimeResourceAdmissionError), failure
        assert not isinstance(classified, RuntimeResourceAdmissionTransientError), failure


def test_begin_immediate_retries_a_busy_code_whose_message_changed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same code, different wording: the lock wait must still be spent on it."""
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionLockWaitTimeoutError,
        RuntimeResourceAdmissionTransientError,
    )

    store = _contention_store(tmp_path)
    clock = _install_step_clock(monkeypatch)

    transient = _ScriptedBeginConnection(
        _sqlite_failure("totally unrelated wording", sqlite3.SQLITE_BUSY),
        failures=3,
    )
    store._begin_immediate(
        transient,
        lock_wait_timeout_seconds=_LEGACY_RESERVATION_LOCK_WAIT_BUDGET_SECONDS,
        stop_requested=None,
    )
    assert transient.attempts == 4
    assert clock.sleeps == [0.005, 0.005, 0.005]
    assert transient.rollbacks == 0

    clock.sleeps.clear()
    forever = _ScriptedBeginConnection(
        _sqlite_failure("totally unrelated wording", sqlite3.SQLITE_BUSY),
        failures=None,
    )
    with pytest.raises(RuntimeResourceAdmissionLockWaitTimeoutError) as timed_out:
        store._begin_immediate(
            forever,
            lock_wait_timeout_seconds=_LEGACY_RESERVATION_LOCK_WAIT_BUDGET_SECONDS,
            stop_requested=None,
        )

    assert isinstance(timed_out.value, RuntimeResourceAdmissionTransientError)
    assert sum(clock.sleeps) == pytest.approx(_LEGACY_RESERVATION_LOCK_WAIT_BUDGET_SECONDS)
    assert forever.attempts == len(clock.sleeps) + 1
    assert forever.rollbacks == 0


def test_begin_immediate_refuses_a_locked_message_that_is_not_a_contention_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A permanent fault that happens to say `locked` must not be retried.

    This is the discrimination a text match cannot make: before the fix the
    loop spent the whole lock-wait budget on an error SQLite had already
    classified as `SQLITE_ERROR`, then reported it as transient contention.
    """
    from rquant.runtime_resource_admission import RuntimeResourceAdmissionError

    store = _contention_store(tmp_path)
    clock = _install_step_clock(monkeypatch)

    failure = _sqlite_failure("database is locked", sqlite3.SQLITE_ERROR)
    connection = _ScriptedBeginConnection(failure, failures=None)
    with pytest.raises(sqlite3.OperationalError) as refused:
        store._begin_immediate(
            connection,
            lock_wait_timeout_seconds=_LEGACY_RESERVATION_LOCK_WAIT_BUDGET_SECONDS,
            stop_requested=None,
        )

    assert refused.value is failure
    assert not isinstance(refused.value, RuntimeResourceAdmissionError)
    assert connection.attempts == 1
    assert clock.sleeps == []
    assert connection.rollbacks == 0


def test_initialize_retry_loop_classifies_by_error_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The third classification point answers to the same code, not the text."""
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        RuntimeResourceAdmissionTransientError,
    )

    store = _contention_store(tmp_path)
    clock = _install_step_clock(monkeypatch)

    permanent = _sqlite_failure("database is locked", sqlite3.SQLITE_ERROR)
    permanent_attempts: list[int] = []

    def always_permanent() -> None:
        permanent_attempts.append(1)
        raise permanent

    monkeypatch.setattr(store, "_initialize_once", always_permanent)
    with pytest.raises(RuntimeResourceAdmissionError) as refused:
        store._initialize()

    assert not isinstance(refused.value, RuntimeResourceAdmissionTransientError)
    assert refused.value.__cause__ is permanent
    assert len(permanent_attempts) == 1
    assert clock.sleeps == []

    contended = _sqlite_failure("totally unrelated wording", sqlite3.SQLITE_BUSY)
    contended_attempts: list[int] = []

    def busy_twice() -> None:
        contended_attempts.append(1)
        if len(contended_attempts) <= 2:
            raise contended

    monkeypatch.setattr(store, "_initialize_once", busy_twice)
    store._initialize()

    assert len(contended_attempts) == 3
    assert clock.sleeps == [0.005, 0.005]


def test_initialize_reports_an_exhausted_busy_code_as_transient(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Contention that outlives the initialisation budget is still retryable."""
    from rquant.runtime_resource_admission import (
        _MAX_RESOURCE_LOCK_WAIT_SECONDS,
        RuntimeResourceAdmissionTransientError,
    )

    store = _contention_store(tmp_path)
    clock = _install_step_clock(monkeypatch)
    contended = _sqlite_failure("totally unrelated wording", sqlite3.SQLITE_LOCKED)

    def always_contended() -> None:
        raise contended

    monkeypatch.setattr(store, "_initialize_once", always_contended)
    with pytest.raises(RuntimeResourceAdmissionTransientError) as exhausted:
        store._initialize()

    assert exhausted.value.__cause__ is contended
    assert sum(clock.sleeps) == pytest.approx(_MAX_RESOURCE_LOCK_WAIT_SECONDS)


def test_real_two_connection_contention_reports_a_busy_error_code(
    tmp_path: Path,
) -> None:
    """Regression: the real contention path still lands on the transient branch.

    The synthetic cases above set `sqlite_errorcode` by hand, so this one pins
    the assumption they rest on - that a genuine write-lock collision arrives
    as `SQLITE_BUSY` - against the real library rather than against a fake.
    """
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionLockWaitTimeoutError,
        RuntimeResourceAdmissionTransientError,
        _reservation_failure,
    )

    database_path = tmp_path / "resource-reservations.sqlite3"
    store = _contention_store(tmp_path)
    identity = _reservation_identity("worker-real-contention", 11)

    holder = sqlite3.connect(database_path, isolation_level=None, check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE")
    try:
        loser = sqlite3.connect(database_path, timeout=0, isolation_level=None)
        loser.execute("PRAGMA busy_timeout = 0")
        try:
            with pytest.raises(sqlite3.OperationalError) as observed:
                loser.execute("BEGIN IMMEDIATE")
        finally:
            loser.close()

        assert observed.value.sqlite_errorname == "SQLITE_BUSY"
        assert observed.value.sqlite_errorcode & 0xFF == sqlite3.SQLITE_BUSY
        assert isinstance(
            _reservation_failure("resource reservation store failed", observed.value),
            RuntimeResourceAdmissionTransientError,
        )

        with pytest.raises(RuntimeResourceAdmissionLockWaitTimeoutError):
            store.reserve(
                identity=identity,
                request=_reservation_request(identity),
                policy=_reservation_policy(),
                snapshot_provider=_reservation_snapshot,
                lease_seconds=30,
                lock_wait_timeout_seconds=_LEGACY_RESERVATION_LOCK_WAIT_BUDGET_SECONDS,
            )
    finally:
        holder.rollback()
        holder.close()

    assert store.active_leases() == ()
