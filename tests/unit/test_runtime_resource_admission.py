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
    from pathlib import Path

    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

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
    outcomes.put((worker_id, result.decision.outcome.value, result.lease is not None))


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

        assert [process.exitcode for process in processes] == [0, 0]
        results = sorted(outcomes.get(timeout=1) for _ in processes)
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
