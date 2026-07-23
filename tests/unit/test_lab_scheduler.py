from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from threading import Thread
from uuid import uuid4

import pytest

from rquant.lab_job_protocol import (
    LabAcknowledgedCommand,
    LabCommandEnvelope,
    LabCommandReceipt,
    LabCommandSpool,
    LabSpoolEntry,
    SubmitJobCommand,
)
from rquant.lab_jobs import (
    JobStatus,
    LabJobReader,
    LabJobStore,
    SchedulerLeaseFencedError,
    SchedulerLeaseUnavailableError,
)
from rquant.lab_scheduler import LabScheduler, SchedulerTickResult
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    FeatureContractIdentity,
    ResearchJobType,
    ResearchRunParameters,
    ResearchRunSpec,
    ResourceClass,
)

NOW = datetime(2026, 7, 24, 1, 0, tzinfo=UTC)


def _spec() -> ResearchRunSpec:
    return ResearchRunSpec(
        job_type=ResearchJobType.STRATEGY_REPLAY,
        parameters=ResearchRunParameters(
            strategy_name="n_shape",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 7, 14),
        ),
        code_sha="1" * 40,
        dataset_snapshot=DatasetSnapshotIdentity(
            snapshot_id="a" * 64,
            binding_hash="b" * 64,
            audit_run_id="d" * 64,
        ),
        feature_contract=FeatureContractIdentity(
            contract_id="intraday-core",
            contract_version="v1",
            contract_hash="c" * 64,
        ),
        execution_costs=ExecutionCostSpec(
            commission_bps=Decimal("2.5"),
            stamp_duty_bps=Decimal("5"),
            transfer_fee_bps=Decimal("0.1"),
            slippage_bps=Decimal("3"),
        ),
        random_seed=20260724,
        resource_class=ResourceClass.STANDARD,
        deadline=datetime(2026, 7, 25, 2, tzinfo=UTC),
        research_status="comparable",
    )


def _envelope() -> LabCommandEnvelope:
    return LabCommandEnvelope(
        request_id=uuid4(),
        command=SubmitJobCommand(job_id=uuid4(), spec=_spec(), max_attempts=3),
    )


def _components(tmp_path: Path) -> tuple[LabJobStore, LabCommandSpool]:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    return store, LabCommandSpool(tmp_path / "commands")


def _scheduler(
    store: LabJobStore,
    spool: LabCommandSpool,
    *,
    owner: str = "scheduler-a",
    now: datetime = NOW,
    batch_size: int = 32,
) -> LabScheduler:
    return LabScheduler(
        store=store,
        spool=spool,
        owner_id=owner,
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        max_commands_per_tick=batch_size,
        clock=lambda: now,
    )


def test_run_once_consumes_submit_but_keeps_job_queued_without_adapter(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    envelope = _envelope()
    spool.publish(envelope)
    scheduler = _scheduler(store, spool)

    result = scheduler.run_once()
    job = LabJobReader(store.path).get_job(envelope.command.job_id)

    assert isinstance(result, SchedulerTickResult)
    assert result.lease_acquired is True
    assert result.processed == 1
    assert result.applied == 1
    assert result.rejected == 0
    assert result.quarantined == 0
    assert result.recovered == 0
    assert job is not None
    assert job.status is JobStatus.QUEUED
    assert spool.pending() == ()


def test_run_once_processes_only_bounded_batch(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    for _ in range(3):
        spool.publish(_envelope())
    scheduler = _scheduler(store, spool, batch_size=2)

    result = scheduler.run_once()

    assert result.processed == 2
    assert len(spool.pending()) == 1


class _CrashBeforeAckSpool(LabCommandSpool):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.crash = True

    def ack(
        self,
        entry: LabSpoolEntry,
        receipt: LabCommandReceipt,
    ) -> LabAcknowledgedCommand:
        if self.crash:
            self.crash = False
            raise RuntimeError("simulated crash after ledger commit")
        return super().ack(entry, receipt)


def test_commit_before_ack_crash_replays_without_duplicate_effect(
    tmp_path: Path,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    spool = _CrashBeforeAckSpool(tmp_path / "commands")
    envelope = _envelope()
    spool.publish(envelope)
    scheduler = _scheduler(store, spool)

    with pytest.raises(RuntimeError, match="after ledger commit"):
        scheduler.run_once()
    reader = LabJobReader(store.path)
    first_events = reader.list_events(envelope.command.job_id)
    assert len(first_events) == 1
    assert len(spool.pending()) == 1

    replay = scheduler.run_once()

    assert replay.processed == 1
    assert replay.applied == 1
    assert len(reader.list_events(envelope.command.job_id)) == 1
    assert spool.pending() == ()


def test_each_command_mutation_uses_a_fresh_clock_value(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    envelopes = (_envelope(), _envelope())
    for envelope in envelopes:
        spool.publish(envelope)
    moments = iter(
        (
            NOW,
            NOW + timedelta(seconds=1),
            NOW + timedelta(seconds=2),
            NOW + timedelta(seconds=3),
        )
    )
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        clock=lambda: next(moments),
    )

    scheduler.run_once()

    reader = LabJobReader(store.path)
    applied_at = {reader.get_command(envelope.request_id).applied_at for envelope in envelopes}
    assert applied_at == {
        NOW + timedelta(seconds=2),
        NOW + timedelta(seconds=3),
    }


class _SlowAckSpool(LabCommandSpool):
    def __init__(self, root: Path, current: list[datetime]) -> None:
        super().__init__(root)
        self.current = current

    def ack(
        self,
        entry: LabSpoolEntry,
        receipt: LabCommandReceipt,
    ) -> LabAcknowledgedCommand:
        acknowledged = super().ack(entry, receipt)
        self.current[0] += timedelta(seconds=70)
        return acknowledged


def test_slow_ack_expiry_fences_next_command_in_same_tick(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    current = [NOW]
    spool = _SlowAckSpool(tmp_path / "commands", current)
    for _ in range(2):
        spool.publish(_envelope())
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        clock=lambda: current[0],
    )

    with pytest.raises(SchedulerLeaseFencedError):
        scheduler.run_once()

    assert len(spool.pending()) == 1


def test_bad_json_is_quarantined_and_does_not_block_valid_command(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    bad = spool.pending_dir / "00000000-0000-0000-0000-000000000000.json"
    bad.write_text("{broken", encoding="utf-8")
    valid = _envelope()
    spool.publish(valid)

    result = _scheduler(store, spool).run_once()

    assert result.processed == 1
    assert result.quarantined == 1
    assert result.applied == 1
    assert not bad.exists()
    assert len(tuple(spool.quarantine_dir.glob("*.bad"))) == 1
    assert LabJobReader(store.path).get_job(valid.command.job_id) is not None


def test_malformed_filename_is_quarantined_across_restart_without_blocking(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    bad = spool.pending_dir / "not-a-command.json"
    bad.write_text("{broken", encoding="utf-8")
    valid = _envelope()
    spool.publish(valid)
    restarted = LabCommandSpool(spool.root)

    result = _scheduler(store, restarted).run_once()

    assert result.quarantined == 1
    assert result.processed == 1
    assert result.applied == 1
    assert restarted.pending() == ()
    assert not bad.exists()
    assert len(tuple(restarted.quarantine_dir.glob("not-a-command.json*.bad"))) == 1
    assert LabJobReader(store.path).get_job(valid.command.job_id) is not None
    assert LabCommandSpool(spool.root).pending() == ()


def test_pending_symlink_is_recorded_without_touching_target_or_blocking_after_restart(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    victim = tmp_path / "external-target.json"
    victim.write_text("do-not-touch", encoding="utf-8")
    symlink = spool.pending_dir / "not-a-command.json"
    symlink.symlink_to(victim)
    valid = _envelope()
    spool.publish(valid)
    restarted = LabCommandSpool(spool.root)

    result = _scheduler(store, restarted).run_once()

    assert result.quarantined == 1
    assert result.processed == 1
    assert result.applied == 1
    assert not symlink.exists()
    assert not symlink.is_symlink()
    assert victim.read_text(encoding="utf-8") == "do-not-touch"
    artifacts = tuple(restarted.quarantine_dir.glob("not-a-command.json*.symlink.bad.json"))
    assert len(artifacts) == 1
    assert artifacts[0].is_file()
    assert not artifacts[0].is_symlink()
    metadata = json.loads(artifacts[0].read_text(encoding="utf-8"))
    assert metadata["original_name"] == "not-a-command.json"
    assert metadata["link_target"] == str(victim)
    assert "invalid_envelope" in metadata["reason"]
    assert LabJobReader(store.path).get_job(valid.command.job_id) is not None
    assert LabCommandSpool(spool.root).pending() == ()


def test_semantic_request_conflict_is_quarantined_and_does_not_block_next_command(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    scheduler = _scheduler(store, spool)
    scheduler.run_once()
    assert scheduler.lease is not None
    request_id = uuid4()
    accepted = LabCommandEnvelope(
        request_id=request_id,
        command=SubmitJobCommand(job_id=uuid4(), spec=_spec(), max_attempts=3),
    )
    store.apply_command(accepted, lease=scheduler.lease, now=NOW)
    conflict = LabCommandEnvelope(
        request_id=request_id,
        command=SubmitJobCommand(job_id=uuid4(), spec=_spec(), max_attempts=3),
    )
    valid = _envelope()
    spool.publish(conflict)
    spool.publish(valid)

    result = scheduler.run_once()

    assert result.quarantined == 1
    assert result.processed == 1
    assert result.applied == 1
    assert spool.pending() == ()
    quarantine_records = tuple(spool.quarantine_dir.glob("*.bad.json"))
    assert len(quarantine_records) == 1
    assert "request_content_conflict" in quarantine_records[0].read_text(encoding="utf-8")
    assert LabJobReader(store.path).get_job(valid.command.job_id) is not None


def test_second_scheduler_is_refused_while_first_lease_is_valid(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    first = _scheduler(store, spool, owner="scheduler-a")
    second = _scheduler(store, spool, owner="scheduler-b")
    first.run_once()

    with pytest.raises(SchedulerLeaseUnavailableError):
        second.run_once()


def test_scheduler_takeover_recovers_old_running_job_to_checkpointed(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    old = store.acquire_scheduler_lease(
        owner_id="scheduler-old",
        lease_seconds=10,
        now=NOW,
    )
    envelope = _envelope()
    store.apply_command(envelope, lease=old, now=NOW)
    store.transition_job(
        envelope.command.job_id,
        expected_version=0,
        target_status=JobStatus.RUNNING,
        lease=old,
        reason="started",
        now=NOW + timedelta(seconds=1),
    )
    takeover = _scheduler(
        store,
        spool,
        owner="scheduler-new",
        now=NOW + timedelta(seconds=11),
    )

    result = takeover.run_once()
    recovered = LabJobReader(store.path).get_job(envelope.command.job_id)

    assert result.lease_acquired is True
    assert result.recovered == 1
    assert recovered is not None
    assert recovered.status is JobStatus.CHECKPOINTED


def test_subsequent_tick_renews_existing_lease(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    current = [NOW]
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        clock=lambda: current[0],
    )
    first = scheduler.run_once()
    current[0] = NOW + timedelta(seconds=20)

    second = scheduler.run_once()

    assert first.lease_acquired is True
    assert second.lease_acquired is False
    assert scheduler.lease is not None
    assert scheduler.lease.heartbeat_at == NOW + timedelta(seconds=20)
    assert len(LabJobReader(store.path).list_leases()) == 1


def test_tick_before_heartbeat_deadline_does_not_write_lease(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    current = [NOW]
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        clock=lambda: current[0],
    )
    scheduler.run_once()
    current[0] = NOW + timedelta(seconds=5)

    scheduler.run_once()

    assert scheduler.lease is not None
    assert scheduler.lease.heartbeat_at == NOW
    assert LabJobReader(store.path).list_leases()[0].heartbeat_at == NOW


def test_run_forever_stops_cooperatively_and_releases_lease(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    scheduler = _scheduler(store, spool)
    calls = 0
    original = scheduler.run_once

    def one_tick() -> SchedulerTickResult:
        nonlocal calls
        calls += 1
        result = original()
        scheduler.request_stop()
        return result

    scheduler.run_once = one_tick  # type: ignore[method-assign]
    thread = Thread(target=scheduler.run_forever)
    thread.start()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert calls == 1
    assert LabJobReader(store.path).list_leases()[-1].released_at is not None
