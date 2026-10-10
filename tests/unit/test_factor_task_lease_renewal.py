"""Claim-scoped immutable reuse over real SQLite lease and command authority."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event, Lock, Thread, current_thread
from threading import enumerate as threads

import pytest

_CLAIM_COST = 11.877946
_WAIT_COST = 5.089033
_FULL_DECODE_COST = 12.253724845
_REUSED_DECODE_COST = 0.401322759
_WRITER_COST = 0.160574
_STORE_COST = 0.376749396


class _Clock:
    def __init__(self, base: datetime) -> None:
        self.base, self.seconds, self.lock = base, 0.0, Lock()

    def __call__(self) -> datetime:
        with self.lock:
            return self.base + timedelta(seconds=self.seconds)

    def advance(self, seconds: float) -> None:
        with self.lock:
            self.seconds += seconds


def _persisted(ledger: object, job_id: str) -> dict:
    with closing(sqlite3.connect(f"{ledger.path.as_uri()}?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute("SELECT * FROM factor_jobs WHERE job_id=?", (job_id,)).fetchone()
        return {k: v for k, v in dict(row).items() if k not in ("lease_token", "spec_json")}


def _ledger(tmp_path: Path, spec: object) -> tuple:
    from rquant.factor.job_ledger import FactorEvaluationJobLedger
    from tests.unit.test_factor_job_ledger import NOW

    root = tmp_path / "ledger"
    root.mkdir(mode=0o700)
    clock = _Clock(NOW)
    ledger = FactorEvaluationJobLedger(root / "jobs.sqlite", clock=clock)
    ledger.initialize()
    job = ledger.submit("claim-scoped-renewal", spec)
    return ledger, clock, job


@pytest.fixture(scope="module")
def large_task(tmp_path_factory: pytest.TempPathFactory) -> object:
    from rquant.factor.stream_job_spec import decode_factor_job_spec_json
    from rquant.strict_json import canonical_json_bytes
    from tests.unit.test_factor_minute_feature_descriptor import _minute_shape, _minute_spec
    from tests.unit.test_factor_stock_feature_descriptor import (
        _shape_context,
        _shape_source,
        context_template,
        descriptor_template,
    )

    source, spec, *_ = descriptor_template.__wrapped__(tmp_path_factory)
    context = context_template.__wrapped__(tmp_path_factory)
    source = _minute_shape(_shape_source(source, 5571, days=6, longest=True))
    spec = _minute_spec(spec, source, _shape_context(source, *context))
    data = canonical_json_bytes(spec.model_dump(mode="json", round_trip=True))
    assert len(data) < 2 * 1024 * 1024
    checked = decode_factor_job_spec_json(data.decode())
    assert checked == spec
    if target := os.environ.get("RQUANT_LEASE_REPAIR_EVIDENCE"):
        (Path(target) / "large-task.json").write_text(
            json.dumps(
                {
                    "codes": 5571,
                    "fields": 50,
                    "days": 6,
                    "industry_and_size": True,
                    "bytes": len(data),
                    "canonical_sha256": hashlib.sha256(data).hexdigest(),
                    "spec_sha256": checked.spec_sha256,
                },
                indent=2,
            )
            + "\n"
        )
    return checked


class _RunnerStoppedError(RuntimeError):
    pass


def test_default_worker_large_task_first_renewal_retains_durable_authority(
    tmp_path: Path, large_task: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_ledger as module
    from rquant.factor import job_worker as worker

    # Explicit operation costs replay the observed first-renewal boundary, not wall time.
    root = tmp_path / "ledger"
    root.mkdir(mode=0o700)
    clock = _Clock(large_task.adapter_request.formula.as_of)
    ledger = module.FactorEvaluationJobLedger(root / "jobs.sqlite", clock=clock)
    ledger.initialize()
    job = ledger.submit("large-first-renewal", large_task)
    finished = Event()
    events: list[dict] = []
    phase = {"name": "claim"}
    decode, store, writer, heartbeat = (
        module._decoded_job,
        ledger._store_job,
        ledger._writer,
        ledger.heartbeat,
    )

    def timed_decode(row: sqlite3.Row, **kwargs: object) -> object:
        state = decode(row, **kwargs)
        if phase["name"] == "heartbeat":
            cost = (
                _REUSED_DECODE_COST
                if kwargs.get("verified_spec") is not None
                else _FULL_DECODE_COST
            )
            clock.advance(cost)
        return state

    def timed_store(connection: sqlite3.Connection, state: object) -> None:
        store(connection, state)
        if phase["name"] == "claim":
            clock.advance(_CLAIM_COST)
        elif phase["name"] == "heartbeat":
            clock.advance(_STORE_COST)

    @contextmanager
    def timed_writer() -> Iterator[sqlite3.Connection]:
        with writer() as connection:
            if phase["name"] == "heartbeat":
                clock.advance(_WRITER_COST)
            yield connection

    def first_heartbeat(*args: object, **kwargs: object) -> object:
        clock.advance(_WAIT_COST)
        phase["name"] = "heartbeat"
        try:
            renewed = heartbeat(*args, **kwargs)
            persisted = _persisted(ledger, job.job_id)
            events.append(
                {
                    "clock_seconds": clock.seconds,
                    "version": renewed.version,
                    "lease_expires_at": persisted["lease_expires_at"],
                    "updated_at": persisted["updated_at"],
                    "row_sha256": persisted["row_sha256"],
                }
            )
            assert renewed.version == persisted["version"] == 2
            assert renewed.expires_at > clock()
            return renewed
        finally:
            phase["name"] = "done"
            finished.set()

    def stop_compute(*_args: object, **_kwargs: object) -> object:
        assert finished.wait(20), "first default 5s heartbeat did not finish"
        raise _RunnerStoppedError("synthetic compute stops after first renewal")

    monkeypatch.setattr(module, "_decoded_job", timed_decode)
    monkeypatch.setattr(ledger, "_store_job", timed_store)
    monkeypatch.setattr(ledger, "_writer", timed_writer)
    monkeypatch.setattr(ledger, "heartbeat", first_heartbeat)
    monkeypatch.setattr(worker, "run_factor_stream_job", stop_compute)
    outcome = worker.run_one_factor_job(
        ledger,
        metadata_store=object(),
        lake_root=tmp_path,
        artifact_root=tmp_path,
        member_root=tmp_path,
        runner_now=clock,
        job_id=job.job_id,
    )
    if target := os.environ.get("RQUANT_LEASE_REPAIR_EVIDENCE"):
        (Path(target) / "worker-renewal-events.json").write_text(
            json.dumps(events, indent=2) + "\n"
        )
    assert outcome.status == "failed", "valid first renewal lost the legitimate task lease"
    assert outcome.record.failure_code == "evaluation_failed"
    assert _persisted(ledger, job.job_id)["status"] == "failed"
    assert finished.is_set() and not ledger._prepared
    assert not any(t.name == "factor-job-heartbeat" for t in threads())


@pytest.fixture(scope="module")
def small_task(tmp_path_factory: pytest.TempPathFactory) -> tuple:
    from tests.unit.test_factor_job_ledger import _sealed

    return _sealed(tmp_path_factory.mktemp("claim-scope-artifacts"))


@pytest.mark.parametrize(
    "guard",
    [
        "expired",
        "token",
        "version",
        "bool_version",
        "deadline",
        "clock_backwards",
        "queued",
        "failed",
        "succeeded",
        "reclaimed",
    ],
)
def test_claim_reuse_never_substitutes_for_current_lease_authority(
    tmp_path: Path, small_task: tuple, guard: str
) -> None:
    from rquant.factor import job_ledger as module

    spec, artifacts, completion = small_task
    if guard == "deadline":
        spec = spec.model_copy(update={"deadline": completion.completed_at + timedelta(seconds=20)})
    ledger, clock, job = _ledger(tmp_path, spec)
    lease = ledger.claim(lease_seconds=30, job_id=job.job_id)
    token, version = lease.lease_token, lease.version
    with ledger._reuse_claimed_spec(lease):
        if guard == "expired":
            clock.advance(31)
        elif guard == "token":
            token = ("1" if token[0] != "1" else "2") + token[1:]
        elif guard == "version":
            version += 1
        elif guard == "bool_version":
            version = True
        elif guard == "deadline":
            clock.advance(20)
        elif guard == "clock_backwards":
            clock.advance(-1)
        elif guard == "queued":
            with ledger._writer() as connection:
                state = ledger._load_job(connection, job.job_id)
                queued = state.model_copy(
                    update={
                        "status": "queued",
                        "version": 0,
                        "attempts": 0,
                        "lease_token": None,
                        "lease_expires_at": None,
                    }
                )
                ledger._store_job(connection, module._JobState.model_validate(queued.model_dump()))
        elif guard == "failed":
            ledger.fail(job.job_id, token, version, "evaluation_failed")
        elif guard == "succeeded":
            ledger.complete(job.job_id, token, version, completion, artifacts)
        elif guard == "reclaimed":
            clock.advance(31)
            fresh = ledger.claim(lease_seconds=30, job_id=job.job_id)
            assert fresh.lease_token != token
            version = fresh.version
        before = _persisted(ledger, job.job_id)
        with pytest.raises(module.FactorLedgerLeaseError):
            ledger.heartbeat(job.job_id, token, version, 30)
        assert _persisted(ledger, job.job_id) == before
    assert not hasattr(ledger._claim_spec, "value") and not ledger._prepared


@pytest.mark.parametrize(
    "change", ["spec_whitespace", "row_sha", "command_sha", "valid_alternate_spec"]
)
def test_claim_reuse_requires_exact_current_spec_row_and_command(
    tmp_path: Path, small_task: tuple, change: str
) -> None:
    from rquant.factor import job_ledger as module

    spec, _, _ = small_task
    ledger, _, job = _ledger(tmp_path, spec)
    lease = ledger.claim(lease_seconds=30, job_id=job.job_id)
    with closing(sqlite3.connect(ledger.path)) as connection:
        connection.row_factory = sqlite3.Row
        if change == "row_sha":
            connection.execute("UPDATE factor_jobs SET row_sha256=?", ("0" * 64,))
        elif change == "command_sha":
            connection.execute("UPDATE factor_commands SET row_sha256=?", ("0" * 64,))
        else:
            row = connection.execute("SELECT * FROM factor_jobs").fetchone()
            payload = {column: row[column] for column in module._COLUMNS}
            if change == "spec_whitespace":
                payload["spec_json"] += " "
            else:
                alternate = spec.model_copy(update={"code_revision": "d" * 40})
                payload.update(
                    spec_json=module._canonical_model(alternate), spec_sha256=alternate.spec_sha256
                )
                command = module._command_payload(
                    "claim-scoped-renewal", alternate.spec_sha256, job.job_id
                )
                connection.execute(
                    "UPDATE factor_commands SET spec_sha256=?,row_sha256=?",
                    (alternate.spec_sha256, module._digest(command)),
                )
            connection.execute(
                "UPDATE factor_jobs SET spec_json=?,spec_sha256=?,row_sha256=?",
                (payload["spec_json"], payload["spec_sha256"], module._digest(payload)),
            )
        connection.commit()
    before = _persisted(ledger, job.job_id)
    with ledger._reuse_claimed_spec(lease), pytest.raises(module.FactorLedgerIntegrityError):
        ledger.heartbeat(job.job_id, lease.lease_token, lease.version, 30)
    assert _persisted(ledger, job.job_id) == before
    assert not hasattr(ledger._claim_spec, "value") and not ledger._prepared


def test_claim_scope_is_local_to_ledger_job_token_and_thread(
    tmp_path: Path, small_task: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_ledger as module

    spec, _, _ = small_task
    ledger, _, job = _ledger(tmp_path, spec)
    lease = ledger.claim(lease_seconds=30, job_id=job.job_id)
    other = ledger.submit("another-job", spec.model_copy(update={"code_revision": "d" * 40}))
    other_lease = ledger.claim(lease_seconds=30, job_id=other.job_id)
    second = tmp_path / "second"
    second.mkdir(mode=0o700)
    other_ledger, _, separate = _ledger(second, other.spec)
    separate_lease = other_ledger.claim(lease_seconds=30, job_id=separate.job_id)
    original = module._decoded_job
    cold_reads: list[tuple[str, str]] = []
    results: list[object] = []

    def observed(row: sqlite3.Row, **kwargs: object) -> object:
        state = original(row, **kwargs)
        if not kwargs:
            cold_reads.append((state.job_id, current_thread().name))
        return state

    def standalone() -> None:
        assert not hasattr(ledger._claim_spec, "value")
        results.append(ledger.heartbeat(job.job_id, lease.lease_token, lease.version, 30))

    monkeypatch.setattr(module, "_decoded_job", observed)
    with ledger._reuse_claimed_spec(lease):
        renewed = ledger.heartbeat(other.job_id, other_lease.lease_token, other_lease.version, 30)
        assert renewed.version == 2 and _persisted(ledger, other.job_id)["version"] == 2
        distinct = other_ledger.heartbeat(
            separate.job_id, separate_lease.lease_token, separate_lease.version, 30
        )
        assert distinct.version == 2
        thread = Thread(target=standalone, name="standalone-renewal")
        thread.start()
        thread.join(timeout=5)
        assert not thread.is_alive() and len(results) == 1
        assert (job.job_id, "standalone-renewal") in cold_reads
        assert (other.job_id, current_thread().name) in cold_reads
        assert (separate.job_id, current_thread().name) in cold_reads
    assert not hasattr(ledger._claim_spec, "value")
    # Public direct calls outside the worker scope still fully decode their current task.
    renewed = ledger.heartbeat(job.job_id, results[0].lease_token, results[0].version, 30)
    assert renewed.version == _persisted(ledger, job.job_id)["version"] == 3


def test_worker_claim_scope_closes_after_controlled_join_timeout(
    tmp_path: Path, small_task: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_worker as worker

    spec, artifacts, completion = small_task
    ledger, clock, job = _ledger(tmp_path, spec)
    entered, release, exited = Event(), Event(), Event()
    original_heartbeat, original_scope = ledger.heartbeat, ledger._reuse_claimed_spec
    observed: list[bool] = []
    active_thread: list[Thread] = []

    @contextmanager
    def observed_scope(claim: object) -> Iterator[None]:
        active_thread.append(current_thread())
        try:
            with original_scope(claim):
                yield
        finally:
            observed.append(hasattr(ledger._claim_spec, "value"))
            exited.set()

    def blocked(*args: object, **kwargs: object) -> object:
        entered.set()
        assert release.wait(5)
        return original_heartbeat(*args, **kwargs)

    def compute(*_args: object, **_kwargs: object) -> object:
        assert entered.wait(5)
        return completion

    monkeypatch.setattr(ledger, "_reuse_claimed_spec", observed_scope)
    monkeypatch.setattr(ledger, "heartbeat", blocked)
    monkeypatch.setattr(worker, "run_factor_evaluation_job", compute)
    try:
        outcome = worker.run_one_factor_job(
            ledger,
            metadata_store=object(),
            lake_root=tmp_path,
            artifact_root=artifacts,
            runner_now=clock,
            job_id=job.job_id,
            heartbeat_interval_seconds=0.01,
            heartbeat_join_timeout_seconds=0.01,
        )
        assert outcome.status == "lease_lost"
        assert _persisted(ledger, job.job_id)["status"] == "running"
    finally:
        release.set()
        assert exited.wait(5)
        active_thread[0].join(timeout=5)
        assert not active_thread[0].is_alive()
    assert observed == [False] and not ledger._prepared
