"""Deterministic lease timing over complete synthetic, integrity-checked job rows."""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event, Lock, Thread, current_thread

import pytest

_BASE = datetime(2030, 1, 1, tzinfo=UTC)


class _Clock:
    def __init__(self) -> None:
        self.seconds = 0
        self.lock = Lock()

    def __call__(self) -> datetime:
        with self.lock:
            return _BASE + timedelta(seconds=self.seconds)

    def set(self, value: int) -> None:
        with self.lock:
            self.seconds = value


class _BeforeArtifactsError(RuntimeError):
    """The lease boundary passed; artifact acceptance is tested elsewhere."""


@pytest.fixture(scope="module")
def spec_template(tmp_path_factory: pytest.TempPathFactory) -> object:
    from tests.unit.test_factor_stock_feature_pipeline import _configuration, _plan

    root, reference, browser, _, config, _ = _configuration(
        tmp_path_factory.mktemp("lease-source-template")
    )
    return _plan(root, reference, browser, config).spec.model_copy(
        update={"deadline": _BASE + timedelta(seconds=300)}
    )


@pytest.fixture
def setup(tmp_path: Path, spec_template: object) -> Iterator[tuple]:
    from rquant.factor.job_ledger import FactorEvaluationJobLedger

    root = tmp_path / "ledger"
    root.mkdir(mode=0o700)
    clock = _Clock()
    ledger = FactorEvaluationJobLedger(root / "jobs.sqlite", clock=clock)
    identity = ledger.initialize()
    job = ledger.submit("lease-boundary", spec_template)
    lease = ledger.claim(lease_seconds=30, job_id=job.job_id)
    assert lease.expires_at == _BASE + timedelta(seconds=30)
    yield ledger, clock, job, lease, identity
    assert not ledger._prepared


def _loaded(ledger: object, job_id: str) -> object:
    with ledger._reader() as connection:
        return ledger._load_job(connection, job_id)


def _completion(spec: object) -> object:
    from rquant.factor.stream_job_runner import FactorStreamCompletion

    sources = spec.adapter_request.formula.sources
    return FactorStreamCompletion(
        schema_version=2,
        spec_sha256=spec.spec_sha256,
        artifact_sha256="a" * 64,
        artifact_filename=f"factor-stream-full-v2-{'a' * 64}.json",
        artifact_byte_count=1,
        display_artifact_sha256="b" * 64,
        display_artifact_filename=f"factor-stream-display-v2-{'b' * 64}.json",
        display_artifact_byte_count=1,
        result_sha256="c" * 64,
        source_sha256=sources.feature_source_sha256,
        snapshot_id=sources.feature_source_id,
        binding_hash=sources.feature_source_sha256,
        snapshot_as_of_time=spec.adapter_request.formula.as_of,
        code_revision=spec.code_revision,
        completed_at=_BASE,
    )


def test_heartbeat_rejects_expiry_crossed_during_complete_decode(
    setup: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_ledger as module

    ledger, clock, job, lease, _ = setup
    original = module._decoded_job
    clock.set(20)

    def delayed(row: sqlite3.Row, **kwargs: object) -> object:
        state = original(row, **kwargs)
        if not kwargs:
            clock.set(31)
        return state

    monkeypatch.setattr(module, "_decoded_job", delayed)
    with pytest.raises(module.FactorLedgerLeaseError):
        ledger.heartbeat(job.job_id, lease.lease_token, lease.version, lease_seconds=30)
    monkeypatch.setattr(module, "_decoded_job", original)
    state = _loaded(ledger, job.job_id)
    assert state.version == lease.version
    assert state.lease_expires_at == _BASE + timedelta(seconds=30)


@pytest.mark.parametrize("start,commit_time", [(5, 36), (25, 31)])
def test_heartbeat_reports_loss_if_actual_commit_crosses_expiry(
    setup: tuple, monkeypatch: pytest.MonkeyPatch, start: int, commit_time: int
) -> None:
    from rquant.factor.job_ledger import FactorLedgerLeaseError

    ledger, clock, job, lease, _ = setup
    original = ledger._writer
    clock.set(start)

    @contextmanager
    def delayed_commit() -> Iterator[sqlite3.Connection]:
        with original() as connection:
            connection.set_trace_callback(
                lambda sql: clock.set(commit_time) if sql.strip().upper() == "COMMIT" else None
            )
            yield connection

    monkeypatch.setattr(ledger, "_writer", delayed_commit)
    with pytest.raises(FactorLedgerLeaseError):
        ledger.heartbeat(job.job_id, lease.lease_token, lease.version, lease_seconds=30)
    monkeypatch.setattr(ledger, "_writer", original)
    # The committed row stays recoverable; no forced rollback protocol is introduced.
    persisted = _loaded(ledger, job.job_id)
    assert persisted.version == 2
    assert persisted.lease_expires_at == _BASE + timedelta(seconds=start + 30)
    clock.set(start + 31)
    replacement = ledger.claim(lease_seconds=30, job_id=job.job_id)
    assert replacement.version == 3 and replacement.lease_token != lease.lease_token


@pytest.mark.parametrize("finish_time", [26, 31])
def test_completion_decode_allows_same_lease_heartbeat_to_commit(
    setup: tuple, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, finish_time: int
) -> None:
    from rquant.factor import job_ledger as module

    ledger, clock, job, lease, _ = setup
    clock.set(25)
    decoded = Event()
    commit_entered = Event()
    heartbeat_done = Event()
    original_decoder = module._decoded_job
    original_writer = ledger._writer
    outcomes: dict[str, object] = {}

    def delayed(row: sqlite3.Row, **kwargs: object) -> object:
        state = original_decoder(row, **kwargs)
        if current_thread().name == "lease-completion" and not kwargs:
            decoded.set()
            assert commit_entered.wait(5)
            # Both versions execute this delay at the complete decoder boundary;
            # only the old version still owns the reader transaction here.
            outcomes["renewal_completed_during_decode"] = heartbeat_done.wait(1)
            clock.set(finish_time)
        return state

    @contextmanager
    def traced_writer() -> Iterator[sqlite3.Connection]:
        with original_writer() as connection:
            connection.set_trace_callback(
                lambda sql: commit_entered.set() if sql.strip().upper() == "COMMIT" else None
            )
            yield connection

    def before_artifacts(*args: object) -> object:
        raise _BeforeArtifactsError

    def prepare() -> None:
        try:
            ledger.prepare_stream_completion(
                job.job_id, lease.lease_token, _completion(job.spec), tmp_path, tmp_path
            )
        except BaseException as exc:
            outcomes["prepare"] = exc

    def renew() -> None:
        try:
            outcomes["heartbeat"] = ledger.heartbeat(
                job.job_id, lease.lease_token, lease.version, lease_seconds=30
            )
        except BaseException as exc:
            outcomes["heartbeat"] = exc
        finally:
            heartbeat_done.set()

    monkeypatch.setattr(module, "_decoded_job", delayed)
    monkeypatch.setattr(ledger, "_writer", traced_writer)
    monkeypatch.setattr(module, "verify_factor_stream_artifacts", before_artifacts)
    reader = Thread(target=prepare, name="lease-completion")
    writer = Thread(target=renew, name="lease-heartbeat")
    reader.start()
    try:
        assert decoded.wait(5)
        writer.start()
    finally:
        reader.join(timeout=5.0)
        if writer.ident is not None:
            writer.join(timeout=5.0)
    assert not reader.is_alive() and not writer.is_alive()
    assert isinstance(outcomes["prepare"], _BeforeArtifactsError)
    assert not isinstance(outcomes["heartbeat"], BaseException)
    if finish_time == 31:
        assert outcomes["renewal_completed_during_decode"] is True


@pytest.mark.parametrize("finish_time", [31, 36])
def test_heartbeat_checks_trusted_time_after_public_lease_construction(
    setup: tuple, monkeypatch: pytest.MonkeyPatch, finish_time: int
) -> None:
    from rquant.factor import job_ledger as module

    ledger, clock, job, lease, _ = setup
    clock.set(5)
    original = module._JobState.lease

    def delayed(state: object) -> object:
        result = original(state)
        clock.set(finish_time)
        return result

    monkeypatch.setattr(module._JobState, "lease", delayed)
    if finish_time == 36:
        with pytest.raises(module.FactorLedgerLeaseError):
            ledger.heartbeat(job.job_id, lease.lease_token, lease.version, lease_seconds=30)
    else:
        renewed = ledger.heartbeat(job.job_id, lease.lease_token, lease.version, lease_seconds=30)
        assert renewed.expires_at == _BASE + timedelta(seconds=35)


def _rewrite_job(ledger: object, job_id: str, updates: dict[str, object]) -> None:
    from rquant.factor import job_ledger as module

    with sqlite3.connect(ledger.path) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute("SELECT * FROM factor_jobs WHERE job_id = ?", (job_id,)).fetchone()
        payload = {column: row[column] for column in module._COLUMNS}
        payload.update(updates)
        connection.execute(
            "UPDATE factor_jobs SET "
            + ", ".join(f"{column} = ?" for column in payload)
            + ", row_sha256 = ? WHERE job_id = ?",
            (*payload.values(), module._digest(payload), job_id),
        )


@pytest.mark.parametrize("operation", ["heartbeat", "prepare"])
@pytest.mark.parametrize(
    "damage",
    ["command_digest", "command_binding", "row_digest", "typed_version", "spec", "identity"],
)
def test_current_authority_rechecks_full_binding_after_detached_decode(
    setup: tuple, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, operation: str, damage: str
) -> None:
    from rquant.factor import job_ledger as module

    ledger, _, job, lease, _ = setup
    original_decoder = module._decoded_job
    original_command = module._checked_command
    captured_commands: list[str] = []
    changed = False
    with sqlite3.connect(ledger.path) as connection:
        original_digest = connection.execute("SELECT row_sha256 FROM factor_commands").fetchone()[0]

    def checked_command(row: sqlite3.Row) -> object:
        captured_commands.append(row["row_sha256"])
        return original_command(row)

    def decoded(row: sqlite3.Row, **kwargs: object) -> object:
        nonlocal changed
        state = original_decoder(row, **kwargs)
        if changed or kwargs:
            return state
        changed = True
        if damage == "identity":
            replacement = ledger.path.parent / "replacement.sqlite"
            replacement.write_bytes(ledger.path.read_bytes())
            replacement.chmod(0o600)
            os.replace(replacement, ledger.path)
        elif damage == "typed_version":
            _rewrite_job(ledger, job.job_id, {"version": 0})
        elif damage == "spec":
            alternate = job.spec.model_copy(update={"code_revision": "d" * 40})
            _rewrite_job(
                ledger,
                job.job_id,
                {
                    "spec_json": module._canonical_model(alternate),
                    "spec_sha256": alternate.spec_sha256,
                },
            )
            with sqlite3.connect(ledger.path) as connection:
                payload = module._command_payload(
                    "lease-boundary", alternate.spec_sha256, job.job_id
                )
                connection.execute(
                    "UPDATE factor_commands SET spec_sha256 = ?, row_sha256 = ?",
                    (alternate.spec_sha256, module._digest(payload)),
                )
        else:
            with sqlite3.connect(ledger.path) as connection:
                if damage == "command_digest":
                    connection.execute("UPDATE factor_commands SET row_sha256 = ?", ("0" * 64,))
                elif damage == "command_binding":
                    payload = module._command_payload("lease-boundary", "d" * 64, job.job_id)
                    connection.execute(
                        "UPDATE factor_commands SET spec_sha256 = ?, row_sha256 = ?",
                        ("d" * 64, module._digest(payload)),
                    )
                else:
                    connection.execute("UPDATE factor_jobs SET version = version + 1")
        return state

    monkeypatch.setattr(module, "_decoded_job", decoded)
    monkeypatch.setattr(module, "_checked_command", checked_command)
    error = (
        module.FactorLedgerIdentityError
        if damage == "identity"
        else module.FactorLedgerIntegrityError
    )
    with pytest.raises(error):
        if operation == "heartbeat":
            ledger.heartbeat(job.job_id, lease.lease_token, lease.version, lease_seconds=30)
        else:
            ledger.prepare_stream_completion(
                job.job_id, lease.lease_token, _completion(job.spec), tmp_path, tmp_path
            )
    # This command was sealed together with the original job before detached decoding.
    assert captured_commands[0] == original_digest
    assert changed and not ledger._prepared


@pytest.mark.parametrize("change", ["renewed_version", "new_claim", "failed", "expiry", "clock"])
def test_heartbeat_uses_fresh_lease_fencing_after_complete_decode(
    setup: tuple, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    from rquant.factor import job_ledger as module

    ledger, clock, job, lease, _ = setup
    original = module._decoded_job
    changed = False
    clock.set(25)

    def decoded(row: sqlite3.Row, **kwargs: object) -> object:
        nonlocal changed
        state = original(row, **kwargs)
        if changed or kwargs:
            return state
        changed = True
        if change == "renewed_version":
            ledger.heartbeat(job.job_id, lease.lease_token, lease.version, lease_seconds=30)
        elif change == "new_claim":
            clock.set(31)
            replacement = ledger.claim(lease_seconds=30, job_id=job.job_id)
            assert replacement.lease_token != lease.lease_token
        elif change == "failed":
            ledger.fail(job.job_id, lease.lease_token, lease.version, "evaluation_failed")
        else:
            clock.set(31 if change == "expiry" else -1)
        return state

    monkeypatch.setattr(module, "_decoded_job", decoded)
    with pytest.raises(module.FactorLedgerLeaseError):
        ledger.heartbeat(job.job_id, lease.lease_token, lease.version, lease_seconds=30)
    assert changed


@pytest.mark.parametrize("change", ["renew", "reclaim", "command"])
def test_completion_second_read_checks_current_authority_after_verified_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    from rquant.factor import job_ledger as module
    from tests.unit.test_factor_stream_job_authority import _sealed

    with _sealed(tmp_path) as (ledger, clock, claim, completion, root, members, *_):
        original = module.verify_factor_stream_artifacts
        initial = clock.instant
        renewals: list[object] = []

        def verified(*args: object) -> object:
            result = original(*args)
            if change == "renew":
                clock.instant = initial + timedelta(seconds=25)
                renewals.append(
                    ledger.heartbeat(claim.job.job_id, claim.lease_token, claim.version, 30)
                )
                clock.instant = initial + timedelta(seconds=31)
            elif change == "reclaim":
                clock.instant = initial + timedelta(seconds=31)
                replacement = ledger.claim(lease_seconds=30, job_id=claim.job.job_id)
                assert replacement.lease_token != claim.lease_token
            else:
                with sqlite3.connect(ledger.path) as connection:
                    connection.execute("UPDATE factor_commands SET row_sha256 = ?", ("0" * 64,))
            return result

        monkeypatch.setattr(module, "verify_factor_stream_artifacts", verified)
        if change == "renew":
            handle = ledger.prepare_stream_completion(
                claim.job.job_id, claim.lease_token, completion, root, members
            )
            try:
                record = ledger.complete_prepared_stream(
                    claim.job.job_id, claim.lease_token, renewals[0].version, handle
                )
                assert record.status == "succeeded"
            finally:
                ledger.discard_prepared_stream(handle)
        else:
            error = (
                module.FactorLedgerLeaseError
                if change == "reclaim"
                else module.FactorLedgerIntegrityError
            )
            with pytest.raises(error):
                ledger.prepare_stream_completion(
                    claim.job.job_id, claim.lease_token, completion, root, members
                )
        assert not ledger._prepared
