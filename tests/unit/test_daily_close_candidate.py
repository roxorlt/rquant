from __future__ import annotations

import multiprocessing as mp
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from queue import Empty
from typing import Any

import pytest

from rquant.daily_close_candidate import (
    DailyCandidateHmacSigner,
    DailyCloseCandidateError,
    DailyCloseCandidateStore,
)
from rquant.daily_close_validation import DailyCloseValidator, VerifiedDailyCloseBatch
from rquant.daily_pipeline_ledger import DailyPipelineLedgerError, DailyStageAttempt
from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool
from tests.unit.test_daily_close_validation import (
    AVAILABLE_AT,
    OBSERVED_AT,
    TRADE_DATE,
    _calendar,
    _policy,
    _published,
    _snapshot,
)

CANDIDATE_PUBLISHED_AT = AVAILABLE_AT + timedelta(seconds=1)


class _CurrentFence:
    def assert_current(self, checked_at: datetime) -> None:
        assert _candidate_attempt().claimed_at <= checked_at < _candidate_attempt().lease_expires_at

    def assert_source(
        self,
        _source_generation_id: str,
        _source_content_hash: str,
    ) -> None:
        return None

    def assert_input(self, _input_identity: str) -> None:
        return None


def _candidate_attempt() -> DailyStageAttempt:
    return DailyStageAttempt(
        run_id="daily-candidate-test",
        stage_id="validate",
        attempt_number=1,
        fencing_token=1,
        claimed_at=datetime(2026, 7, 31, 9, 5, tzinfo=UTC),
        lease_expires_at=datetime(2026, 7, 31, 9, 20, tzinfo=UTC),
    )


@contextmanager
def _active_fence(
    attempt: DailyStageAttempt,
    checked_at: datetime,
) -> Iterator[_CurrentFence]:
    assert attempt == _candidate_attempt()
    assert attempt.claimed_at <= checked_at < attempt.lease_expires_at
    yield _CurrentFence()


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
        published_at=CANDIDATE_PUBLISHED_AT,
        fence_guard=_active_fence,
    )


def _signer() -> DailyCandidateHmacSigner:
    return DailyCandidateHmacSigner(
        key_id="daily-candidate-test",
        secret=b"daily-candidate-test-secret-32-bytes",
    )


def _verified(tmp_path: Path, *, snapshots: list[dict[str, object]] | None = None):
    gateway, record = _published(tmp_path, snapshots)
    verified = DailyCloseValidator(
        spool=gateway.spool,
        policy=_policy(),
        calendar=_calendar(),
    ).validate(record)
    return gateway, verified


def _concurrent_candidate_worker(
    raw_root: str,
    candidate_root: str,
    barrier: Any,
    results: Any,
) -> None:
    try:
        spool = LiveBatchSpool(Path(raw_root))
        record = spool.list_after(LiveChannel.DAILY_CLOSE, sequence=-1)[-1]
        verified = DailyCloseValidator(
            spool=spool,
            policy=_policy(),
            calendar=_calendar(),
        ).validate(record)
        store = DailyCloseCandidateStore(Path(candidate_root), signer=_signer())
        barrier.wait(timeout=5)
        candidate = _publish_candidate(store, verified, spool=spool)
        results.put(candidate.generation_id)
    except BaseException as exc:
        results.put(f"{type(exc).__name__}:{exc}")


def test_candidate_is_private_signed_content_addressed_and_idempotent(tmp_path: Path) -> None:
    gateway, verified = _verified(tmp_path)
    store = DailyCloseCandidateStore(tmp_path / "candidates", signer=_signer())

    first = _publish_candidate(store, verified, spool=gateway.spool)
    replay = _publish_candidate(store, verified, spool=gateway.spool)
    loaded = store.load_current(TRADE_DATE)

    assert replay == first
    assert loaded == first
    assert first.manifest.generation_id == first.generation_id
    assert first.manifest.raw_content_sha256 == verified.raw_content_sha256
    assert first.manifest.validation_sha256 == verified.validation_sha256
    assert first.manifest.source_generation_id == verified.source_generation_id
    assert first.manifest.source_sequence == verified.source_sequence
    assert first.manifest.source_batch_id == verified.source_batch_id
    assert first.manifest.available_at == AVAILABLE_AT
    assert first.manifest.signature
    assert first.path.name == first.generation_id
    assert stat.S_IMODE(first.path.stat().st_mode) == 0o700
    assert stat.S_IMODE((first.path / "facts.json").stat().st_mode) == 0o600
    assert stat.S_IMODE((first.path / "manifest.json").stat().st_mode) == 0o600


def test_candidate_rejects_validation_after_source_current_advances(tmp_path: Path) -> None:
    gateway, verified = _verified(
        tmp_path,
        snapshots=[_snapshot(), _snapshot(close=10.3)],
    )
    gateway.capture_once(
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT + timedelta(seconds=1),
        refresh=True,
    )

    with pytest.raises(DailyCloseCandidateError, match="source current"):
        DailyCloseCandidateStore(
            tmp_path / "candidates",
            signer=_signer(),
        ).publish(
            verified,
            spool=gateway.spool,
            attempt=_candidate_attempt(),
            published_at=CANDIDATE_PUBLISHED_AT,
            fence_guard=_active_fence,
        )


def test_revision_creates_new_generation_and_retains_diff_evidence(tmp_path: Path) -> None:
    gateway, first_verified = _verified(
        tmp_path,
        snapshots=[_snapshot(), _snapshot(close=10.3)],
    )
    store = DailyCloseCandidateStore(tmp_path / "candidates", signer=_signer())
    first = _publish_candidate(store, first_verified, spool=gateway.spool)
    gateway.capture_once(
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT + timedelta(seconds=1),
        refresh=True,
    )
    revised_record = gateway.spool.list_after(
        LiveChannel.DAILY_CLOSE,
        sequence=first_verified.source_sequence,
    )[0]
    revised_verified = DailyCloseValidator(
        spool=gateway.spool,
        policy=_policy(),
        calendar=_calendar(),
    ).validate(revised_record)

    revised = _publish_candidate(store, revised_verified, spool=gateway.spool)

    assert revised.generation_id != first.generation_id
    assert revised.manifest.revision == 2
    assert revised.manifest.parent_generation_id == first.generation_id
    assert revised.manifest.diff.changed_datasets == ("daily_bar",)
    assert revised.manifest.diff.changed_row_count == 1
    assert first.path.is_dir()
    assert store.load_generation(first.generation_id) == first
    assert store.load_current(TRADE_DATE) == revised


def test_candidate_recovers_after_generation_before_current_and_rejects_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway, verified = _verified(tmp_path)
    signer = _signer()
    store = DailyCloseCandidateStore(tmp_path / "candidates", signer=signer)

    class SimulatedHardExit(BaseException):
        pass

    def fail_current(*_args: object, **_kwargs: object) -> None:
        raise SimulatedHardExit

    monkeypatch.setattr(store, "_write_current", fail_current)
    with pytest.raises(SimulatedHardExit):
        _publish_candidate(store, verified, spool=gateway.spool)

    restarted = DailyCloseCandidateStore(tmp_path / "candidates", signer=signer)
    recovered = _publish_candidate(restarted, verified, spool=gateway.spool)
    assert restarted.load_current(TRADE_DATE) == recovered

    facts_path = recovered.path / "facts.json"
    facts_path.write_bytes(facts_path.read_bytes() + b"\n")
    with pytest.raises(DailyCloseCandidateError, match="facts"):
        restarted.load_generation(recovered.generation_id)


def test_candidate_does_not_write_when_the_stage_fence_is_stale(tmp_path: Path) -> None:
    gateway, verified = _verified(tmp_path)
    store = DailyCloseCandidateStore(tmp_path / "candidates", signer=_signer())

    @contextmanager
    def stale_fence(
        _attempt: DailyStageAttempt,
        _checked_at: datetime,
    ) -> Iterator[None]:
        raise DailyPipelineLedgerError("daily stage fencing token is stale")
        yield

    with pytest.raises(DailyCloseCandidateError, match="fenc"):
        store.publish(
            verified,
            spool=gateway.spool,
            attempt=_candidate_attempt(),
            published_at=CANDIDATE_PUBLISHED_AT,
            fence_guard=stale_fence,
        )

    assert not list(store.generations_root.iterdir())
    assert not list(store.current_root.iterdir())


def test_candidate_requires_the_fence_to_bind_its_exact_raw_source(tmp_path: Path) -> None:
    gateway, verified = _verified(tmp_path)
    store = DailyCloseCandidateStore(tmp_path / "candidates", signer=_signer())

    class WrongSourceFence:
        def assert_current(self, _checked_at: datetime) -> None:
            return None

        def assert_source(
            self,
            _source_generation_id: str,
            _source_content_hash: str,
        ) -> None:
            raise DailyPipelineLedgerError("daily stage source identity is stale")

    @contextmanager
    def wrong_source_guard(
        _attempt: DailyStageAttempt,
        _checked_at: datetime,
    ) -> Iterator[WrongSourceFence]:
        yield WrongSourceFence()

    with pytest.raises(DailyCloseCandidateError, match="source identity"):
        store.publish(
            verified,
            spool=gateway.spool,
            attempt=_candidate_attempt(),
            published_at=CANDIDATE_PUBLISHED_AT,
            fence_guard=wrong_source_guard,
        )

    assert not list(store.generations_root.iterdir())


def test_candidate_does_not_advance_current_when_raw_revises_during_generation_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway, verified = _verified(
        tmp_path,
        snapshots=[_snapshot(), _snapshot(close=10.3)],
    )
    store = DailyCloseCandidateStore(tmp_path / "candidates", signer=_signer())
    persist_generation = store._persist_generation

    def persist_then_refresh(*args: object, **kwargs: object) -> None:
        persist_generation(*args, **kwargs)
        gateway.capture_once(
            trade_date=TRADE_DATE,
            observed_at=OBSERVED_AT + timedelta(seconds=1),
            refresh=True,
        )

    monkeypatch.setattr(store, "_persist_generation", persist_then_refresh)

    with pytest.raises(DailyCloseCandidateError, match="source current"):
        _publish_candidate(store, verified, spool=gateway.spool)

    assert not list(store.current_root.iterdir())
    assert len(list(store.generations_root.iterdir())) == 1


def test_two_processes_publish_the_same_candidate_as_one_generation(tmp_path: Path) -> None:
    gateway, _verified_batch = _verified(tmp_path / "raw")
    candidate_root = tmp_path / "candidates"
    context = mp.get_context("spawn")
    barrier = context.Barrier(2)
    results = context.Queue()
    processes = [
        context.Process(
            target=_concurrent_candidate_worker,
            args=(str(gateway.spool.root), str(candidate_root), barrier, results),
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
            pytest.fail("candidate worker timed out")

    observed: list[str] = []
    try:
        for _ in processes:
            observed.append(results.get(timeout=2))
    except Empty:
        pytest.fail("candidate worker did not report a result")

    assert [process.exitcode for process in processes] == [0, 0]
    assert all("Error:" not in item for item in observed)
    assert len(set(observed)) == 1
    store = DailyCloseCandidateStore(candidate_root, signer=_signer())
    assert len(list(store.generations_root.iterdir())) == 1
    assert store.load_current(TRADE_DATE).generation_id == observed[0]
