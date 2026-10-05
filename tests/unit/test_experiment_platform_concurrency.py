from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from threading import Barrier
from uuid import UUID

from rquant.experiment_platform import ExperimentFamilyRecord, ExperimentNote, ExperimentOuterGrant
from tests.unit.test_experiment_platform import NOW, prepared_family
from tests.unit.test_experiment_platform_results import complete_family as complete_family


def test_exp05_two_actual_registrations_commit_one_complete_original_family(tmp_path: Path) -> None:
    store, family, children, _ = prepared_family(tmp_path)
    start = Barrier(2)

    def register() -> ExperimentFamilyRecord:
        start.wait()
        return store.register_family_submission(
            owner=family.owner, request_id=family.request_id, children=children
        )

    with ThreadPoolExecutor(max_workers=2) as workers:
        first, second = tuple(workers.submit(register) for _ in range(2))
        results = (first.result(), second.result())
    assert results[0] == results[1] and results[0].state == "ready"
    assert len(store.registry.list_family_attempts(family.family_id)) == len(children) == 4
    assert {intent.job_id for intent in store.registry.list_pending_submissions()} == {
        child.intent.job_id for child in children
    }


def test_exp12_exp23_two_actual_outer_admissions_and_note_cas_have_one_winner(
    complete_family,
) -> None:
    store, projection, _, _ = complete_family
    snapshot = projection.snapshot(NOW + timedelta(seconds=3))
    family, fact = snapshot.families[0], snapshot.attempts[0]
    barrier = Barrier(2)

    def admit(index: int) -> ExperimentOuterGrant | str:
        barrier.wait()
        try:
            return store.admit_outer(
                owner="alice",
                family_id=family.family_id,
                request_id=UUID(int=1100 + index),
                experiment_id=fact.attempt.spec.experiment_id,
                now=NOW + timedelta(seconds=4),
                body_hash=str(index) * 64,
                result_hash=fact.result_hash,
                source_identity=fact.source_identity,
                expected_policy_version=store.policy().version,
                cutoff=family.request.protocol.frozen_outer_test_range.end_date,
            )
        except ValueError as error:
            assert "overlap" in str(error)
            return "overlap_rejected"

    with ThreadPoolExecutor(max_workers=2) as workers:
        results = [future.result() for future in [workers.submit(admit, i) for i in (1, 2)]]
    winners = [value for value in results if isinstance(value, ExperimentOuterGrant)]
    assert len(winners) == 1 and results.count("overlap_rejected") == 1
    assert store.list_outer_grants("alice") == tuple(winners)
    # Both threads use the same original CAS version. There is no lost update.
    barrier = Barrier(2)

    def note(index: int) -> ExperimentNote | str:
        barrier.wait()
        try:
            return store.set_note(
                owner="alice",
                family_id=family.family_id,
                request_id=UUID(int=1110 + index),
                expected_version=0,
                text=f"合成并发备注{index}",
                now=NOW + timedelta(seconds=4),
            )
        except ValueError as error:
            assert "version" in str(error)
            return "version_rejected"

    with ThreadPoolExecutor(max_workers=2) as workers:
        notes = [future.result() for future in [workers.submit(note, i) for i in (1, 2)]]
    saved = [value for value in notes if isinstance(value, ExperimentNote)]
    assert len(saved) == 1 and saved[0].version == 1 and notes.count("version_rejected") == 1
    print(
        "PRIVATE_CONCURRENT_FACT",
        {
            "family_ready": 1,
            "outer_grants": 1,
            "note_version": 1,
            "worker_threads_joined": True,
            "synthetic_market": True,
        },
    )
