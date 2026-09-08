"""#248 shape (1): the runner database our own previous generation wrote.

`live/strategies/<svc>/runner.sqlite3` records the strategy spec fingerprint and the
evaluator contract fingerprint of the generation that created it. Both include the
producer commit, so a release changes both, and on 2026-09-09 all three strategy units
exited after ~2 minutes with `strategy spec does not match persisted runner identity`,
looped on `Restart=`, and pushed three real alerts before the window moved the databases
aside by hand.

Fail-closed is right for a database somebody else wrote. It is wrong for the one this
service's own previous generation wrote, which is the state of every release. So the
store archives that database under the previous generation's id and creates a fresh one
with the current identity; everything else — a foreign fingerprint pair, a pair split
across two generations, a corrupt file — refuses exactly as before.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from rquant.strategy_runner import StrategyRunnerStore
from tests.unit.test_strategy_runner import EVALUATOR_FINGERPRINT, _spec

PREVIOUS_COMMIT = "1" * 40
CURRENT_COMMIT = "2" * 40
FOREIGN_COMMIT = "3" * 40
PREVIOUS_EVALUATOR = "a" * 64


def _previous_generation(mapping: dict[tuple[str, str], str]) -> object:
    def resolve(spec_fingerprint: str, evaluator_fingerprint: str) -> str | None:
        return mapping.get((spec_fingerprint, evaluator_fingerprint))

    return resolve


def _write_previous_runner(path: Path, *, commit: str, evaluator: str) -> str:
    """Create the database exactly the way the previous generation's role created it."""

    spec = _spec(producer_commit=commit)
    StrategyRunnerStore(path, spec=spec, evaluator_contract_fingerprint=evaluator)
    return spec.spec_fingerprint


def _persisted_identity(path: Path) -> tuple[str, str]:
    connection = sqlite3.connect(path)
    try:
        row = connection.execute(
            "SELECT strategy_spec_fingerprint, evaluator_contract_fingerprint "
            "FROM runner_metadata WHERE singleton = 1"
        ).fetchone()
    finally:
        connection.close()
    return str(row[0]), str(row[1])


def test_a_runner_from_our_own_previous_generation_is_archived_and_recreated(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runner.sqlite3"
    previous_fingerprint = _write_previous_runner(
        path,
        commit=PREVIOUS_COMMIT,
        evaluator=PREVIOUS_EVALUATOR,
    )
    #: the previous generation's store leaves both sidecars behind (journal_mode = WAL)
    assert (path.parent / "runner.sqlite3-wal").is_file()

    current = _spec(producer_commit=CURRENT_COMMIT)
    store = StrategyRunnerStore(
        path,
        spec=current,
        evaluator_contract_fingerprint=EVALUATOR_FINGERPRINT,
        previous_generation_of_identity=_previous_generation(
            {(previous_fingerprint, PREVIOUS_EVALUATOR): "f" * 64}
        ),
    )

    archived = path.parent / f"runner.sqlite3.{'f' * 64}.archived"
    assert archived.is_file()
    assert (path.parent / f"runner.sqlite3.{'f' * 64}.archived-wal").is_file()
    assert (path.parent / f"runner.sqlite3.{'f' * 64}.archived-shm").is_file()
    assert _persisted_identity(archived) == (previous_fingerprint, PREVIOUS_EVALUATOR)
    assert _persisted_identity(path) == (current.spec_fingerprint, EVALUATOR_FINGERPRINT)
    assert store.identity_rotation is not None
    assert store.identity_rotation.previous_generation_id == "f" * 64
    assert store.identity_rotation.archived_path == archived
    assert store.identity_rotation.event == f"runner_identity_rotated:{'f' * 64}"


def test_a_foreign_runner_identity_still_refuses(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    _write_previous_runner(path, commit=FOREIGN_COMMIT, evaluator=PREVIOUS_EVALUATOR)

    with pytest.raises(ValueError, match="strategy spec does not match persisted runner identity"):
        StrategyRunnerStore(
            path,
            spec=_spec(producer_commit=CURRENT_COMMIT),
            evaluator_contract_fingerprint=EVALUATOR_FINGERPRINT,
            previous_generation_of_identity=_previous_generation({}),
        )
    assert not any(item.name.endswith(".archived") for item in path.parent.iterdir())


def test_a_fingerprint_pair_split_across_two_generations_still_refuses(
    tmp_path: Path,
) -> None:
    """Both halves have to come from the same previous generation, not from two."""

    path = tmp_path / "runner.sqlite3"
    previous_fingerprint = _write_previous_runner(
        path,
        commit=PREVIOUS_COMMIT,
        evaluator=PREVIOUS_EVALUATOR,
    )

    with pytest.raises(ValueError, match="strategy spec does not match persisted runner identity"):
        StrategyRunnerStore(
            path,
            spec=_spec(producer_commit=CURRENT_COMMIT),
            evaluator_contract_fingerprint=EVALUATOR_FINGERPRINT,
            #: the spec fingerprint is a previous generation's, the evaluator one is not
            previous_generation_of_identity=_previous_generation(
                {(previous_fingerprint, EVALUATOR_FINGERPRINT): "f" * 64}
            ),
        )


def test_without_a_lineage_the_refusal_is_unchanged(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    _write_previous_runner(path, commit=PREVIOUS_COMMIT, evaluator=PREVIOUS_EVALUATOR)

    with pytest.raises(ValueError, match="strategy spec does not match persisted runner identity"):
        StrategyRunnerStore(
            path,
            spec=_spec(producer_commit=CURRENT_COMMIT),
            evaluator_contract_fingerprint=EVALUATOR_FINGERPRINT,
        )


def test_a_matching_identity_rotates_nothing(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    fingerprint = _write_previous_runner(
        path,
        commit=CURRENT_COMMIT,
        evaluator=EVALUATOR_FINGERPRINT,
    )

    store = StrategyRunnerStore(
        path,
        spec=_spec(producer_commit=CURRENT_COMMIT),
        evaluator_contract_fingerprint=EVALUATOR_FINGERPRINT,
        previous_generation_of_identity=_previous_generation(
            {(fingerprint, EVALUATOR_FINGERPRINT): "f" * 64}
        ),
    )

    assert store.identity_rotation is None
    assert not any(item.name.endswith(".archived") for item in path.parent.iterdir())


def test_a_file_that_is_not_a_database_still_refuses(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    path.write_bytes(b"not a database")

    with pytest.raises(sqlite3.DatabaseError):
        StrategyRunnerStore(
            path,
            spec=_spec(producer_commit=CURRENT_COMMIT),
            evaluator_contract_fingerprint=EVALUATOR_FINGERPRINT,
            previous_generation_of_identity=_previous_generation({}),
        )
    assert not any(item.name.endswith(".archived") for item in path.parent.iterdir())


def test_an_archive_that_already_exists_is_not_overwritten(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    previous_fingerprint = _write_previous_runner(
        path,
        commit=PREVIOUS_COMMIT,
        evaluator=PREVIOUS_EVALUATOR,
    )
    (path.parent / f"runner.sqlite3.{'f' * 64}.archived").write_bytes(b"earlier archive")

    with pytest.raises(ValueError, match="archive already exists"):
        StrategyRunnerStore(
            path,
            spec=_spec(producer_commit=CURRENT_COMMIT),
            evaluator_contract_fingerprint=EVALUATOR_FINGERPRINT,
            previous_generation_of_identity=_previous_generation(
                {(previous_fingerprint, PREVIOUS_EVALUATOR): "f" * 64}
            ),
        )
    assert (path.parent / f"runner.sqlite3.{'f' * 64}.archived").read_bytes() == b"earlier archive"
