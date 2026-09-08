"""#248 shape (3): the candidate authority binding our own previous generation created.

`live/candidates/<svc>/authority.json` is created once and never rewritten: it pins the
strategy's definition and executable fingerprints, and both are derived from the producer
commit. On 2026-09-09 the candidate publishers went DEGRADED on every iteration with
`strategy candidate authority is bound to a different identity`, and the window moved
both candidate roots aside by hand (`rquant-candidate-aside-20260909-034659/`).

Re-binding is allowed on one shape only: everything about the binding is the same except
the two fingerprints, and the pair on disk is one our own previous generation published.
The published generations underneath are archived with it rather than relabelled — a
candidate generation is evidence about the executable that produced it, and stamping the
new binding's hash onto it would claim the new executable produced last release's rows.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.strategy_candidate_snapshot import (
    StrategyCandidateSnapshotIntegrityError,
    StrategyCandidateSnapshotSpool,
)
from tests.unit.test_strategy_candidate_snapshot import (
    COMMIT_A,
    HASH_A,
    STATIC_FEATURE_SCHEMA,
    TRADE_DATE,
    _candidate_schema_fingerprint,
)

PREVIOUS_DEFINITION = "4" * 64
PREVIOUS_EXECUTABLE = "5" * 64
CURRENT_DEFINITION = "6" * 64
CURRENT_EXECUTABLE = "7" * 64
FOREIGN_DEFINITION = "8" * 64
FOREIGN_EXECUTABLE = "9" * 64
PREVIOUS_GENERATION = "f" * 64
FIRST_CAPTURE = datetime(2026, 9, 8, 15, 37, tzinfo=UTC)
SECOND_CAPTURE = datetime(2026, 9, 9, 3, 2, tzinfo=UTC)


def _previous_generation(pairs: dict[tuple[str, str], str]) -> object:
    def resolve(binding: object) -> str | None:
        return pairs.get(
            (
                str(binding.definition_fingerprint),
                str(binding.executable_fingerprint),
            )
        )

    return resolve


def _publish(
    root: Path,
    *,
    definition: str,
    executable: str,
    captured_at: datetime,
    previous_generation_of_binding: object | None = None,
) -> StrategyCandidateSnapshotSpool:
    spool = StrategyCandidateSnapshotSpool(
        root,
        previous_generation_of_binding=previous_generation_of_binding,
    )
    spool.publish_strategy_records(
        strategy_id="n_shape",
        strategy_version="1",
        definition_fingerprint=definition,
        executable_fingerprint=executable,
        candidate_schema_fingerprint=_candidate_schema_fingerprint(),
        static_feature_schema=STATIC_FEATURE_SCHEMA,
        source_snapshot_ids={"candidate_input": HASH_A},
        trade_date=TRADE_DATE,
        captured_at=captured_at,
        producer_commit=COMMIT_A,
        rows=(),
    )
    return spool


def _previous_root(tmp_path: Path) -> Path:
    root = tmp_path / "candidates"
    _publish(
        root,
        definition=PREVIOUS_DEFINITION,
        executable=PREVIOUS_EXECUTABLE,
        captured_at=FIRST_CAPTURE,
    )
    return root


def test_a_binding_from_our_own_previous_generation_is_archived_and_rebound(
    tmp_path: Path,
) -> None:
    root = _previous_root(tmp_path)
    published = sorted(item.name for item in (root / "generations").iterdir())
    assert len(published) == 1

    spool = _publish(
        root,
        definition=CURRENT_DEFINITION,
        executable=CURRENT_EXECUTABLE,
        captured_at=SECOND_CAPTURE,
        previous_generation_of_binding=_previous_generation(
            {(PREVIOUS_DEFINITION, PREVIOUS_EXECUTABLE): PREVIOUS_GENERATION}
        ),
    )

    rebind = spool.authority_rebind
    assert rebind is not None
    assert rebind.previous_generation_id == PREVIOUS_GENERATION
    assert rebind.event == f"candidate_authority_rebound:{PREVIOUS_GENERATION}"

    archive = root / f"rotated-{PREVIOUS_GENERATION}"
    assert rebind.archive_root == archive
    assert (archive / "authority.json").is_file()
    assert (archive / "generation-index.json").is_file()
    assert (archive / "current.json").is_file()
    assert sorted(item.name for item in (archive / "generations").iterdir()) == published

    binding = spool.read_authority_binding(
        strategy_id="n_shape",
        strategy_version="1",
        definition_fingerprint=CURRENT_DEFINITION,
        executable_fingerprint=CURRENT_EXECUTABLE,
        candidate_schema_fingerprint=_candidate_schema_fingerprint(),
        static_feature_schema=STATIC_FEATURE_SCHEMA,
    )
    assert binding.definition_fingerprint == CURRENT_DEFINITION
    assert binding.executable_fingerprint == CURRENT_EXECUTABLE
    #: the new binding starts its own sequence; nothing from the old one is claimed by it
    assert len(sorted((root / "generations").iterdir())) == 1


def test_a_foreign_binding_still_refuses(tmp_path: Path) -> None:
    root = _previous_root(tmp_path)

    with pytest.raises(
        StrategyCandidateSnapshotIntegrityError,
        match="bound to a different identity",
    ):
        _publish(
            root,
            definition=FOREIGN_DEFINITION,
            executable=FOREIGN_EXECUTABLE,
            captured_at=SECOND_CAPTURE,
            previous_generation_of_binding=_previous_generation({}),
        )
    assert not any(item.name.startswith("rotated-") for item in root.iterdir())


def test_a_different_candidate_schema_still_refuses(tmp_path: Path) -> None:
    """Only the two fingerprints may differ: a schema change is a different question."""

    root = _previous_root(tmp_path)
    spool = StrategyCandidateSnapshotSpool(
        root,
        previous_generation_of_binding=_previous_generation(
            {(PREVIOUS_DEFINITION, PREVIOUS_EXECUTABLE): PREVIOUS_GENERATION}
        ),
    )

    with pytest.raises(StrategyCandidateSnapshotIntegrityError):
        spool.publish_strategy_records(
            strategy_id="n_shape",
            strategy_version="1",
            definition_fingerprint=CURRENT_DEFINITION,
            executable_fingerprint=CURRENT_EXECUTABLE,
            candidate_schema_fingerprint=_candidate_schema_fingerprint(
                static_feature_schema={"pool": {"dtype": "string", "semantic": "only one"}},
            ),
            static_feature_schema={"pool": {"dtype": "string", "semantic": "only one"}},
            source_snapshot_ids={"candidate_input": HASH_A},
            trade_date=TRADE_DATE,
            captured_at=SECOND_CAPTURE,
            producer_commit=COMMIT_A,
            rows=(),
        )
    assert not any(item.name.startswith("rotated-") for item in root.iterdir())


def test_without_a_lineage_the_refusal_is_unchanged(tmp_path: Path) -> None:
    root = _previous_root(tmp_path)

    with pytest.raises(
        StrategyCandidateSnapshotIntegrityError,
        match="bound to a different identity",
    ):
        _publish(
            root,
            definition=CURRENT_DEFINITION,
            executable=CURRENT_EXECUTABLE,
            captured_at=SECOND_CAPTURE,
        )


def test_a_matching_binding_rebinds_nothing(tmp_path: Path) -> None:
    root = _previous_root(tmp_path)

    spool = _publish(
        root,
        definition=PREVIOUS_DEFINITION,
        executable=PREVIOUS_EXECUTABLE,
        captured_at=SECOND_CAPTURE,
        previous_generation_of_binding=_previous_generation(
            {(PREVIOUS_DEFINITION, PREVIOUS_EXECUTABLE): PREVIOUS_GENERATION}
        ),
    )

    assert spool.authority_rebind is None
    assert not any(item.name.startswith("rotated-") for item in root.iterdir())


def test_readers_never_rebind(tmp_path: Path) -> None:
    """Only the owner rebinds; a consumer reading the wrong identity still refuses."""

    root = _previous_root(tmp_path)
    spool = StrategyCandidateSnapshotSpool(
        root,
        previous_generation_of_binding=_previous_generation(
            {(PREVIOUS_DEFINITION, PREVIOUS_EXECUTABLE): PREVIOUS_GENERATION}
        ),
    )

    with pytest.raises(
        StrategyCandidateSnapshotIntegrityError,
        match="bound to a different identity",
    ):
        spool.read_strategy_as_of(
            strategy_id="n_shape",
            strategy_version="1",
            definition_fingerprint=CURRENT_DEFINITION,
            executable_fingerprint=CURRENT_EXECUTABLE,
            candidate_schema_fingerprint=_candidate_schema_fingerprint(),
            static_feature_schema=STATIC_FEATURE_SCHEMA,
            as_of=SECOND_CAPTURE,
        )
    assert not any(item.name.startswith("rotated-") for item in root.iterdir())


def test_an_archive_that_already_exists_is_not_overwritten(tmp_path: Path) -> None:
    root = _previous_root(tmp_path)
    (root / f"rotated-{PREVIOUS_GENERATION}").mkdir(mode=0o700)

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="archive already exists"):
        _publish(
            root,
            definition=CURRENT_DEFINITION,
            executable=CURRENT_EXECUTABLE,
            captured_at=SECOND_CAPTURE,
            previous_generation_of_binding=_previous_generation(
                {(PREVIOUS_DEFINITION, PREVIOUS_EXECUTABLE): PREVIOUS_GENERATION}
            ),
        )
    assert (root / "authority.json").is_file()
