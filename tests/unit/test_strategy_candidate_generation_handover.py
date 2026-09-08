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

import json
import os
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

    archive = root / f"rotated-000000-{PREVIOUS_GENERATION}"
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
    (root / f"rotated-000000-{PREVIOUS_GENERATION}").mkdir(mode=0o700)

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


# ---------------------------------------------------------------------------------------
# The rotation is interruptible: a kill in the middle leaves something the next start
# finishes, not something a person has to repair (review SF-1)
# ---------------------------------------------------------------------------------------


class _KilledError(RuntimeError):
    """Stands in for the process going away mid-rotation."""


def _rotating_spool(root: Path) -> StrategyCandidateSnapshotSpool:
    return StrategyCandidateSnapshotSpool(
        root,
        previous_generation_of_binding=_previous_generation(
            {(PREVIOUS_DEFINITION, PREVIOUS_EXECUTABLE): PREVIOUS_GENERATION}
        ),
    )


def _rebind(spool: StrategyCandidateSnapshotSpool) -> object:
    return spool.rebind_previous_generation_authority(
        strategy_id="n_shape",
        strategy_version="1",
        definition_fingerprint=CURRENT_DEFINITION,
        executable_fingerprint=CURRENT_EXECUTABLE,
        candidate_schema_fingerprint=_candidate_schema_fingerprint(),
        static_feature_schema=STATIC_FEATURE_SCHEMA,
    )


def test_a_kill_between_the_root_documents_and_the_generations_is_finished_next_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _previous_root(tmp_path)
    published = sorted(item.name for item in (root / "generations").iterdir())
    assert published

    #: die exactly where the old order was unrecoverable: root documents gone, generation
    #: entries still in `generations/`
    real_rename = os.rename
    calls = {"n": 0}

    def rename_until_the_root_documents_are_moved(*args: object, **kwargs: object) -> None:
        if kwargs.get("src_dir_fd") is not None and kwargs.get("dst_dir_fd") is not None:
            calls["n"] += 1
            if calls["n"] > 3:
                raise _KilledError("process died mid-rotation")
        real_rename(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "rename", rename_until_the_root_documents_are_moved)
    with pytest.raises(_KilledError):
        _rebind(_rotating_spool(root))
    monkeypatch.undo()

    staging = root / f"rotated-000000-{PREVIOUS_GENERATION}.partial"
    assert staging.is_dir()
    assert not (root / "authority.json").exists()
    assert (staging / "authority.json").is_file()

    #: the next start finishes it with no human anywhere in the loop
    spool = _rotating_spool(root)
    rebind = _rebind(spool)

    assert rebind is not None
    assert rebind.resumed is True
    assert rebind.previous_generation_id == PREVIOUS_GENERATION
    assert not staging.exists()
    archive = root / f"rotated-000000-{PREVIOUS_GENERATION}"
    assert sorted(item.name for item in (archive / "generations").iterdir()) == published
    assert (archive / "authority.json").is_file()
    binding = json.loads((root / "authority.json").read_text(encoding="utf-8"))
    assert binding["definition_fingerprint"] == CURRENT_DEFINITION


def test_a_kill_after_the_archive_is_published_leaves_a_root_the_publisher_binds(
    tmp_path: Path,
) -> None:
    """The window after the single publishing rename is the one the publish path closes."""

    root = _previous_root(tmp_path)
    spool = _rotating_spool(root)
    _rebind(spool)
    #: what a kill between the rename and the new binding leaves behind
    (root / "authority.json").unlink()
    assert (root / f"rotated-000000-{PREVIOUS_GENERATION}").is_dir()
    assert list((root / "generations").iterdir()) == []

    _publish(
        root,
        definition=CURRENT_DEFINITION,
        executable=CURRENT_EXECUTABLE,
        captured_at=SECOND_CAPTURE,
        previous_generation_of_binding=_previous_generation({}),
    )

    binding = json.loads((root / "authority.json").read_text(encoding="utf-8"))
    assert binding["definition_fingerprint"] == CURRENT_DEFINITION


def test_two_interrupted_rotations_at_once_are_refused(tmp_path: Path) -> None:
    """One exclusive lock means one rotation; two staging directories is somebody else."""

    root = _previous_root(tmp_path)
    (root / f"rotated-000000-{PREVIOUS_GENERATION}.partial").mkdir(mode=0o700)
    (root / f"rotated-000001-{'a' * 64}.partial").mkdir(mode=0o700)

    with pytest.raises(
        StrategyCandidateSnapshotIntegrityError,
        match="more than one interrupted rotation",
    ):
        _rebind(_rotating_spool(root))


def test_a_root_keeps_two_archives_and_prunes_the_third(tmp_path: Path) -> None:
    """Nothing else on the host can reach `live/`, so the rotation bounds itself."""

    root = tmp_path / "candidates"
    #: deliberately in reverse lexicographic order, so a prune that sorts by *name*
    #: instead of by the rotation sequence keeps the wrong two (review SF-8)
    generations = ["3" * 64, "2" * 64, "1" * 64]
    fingerprints = [("4" * 64, "5" * 64), ("6" * 64, "7" * 64), ("8" * 64, "9" * 64)]
    _publish(
        root,
        definition=fingerprints[0][0],
        executable=fingerprints[0][1],
        captured_at=FIRST_CAPTURE,
    )
    pruned_by: list[tuple[str, ...]] = []
    for index, generation in enumerate(generations):
        following = fingerprints[index + 1] if index + 1 < len(fingerprints) else (
            CURRENT_DEFINITION,
            CURRENT_EXECUTABLE,
        )
        spool = StrategyCandidateSnapshotSpool(
            root,
            previous_generation_of_binding=_previous_generation(
                {fingerprints[index]: generation}
            ),
        )
        rebind = spool.rebind_previous_generation_authority(
            strategy_id="n_shape",
            strategy_version="1",
            definition_fingerprint=following[0],
            executable_fingerprint=following[1],
            candidate_schema_fingerprint=_candidate_schema_fingerprint(),
            static_feature_schema=STATIC_FEATURE_SCHEMA,
        )
        assert rebind is not None, generation
        pruned_by.append(rebind.pruned_archives)

    on_disk = sorted(item.name for item in root.iterdir() if item.name.startswith("rotated-"))
    assert on_disk == [
        f"rotated-000001-{generations[1]}",
        f"rotated-000002-{generations[2]}",
    ]
    #: the first two rotations keep everything; the third is the one that prunes
    assert pruned_by == [(), (), (f"rotated-000000-{generations[0]}",)]


#: What a child process runs to die at an exact point in the rotation. Kept as source so
#: the child is a *real* fresh interpreter: `monkeypatch` plus an exception unwinds the
#: stack and runs `finally` blocks, which is not what SIGKILL does, and the whole question
#: here is what is left on disk when nothing gets to clean up (review §B's killprobe).
_KILL_PROBE = """
import os, signal, sys
sys.path.insert(0, {repo!r})
from tests.unit.test_strategy_candidate_generation_handover import (
    CURRENT_DEFINITION, CURRENT_EXECUTABLE, STATIC_FEATURE_SCHEMA,
    PREVIOUS_DEFINITION, PREVIOUS_EXECUTABLE, PREVIOUS_GENERATION,
    _candidate_schema_fingerprint,
)
from rquant.strategy_candidate_snapshot import StrategyCandidateSnapshotSpool
from pathlib import Path

real_rename = os.rename
calls = [0]

def rename(*args, **kwargs):
    if kwargs.get("src_dir_fd") is not None and kwargs.get("dst_dir_fd") is not None:
        calls[0] += 1
        if calls[0] > {survive}:
            os.kill(os.getpid(), signal.SIGKILL)
    return real_rename(*args, **kwargs)

os.rename = rename
spool = StrategyCandidateSnapshotSpool(
    Path({root!r}),
    previous_generation_of_binding=lambda binding: (
        PREVIOUS_GENERATION
        if (binding.definition_fingerprint, binding.executable_fingerprint)
        == (PREVIOUS_DEFINITION, PREVIOUS_EXECUTABLE)
        else None
    ),
)
spool.rebind_previous_generation_authority(
    strategy_id="n_shape",
    strategy_version="1",
    definition_fingerprint=CURRENT_DEFINITION,
    executable_fingerprint=CURRENT_EXECUTABLE,
    candidate_schema_fingerprint=_candidate_schema_fingerprint(),
    static_feature_schema=STATIC_FEATURE_SCHEMA,
)
"""


def test_a_real_sigkill_before_the_first_root_document_moves_still_resumes(
    tmp_path: Path,
) -> None:
    """The one window the first rework left: `.partial` made, nothing moved into it yet.

    Killed there, the staging directory is empty and the root is *still fully bound*, so
    the resume had nothing to read the previous binding from and the next start failed
    once with `interrupted rotation carries no previous authority binding`. On these units
    a failed start is `Restart=` plus a real push, so "no human and no page at a
    generation change" was not yet true. The binding to archive is the one still in the
    root, and reading it there closes the window (review SF-6).
    """

    import subprocess
    import sys

    root = _previous_root(tmp_path)
    published = sorted(item.name for item in (root / "generations").iterdir())
    repo = str(Path(__file__).resolve().parents[2])

    #: survive=0 -> die on the very first dir_fd rename, which is the first root document
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", _KILL_PROBE.format(repo=repo, root=str(root), survive=0)],
        capture_output=True,
        cwd=repo,
        check=False,
    )
    assert result.returncode == -9, result.stderr.decode()

    staging = root / f"rotated-000000-{PREVIOUS_GENERATION}.partial"
    assert staging.is_dir()
    #: only the empty `generations/` the rotation makes first; crucially no
    #: `authority.json`, which is what the resume used to have nothing to read
    assert sorted(item.name for item in staging.iterdir()) == ["generations"]
    assert list((staging / "generations").iterdir()) == []
    #: the root is untouched: still bound, still holding every generation
    assert (root / "authority.json").is_file()
    assert sorted(item.name for item in (root / "generations").iterdir()) == published

    #: one fresh start finishes it -- not two
    rebind = _rebind(_rotating_spool(root))

    assert rebind is not None
    assert rebind.resumed is True
    assert not staging.exists()
    archive = root / f"rotated-000000-{PREVIOUS_GENERATION}"
    assert sorted(item.name for item in (archive / "generations").iterdir()) == published
    assert json.loads((archive / "authority.json").read_text(encoding="utf-8"))[
        "definition_fingerprint"
    ] == PREVIOUS_DEFINITION
    assert json.loads((root / "authority.json").read_text(encoding="utf-8"))[
        "definition_fingerprint"
    ] == CURRENT_DEFINITION


def test_back_dating_the_newest_archive_does_not_make_the_prune_take_it(
    tmp_path: Path,
) -> None:
    """The ordering must come from the rotation, not from the filesystem.

    A restore, a `touch`, or a sync tool that replays mtimes can make the newest archive
    look like the oldest. If pruning read the ordering off the mtime, the next rotation
    would delete the archive it had just made and keep two older ones (review SF-7). Only
    audit history is at stake -- live state is unreachable from here either way -- but
    "correct" should not rest on "nobody touched the mtimes".
    """

    root = tmp_path / "candidates"
    fingerprints = [
        ("4" * 64, "5" * 64),
        ("6" * 64, "7" * 64),
        ("8" * 64, "9" * 64),
        (CURRENT_DEFINITION, CURRENT_EXECUTABLE),
    ]
    generations = ["3" * 64, "2" * 64, "1" * 64]
    _publish(
        root,
        definition=fingerprints[0][0],
        executable=fingerprints[0][1],
        captured_at=FIRST_CAPTURE,
    )
    for index, generation in enumerate(generations):
        StrategyCandidateSnapshotSpool(
            root,
            previous_generation_of_binding=_previous_generation(
                {fingerprints[index]: generation}
            ),
        ).rebind_previous_generation_authority(
            strategy_id="n_shape",
            strategy_version="1",
            definition_fingerprint=fingerprints[index + 1][0],
            executable_fingerprint=fingerprints[index + 1][1],
            candidate_schema_fingerprint=_candidate_schema_fingerprint(),
            static_feature_schema=STATIC_FEATURE_SCHEMA,
        )

    newest = root / f"rotated-000002-{generations[2]}"
    oldest = root / f"rotated-000001-{generations[1]}"
    assert newest.is_dir() and oldest.is_dir()
    #: the newest archive now looks a week older than the one before it
    stale = oldest.lstat().st_mtime - 7 * 24 * 3600
    os.utime(newest, (stale, stale))
    assert newest.lstat().st_mtime < oldest.lstat().st_mtime

    rebind = StrategyCandidateSnapshotSpool(
        root,
        previous_generation_of_binding=_previous_generation(
            {fingerprints[3]: "0" * 64}
        ),
    ).rebind_previous_generation_authority(
        strategy_id="n_shape",
        strategy_version="1",
        definition_fingerprint=PREVIOUS_DEFINITION,
        executable_fingerprint=PREVIOUS_EXECUTABLE,
        candidate_schema_fingerprint=_candidate_schema_fingerprint(),
        static_feature_schema=STATIC_FEATURE_SCHEMA,
    )

    assert rebind is not None
    assert rebind.pruned_archives == (f"rotated-000001-{generations[1]}",)
    assert newest.is_dir(), "the back-dated archive is still the second newest"
    assert not oldest.exists()
