"""#250: every read-side runtime role reads the five-minute replica, not the main database.

`rquant-monitor` holds the write lock on `/home/lighthouse/rquant/data/rquant.duckdb` from
09:25 to 15:00, and DuckDB refuses *every* new connection while it does, `read_only=True`
included (CLAUDE.md's single-writer rule, the 2026-05-20 incident). Package N moved the
auction universe onto `rquant_ro.duckdb` (#249). The package O review found the three that
were still pointed at the main database, and what each one costs:

* the `auction_gap` candidate publisher's `daily_database_path`. It publishes only inside
  09:26-09:30 Asia/Shanghai (`runtime_builder_candidate.py:499`), which is entirely inside
  the lock window, so it could never publish — and `market-minute.source.v1` and
  `watchlist-quote.source.v1` then failed every iteration on `required authority has no
  not_visible snapshot`, which is where the whole live chain stopped;
* `reference-slow.source.v1`'s `database_path`. It copies the file it is given before
  reading it and refuses a database with an unsealed `.wal` sidecar — which the main
  database has whenever a writer holds it;
* the notifier's `page_projection_database_path`, polled every two seconds (#255).

The recovery binding is the one place the main database is still named, by design: it is
the artifact recovery *backs up*, not something a live role reads.
"""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from typing import Any

import pytest

from rquant.runtime_production_profile import (
    ProductionRuntimeProfileInputs,
    build_production_runtime_profile,
)


def _inputs(tmp_path: Path) -> ProductionRuntimeProfileInputs:
    import tests.unit.test_runtime_production_profile as profile_fixtures

    return profile_fixtures._inputs(tmp_path)


def _manifest(profile: object, service_id: str) -> object:
    return next(item for item in profile.manifests if item.service_id == service_id)


# ---------------------------------------------------------------------------------------
# The three bindings ruling 24 moves
# ---------------------------------------------------------------------------------------


def test_the_auction_gap_publisher_reads_the_replica(tmp_path: Path) -> None:
    """Its publish window is 09:26-09:30, entirely inside the monitor's lock."""

    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)

    manifest = _manifest(profile, "candidate.auction_gap.v1")
    assert Path(str(manifest.settings["daily_database_path"])) == (
        inputs.readonly_replica_database_path
    )
    assert Path(str(manifest.settings["daily_database_path"])) != (
        inputs.operational_database_path
    )


def test_the_reference_slow_source_reads_the_replica(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)

    manifest = _manifest(profile, "reference-slow.source.v1")
    assert Path(str(manifest.settings["database_path"])) == (
        inputs.readonly_replica_database_path
    )


def test_the_notifier_page_projection_reads_the_replica(tmp_path: Path) -> None:
    """Every two seconds, so on the main database it hit the lock every two seconds."""

    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)

    manifest = _manifest(profile, "notifier.admin.shadow.v1")
    assert Path(str(manifest.settings["page_projection_database_path"])) == (
        inputs.readonly_replica_database_path
    )
    #: `runtime_deployment_bundle` requires this to be the projection database's own
    #: `surge_live` sibling, and in production both databases live in the same data
    #: directory, so the value does not move — it is only expressed against the replica.
    assert Path(str(manifest.settings["page_projection_surge_live_root"])) == (
        inputs.readonly_replica_database_path.parent / "surge_live"
    )


def test_no_manifest_anywhere_names_the_main_database(tmp_path: Path) -> None:
    """The whole point, stated once over every manifest rather than three times."""

    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)

    main = str(inputs.operational_database_path)
    named = tuple(
        manifest.service_id
        for manifest in profile.manifests
        if main in json.dumps(manifest.model_dump(mode="json"))
    )
    assert named == ()


def test_the_recovery_binding_still_names_the_main_database(tmp_path: Path) -> None:
    """The explicit exemption: recovery backs the main database up, it does not read it."""

    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)

    recovery = profile.recovery
    role = next(
        item
        for item in recovery.artifact_roles
        if item.logical_role == recovery.production_artifact_role
    )
    assert recovery.backup_source_root.joinpath(*Path(role.source_path).parts) == (
        inputs.operational_database_path
    )


# ---------------------------------------------------------------------------------------
# The refusal rule
# ---------------------------------------------------------------------------------------


def test_a_read_side_role_pointed_at_the_main_database_is_refused(tmp_path: Path) -> None:
    """The role and the field are both named, because the operator has to fix one line."""

    from rquant.runtime_production_profile import _validate_read_side_database_bindings

    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)
    manifest = _manifest(profile, "reference-slow.source.v1")
    doctored = manifest.model_copy(
        update={
            "settings": {
                **dict(manifest.settings),
                "database_path": str(inputs.operational_database_path),
            }
        }
    )

    with pytest.raises(ValueError) as error:
        _validate_read_side_database_bindings(
            (doctored,),
            operational_database_path=inputs.operational_database_path,
            readonly_replica_database_path=inputs.readonly_replica_database_path,
        )
    assert "reference-slow.source.v1" in str(error.value)
    assert "database_path" in str(error.value)


def test_the_refusal_resolves_symlinks(tmp_path: Path) -> None:
    """A replica *named* `rquant_ro.duckdb` that is a symlink to the main database.

    Neither of package N's two input checks catches this: the paths are not equal and the
    name is not `rquant.duckdb`. Only resolving the link does, and it has to be resolved at
    the inputs layer, because that is the only layer the generator runs
    (`test_the_inputs_document_alone_refuses_a_symlinked_replica` is the same rule without
    a profile at all; the deployment topology's own symlink refusal is a third net, one
    layer further in).
    """

    inputs = _inputs(tmp_path)
    inputs.operational_database_path.parent.mkdir(parents=True, exist_ok=True)
    inputs.operational_database_path.write_bytes(b"")
    replica = inputs.readonly_replica_database_path
    replica.unlink(missing_ok=True)
    replica.symlink_to(inputs.operational_database_path)

    with pytest.raises(ValueError, match="main database"):
        build_production_runtime_profile(inputs)


def test_the_inputs_document_alone_refuses_a_symlinked_replica(tmp_path: Path) -> None:
    """The generator's own layer, which never builds a profile (#250, ruling 24.2).

    `scripts/build_runtime_production_inputs.py` validates the document and writes it; the
    profile is built later, on the host. So the refusal has to exist here too, or a
    document naming the main database through a link is written, reviewed and installed
    before anything notices.
    """

    inputs = _inputs(tmp_path)
    inputs.operational_database_path.parent.mkdir(parents=True, exist_ok=True)
    inputs.operational_database_path.write_bytes(b"")
    replica = inputs.readonly_replica_database_path
    replica.unlink(missing_ok=True)
    replica.symlink_to(inputs.operational_database_path)
    payload = inputs.model_dump(mode="python")

    with pytest.raises(ValueError, match="resolve to the main database"):
        ProductionRuntimeProfileInputs.model_validate(payload)


def test_every_read_side_binding_is_checked_by_name(tmp_path: Path) -> None:
    """A binding silently dropped back onto the main database is caught by field, too."""

    from rquant.runtime_production_profile import READ_SIDE_DATABASE_BINDINGS

    assert READ_SIDE_DATABASE_BINDINGS == {
        "candidate.auction_gap.v1": ("daily_database_path",),
        "notifier.admin.shadow.v1": ("page_projection_database_path",),
        "reference-slow.source.v1": ("database_path",),
        "auction-universe.publisher.v1": ("database_path",),
    }

    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)
    for service_id, fields in READ_SIDE_DATABASE_BINDINGS.items():
        manifest = _manifest(profile, service_id)
        for field in fields:
            assert Path(str(manifest.settings[field])) == (
                inputs.readonly_replica_database_path
            ), f"{service_id}.{field}"


def test_a_read_side_binding_that_is_not_the_replica_is_refused(tmp_path: Path) -> None:
    """Not the main database, but not the replica either: still refused, by name."""

    from rquant.runtime_production_profile import _validate_read_side_database_bindings

    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)
    manifest = _manifest(profile, "candidate.auction_gap.v1")
    elsewhere = inputs.operational_database_path.parent / "somewhere_else.duckdb"
    doctored = manifest.model_copy(
        update={
            "settings": {
                **dict(manifest.settings),
                "daily_database_path": str(elsewhere),
            }
        }
    )

    with pytest.raises(ValueError) as error:
        _validate_read_side_database_bindings(
            (doctored,),
            operational_database_path=inputs.operational_database_path,
            readonly_replica_database_path=inputs.readonly_replica_database_path,
        )
    assert "candidate.auction_gap.v1" in str(error.value)
    assert "daily_database_path" in str(error.value)


# ---------------------------------------------------------------------------------------
# Ruling 24.3: the mode rule the replica can actually satisfy (#249's rule)
# ---------------------------------------------------------------------------------------


@pytest.mark.parametrize("mode", [0o600, 0o400, 0o644, 0o640, 0o444])
def test_the_auction_gap_reader_accepts_a_replica_the_runtime_owns(
    tmp_path: Path,
    mode: int,
) -> None:
    """`sync-readonly-replica.sh` recreates the replica at 0644 every five minutes."""

    from rquant.auction_gap_candidate_input import _private_snapshot_identity

    path = tmp_path / "rquant_ro.duckdb"
    path.write_bytes(b"")
    path.chmod(mode)

    observed, normalized = _private_snapshot_identity(path)
    assert normalized == path
    assert stat.S_IMODE(observed.st_mode) == mode


@pytest.mark.parametrize("mode", [0o664, 0o666, 0o620, 0o602, 0o606])
def test_the_auction_gap_reader_refuses_a_group_or_other_writable_replica(
    tmp_path: Path,
    mode: int,
) -> None:
    from rquant.auction_gap_candidate_input import (
        AuctionGapCandidateInputError,
        _private_snapshot_identity,
    )

    path = tmp_path / "rquant_ro.duckdb"
    path.write_bytes(b"")
    path.chmod(mode)

    with pytest.raises(AuctionGapCandidateInputError, match="group or other writable"):
        _private_snapshot_identity(path)


def test_the_auction_gap_reader_still_refuses_a_symlink_and_a_hard_link(
    tmp_path: Path,
) -> None:
    from rquant.auction_gap_candidate_input import (
        AuctionGapCandidateInputError,
        _private_snapshot_identity,
    )

    target = tmp_path / "rquant_ro.duckdb"
    target.write_bytes(b"")
    target.chmod(0o644)
    link = tmp_path / "link.duckdb"
    link.symlink_to(target)
    with pytest.raises(AuctionGapCandidateInputError, match="symlink or unsafe file"):
        _private_snapshot_identity(link)

    os.link(target, tmp_path / "second-name.duckdb")
    with pytest.raises(AuctionGapCandidateInputError, match="one hard link"):
        _private_snapshot_identity(target)


@pytest.mark.parametrize("mode", [0o600, 0o400, 0o644, 0o640, 0o444])
def test_the_reference_slow_reader_accepts_a_replica_the_runtime_owns(
    tmp_path: Path,
    mode: int,
) -> None:
    from rquant.reference_slow_source import _validate_database

    path = tmp_path / "rquant_ro.duckdb"
    path.write_bytes(b"")
    path.chmod(mode)

    _validate_database(path.lstat())


@pytest.mark.parametrize("mode", [0o664, 0o666, 0o620, 0o602, 0o606])
def test_the_reference_slow_reader_refuses_a_group_or_other_writable_replica(
    tmp_path: Path,
    mode: int,
) -> None:
    from rquant.reference_slow_source import ReferenceSlowSourceError, _validate_database

    path = tmp_path / "rquant_ro.duckdb"
    path.write_bytes(b"")
    path.chmod(mode)

    with pytest.raises(ReferenceSlowSourceError, match="group or other writable"):
        _validate_database(path.lstat())


def test_the_reference_slow_reader_still_refuses_a_symlink_and_a_hard_link(
    tmp_path: Path,
) -> None:
    from rquant.reference_slow_source import ReferenceSlowSourceError, _validate_database

    target = tmp_path / "rquant_ro.duckdb"
    target.write_bytes(b"")
    target.chmod(0o644)
    link = tmp_path / "link.duckdb"
    link.symlink_to(target)
    with pytest.raises(ReferenceSlowSourceError, match="symlink or unsafe file"):
        _validate_database(link.lstat())

    os.link(target, tmp_path / "second-name.duckdb")
    with pytest.raises(ReferenceSlowSourceError, match="one hard link"):
        _validate_database(target.lstat())


# ---------------------------------------------------------------------------------------
# Ruling 24.2: the generator has no override that puts a reader back on the main database
# ---------------------------------------------------------------------------------------


def test_the_generator_has_no_allow_primary_database_override() -> None:
    """`--allow-primary-database` is gone: it applied to no read-side role and no recovery
    binding, only to the generator's own calendar read, and an operator reading `--help`
    could take it for permission to point a live role at the write-locked main file."""

    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
    import build_runtime_production_inputs as generator

    options = {
        option
        for action in generator.build_argument_parser()._actions
        for option in action.option_strings
    }
    assert "--allow-primary-database" not in options


def test_the_generator_refuses_the_primary_duckdb_unconditionally(tmp_path: Path) -> None:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
    import build_runtime_production_inputs as generator

    with pytest.raises(generator.GeneratorError, match="refusing to open the primary"):
        generator.read_sse_calendar(tmp_path / "rquant.duckdb")


# ---------------------------------------------------------------------------------------
# Ruling 24.3: open, read, close inside one iteration — never held across a replacement
# ---------------------------------------------------------------------------------------


def _replica_with(path: Path, *, volume: float, trade_dates: tuple[Any, ...]) -> None:
    import duckdb

    if path.exists():
        path.unlink()
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            "CREATE TABLE daily_bar(ts_code VARCHAR, trade_date DATE, vol DOUBLE)"
        )
        connection.executemany(
            "INSERT INTO daily_bar VALUES (?, ?, ?)",
            [("300001.SZ", trade_date, volume) for trade_date in trade_dates],
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    path.chmod(0o644)


def test_the_auction_gap_reader_reopens_the_replica_on_every_read(tmp_path: Path) -> None:
    """The replica is replaced by `mv` every five minutes; a held connection would miss it.

    Two reads with an atomic replacement between them return the two files' contents, which
    only a reader that opened and closed inside each read can do.
    """

    from datetime import date

    from rquant.auction_gap_candidate_input import _daily_volume_rows

    dates = (date(2026, 8, 10),)
    replica = tmp_path / "rquant_ro.duckdb"
    _replica_with(replica, volume=1_000.0, trade_dates=dates)

    first, _first_at = _daily_volume_rows(replica, ts_codes=("300001.SZ",), trade_dates=dates)

    staging = tmp_path / "rquant_ro.duckdb.tmp"
    _replica_with(staging, volume=2_000.0, trade_dates=dates)
    os.replace(staging, replica)

    second, _second_at = _daily_volume_rows(replica, ts_codes=("300001.SZ",), trade_dates=dates)

    assert [row[2] for row in first] == [1_000.0]
    assert [row[2] for row in second] == [2_000.0]


# ---------------------------------------------------------------------------------------
# Review SF-1/SF-2/SF-3: what the sweep looks at, and what "the main database" means
# ---------------------------------------------------------------------------------------


def _doctored(profile: object, service_id: str, **settings: object) -> object:
    manifest = _manifest(profile, service_id)
    return manifest.model_copy(
        update={"settings": {**dict(manifest.settings), **settings}}
    )


def _refuse(inputs: ProductionRuntimeProfileInputs, manifest: object) -> str:
    from rquant.runtime_production_profile import _validate_read_side_database_bindings

    with pytest.raises(ValueError) as error:
        _validate_read_side_database_bindings(
            (manifest,),
            operational_database_path=inputs.operational_database_path,
            readonly_replica_database_path=inputs.readonly_replica_database_path,
        )
    return str(error.value)


@pytest.mark.parametrize(
    "field",
    [
        #: not a `*_database_path` name at all, and two manifests already carry a DuckDB
        #: under exactly this key (`artifact-catalog.primary.v1`, `lab-jobs.serving.v1`)
        "dataset_authority_path",
        #: the plural, which the suffix test also missed
        "extra_database_paths",
        #: and a name that says nothing about databases
        "somewhere_else",
    ],
)
def test_the_main_database_is_refused_under_any_setting_name(
    tmp_path: Path,
    field: str,
) -> None:
    """Filtering by key name only refuses the mistakes that spell themselves out (SF-1)."""

    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)
    doctored = _doctored(
        profile,
        "auction-universe.publisher.v1",
        **{field: str(inputs.operational_database_path)},
    )

    message = _refuse(inputs, doctored)
    assert "auction-universe.publisher.v1" in message
    assert field in message


def test_the_main_database_is_refused_inside_a_list_and_inside_a_nested_mapping(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)
    main = str(inputs.operational_database_path)

    assert "in_a_list" in _refuse(
        inputs,
        _doctored(profile, "auction-universe.publisher.v1", in_a_list=["/tmp/fine", main]),
    )
    assert "in_a_mapping" in _refuse(
        inputs,
        _doctored(
            profile,
            "auction-universe.publisher.v1",
            in_a_mapping=[{"label": "authority", "path": main}],
        ),
    )


def test_a_relative_setting_is_never_resolved_against_the_building_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same document must not mean two things depending on where it was built (SF-2).

    `os.path.realpath` resolves a relative value against the *process* working directory,
    so `rquant.duckdb` was refused when the profile happened to be built in the data
    directory and accepted anywhere else. A relative value is never resolved now; one that
    spells the main database's own file name is refused wherever the build runs.
    """

    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)
    inputs.operational_database_path.parent.mkdir(parents=True, exist_ok=True)
    doctored = _doctored(
        profile,
        "auction-universe.publisher.v1",
        relative_database_path=inputs.operational_database_path.name,
    )

    for directory in (tmp_path, inputs.operational_database_path.parent):
        monkeypatch.chdir(directory)
        assert "relative_database_path" in _refuse(inputs, doctored)


def test_a_hard_link_to_the_main_database_is_refused(tmp_path: Path) -> None:
    """`realpath` sees two different names; the inode sees one file (SF-3)."""

    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)
    inputs.operational_database_path.parent.mkdir(parents=True, exist_ok=True)
    inputs.operational_database_path.write_bytes(b"")
    linked = inputs.operational_database_path.parent / "second-name.duckdb"
    os.link(inputs.operational_database_path, linked)
    assert os.path.realpath(linked) != os.path.realpath(inputs.operational_database_path)

    doctored = _doctored(
        profile,
        "auction-universe.publisher.v1",
        hard_linked_path=str(linked),
    )

    assert "hard_linked_path" in _refuse(inputs, doctored)


def test_a_hard_linked_replica_is_refused_at_the_inputs_layer(tmp_path: Path) -> None:
    """A replica whose name is its own but whose inode is the main database's (SF-3)."""

    inputs = _inputs(tmp_path)
    inputs.operational_database_path.parent.mkdir(parents=True, exist_ok=True)
    inputs.operational_database_path.write_bytes(b"")
    replica = inputs.readonly_replica_database_path
    replica.unlink(missing_ok=True)
    os.link(inputs.operational_database_path, replica)
    payload = inputs.model_dump(mode="python")

    with pytest.raises(ValueError, match="resolve to the main database"):
        ProductionRuntimeProfileInputs.model_validate(payload)


def test_the_generator_reports_a_symlinked_replica_without_a_traceback(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The operator reads `error: ...` on stderr, not a pydantic stack (SF-6)."""

    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
    import build_runtime_production_inputs as generator

    from tests.unit.test_build_runtime_production_inputs import (
        _argv,
        _write_calendar_database,
    )

    _write_calendar_database(tmp_path / "calendar.duckdb")
    data_root = tmp_path / "data"
    data_root.mkdir(parents=True, exist_ok=True)
    main = data_root / "rquant.duckdb"
    main.write_bytes(b"")
    replica = data_root / "rquant_ro.duckdb"
    replica.symlink_to(main)

    code = generator.main(_argv(tmp_path))

    assert code == 2
    captured = capsys.readouterr()
    assert "read-only replica cannot resolve to the main database" in captured.err
    assert "Traceback" not in captured.err
