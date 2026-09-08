"""#249: the auction universe read the production main DuckDB, and wanted it at 0600.

`auction-universe.publisher.v1` has been DEGRADED on every iteration of every Route A
window with `AuctionUniverseSourceError: daily snapshot must have mode 0600`, and nobody
was paged for it (#235). Two separate mistakes:

* the installed manifest's `database_path` was `/home/lighthouse/rquant/data/rquant.duckdb`
  — the production main database. CLAUDE.md's single-writer rule says `rquant-monitor`
  holds its write lock 09:25-15:00 and refuses *every* new connection, read-only included,
  so this source would fail exactly during the session it exists to prepare for. Readers
  use `rquant_ro.duckdb`;
* 0600 is a mode neither file can have. The replica is recreated every five minutes by
  `scripts/sync-readonly-replica.sh` at 0644, and the main database's mode is production
  configuration no runtime window should be changing.

So the profile points the source at the replica and refuses a path that resolves to the
main database, and the mode rule becomes what actually protects the read: owned by the
runtime uid and not writable by group or other.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from rquant.auction_universe_source import AuctionUniverseSourceError, _validate_snapshot_stat


def _stat(path: Path) -> os.stat_result:
    return path.lstat()


@pytest.mark.parametrize("mode", [0o600, 0o400, 0o644, 0o640, 0o444])
def test_a_snapshot_the_runtime_owns_and_nobody_else_can_write_is_accepted(
    tmp_path: Path,
    mode: int,
) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    path.write_bytes(b"")
    path.chmod(mode)

    _validate_snapshot_stat(_stat(path))


@pytest.mark.parametrize("mode", [0o664, 0o666, 0o620, 0o602, 0o606])
def test_a_group_or_other_writable_snapshot_is_refused(tmp_path: Path, mode: int) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    path.write_bytes(b"")
    path.chmod(mode)

    with pytest.raises(AuctionUniverseSourceError, match="group or other writable"):
        _validate_snapshot_stat(_stat(path))


def test_a_directory_and_a_symlink_are_still_refused(tmp_path: Path) -> None:
    directory = tmp_path / "dir"
    directory.mkdir(mode=0o700)
    with pytest.raises(AuctionUniverseSourceError, match="symlink or unsafe file"):
        _validate_snapshot_stat(_stat(directory))

    target = tmp_path / "real.duckdb"
    target.write_bytes(b"")
    target.chmod(0o644)
    link = tmp_path / "link.duckdb"
    link.symlink_to(target)
    with pytest.raises(AuctionUniverseSourceError, match="symlink or unsafe file"):
        _validate_snapshot_stat(_stat(link))


def test_a_hard_linked_snapshot_is_still_refused(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    path.write_bytes(b"")
    path.chmod(0o644)
    os.link(path, tmp_path / "second-name.duckdb")

    with pytest.raises(AuctionUniverseSourceError, match="one hard link"):
        _validate_snapshot_stat(_stat(path))


# ---------------------------------------------------------------------------------------
# The other half of #249: which database the profile points the source at
# ---------------------------------------------------------------------------------------


def _profile_inputs(tmp_path: Path) -> object:
    import tests.unit.test_runtime_production_profile as profile_fixtures

    return profile_fixtures._inputs(tmp_path)


def test_the_auction_universe_reads_the_replica_and_not_the_main_database(
    tmp_path: Path,
) -> None:
    from rquant.runtime_production_profile import build_production_runtime_profile
    from rquant.runtime_service_entrypoint import RuntimeServiceKind

    inputs = _profile_inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)

    manifest = next(
        item
        for item in profile.manifests
        if item.service_kind is RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER
    )
    assert Path(str(manifest.settings["database_path"])) == (
        inputs.readonly_replica_database_path
    )
    assert Path(str(manifest.settings["database_path"])) != inputs.operational_database_path


def test_a_replica_path_that_is_the_main_database_is_refused(tmp_path: Path) -> None:
    """Named anything at all: if it *is* the operational database, it is refused.

    The two halves of the rule are separated on purpose — this case is not also caught by
    the `rquant.duckdb` name check, so it can only be the identity check that rejects it.
    """

    from rquant.runtime_production_profile import ProductionRuntimeProfileInputs

    inputs = _profile_inputs(tmp_path)
    shared = inputs.readonly_replica_database_path
    assert shared.name != "rquant.duckdb"
    payload = inputs.model_dump(mode="python")
    payload["operational_database_path"] = shared
    payload["readonly_replica_database_path"] = shared

    with pytest.raises(ValueError, match="read-only replica"):
        ProductionRuntimeProfileInputs.model_validate(payload)


def test_a_replica_path_named_rquant_duckdb_is_refused(tmp_path: Path) -> None:
    from rquant.runtime_production_profile import ProductionRuntimeProfileInputs

    inputs = _profile_inputs(tmp_path)
    payload = inputs.model_dump(mode="python")
    payload["readonly_replica_database_path"] = (
        inputs.operational_database_path.parent / "rquant.duckdb"
    )

    with pytest.raises(ValueError, match="read-only replica"):
        ProductionRuntimeProfileInputs.model_validate(payload)
