"""The Lab publisher must bind a proposal to one fixed, read-only file."""

from __future__ import annotations

import hashlib
import os
import shutil
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from rquant.backfill_plan_core import DailyBarBackfillPlan
from tests.unit.test_backfill_plan_core import _assumptions, _database

START = date(2026, 1, 29)
END = date(2026, 2, 5)
OBSERVED = datetime(2026, 2, 6, 1, tzinfo=UTC)


def _publish(
    snapshot: Path,
    directory: Path,
    *,
    expected_file_sha256: str | None = None,
    evidence_code_revision: str = "revision-1",
) -> Path:
    from rquant.backfill_plan_artifact import create_and_publish_daily_bar_backfill_plan

    return create_and_publish_daily_bar_backfill_plan(
        snapshot_path=snapshot,
        expected_file_sha256=(
            expected_file_sha256 or hashlib.sha256(snapshot.read_bytes()).hexdigest()
        ),
        snapshot_label="fixed-replica",
        evidence_code_revision=evidence_code_revision,
        audit_start=START,
        completed_through=END,
        observed_at=OBSERVED,
        assumptions=_assumptions(),
        directory=directory,
    )


def _snapshot(tmp_path: Path) -> Path:
    return _database(tmp_path / "replica.duckdb", START, END, [])


def test_publish_measures_file_digest_and_uses_read_only_connection(tmp_path: Path) -> None:
    from rquant.backfill_plan_artifact import load_daily_bar_backfill_plan

    snapshot = _snapshot(tmp_path)
    expected = hashlib.sha256(snapshot.read_bytes()).hexdigest()
    before = snapshot.read_bytes()

    destination = _publish(snapshot, tmp_path / "plans", expected_file_sha256=expected)

    plan = load_daily_bar_backfill_plan(destination)
    assert isinstance(plan, DailyBarBackfillPlan)
    assert destination.name == f"daily-bar-backfill-plan-v1-{plan.content_sha256}.json"
    assert plan.source.claimed_file_sha256 == expected
    assert plan.source.mode == "production_unverified"
    assert plan.source.identity_verified is False
    assert plan.source.collection_complete_verified is False
    assert plan.executable is False
    assert plan.missing_dates
    assert snapshot.read_bytes() == before
    with duckdb.connect(str(snapshot), read_only=True) as connection:
        assert connection.execute("SELECT current_setting('access_mode')").fetchone() == (
            "read_only",
        )


def test_false_caller_digest_is_rejected_before_artifact_publication(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path)
    directory = tmp_path / "plans"

    with pytest.raises(ValueError, match="SHA256|digest"):
        _publish(snapshot, directory, expected_file_sha256="0" * 64)

    assert not directory.exists() or not list(directory.iterdir())


def test_replaced_snapshot_during_evidence_read_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import backfill_plan_artifact as artifact

    snapshot = _snapshot(tmp_path)
    directory = tmp_path / "plans"
    original_build = artifact.build_daily_bar_backfill_plan

    def replace_after_evidence(*args: object, **kwargs: object) -> DailyBarBackfillPlan:
        plan = original_build(*args, **kwargs)
        old = tmp_path / "previous.duckdb"
        os.replace(snapshot, old)
        shutil.copyfile(old, snapshot)
        return plan

    monkeypatch.setattr(artifact, "build_daily_bar_backfill_plan", replace_after_evidence)

    with pytest.raises(ValueError, match="changed|identity|snapshot"):
        _publish(snapshot, directory)

    assert not directory.exists() or not list(directory.iterdir())


def test_repeated_content_publish_is_idempotent_and_corrupt_target_refuses_reuse(
    tmp_path: Path,
) -> None:
    from rquant.backfill_plan_artifact import load_daily_bar_backfill_plan

    snapshot = _snapshot(tmp_path)
    directory = tmp_path / "plans"
    first = _publish(snapshot, directory)
    initial = first.read_bytes()

    assert _publish(snapshot, directory) == first
    assert first.read_bytes() == initial
    assert len(list(directory.iterdir())) == 1

    first.chmod(0o600)
    first.write_bytes(initial.replace(b"production_unverified", b"production_verified", 1))
    with pytest.raises(ValueError, match="invalid|hash|canonical|digest|existing"):
        load_daily_bar_backfill_plan(first)
    with pytest.raises(ValueError, match="invalid|hash|canonical|digest|existing"):
        _publish(snapshot, directory)
    assert first.read_bytes() != initial
    assert len(list(directory.iterdir())) == 1


def test_interrupted_publish_keeps_prior_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import backfill_plan_artifact as artifact

    snapshot = _snapshot(tmp_path)
    directory = tmp_path / "plans"
    previous = _publish(snapshot, directory)
    before = previous.read_bytes()

    def fail_link(_source: str, _target: str) -> None:
        raise OSError("simulated interruption before publication")

    monkeypatch.setattr(artifact.os, "link", fail_link)
    with pytest.raises(OSError, match="simulated interruption"):
        _publish(snapshot, directory, evidence_code_revision="revision-2")

    assert previous.read_bytes() == before
    assert sorted(item.name for item in directory.iterdir()) == [previous.name]


def test_symlinked_snapshot_is_not_accepted(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path)
    alias = tmp_path / "replica-alias.duckdb"
    alias.symlink_to(snapshot)

    with pytest.raises(OSError):
        _publish(alias, tmp_path / "plans")
