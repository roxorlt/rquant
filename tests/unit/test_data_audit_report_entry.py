"""A trusted local caller seals one fixed read-only replica into an audit report."""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from rquant.data_audit_evidence import DailyBarNullFieldSpec
from rquant.data_audit_report import load_data_audit_report
from tests.unit.test_data_audit_report import END, START, _database


def _sources(tmp_path: Path) -> tuple[Path, Path]:
    primary = _database(tmp_path / "primary.duckdb")
    replica = tmp_path / "replica.duckdb"
    shutil.copyfile(primary, replica)
    return primary, replica


def _publish(primary: Path, replica: Path, directory: Path) -> Path:
    from rquant.data_audit_report import create_and_publish_data_audit_report

    return create_and_publish_data_audit_report(
        primary_path=primary,
        replica_path=replica,
        audit_start=START,
        observed_through=END,
        null_fields=(
            DailyBarNullFieldSpec(field_name="close", max_null_numerator=0, max_null_denominator=1),
        ),
        directory=directory,
    )


def test_entry_seals_fixed_replica_with_missing_day_and_unassessed_rules(tmp_path: Path) -> None:
    primary, replica = _sources(tmp_path)
    before = replica.read_bytes()

    published = _publish(primary, replica, tmp_path / "reports")

    report = load_data_audit_report(published)
    assert report.source.mode == "production_unverified"
    assert report.source.namespace == "production"
    assert report.source.replica_generation_id is None
    assert report.source.snapshot_label == f"sha256:{hashlib.sha256(before).hexdigest()}"
    assert report.coverage.snapshot_label == report.source.snapshot_label
    assert report.collection_status == "collection_unconfirmed"
    assert report.collection_completed_through is None
    assert report.coverage_conclusion == "unconfirmed"
    assert report.run_status == "completed"
    assert report.coverage.monthly[0].expected_open_days == 3
    assert report.coverage.monthly[0].covered_open_days == 2
    assert report.coverage.gaps[0].start.isoformat() == "2026-09-28"
    assert all(
        rule.expected_days == (START, report.coverage.gaps[0].start, END)
        for rule in report.quality_rules
    )
    assert all(rule.unassessed for rule in report.quality_rules)
    assert report.quality_conclusion == "not_fully_assessed"
    assert published.name == f"data-audit-v1-{report.content_hash}.json"
    assert _publish(primary, replica, tmp_path / "reports") == published
    assert replica.read_bytes() == before


@pytest.mark.parametrize(
    "alias_kind", ["same_path", "hardlink", "symlink", "directory", "relative"]
)
def test_entry_rejects_primary_alias_and_unsafe_input(tmp_path: Path, alias_kind: str) -> None:
    primary, replica = _sources(tmp_path)
    if alias_kind == "same_path":
        replica = primary
    elif alias_kind == "hardlink":
        replica.unlink()
        os.link(primary, replica)
    elif alias_kind == "symlink":
        replica.unlink()
        replica.symlink_to(primary)
    elif alias_kind == "directory":
        replica = tmp_path
    else:
        replica = Path("replica.duckdb")
    directory = tmp_path / "reports"

    with pytest.raises((OSError, ValueError)):
        _publish(primary, replica, directory)

    assert not directory.exists() or not list(directory.iterdir())


def test_entry_rejects_unsealed_wal_without_publishing(tmp_path: Path) -> None:
    primary, replica = _sources(tmp_path)
    wal = Path(f"{replica}.wal")
    wal.write_bytes(b"pending")
    directory = tmp_path / "reports"

    with pytest.raises(ValueError, match="WAL"):
        _publish(primary, replica, directory)

    assert not directory.exists() or not list(directory.iterdir())


def test_entry_rejects_shm_appearing_during_read_without_publishing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import data_audit_report as artifact

    primary, replica = _sources(tmp_path)
    directory = tmp_path / "reports"
    original_build = artifact.build_data_audit_report

    def add_sidecar_after_read(*args: object, **kwargs: object) -> object:
        result = original_build(*args, **kwargs)
        Path(f"{replica}.shm").write_bytes(b"unsealed")
        return result

    monkeypatch.setattr(artifact, "build_data_audit_report", add_sidecar_after_read)

    with pytest.raises(ValueError, match="replica|sidecar|SHM"):
        _publish(primary, replica, directory)
    assert not directory.exists() or not list(directory.iterdir())


def test_entry_rejects_replica_replacement_during_read_and_keeps_old_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import data_audit_report as artifact

    primary, replica = _sources(tmp_path)
    directory = tmp_path / "reports"
    previous = _publish(primary, replica, directory)
    previous_bytes = previous.read_bytes()
    original_build = artifact.build_data_audit_report

    def replace_after_read(*args: object, **kwargs: object) -> object:
        result = original_build(*args, **kwargs)
        old = tmp_path / "old-replica.duckdb"
        os.replace(replica, old)
        shutil.copyfile(old, replica)
        return result

    monkeypatch.setattr(artifact, "build_data_audit_report", replace_after_read)

    with pytest.raises(ValueError, match="replica|snapshot|changed|identity"):
        _publish(primary, replica, directory)

    assert previous.read_bytes() == previous_bytes
    assert sorted(directory.iterdir()) == [previous]


def test_entry_rejects_in_place_mutation_despite_restored_mtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import data_audit_report as artifact

    primary, replica = _sources(tmp_path)
    initial_stat = replica.stat()
    original_build = artifact.build_data_audit_report

    def mutate_after_read(*args: object, **kwargs: object) -> object:
        result = original_build(*args, **kwargs)
        with replica.open("r+b") as handle:
            handle.seek(-1, os.SEEK_END)
            original = handle.read(1)
            handle.seek(-1, os.SEEK_END)
            handle.write(bytes([original[0] ^ 1]))
        os.utime(replica, ns=(initial_stat.st_atime_ns, initial_stat.st_mtime_ns))
        return result

    monkeypatch.setattr(artifact, "build_data_audit_report", mutate_after_read)
    directory = tmp_path / "reports"

    with pytest.raises(ValueError, match="replica|snapshot|changed|digest|identity"):
        _publish(primary, replica, directory)

    assert not directory.exists() or not list(directory.iterdir())


def _run_fifo_probe(script: str, *paths: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", script, *(str(path) for path in paths)],
        capture_output=True,
        text=True,
        timeout=3,
        check=False,
        env={
            "PATH": os.environ.get("PATH", ""),
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
            "RQUANT_DISABLE_DOTENV": "1",
            "TUSHARE_TOKEN_MAIN": "0" * 40,
        },
    )


def test_entry_refuses_fifo_source_without_blocking_or_replacing_old_report(tmp_path: Path) -> None:
    primary, replica = _sources(tmp_path)
    directory = tmp_path / "reports"
    previous = _publish(primary, replica, directory)
    previous_bytes = previous.read_bytes()
    replica.unlink()
    os.mkfifo(replica)
    script = """
from datetime import date
from pathlib import Path
import sys
from rquant.data_audit_evidence import DailyBarNullFieldSpec
from rquant.data_audit_report import create_and_publish_data_audit_report
try:
    create_and_publish_data_audit_report(
        primary_path=Path(sys.argv[1]), replica_path=Path(sys.argv[2]),
        audit_start=date(2026, 9, 25), observed_through=date(2026, 9, 29),
        null_fields=(DailyBarNullFieldSpec(
            field_name='close', max_null_numerator=0, max_null_denominator=1,
        ),), directory=Path(sys.argv[3]),
    )
except (OSError, ValueError):
    sys.exit(0)
sys.exit(1)
"""

    result = _run_fifo_probe(script, primary, replica, directory)

    assert result.returncode == 0, result.stderr
    assert previous.read_bytes() == previous_bytes
    assert sorted(directory.iterdir()) == [previous]


def test_report_loader_refuses_fifo_without_blocking(tmp_path: Path) -> None:
    fifo = tmp_path / f"data-audit-v1-{'0' * 64}.json"
    os.mkfifo(fifo)
    script = """
from pathlib import Path
import sys
from rquant.data_audit_report import load_data_audit_report
try:
    load_data_audit_report(Path(sys.argv[1]))
except (OSError, ValueError):
    sys.exit(0)
sys.exit(1)
"""

    result = _run_fifo_probe(script, fifo)

    assert result.returncode == 0, result.stderr
