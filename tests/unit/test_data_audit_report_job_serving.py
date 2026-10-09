"""A task and its sealed report enter one unconfirmed Serving generation."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant.data_audit_evidence import DailyBarNullFieldSpec
from rquant.data_audit_report import (
    CatalogDataAuditReport,
    capture_data_audit_replica_identity,
    data_audit_report_path,
    load_data_audit_report,
)
from rquant.data_audit_report_job_projection import read_data_audit_report_job_snapshot
from rquant.data_audit_report_jobs import (
    DataAuditReportJobRequest,
    DataAuditReportJobStore,
    DataAuditReportJobWorker,
    _canonical_request,
)
from rquant.runtime_builder_authority import LabJobsPublisherSettings
from rquant.serving_page_projection_source import (
    DuckDBLabPageProjectionSource,
    PageProjectionSourceIntegrityError,
)
from rquant.serving_read_models import (
    ServingProjectionInput,
    ServingReadModelInput,
    build_serving_read_models,
)
from rquant.storage.duckdb import DuckDBStore
from tests.unit.test_data_audit_report import END, START, _database


class _Clock:
    def __init__(self) -> None:
        self.now = datetime.now(UTC) - timedelta(minutes=20)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, minutes: int) -> None:
        self.now += timedelta(minutes=minutes)


def _fixture(tmp_path: Path) -> tuple[DataAuditReportJobStore, Path, Path, Path, _Clock]:
    primary = _database(tmp_path / "primary.duckdb")
    replica = tmp_path / "replica.duckdb"
    shutil.copyfile(primary, replica)
    research = tmp_path / "research_ro.duckdb"
    with DuckDBStore(research):
        pass
    clock = _Clock()
    state = tmp_path / "audit-jobs.sqlite"
    reports = tmp_path / "reports"
    store = DataAuditReportJobStore(state_path=state, report_directory=reports, clock=clock)
    return store, primary, replica, research, clock


def _request(primary: Path, replica: Path, *, key: str) -> DataAuditReportJobRequest:
    return DataAuditReportJobRequest(
        idempotency_key=key,
        primary_path=primary,
        replica_path=replica,
        replica_file_identity=capture_data_audit_replica_identity(primary, replica),
        audit_start=START,
        observed_through=END,
        null_fields=(
            DailyBarNullFieldSpec(field_name="close", max_null_numerator=0, max_null_denominator=1),
        ),
    )


def _source(research: Path, state: Path, reports: Path) -> DuckDBLabPageProjectionSource:
    return DuckDBLabPageProjectionSource(
        research,
        audit_report_job_state_path=state,
        audit_report_job_directory=reports,
    )


def _rows(source: DuckDBLabPageProjectionSource) -> dict[str, tuple[object, ...]]:
    snapshot = source(datetime.now(UTC))
    return {projection.table_name: projection.rows for projection in snapshot.projections}


def _date_artifact_for_fixture(store: DataAuditReportJobStore, report_hash: str) -> None:
    artifact = data_audit_report_path(store.report_directory, report_hash)
    date = datetime(2026, 10, 1, tzinfo=UTC).timestamp()
    os.utime(artifact, (date, date))


def test_opt_in_requires_paired_paths_and_rejects_explicit_file_mode(tmp_path: Path) -> None:
    research = tmp_path / "research.duckdb"
    state = tmp_path / "audit.sqlite"
    reports = tmp_path / "reports"
    with pytest.raises(ValueError, match="requires|paired"):
        DuckDBLabPageProjectionSource(research, audit_report_job_state_path=state)
    with pytest.raises(ValueError, match="requires|paired"):
        DuckDBLabPageProjectionSource(research, audit_report_job_directory=reports)
    with pytest.raises(ValueError, match="ambiguous|exclusive"):
        DuckDBLabPageProjectionSource(
            research,
            audit_report_path=reports / "old.json",
            audit_report_job_state_path=state,
            audit_report_job_directory=reports,
        )


def test_missing_and_empty_task_state_do_not_claim_a_report(tmp_path: Path) -> None:
    store, _, _, research, _ = _fixture(tmp_path)
    source = _source(research, store.state_path, store.report_directory)
    empty = _rows(source)
    assert empty["audit_report_job"][0]["availability"] == "empty"
    assert empty["audit_report_job_event"] == ()
    assert "audit_report_overview" not in empty
    assert not Path(f"{store.state_path}-wal").exists()
    assert not Path(f"{store.state_path}-shm").exists()

    store.state_path.unlink()
    missing = _rows(source)
    assert missing["audit_report_job"][0]["availability"] == "unavailable"
    assert "audit_report_overview" not in missing


def test_running_task_has_progress_without_a_report(tmp_path: Path) -> None:
    store, primary, replica, research, _ = _fixture(tmp_path)
    queued = store.submit(_request(primary, replica, key="audit-command-0001"))
    assert store._claim() is not None
    rows = _rows(_source(research, store.state_path, store.report_directory))

    job = rows["audit_report_job"][0]
    assert job["availability"] == "ready"
    assert job["latest_task_id"] == queued.task_id
    assert job["latest_status"] == "running"
    assert job["successful_task_id"] is None
    assert [event["event_type"] for event in rows["audit_report_job_event"]] == [
        "queued",
        "started",
    ]
    assert "audit_report_overview" not in rows


def test_v2_success_receipt_artifact_serving_and_api_share_identity(tmp_path: Path) -> None:
    from rquant.web.settings import WebSettings
    from tests.support.web_proxy_identity import ResearcherTestClient, create_private_test_app
    from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

    store, primary, replica, research, _ = _fixture(tmp_path)
    original_source = replica.read_bytes()
    request = _request(primary, replica, key="audit-command-0001")
    queued = store.submit(request)
    finished = DataAuditReportJobWorker(store).run_one()
    assert finished is not None and finished.status == "succeeded"
    assert finished.task_id == queued.task_id and finished.report_hash is not None
    assert store.lookup_by_key(request.idempotency_key) == (request, finished)

    path = data_audit_report_path(store.report_directory, finished.report_hash)
    report = load_data_audit_report(path)
    assert isinstance(report, CatalogDataAuditReport) and len(report.datasets) == 24
    assert path.name == f"data-audit-v2-{finished.report_hash}.json"
    observed = datetime.now(UTC)
    job = read_data_audit_report_job_snapshot(store.state_path, observed_at=observed)
    assert job.successful is not None and job.successful.receipt == finished
    assert report.source.snapshot_label == f"sha256:{job.successful.replica_sha256}"

    snapshot = _source(research, store.state_path, store.report_directory)(observed)
    projections = tuple(
        item for item in snapshot.projections if item.table_name.startswith("audit_report_")
    )
    assert len(projections) == 7
    assert len({item.available_at for item in projections}) == 1
    dataset_rows = next(p.rows for p in projections if p.table_name == "audit_report_dataset")
    assert len(dataset_rows) == 24
    assert {r["report_hash"] for r in dataset_rows} == {finished.report_hash}

    # The existing publisher fixture uses minute sequence offsets. Keep its build
    # later than the real inode times instead of backdating source identities.
    sequence = int((observed - FIXTURE_BUILT_AT).total_seconds() // 60) + 1
    root = tmp_path / "serving"
    manifest = build_web_fixture(
        root, "baseline", sequence=sequence, audit_report_projections=projections
    )
    app = create_private_test_app(
        WebSettings(serving_root=root),
        clock=lambda: manifest.built_at + timedelta(seconds=20),
        background=False,
    )
    with ResearcherTestClient(app) as client:
        response = client.get("/api/v1/data/report", params={"generation": manifest.generation_id})
    assert response.status_code == 200
    payload = response.json()
    assert payload["serving"]["generation_id"] == manifest.generation_id
    assert response.headers["X-Rquant-Generation"] == manifest.generation_id
    data = payload["data"]
    assert data["source_state"] == data["dataset_state"] == "ready"
    assert data["overview"]["schema_version"] == 2
    assert data["overview"]["report_hash"] == finished.report_hash
    assert data["overview"]["current"] is False
    assert data["overview"]["collection_status"] == "collection_unconfirmed"
    assert (
        data["progress"]["successful_task_id"]
        == data["progress"]["latest_task_id"]
        == finished.task_id
    )
    assert data["progress"]["successful_report_hash"] == finished.report_hash
    assert data["progress"]["latest_status"] == "succeeded"
    assert [event["event_type"] for event in data["progress"]["events"]] == [
        "queued",
        "started",
        "source_check",
        "succeeded",
    ]
    assert len(data["datasets"]) == 24
    assert {d["source_id"] for d in data["datasets"]} == {report.source.snapshot_label}
    daily = next(d for d in data["datasets"] if d["dataset_id"] == "daily_bar")
    assert daily["observed_rows"] == 2
    assert isinstance(daily["monthly"][0]["coverage_ratio"], str)
    assert len(data["rules"]) == 3
    assert replica.read_bytes() == original_source


@pytest.mark.parametrize("status", ["queued", "running", "failed"])
def test_latest_task_with_corrupt_request_cannot_publish_ready_progress(
    tmp_path: Path, status: str
) -> None:
    store, primary, replica, research, _ = _fixture(tmp_path)
    queued = store.submit(_request(primary, replica, key="audit-command-0001"))
    if status == "running":
        assert store._claim() is not None
    elif status == "failed":
        replacement = _database(tmp_path / "replacement.duckdb")
        os.replace(replacement, replica)
        result = DataAuditReportJobWorker(store).run_one()
        assert result is not None and result.status == "failed"
    with sqlite3.connect(store.state_path) as connection:
        connection.execute(
            "UPDATE data_audit_report_job SET request_json = ? WHERE task_id = ?",
            ("broken-json", queued.task_id),
        )

    with pytest.raises(PageProjectionSourceIntegrityError, match="audit report job state"):
        _rows(_source(research, store.state_path, store.report_directory))


def test_latest_task_request_hash_mismatch_cannot_publish_ready_progress(tmp_path: Path) -> None:
    store, primary, replica, research, _ = _fixture(tmp_path)
    queued = store.submit(_request(primary, replica, key="audit-command-0001"))
    with sqlite3.connect(store.state_path) as connection:
        connection.execute(
            "UPDATE data_audit_report_job SET request_sha256 = ? WHERE task_id = ?",
            ("0" * 64, queued.task_id),
        )

    with pytest.raises(PageProjectionSourceIntegrityError, match="request hash"):
        _rows(_source(research, store.state_path, store.report_directory))


def test_latest_failure_keeps_verified_older_report_and_separate_times(tmp_path: Path) -> None:
    store, primary, replica, research, clock = _fixture(tmp_path)
    store.submit(_request(primary, replica, key="audit-command-0001"))
    success = DataAuditReportJobWorker(store).run_one()
    assert success is not None and success.report_hash is not None
    _date_artifact_for_fixture(store, success.report_hash)
    clock.advance(10)
    store.submit(_request(primary, replica, key="audit-command-0002"))
    replacement = _database(tmp_path / "replacement.duckdb")
    os.replace(replacement, replica)
    failure = DataAuditReportJobWorker(store).run_one()
    assert failure is not None and failure.status == "failed"

    source = _source(research, store.state_path, store.report_directory)
    observed = datetime.now(UTC)
    snapshot = source(observed)
    projections = {item.table_name: item for item in snapshot.projections}
    job = projections["audit_report_job"].rows[0]
    overview = projections["audit_report_overview"].rows[0]
    assert job["latest_task_id"] == failure.task_id
    assert job["latest_status"] == "failed"
    assert job["latest_error_code"] == "replica_changed"
    assert job["successful_task_id"] == success.task_id
    assert job["successful_report_hash"] == success.report_hash
    assert job["latest_updated_at"] != job["successful_updated_at"]
    assert overview["report_hash"] == success.report_hash
    assert overview["current"] is False
    assert overview["collection_status"] == "collection_unconfirmed"
    assert {
        projections[name].available_at
        for name in (
            "audit_report_job",
            "audit_report_job_event",
            "audit_report_overview",
            "audit_report_month",
            "audit_report_rule",
            "audit_report_issue",
        )
    } == {projections["audit_report_job"].available_at}
    tables = build_serving_read_models(
        ServingReadModelInput(
            observed_at=observed,
            projections=tuple(
                ServingProjectionInput.bind(
                    item, owner_dataset_id="lab_jobs", owner_generation_id="a" * 64
                )
                for item in snapshot.projections
            ),
        )
    )
    assert tables["audit_report_job"].iloc[0]["latest_status"] == "failed"
    assert tables["audit_report_overview"].iloc[0]["current"] == 0


def test_damaged_success_artifact_rejects_entire_generation(tmp_path: Path) -> None:
    store, primary, replica, research, _ = _fixture(tmp_path)
    store.submit(_request(primary, replica, key="audit-command-0001"))
    success = DataAuditReportJobWorker(store).run_one()
    assert success is not None and success.report_hash is not None
    _date_artifact_for_fixture(store, success.report_hash)
    artifact = data_audit_report_path(store.report_directory, success.report_hash)
    artifact.chmod(0o600)
    artifact.write_bytes(artifact.read_bytes().replace(b"collection_unconfirmed", b"completed", 1))

    with pytest.raises(PageProjectionSourceIntegrityError, match="audit report"):
        _rows(_source(research, store.state_path, store.report_directory))


def test_success_report_must_match_stored_request(tmp_path: Path) -> None:
    store, primary, replica, research, _ = _fixture(tmp_path)
    store.submit(_request(primary, replica, key="audit-command-0001"))
    success = DataAuditReportJobWorker(store).run_one()
    assert success is not None and success.report_hash is not None
    _date_artifact_for_fixture(store, success.report_hash)
    with sqlite3.connect(store.state_path) as connection:
        raw = connection.execute(
            "SELECT request_json FROM data_audit_report_job WHERE task_id = ?",
            (success.task_id,),
        ).fetchone()[0]
        request = json.loads(raw)
        request["audit_start"] = (START + timedelta(days=1)).isoformat()
        changed = _canonical_request(DataAuditReportJobRequest.model_validate(request))
        connection.execute(
            "UPDATE data_audit_report_job SET request_json = ?, request_sha256 = ? "
            "WHERE task_id = ?",
            (changed, hashlib.sha256(changed.encode()).hexdigest(), success.task_id),
        )

    with pytest.raises(PageProjectionSourceIntegrityError, match="differs"):
        _rows(_source(research, store.state_path, store.report_directory))


def test_wal_sidecar_and_state_rotation_are_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import data_audit_report_job_projection as projection

    store, _, _, _, _ = _fixture(tmp_path)
    sidecar = Path(f"{store.state_path}-wal")
    sidecar.symlink_to(tmp_path / "elsewhere")
    with pytest.raises(ValueError, match="sidecar|WAL"):
        read_data_audit_report_job_snapshot(
            store.state_path, observed_at=datetime.now(UTC) + timedelta(minutes=1)
        )
    sidecar.unlink()

    original_read = projection._read_snapshot

    def rotate_after_read(*args: object, **kwargs: object):
        snapshot = original_read(*args, **kwargs)
        replacement = tmp_path / "replacement.sqlite"
        replacement.write_bytes(store.state_path.read_bytes())
        os.replace(replacement, store.state_path)
        return snapshot

    monkeypatch.setattr(projection, "_read_snapshot", rotate_after_read)
    with pytest.raises(ValueError, match="rotated|changed"):
        read_data_audit_report_job_snapshot(
            store.state_path, observed_at=datetime.now(UTC) + timedelta(minutes=1)
        )


def test_orphan_sqlite_sidecar_cannot_be_reported_as_empty_state(tmp_path: Path) -> None:
    store, _, _, _, _ = _fixture(tmp_path)
    shm = Path(f"{store.state_path}-shm")
    shm.write_bytes(b"orphan")
    with pytest.raises(ValueError, match="sidecar|WAL"):
        read_data_audit_report_job_snapshot(store.state_path, observed_at=datetime.now(UTC))
    shm.unlink()
    store.state_path.unlink()
    Path(f"{store.state_path}-wal").write_bytes(b"orphan")
    with pytest.raises(ValueError, match="sidecar|WAL"):
        read_data_audit_report_job_snapshot(store.state_path, observed_at=datetime.now(UTC))


def test_latest_and_success_are_read_in_one_sqlite_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import data_audit_report_job_projection as projection

    store, primary, replica, _, clock = _fixture(tmp_path)
    store.submit(_request(primary, replica, key="audit-command-0001"))
    first = DataAuditReportJobWorker(store).run_one()
    assert first is not None and first.status == "succeeded"
    keepalive = sqlite3.connect(store.state_path)
    keepalive.execute("PRAGMA journal_mode = WAL")
    keepalive.execute("SELECT 1 FROM data_audit_report_job").fetchone()
    original = projection._read_latest_success_row

    def insert_between_queries(connection: sqlite3.Connection) -> sqlite3.Row | None:
        clock.advance(10)
        store.submit(_request(primary, replica, key="audit-command-0002"))
        return original(connection)

    monkeypatch.setattr(projection, "_read_latest_success_row", insert_between_queries)
    try:
        snapshot = read_data_audit_report_job_snapshot(
            store.state_path, observed_at=datetime.now(UTC) + timedelta(minutes=1)
        )
    finally:
        keepalive.close()
    assert snapshot.latest is not None and snapshot.latest.task_id == first.task_id
    assert snapshot.successful is not None and snapshot.successful.receipt.task_id == first.task_id


def test_wal_publication_time_cannot_be_backdated_by_task_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import data_audit_report_job_projection as projection

    store, primary, replica, _, _ = _fixture(tmp_path)
    keepalive = sqlite3.connect(store.state_path)
    keepalive.execute("PRAGMA journal_mode = WAL")
    keepalive.execute("SELECT 1 FROM data_audit_report_job").fetchone()
    store.submit(_request(primary, replica, key="audit-command-0001"))
    observed = datetime.now(UTC) - timedelta(minutes=10)
    real_lstat = projection.os.lstat

    def backdate_main_file(path: Path) -> os.stat_result | SimpleNamespace:
        found = real_lstat(path)
        if Path(path) != store.state_path:
            return found
        return SimpleNamespace(
            st_mode=found.st_mode,
            st_dev=found.st_dev,
            st_ino=found.st_ino,
            st_size=found.st_size,
            st_ctime_ns=int((observed - timedelta(minutes=10)).timestamp() * 1e9),
        )

    monkeypatch.setattr(projection.os, "lstat", backdate_main_file)
    try:
        with pytest.raises(ValueError, match="newer|available|future"):
            read_data_audit_report_job_snapshot(store.state_path, observed_at=observed)
    finally:
        keepalive.close()


def test_publisher_settings_require_explicit_paired_job_mode(tmp_path: Path) -> None:
    common = {
        "lab_jobs_path": tmp_path / "jobs.sqlite",
        "authority_root": tmp_path / "authority",
        "research_metadata_path": tmp_path / "research.duckdb",
        "audit_report_job_state_path": tmp_path / "audit-jobs.sqlite",
    }
    with pytest.raises(ValueError, match="paired"):
        LabJobsPublisherSettings(**common)
    with pytest.raises(ValueError, match="ambiguous"):
        LabJobsPublisherSettings(
            **common,
            audit_report_job_directory=tmp_path / "reports",
            audit_report_path=tmp_path / "old.json",
        )
    settings = LabJobsPublisherSettings(**common, audit_report_job_directory=tmp_path / "reports")
    assert settings.audit_report_job_directory == tmp_path / "reports"
