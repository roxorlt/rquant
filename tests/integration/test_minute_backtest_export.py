from __future__ import annotations

import hashlib
import io
import json
import zipfile
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from rquant.lab_artifact_export import LabJobZipExportFacade
from rquant.lab_artifacts import LabJobArtifactStore
from rquant.minute_backtest_artifact import MINUTE_RESULT_TABLE_NAMES
from rquant.minute_backtest_export import MinuteReportReader, MinuteZipExportFacade
from rquant.sealed_result_html import validate_offline_html
from tests.integration.test_minute_backtest_web_submission import sealed_web_minute, web_minute
from tests.support.minute_backtest_installed import installed_minute


def test_original_physical_eight_tables_and_owner_produce_complete_deterministic_report_zip(
    sealed_web_minute: SimpleNamespace, tmp_path: Path,
) -> None:
    case = sealed_web_minute
    reader = MinuteReportReader(case.context.readonly, owner_authority=case.web.control)
    original_root, minute_root = tmp_path / "stock-exports", tmp_path / "minute-exports"
    original_root.mkdir(mode=0o700)
    minute_root.mkdir(mode=0o700)
    artifacts = LabJobArtifactStore(case.context.profile.final_artifact_root)
    try:
        original = LabJobZipExportFacade(reader=reader.installation.reader, artifact_store=artifacts, export_root=original_root)
        facade = MinuteZipExportFacade(reader=reader.installation.reader, artifact_store=artifacts, report_reader=reader,
            original_exports=original, export_root=minute_root)
        report = reader.read(case.job_id, owner_id=case.web.owner, expected_result_hash=case.full.complete_result_hash)
        validate_offline_html(report.report.html_bytes())
        assert report.sealed == case.full
        assert report.owner.command_kind == "submit_minute_replay"
        assert report.report.performance.metrics.round_trips[0].net_pnl == pytest.approx(-743.49)
        request = uuid4()
        first = facade.export_minute(case.job_id, owner_id=case.web.owner, request_id=request,
            expected_result_hash=case.full.complete_result_hash)
        content = facade.read_bytes(first, owner_id=case.web.owner)
        assert hashlib.sha256(content).hexdigest() == first.sha256
        assert first.full_input_hash == case.full.full_input_hash
        assert first.core_input_hash == case.full.core_input_hash and first.seed_hash == case.full.seed_hash
        assert first.html_sha256 == report.report.html_sha256
        assert first.owner_binding_hash == report.owner.content_sha256
        assert facade.recover_minute(case.job_id, owner_id=case.web.owner, request_id=request,
            expected_result_hash=case.full.complete_result_hash) == first
        assert facade.export_minute(case.job_id, owner_id=case.web.owner, request_id=request,
            expected_result_hash=case.full.complete_result_hash) == first
        second = facade.export_minute(case.job_id, owner_id=case.web.owner, request_id=uuid4(),
            expected_result_hash=case.full.complete_result_hash)
        assert facade.read_bytes(second, owner_id=case.web.owner) == content
        stock_path = next(original_root.glob("*/*/result.zip"))
        with zipfile.ZipFile(stock_path) as stock, zipfile.ZipFile(io.BytesIO(content)) as exported:
            expected = {"manifest.json", "SHA256SUMS", "spec.json", "metrics.json", "report.md", "report.html",
                *("tables/" + name + ".parquet" for name in MINUTE_RESULT_TABLE_NAMES)}
            assert set(exported.namelist()) == expected and len(exported.infolist()) == len(expected)
            assert all(exported.read(name) == stock.read(name) for name in stock.namelist())
            assert exported.read("report.html") == report.report.html_bytes()
            assert all(info.date_time == (1980, 1, 1, 0, 0, 0) for info in exported.infolist())
        for actor in ("other-owner", "viewer", "admin"):
            with pytest.raises((LookupError, PermissionError)):
                reader.read(case.job_id, owner_id=actor, expected_result_hash=case.full.complete_result_hash)
            with pytest.raises((LookupError, PermissionError)):
                facade.read_bytes(first, owner_id=actor)
        with pytest.raises(ValueError):
            facade.recover_minute(case.job_id, owner_id=case.web.owner, request_id=request, expected_result_hash="0" * 64)
    finally:
        artifacts.close()


def test_actual_installed_asgi_reports_and_pagecontrol_export_original_eight_tables(
    sealed_web_minute: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.minute_backtest_commands import ExportMinuteReplayZip, minute_zip_request_id
    from rquant.web.models.minute_backtests import MinuteExportRequest
    from tests.unit.test_minute_backtest_producer import NOW

    case, prefix = sealed_web_minute, "/api/v1/backtests/minute-runtime"
    client, result_hash = case.web.client, case.full.complete_result_hash
    summary = client.get(f"{prefix}/runs/{case.job_id}")
    assert summary.status_code == 200, summary.text
    data = summary.json()["data"]
    assert data["job"]["native_name"] == data["source"]["native_name"] == "N 字形态"
    assert data["performance"]["status"] == "complete" and data["can_report"] is True
    assert data["performance"]["metrics"]["round_trips"][0]["net_pnl"] == pytest.approx(-743.49)
    html = client.get(f"{prefix}/runs/{case.job_id}/report.html", params={"result_hash": result_hash})
    assert html.status_code == 200, html.text
    validate_offline_html(html.content)
    assert html.headers["cache-control"] == "no-store" and "attachment" in html.headers["content-disposition"]
    create = MinuteExportRequest(command_id=uuid4(), requested_at=NOW, job_id=case.job_id, result_hash=result_hash)
    body = create.model_dump(mode="json")
    first = client.post(prefix + "/exports", json=body, headers={"X-Rquant-Csrf": "1"})
    assert first.status_code == 200 and first.json()["status"] == "exported", first.text
    receipt = first.json()
    command = ExportMinuteReplayZip.model_validate_json(json.dumps(body | {"actor_id": case.web.owner}))
    assert receipt["zip_request_id"] == str(minute_zip_request_id(command))
    original = case.web.outbox.effect(str(create.command_id))
    assert original is not None and original.status.value == "succeeded"
    assert original.result["full_input_hash"] == case.full.full_input_hash
    assert original.result["core_input_hash"] == case.full.core_input_hash
    assert original.result["seed_hash"] == case.full.seed_hash
    again = client.post(prefix + "/exports", json=body, headers={"X-Rquant-Csrf": "1"})
    assert again.status_code == 200 and again.json() == receipt
    installed_root = case.context.profile.runtime_root
    def private_files() -> dict[str, tuple[int, int, int, str]]:
        return {str(path): (path.stat().st_ino, path.stat().st_mode, path.stat().st_mtime_ns,
            hashlib.sha256(path.read_bytes()).hexdigest()) for path in installed_root.rglob("*") if path.is_file()}
    def private_directories() -> dict[str, tuple[int, int, int, int]]:
        return {str(path): (path.stat().st_dev, path.stat().st_ino, path.stat().st_mode, path.stat().st_mtime_ns)
            for path in (installed_root, *installed_root.rglob("*")) if path.is_dir()}
    before = private_files()
    directory_before = private_directories()
    def writer_unavailable(*args: object, **kwargs: object) -> None:
        raise AssertionError("GET cannot construct a writer ArtifactStore")
    with monkeypatch.context() as scope:
        scope.setattr(LabJobArtifactStore, "__init__", writer_unavailable)
        download = client.get(f"{prefix}/runs/{case.job_id}/exports/{receipt['zip_request_id']}.zip", params={"result_hash": result_hash})
    after = private_files()
    assert private_directories() == directory_before
    assert set(after) == set(before)
    coordination = {
        str(case.context.profile.lab_jobs_path) + "-shm",
        str(case.context.readonly.authority.experiment_registry_path) + "-shm",
    }
    changed_times = []
    for path, initial in before.items():
        current = after[path]
        assert (current[0], current[1], current[3]) == (initial[0], initial[1], initial[3])
        if current[2] != initial[2]:
            # The original mode=ro WAL readers may touch their existing SHM
            # read-lock timestamp. No persistent bytes or directory is changed.
            assert path in coordination
            changed_times.append({"path": path, "before_mtime_ns": initial[2], "after_mtime_ns": current[2],
                "unchanged_inode": current[0], "unchanged_mode": current[1], "unchanged_sha256": current[3]})
    print("minute_zip_get_original_read_coordination=" + json.dumps(changed_times, sort_keys=True))
    assert download.status_code == 200, download.text
    assert hashlib.sha256(download.content).hexdigest() == receipt["sha256"]
    assert len(download.content) == receipt["byte_size"]
    with zipfile.ZipFile(io.BytesIO(download.content)) as archive:
        assert archive.read("report.html") == html.content
        assert set(archive.namelist()) == {"manifest.json", "SHA256SUMS", "spec.json", "metrics.json", "report.md", "report.html",
            *("tables/" + name + ".parquet" for name in MINUTE_RESULT_TABLE_NAMES)}
    for actor in ("other-owner", "viewer", "admin"):
        actor_headers = {"x-rquant-user": actor}
        assert client.get(f"{prefix}/runs/{case.job_id}/report.html", params={"result_hash": result_hash}, headers=actor_headers).status_code in (403, 404)
        assert client.get(f"{prefix}/runs/{case.job_id}/exports/{receipt['zip_request_id']}.zip", params={"result_hash": result_hash}, headers=actor_headers).status_code in (403, 404)
    failed = client.post(prefix + "/exports", json=body | {"command_id": str(uuid4()), "result_hash": "0" * 64}, headers={"X-Rquant-Csrf": "1"})
    assert failed.status_code == 200 and failed.json()["status"] == "failed"
    conflict = client.post(prefix + "/exports", json=body | {"result_hash": "0" * 64}, headers={"X-Rquant-Csrf": "1"})
    assert conflict.status_code == 409 and conflict.json()["status"] == "conflict"
