from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from rquant.dashboard.lab.job_center import StrategyLabJobCenterController
from rquant.lab_artifact_export import LabJobZipExportFacade
from rquant.lab_artifact_preview import ArtifactPreviewReader
from rquant.lab_artifacts import LabJobArtifactStore
from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandSpool
from rquant.lab_jobs import JobStatus, LabJobListFilters, LabJobReader
from tests.unit.test_lab_finalizer import _ready_scenario


def test_completed_job_survives_page_lifecycle_and_remains_exportable(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    runtime_root.mkdir(mode=0o700)
    scenario = _ready_scenario(runtime_root, hold_days=(1,))
    published = scenario.finalizer().finalize(scenario.job_id)
    accepted = scenario.scheduler.run_once()
    scenario.scheduler.release()

    assert published.status == "published"
    assert accepted.artifact_commits_accepted == 1

    reader = LabJobReader(scenario.store.path)
    artifact_store = LabJobArtifactStore(runtime_root / "job-artifacts")
    controller = StrategyLabJobCenterController(
        reader=reader,
        commands=LabCommandSubmissionFacade(
            reader=reader,
            spool=LabCommandSpool(runtime_root / "commands"),
        ),
        preview_reader=ArtifactPreviewReader(
            reader=reader,
            artifact_root=runtime_root / "job-artifacts",
        ),
        zip_exports=LabJobZipExportFacade(
            reader=reader,
            artifact_store=artifact_store,
            export_root=runtime_root / "exports",
            max_export_records=4,
        ),
    )

    page = controller.list_jobs(
        filters=LabJobListFilters(statuses=(JobStatus.SUCCEEDED,)),
        page_size=20,
    )
    detail = controller.get_job_detail(scenario.job_id, as_of=datetime.now(UTC))
    preview = controller.preview_artifact(scenario.job_id, table_name="trades")
    receipt = controller.export_zip(scenario.job_id)

    assert [item.job_id for item in page.items] == [scenario.job_id]
    assert detail is not None
    assert detail.job.status is JobStatus.SUCCEEDED
    assert preview.table is not None
    assert preview.table.table_name == "trades"
    assert preview.table.total_rows == 1
    assert receipt.path.is_file()
    assert receipt.path.stat().st_size == receipt.byte_size

    controller.discard_zip(receipt)

    assert not receipt.path.exists()
    tombstones = tuple(receipt.path.parent.glob("*.discarded"))
    assert len(tombstones) == 1
    assert tombstones[0].stat().st_size == 0
