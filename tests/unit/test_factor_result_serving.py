"""Only a pinned factor ledger and sealed compact files can publish result rows."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.factor.job_ledger import FactorEvaluationJobLedger
from rquant.lab_jobs import LabJobReader, LabJobStore
from rquant.lab_jobs_serving_authority import (
    LabJobsServingAuthorityIntegrityError,
    LabJobsServingAuthorityPublisher,
    LabJobsServingSourceReader,
    lab_jobs_state_identity,
)
from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
from rquant.runtime_serving_snapshot import LAB_JOBS_DATASET_ID, LabJobsPayload
from rquant.serving_page_projection_source import LabPageProjectionSnapshot
from rquant.serving_read_models import (
    ServingProjectionInput,
    ServingProjectionPayload,
    ServingReadModelInput,
)
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_factor_job_ledger import NOW, _Clock, _sealed


def _success(tmp_path: Path):
    spec, root, completion = _sealed(tmp_path)
    ledger = FactorEvaluationJobLedger(tmp_path / "factor-jobs.sqlite3", clock=_Clock())
    identity = ledger.initialize()
    job = ledger.submit("research-1", spec)
    lease = ledger.claim(lease_seconds=60)
    assert lease is not None
    ledger.complete(job.job_id, lease.lease_token, lease.version, completion, root)
    return identity, root, job.job_id, completion


def test_success_projects_one_verified_display_and_stable_three_table_group(
    tmp_path: Path,
) -> None:
    from rquant.factor.result_serving import (
        project_factor_result_projections,
        validate_factor_result_projections,
    )

    identity, root, job_id, completion = _success(tmp_path)
    projections = project_factor_result_projections(identity, root, available_at=NOW)
    assert {projection.table_name for projection in projections} == {
        "factor_result_state",
        "factor_result_index",
        "factor_result_display",
    }
    verified = validate_factor_result_projections(
        {projection.table_name: projection for projection in projections}
    )
    assert verified.state.job_count == 1
    assert verified.index[0].job_id == job_id
    assert verified.index[0].display_status == "available"
    assert verified.displays[0].content_sha256 == completion.display_artifact_sha256
    assert verified.displays[0].full_artifact_sha256 == completion.artifact_sha256
    assert verified.displays[0].definition_content_sha256 == (
        verified.index[0].definition_content_sha256
    )
    assert project_factor_result_projections(identity, root, available_at=NOW) == projections


def test_empty_ledger_is_trusted_empty_but_missing_display_fails_closed(tmp_path: Path) -> None:
    from rquant.factor.result_serving import (
        project_factor_result_projections,
        validate_factor_result_projections,
    )

    empty_root = tmp_path / "empty"
    empty_root.mkdir(mode=0o700)
    empty_ledger = FactorEvaluationJobLedger(empty_root / "jobs.sqlite3", clock=_Clock())
    identity = empty_ledger.initialize()
    artifacts = empty_root / "artifacts"
    artifacts.mkdir(mode=0o700)
    projections = project_factor_result_projections(identity, artifacts, available_at=NOW)
    verified = validate_factor_result_projections(
        {projection.table_name: projection for projection in projections}
    )
    assert verified.state.status == "empty"
    assert verified.index == ()
    assert verified.displays == ()

    success_root = tmp_path / "success"
    success_root.mkdir(mode=0o700)
    success_identity, root, _job_id, completion = _success(success_root)
    (root / completion.display_artifact_filename).unlink()
    with pytest.raises((OSError, ValueError)):
        project_factor_result_projections(success_identity, root, available_at=NOW)


def test_projection_refuses_an_unzoned_observation_clock(tmp_path: Path) -> None:
    from rquant.factor.result_serving import project_factor_result_projections

    identity, root, _job_id, _completion = _success(tmp_path)
    with pytest.raises(ValueError, match="timezone"):
        project_factor_result_projections(identity, root, available_at=NOW.replace(tzinfo=None))


def test_lab_and_serving_reject_partial_group_and_accept_complete_group(tmp_path: Path) -> None:
    from rquant.factor.result_serving import project_factor_result_projections

    identity, root, _job_id, _completion = _success(tmp_path)
    group = project_factor_result_projections(identity, root, available_at=NOW)
    page = LabPageProjectionSnapshot.create(available_at=NOW, factor_result_projections=group)
    assert {item.table_name for item in page.projections} >= {
        "factor_result_state",
        "factor_result_index",
        "factor_result_display",
    }
    LabJobsPayload(projections=group)
    bound = tuple(
        ServingProjectionInput.bind(
            projection, owner_dataset_id="lab_jobs", owner_generation_id="a" * 64
        )
        for projection in group
    )
    ServingReadModelInput(observed_at=NOW, projections=bound)
    with pytest.raises(ValueError, match="complete"):
        LabPageProjectionSnapshot.create(available_at=NOW, factor_result_projections=group[:1])
    with pytest.raises(ValueError, match="incomplete"):
        LabJobsPayload(projections=group[:1])
    with pytest.raises(ValueError, match="incomplete"):
        ServingReadModelInput(observed_at=NOW, projections=bound[:1])


def test_lab_source_reads_factor_results_twice_and_ignores_only_observation_time(
    tmp_path: Path,
) -> None:
    from rquant.factor.result_serving import FactorResultProjectionReader

    factor_root = tmp_path / "factor"
    factor_root.mkdir(mode=0o700)
    identity, artifacts, _job_id, _completion = _success(factor_root)
    jobs = LabJobStore(tmp_path / "lab-jobs.sqlite3")
    jobs.initialize()
    source = LabJobsServingSourceReader(
        reader=LabJobReader(jobs.path),
        factor_result_projection_reader=FactorResultProjectionReader(identity, artifacts),
    )
    first = source(NOW)
    second = source(NOW.replace(second=1))
    assert lab_jobs_state_identity(first) == lab_jobs_state_identity(second)
    assert {item.table_name for item in first.payload.projections} >= {
        "factor_result_state",
        "factor_result_index",
        "factor_result_display",
    }

    calls = 0

    def changing_reader(observed_at, *, other_projections):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        group = FactorResultProjectionReader(identity, artifacts)(
            observed_at, other_projections=other_projections
        )
        return group if calls == 1 else ()

    unstable = LabJobsServingSourceReader(
        reader=LabJobReader(jobs.path), factor_result_projection_reader=changing_reader
    )
    with pytest.raises(LabJobsServingAuthorityIntegrityError, match="factor result"):
        unstable(NOW)


def test_lab_publication_ignores_observation_only_but_detects_new_jobs(tmp_path: Path) -> None:
    from rquant.factor.result_serving import FactorResultProjectionReader

    factor_root = tmp_path / "factor"
    factor_root.mkdir(mode=0o700)
    identity, artifacts, _job_id, _completion = _success(factor_root)
    jobs = LabJobStore(tmp_path / "lab-jobs.sqlite3")
    jobs.initialize()
    authority = LabJobsServingAuthorityPublisher(
        reader=LabJobsServingSourceReader(
            reader=LabJobReader(jobs.path),
            factor_result_projection_reader=FactorResultProjectionReader(identity, artifacts),
        ),
        publisher=ServingSourceAuthorityPublisher(
            root=tmp_path / "authority",
            producer_commit="a" * 40,
            dataset_id=LAB_JOBS_DATASET_ID,
            payload_kind="lab_jobs",
            clock=lambda: NOW + timedelta(minutes=1),
        ),
    )
    first = authority.publish(NOW)
    repeated = authority.publish(NOW + timedelta(seconds=1))
    assert first.written is True
    assert repeated.written is False
    assert repeated.pointer == first.pointer

    ledger = FactorEvaluationJobLedger.open_existing(identity, clock=_Clock())
    previous = ledger.get(_job_id)
    assert previous is not None
    ledger.submit("research-2", previous.spec.model_copy(update={"code_revision": "d" * 40}))
    changed = authority.publish(NOW + timedelta(seconds=2))
    assert changed.written is True
    assert changed.pointer != first.pointer


def test_unfinished_index_cannot_claim_a_completed_source(tmp_path: Path) -> None:
    from rquant.factor.result_serving import FactorResultIndexRow, project_factor_result_projections

    identity, root, _job_id, _completion = _success(tmp_path)
    group = project_factor_result_projections(identity, root, available_at=NOW)
    row = next(item for item in group if item.table_name == "factor_result_index").rows[0]
    unfinished = {
        **row,
        "status": "running",
        "display_status": "not_ready",
        "result_sha256": None,
        "full_artifact_sha256": None,
        "display_artifact_sha256": None,
        "display_byte_count": None,
        "completion_sha256": None,
    }
    with pytest.raises(ValidationError, match="unfinished factor index"):
        FactorResultIndexRow.model_validate_json(json.dumps(unfinished))
    with pytest.raises(ValidationError, match="failure code"):
        FactorResultIndexRow.model_validate_json(
            json.dumps(
                {
                    **unfinished,
                    "source_sha256": None,
                    "status": "failed",
                    "failure_code": None,
                }
            )
        )


def test_running_failed_and_legacy_success_have_no_invented_chart(tmp_path: Path) -> None:
    from rquant.factor.job_ledger import _COLUMNS
    from rquant.factor.result_serving import (
        project_factor_result_projections,
        validate_factor_result_projections,
    )

    for status in ("running", "failed", "legacy"):
        case = tmp_path / status
        case.mkdir(mode=0o700)
        spec, artifacts, completion = _sealed(case)
        ledger = FactorEvaluationJobLedger(case / "jobs.sqlite3", clock=_Clock())
        identity = ledger.initialize()
        job = ledger.submit("research-1", spec)
        lease = ledger.claim(lease_seconds=60)
        assert lease is not None
        if status == "failed":
            ledger.fail(job.job_id, lease.lease_token, lease.version, "evaluation_failed")
        elif status == "legacy":
            old = canonical_json_bytes(
                completion.model_dump(
                    mode="json",
                    round_trip=True,
                    exclude={
                        "display_artifact_sha256",
                        "display_artifact_filename",
                        "display_artifact_byte_count",
                    },
                )
            ).decode()
            with sqlite3.connect(ledger.path) as connection:
                connection.row_factory = sqlite3.Row
                raw = connection.execute(
                    "SELECT * FROM factor_jobs WHERE job_id = ?", (job.job_id,)
                ).fetchone()
                payload = {column: raw[column] for column in _COLUMNS}
                payload.update(
                    status="succeeded",
                    version=lease.version + 1,
                    updated_at=NOW.isoformat(timespec="microseconds"),
                    lease_token=None,
                    lease_expires_at=None,
                    completion_json=old,
                )
                digest = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
                connection.execute(
                    "UPDATE factor_jobs SET status = ?, version = ?, updated_at = ?, "
                    "lease_token = NULL, lease_expires_at = NULL, completion_json = ?, "
                    "row_sha256 = ? WHERE job_id = ?",
                    (
                        "succeeded",
                        lease.version + 1,
                        payload["updated_at"],
                        old,
                        digest,
                        job.job_id,
                    ),
                )
        group = project_factor_result_projections(identity, artifacts, available_at=NOW)
        snapshot = validate_factor_result_projections({item.table_name: item for item in group})
        assert snapshot is not None
        row = snapshot.index[0]
        assert row.status == ("succeeded" if status == "legacy" else status)
        assert row.display_status == ("display_unavailable" if status == "legacy" else "not_ready")
        assert snapshot.chunks == ()
        assert snapshot.displays == ()


def test_large_valid_display_splits_and_corrupt_chunks_fail(tmp_path: Path) -> None:
    from rquant.factor.display_artifact import FactorDisplayArtifactV1, load_factor_display_artifact
    from rquant.factor.result_serving import (
        _chunks,
        _projections,
        _snapshot,
        project_factor_result_projections,
        validate_factor_result_projections,
    )

    identity, root, job_id, completion = _success(tmp_path)
    base = project_factor_result_projections(identity, root, available_at=NOW)
    snapshot = validate_factor_result_projections({item.table_name: item for item in base})
    assert snapshot is not None
    display = load_factor_display_artifact(root, completion.display_artifact_sha256)
    expanded = display.model_copy(update={"coverage_days": display.coverage_days * 1024})
    unsigned = expanded.model_dump(mode="json", exclude={"content_sha256"})
    big = FactorDisplayArtifactV1.model_validate(
        {
            **expanded.model_dump(mode="python", exclude={"content_sha256"}),
            "content_sha256": hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest(),
        }
    )
    data = canonical_json_bytes(big.model_dump(mode="json", round_trip=True))
    assert 64 * 1024 < len(data) < 4 * 1024 * 1024
    row = snapshot.index[0].model_copy(
        update={
            "display_artifact_sha256": big.content_sha256,
            "display_byte_count": len(data),
        }
    )
    chunks = _chunks(job_id, big)
    assert len(chunks) > 2
    complete = _snapshot(
        available_at=NOW,
        ledger_instance_id=identity.instance_id,
        index=(row,),
        chunks=chunks,
    )
    verified = validate_factor_result_projections(
        {item.table_name: item for item in _projections(complete)}
    )
    assert verified is not None and verified.displays[0] == big
    for changed in (
        chunks[:-1],
        tuple(reversed(chunks)),
        (chunks[0], *chunks),
        (chunks[0].model_copy(update={"data_b64": "?"}), *chunks[1:]),
        (chunks[0].model_copy(update={"chunk_count": len(chunks) + 1}), *chunks[1:]),
    ):
        with pytest.raises(ValueError):
            _snapshot(
                available_at=NOW,
                ledger_instance_id=identity.instance_id,
                index=(row,),
                chunks=changed,
            )


def test_owner_budget_uses_existing_lab_bytes_and_marks_older_chart_unpublished(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.factor.result_serving as serving

    identity, root, _job_id, _completion = _success(tmp_path)
    full = serving.project_factor_result_projections(identity, root, available_at=NOW)
    checked = serving.validate_factor_result_projections({item.table_name: item for item in full})
    assert checked is not None
    bare = serving._projections(
        serving._snapshot(
            available_at=NOW,
            ledger_instance_id=identity.instance_id,
            index=(checked.index[0].model_copy(update={"display_status": "not_published"}),),
            chunks=(),
        )
    )
    existing = (ServingProjectionPayload(table_name="lab_job_event", available_at=NOW, rows=()),)
    minimum = serving._owner_bytes((*existing, *bare))
    monkeypatch.setattr(serving, "_OWNER_BUDGET", minimum)
    group = serving.project_factor_result_projections(
        identity, root, available_at=NOW, other_projections=existing
    )
    snapshot = serving.validate_factor_result_projections({item.table_name: item for item in group})
    assert snapshot is not None
    assert snapshot.index[0].display_status == "not_published"
    assert snapshot.displays == ()
    monkeypatch.setattr(serving, "_OWNER_BUDGET", minimum - 1)
    with pytest.raises(ValueError, match="mandatory"):
        serving.project_factor_result_projections(
            identity, root, available_at=NOW, other_projections=existing
        )


def test_display_file_tamper_or_different_source_rejects_publication(tmp_path: Path) -> None:
    from rquant.factor.result_serving import project_factor_result_projections

    identity, root, _job_id, completion = _success(tmp_path)
    target = root / completion.display_artifact_filename
    original = target.read_bytes()
    target.write_bytes(original[:-1])
    with pytest.raises((OSError, ValueError)):
        project_factor_result_projections(identity, root, available_at=NOW)
    target.write_bytes(original)
    target.chmod(0o600)
    wrong = identity.model_copy(update={"instance_id": "f" * 32})
    with pytest.raises((OSError, ValueError, RuntimeError)):
        project_factor_result_projections(wrong, root, available_at=NOW)
    target.unlink()
    real = root / "original-display.json"
    real.write_bytes(original)
    real.chmod(0o600)
    target.symlink_to(real)
    with pytest.raises((OSError, ValueError)):
        project_factor_result_projections(identity, root, available_at=NOW)


def test_reassembled_chart_cannot_claim_a_different_source_or_definition(
    tmp_path: Path,
) -> None:
    from rquant.factor.display_artifact import FactorDisplayArtifactV1, load_factor_display_artifact
    from rquant.factor.result_serving import (
        _chunks,
        _snapshot,
        project_factor_result_projections,
        validate_factor_result_projections,
    )

    identity, root, job_id, completion = _success(tmp_path)
    group = project_factor_result_projections(identity, root, available_at=NOW)
    snapshot = validate_factor_result_projections({item.table_name: item for item in group})
    assert snapshot is not None
    original = load_factor_display_artifact(root, completion.display_artifact_sha256)
    for change in (
        {"source_sha256": "f" * 64},
        {"code_revision": "f" * 40},
    ):
        modified = original.model_copy(update=change)
        unsigned = modified.model_dump(mode="json", exclude={"content_sha256"})
        modified = FactorDisplayArtifactV1.model_validate(
            {
                **modified.model_dump(mode="python", exclude={"content_sha256"}),
                "content_sha256": hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest(),
            }
        )
        data = canonical_json_bytes(modified.model_dump(mode="json", round_trip=True))
        row = snapshot.index[0].model_copy(
            update={
                "display_artifact_sha256": modified.content_sha256,
                "display_byte_count": len(data),
            }
        )
        with pytest.raises(ValueError, match="indexed research"):
            _snapshot(
                available_at=NOW,
                ledger_instance_id=identity.instance_id,
                index=(row,),
                chunks=_chunks(job_id, modified),
            )
