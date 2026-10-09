"""A new durable study publication through original control, seal and public reads."""

from __future__ import annotations

import hashlib
import io
import json
import time
import zipfile
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from tests.integration.test_minute_backtest_parameter_installed import (
    installed_parameters,
    sealed_parameters,
)
from tests.integration.test_minute_backtest_parameter_views import (
    parameter_view,
    test_actual_parameter_job_and_installed_fact_choices as check_public_jobs,
    test_actual_parameter_original_admission_owner_and_full_marker_rejection as check_owner,
    test_actual_parameter_summary_keeps_complete_recipe_broker_performance_and_three_hashes as check_summary,
)
from tests.support.minute_backtest_installed import installed_minute

pytestmark = [
    pytest.mark.usefixtures("installed_parameters"),
    pytest.mark.parametrize("installed_parameters", [True], indirect=True, scope="module"),
]


def test_actual_study_request_publication_worker_seal_and_full_binding(
    sealed_parameters: SimpleNamespace,
) -> None:
    from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalParameters
    from rquant.minute_backtest_parameter_producer import MinuteParameterPreparedPublication
    from rquant.minute_backtest_parameter_runner import minute_parameter_result_tables
    from rquant.runtime_contracts import canonical_sha256
    from rquant.signal_contracts import SignalAction

    case = sealed_parameters
    result = case.full.result
    runtime = result.publication.frozen.runtime
    binding = runtime.study_binding
    assert binding is not None and result.replay.study_binding == binding
    binding.verify_request(case.command.config)
    assert binding.request_hash == canonical_sha256(case.command.config.model_dump(mode="json"))
    assert binding.protocol.source.full_input_hash == case.context.baseline.receipt.frozen.full_input_hash
    assert binding.protocol.source.full_input_hash != case.full.full_input_hash
    assert binding.protocol.parameters == runtime.parameters == case.command.config.parameters
    assert binding.protocol.head.registration_fingerprint == runtime.strategy.registration_fingerprint
    assert binding.protocol.head.executable_fingerprint == runtime.strategy.executable_fingerprint
    assert binding.protocol.random_seed == case.spec.random_seed
    formal = MinuteParameterFormalParameters.model_validate({item.name: item.value for item in case.spec.parameters.arguments})
    prepared = MinuteParameterPreparedPublication.model_validate_json(formal.prepared_publication_json)
    assert prepared.study_binding == binding
    assert prepared.full_input_hash == case.full.full_input_hash
    work = runtime.parameter_work
    assert work.study_prefix_rows and work.study_feature_rows and work.study_selection_rows
    assert result.replay.parameter_work == work and work.work_units <= 20_000
    assert len(result.replay.signals) == len(result.replay.fills) == 2
    assert result.replay.daily_status == "complete" and len(result.replay.daily_valuations) == 3
    assert all(fill.total_fees > 0 for fill in result.replay.fills)
    assert len(minute_parameter_result_tables(result.replay)) == 8
    entries = tuple(signal for signal in result.replay.signals if signal.action is SignalAction.B_INTENT)
    assert entries and all(signal.evidence["minute_study_binding_hash"] == binding.binding_hash for signal in entries)
    assert all(signal.evidence["minute_study_selection"]["source_hash"] == binding.protocol.source.full_input_hash for signal in entries)
    (case.context.root / "actual-study-binding.json").write_text(binding.model_dump_json())


@pytest.mark.parametrize("change", ["profile", "ranges", "request_hash"])
def test_full_formal_result_rejects_substituted_study_binding(
    sealed_parameters: SimpleNamespace, change: str,
) -> None:
    from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalReplayResult

    result = sealed_parameters.full.result
    original = result.replay.study_binding
    body = original.model_dump(mode="python")
    if change == "profile":
        body["protocol"]["score_profile"] = "no_market"
    elif change == "request_hash":
        body["request_hash"] = "f" * 64
    else:
        body["train_range"] = body["validation_range"]
        body["protocol"]["split"]["train_start"] = body["validation_range"]["start_date"]
        body["protocol"]["split"]["train_end"] = body["validation_range"]["end_date"]
        body["validation_range"] = original.train_range.model_dump(mode="python")
        with pytest.raises(ValueError):
            type(original).model_validate(body)
        # Also exercise the formal full-object equality gate independently of
        # the chronology validator, using the already verified real result.
        altered = original.model_copy(update={"validation_range": original.train_range})
    if change != "ranges":
        altered = type(original).model_validate(body)
    swapped = result.model_copy(update={"replay": result.replay.model_copy(update={"study_binding": altered})})
    with pytest.raises(ValueError, match="recipe/work/study"):
        MinuteParameterFormalReplayResult.complete_parameter_result_binding(swapped)


def test_current_parameter_job_sources_and_permission(parameter_view: SimpleNamespace) -> None:
    check_public_jobs(parameter_view)


def test_original_admission_owner_and_complete_marker(parameter_view: SimpleNamespace) -> None:
    check_owner(parameter_view)


def test_current_parameter_full_summary_and_broker_performance(
    parameter_view: SimpleNamespace, tmp_path: Path,
) -> None:
    check_summary(parameter_view, tmp_path)


def test_current_parameter_html_and_full_zip_from_same_real_seal(
    sealed_parameters: SimpleNamespace, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_artifacts import LabJobArtifactStore
    from rquant.minute_backtest_artifact import MINUTE_RESULT_TABLE_NAMES
    from rquant.sealed_result_html import validate_offline_html
    from rquant.web.models.minute_backtests import MinuteExportRequest

    case = sealed_parameters
    prefix = "/api/v1/backtests/minute-runtime"
    client, digest = case.web.client, case.full.complete_result_hash
    started = time.monotonic()
    html = client.get(f"{prefix}/runs/{case.job_id}/report.html", params={"result_hash": digest})
    html_seconds = time.monotonic() - started
    assert html.status_code == 200, html.text
    validate_offline_html(html.content)
    assert "合成验收来源" in html.text and "完整参数与定义语义" in html.text
    body = MinuteExportRequest(command_id=uuid4(), requested_at=case.context.now[0],
        job_id=case.job_id, result_hash=digest).model_dump(mode="json")
    started = time.monotonic()
    created = client.post(prefix + "/exports", json=body, headers={"X-Rquant-Csrf": "1"})
    export_seconds = time.monotonic() - started
    assert created.status_code == 200 and created.json()["status"] == "exported", created.text
    receipt = created.json()
    def no_artifact_writer(*args: object, **kwargs: object) -> None:
        raise AssertionError("GET cannot construct a writer ArtifactStore")
    started = time.monotonic()
    with monkeypatch.context() as scope:
        scope.setattr(LabJobArtifactStore, "__init__", no_artifact_writer)
        content = client.get(f"{prefix}/runs/{case.job_id}/exports/{receipt['zip_request_id']}.zip",
            params={"result_hash": digest})
    download_seconds = time.monotonic() - started
    assert content.status_code == 200, content.text
    assert hashlib.sha256(content.content).hexdigest() == receipt["sha256"]
    with zipfile.ZipFile(io.BytesIO(content.content)) as archive:
        assert archive.read("report.html") == html.content
        assert set(archive.namelist()) == {"manifest.json", "SHA256SUMS", "spec.json", "metrics.json", "report.md", "report.html",
            *("tables/" + name + ".parquet" for name in MINUTE_RESULT_TABLE_NAMES)}
    (tmp_path / "actual-parameter-report-timing.json").write_text(json.dumps({
        "html_seconds": html_seconds, "export_seconds": export_seconds,
        "download_seconds": download_seconds, "html_bytes": len(html.content), "zip_bytes": len(content.content)}))
    denied = client.get(f"{prefix}/runs/{case.job_id}/report.html", params={"result_hash": digest},
        headers={"x-rquant-user": "other-owner"})
    assert denied.status_code in (403, 404)
