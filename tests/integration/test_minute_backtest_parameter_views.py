"""Read-only public bindings of the actual parameter worker/finalizer seal."""

from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from tests.integration.test_minute_backtest_parameter_installed import installed_parameters, sealed_parameters


@pytest.fixture(scope="module")
def parameter_view(request: pytest.FixtureRequest) -> SimpleNamespace:
    from rquant.minute_backtest_installation import load_minute_replay_installation
    from rquant.web.minute_backtest_service import MinuteWebService

    checkpoint = os.environ.get("MINUTE_PARAMETER_SEAL_CHECKPOINT")
    if checkpoint is None:
        sealed = request.getfixturevalue("sealed_parameters")
        root, installation_path = sealed.context.root, sealed.context.path
        job_id, owner, spec_hash = sealed.job_id, sealed.full.owner_id, sealed.spec.spec_hash
    else:
        index = json.loads((Path(checkpoint) / "index.json").read_text())
        root, installation_path = Path(index["physical_root"]), Path(index["installation_path"])
        control = json.loads((Path(checkpoint) / "parameter-sealed-control.json").read_text())
        job_id, owner = UUID(index["job_id"]), index["actor_id"]
        from rquant.research_run_spec import ResearchRunSpec

        spec_hash = ResearchRunSpec.model_validate_json(json.dumps(control["spec"])).spec_hash
    now = datetime.now(UTC)
    installed = load_minute_replay_installation(installation_path, expected_code_sha="a" * 40,
        clock=lambda: now)
    return SimpleNamespace(root=root, installation=installed, service=MinuteWebService(installed),
        job_id=job_id, owner=owner, spec_hash=spec_hash)


def test_actual_parameter_job_and_installed_fact_choices(parameter_view: SimpleNamespace) -> None:
    from rquant.web.models.minute_backtests import MinuteParameterJob

    view = parameter_view
    job = view.service.job(view.job_id, owner_id=view.owner)
    assert type(job) is MinuteParameterJob
    assert job.status == "completed" and job.spec_hash == view.spec_hash
    assert job.native_id == job.parameters.definition_id
    assert job.parameter_hash == job.parameters.fingerprint
    assert any(item.job_id == view.job_id for item in view.service.jobs(owner_id=view.owner, limit=50, cursor=None).jobs)
    with pytest.raises(LookupError):
        view.service.job(view.job_id, owner_id="other-owner")
    sources = view.service.parameter_sources(owner_id=view.owner)
    assert sources.available and sources.sources
    assert not view.service.parameter_sources(owner_id="other-owner").sources
    source, = sources.sources
    assert source.source_nature == "synthetic_validation" and source.frequency == "1min"
    capability, = source.capabilities
    assert capability.family == "n_shape"
    assert capability.supported_parameter_names == ("paper.stop_loss_pct",)
    assert "path" not in source.model_dump() and "head" not in source.model_dump()
    assert view.service.capabilities(owner_id=view.owner, can_write=True).can_run
    assert not view.service.capabilities(owner_id=view.owner, can_write=False).can_run


def _original_owner_row(view: SimpleNamespace) -> tuple[dict[str, object], object]:
    from rquant.collaboration_commands import PageControlRoleAuthority
    from rquant.page_control import PageControlOutbox

    path = view.root / "page-control.sqlite3"
    authority = PageControlRoleAuthority(mode="enforced", roles_path=view.root / "roles.json")
    authority.bind_outbox(path)
    assert authority.current_role(view.owner) == "researcher"
    with sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("BEGIN")
        rows = tuple(PageControlOutbox._result_submission_rows(connection, "submit_minute_replay"))
    row = next(item for item in rows if json.loads(item["result_json"])["job_id"] == str(view.job_id))
    return dict(row), authority


def test_actual_parameter_original_admission_owner_and_full_marker_rejection(parameter_view: SimpleNamespace) -> None:
    from rquant.page_control import PageControlOutbox

    view = parameter_view
    row, authority = _original_owner_row(view)
    proof = PageControlOutbox._result_submission_binding(row, authority)
    assert proof is not None and (proof.owner_id, proof.job_id, proof.spec_hash) == (
        view.owner, str(view.job_id), view.spec_hash)
    marker = json.loads(row["original_admission_json"])
    marker["config_hash"] = "b" * 64
    with pytest.raises(PermissionError):
        PageControlOutbox._result_submission_binding(row | {"original_admission_json": json.dumps(marker)}, authority)


def test_actual_parameter_summary_keeps_complete_recipe_broker_performance_and_three_hashes(
    parameter_view: SimpleNamespace, tmp_path: Path,
) -> None:
    from rquant.web.models.minute_backtests import MinuteParameterJob, MinuteParameterResultSource

    view = parameter_view
    started = time.monotonic()
    summary = view.service.summary(view.job_id, owner_id=view.owner)
    elapsed = time.monotonic() - started
    assert type(summary.job) is MinuteParameterJob and type(summary.source) is MinuteParameterResultSource
    assert summary.source.parameters == summary.job.parameters
    assert summary.source.parameter_hash == summary.job.parameter_hash
    assert summary.source.full_input_hash == summary.job.full_input_hash
    assert summary.source.baseline_full_input_hash != summary.source.full_input_hash
    assert summary.source.core_input_hash and summary.source.seed_hash
    assert summary.signal_count == summary.fill_count == 2
    assert summary.daily_status == "complete" and summary.can_report
    assert summary.performance is not None and len(summary.performance.daily) == 3
    assert len(summary.tables) == 8
    (tmp_path / "actual-parameter-summary.json").write_text(summary.model_dump_json())
    (tmp_path / "actual-parameter-summary-timing.json").write_text(json.dumps({"elapsed_seconds": elapsed}))
