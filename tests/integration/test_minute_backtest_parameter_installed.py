"""Actual parameter publication, original control, worker and physical finalizer."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator
from datetime import UTC, date, datetime, time as local_time, timedelta
from html.parser import HTMLParser
from types import SimpleNamespace
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest

from tests.support.minute_backtest_installed import installed_minute

if TYPE_CHECKING:
    from rquant.minute_backtest_parameters import MinuteParameterSet


def _auction_public_recipe() -> MinuteParameterSet:
    from rquant.minute_backtest_parameters import MinuteAuctionGapParameters, MinuteParameterSet

    return MinuteParameterSet(parameters=MinuteAuctionGapParameters(
        start_date="2026-07-31", end_date="2026-08-04", min_auction_vol_ratio_5d=0.3,
        max_auction_vol_ratio_5d=2.1, entry_pullback_tolerance_pct=0.013,
        entry_vwap_buffer_pct=0.0001, min_limit_progress_pct=0.02,
        next_auction_weak_gap_pct=-0.025, strong_seal_min_close_minutes=12,
        strong_seal_weak_gap_pct=-0.04, next_morning_exit_until=local_time(10, 15),
        next_morning_vwap_break_buffer_pct=0.009, seal_hold_max_days=8,
        seal_hold_max_open_times=2, seal_hold_min_fd_to_circ_pct=0.12,
        paper={"candidate_id": "explicit-auction-public", "stop_loss_pct": 0.012345,
            "take_profit_pct": 0.123456, "trailing_stop_pct": 0.034567,
            "entry_buffer_pct": 0.004567, "entry_slippage_pct": 0.0002}))


@pytest.fixture(scope="module")
def installed_parameters(request: pytest.FixtureRequest) -> Iterator[SimpleNamespace]:
    from rquant.metadata_catalog import ImmutableDuckDBMetadataCatalog
    from rquant.minute_backtest_installation import load_minute_replay_installation
    from rquant.minute_backtest_parameter_producer import (
        MinuteParameterFactSourceReference, MinuteParameterReplayCatalog, publish_minute_parameter_input,
    )
    from rquant.research_catalog import ResearchCatalog
    from rquant.storage.duckdb import DuckDBStore
    from tests.support.minute_parameter_formal_fixture import parameter_source_seed

    requested_case = getattr(request, "param", False)
    public_family = requested_case == "auction_gap"
    if not isinstance(requested_case, bool) and not public_family:
        raise ValueError("unknown explicit parameter fixture case")
    retained = os.environ.get("MINUTE_PARAMETER_REUSE_INSTALLATION")
    if public_family and retained is not None:
        raise ValueError("family public acceptance requires a fresh test-owned temporary fixture")
    if retained is not None:
        from pathlib import Path
        from rquant.lab_jobs import LabJobStore

        now = [datetime.now(UTC)]
        path = Path(retained)
        writer = load_minute_replay_installation(path, expected_code_sha="a" * 40, writable=True, clock=lambda: now[0])
        profile = writer.profile
        readonly = load_minute_replay_installation(path, expected_code_sha=profile.code_sha, clock=lambda: now[0])
        reference, = profile.parameter_catalog.fact_sources
        receipt = profile.parameter_catalog.resolve_fact(source_key=reference.source_key, source_version=reference.source_version,
            owner_id=reference.owner_id, full_input_hash=reference.full_input_hash)
        patch = pytest.MonkeyPatch()
        patch.setattr("rquant.storage.duckdb._settings", lambda: SimpleNamespace(primary_writer_gate_path=None))
        try:
            yield SimpleNamespace(root=path.parent, profile=profile, path=path, writer=writer, readonly=readonly,
                jobs=LabJobStore(profile.lab_jobs_path), now=now,
                baseline=SimpleNamespace(receipt=receipt, reference=reference),
                published=SimpleNamespace(receipt=receipt, reference=reference),
                sealer_calls=["reused original gate74 synthetic credential transport installation"])
        finally:
            patch.undo()
        return
    context = request.getfixturevalue("installed_minute")
    study_enabled = requested_case if isinstance(requested_case, bool) else False
    suffix = "-auction-public" if public_family else ""
    root = context.root / ("complete-parameter-facts" + suffix)
    root.mkdir(mode=0o700)
    seed = parameter_source_seed(root / "originals", parameters=_auction_public_recipe() if public_family else None,
        days=(date(2026, 7, 31), date(2026, 8, 3), date(2026, 8, 4)),
        sparse=True, study_facts=study_enabled)
    with DuckDBStore(root / "metadata.duckdb") as metadata:
        metadata.path.chmod(0o600)
        baseline = publish_minute_parameter_input(seed, metadata_store=metadata,
            source_path=root / "source.duckdb", receipt_path=root / "receipt.json",
            catalog=ResearchCatalog(root / "catalog.duckdb"), lake_root=context.profile.research_lake_root,
            installed_policies=(seed.provenance.visibility_policy,), now=seed.provenance.published_at)
    with ImmutableDuckDBMetadataCatalog.open(root / "metadata.duckdb", snapshot_root=context.profile.snapshot_root) as metadata:
        identity = metadata.descriptor
    fact = MinuteParameterFactSourceReference(**baseline.reference.model_dump(mode="python"),
        full_input_hash=baseline.receipt.frozen.full_input_hash, metadata_identity=identity,
        display_name="三日完整合成参数事实", source_nature="synthetic_validation",
        supported_parameter_names=("paper.stop_loss_pct",))
    catalog = MinuteParameterReplayCatalog(fact_sources=(fact,),
        prepared_root=context.profile.runtime_root / "minute-parameter-prepared",
        snapshot_root=context.profile.snapshot_root, research_lake_root=context.profile.research_lake_root,
        forbidden_paths=context.profile.forbidden_paths, installed_policies=(seed.provenance.visibility_policy,))
    profile = context.profile.model_copy(update={"parameter_catalog": catalog})
    path = context.root / ("installed-parameters" + suffix + ".json")
    path.write_text(profile.model_dump_json(exclude_computed_fields=True))
    path.chmod(0o600)
    context.now[0] = datetime.now(UTC)
    writer = load_minute_replay_installation(path, expected_code_sha=profile.code_sha, writable=True, clock=lambda: context.now[0])
    readonly = load_minute_replay_installation(path, expected_code_sha=profile.code_sha, clock=lambda: context.now[0])
    yield SimpleNamespace(root=context.root, profile=profile, path=path, writer=writer, readonly=readonly,
        jobs=context.jobs, now=context.now, baseline=baseline, published=baseline, sealer_calls=context.sealer_calls,
        study_enabled=study_enabled, public_family=public_family)


@pytest.fixture(scope="module")
def sealed_parameters(installed_parameters: SimpleNamespace) -> Iterator[SimpleNamespace]:
    from rquant.experiment_registry import DateRange
    from rquant.lab_artifact_preview import ArtifactPreviewReader
    from rquant.lab_artifact_protocol import LabArtifactCommitSpool, LabFinalizerAuthorityKey
    from rquant.lab_artifacts import LabJobArtifactStore
    from rquant.lab_finalizer import LabFinalizer
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.lab_jobs import JobStatus, LabResultState
    from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool, LabShardSucceeded, LabShardTelemetry, LabWorkerReport
    from rquant.lab_worker import LabWorker
    from rquant.lab_worker_registry import execute_builtin_lab_shard
    from rquant.minute_backtest_commands import MinuteParameterRunConfig, SubmitMinuteReplay, minute_job_id
    from rquant.minute_backtest_formal import MinuteExperimentProtocol
    from rquant.minute_backtest_installation import load_minute_replay_installation
    from rquant.minute_backtest_parameter_artifact import MinuteParameterSealedReplayReader
    from rquant.minute_backtest_parameters import MinuteParameterSet
    from tests.integration.test_minute_backtest_web_submission import web_minute

    context = installed_parameters
    web_fixture = web_minute.__wrapped__(context)
    web = next(web_fixture)
    try:
        original = context.baseline.receipt.frozen.runtime
        parameters = MinuteParameterSet(parameters=original.parameters.parameters.model_copy(update={
            "paper": original.parameters.parameters.paper.model_copy(update={"stop_loss_pct": 0.012346})}))
        days = original.daily_trade_dates
        config = MinuteParameterRunConfig(source_key=original.source_key, source_version=original.source_version,
            full_input_hash=context.baseline.receipt.frozen.full_input_hash, parameters=parameters,
            protocol=MinuteExperimentProtocol(train_range=DateRange(start_date=days[0], end_date=days[0]),
                validation_range=DateRange(start_date=days[1], end_date=days[1]),
                frozen_outer_test_range=DateRange(start_date=days[2], end_date=days[2])),
            deadline=context.now[0] + timedelta(hours=1))
        if getattr(context, "study_enabled", False):
            from rquant.minute_backtest_parameter_study import MinuteParameterStudySettings

            config = config.model_copy(update={"study": MinuteParameterStudySettings(
                score_profile="v1", top_n=1, min_trades=1)})
        command = SubmitMinuteReplay(command_id=str(uuid4()), requested_at=context.now[0], actor_id=web.owner, config=config)
        job_id = minute_job_id(web.owner, command.command_id)
        public_request_json = None
        if getattr(context, "public_family", False):
            from rquant.web.models.minute_backtests import MinuteCreateRequest

            create = MinuteCreateRequest(command_id=UUID(command.command_id), requested_at=command.requested_at,
                config=command.config)
            public_request_json = create.model_dump_json()
            (context.root / "parameter-family-public-create-request.json").write_text(public_request_json)
            first = web.client.post("/api/v1/backtests/minute-runtime/runs", content=public_request_json,
                headers={"X-Rquant-Csrf": "1", "Content-Type": "application/json"})
            (context.root / "parameter-family-public-create-response.json").write_text(json.dumps({
                "status_code": first.status_code, "body": first.text}, indent=2))
            assert first.status_code == 200, first.text
            receipt = first.json()
            assert receipt["status"] == "submitted" and UUID(receipt["job_id"]) == job_id, receipt
            again = web.client.post("/api/v1/backtests/minute-runtime/runs", content=public_request_json,
                headers={"X-Rquant-Csrf": "1", "Content-Type": "application/json"})
            (context.root / "parameter-family-public-repeat-response.json").write_text(json.dumps({
                "status_code": again.status_code, "body": again.text}, indent=2))
            assert again.status_code == 200 and again.json() == receipt
            assert len(web.posted) == 2 and web.posted[0] == web.posted[1] == command.model_dump(mode="json")
        else:
            proof = web.roles.issue_authorization(web.owner, command.model_dump(mode="json"))
            first = web.control.submit_authorized(command, proof)
            assert first.status == "succeeded", first
            again = web.control.submit_authorized(command, proof)
            assert again == first
        spool = LabCommandSpool(context.profile.command_spool_path)
        entry, = spool.pending()
        assert entry.envelope.command.job_id == job_id
        lease = context.jobs.acquire_scheduler_lease(owner_id="synthetic-parameter-scheduler", lease_seconds=3600, now=context.now[0])
        accepted = context.jobs.apply_command(entry.envelope, lease=lease, now=context.now[0],
            submission_authority=lambda envelope, at: context.writer.commands.validate_prepared_experiment_submission(envelope, observed_at=at))
        assert accepted.status == "applied"
        spool.ack(entry, accepted)
        spec = entry.envelope.command.spec
        assert spec.parameters.strategy_name == "minute_parameter_replay"
        context.jobs.plan_job(job_id, context.writer.registry.plan(spec), lease=lease, now=context.now[0])
        claim = context.jobs.claim_next_shard(worker_id="synthetic-parameter-worker", shard_lease_seconds=3600, lease=lease, now=context.now[0])
        assert claim is not None and claim.job_id == job_id
        validated = context.writer.registry.validate_claim(claim)
        started = time.monotonic()
        actual = execute_builtin_lab_shard(json.loads(context.writer.shard_manifest.registry.configuration_json),
            validated, runtime_code_sha=spec.code_sha)
        finished = time.monotonic()
        commits = LabArtifactCommitSpool(context.root / "parameter-artifact-commits")
        key = LabFinalizerAuthorityKey(key_id="synthetic-parameter-finalizer", secret=b"synthetic-offline-test-key-only-32bytes")
        claims, reports = LabClaimSpool(context.root / "parameter-claims"), LabReportSpool(context.root / "parameter-reports")
        claims.publish(claim)
        artifacts = LabJobArtifactStore(context.profile.final_artifact_root)
        worker = LabWorker(worker_id=claim.worker_id, claim_spool=claims, report_spool=reports,
            artifact_root=context.root / "parameter-shard-artifacts", shard_runtime_manifest=context.writer.shard_manifest,
            verified_code_sha_provider=lambda: spec.code_sha, clock=lambda: context.now[0])
        try:
            manifest = worker._seal_result(claim, actual)
            telemetry = LabShardTelemetry.from_work_plan(claim.definition.work_plan,
                monotonic_started=started, monotonic_finished=finished)
            report = LabWorkerReport.from_claim(claim, report_id=uuid4(), reported_at=context.now[0],
                body=LabShardSucceeded.current(result_manifest_hash=manifest.manifest_hash, worker_code_sha=spec.code_sha, telemetry=telemetry))
            report_entry = reports.publish(report)
            success = context.jobs.apply_worker_report(report, lease=lease, now=context.now[0])
            assert success.status == "accepted"
            reports.ack(report_entry, success)
            assert context.writer.reader.get_artifact_preview_authority(job_id) is None
            finalizer = LabFinalizer(reader=context.writer.reader, shard_artifact_root=worker.artifact_root,
                artifact_store=artifacts, commit_spool=commits, verified_code_sha_provider=lambda: spec.code_sha,
                finalizer_authority_key_provider=lambda: key, adapter_registry=context.writer.registry)
            finalized = finalizer.finalize(job_id)
            assert finalized.status == "published"
            commit, = commits.pending()
            with artifacts.bind_verified_sealed(commit.envelope.commit.sealed_path, indexed_at=context.now[0]) as binding:
                with context.jobs.stage_artifact_commit(commit.envelope, binding,
                    authority_key_provider=lambda key_id: key if key_id == key.key_id else None,
                    lease=lease, now=context.now[0]) as staged:
                    committed = staged.commit(lease=lease, now=context.now[0])
            assert committed.status == "accepted"
            commits.ack(commit, committed)
            job = context.writer.reader.get_job(job_id)
            assert job.status is JobStatus.SUCCEEDED and job.result_state is LabResultState.SEALED
            fresh = load_minute_replay_installation(context.path, expected_code_sha=spec.code_sha, clock=lambda: context.now[0])
            results = MinuteParameterSealedReplayReader(reader=fresh.reader,
                artifact_reader=ArtifactPreviewReader(reader=fresh.reader, artifact_root=fresh.authority.final_artifact_root),
                submission_facade=fresh.parameter_submission_facade(spec), catalog=fresh.profile.parameter_catalog)
            full = results.read(job_id, owner_id=web.owner, native_id=parameters.definition_id,
                native_version=parameters.definition_version, as_of=context.now[0])
            assert full is not None
            (context.root / "parameter-sealed-full.json").write_text(full.model_dump_json(exclude_computed_fields=True))
            (context.root / "parameter-sealed-control.json").write_text(json.dumps({"synthetic": True,
                "credential_sealer_transport": "synthetic", "installation_path": str(context.path),
                "job_id": str(job_id), "actor_id": web.owner, "command": command.model_dump(mode="json"),
                "spec": spec.model_dump(mode="json"), "worker_seconds": finished-started,
                "public_request_json": public_request_json,
                "finalized": finalized.model_dump(mode="json"), "committed": committed.model_dump(mode="json")}, indent=2))
            yield SimpleNamespace(context=context, full=full, job_id=job_id, spec=spec, command=command, web=web)
        finally:
            worker.close()
            artifacts.close()
    finally:
        web_fixture.close()


def test_actual_parameter_pagecontrol_worker_finalizer_and_complete_eight_tables(sealed_parameters: SimpleNamespace) -> None:
    value = sealed_parameters
    replay = value.full.result.replay
    assert replay.parameters == value.command.config.parameters
    assert replay.daily_status == "complete"
    assert len(replay.daily_valuations) == 3
    assert replay.signals and replay.fills and all(fill.total_fees > 0 for fill in replay.fills)
    assert value.full.complete_result_hash
    from rquant.minute_backtest_parameter_runner import minute_parameter_result_tables

    tables = minute_parameter_result_tables(replay)
    assert len(tables) == 8
    assert all(not table.empty for table in tables.values())


class _ReportFields(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.fields: dict[str, str] = {}
        self._field: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "td":
            self._field = dict(attrs).get("data-field")
            self._text = []

    def handle_data(self, data: str) -> None:
        if self._field is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "td" and self._field is not None:
            self.fields[self._field] = "".join(self._text)
            self._field = None


@pytest.mark.parametrize("installed_parameters", ["auction_gap"], indirect=True, scope="module")
def test_actual_auction_public_request_worker_finalizer_full8_and_original_report(
    sealed_parameters: SimpleNamespace,
) -> None:
    from rquant.minute_backtest_parameter_runner import minute_parameter_result_tables

    value = sealed_parameters
    root = value.context.root
    recipe = value.command.config.parameters
    replay = value.full.result.replay
    assert recipe.parameters.family == "auction_gap"
    assert replay.parameters.model_dump_json() == recipe.model_dump_json()
    assert replay.strategy_id == recipe.definition_id and recipe.definition_id.startswith("ap.")
    arguments = {item.name: item.value for item in value.spec.parameters.arguments}
    assert json.loads(arguments["parameter_set_json"]) == recipe.model_dump(mode="json")
    assert arguments["parameter_hash"] == recipe.fingerprint
    assert arguments["native_strategy_id"] == recipe.definition_id
    assert arguments["source_frequency"] == recipe.parameters.freq
    assert replay.daily_status == "complete" and len(replay.daily_valuations) == 3
    assert replay.signals and replay.fills and all(fill.total_fees > 0 for fill in replay.fills)
    tables = minute_parameter_result_tables(replay)
    expected_tables = {"signals", "orders", "fills", "paper_queue", "account",
        "daily_valuations", "execution_profile", "replay_summary"}
    assert set(tables) == expected_tables and all(not table.empty for table in tables.values())
    summary_response = value.web.client.get(f"/api/v1/backtests/minute-runtime/runs/{value.job_id}")
    (root / "parameter-family-public-summary.json").write_text(summary_response.text)
    assert summary_response.status_code == 200, summary_response.text
    summary = summary_response.json()["data"]
    assert summary["job"]["parameters"] == summary["source"]["parameters"] == recipe.model_dump(mode="json")
    assert summary["job"]["native_id"] == summary["source"]["native_id"] == recipe.definition_id
    assert summary["job"]["parameter_hash"] == summary["source"]["parameter_hash"] == recipe.fingerprint
    assert summary["source"]["evaluator_semantic_version"] == "2.0.0"
    assert summary["source"]["full_input_hash"] == value.full.full_input_hash
    assert summary["source"]["core_input_hash"] == value.full.core_input_hash
    assert summary["source"]["seed_hash"] == value.full.seed_hash
    assert summary["result_hash"] == value.full.complete_result_hash
    assert set(summary["tables"]) == expected_tables
    response = value.web.client.get(f"/api/v1/backtests/minute-runtime/runs/{value.job_id}/report.html",
        params={"result_hash": value.full.complete_result_hash})
    (root / "parameter-family-original-report.html").write_bytes(response.content)
    (root / "parameter-family-original-report-response.json").write_text(json.dumps({
        "status_code": response.status_code, "generation": response.headers.get("X-Rquant-Generation")}, indent=2))
    assert response.status_code == 200, response.text
    assert response.headers["X-Rquant-Generation"] == value.full.complete_result_hash
    fields = _ReportFields()
    fields.feed(response.text)
    expected_fields = {"parameters.kind": "minute-parameter-set", "parameters.schema_version": "1",
        "parameters.parameters.family": "auction_gap", "parameters.parameters.freq": "1min",
        "parameters.parameters.entry_pullback_tolerance_pct": "0.013",
        "parameters.parameters.next_morning_exit_until": "10:15:00",
        "parameters.parameters.strong_seal_min_close_minutes": "12",
        "parameters.parameters.seal_hold_max_days": "8",
        "parameters.parameters.seal_hold_min_fd_to_circ_pct": "0.12",
        "parameters.parameters.paper.candidate_id": "explicit-auction-public",
        "parameters.parameters.paper.stop_loss_pct": "0.012346",
        "parameters.parameters.paper.take_profit_pct": "0.123456",
        "parameters.parameters.paper.trailing_stop_pct": "0.034567",
        "parameters.parameters.paper.entry_buffer_pct": "0.004567",
        "parameters.parameters.paper.entry_slippage_pct": "0.0002",
        "evaluator_semantic_version": "2.0.0"}
    assert {name: fields.fields.get(name) for name in expected_fields} == expected_fields
    (root / "parameter-family-public-evidence.json").write_text(json.dumps({"synthetic": True,
        "family": "auction_gap", "job_id": str(value.job_id), "native_id": recipe.definition_id,
        "parameter_hash": recipe.fingerprint, "complete_result_hash": value.full.complete_result_hash,
        "table_rows": {name: len(table) for name, table in tables.items()},
        "manifest": value.full.manifest.model_dump(mode="json"), "report_fields": expected_fields,
        "require_next_day_decision": "not changed or certified by this acceptance"}, indent=2))
