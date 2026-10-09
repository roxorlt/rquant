"""Complete owned facts use the original audited immutable Research snapshot."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import date, datetime
from pathlib import Path

import duckdb

from rquant.data_metadata import DataAuditRun, DataAuditRunFinalization, DatasetCoverage, DatasetSnapshot, DatasetSnapshotFinalization
from rquant.paper_research import (FrozenPaperResearchInput, PAPER_RESEARCH_INPUT_CONTRACT, PAPER_RESEARCH_INPUT_TABLE,
                                    PAPER_RESEARCH_TASKS, PaperResearchCatalog, NativePaperResearchAdapterCatalog,
                                    PaperResearchRunParameters)
from rquant.paper_research_adapter import read_paper_research_input, write_paper_research_input
from rquant.research_catalog import ResearchCatalog
from rquant.research_gate import ResearchGateDecision, ResearchGateRequest
from rquant.research_run_spec import DatasetSnapshotIdentity
from rquant.research_snapshot import ResearchExecutionSession, build_dataset_snapshot_binding
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_dependencies import strategy_execution_dependencies

_AUDIT_RULE = "paper-research-source/v1"
_CODE_FILES = ("paper_research.py", "paper_research_adapter.py", "paper_research_source.py", "paper_reconcile.py",
               "paper_portfolio_band.py", "paper_portfolio_ledger.py", "paper_portfolio_models.py", "paper_broker.py",
               "paper_contracts.py", "paper_ledger_anchor.py", "portfolio/exposure.py", "portfolio/drawdown.py", "portfolio/weights.py",
               "paper_research_submission.py", "paper_research_commands.py", "paper_research_artifact.py",
               "paper_backtest_source.py", "paper_portfolio_view_source.py", "paper_portfolio_exposure_source.py",
               "paper_portfolio_views.py", "paper_portfolio_state.py", "paper_portfolio_runtime.py")
_NATIVE_EXTRA_CODE_FILES = ("strategy_promotion_contracts.py", "paper_research_runtime.py",
    "strategy_live_service.py", "runtime_builder_strategy.py", "runtime_service_builtin.py", "strategy_runner.py", "runtime_paper_quote.py",
    "paper_execution_constraints.py", "live_spool.py", "live_contracts.py", "minute_backtest_artifact.py",
    "minute_backtest_formal.py", "minute_backtest_publication_contracts.py", "minute_backtest_contracts.py",
    "strategy_promotion_evidence.py", "experiment_platform_evidence.py", "experiment_platform_projection.py",
    "runtime_builder_paper.py", "minute_backtest_validation.py")
_NATIVE_CODE_FILES = _CODE_FILES + _NATIVE_EXTRA_CODE_FILES


def paper_research_code_identity(*, native: bool = False) -> str:
    root = Path(__file__).resolve().parent
    names = _NATIVE_CODE_FILES if native else _CODE_FILES
    return canonical_sha256({name: hashlib.sha256((root/name).read_bytes()).hexdigest() for name in names})


def verify_paper_snapshot_source(connection: duckdb.DuckDBPyConnection, *, task_name: str, code_sha: str,
                                 start_date: date, end_date: date, input_hash: str, as_of: datetime,
                                 catalog: PaperResearchCatalog | None = None) -> FrozenPaperResearchInput:
    if task_name not in PAPER_RESEARCH_TASKS:
        raise ValueError("paper source requires one of the two exact tasks")
    value = read_paper_research_input(connection, input_hash=input_hash)
    if (value.task_name, value.code_sha, value.dates, value.catalog.source_code_identity) != (
            task_name, code_sha, (start_date, end_date),
            paper_research_code_identity(native=isinstance(value.catalog, NativePaperResearchAdapterCatalog))):
        raise ValueError("paper complete source differs from its task, code or original dates")
    if value.available_at > as_of or (catalog is not None and value.catalog != catalog):
        raise ValueError("paper source is future or outside its exact metadata catalog")
    return value


def paper_research_watermarks(value: FrozenPaperResearchInput, *, audit_id: str) -> dict[str, str]:
    parameters = PaperResearchRunParameters.from_input(value, request_id="00000000-0000-4000-8000-000000000000")
    config, binding = value.catalog.configuration, value.catalog.configuration.binding
    return {"manifest_start_date": value.dates[0].isoformat(), "manifest_end_date": value.dates[1].isoformat(),
            "paper_input_hash": value.fingerprint, "paper_task": value.task_name, "paper_owner": binding.owner_id,
            "paper_account": binding.account_id, "paper_configuration": config.fingerprint, "paper_version": str(config.version),
            "paper_strategy": binding.strategy_id, "paper_strategy_version": binding.strategy_version,
            "paper_parameters": binding.parameter_fingerprint, "paper_cost": binding.cost_spec_id,
            "paper_metadata": canonical_sha256(value.catalog.metadata_identity), "paper_code": value.catalog.source_code_identity,
            "paper_audit": audit_id, "paper_work_units": str(parameters.work_units),
            "paper_days": str(len(value.band.comparison_dates) if value.band is not None else 1),
            "paper_source": value.reconcile.fingerprint if value.reconcile is not None else value.band.fingerprint}


class PublishedPaperResearchInput(RuntimeContractModel):
    input_hash: str
    identity: DatasetSnapshotIdentity
    gate_decision: ResearchGateDecision


def publish_paper_research_input(value: FrozenPaperResearchInput, *, metadata_store: DuckDBStore, source_path: Path,
                                 research_catalog: ResearchCatalog, lake_root: Path, now: datetime) -> PublishedPaperResearchInput:
    value = FrozenPaperResearchInput.model_validate(value.model_dump(mode="python"))
    if source_path.exists() or source_path.is_symlink():
        raise ValueError("paper producer requires a new private source path")
    source_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with duckdb.connect(str(source_path)) as connection:
        os.chmod(source_path, 0o600)
        write_paper_research_input(connection, value)
        verify_paper_snapshot_source(connection, task_name=value.task_name, code_sha=value.code_sha, start_date=value.dates[0],
                                     end_date=value.dates[1], input_hash=value.fingerprint, as_of=now, catalog=value.catalog)
        audit = DataAuditRun.create(as_of_date=now.date(), range_start=value.dates[0], range_end=value.dates[1], observed_at=now,
                                    rule_set_version=_AUDIT_RULE+":"+value.fingerprint)
        metadata_store.begin_data_audit_run(audit)
        snapshot = DatasetSnapshot.create(strategy_name=value.task_name, manifest_id=value.fingerprint, as_of_time=now,
                                           code_commit=value.code_sha, origin="trusted-paper-producer", created_at=now)
        metadata_store.begin_dataset_snapshot(snapshot)
        marks = paper_research_watermarks(value, audit_id=audit.audit_run_id)
        for scope, count in (("paper_input", 1), ("paper_days", int(marks["paper_days"])), ("paper_work_units", int(marks["paper_work_units"]))):
            metadata_store.upsert_dataset_coverage(DatasetCoverage(snapshot_id=snapshot.snapshot_id, dataset_id=PAPER_RESEARCH_INPUT_TABLE,
                                                                  table_name=PAPER_RESEARCH_INPUT_TABLE, coverage_scope=scope,
                                                                  expected_count=count, available_count=count, created_at=now))
        metadata_store.finalize_dataset_snapshot(snapshot.snapshot_id, DatasetSnapshotFinalization(table_watermarks=marks, completed_at=now))
        binding = build_dataset_snapshot_binding(metadata_store=metadata_store, source_connection=connection, catalog=research_catalog,
                                                lake_root=lake_root, snapshot_id=snapshot.snapshot_id, start_date=value.dates[0], end_date=value.dates[1],
                                                dependencies=strategy_execution_dependencies(value.task_name), now=lambda: now)
    metadata_store.finalize_data_audit_run(audit.audit_run_id, DataAuditRunFinalization(p0_count=0, completed_at=now))
    request = ResearchGateRequest(mode="exploratory", strategy_name=value.task_name, start_date=value.dates[0], end_date=value.dates[1],
                                  code_commit=value.code_sha, audit_run_id=audit.audit_run_id,
                                  dataset_snapshot_id=snapshot.snapshot_id, dataset_binding_hash=binding.binding_hash)
    with ResearchExecutionSession(binding=binding, lake_root=lake_root) as session:
        verify_bound_paper_input(metadata_store, request, session, catalog=value.catalog)
    return PublishedPaperResearchInput(input_hash=value.fingerprint,
                                       identity=DatasetSnapshotIdentity(snapshot_id=snapshot.snapshot_id, binding_hash=binding.binding_hash, audit_run_id=audit.audit_run_id),
                                       gate_decision=require_paper_gate(metadata_store, request, catalog=value.catalog))


def require_paper_gate(store: DuckDBStore, request: ResearchGateRequest, *, catalog: PaperResearchCatalog) -> ResearchGateDecision:
    snapshot = store.get_dataset_snapshot(request.dataset_snapshot_id)
    audit = store.get_data_audit_run(request.audit_run_id)
    binding = store.get_dataset_snapshot_binding(request.dataset_snapshot_id)
    if snapshot is None or audit is None or binding is None or request.mode != "exploratory" or request.strategy_name not in PAPER_RESEARCH_TASKS:
        raise PermissionError("paper original immutable source is unavailable")
    marks, manifest = snapshot.table_watermarks, binding.manifest
    config = catalog.configuration
    if (snapshot.status != "ready" or snapshot.quality_issue_ids
            or (snapshot.strategy_name, snapshot.code_commit, snapshot.manifest_id, marks.get("paper_owner"), marks.get("paper_configuration"),
                marks.get("paper_metadata"), marks.get("paper_code"), marks.get("manifest_start_date"), marks.get("manifest_end_date")) != (
                request.strategy_name, request.code_commit, marks.get("paper_input_hash"), config.binding.owner_id, config.fingerprint,
                canonical_sha256(catalog.metadata_identity), catalog.source_code_identity, request.start_date.isoformat(), request.end_date.isoformat())):
        raise PermissionError("paper snapshot differs from exact original metadata or owner")
    if (audit.status != "completed" or audit.p0_count or audit.finding_issue_ids or audit.audit_run_id != marks.get("paper_audit")
            or audit.rule_set_version != _AUDIT_RULE+":"+marks.get("paper_input_hash", "")
            or (audit.range_start, audit.range_end) != (request.start_date, request.end_date)):
        raise PermissionError("paper audit differs from its complete original input")
    if (binding.status != "ready" or binding.binding_hash != request.dataset_binding_hash
            or (manifest.strategy_name, manifest.code_commit, manifest.as_of_time, manifest.start_date, manifest.end_date,
                manifest.dependency_contract_version, manifest.eligibility_resolution_hash) != (
                snapshot.strategy_name, snapshot.code_commit, snapshot.as_of_time, request.start_date, request.end_date,
                PAPER_RESEARCH_INPUT_CONTRACT, None)
            or len(manifest.artifacts) != 1 or (manifest.artifacts[0].dataset_id, manifest.artifacts[0].table_name,
                                             manifest.artifacts[0].row_count, manifest.artifacts[0].artifact_type) != (
                                             PAPER_RESEARCH_INPUT_TABLE, PAPER_RESEARCH_INPUT_TABLE, 1, "materialized_table")):
        raise PermissionError("paper immutable artifact binding differs")
    scopes = {"paper_input": "1", "paper_days": marks.get("paper_days"), "paper_work_units": marks.get("paper_work_units")}
    coverages = store.list_dataset_coverages(snapshot.snapshot_id)
    if (len(coverages) != 3 or {item.coverage_scope for item in coverages} != set(scopes)
            or any((item.dataset_id, item.table_name, str(item.expected_count), item.available_count, bool(item.missing_reasons)) != (
                    PAPER_RESEARCH_INPUT_TABLE, PAPER_RESEARCH_INPUT_TABLE, scopes[item.coverage_scope], item.expected_count, False) for item in coverages)
            or store.list_open_data_quality_issues(severities=("P0",))):
        raise PermissionError("paper complete source coverage differs")
    counts = {item.coverage_scope: (item.available_count, item.expected_count) for item in coverages}
    return ResearchGateDecision(allowed=True, research_status="exploratory", audit_run_id=audit.audit_run_id,
                                dataset_snapshot_id=snapshot.snapshot_id, dataset_binding_hash=binding.binding_hash,
                                coverage_counts=counts, coverage_ratios={key: 1.0 for key in counts}, failures=())


def verify_bound_paper_input(store: DuckDBStore, request: ResearchGateRequest, session: ResearchExecutionSession, *,
                             catalog: PaperResearchCatalog) -> FrozenPaperResearchInput:
    snapshot = store.get_dataset_snapshot(request.dataset_snapshot_id)
    if snapshot is None or session.snapshot_id != snapshot.snapshot_id or session.binding_hash != request.dataset_binding_hash:
        raise PermissionError("paper bound execution source differs")
    value = verify_paper_snapshot_source(session._conn, task_name=request.strategy_name, code_sha=request.code_commit,
                                         start_date=request.start_date, end_date=request.end_date,
                                         input_hash=snapshot.table_watermarks.get("paper_input_hash", ""), as_of=snapshot.as_of_time, catalog=catalog)
    if snapshot.table_watermarks != paper_research_watermarks(value, audit_id=request.audit_run_id):
        raise PermissionError("paper complete original input material differs")
    return value


@contextmanager
def open_gated_paper_store(request: ResearchGateRequest, *, metadata_store_factory: Callable[[], AbstractContextManager[DuckDBStore]],
                            lake_root: Path, catalog: PaperResearchCatalog) -> Iterator[ResearchExecutionSession]:
    with metadata_store_factory() as metadata:
        require_paper_gate(metadata, request, catalog=catalog)
        binding = metadata.get_dataset_snapshot_binding(request.dataset_snapshot_id)
        with ResearchExecutionSession(binding=binding, lake_root=lake_root) as session:
            verify_bound_paper_input(metadata, request, session, catalog=catalog)
            yield session
