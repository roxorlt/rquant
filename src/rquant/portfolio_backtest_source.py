"""Trusted daily preparation and an exact, domain-specific immutable source gate.

Only an offline producer may supply PortfolioSourceData. Web requests contain a
small config, never source rows, registries, paths, a code identity or a plan.
"""

from __future__ import annotations

import os
import re
import stat
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Self
from uuid import UUID, uuid4

import duckdb
from pydantic import Field, field_validator, model_validator

from rquant.backtest.contracts import BacktestRequest, Sha256
from rquant.data_metadata import (
    DataAuditRun,
    DataAuditRunFinalization,
    DatasetCoverage,
    DatasetSnapshot,
    DatasetSnapshotFinalization,
)
from rquant.definition_registry import ImmutableDefinitionRegistry, StrategySpecRegistration
from rquant.experiment_registry import (
    DateRange,
    ExperimentRegistry,
    ExperimentSpec,
    FormalExperimentPlan,
    HypothesisFamilyManifest,
    IncompleteHypothesisFamilyError,
)
from rquant.lab_job_center import (
    ResearchJobSubmission,
    _research_parameter,
    build_research_job_submission,
)
from rquant.portfolio_backtest_adapter import (
    PORTFOLIO_INPUT_TABLE,
    PORTFOLIO_SOURCE_CONTRACT,
    PortfolioBacktestRunInput,
    read_portfolio_input_table,
    write_portfolio_input_table,
)
from rquant.portfolio_backtest_models import (
    FrozenPortfolioInput,
    PortfolioBacktestConfig,
    PortfolioSourceManifest,
)
from rquant.research_catalog import ResearchCatalog
from rquant.research_gate import ResearchGateDecision, ResearchGateFailure, ResearchGateRequest
from rquant.research_run_spec import DatasetSnapshotIdentity, ResearchRunParameters, ResourceClass
from rquant.research_snapshot import ResearchExecutionSession, build_dataset_snapshot_binding
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_job_adapters import build_adapter_execution_contract

PORTFOLIO_AUDIT_RULE = "portfolio-source/v1"
_SHA = re.compile(r"^[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")


class PortfolioSourceData(RuntimeContractModel):
    """A reusable verified candidate/price/reference slice, independent of weights."""

    source_key: str = Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    source_version: int = Field(strict=True, ge=1)
    template: BacktestRequest
    sources: PortfolioSourceManifest
    benchmarks: dict[str, tuple[tuple[date, float], ...]]
    material_hash: Sha256 | None = None

    @field_validator("benchmarks", mode="before")
    @classmethod
    def reject_boolean_benchmark(cls, value: object) -> object:
        if isinstance(value, dict):
            for rows in value.values():
                if isinstance(rows, (list, tuple)) and any(
                    isinstance(row, (list, tuple)) and len(row) == 2 and isinstance(row[1], bool)
                    for row in rows
                ):
                    raise ValueError("benchmark prices cannot be boolean")
        return value

    @model_validator(mode="after")
    def validate_material(self) -> Self:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"material_hash"}))
        if self.material_hash is None:
            object.__setattr__(self, "material_hash", expected)
        elif self.material_hash != expected:
            raise ValueError("portfolio source material changed")
        return self


def freeze_portfolio_config(
    source: PortfolioSourceData, config: PortfolioBacktestConfig
) -> FrozenPortfolioInput:
    checked = PortfolioSourceData.model_validate(source.model_dump(mode="python"))
    selected = PortfolioBacktestConfig.model_validate(config.model_dump(mode="python"))
    if (checked.source_key, checked.source_version) != (
        selected.source_key,
        selected.source_version,
    ):
        raise ValueError("portfolio source version is unavailable")
    days = tuple(
        day
        for day in checked.template.days
        if selected.start_date <= day.trade_date <= selected.end_date
    )
    expected = tuple(
        d for d in checked.template.calendar.dates if selected.start_date <= d <= selected.end_date
    )
    if not days or tuple(day.trade_date for day in days) != expected:
        raise ValueError("portfolio source does not cover the requested trading dates")
    generation = canonical_sha256({"material": checked.material_hash, "dates": expected})
    request = BacktestRequest.model_validate(
        checked.template.model_dump(mode="python")
        | {
            "days": days,
            "initial_cash": selected.initial_cash,
            "weight_rule": selected.weight_rule,
            "rebalance_rule": selected.rebalance_rule,
            "execution_cost_spec": selected.execution_cost_spec,
            "drawdown_rule": selected.drawdown_rule,
            "input_generation_id": generation,
        }
    )
    rows = checked.benchmarks.get(selected.benchmark_code)
    closes = None
    unavailable = "missing_source"
    if rows is not None:
        index = request.calendar.dates.index(days[0].trade_date)
        dates = (request.calendar.dates[index - 1], *expected)
        matching = tuple(row for row in rows if row[0] in dates)
        if tuple(row[0] for row in matching) == dates:
            closes, unavailable = matching, None
        else:
            unavailable = "missing_dates"
    return FrozenPortfolioInput(
        config=selected,
        request=request,
        sources=checked.sources,
        benchmark_closes=closes,
        benchmark_unavailable=unavailable,
    )


class PublishedPortfolioInput(RuntimeContractModel):
    config_hash: Sha256
    input_hash: Sha256
    identity: DatasetSnapshotIdentity
    gate_decision: ResearchGateDecision


class PortfolioExperimentProtocol(RuntimeContractModel):
    """Producer-injected preregistration ranges; no claim that outer tests ran."""

    train_range: DateRange
    validation_range: DateRange
    frozen_outer_test_range: DateRange


class PreparedPortfolioRequest(RuntimeContractModel):
    frozen: FrozenPortfolioInput
    published: PublishedPortfolioInput
    registration: StrategySpecRegistration
    formal_plan: FormalExperimentPlan
    deadline: datetime
    random_seed: int = Field(default=0, strict=True, ge=0)
    resource_class: ResourceClass = ResourceClass.STANDARD
    max_attempts: int = Field(default=2, strict=True, ge=1, le=5)

    def submission(self, *, job_id: UUID) -> ResearchJobSubmission:
        if (self.frozen.input_hash, self.frozen.config.config_hash) != (
            self.published.input_hash,
            self.published.config_hash,
        ):
            raise ValueError("portfolio prepared config changed")
        return build_research_job_submission(
            PortfolioBacktestRunInput.from_frozen(self.frozen),
            gate_decision=self.published.gate_decision,
            code_sha=self.frozen.request.producer_commit,
            dataset_snapshot=self.published.identity,
            feature_contract=build_adapter_execution_contract(
                "portfolio-backtest", "1", self.frozen.request.producer_commit
            ),
            execution_costs=self.frozen.config.execution_cost_spec,
            random_seed=self.random_seed,
            resource_class=self.resource_class,
            deadline=self.deadline,
            job_id=job_id,
            max_attempts=self.max_attempts,
            trusted_strategy_registration=self.registration,
            formal_experiment_plan=self.formal_plan,
        )


def build_portfolio_plan(
    value: FrozenPortfolioInput,
    published: PublishedPortfolioInput,
    *,
    definitions: ImmutableDefinitionRegistry,
    protocol: PortfolioExperimentProtocol,
    now: datetime,
    deadline: datetime,
    random_seed: int = 0,
    family_id: str | None = None,
    hypothesis_variant: str = "daily-portfolio",
) -> PreparedPortfolioRequest:
    """Build the original immutable receipts; do not register a partial family."""
    checked = FrozenPortfolioInput.model_validate(value.model_dump(mode="python"))
    publication = PublishedPortfolioInput.model_validate(published.model_dump(mode="python"))
    if (checked.config.config_hash, checked.input_hash) != (
        publication.config_hash,
        publication.input_hash,
    ) or not publication.gate_decision.allowed:
        raise PermissionError("portfolio preparation identity changed")
    registration = definitions.latest_strategy_spec("portfolio_backtest", as_of=now)
    if registration is None or registration.producer_commit != checked.request.producer_commit:
        raise PermissionError("portfolio trusted definition is unavailable")
    run = PortfolioBacktestRunInput.from_frozen(checked)
    parameters = ResearchRunParameters(
        strategy_name="portfolio_backtest",
        start_date=run.start_date,
        end_date=run.end_date,
        arguments=tuple(
            _research_parameter(name, getattr(run.parameters, name))
            for name in type(run.parameters).model_fields
        ),
    )
    contract = build_adapter_execution_contract(
        "portfolio-backtest", "1", checked.request.producer_commit
    )
    family = family_id or "portfolio:" + canonical_sha256(
        {"input": checked.input_hash, "protocol": protocol}
    )
    spec = ExperimentSpec(
        strategy_spec_fingerprint=registration.spec.spec_fingerprint,
        strategy_executable_fingerprint=registration.executable_fingerprint,
        candidate_schema_fingerprint=registration.candidate_schema_fingerprint,
        dataset_snapshot_id=publication.identity.snapshot_id,
        code_commit=checked.request.producer_commit,
        parameter_fingerprint=canonical_sha256(parameters),
        hypothesis_family=family,
        metric_definition_fingerprint=canonical_sha256(
            {"contract": "portfolio-performance/v1", "overfit": "not_evaluated"}
        ),
        train_range=protocol.train_range,
        validation_range=protocol.validation_range,
        frozen_outer_test_range=protocol.frozen_outer_test_range,
        cost_model_fingerprint=canonical_sha256(checked.config.execution_cost_spec),
        execution_model_fingerprint=canonical_sha256(
            {
                "contract": "lab-adapter-execution/v1",
                "adapter_id": "portfolio-backtest",
                "adapter_version": "1",
                "feature_contract": contract,
            }
        ),
        seed=random_seed,
    )
    plan = FormalExperimentPlan(
        schema_version=2,
        spec=spec,
        hypothesis_variant=hypothesis_variant,
        strategy_definition_fingerprint=registration.fingerprint,
        definition_registration_record_hash=registration.record_hash,
        preregistered_at=now,
    )
    prepared = PreparedPortfolioRequest(
        frozen=checked,
        published=publication,
        registration=registration,
        formal_plan=plan,
        deadline=deadline,
        random_seed=random_seed,
    )
    prepared.submission(job_id=UUID(int=0))
    return prepared


def register_portfolio_plan(
    value: FrozenPortfolioInput,
    published: PublishedPortfolioInput,
    *,
    definitions: ImmutableDefinitionRegistry,
    experiments: ExperimentRegistry,
    protocol: PortfolioExperimentProtocol,
    now: datetime,
    deadline: datetime,
    random_seed: int = 0,
) -> PreparedPortfolioRequest:
    prepared = build_portfolio_plan(
        value,
        published,
        definitions=definitions,
        protocol=protocol,
        now=now,
        deadline=deadline,
        random_seed=random_seed,
    )
    checked, publication = prepared.frozen, prepared.published
    registration, spec = prepared.registration, prepared.formal_plan.spec
    family = spec.hypothesis_family
    try:
        existing = experiments.resolve_formal_plan(
            strategy_spec_fingerprint=spec.strategy_spec_fingerprint,
            strategy_executable_fingerprint=spec.strategy_executable_fingerprint,
            candidate_schema_fingerprint=spec.candidate_schema_fingerprint,
            dataset_snapshot_id=spec.dataset_snapshot_id,
            code_commit=spec.code_commit,
            parameter_fingerprint=spec.parameter_fingerprint,
            cost_model_fingerprint=spec.cost_model_fingerprint,
            execution_model_fingerprint=spec.execution_model_fingerprint,
            seed=spec.seed,
            as_of=now,
        )
    except IncompleteHypothesisFamilyError:
        # The original registry rejects a conflicting or ambiguous registration.
        existing = None
    if existing is not None:
        if existing.spec != spec or (
            existing.strategy_definition_fingerprint,
            existing.definition_registration_record_hash,
        ) != (registration.fingerprint, registration.record_hash):
            raise PermissionError("portfolio formal plan conflicts with preparation")
        plan = existing
    else:
        plan = FormalExperimentPlan(
            schema_version=2,
            spec=spec,
            hypothesis_variant="daily-portfolio",
            strategy_definition_fingerprint=registration.fingerprint,
            definition_registration_record_hash=registration.record_hash,
            preregistered_at=now,
        )
        experiments.register_formal_plan(
            plan,
            family_manifest=HypothesisFamilyManifest(
                hypothesis_family=family,
                experiment_ids=(spec.experiment_id,),
                search_space_fingerprint=canonical_sha256(checked.config),
                metric_definition_fingerprint=spec.metric_definition_fingerprint,
                preregistered_at=now,
            ),
        )
    restored = prepared.model_copy(update={"formal_plan": plan})
    restored.submission(job_id=UUID(int=0))
    return restored


def publish_portfolio_input(
    value: FrozenPortfolioInput,
    *,
    metadata_store: DuckDBStore,
    source_path: Path,
    catalog: ResearchCatalog,
    lake_root: Path,
    now: datetime,
) -> PublishedPortfolioInput:
    """Build one new owned source, then register original metadata and binding."""
    checked = FrozenPortfolioInput.model_validate(value.model_dump(mode="python"))
    if source_path.exists() or source_path.is_symlink():
        raise ValueError("portfolio producer requires a new private source path")
    source_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    audit = DataAuditRun.create(
        as_of_date=now.date(),
        range_start=checked.config.start_date,
        range_end=checked.config.end_date,
        observed_at=now,
        rule_set_version=f"{PORTFOLIO_AUDIT_RULE}:{checked.input_hash}",
    )
    metadata_store.begin_data_audit_run(audit)
    snapshot = DatasetSnapshot.create(
        strategy_name="portfolio_backtest",
        manifest_id=checked.input_hash,
        as_of_time=now,
        code_commit=checked.request.producer_commit,
        origin="trusted-portfolio-producer",
        created_at=now,
    )
    metadata_store.begin_dataset_snapshot(snapshot)
    with duckdb.connect(str(source_path)) as connection:
        os.chmod(source_path, 0o600)
        write_portfolio_input_table(connection, checked)
        restored = read_portfolio_input_table(connection, require_primary_key=True)
        if restored != checked:
            raise ValueError("portfolio source round trip differs")
        days = len(checked.request.days)
        pairs = sum(len(day.instruments) for day in checked.request.days)
        watermarks = {
            "manifest_start_date": checked.config.start_date.isoformat(),
            "manifest_end_date": checked.config.end_date.isoformat(),
            "portfolio_input_hash": checked.input_hash,
            "portfolio_config_hash": checked.config.config_hash,
            "portfolio_request_id": checked.request.request_id,
            "portfolio_source_hash": canonical_sha256(checked.sources),
            "portfolio_audit_id": audit.audit_run_id,
            "portfolio_days": str(days),
            "portfolio_pairs": str(pairs),
        }
        for scope, count in (
            ("portfolio_input", 1),
            ("portfolio_days", days),
            ("portfolio_pairs", pairs),
        ):
            metadata_store.upsert_dataset_coverage(
                DatasetCoverage(
                    snapshot_id=snapshot.snapshot_id,
                    dataset_id=PORTFOLIO_INPUT_TABLE,
                    table_name=PORTFOLIO_INPUT_TABLE,
                    coverage_scope=scope,
                    expected_count=count,
                    available_count=count,
                    created_at=now,
                )
            )
        metadata_store.finalize_dataset_snapshot(
            snapshot.snapshot_id,
            DatasetSnapshotFinalization(table_watermarks=watermarks, completed_at=now),
        )
        binding = build_dataset_snapshot_binding(
            metadata_store=metadata_store,
            source_connection=connection,
            catalog=catalog,
            lake_root=lake_root,
            snapshot_id=snapshot.snapshot_id,
            start_date=checked.config.start_date,
            end_date=checked.config.end_date,
            now=lambda: now,
        )
    metadata_store.finalize_data_audit_run(
        audit.audit_run_id, DataAuditRunFinalization(p0_count=0, completed_at=now)
    )
    request = ResearchGateRequest(
        mode="formal",
        strategy_name="portfolio_backtest",
        start_date=checked.config.start_date,
        end_date=checked.config.end_date,
        code_commit=checked.request.producer_commit,
        audit_run_id=audit.audit_run_id,
        dataset_snapshot_id=snapshot.snapshot_id,
        dataset_binding_hash=binding.binding_hash,
    )
    # Actual artifact hashes/schema/content are checked by the original session.
    with ResearchExecutionSession(binding=binding, lake_root=lake_root) as session:
        verify_bound_portfolio_input(metadata_store, request, session)
    decision = require_portfolio_gate(metadata_store, request, binding_verified=True)
    return PublishedPortfolioInput(
        config_hash=checked.config.config_hash,
        input_hash=checked.input_hash,
        identity=DatasetSnapshotIdentity(
            snapshot_id=snapshot.snapshot_id,
            binding_hash=binding.binding_hash,
            audit_run_id=audit.audit_run_id,
        ),
        gate_decision=decision,
    )


def evaluate_portfolio_gate(
    store: DuckDBStore, request: ResearchGateRequest, *, binding_verified: bool = False
) -> ResearchGateDecision:
    failures: list[ResearchGateFailure] = []

    def check(ok: bool, code: str) -> None:
        if not ok:
            failures.append(ResearchGateFailure(code=code, message="组合回测来源核验未通过"))

    check(
        request.mode == "formal" and request.strategy_name == "portfolio_backtest",
        "portfolio_gate_identity",
    )
    check(bool(_COMMIT.fullmatch(request.code_commit or "")), "portfolio_clean_code")
    snapshot = (
        store.get_dataset_snapshot(request.dataset_snapshot_id)
        if request.dataset_snapshot_id
        else None
    )
    audit = store.get_data_audit_run(request.audit_run_id) if request.audit_run_id else None
    binding = (
        store.get_dataset_snapshot_binding(request.dataset_snapshot_id)
        if request.dataset_snapshot_id
        else None
    )
    check(
        snapshot is not None and audit is not None and binding is not None,
        "portfolio_metadata_missing",
    )
    counts: dict[str, tuple[int, int]] = {}
    if snapshot is not None:
        w = snapshot.table_watermarks
        for key in (
            "portfolio_input_hash",
            "portfolio_config_hash",
            "portfolio_request_id",
            "portfolio_source_hash",
            "portfolio_audit_id",
        ):
            check(bool(_SHA.fullmatch(w.get(key, ""))), f"portfolio_watermark_{key}")
        check(
            snapshot.status == "ready"
            and snapshot.strategy_name == request.strategy_name
            and snapshot.code_commit == request.code_commit
            and snapshot.snapshot_id == request.dataset_snapshot_id
            and snapshot.as_of_time.date() >= request.end_date
            and not snapshot.quality_issue_ids
            and snapshot.manifest_id == w.get("portfolio_input_hash")
            and (w.get("manifest_start_date"), w.get("manifest_end_date"))
            == (request.start_date.isoformat(), request.end_date.isoformat()),
            "portfolio_snapshot_identity",
        )
        check(
            audit is not None
            and audit.status == "completed"
            and audit.audit_run_id == w.get("portfolio_audit_id")
            and audit.rule_set_version == f"{PORTFOLIO_AUDIT_RULE}:{w.get('portfolio_input_hash')}"
            and audit.range_start == request.start_date
            and audit.range_end == request.end_date
            and audit.as_of_date >= request.end_date
            and audit.p0_count == 0
            and not audit.finding_issue_ids,
            "portfolio_audit_identity",
        )
        coverages = store.list_dataset_coverages(snapshot.snapshot_id)
        expected_scopes = {
            "portfolio_input": "1",
            "portfolio_days": w.get("portfolio_days"),
            "portfolio_pairs": w.get("portfolio_pairs"),
        }
        check(
            len(coverages) == 3 and {c.coverage_scope for c in coverages} == set(expected_scopes),
            "portfolio_coverage_identity",
        )
        for coverage in coverages:
            check(
                coverage.snapshot_id == snapshot.snapshot_id
                and coverage.dataset_id == PORTFOLIO_INPUT_TABLE
                and coverage.table_name == PORTFOLIO_INPUT_TABLE
                and str(coverage.expected_count) == expected_scopes.get(coverage.coverage_scope)
                and coverage.available_count == coverage.expected_count
                and not coverage.missing_reasons,
                "portfolio_coverage_count",
            )
            counts[coverage.coverage_scope] = (coverage.available_count, coverage.expected_count)
        if binding is not None:
            manifest = binding.manifest
            check(
                binding.status == "ready"
                and binding.snapshot_id == snapshot.snapshot_id
                and binding.binding_hash == request.dataset_binding_hash,
                "portfolio_binding_identity",
            )
            check(
                (
                    manifest.strategy_name,
                    manifest.code_commit,
                    manifest.as_of_time,
                    manifest.start_date,
                    manifest.end_date,
                    manifest.dependency_contract_version,
                    manifest.eligibility_resolution_hash,
                )
                == (
                    snapshot.strategy_name,
                    snapshot.code_commit,
                    snapshot.as_of_time,
                    request.start_date,
                    request.end_date,
                    PORTFOLIO_SOURCE_CONTRACT,
                    None,
                ),
                "portfolio_manifest_identity",
            )
            check(
                len(manifest.artifacts) == 1
                and manifest.artifacts[0].dataset_id == PORTFOLIO_INPUT_TABLE
                and manifest.artifacts[0].table_name == PORTFOLIO_INPUT_TABLE
                and manifest.artifacts[0].row_count == 1
                and manifest.artifacts[0].artifact_type == "materialized_table",
                "portfolio_exact_artifact",
            )
    check(not store.list_open_data_quality_issues(severities=("P0",)), "portfolio_open_p0")
    check(binding_verified, "snapshot_artifacts_unverified")
    return ResearchGateDecision(
        allowed=not failures,
        research_status="comparable" if not failures else "exploratory",
        audit_run_id=None if audit is None else audit.audit_run_id,
        dataset_snapshot_id=None if snapshot is None else snapshot.snapshot_id,
        dataset_binding_hash=None if binding is None else binding.binding_hash,
        coverage_counts=counts,
        coverage_ratios={
            key: available / expected if expected else None
            for key, (available, expected) in counts.items()
        },
        failures=tuple(failures),
    )


def require_portfolio_gate(
    store: DuckDBStore, request: ResearchGateRequest, *, binding_verified: bool = False
) -> ResearchGateDecision:
    decision = evaluate_portfolio_gate(store, request, binding_verified=binding_verified)
    if not decision.allowed:
        raise PermissionError(
            "portfolio source gate rejected: " + ",".join(f.code for f in decision.failures)
        )
    return decision


def verify_bound_portfolio_input(
    store: DuckDBStore, request: ResearchGateRequest, session: ResearchExecutionSession
) -> FrozenPortfolioInput:
    value = read_portfolio_input_table(session._conn)
    snapshot = store.get_dataset_snapshot(request.dataset_snapshot_id)
    if snapshot is None:
        raise PermissionError("portfolio snapshot disappeared")
    w = snapshot.table_watermarks
    if (
        value.input_hash,
        value.config.config_hash,
        value.request.request_id,
        canonical_sha256(value.sources),
        value.request.producer_commit,
        value.config.start_date,
        value.config.end_date,
        str(len(value.request.days)),
        str(sum(len(day.instruments) for day in value.request.days)),
    ) != (
        w.get("portfolio_input_hash"),
        w.get("portfolio_config_hash"),
        w.get("portfolio_request_id"),
        w.get("portfolio_source_hash"),
        request.code_commit,
        request.start_date,
        request.end_date,
        w.get("portfolio_days"),
        w.get("portfolio_pairs"),
    ):
        raise PermissionError("portfolio bound source content changed")
    return value


@contextmanager
def open_gated_portfolio_store(
    request: ResearchGateRequest,
    *,
    metadata_store_factory: Callable[[], AbstractContextManager[DuckDBStore]],
    lake_root: Path,
) -> Iterator[tuple[ResearchExecutionSession, ResearchGateDecision]]:
    with metadata_store_factory() as store:
        before = evaluate_portfolio_gate(store, request)
        if any(f.code != "snapshot_artifacts_unverified" for f in before.failures):
            raise PermissionError("portfolio source metadata gate rejected")
        binding = store.get_dataset_snapshot_binding(request.dataset_snapshot_id)
        if binding is None or binding.binding_hash != request.dataset_binding_hash:
            raise PermissionError("portfolio binding changed")
        with ResearchExecutionSession(binding=binding, lake_root=lake_root) as session:
            verify_bound_portfolio_input(store, request, session)
            after = require_portfolio_gate(store, request, binding_verified=True)
            if after.dataset_binding_hash != before.dataset_binding_hash:
                raise PermissionError("portfolio binding changed")
            yield session, after


class PortfolioRequestPreparer:
    """Writer-installed producer; each small config gets one exact original plan.

    Source lookup, code, paths and the experiment protocol are installed by a
    trusted producer. They are never fields in the browser command.
    """

    def __init__(
        self,
        *,
        source_provider: Callable[[str, int], PortfolioSourceData],
        metadata_store_factory: Callable[[], AbstractContextManager[DuckDBStore]],
        catalog: ResearchCatalog,
        lake_root: Path,
        input_root: Path,
        definitions: ImmutableDefinitionRegistry,
        experiments: ExperimentRegistry,
        protocol: PortfolioExperimentProtocol,
        code_commit: str,
        clock: Callable[[], datetime],
        max_task_seconds: int = 3600,
        protected_sources: frozenset[tuple[str, int]] = frozenset(),
    ) -> None:
        observed = input_root.lstat()
        if (
            not stat.S_ISDIR(observed.st_mode)
            or stat.S_IMODE(observed.st_mode) != 0o700
            or observed.st_uid != os.geteuid()
        ):
            raise PermissionError("portfolio input root must be private and producer-owned")
        if (
            not _COMMIT.fullmatch(code_commit)
            or type(max_task_seconds) is not int
            or not 1 <= max_task_seconds <= 86400
        ):
            raise ValueError("portfolio producer code or deadline policy is invalid")
        self.source_provider, self.metadata_store_factory = source_provider, metadata_store_factory
        self.catalog, self.lake_root, self.input_root = catalog, lake_root, input_root
        self.definitions, self.experiments, self.protocol = definitions, experiments, protocol
        self.code_commit, self.clock, self.max_task_seconds = code_commit, clock, max_task_seconds
        self.protected_sources = frozenset(protected_sources)

    def __call__(self, config: PortfolioBacktestConfig) -> PreparedPortfolioRequest:
        checked = PortfolioBacktestConfig.model_validate(config.model_dump(mode="python"))
        if (checked.source_key, checked.source_version) in self.protected_sources:
            raise PermissionError("protected source requires formal experiment phase admission")
        source = self.source_provider(checked.source_key, checked.source_version)
        value = freeze_portfolio_config(source, checked)
        if value.request.producer_commit != self.code_commit:
            raise PermissionError("portfolio source code differs from installed producer")
        now = self.clock()
        directory = self.input_root / uuid4().hex
        directory.mkdir(mode=0o700)
        # Failures may leave honest source/audit preparation records, but never
        # publish a Lab job. PageControl saves the exact successful plan first.
        with self.metadata_store_factory() as metadata:
            published = publish_portfolio_input(
                value,
                metadata_store=metadata,
                source_path=directory / "input.duckdb",
                catalog=self.catalog,
                lake_root=self.lake_root,
                now=now,
            )
        return register_portfolio_plan(
            value,
            published,
            definitions=self.definitions,
            experiments=self.experiments,
            protocol=self.protocol,
            now=now,
            deadline=now + timedelta(seconds=self.max_task_seconds),
        )
