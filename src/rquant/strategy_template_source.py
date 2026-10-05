"""Trusted facts become one exact original immutable Lab execution source."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Self

import duckdb
from pydantic import Field, model_validator

from rquant.backtest.contracts import BacktestRequest, Sha256
from rquant.data_metadata import (
    DataAuditRun,
    DataAuditRunFinalization,
    DatasetCoverage,
    DatasetSnapshot,
    DatasetSnapshotFinalization,
)
from rquant.portfolio_backtest_models import PortfolioBacktestConfig
from rquant.portfolio_backtest_source import PortfolioSourceData
from rquant.research_catalog import ResearchCatalog
from rquant.research_gate import ResearchGateDecision, ResearchGateRequest
from rquant.research_run_spec import DatasetSnapshotIdentity
from rquant.research_snapshot import ResearchExecutionSession, build_dataset_snapshot_binding
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_authoring_source import (
    StrategySourceCatalog,
    produce_template_entry,
    template_source_code_identity,
)
from rquant.strategy_dependencies import StrategyExecutionDependencies, StrategyTableDependency
from rquant.strategy_template_adapter import (
    TEMPLATE_INPUT_CONTRACT,
    TEMPLATE_INPUT_TABLE,
    read_strategy_template_input,
    template_input_work_units,
    write_strategy_template_input,
)
from rquant.strategy_template_definition import StrategyTemplateExecutionVersion
from rquant.strategy_template_execution import TemplateEntryEvidence
from rquant.strategy_template_run import (
    FrozenStrategyTemplateInput,
    TemplateDayEvidence,
    TemplateIndexClose,
    TemplateMinuteExecution,
    _at,
)
from rquant.strategy_template_run_commands import RunStrategyTemplate

TEMPLATE_AUDIT_RULE = "strategy-template-source/v1"


class TemplateRawDay(RuntimeContractModel):
    trade_date: date
    entry: TemplateEntryEvidence
    index_closes: tuple[TemplateIndexClose, ...] = ()
    minutes: tuple[TemplateMinuteExecution, ...] = ()


class StrategyTemplateSourceData(RuntimeContractModel):
    owner_id: str = Field(min_length=1, max_length=128)
    catalog: StrategySourceCatalog
    portfolio: PortfolioSourceData
    days: tuple[TemplateRawDay, ...] = Field(min_length=1, max_length=2520)
    material_hash: Sha256 | None = None

    @model_validator(mode="before")
    @classmethod
    def original_numeric_admission(cls, value: object) -> object:
        if isinstance(value, Mapping):
            source = value.get("portfolio")
            source = (
                source.model_dump(mode="python")
                if isinstance(source, PortfolioSourceData)
                else source
            )
            if isinstance(source, Mapping):
                request = source.get("template")
                request = (
                    request.model_dump(mode="python")
                    if isinstance(request, BacktestRequest)
                    else request
                )
                PortfolioBacktestConfig.validate_numeric_admission(request)
        return value

    @model_validator(mode="after")
    def complete_source(self) -> Self:
        if self.owner_id != self.catalog.owner_id or tuple(
            day.trade_date for day in self.days
        ) != tuple(day.trade_date for day in self.portfolio.template.days):
            raise ValueError("template source owner or complete trading dates differ")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"material_hash"}))
        if self.material_hash is None:
            object.__setattr__(self, "material_hash", expected)
        elif self.material_hash != expected:
            raise ValueError("template source material changed")
        if len(self.model_dump_json().encode()) > 16 * 1024 * 1024:
            raise ValueError("template source exceeds the original byte budget")
        return self


def freeze_strategy_template_source(
    source: StrategyTemplateSourceData,
    version: StrategyTemplateExecutionVersion,
    command: RunStrategyTemplate,
) -> FrozenStrategyTemplateInput:
    source = StrategyTemplateSourceData.model_validate(source.model_dump(mode="python"))
    version = StrategyTemplateExecutionVersion.model_validate(version.model_dump(mode="python"))
    command = RunStrategyTemplate.model_validate(command.model_dump(mode="python"))
    if source.owner_id != version.owner_id or (command.strategy_id, command.head) != (
        version.strategy_id,
        version.head,
    ):
        raise PermissionError("template source differs from the original owner and version")
    source.catalog.validate_rules(
        version.rules, owner_id=version.owner_id, generation_id=command.generation_id
    )
    expected_dates = tuple(
        day
        for day in source.portfolio.template.calendar.dates
        if command.start_date <= day <= command.end_date
    )
    selected = tuple(
        day
        for day in source.portfolio.template.days
        if command.start_date <= day.trade_date <= command.end_date
    )
    if (
        not selected
        or tuple(day.trade_date for day in selected) != expected_dates
        or (selected[0].trade_date, selected[-1].trade_date)
        != (command.start_date, command.end_date)
    ):
        raise ValueError("template source does not cover exact requested trading boundaries")
    data = source.portfolio.template.model_dump(mode="python") | {
        "days": selected,
        "initial_cash": command.initial_cash,
        "weight_rule": version.rules.weight_rule,
        "rebalance_rule": version.rules.rebalance_rule,
        "input_generation_id": canonical_sha256(
            {
                "source": source.material_hash,
                "rules": version.rules,
                "dates": expected_dates,
                "cash": command.initial_cash,
            }
        ),
    }
    PortfolioBacktestConfig.validate_numeric_admission(data)
    request = BacktestRequest.model_validate(data)
    raw = tuple(day for day in source.days if day.trade_date in expected_dates)
    days = tuple(
        TemplateDayEvidence(
            trade_date=day.trade_date,
            entry=produce_template_entry(
                version.rules, day.entry, decision_time=_at(day.trade_date, 9, 25)
            ),
            index_closes=day.index_closes,
            minutes=day.minutes,
        )
        for day in raw
    )
    return FrozenStrategyTemplateInput(
        owner_id=version.owner_id,
        rules=version.rules,
        definition=version.definition,
        request=request,
        source_code_identity=template_source_code_identity(),
        sources=source.portfolio.sources,
        source_material_hash=source.material_hash,
        catalog_generation_id=source.catalog.generation_id,
        days=days,
    )


def template_execution_dependencies(
    version: StrategyTemplateExecutionVersion,
) -> StrategyExecutionDependencies:
    return StrategyExecutionDependencies(
        strategy_id=version.strategy_id,
        contract_version=TEMPLATE_INPUT_CONTRACT,
        lake_datasets=(),
        materialized_tables=(
            StrategyTableDependency(
                dataset_id=TEMPLATE_INPUT_TABLE, table_name=TEMPLATE_INPUT_TABLE
            ),
        ),
        template_definition=version,
    )


def verify_template_snapshot_source(
    connection: duckdb.DuckDBPyConnection,
    *,
    version: StrategyTemplateExecutionVersion,
    code_sha: str,
    start_date: date,
    end_date: date,
    input_hash: str,
    as_of: datetime,
) -> FrozenStrategyTemplateInput:
    value = read_strategy_template_input(connection, input_hash=input_hash)
    if (
        value.owner_id,
        value.rules,
        value.definition,
        value.request.producer_commit,
        value.request.days[0].trade_date,
        value.request.days[-1].trade_date,
    ) != (version.owner_id, version.rules, version.definition, code_sha, start_date, end_date):
        raise ValueError(
            "template source differs from exact owner, definition, rules, code or dates"
        )
    if value.definition.available_at > as_of:
        raise ValueError("template definition is not available at snapshot time")
    observations: list[datetime] = []
    for day, facts in zip(value.request.days, value.days, strict=True):
        observations.extend((day.ranking.observed_at, facts.entry.evidence.observed_at))
        observations.extend(signal.observed_at for signal in facts.entry.evidence.signals)
        observations.extend(close.observed_at for close in facts.index_closes)
        for minute in facts.minutes:
            observations.extend((minute.quote.observed_at, minute.execution_at))
        for instrument in day.instruments:
            observations.append(instrument.classification_observed_at)
            observations.extend(
                t
                for t in (
                    instrument.decision_price_observed_at,
                    instrument.open_observed_at,
                    instrument.close_observed_at,
                )
                if t is not None
            )
            if instrument.conditions is not None:
                observations.append(instrument.conditions.observed_at)
    if any(observed > as_of for observed in observations):
        raise ValueError("template source has future facts at snapshot time")
    sources = value.sources
    if (
        any(
            item.open_price is not None or item.conditions is not None
            for day in value.request.days
            for item in day.instruments
        )
        and sources.opening_hash is None
    ):
        raise ValueError("template opening facts require their original source identity")
    if sources.ranking_hash is None:
        raise ValueError("template full ranking requires its original source identity")
    if value.rules.weight_rule.max_industry_weight is not None and sources.industry_hash is None:
        raise ValueError("template industry limits require their original source identity")
    return value


def template_watermarks(value: FrozenStrategyTemplateInput, *, audit_id: str) -> dict[str, str]:
    return {
        "manifest_start_date": value.request.days[0].trade_date.isoformat(),
        "manifest_end_date": value.request.days[-1].trade_date.isoformat(),
        "template_input_hash": value.input_hash,
        "template_owner_id": value.owner_id,
        "template_strategy_id": value.definition.logical_id,
        "template_version": str(value.definition.version),
        "template_definition": value.definition.fingerprint,
        "template_record_hash": value.definition.record_hash,
        "template_rules_hash": value.rules.rules_hash,
        "template_source_code": value.source_code_identity,
        "template_source_material": value.source_material_hash,
        "template_source_manifest": canonical_sha256(value.sources),
        "template_catalog_generation": value.catalog_generation_id,
        "template_request_hash": value.request.request_id,
        "template_audit_id": audit_id,
        "template_days": str(len(value.days)),
        "template_work_units": str(template_input_work_units(value)),
    }


class PublishedStrategyTemplateInput(RuntimeContractModel):
    input_hash: Sha256
    identity: DatasetSnapshotIdentity
    request: ResearchGateRequest
    gate_decision: ResearchGateDecision


def publish_strategy_template_input(
    value: FrozenStrategyTemplateInput,
    *,
    metadata_store: DuckDBStore,
    source_path: Path,
    catalog: ResearchCatalog,
    lake_root: Path,
    version: StrategyTemplateExecutionVersion,
    now: datetime,
) -> PublishedStrategyTemplateInput:
    value = FrozenStrategyTemplateInput.model_validate(value.model_dump(mode="python"))
    if source_path.exists() or source_path.is_symlink():
        raise ValueError("template producer requires a new private source path")
    source_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with duckdb.connect(str(source_path)) as connection:
        os.chmod(source_path, 0o600)
        write_strategy_template_input(connection, value)
        verify_template_snapshot_source(
            connection,
            version=version,
            code_sha=value.request.producer_commit,
            start_date=value.request.days[0].trade_date,
            end_date=value.request.days[-1].trade_date,
            input_hash=value.input_hash,
            as_of=now,
        )
        audit = DataAuditRun.create(
            as_of_date=now.date(),
            range_start=value.request.days[0].trade_date,
            range_end=value.request.days[-1].trade_date,
            observed_at=now,
            rule_set_version=f"{TEMPLATE_AUDIT_RULE}:{value.input_hash}",
        )
        metadata_store.begin_data_audit_run(audit)
        snapshot = DatasetSnapshot.create(
            strategy_name=version.strategy_id,
            manifest_id=value.input_hash,
            as_of_time=now,
            code_commit=value.request.producer_commit,
            origin="trusted-template-producer",
            created_at=now,
        )
        metadata_store.begin_dataset_snapshot(snapshot)
        for scope, count in (
            ("template_input", 1),
            ("template_days", len(value.days)),
            ("template_work_units", template_input_work_units(value)),
        ):
            metadata_store.upsert_dataset_coverage(
                DatasetCoverage(
                    snapshot_id=snapshot.snapshot_id,
                    dataset_id=TEMPLATE_INPUT_TABLE,
                    table_name=TEMPLATE_INPUT_TABLE,
                    coverage_scope=scope,
                    expected_count=count,
                    available_count=count,
                    created_at=now,
                )
            )
        metadata_store.finalize_dataset_snapshot(
            snapshot.snapshot_id,
            DatasetSnapshotFinalization(
                table_watermarks=template_watermarks(value, audit_id=audit.audit_run_id),
                completed_at=now,
            ),
        )
        binding = build_dataset_snapshot_binding(
            metadata_store=metadata_store,
            source_connection=connection,
            catalog=catalog,
            lake_root=lake_root,
            snapshot_id=snapshot.snapshot_id,
            start_date=value.request.days[0].trade_date,
            end_date=value.request.days[-1].trade_date,
            dependencies=template_execution_dependencies(version),
            now=lambda: now,
        )
    metadata_store.finalize_data_audit_run(
        audit.audit_run_id, DataAuditRunFinalization(p0_count=0, completed_at=now)
    )
    request = ResearchGateRequest(
        mode="exploratory",
        strategy_name=version.strategy_id,
        start_date=value.request.days[0].trade_date,
        end_date=value.request.days[-1].trade_date,
        code_commit=value.request.producer_commit,
        audit_run_id=audit.audit_run_id,
        dataset_snapshot_id=snapshot.snapshot_id,
        dataset_binding_hash=binding.binding_hash,
    )
    with ResearchExecutionSession(binding=binding, lake_root=lake_root) as session:
        verify_bound_template_input(metadata_store, request, session, version=version)
    decision = require_template_gate(
        metadata_store, request, version=version, binding_verified=True
    )
    return PublishedStrategyTemplateInput(
        input_hash=value.input_hash,
        identity=DatasetSnapshotIdentity(
            snapshot_id=snapshot.snapshot_id,
            binding_hash=binding.binding_hash,
            audit_run_id=audit.audit_run_id,
        ),
        request=request,
        gate_decision=decision,
    )


def require_template_gate(
    store: DuckDBStore,
    request: ResearchGateRequest,
    *,
    version: StrategyTemplateExecutionVersion,
    binding_verified: bool,
) -> ResearchGateDecision:
    snapshot = store.get_dataset_snapshot(request.dataset_snapshot_id)
    audit = store.get_data_audit_run(request.audit_run_id)
    binding = store.get_dataset_snapshot_binding(request.dataset_snapshot_id)
    if (
        snapshot is None
        or audit is None
        or binding is None
        or request.strategy_name != version.strategy_id
        or request.mode != "exploratory"
    ):
        raise PermissionError("template original source metadata is unavailable")
    w, m = snapshot.table_watermarks, binding.manifest
    if (
        snapshot.status != "ready"
        or snapshot.quality_issue_ids
        or (
            snapshot.strategy_name,
            snapshot.code_commit,
            snapshot.manifest_id,
            w.get("template_owner_id"),
            w.get("template_definition"),
            w.get("template_record_hash"),
            w.get("template_rules_hash"),
            w.get("manifest_start_date"),
            w.get("manifest_end_date"),
        )
        != (
            version.strategy_id,
            request.code_commit,
            w.get("template_input_hash"),
            version.owner_id,
            version.head.registration_fingerprint,
            version.head.record_hash,
            version.rules.rules_hash,
            request.start_date.isoformat(),
            request.end_date.isoformat(),
        )
    ):
        raise PermissionError("template snapshot identity differs")
    if (
        audit.status != "completed"
        or audit.p0_count
        or audit.finding_issue_ids
        or audit.audit_run_id != w.get("template_audit_id")
        or audit.rule_set_version != f"{TEMPLATE_AUDIT_RULE}:{w.get('template_input_hash')}"
        or (audit.range_start, audit.range_end) != (request.start_date, request.end_date)
    ):
        raise PermissionError("template audit identity differs")
    if (
        binding.status != "ready"
        or binding.binding_hash != request.dataset_binding_hash
        or (
            m.strategy_name,
            m.code_commit,
            m.as_of_time,
            m.start_date,
            m.end_date,
            m.dependency_contract_version,
            m.eligibility_resolution_hash,
        )
        != (
            snapshot.strategy_name,
            snapshot.code_commit,
            snapshot.as_of_time,
            request.start_date,
            request.end_date,
            TEMPLATE_INPUT_CONTRACT,
            None,
        )
    ):
        raise PermissionError("template immutable binding differs")
    if len(m.artifacts) != 1 or (
        m.artifacts[0].dataset_id,
        m.artifacts[0].table_name,
        m.artifacts[0].row_count,
        m.artifacts[0].artifact_type,
    ) != (TEMPLATE_INPUT_TABLE, TEMPLATE_INPUT_TABLE, 1, "materialized_table"):
        raise PermissionError("template immutable artifact differs")
    scopes = {
        "template_input": "1",
        "template_days": w.get("template_days"),
        "template_work_units": w.get("template_work_units"),
    }
    coverages = store.list_dataset_coverages(snapshot.snapshot_id)
    if (
        len(coverages) != 3
        or {c.coverage_scope for c in coverages} != set(scopes)
        or any(
            (
                c.dataset_id,
                c.table_name,
                str(c.expected_count),
                c.available_count,
                bool(c.missing_reasons),
            )
            != (
                TEMPLATE_INPUT_TABLE,
                TEMPLATE_INPUT_TABLE,
                scopes[c.coverage_scope],
                c.expected_count,
                False,
            )
            for c in coverages
        )
    ):
        raise PermissionError("template source coverage differs")
    if store.list_open_data_quality_issues(severities=("P0",)) or not binding_verified:
        raise PermissionError("template source is not verified")
    counts = {c.coverage_scope: (c.available_count, c.expected_count) for c in coverages}
    return ResearchGateDecision(
        allowed=True,
        research_status="exploratory",
        audit_run_id=audit.audit_run_id,
        dataset_snapshot_id=snapshot.snapshot_id,
        dataset_binding_hash=binding.binding_hash,
        coverage_counts=counts,
        coverage_ratios={key: 1.0 for key in counts},
        failures=(),
    )


def verify_bound_template_input(
    store: DuckDBStore,
    request: ResearchGateRequest,
    session: ResearchExecutionSession,
    *,
    version: StrategyTemplateExecutionVersion,
) -> FrozenStrategyTemplateInput:
    snapshot = store.get_dataset_snapshot(request.dataset_snapshot_id)
    if (
        snapshot is None
        or session.snapshot_id != snapshot.snapshot_id
        or session.binding_hash != request.dataset_binding_hash
    ):
        raise PermissionError("template bound snapshot differs")
    value = verify_template_snapshot_source(
        session._conn,
        version=version,
        code_sha=request.code_commit,
        start_date=request.start_date,
        end_date=request.end_date,
        input_hash=snapshot.table_watermarks.get("template_input_hash", ""),
        as_of=snapshot.as_of_time,
    )
    if snapshot.table_watermarks != template_watermarks(value, audit_id=request.audit_run_id):
        raise PermissionError("template bound complete source facts differ")
    return value


@contextmanager
def open_gated_template_store(
    request: ResearchGateRequest,
    *,
    metadata_store_factory: Callable[[], AbstractContextManager[DuckDBStore]],
    lake_root: Path,
    version: StrategyTemplateExecutionVersion,
) -> Iterator[ResearchExecutionSession]:
    with metadata_store_factory() as metadata:
        require_template_gate(metadata, request, version=version, binding_verified=True)
        binding = metadata.get_dataset_snapshot_binding(request.dataset_snapshot_id)
        with ResearchExecutionSession(binding=binding, lake_root=lake_root) as session:
            verify_bound_template_input(metadata, request, session, version=version)
            yield session
