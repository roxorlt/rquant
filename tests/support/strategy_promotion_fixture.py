"""Original M13 preparation and Lab admission; workers remain Root-only."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

NEUTRAL_GENERATION = "synthetic-fixed-version-no-entry/v1"

if TYPE_CHECKING:
    from sqlite3 import Connection

    from rquant.experiment_platform import ExperimentFamilyRecord, ExperimentPlatformStore
    from rquant.experiment_platform_commands import (
        ExperimentCommand,
        ExperimentCommandWriter,
        ExperimentFamilyPreparer,
    )
    from rquant.experiment_platform_projection import ExperimentPrivateProjectionReader
    from rquant.experiment_platform_templates import ExperimentTemplateBinding
    from rquant.lab_finalizer import LabFinalizerResult
    from rquant.lab_worker import LabWorkerTickResult
    from rquant.page_control import PageControlReceipt, PageControlService
    from rquant.paper_portfolio_view_source import PaperPortfolioViewSource
    from rquant.strategy_authoring_admission import StrategyAuthoringAdmission
    from rquant.strategy_promotion import StrategyPromotionPageControlBackend
    from rquant.strategy_promotion_commands import StrategyPromotionCommand
    from rquant.strategy_promotion_contracts import (
        PromotionEvidenceSelection,
        StrategyPromotionApproval,
        StrategyPromotionTarget,
    )
    from rquant.strategy_template_run_commands import StrategyTemplateRunReceipt
    from rquant.strategy_template_source import StrategyTemplateSourceData
    from tests.support.ai_assistance_fixture import PortfolioFoundation


@dataclass(frozen=True)
class StrategyPromotionIntegrationFixture:
    root: Path
    foundation: PortfolioFoundation
    platform: ExperimentPlatformStore
    binding: ExperimentTemplateBinding
    projection: ExperimentPrivateProjectionReader
    backend: StrategyPromotionPageControlBackend
    record: ExperimentFamilyRecord
    time: list[datetime]
    control: PageControlService
    admission: StrategyAuthoringAdmission
    family_preparer: ExperimentFamilyPreparer
    experiment_backend: ExperimentCommandWriter
    publication_sequence: list[int] = field(default_factory=lambda: [0])
    owned_broker_connections: list[Connection] = field(default_factory=list)

    def clock(self) -> datetime:
        return self.time[0]

    def publish(self, *, sequence: int | None = None) -> object:
        from rquant.serving_publisher import ServingPublisher
        from rquant.serving_read_models import (
            SERVING_TABLE_SPECS,
            ServingProjectionInput,
            ServingReadModelInput,
            build_serving_read_models,
        )
        from rquant.strategy_authoring_projection import (
            build_strategy_authoring_snapshot,
            project_strategy_authoring,
        )
        from rquant.strategy_promotion_projection import StrategyPromotionProjectionReader
        from tests.support.web_serving_fixture import _generation_ids, _watermarks

        at = self.clock()
        if sequence is None:
            sequence = self.publication_sequence[0]
        generations = _generation_ids("baseline", sequence)
        parent = build_strategy_authoring_snapshot(
            self.binding.original, available_at=at, source_catalogs=self.binding.catalogs
        )
        payloads = (
            ("lab_jobs", project_strategy_authoring(parent).serving_payloads()),
            (
                "promotions",
                (
                    *self.projection(at),
                    *StrategyPromotionProjectionReader(self.binding.private)(at),
                ),
            ),
        )
        projections = tuple(
            ServingProjectionInput.bind(
                p, owner_dataset_id=dataset, owner_generation_id=generations[dataset]
            )
            for dataset, group in payloads
            for p in group
        )
        tables = build_serving_read_models(
            ServingReadModelInput(observed_at=at, projections=projections)
        )
        value = ServingPublisher(
            self.root / "serving",
            producer_commit=self.foundation.code_commit,
            schema_version=3,
            table_specs=SERVING_TABLE_SPECS,
        ).publish(
            tables,
            watermarks=_watermarks(
                "baseline", built_at=at, generations=generations, sequence=sequence
            ),
            source_generations=generations,
            built_at=at,
        )
        self.publication_sequence[0] = max(self.publication_sequence[0], sequence + 1)
        return value

    def close(self) -> None:
        try:
            for connection in self.owned_broker_connections:
                connection.close()
            self.owned_broker_connections.clear()
        finally:
            self.foundation.artifacts.close()


def build_original_variant_fixture(
    root: Path, *, complete_history: bool = False, clock: Callable[[], datetime] | None = None
) -> StrategyPromotionIntegrationFixture:
    """Prepare full original slots and queued jobs, without supplying terminal status."""
    from rquant.collaboration_commands import PageControlRoleAuthority
    from rquant.collaboration_roles import RoleEntry, RoleState
    from rquant.experiment_platform import (
        ExperimentPlatformStore,
        stable_experiment_interaction,
        stable_experiment_job,
    )
    from rquant.experiment_platform_projection import ExperimentPrivateProjectionReader
    from rquant.experiment_platform_templates import ExperimentTemplateRuntimeBinding
    from rquant.experiment_registry import ExperimentRegistryReadonlyReader
    from rquant.lab_artifact_preview import ArtifactPreviewReader
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService
    from rquant.strategy_authoring_admission import StrategyAuthoringAdmission
    from rquant.strategy_promotion import (
        StrategyPromotionBackend,
        StrategyPromotionPageControlBackend,
    )
    from rquant.strategy_promotion_evidence import StrategyPromotionEvidenceSource
    from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader
    from tests.support.ai_assistance_fixture import build_portfolio_foundation, private_directory
    from tests.unit.test_experiment_platform import NOW
    from tests.unit.test_experiment_platform_flow import preparation
    from tests.unit.test_experiment_platform_templates import configure_template, ready_template

    root = root.absolute()
    private_directory(root)
    original = preparation.__wrapped__(private_directory(root / "source"))
    time = [NOW if clock is None else clock()]
    if complete_history:
        original = _extend_original_history(original, observed_at=time[0])
    _, producer, data, _, _, _ = original
    foundation = build_portfolio_foundation(
        private_directory(root / "lab"), data, clock=lambda: time[0]
    )
    platform = ExperimentPlatformStore(
        foundation.commands.experiment_registry, activate_private_schema=True
    )
    producer.store = platform
    producer.definitions = foundation.commands.definition_registry
    original = (platform, *original[1:])
    templates = private_directory(root / "templates")
    if complete_history:
        from uuid import UUID

        from rquant.experiment_platform import ExperimentSearchRequest
        from rquant.research_catalog import ResearchCatalog
        from rquant.runtime_contracts import canonical_sha256
        from rquant.storage.duckdb import DuckDBStore

        producer.metadata_store_factory = lambda: DuckDBStore(foundation.metadata_path)
        producer.catalog, producer.lake_root = (
            ResearchCatalog(foundation.catalog_path),
            foundation.lake_root,
        )
        producer.input_root = foundation.input_root
        stat = producer.input_root.stat()
        producer._input_root_identity = (stat.st_dev, stat.st_ino)
        producer.clock = lambda: time[0]
        platform, producer, _, binding, request = configure_template(original, templates)
        history = tuple(day.trade_date for day in data.template.days)
        protocol = _full_protocol(history)
        request = ExperimentSearchRequest.model_validate(
            request.model_dump(mode="python")
            | {
                "protocol": protocol,
                "base_config": request.base_config.model_dump(mode="python")
                | {
                    "start_date": history[0],
                    "end_date": history[83],
                    "initial_cash": Decimal("100000"),
                },
            }
        )
        binding.original.clock = binding.private.clock = lambda: time[0]
        record = platform.begin_request(
            owner="alice",
            request_id=UUID(int=900),
            body_hash=canonical_sha256(request),
            request=request,
            registered_at=time[0],
            template_baseline=binding.baseline(owner="alice", request=request),
        )
        record = producer(record)
        foundation.protocol = protocol
    else:
        platform, producer, _, binding, _, record = ready_template(original, templates)
    runtime = ExperimentTemplateRuntimeBinding(store=platform, binding=binding)
    foundation.commands.template_directory = foundation.scheduler.template_directory = (
        binding.directory
    )
    foundation.commands.experiment_template_binding = (
        foundation.scheduler.experiment_template_binding
    ) = runtime
    for index in range(len(record.actual_configurations)):
        prepared = platform.preparation(record.owner, record.family_id, index).prepared
        foundation.commands.submit_create(
            prepared.submission(
                job_id=stable_experiment_job(record.owner, record.request_id, index)
            ).command,
            interaction_key=stable_experiment_interaction(record.owner, record.request_id, index),
        )
    foundation.scheduler.run_once()
    time[0] += timedelta(seconds=1)
    registry = ExperimentRegistryReadonlyReader(
        platform.registry.path, managed_trust_root=platform.registry.path.parent
    )
    projection = ExperimentPrivateProjectionReader(
        registry=registry, jobs=foundation.reader, owners=frozenset({"alice"})
    )
    roles = private_directory(root / "roles") / "roles.json"
    roles.write_text(
        RoleState.create(
            revision=1,
            users=(
                RoleEntry(username="alice", role="admin"),
                RoleEntry(username="admin", role="admin"),
                RoleEntry(username="viewer", role="viewer"),
            ),
        ).model_dump_json()
    )
    roles.chmod(0o600)
    authority = PageControlRoleAuthority(mode="enforced", roles_path=roles, clock=lambda: time[0])
    template_results = StrategyTemplateSealedResultReader(
        reader=foundation.reader,
        artifact_reader=ArtifactPreviewReader(
            reader=foundation.reader, artifact_root=foundation.artifacts.root
        ),
    )
    source = StrategyPromotionEvidenceSource(
        registry=platform.registry,
        platform=platform,
        projection=projection,
        results=foundation.results,
        template_results=template_results,
        template_binding=binding,
    )
    if complete_history:
        from rquant.experiment_platform import ExperimentPhaseRead
        from rquant.experiment_platform_evidence import ExperimentIndependenceEvidence
        from rquant.runtime_contracts import canonical_sha256
        from rquant.strategy_promotion_walk_forward import StrategyPromotionWalkForwardBackend
        from rquant.strategy_template_definition import StrategyTemplateExecutionVersion
        from rquant.strategy_template_source import StrategyTemplateSourceData
        from rquant.strategy_template_submission import (
            StrategyTemplateRunBackend,
            StrategyTemplateRunPreparer,
        )

        def run_source(
            owner: str, generation: str, version: StrategyTemplateExecutionVersion
        ) -> StrategyTemplateSourceData:
            if (
                owner != "alice"
                or binding.private.get_version(
                    version.strategy_id, version.head.version, owner_id=owner
                ).head
                != version.head
            ):
                raise PermissionError("exact original fixed version is required")
            profile = producer.profiles[0]
            read = ExperimentPhaseRead(
                owner=owner,
                family_id=record.family_id,
                source_identity=profile.source_identity,
                source_key=profile.source_key,
                source_version=profile.source_version,
                phase="search",
                window=record.request.protocol.train_range.model_copy(
                    update={"end_date": record.request.protocol.validation_range.end_date}
                ),
            )
            value = binding.phase_provider(read, version)
            if generation == NEUTRAL_GENERATION:
                value = _original_no_entry_source(value)
            return StrategyTemplateSourceData.model_validate(
                value.model_dump(mode="python")
                | {
                    "catalog": value.catalog.model_copy(update={"generation_id": generation}),
                    "material_hash": None,
                }
            )

        preparer = StrategyTemplateRunPreparer(
            source_provider=run_source,
            metadata_store_factory=producer.metadata_store_factory,
            catalog=producer.catalog,
            lake_root=producer.lake_root,
            input_root=producer.input_root,
            experiments=platform.registry,
            protocol=record.request.protocol,
            code_commit=foundation.code_commit,
            clock=lambda: time[0],
        )
        runs = StrategyTemplateRunBackend(
            binding.private,
            facade=foundation.commands,
            preparer=preparer,
            expected_identity=binding.expected_private_identity,
        )
        source.walk_forward = StrategyPromotionWalkForwardBackend(
            registry=platform.registry, runs=runs, results=template_results
        )

        def synthetic_independence(
            family: Any, facts: tuple[Any, ...], values: tuple[Any, ...]
        ) -> ExperimentIndependenceEvidence:
            dates = tuple(point.trade_date for point in values[0].curves)
            hashes = tuple(value.result_hash for value in values)
            if len(values) != len(facts) or len(values) != family.planned_count:
                raise ValueError(
                    "synthetic independence requires the complete actually sealed parent"
                )
            return ExperimentIndependenceEvidence(
                evidence_id="synthetic-original-worker-validation/v1",
                body_hash=canonical_sha256({"dates": dates, "hashes": hashes}),
                family_id=family.family_id,
                period_end_dates=dates,
                result_hashes=hashes,
                independent_observations=len(dates),
                independent_trial_count=len(facts),
                assumptions=(
                    "合成市场的完整原 worker 结果，用于验收原统计和完整父族校正；"
                    "此独立性假设不用于真实研究。",
                ),
            )

        source.independence_resolver = synthetic_independence
    domain = StrategyPromotionBackend(binding.private, roles=authority, source=source, enabled=True)
    backend = StrategyPromotionPageControlBackend(domain, operator_users=frozenset({"alice"}))
    outbox = PageControlOutbox(roles.parent / "control.sqlite")
    outbox.path.chmod(0o600)
    from rquant.experiment_platform_commands import ExperimentCommandWriter

    experiments = ExperimentCommandWriter(
        store=platform,
        commands=foundation.commands,
        prepare=producer,
        results=foundation.results,
        template_results=template_results,
        enabled=True,
        owners=frozenset({"alice"}),
        administrators=frozenset({"alice"}),
        private_authority=projection.authority,
    )
    control = PageControlService(
        outbox=outbox,
        collaboration=authority,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=root / "data",
            log_dir=root / "logs",
            clock=lambda: time[0],
            strategy_promotion_backend=backend,
            experiment_backend=experiments,
        ),
    )
    admission = StrategyAuthoringAdmission(
        control, source_catalog_provider=lambda *_: binding.catalogs[0]
    )
    return StrategyPromotionIntegrationFixture(
        root,
        foundation,
        platform,
        binding,
        projection,
        backend,
        record,
        time,
        control,
        admission,
        producer,
        experiments,
    )


def _full_protocol(history: tuple[date, ...]) -> Any:
    from rquant.experiment_registry import DateRange
    from rquant.portfolio_backtest_source import PortfolioExperimentProtocol

    return PortfolioExperimentProtocol(
        train_range=DateRange(start_date=history[0], end_date=history[11]),
        validation_range=DateRange(start_date=history[12], end_date=history[83]),
        frozen_outer_test_range=DateRange(start_date=history[84], end_date=history[103]),
    )


def _extend_original_history(
    original: tuple[Any, ...], *, observed_at: datetime
) -> tuple[Any, ...]:
    from rquant.backtest.contracts import BacktestRequest, SSECalendar
    from rquant.experiment_registry import DateRange
    from rquant.portfolio_backtest_source import PortfolioSourceData
    from rquant.runtime_contracts import canonical_sha256
    from tests.unit.test_portfolio_backtest import _CODES, _day

    platform, producer, short, profile, reads, kwargs = original
    first = date(2026, 1, 5)
    all_dates = tuple(
        first + timedelta(days=index)
        for index in range(
            (max(observed_at.date(), date(2026, 8, 1)) + timedelta(days=60) - first).days + 1
        )
        if (first + timedelta(days=index)).weekday() < 5
    )
    history = all_dates[:104]
    previous = date(2026, 1, 2)
    calendar = SSECalendar(
        source_identity=canonical_sha256(
            {"kind": "synthetic-weekday-calendar/v1", "dates": (previous, *all_dates)}
        ),
        coverage_start=previous,
        coverage_end=all_dates[-1],
        dates=(previous, *all_dates),
    )
    days, price = [], Decimal("10.00")
    for index, day in enumerate(history):
        market = _day(day, previous if index == 0 else history[index - 1], _CODES[index % 2])
        close = (price * (Decimal("1.025") if index % 3 else Decimal("1.015"))).quantize(
            Decimal(".01")
        )
        days.append(
            market.model_copy(
                update={
                    "instruments": tuple(
                        item.model_copy(
                            update={
                                "decision_price": price,
                                "open_price": price,
                                "close_price": close,
                            }
                        )
                        for item in market.instruments
                    )
                }
            )
        )
        price = close
    raw = BacktestRequest.model_validate(
        short.template.model_dump(mode="python")
        | {"calendar": calendar, "days": tuple(days), "initial_cash": Decimal("100000")}
    )
    data = PortfolioSourceData(
        source_key=short.source_key,
        source_version=short.source_version,
        template=raw,
        sources=short.sources,
        benchmarks={
            "000300.SH": tuple(
                (day, 100.0 + index) for index, day in enumerate((previous, *history))
            )
        },
    )
    profile = type(profile).model_validate(
        profile.model_dump(mode="python")
        | {
            "calendar": calendar,
            "coverage": DateRange(start_date=history[0], end_date=history[-1]),
            "latest_complete": history[-1],
            "source_identity": None,
        }
    )
    producer.profiles = (profile,)

    def phase_provider(request: Any) -> PortfolioSourceData:
        reads.append(request)
        baseline = calendar.dates[calendar.dates.index(request.window.start_date) - 1]
        return PortfolioSourceData(
            source_key=data.source_key,
            source_version=data.source_version,
            template=data.template.model_copy(
                update={
                    "days": tuple(
                        day
                        for day in data.template.days
                        if request.window.start_date <= day.trade_date <= request.window.end_date
                    )
                }
            ),
            sources=data.sources,
            benchmarks={
                key: tuple(
                    (day, value)
                    for day, value in rows
                    if baseline <= day <= request.window.end_date
                )
                for key, rows in data.benchmarks.items()
            },
        )

    producer.phase_provider = phase_provider
    return platform, producer, data, profile, reads, kwargs


def build_original_promotion_fixture(
    root: Path, *, clock: Callable[[], datetime] | None = None
) -> StrategyPromotionIntegrationFixture:
    """Full synthetic inputs; original workers still provide every sealed result."""
    return build_original_variant_fixture(root, complete_history=True, clock=clock)


def _original_no_entry_source(value: StrategyTemplateSourceData) -> StrategyTemplateSourceData:
    """Explicit synthetic no-entry input; every return still comes from the original worker."""
    from rquant.portfolio_backtest_source import PortfolioSourceData
    from rquant.runtime_contracts import canonical_sha256
    from rquant.strategy_template_source import StrategyTemplateSourceData

    days = tuple(
        day.model_copy(
            update={
                "ranking": day.ranking.model_copy(
                    update={
                        "candidates": (),
                        "source_identity": canonical_sha256(
                            {
                                "kind": NEUTRAL_GENERATION,
                                "trade_date": day.trade_date,
                                "candidates": (),
                            }
                        ),
                    }
                )
            }
        )
        for day in value.portfolio.template.days
    )
    sources = value.portfolio.sources.model_copy(
        update={"ranking_hash": canonical_sha256(tuple(day.ranking for day in days))}
    )
    portfolio = PortfolioSourceData.model_validate(
        value.portfolio.model_dump(mode="python")
        | {
            "template": value.portfolio.template.model_copy(update={"days": days}),
            "sources": sources,
            "material_hash": None,
        }
    )
    raw = tuple(
        day.model_copy(
            update={
                "entry": day.entry.model_copy(
                    update={
                        "rows": tuple(
                            {"ts_code": instrument.ts_code, "is_st": True}
                            for instrument in market.instruments
                        ),
                        "source_hash": market.ranking.source_identity,
                    }
                )
            }
        )
        for day, market in zip(value.days, days, strict=True)
    )
    return StrategyTemplateSourceData.model_validate(
        value.model_dump(mode="python")
        | {"portfolio": portfolio, "days": raw, "material_hash": None}
    )


def install_original_empty_paper(
    fixture: StrategyPromotionIntegrationFixture, target: StrategyPromotionTarget
) -> PaperPortfolioViewSource:
    """An original empty ledger is complete evidence; it supplies no fabricated fills."""
    from rquant.paper_broker import PaperBrokerStore
    from rquant.paper_operator import PaperOperatorControlStore
    from rquant.paper_portfolio_models import PaperPortfolioBinding, PaperPortfolioConfiguration
    from rquant.paper_portfolio_runtime import PaperPortfolioRuntime
    from rquant.paper_portfolio_source import PaperPortfolioMaterialStore
    from rquant.paper_portfolio_state import PaperPortfolioStateStore
    from rquant.paper_portfolio_view_source import PaperPortfolioViewSource
    from rquant.paper_signal_worker import PaperSignalQueueStore
    from rquant.runtime_contracts import canonical_sha256
    from tests.paper_cost_fixtures import paper_cost_policy
    from tests.support.ai_assistance_fixture import private_directory
    from tests.unit.test_paper_signal_worker import _policy

    metadata = fixture.binding.private.get_version(
        target.strategy_id, target.head.version, owner_id=target.owner_id
    )
    receipt = next(
        fixture.platform.preparation("alice", fixture.record.family_id, index)
        for index in range(len(fixture.record.actual_configurations))
        if fixture.platform.preparation(
            "alice", fixture.record.family_id, index
        ).prepared.registration.logical_id
        == target.strategy_id
    )
    prepared = receipt.prepared
    costs = prepared.frozen.request.execution_cost_spec
    if metadata.head != target.head or canonical_sha256(costs) != target.cost_fingerprint:
        raise ValueError("paper fixture requires the exact original saved version and full costs")
    root = private_directory(fixture.root / "paper")
    binding = PaperPortfolioBinding(
        role_id="paper.main.v1",
        account_id="paper-c5",
        owner_id=target.owner_id,
        strategy_id=target.strategy_id,
        strategy_version=str(target.head.version),
        parameter_fingerprint=target.parameter_fingerprint,
        cost_spec_id=costs.cost_spec_id,
        ledger_id="synthetic-c5-original-ledger",
        manifest_fingerprint=canonical_sha256(
            {"kind": "synthetic-original-role/v1", "target": target}
        ),
    )
    configuration = PaperPortfolioConfiguration(
        binding=binding,
        version=1,
        configured_at=fixture.clock(),
        weight_rule=metadata.rules.weight_rule,
        drawdown_rule=receipt.configuration.drawdown_rule,
        execution_cost_spec=costs,
    )
    state = PaperPortfolioStateStore(root / "state.sqlite", configuration=configuration)
    operator = PaperOperatorControlStore(state, root=root / "operator", clock=fixture.clock)
    broker = PaperBrokerStore(
        root / "broker.duckdb",
        account_id=binding.account_id,
        initial_cash=Decimal("1000"),
        cost_policy=paper_cost_policy(),
    )
    fixture.owned_broker_connections.append(broker._connect())
    broker.account_authority_snapshot(
        as_of=fixture.clock(), market_prices={}, producer_commit=fixture.foundation.code_commit
    )
    runtime = PaperPortfolioRuntime(
        state,
        operator=operator,
        materials=PaperPortfolioMaterialStore(state),
        producer_commit=fixture.foundation.code_commit,
    )
    runtime.calendar = prepared.frozen.request.calendar
    queue = PaperSignalQueueStore(
        root / "signals.sqlite",
        policy=_policy(producer_commit=fixture.foundation.code_commit).model_copy(
            update={"account_id": binding.account_id}
        ),
    )
    source = PaperPortfolioViewSource(runtime, broker=broker, queue=queue)
    fixture.backend.domain.source.paper_sources = (source,)
    return source


def install_original_paper_research(
    fixture: StrategyPromotionIntegrationFixture, source: PaperPortfolioViewSource
) -> None:
    """Same concrete metadata, reader, producer catalog, worker and finalizer directory."""
    from rquant.lab_artifact_preview import ArtifactPreviewReader
    from rquant.paper_backtest_source import PaperBacktestSourceReader
    from rquant.paper_research_artifact import PaperResearchResultReader
    from rquant.paper_research_runtime import PaperResearchRuntimeDirectory
    from rquant.paper_research_submission import PaperResearchRunBackend, PaperResearchRunPreparer
    from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader

    foundation, producer, state = fixture.foundation, fixture.family_preparer, source.runtime.state
    templates = StrategyTemplateSealedResultReader(
        reader=foundation.reader,
        artifact_reader=ArtifactPreviewReader(
            reader=foundation.reader, artifact_root=foundation.artifacts.root
        ),
    )
    backtests = PaperBacktestSourceReader(
        store=fixture.binding.private,
        expected_identity=fixture.binding.expected_private_identity,
        sealed_reader=templates,
    )
    preparer = PaperResearchRunPreparer(
        sources=(source,),
        metadata_store_factory=producer.metadata_store_factory,
        research_catalog=producer.catalog,
        input_root=producer.input_root,
        lake_root=producer.lake_root,
        code_sha=foundation.code_commit,
        clock=fixture.clock,
        backtest_reader=backtests,
    )
    runs = PaperResearchRunBackend(preparer=preparer, facade=foundation.commands)
    runs._table(state)
    source.research_results = PaperResearchResultReader(
        backend=runs,
        reader=foundation.reader,
        artifact_reader=ArtifactPreviewReader(
            reader=foundation.reader, artifact_root=foundation.artifacts.root
        ),
    )
    directory = PaperResearchRuntimeDirectory(
        states=(state,), expected_identities=(state.identity(),)
    )
    foundation.commands.paper_directory = foundation.scheduler.paper_directory = directory


def record_original_twenty_paper_closes(
    fixture: StrategyPromotionIntegrationFixture,
    source: PaperPortfolioViewSource,
    *,
    approved_at: datetime,
) -> tuple[date, ...]:
    """Record twenty original calendar closes from the empty broker; supply no fills or NAV."""
    from datetime import UTC, time
    from zoneinfo import ZoneInfo

    from rquant.paper_portfolio_source import PaperPortfolioMarketSnapshot, PaperPortfolioRawFact
    from rquant.runtime_contracts import canonical_sha256

    calendar = source.runtime.calendar
    if calendar is None:
        raise ValueError("exact original forward calendar is required")
    approved_date = approved_at.astimezone(ZoneInfo("Asia/Shanghai")).date()
    dates = tuple(day for day in calendar.dates if day > approved_date)[:20]
    if len(dates) != 20 or source.views.nav_series():
        raise ValueError("fresh original complete twenty-day calendar is required")
    configuration = source.runtime.state.configuration
    fixture.foundation.scheduler.release()
    for day in dates:
        at = datetime.combine(day, time(7), tzinfo=UTC)
        fixture.time[0] = at
        snapshot = canonical_sha256(
            {"kind": "synthetic-empty-paper-market/v1", "trade_date": day, "price": Decimal("10")}
        )
        source.runtime.materials.publish(
            PaperPortfolioMarketSnapshot(
                binding=configuration.binding,
                configuration_fingerprint=configuration.fingerprint,
                dataset_snapshot_id=snapshot,
                feature_snapshot_id=snapshot,
                observed_at=at,
                available_at=at,
                facts=(
                    PaperPortfolioRawFact(
                        ts_code="600001.SH",
                        candidate=False,
                        rank_score=None,
                        industry_l1="合成行业",
                        valuation_price=Decimal("10"),
                        trading_status="normal",
                        observed_at=at,
                        available_at=at,
                        source_snapshot_id=snapshot,
                    ),
                ),
            )
        )
        value = source.read(as_of=at)
        if (
            value.status != "complete"
            or value.nav[-1].trade_date != day
            or value.nav[-1].status != "complete"
        ):
            raise ValueError("original daily close was not complete")
    if tuple(point.trade_date for point in source.views.nav_series()) != dates:
        raise ValueError("original NAV did not retain every forward trading day")
    return dates


def submit_original_neutral_backtest(
    fixture: StrategyPromotionIntegrationFixture, target: StrategyPromotionTarget
) -> StrategyTemplateRunReceipt:
    """Root seals this original same-version run before using it for the bootstrap band."""
    from uuid import uuid5

    from rquant.strategy_template_run_commands import RunStrategyTemplate

    wf = fixture.backend.domain.source.walk_forward
    if wf is None:
        raise ValueError("original template run backend is not installed")
    protocol = fixture.record.request.protocol
    request = RunStrategyTemplate(
        command_id=str(uuid5(fixture.record.request_id, "synthetic-no-entry-band-backtest")),
        requested_at=fixture.clock(),
        generation_id=NEUTRAL_GENERATION,
        strategy_id=target.strategy_id,
        head=target.head,
        expected_head=target.head,
        start_date=protocol.train_range.start_date,
        end_date=protocol.validation_range.end_date,
        initial_cash=Decimal("100000"),
    )
    return wf.runs.submit(
        wf.runs.compile(
            request,
            owner_id=target.owner_id,
            expected_identity=fixture.binding.expected_private_identity,
        )
    )


def submit_original_paper_band(
    fixture: StrategyPromotionIntegrationFixture,
    source: PaperPortfolioViewSource,
    *,
    backtest_job: UUID,
) -> UUID:
    """Actual original band admission; no caller-computed interval or terminal status."""
    from uuid import uuid5

    from rquant.paper_research_commands import RunPaperPortfolioResearch

    if source.research_results is None:
        raise ValueError("original paper research chain is required")
    state = source.runtime.state
    command_id = uuid5(fixture.record.request_id, "synthetic-original-bootstrap-band")
    request = RunPaperPortfolioResearch(
        command_id=str(command_id),
        requested_at=fixture.clock(),
        generation_id=fixture.publish().generation_id,
        account_id=state.configuration.binding.account_id,
        configuration_fingerprint=state.configuration.fingerprint,
        task_name="paper_backtest_band",
        backtest_job_id=backtest_job,
    )
    runs = source.research_results.backend
    owned = runs.compile(
        request, owner_id=state.configuration.binding.owner_id, expected_identity=state.identity()
    )
    receipt = runs.submit(owned)
    if receipt["job_id"] != str(command_id):
        raise ValueError("original band receipt differs from its UUID")
    return command_id


def approve_original_next_stage(
    fixture: StrategyPromotionIntegrationFixture,
    *,
    target: StrategyPromotionTarget,
    selection: PromotionEvidenceSelection,
) -> StrategyPromotionApproval:
    """Three real owner commands, typed eligibility, exact confirmation, same original journal."""
    from uuid import uuid5

    from rquant.strategy_promotion_commands import (
        ApprovePromotion,
        PreparePromotionApproval,
        RequestPromotionReview,
    )
    from rquant.strategy_promotion_contracts import (
        PreparedPromotionApproval,
        StrategyPromotionApproval,
        StrategyPromotionReview,
    )

    state = fixture.binding.private.promotion_state(target)
    generation = fixture.publish().generation_id
    request = RequestPromotionReview(
        command_id=str(uuid5(fixture.record.request_id, "synthetic-review:" + str(state.revision))),
        requested_at=fixture.clock(),
        generation_id=generation,
        target=target,
        expected_revision=state.revision,
        selection=selection,
    )

    def submit(command: StrategyPromotionCommand) -> Any:
        receipt = fixture.control._submit_trusted_strategy_promotion(
            command,
            authenticated_actor_id=target.owner_id,
            verified_metadata_identity=fixture.binding.expected_private_identity,
        )
        if receipt.status.value != "succeeded" or receipt.result is None:
            raise ValueError(
                "original manual promotion command did not complete: " + str(receipt.error)
            )
        return receipt.result

    review = StrategyPromotionReview.model_validate(submit(request))
    if not review.eligible:
        raise ValueError(
            "actual original promotion gates did not pass: " + review.model_dump_json()
        )
    prepare = PreparePromotionApproval(
        command_id=str(
            uuid5(fixture.record.request_id, "synthetic-prepare:" + str(state.revision))
        ),
        requested_at=fixture.clock(),
        generation_id=generation,
        target=target,
        review_id=review.review_id,
    )
    preparation = PreparedPromotionApproval.model_validate(submit(prepare))
    confirm = ApprovePromotion(
        command_id=str(
            uuid5(fixture.record.request_id, "synthetic-confirm:" + str(state.revision))
        ),
        requested_at=fixture.clock(),
        generation_id=generation,
        target=target,
        preparation=preparation,
        entered_name=target.name,
    )
    approval = StrategyPromotionApproval.model_validate(submit(confirm))
    if approval.after.revision != state.revision + 1:
        raise ValueError("original manual approval did not change only the next stage")
    return approval


def submit_original_experiment_command(
    fixture: StrategyPromotionIntegrationFixture, command: ExperimentCommand
) -> PageControlReceipt:
    """In-process equivalent of the installed original private experiment ingress."""
    from rquant.collaboration_commands import CommandAuthorization
    from rquant.web.models.collaboration import CollaborationPrivateRequest

    proof = fixture.control.collaboration_request(
        CollaborationPrivateRequest(
            schema_version=1,
            operation="authorize_command",
            authenticated_actor_id=command.actor_id,
            original_command=command.model_dump(mode="json"),
        )
    )
    if type(proof) is not CommandAuthorization:
        raise TypeError("original private command authorization required")
    return fixture.control.submit_authorized(command, proof)


def seal_original_complete_promotion(
    fixture: StrategyPromotionIntegrationFixture,
) -> tuple[StrategyPromotionApproval, ...]:
    """Root-only workers: full parent, six folds, unique outer, forward/band and human stages."""
    from uuid import uuid5

    from rquant.experiment_platform import stable_experiment_job
    from rquant.experiment_platform_commands import (
        ExperimentCommandResult,
        UnsealExperimentOuterTest,
    )
    from rquant.strategy_promotion_commands import RunStrategyWalkForward
    from rquant.strategy_promotion_contracts import PromotionEvidenceSelection
    from rquant.strategy_promotion_walk_forward import StrategyPromotionWalkForwardSubmission

    parent_jobs = tuple(
        stable_experiment_job(fixture.record.owner, fixture.record.request_id, index)
        for index in range(len(fixture.record.actual_configurations))
    )
    seal_original_promotion_jobs(fixture, parent_jobs)
    parent = fixture.record.request.template
    if parent is None:
        raise ValueError("original authored parent template is required")
    choices = fixture.backend.context(
        actor_id=fixture.record.owner,
        source_kind="template",
        strategy_id=parent.strategy_id,
        head=parent.head,
    ).candidates
    chosen = choices[0]
    if not chosen.has_sealed_reference or len(choices) != len(fixture.record.actual_configurations):
        raise ValueError("complete original parent family must be actually sealed")
    comparable = approve_original_next_stage(
        fixture, target=chosen.target, selection=chosen.selection
    )
    wf_id = uuid5(fixture.record.request_id, "synthetic-fixed-six-folds")
    wf = RunStrategyWalkForward(
        command_id=str(wf_id),
        requested_at=fixture.clock(),
        generation_id=fixture.publish().generation_id,
        target=chosen.target,
        selection=chosen.selection,
        fold_count=6,
    )
    wf_receipt = fixture.control._submit_trusted_strategy_promotion(
        wf,
        authenticated_actor_id=chosen.target.owner_id,
        verified_metadata_identity=fixture.binding.expected_private_identity,
    )
    if wf_receipt.status.value != "succeeded":
        raise ValueError("original six-fold submission did not complete")
    submission = StrategyPromotionWalkForwardSubmission.model_validate(wf_receipt.result)
    if len(submission.receipts) != 6:
        raise ValueError("original immutable plan did not submit all six children")
    seal_original_promotion_jobs(fixture, tuple(receipt.job_id for receipt in submission.receipts))
    fixture.projection.authority.refresh_live_identity()
    outer = UnsealExperimentOuterTest(
        command_id=str(uuid5(fixture.record.request_id, "synthetic-unique-outer")),
        requested_at=fixture.clock(),
        actor_id=chosen.target.owner_id,
        family_id=chosen.selection.family_id,
        experiment_id=chosen.selection.experiment_id,
        result_hash=chosen.result_hash,
        confirmed=True,
    )
    outer_receipt = submit_original_experiment_command(fixture, outer)
    if outer_receipt.status.value != "succeeded":
        raise ValueError("original outer admission did not complete")
    outer_result = ExperimentCommandResult.model_validate(outer_receipt.result)
    if outer_result.status != "outer_admitted" or len(outer_result.job_ids) != 1:
        raise ValueError("one original fixed outer job is required")
    seal_original_promotion_jobs(fixture, outer_result.job_ids)
    selection = PromotionEvidenceSelection.model_validate(
        chosen.selection.model_dump(mode="python") | {"walk_forward_id": wf_id}
    )
    fixture.foundation.scheduler.release()
    fixture.time[0] += timedelta(minutes=10)
    paper = approve_original_next_stage(fixture, target=chosen.target, selection=selection)
    source = install_original_empty_paper(fixture, chosen.target)
    install_original_paper_research(fixture, source)
    record_original_twenty_paper_closes(fixture, source, approved_at=paper.applied_at)
    neutral = submit_original_neutral_backtest(fixture, chosen.target)
    seal_original_promotion_jobs(fixture, (neutral.job_id,))
    band_job = submit_original_paper_band(fixture, source, backtest_job=neutral.job_id)
    seal_original_promotion_jobs(fixture, (band_job,))
    complete = PromotionEvidenceSelection.model_validate(
        selection.model_dump(mode="python")
        | {
            "paper_account_id": source.runtime.state.configuration.binding.account_id,
            "band_job_id": band_job,
        }
    )
    monitor = approve_original_next_stage(fixture, target=chosen.target, selection=complete)
    fixture.publish()
    return comparable, paper, monitor


def seal_original_promotion_jobs(
    fixture: StrategyPromotionIntegrationFixture, jobs: tuple[UUID, ...]
) -> tuple[LabFinalizerResult, ...]:
    """Root-only IPC gate using the installed original directory and private binding."""
    import json
    import os
    import shutil
    import tempfile
    import threading
    from uuid import uuid4

    from rquant.lab_finalizer import LabFinalizer
    from rquant.lab_worker import LabWorker, build_builtin_shard_runtime_manifest

    if not jobs or len(jobs) > 20 or len(set(jobs)) != len(jobs):
        raise ValueError("finite exact original job IDs required")
    foundation = fixture.foundation
    # Original SDK receipts precede the scheduler's durable command admission.
    foundation.scheduler.run_once()
    if any(foundation.reader.get_job(job_id) is None for job_id in jobs):
        raise ValueError("original Lab job must already be admitted")
    ipc_root = Path(tempfile.mkdtemp(prefix="rqc5-", dir="/private/tmp"))
    ipc_root.chmod(0o700)
    previous_tempdir, previous_env = tempfile.tempdir, os.environ.get("TMPDIR")
    tempfile.tempdir, os.environ["TMPDIR"] = str(ipc_root), str(ipc_root)
    worker, thread = None, None
    try:
        runtime = foundation.commands.experiment_template_binding
        worker = LabWorker(
            worker_id="ai-synthetic-worker",
            claim_spool=foundation.original_worker_claim_spool(),
            report_spool=foundation.reports,
            artifact_root=foundation.shard_root,
            template_directory=fixture.binding.directory,
            experiment_template_binding=runtime,
            paper_directory=getattr(foundation.commands, "paper_directory", None),
            verified_code_sha_provider=lambda: foundation.code_commit,
            shard_runtime_manifest=build_builtin_shard_runtime_manifest(
                catalog_path=foundation.metadata_path,
                forbidden_paths=(),
                snapshot_root=foundation.shard_root.parent / "worker-copies",
                research_lake_root=foundation.lake_root,
            ),
            heartbeat_interval_seconds=1,
            receipt_timeout_seconds=10,
            clock=fixture.clock,
        )
        finalizer = LabFinalizer(
            reader=foundation.reader,
            shard_artifact_root=foundation.shard_root,
            artifact_store=foundation.artifacts,
            commit_spool=foundation.commits,
            template_directory=fixture.binding.directory,
            experiment_template_binding=runtime,
            paper_directory=getattr(foundation.commands, "paper_directory", None),
            verified_code_sha_provider=lambda: foundation.code_commit,
            finalizer_authority_key_provider=lambda: foundation.key,
        )
        sealed: dict[UUID, LabFinalizerResult] = {}
        while len(sealed) < len(jobs):
            foundation.scheduler.run_once()
            for job_id in jobs:
                if (
                    job_id not in sealed
                    and foundation.reader.get_finalization_snapshot(job_id) is not None
                ):
                    result = finalizer.finalize(job_id)
                    if result.status != "published":
                        raise RuntimeError("original finalizer did not publish")
                    foundation.scheduler.run_once()
                    sealed[job_id] = result
            if len(sealed) == len(jobs):
                break
            outcomes: list[LabWorkerTickResult] = []
            errors: list[BaseException] = []

            def execute(outcomes: list[LabWorkerTickResult], errors: list[BaseException]) -> None:
                try:
                    outcomes.append(worker.run_once())
                except BaseException as error:
                    errors.append(error)

            thread = threading.Thread(
                target=execute, args=(outcomes, errors), name="c5-original-lab-worker"
            )
            thread.start()
            while thread.is_alive():
                foundation.scheduler.run_once()
                thread.join(0.02)
            if errors:
                raise errors[0]
            if len(outcomes) != 1 or outcomes[0].status != "succeeded":
                raise RuntimeError("original promotion worker did not succeed")
        return tuple(sealed[job_id] for job_id in jobs)
    finally:
        cleanup_errors = []
        try:
            if worker is not None:
                worker.request_stop()
            if thread is not None:
                thread.join(10)
            if worker is not None:
                worker.close()
        except BaseException as error:
            cleanup_errors.append(type(error).__name__)
        alive = thread is not None and thread.is_alive()
        remaining = 0
        if worker is not None:
            with worker._managed_authority_children_lock:
                remaining = len(worker._managed_authority_children)
        tempfile.tempdir = previous_tempdir
        if previous_env is None:
            os.environ.pop("TMPDIR", None)
        else:
            os.environ["TMPDIR"] = previous_env
        if not alive and not remaining and not cleanup_errors:
            shutil.rmtree(ipc_root)
        record = {
            "jobs": tuple(str(job_id) for job_id in jobs),
            "original_worker_thread_alive": alive,
            "owned_authority_children_remaining": remaining,
            "cleanup_errors": cleanup_errors,
            "owned_ipc_root": str(ipc_root),
            "mode": "0700",
            "owned_ipc_root_removed": not ipc_root.exists(),
            "tempdir_restored": tempfile.tempdir == previous_tempdir,
            "environment_restored": os.environ.get("TMPDIR") == previous_env,
        }
        with (fixture.root / ("worker-cleanup-" + str(uuid4()) + ".json")).open("x") as handle:
            json.dump(record, handle, indent=2)
        if alive or remaining or cleanup_errors:
            raise RuntimeError("original worker cleanup incomplete; owned IPC root preserved")
