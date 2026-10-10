"""Synthetic original C6 sources; no sealed or forward evidence is fabricated."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.experiment_platform import ExperimentPlatformStore
from rquant.experiment_registry import DateRange, ExperimentRegistry
from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandSpool
from rquant.lab_jobs import LabJobReader, LabJobStore
from rquant.minute_backtest_publication_contracts import MinuteSourceContentSeed
from rquant.research_catalog import ResearchCatalog
from rquant.runtime_contracts import canonical_sha256
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_authoring_commands import StrategyTemplateHead
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from rquant.strategy_promotion_contracts import StrategyPromotionTarget
from tests.unit.test_minute_backtest_producer import NOW, source_seed


@dataclass
class NativePromotionFixture:
    root: Path
    seed: MinuteSourceContentSeed
    definitions: ImmutableDefinitionRegistry
    platform: ExperimentPlatformStore
    jobs: LabJobStore
    commands: LabCommandSubmissionFacade
    now: datetime
    source_reads: list[Any]
    seeds: tuple[MinuteSourceContentSeed, ...] = ()
    protocol: Any = None
    phase_source_builder: Callable[[Any, MinuteSourceContentSeed], MinuteSourceContentSeed] | None = None
    target_names: tuple[tuple[str, str], ...] = ()

    def clock(self) -> datetime:
        return self.now

    def seed_for(self, native_id: str | None = None) -> MinuteSourceContentSeed:
        if native_id is None:
            return self.seed
        selected = tuple(s for s in (self.seeds or (self.seed,)) if s.native_registration.logical_id == native_id)
        if len(selected) != 1:
            raise ValueError("exact original native source is unavailable")
        return selected[0]

    def selection(self, *, native_id: str | None = None) -> Any:
        from rquant import strategy_promotion_contracts as contracts

        seed = self.seed_for(native_id)
        native = seed.native_registration
        return contracts.NativeMinuteSelection(
            target=StrategyPromotionTarget(
                source_kind="builtin", owner_id=seed.runtime.owner_id,
                strategy_id=native.logical_id, name=dict(self.target_names).get(native.logical_id, native.logical_id),
                head=StrategyTemplateHead(version=native.version,
                    registration_fingerprint=native.fingerprint, record_hash=native.record_hash,
                    spec_fingerprint=native.spec.spec_fingerprint),
                parameter_fingerprint=native.spec.parameter_fingerprint,
                cost_fingerprint=canonical_sha256(seed.runtime.execution_profile.execution_costs),
            ),
            source_key=seed.runtime.source_key, source_version=seed.runtime.source_version,
            profile_hash=seed.runtime.execution_profile.profile_hash,
        )

    def configuration(self, *, native_id: str | None = None) -> Any:
        from rquant import strategy_promotion_contracts as contracts

        seed = self.seed_for(native_id)
        return contracts.NativeMinuteConfiguration(selection=self.selection(native_id=native_id),
            start_date=seed.runtime.start_date if self.protocol is None else self.protocol.train_range.start_date,
            end_date=seed.runtime.end_date if self.protocol is None else self.protocol.validation_range.end_date)

    def request(self, *, native_id: str | None = None) -> Any:
        from rquant import experiment_platform as platform
        from rquant.minute_backtest_formal import MinuteExperimentProtocol

        if self.protocol is not None:
            seeds = self.seeds if native_id is None else (self.seed_for(native_id),)
            return platform.NativeMinuteExperimentRequest(name="原生策略完整验证",
                configurations=tuple(self.configuration(native_id=s.native_registration.logical_id) for s in seeds),
                protocol=self.protocol)
        value = self.seed.runtime
        return platform.NativeMinuteExperimentRequest(name="内置策略研究", configurations=(self.configuration(),),
            protocol=MinuteExperimentProtocol(
                train_range=DateRange(start_date=value.start_date, end_date=value.start_date),
                validation_range=DateRange(start_date=value.end_date, end_date=value.end_date),
                frozen_outer_test_range=DateRange(start_date=value.end_date + timedelta(days=1),
                    end_date=value.end_date + timedelta(days=1))))

    def phase_provider(self, read: Any) -> MinuteSourceContentSeed:
        self.source_reads.append(read)
        if self.phase_source_builder is not None:
            return self.phase_source_builder(read, self.seed_for(read.configuration.selection.target.strategy_id))
        value = self.seed.model_dump(mode="python")
        value["runtime"]["source_key"] = read.publication_source_key
        if (read.window.start_date, read.window.end_date) != (
            self.seed.runtime.start_date, self.seed.runtime.end_date
        ):
            raise ValueError("two-day original fixture cannot prove another interval")
        return MinuteSourceContentSeed.model_validate(value)

    def preparer(self) -> Any:
        from rquant import experiment_platform as platform
        from rquant.experiment_platform_commands import ExperimentFamilyPreparer

        profiles = tuple(platform.NativeMinuteSourceProfile(
            selection=self.selection(native_id=s.native_registration.logical_id), execution_profile=s.runtime.execution_profile,
            producer_commit=s.runtime.producer_commit, calendar=s.runtime.market_calendar,
            coverage=DateRange(start_date=s.runtime.start_date if self.protocol is None else self.protocol.train_range.start_date,
                end_date=s.runtime.market_calendar.coverage_end if self.protocol is None else self.protocol.frozen_outer_test_range.end_date),
            latest_complete=s.runtime.market_calendar.open_dates[-1] if self.protocol is None else self.protocol.frozen_outer_test_range.end_date,
            phase_slice_available=True) for s in (self.seeds or (self.seed,)))
        return ExperimentFamilyPreparer(store=self.platform, definitions=self.definitions,
            profiles=(), phase_provider=lambda read: (_ for _ in ()).throw(AssertionError("daily path")),
            native_profiles=profiles, native_phase_provider=self.phase_provider,
            native_visibility_policies=(self.seed.provenance.visibility_policy,),
            metadata_store_factory=lambda: DuckDBStore(self.root / "metadata.duckdb"),
            catalog=ResearchCatalog(self.root / "catalog.sqlite"), lake_root=self.root / "lake",
            input_root=self.root / "inputs", clock=self.clock)


def build_native_promotion_fixture(root: Path, monkeypatch: Any, *, native_id: str = "n_shape") -> NativePromotionFixture:
    import rquant.storage.duckdb as storage

    monkeypatch.setattr(storage, "_settings", lambda: SimpleNamespace(primary_writer_gate_path=None))
    for name in ("inputs", "lake", "trust"):
        (root / name).mkdir(mode=0o700)
    seed = source_seed(root)
    definitions = ImmutableDefinitionRegistry(root / "definitions",
        execution_registry=BuiltinStrategyEvaluatorRegistry(
            producer_commit=seed.runtime.producer_commit).trusted_executable_registry())
    if native_id != seed.native_registration.logical_id:
        from rquant.runtime_definition_bootstrap import plan_builtin_definitions
        native = definitions.latest_strategy_spec(native_id, as_of=NOW)
        if native is None:
            raise ValueError("original builtin definition is unavailable")
        binding = next(item for item in plan_builtin_definitions(producer_commit=seed.runtime.producer_commit).strategies
            if item.strategy_id == native_id)
        seed = MinuteSourceContentSeed.model_validate({**seed.model_dump(mode="python"),
            "native_registration": native, "runtime": {**seed.runtime.model_dump(mode="python"), "strategy": binding}})
    registry = ExperimentRegistry(root / "trust/experiments.sqlite",
        managed_trust_root=root / "trust")
    platform = ExperimentPlatformStore(registry, activate_private_schema=True)
    platform.install_policy(months=0, now=NOW - timedelta(seconds=5))
    jobs = LabJobStore(root / "jobs.sqlite")
    jobs.initialize()
    fixture = NativePromotionFixture(root, seed, definitions, platform, jobs,
        commands=None, now=NOW - timedelta(seconds=5), source_reads=[])
    fixture.commands = LabCommandSubmissionFacade(reader=LabJobReader(jobs.path),
        spool=LabCommandSpool(root / "commands"), experiment_registry=registry,
        definition_registry=definitions, clock=fixture.clock)
    return fixture


def build_native_complete_promotion_fixture(root: Path, monkeypatch: Any) -> NativePromotionFixture:
    """One frozen research profile, complete original N=3, and modeled source facts."""
    import rquant.storage.duckdb as storage
    from rquant.minute_backtest_formal import MinuteExperimentProtocol
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.strategy_catalog_source import _NAMES
    from tests.support.native_minute_phase_sources import build_native_phase_base_seed, build_native_phase_seed

    monkeypatch.setattr(storage, "_settings", lambda: SimpleNamespace(primary_writer_gate_path=None))
    for name in ("inputs", "lake", "trust", "phase-sources"):
        (root / name).mkdir(mode=0o700)
    published_at = datetime(2026, 10, 7, tzinfo=UTC)
    start, end = date(2025, 11, 3), date(2026, 11, 30)
    days = tuple(start + timedelta(days=i) for i in range((end - start).days + 1))
    calendar = MarketCalendarAuthority.create(schema_version=1, exchange="SSE", producer_commit="a" * 40,
        coverage_start=start, coverage_end=end, open_dates=tuple(d for d in days if d.weekday() < 5),
        generated_at=datetime(2025, 11, 1, tzinfo=UTC))
    native_ids = ("n_shape", "auction_gap", "growth_board_surge")
    seeds = tuple(build_native_phase_base_seed(root / ("base-" + native_id), native_id=native_id,
        calendar=calendar, window=DateRange(start_date=date(2026, 1, 5), end_date=date(2026, 1, 9)),
        published_at=published_at, source_key="synthetic.native-minute:" + native_id,
        account_id="native-shared-research") for native_id in native_ids)
    seed = seeds[0]
    definitions = ImmutableDefinitionRegistry(root / "base-n_shape/definitions",
        execution_registry=BuiltinStrategyEvaluatorRegistry(producer_commit=seed.runtime.producer_commit).trusted_executable_registry())
    for value in seeds:
        if definitions.latest_strategy_spec(value.native_registration.logical_id, as_of=published_at) != value.native_registration:
            raise ValueError("native sources did not freeze the same actual definition registry")
    registry = ExperimentRegistry(root / "trust/experiments.sqlite", managed_trust_root=root / "trust")
    platform = ExperimentPlatformStore(registry, activate_private_schema=True)
    platform.install_policy(months=0, now=published_at)
    jobs = LabJobStore(root / "jobs.sqlite")
    jobs.initialize()
    fixture = NativePromotionFixture(root, seed, definitions, platform, jobs, commands=None,
        now=published_at + timedelta(seconds=1), source_reads=[], seeds=seeds,
        protocol=MinuteExperimentProtocol(train_range=DateRange(start_date=date(2026, 1, 5), end_date=date(2026, 2, 27)),
            validation_range=DateRange(start_date=date(2026, 3, 2), end_date=date(2026, 4, 30)),
            frozen_outer_test_range=DateRange(start_date=date(2026, 5, 4), end_date=date(2026, 5, 29))),
        target_names=tuple((native_id, _NAMES[native_id]) for native_id in native_ids))
    fixture.phase_source_builder = lambda read, base: build_native_phase_seed(
        root / "phase-sources" / str(len(fixture.source_reads)), read=read, base_seed=base,
        calendar=calendar, published_at=fixture.now)
    fixture.commands = LabCommandSubmissionFacade(reader=LabJobReader(jobs.path), spool=LabCommandSpool(root / "commands"),
        experiment_registry=registry, definition_registry=definitions, clock=fixture.clock)
    return fixture


def restore_native_complete_promotion_fixture(root: Path, monkeypatch: Any) -> NativePromotionFixture:
    """Reopen retained original preparations; do not bootstrap or publish again."""
    import json
    import rquant.storage.duckdb as storage
    from rquant.experiment_platform_commands import RegisterExperimentFamily
    from rquant.strategy_catalog_source import _NAMES

    monkeypatch.setattr(storage, "_settings", lambda: SimpleNamespace(primary_writer_gate_path=None))
    facts = json.loads((root / "complete-parent-preparation-facts.json").read_text())
    command = RegisterExperimentFamily.model_validate(facts["command"])
    native_ids = tuple(c.selection.target.strategy_id for c in command.request.configurations)
    seeds = tuple(MinuteSourceContentSeed.model_validate_json(
        (root / ("base-" + native_id) / "source-seed.json").read_bytes()) for native_id in native_ids)
    registry = ExperimentRegistry(root / "trust/experiments.sqlite", managed_trust_root=root / "trust")
    platform = ExperimentPlatformStore(registry, activate_private_schema=False)
    record = platform.get_request(command.actor_id, __import__("uuid").UUID(command.command_id))
    if record is None or record.model_dump(mode="json") != facts["family"] or record.body_hash != canonical_sha256(command):
        raise ValueError("retained native family differs from its exact original command")
    receipts = tuple(platform.preparation(record.owner, record.family_id, i) for i in range(len(seeds)))
    if any(value is None for value in receipts):
        raise ValueError("retained native family lost a complete original preparation")
    now = max(value.prepared.formal_plan.preregistered_at for value in receipts)
    definitions = ImmutableDefinitionRegistry(root / ("base-" + native_ids[0]) / "definitions",
        execution_registry=BuiltinStrategyEvaluatorRegistry(producer_commit=seeds[0].runtime.producer_commit).trusted_executable_registry())
    for seed, cfg in zip(seeds, record.actual_configurations, strict=True):
        if (definitions.latest_strategy_spec(seed.native_registration.logical_id, as_of=now) != seed.native_registration
            or cfg.selection.profile_hash != seed.runtime.execution_profile.profile_hash):
            raise ValueError("retained native registry or complete profile differs")
    jobs = LabJobStore(root / "jobs.sqlite")
    fixture = NativePromotionFixture(root, seeds[0], definitions, platform, jobs, commands=None,
        now=now, source_reads=[], seeds=seeds, protocol=command.request.protocol,
        target_names=tuple((native_id, _NAMES[native_id]) for native_id in native_ids))
    from tests.support.native_minute_phase_sources import build_native_phase_seed
    fixture.phase_source_builder = lambda read, base: build_native_phase_seed(
        root / "phase-sources" / ("restored-" + str(len(fixture.source_reads))), read=read,
        base_seed=base, calendar=base.runtime.market_calendar, published_at=fixture.now)
    fixture.commands = LabCommandSubmissionFacade(reader=LabJobReader(jobs.path), spool=LabCommandSpool(root / "commands"),
        experiment_registry=registry, definition_registry=definitions, clock=fixture.clock)
    return fixture


def build_native_independent_replay_fixture(
    root: Path, *, original_root: Path, monkeypatch: Any,
) -> NativePromotionFixture:
    """A new normal installation reuses source bytes; it does not recover failed jobs."""
    import json
    import shutil
    from uuid import UUID
    from rquant.experiment_platform_commands import RegisterExperimentFamily
    from tests.support.native_minute_phase_sources import build_native_phase_seed

    if not root.is_absolute() or root.exists() or root.is_symlink():
        raise ValueError("independent replay requires a new absolute private directory")
    original = restore_native_complete_promotion_fixture(original_root, monkeypatch)
    facts = json.loads((original_root / "complete-parent-preparation-facts.json").read_text())
    command = RegisterExperimentFamily.model_validate(facts["command"])
    record = original.platform.get_request(command.actor_id, UUID(command.command_id))
    receipts = tuple(original.platform.preparation(record.owner, record.family_id, index)
        for index in range(len(record.actual_configurations)))
    complete = {receipt.configuration.selection.target.strategy_id: receipt.prepared.published.receipt.seed
        for receipt in receipts}
    root.mkdir(mode=0o700)
    for name in ("inputs", "lake", "trust", "phase-sources"):
        (root / name).mkdir(mode=0o700)
    for seed in original.seeds:
        native_id = seed.native_registration.logical_id
        directory = root / ("base-" + native_id)
        directory.mkdir(mode=0o700)
        (directory / "source-seed.json").write_bytes((original_root / ("base-" + native_id) / "source-seed.json").read_bytes())
    shutil.copytree(original.definitions.root, root / "base-n_shape/definitions")
    definitions = ImmutableDefinitionRegistry(root / "base-n_shape/definitions",
        execution_registry=BuiltinStrategyEvaluatorRegistry(producer_commit=original.seed.runtime.producer_commit).trusted_executable_registry())
    registry = ExperimentRegistry(root / "trust/experiments.sqlite", managed_trust_root=root / "trust")
    platform = ExperimentPlatformStore(registry, activate_private_schema=True)
    platform.install_policy(months=record.policy.months, now=record.policy.updated_at)
    jobs = LabJobStore(root / "jobs.sqlite")
    jobs.initialize()
    fixture = NativePromotionFixture(root, original.seed, definitions, platform, jobs, commands=None,
        now=command.requested_at, source_reads=[], seeds=original.seeds,
        protocol=original.protocol, target_names=original.target_names)

    def replay_source(read: Any, base: MinuteSourceContentSeed) -> MinuteSourceContentSeed:
        frozen = complete.get(read.configuration.selection.target.strategy_id)
        if read.phase == "search" and (read.window.start_date, read.window.end_date) == (
            frozen.runtime.start_date, frozen.runtime.end_date
        ):
            value = frozen.model_dump(mode="python")
            value["runtime"]["source_key"] = read.publication_source_key
            replayed = MinuteSourceContentSeed.model_validate(value)
            if replayed.origin_materials != frozen.origin_materials or replayed.derivations != frozen.derivations:
                raise ValueError("independent replay changed complete original raw material")
            return replayed
        return build_native_phase_seed(root / "phase-sources" / str(len(fixture.source_reads)),
            read=read, base_seed=base, calendar=base.runtime.market_calendar, published_at=fixture.now)

    fixture.phase_source_builder = replay_source
    fixture.commands = LabCommandSubmissionFacade(reader=LabJobReader(jobs.path), spool=LabCommandSpool(root / "commands"),
        experiment_registry=registry, definition_registry=definitions, clock=fixture.clock)
    return fixture


def install_native_owned_research(fixture: NativePromotionFixture, *, preparations: tuple[Any, ...]) -> Any:
    """Install the actual native readers, owner and WF admission; no outcome is substituted."""
    from rquant.collaboration_commands import PageControlRoleAuthority
    from rquant.collaboration_roles import RoleEntry, RoleState
    from rquant.experiment_platform_commands import ExperimentCommandWriter
    from rquant.experiment_platform_projection import ExperimentPrivateProjectionReader
    from rquant.experiment_registry import ExperimentRegistryReadonlyReader
    from rquant.lab_artifact_preview import ArtifactPreviewReader
    from rquant.minute_backtest_artifact import MinuteSealedReplayReader
    from rquant.minute_backtest_formal import PreparedMinuteRequest
    from rquant.minute_backtest_producer import MinuteReplayCatalog
    from rquant.strategy_authoring import StrategyAuthoringStore
    from rquant.strategy_promotion import StrategyPromotionBackend, StrategyPromotionPageControlBackend
    from rquant.strategy_promotion_evidence import StrategyPromotionEvidenceSource
    from rquant.strategy_promotion_walk_forward import (
        NativeStrategyPromotionWalkForwardBinding, StrategyPromotionWalkForwardBackend,
    )

    references, policies = [], []
    for receipt in preparations:
        if (receipt is None or not isinstance(receipt.prepared, PreparedMinuteRequest)
            or fixture.platform.preparation(receipt.owner, receipt.family_id, receipt.index) != receipt):
            raise ValueError("native complete reader requires original stored preparation receipts")
        for reference in receipt.prepared.catalog.entries:
            if reference not in references:
                references.append(reference)
        for policy in receipt.prepared.catalog.installed_policies:
            if policy not in policies:
                policies.append(policy)
    catalog = MinuteReplayCatalog(entries=tuple(references), installed_policies=tuple(policies))
    owner_id = fixture.seed.runtime.owner_id
    private = fixture.root / "original-promotion-owner"
    private.mkdir(mode=0o700, exist_ok=True)
    role_path = private / "roles.json"
    if not role_path.exists():
        role_path.write_text(RoleState.create(revision=1, users=(
            RoleEntry(username=owner_id, role="admin"), RoleEntry(username="synthetic-admin", role="admin"),
        )).model_dump_json())
        role_path.chmod(0o600)
    roles = PageControlRoleAuthority(mode="enforced", roles_path=role_path, clock=fixture.clock)
    store = StrategyAuthoringStore(private / "metadata.sqlite", definition_root=fixture.definitions.root,
        producer_commit=fixture.seed.runtime.producer_commit, clock=fixture.clock)
    if not store.path.exists():
        store.initialize()
    else:
        store.identity()
    readonly = ExperimentRegistryReadonlyReader(fixture.platform.registry.path,
        managed_trust_root=fixture.root / "trust")
    projection = ExperimentPrivateProjectionReader(registry=readonly, jobs=fixture.commands.reader,
        owners=frozenset({owner_id}))
    native_reader = MinuteSealedReplayReader(reader=fixture.commands.reader,
        artifact_reader=ArtifactPreviewReader(reader=fixture.commands.reader, artifact_root=fixture.root / "artifacts"),
        submission_facade=fixture.commands, catalog=catalog)
    writer = ExperimentCommandWriter(store=fixture.platform, commands=fixture.commands,
        prepare=fixture.preparer(), enabled=True, owners=frozenset({owner_id}),
        native_results=native_reader, private_authority=projection.authority)
    wf = StrategyPromotionWalkForwardBackend(registry=fixture.platform.registry,
        native=NativeStrategyPromotionWalkForwardBinding(store=store, expected_identity=store.identity(),
            writer=writer, results=native_reader))
    source = StrategyPromotionEvidenceSource(registry=fixture.platform.registry, platform=fixture.platform,
        projection=projection, results=None, template_results=None, walk_forward=wf, native_results=native_reader)
    domain = StrategyPromotionBackend(store, roles=roles, source=source, enabled=True,
        builtin_definitions=tuple((fixture.selection(native_id=s.native_registration.logical_id).target, fixture.definitions)
            for s in (fixture.seeds or (fixture.seed,))))
    return SimpleNamespace(domain=domain, page=StrategyPromotionPageControlBackend(domain,
        operator_users=frozenset({owner_id})), writer=writer, roles=roles, roles_path=role_path,
        store=store, native_reader=native_reader, catalog=catalog, source=source,
        projection=projection, walk_forward=wf)


@dataclass
class NativeOriginalWorkerFoundation:
    fixture: NativePromotionFixture
    commands: LabCommandSubmissionFacade
    reader: LabJobReader
    scheduler: Any
    claims: Any
    reports: Any
    commits: Any
    artifacts: Any
    key: Any
    catalog: Any
    adapter_registry: Any
    runtime_manifest: Any
    shard_root: Path

    def original_worker_claim_spool(self) -> Any:
        from rquant.lab_shard_protocol import LabClaimSpool

        barrier = self.scheduler.scheduling_control
        if barrier is None:
            raise RuntimeError("native worker requires its installed original scheduling barrier")
        return LabClaimSpool(self.claims.root, expected_scheduling_barrier_identity=barrier.identity)

    def close(self) -> None:
        try:
            self.scheduler.release()
        finally:
            self.artifacts.close()


def build_native_original_worker_foundation(
    fixture: NativePromotionFixture, *, owner: Any,
) -> NativeOriginalWorkerFoundation:
    """Original queue/registry/lease/commit owner; this function does not execute a worker."""
    import secrets

    from rquant.lab_artifact_protocol import LabArtifactCommitSpool, LabFinalizerAuthorityKey
    from rquant.lab_artifacts import LabJobArtifactStore
    from rquant.lab_job_center import ExperimentLifecycleCoordinator
    from rquant.lab_scheduler import LabScheduler
    from rquant.lab_scheduling_control import LabSchedulingBarrierPort, LabSchedulingMaintenanceScope
    from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool
    from rquant.lab_worker import LabClosedRegistryBinding, LabShardRuntimeManifest, build_builtin_shard_runtime_manifest
    from rquant.lab_worker_registry import builtin_lab_shard_configuration, resolve_builtin_adapter_registry
    from rquant.strict_json import canonical_json_bytes

    if owner.native_reader.submission_facade is not fixture.commands or owner.native_reader.catalog != owner.catalog:
        raise ValueError("native worker and complete reader must use the same original submission owner")
    root = fixture.root / "original-minute-worker"
    root.mkdir(mode=0o700, exist_ok=True)
    claims, reports = LabClaimSpool(root / "claims"), LabReportSpool(root / "reports")
    commits = LabArtifactCommitSpool(root / "commits")
    artifacts = LabJobArtifactStore(fixture.root / "artifacts")
    key = LabFinalizerAuthorityKey(key_id="native-synthetic-only", secret=secrets.token_bytes(32))
    try:
        config = builtin_lab_shard_configuration(catalog_path=fixture.root / "metadata.duckdb",
            forbidden_paths=(), snapshot_root=root / "metadata-copies", research_lake_root=fixture.root / "lake",
            minute_catalog=owner.catalog, minute_registry_mode="installed")
        registry = resolve_builtin_adapter_registry(config)
        registered = build_builtin_shard_runtime_manifest(catalog_path=fixture.root / "metadata.duckdb",
            forbidden_paths=(), snapshot_root=root / "metadata-copies", research_lake_root=fixture.root / "lake")
        manifest = LabShardRuntimeManifest(registry=LabClosedRegistryBinding(
            registry_id=registered.registry.registry_id, registry_version=registered.registry.registry_version,
            registry_hash=registered.registry.registry_hash,
            configuration_json=canonical_json_bytes(config.model_dump(mode="json", round_trip=True)).decode()))
        barrier = LabSchedulingBarrierPort(claims.root, store=fixture.jobs,
            maintenance_scope=LabSchedulingMaintenanceScope(report_root=reports.root,
                artifact_commit_root=commits.root, final_artifact_root=artifacts.root))
        scheduler = LabScheduler(store=fixture.jobs, spool=fixture.commands.spool,
            owner_id="native-synthetic-scheduler", lease_seconds=60, heartbeat_seconds=10,
            poll_interval_ms=5, report_spool=reports, claim_spool=claims,
            claim_worker_ids=("native-synthetic-worker",), shard_lease_seconds=120,
            artifact_commit_spool=commits, artifact_store=artifacts, adapter_registry=registry,
            finalizer_authority_key_provider=lambda key_id: key if key_id == key.key_id else None,
            lifecycle_synchronizer=ExperimentLifecycleCoordinator(fixture.commands),
            scheduling_control=barrier, clock=fixture.clock)
        return NativeOriginalWorkerFoundation(fixture=fixture, commands=fixture.commands,
            reader=fixture.commands.reader, scheduler=scheduler, claims=claims, reports=reports,
            commits=commits, artifacts=artifacts, key=key, catalog=owner.catalog,
            adapter_registry=registry, runtime_manifest=manifest, shard_root=root / "shard-artifacts")
    except BaseException:
        artifacts.close()
        raise


def seal_native_original_jobs(
    foundation: NativeOriginalWorkerFoundation, jobs: tuple[Any, ...],
) -> tuple[Any, ...]:
    """Root-only physical worker/finalizer/ACK gate using the original installed native registry."""
    import json
    import os
    import shutil
    import tempfile
    import threading
    from uuid import UUID, uuid4

    from rquant.lab_finalizer import LabFinalizer
    from rquant.lab_worker import LabWorker

    if not jobs or len(jobs) > 20 or len(set(jobs)) != len(jobs) or any(type(job_id) is not UUID for job_id in jobs):
        raise ValueError("finite exact original native job UUIDs required")
    ipc_root = Path(tempfile.mkdtemp(prefix="rqcn-", dir="/private/tmp"))
    ipc_root.chmod(0o700)
    previous_tempdir, previous_env = tempfile.tempdir, os.environ.get("TMPDIR")
    tempfile.tempdir, os.environ["TMPDIR"] = str(ipc_root), str(ipc_root)
    worker, thread = None, None
    outcomes, errors = [], []
    try:
        foundation.scheduler.run_once()
        if any(foundation.reader.get_job(job_id) is None for job_id in jobs):
            raise ValueError("original native jobs must already be admitted by the original scheduler")
        worker = LabWorker(worker_id="native-synthetic-worker",
            claim_spool=foundation.original_worker_claim_spool(), report_spool=foundation.reports,
            artifact_root=foundation.shard_root, shard_runtime_manifest=foundation.runtime_manifest,
            verified_code_sha_provider=lambda: foundation.fixture.seed.runtime.producer_commit,
            heartbeat_interval_seconds=1, receipt_timeout_seconds=10, clock=foundation.fixture.clock)
        if worker.adapter_registry.closed_descriptor() != foundation.adapter_registry.closed_descriptor():
            raise ValueError("original native worker adapter descriptor differs from scheduler")
        finalizer = LabFinalizer(reader=foundation.reader, shard_artifact_root=foundation.shard_root,
            artifact_store=foundation.artifacts, commit_spool=foundation.commits,
            adapter_registry=foundation.adapter_registry,
            verified_code_sha_provider=lambda: foundation.fixture.seed.runtime.producer_commit,
            finalizer_authority_key_provider=lambda: foundation.key)
        sealed = {}
        while len(sealed) < len(jobs):
            foundation.scheduler.run_once()
            for job_id in jobs:
                if job_id not in sealed and foundation.reader.get_finalization_snapshot(job_id) is not None:
                    result = finalizer.finalize(job_id)
                    if result.status != "published":
                        raise RuntimeError("original native finalizer did not publish")
                    foundation.scheduler.run_once()
                    sealed[job_id] = result
            if len(sealed) == len(jobs):
                break
            outcomes, errors = [], []

            def execute() -> None:
                try:
                    outcomes.append(worker.run_once())
                except BaseException as error:
                    errors.append(error)

            thread = threading.Thread(target=execute, name="c5-original-native-worker")
            thread.start()
            while thread.is_alive():
                foundation.scheduler.run_once()
                thread.join(0.02)
            if errors:
                raise errors[0]
            if len(outcomes) != 1 or outcomes[0].status != "succeeded":
                raise RuntimeError("original native worker did not succeed")
        return tuple(sealed[job_id] for job_id in jobs)
    except BaseException as error:
        jobs_read = tuple(foundation.reader.get_job(job_id) for job_id in jobs)
        failure = {"error_type": type(error).__name__, "message": str(error),
            "outcomes": tuple(outcome.model_dump(mode="json") for outcome in outcomes),
            "jobs": tuple(None if job is None else job.model_dump(mode="json") for job in jobs_read),
            "shards": tuple(shard.model_dump(mode="json") for job_id in jobs
                for shard in foundation.reader.list_shards(job_id)),
            "worker_exceptions": tuple({"error_type": type(item).__name__, "message": str(item)} for item in errors)}
        with (foundation.fixture.root / ("worker-failure-" + str(uuid4()) + ".json")).open("x") as handle:
            json.dump(failure, handle, ensure_ascii=False, indent=2)
        raise
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
        record = {"jobs": tuple(str(job_id) for job_id in jobs), "original_worker_thread_alive": alive,
            "owned_authority_children_remaining": remaining, "cleanup_errors": cleanup_errors,
            "owned_ipc_root": str(ipc_root), "owned_ipc_root_removed": not ipc_root.exists(),
            "tempdir_restored": tempfile.tempdir == previous_tempdir,
            "environment_restored": os.environ.get("TMPDIR") == previous_env}
        with (foundation.fixture.root / ("worker-cleanup-" + str(uuid4()) + ".json")).open("x") as handle:
            json.dump(record, handle, indent=2)
        if alive or remaining or cleanup_errors:
            raise RuntimeError("original native worker cleanup incomplete; owned IPC preserved")


def native_forward_owner_fixture(root: Path, monkeypatch: Any, *, native_id: str = "n_shape") -> tuple[Any, Any, list[datetime], Path]:
    """Real owner/roles/approval SQLite; sealed evidence transport is synthetic here."""
    from decimal import Decimal
    from uuid import uuid4
    from rquant.experiment_registry import PromotionStage
    from rquant.collaboration_roles import RoleEntry, RoleState
    from rquant.strategy_promotion import StrategyPromotionPageControlBackend
    from rquant.strategy_promotion_commands import ApprovePromotion, PreparePromotionApproval, RequestPromotionReview
    from rquant.strategy_promotion_contracts import (
        BoundOuterPromotionEvidence, BoundWalkForwardFold, NativeMinuteForwardConfiguration,
        PromotionEvidenceBundle, PromotionEvidenceSelection,
    )
    from tests.unit.test_strategy_promotion import backend_fixture, validation_bundle
    from tests.unit.test_strategy_promotion_evidence import original_family

    minute_root = root / "minute"
    minute_root.mkdir(mode=0o700)
    original = build_native_promotion_fixture(minute_root, monkeypatch, native_id=native_id)
    domain, _, clock, bundle, _, role_path = backend_fixture(root, monkeypatch)
    target = original.selection().target
    role_path.write_text(RoleState.create(revision=2, users=(
        RoleEntry(username=target.owner_id, role="admin"), RoleEntry(username="root", role="admin"),
    )).model_dump_json())
    domain.builtin_definitions = ((target, original.definitions),)
    record = original.platform.begin_request(owner=target.owner_id, request_id=uuid4(),
        body_hash="1" * 64, request=original.request(), registered_at=original.now)
    domain.source.platform = original.platform
    family_root = root / "synthetic-statistics"
    family_root.mkdir(mode=0o700)
    _, specs, receipt = original_family(family_root)
    validation = validation_bundle(target).validation.model_copy(update={
        "parent_family": record.family_id,
        "parent_manifest_hash": receipt.manifest.manifest_id, "parent_count": 2,
        "experiment_id": specs[0].experiment_id,
    })
    selected = PromotionEvidenceSelection(family_id=validation.parent_family, experiment_id=validation.experiment_id)
    bundle[0] = PromotionEvidenceBundle(target=target, validation=validation, observed_at=clock[0])
    page = StrategyPromotionPageControlBackend(domain, operator_users=frozenset({target.owner_id}))

    for revision in (0, 1):
        if revision == 1:
            bundle[0] = PromotionEvidenceBundle(target=target, validation=validation,
                family_receipt=receipt, adjusted_p=Decimal(".01"), adjustment_hash="4" * 64,
                outer=BoundOuterPromotionEvidence(target=target, parent_experiment_id=validation.experiment_id,
                    outer_experiment_id="5" * 64, grant_hash="6" * 64, reference=validation.reference,
                    window=specs[0].frozen_outer_test_range, net_return=Decimal(".1")),
                folds=tuple(BoundWalkForwardFold(target=target, index=i,
                    train_dates=(specs[0].train_range.start_date,), test_dates=(specs[0].validation_range.start_date,),
                    reference=validation.reference, net_return=Decimal(".01")) for i in range(1, 7)),
                observed_at=clock[0])
        review = domain.review(RequestPromotionReview(command_id=str(uuid4()), requested_at=clock[0],
            generation_id="synthetic-forward-role", target=target, expected_revision=revision,
            selection=selected), actor_id=target.owner_id)
        preparation = domain.prepare(PreparePromotionApproval(command_id=str(uuid4()), requested_at=clock[0],
            generation_id="synthetic-forward-role", target=target, review_id=review.review_id), actor_id=target.owner_id)
        domain.approve(ApprovePromotion(command_id=str(uuid4()), requested_at=clock[0],
            generation_id="synthetic-forward-role", target=target, preparation=preparation, entered_name=target.name),
            actor_id=target.owner_id, effect_id=uuid4())
    state = domain.store.promotion_state(target, verify_builtin=domain._builtin)
    assert state.stage is PromotionStage.PAPER_CANDIDATE
    profile = original.seed.runtime.execution_profile
    configuration = NativeMinuteForwardConfiguration(target=target,
        binding={"role_id": "native:" + target.strategy_id, "account_id": profile.paper_policy.account_id,
            "owner_id": target.owner_id, "strategy_id": target.strategy_id, "strategy_version": "1",
            "parameter_fingerprint": target.parameter_fingerprint, "cost_spec_id": profile.execution_costs.cost_spec_id,
            "ledger_id": "synthetic-native-ledger", "manifest_fingerprint": "1" * 64},
        metadata_identity=domain.store.identity(), source_key=original.seed.runtime.source_key,
        source_version=original.seed.runtime.source_version, execution_profile=profile, version=1,
        configured_at=clock[0], paper_approval_hash=state.paper_approval_hash, paper_approved_at=state.paper_approved_at)
    return page, configuration, clock, role_path


def native_forward_source_fixture(
    root: Path, monkeypatch: Any, *, native_id: str = "n_shape", completion_authority: bool = False,
    candidate_max_age_seconds: int | None = None,
) -> Any:
    """Original installed peers with synthetic post-approval live observation data."""
    import hashlib
    import inspect
    import json
    from rquant.live_spool import LiveBatchSpool
    from rquant.paper_broker import PaperBrokerStore
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.runtime_paper_quote import PaperPitQuoteResolver, PaperQuoteResolverConfig
    from rquant.runtime_service_entrypoint import RuntimeServiceManifest, RuntimeServiceKind
    from rquant.runtime_service_control import RuntimeServicePlane
    from rquant.runtime_builder_paper import PaperBrokerSettings, paper_broker_builder
    from rquant.runtime_builder_strategy import StrategyLiveRuntimeSettings, strategy_live_builder
    from rquant.strategy_runner import StrategyRunnerStore
    from rquant.signal_bus import SignalBusStore
    from rquant.signal_contracts import SignalAction
    from rquant.signal_route_spool import SignalRouteSpool
    from rquant.paper_research_runtime import NativeMinuteForwardState, NativeMinuteForwardRuntime, NativeMinuteForwardViewSource
    from tests.unit.test_runtime_paper_quote import _publish, _minute_row

    owner, original, clock, roles_path = native_forward_owner_fixture(root, monkeypatch, native_id=native_id)
    profile = original.execution_profile
    commit = profile.paper_policy.producer_commit
    registry = owner.domain.builtin_definitions[0][1]
    native = registry.read_strategy_spec(original.target.head.registration_fingerprint)
    dates = (date(2026, 10, 7), date(2026, 10, 8), date(2026, 10, 9))
    calendar = MarketCalendarAuthority.create(schema_version=1, exchange="SSE", producer_commit=commit,
        coverage_start=dates[0], coverage_end=dates[-1], open_dates=dates, generated_at=clock[0])
    content = json.dumps([{"exchange": "SSE", "cal_date": day.isoformat(), "is_open": True} for day in dates],
        sort_keys=True, separators=(",", ":")).encode()
    calendar_path = root / "original-trade-calendar.json"
    calendar_path.write_bytes(content)
    raw = LiveBatchSpool(root / "original-market")
    quote = PaperPitQuoteResolver(PaperQuoteResolverConfig(raw_spool_root=raw.root,
        trade_calendar_path=calendar_path, trade_calendar_sha256=hashlib.sha256(content).hexdigest(),
        execution_constraint_root=root / "original-constraints", expected_producer_commit=commit,
        timestamp_semantics=profile.timestamp_semantics, quote_max_age_seconds=profile.quote_max_age_seconds,
        max_finalize_scan_batches=profile.max_finalize_scan_batches, max_visible_scan_batches=profile.max_visible_scan_batches))
    policy = profile.paper_policy
    route = SignalRouteSpool(root / "original-signal-route")
    bus = SignalBusStore(root / "original-signal-bus.sqlite")
    route.publish(source=bus.source_descriptor(), records=())
    broker_settings = PaperBrokerSettings(account_id=policy.account_id,
        execution_lag_seconds=int(policy.execution_lag.total_seconds()),
        buy_quantity=policy.action_quantities[SignalAction.B_INTENT],
        reduce_quantity=policy.action_quantities[SignalAction.REDUCE],
        sell_quantity=policy.action_quantities[SignalAction.S_INTENT], signal_spool_root=route.paths.root,
        queue_path=root / "original-paper-queue.sqlite", consumer_state_path=root / "original-consumer.sqlite",
        broker_path=root / "native-broker.sqlite", initial_cash=profile.initial_cash,
        execution_cost_spec=profile.execution_costs, limit=10)
    broker_manifest = RuntimeServiceManifest(service_id="paper:" + native_id, service_kind=RuntimeServiceKind.PAPER_BROKER,
        plane=RuntimeServicePlane.LIVE, interval_seconds=2, stale_after_seconds=20,
        producer_commit=commit, settings=broker_settings.model_dump(mode="json"))
    broker_step = paper_broker_builder(clock=lambda: clock[0], quote_resolver=quote,
        trade_date_resolver=quote.trade_date_at)(broker_manifest)
    try:
        # The fixture reads the real builder closure; it never creates a second
        # broker or queue. Production composition remains an explicit owner bind.
        peers = inspect.getclosurevars(broker_step).nonlocals
        broker, queue = peers["broker"], peers["queue"]
        with PaperBrokerStore.open_readonly(broker.path, account_id=broker.account_id,
            initial_cash=broker.initial_cash, cost_policy=broker.cost_policy) as peer:
            _, head = peer._attestation_head(peer._connect())
        settings = StrategyLiveRuntimeSettings(feature_spool_root=root / "original-features",
            runner_state_path=root / "original-runner.sqlite", definition_registry_root=registry.root,
            strategy_registration_fingerprint=native.fingerprint,
            strategy_executable_fingerprint=native.executable_fingerprint,
            candidate_schema_fingerprint=native.candidate_schema_fingerprint,
            candidate_snapshot_root=root / "original-candidates", paper_broker_path=broker.path,
            paper_account_id=broker.account_id,
            candidate_max_age_seconds=profile.candidate_max_age_seconds if candidate_max_age_seconds is None else candidate_max_age_seconds,
            strategy_id=native_id, strategy_version=1)
        signing_authority, routing_policy = None, None
        if completion_authority:
            from rquant.delivery_contracts import DeliveryChannel
            from rquant.runtime_routing_policy import FrozenRoutingPolicyResolver, RoutingPolicyDocument, RoutingPolicyRule
            from tests.shadow_ed25519_support import create_shadow_ed25519_test_authority

            calendar_authority_path = root / "original-session-calendar.json"
            calendar_authority_path.write_text(calendar.model_dump_json())
            calendar_authority_path.chmod(0o600)
            policy_document = RoutingPolicyDocument(default_no_target_reason="unconfigured_synthetic_target",
                rules=tuple(RoutingPolicyRule(strategy_id=native_id, strategy_version="1", action=action,
                    recipient_id=policy.account_id, channel=DeliveryChannel.PUSHDEER, enabled=True)
                    for action in native.spec.allowed_actions))
            policy_path = root / "original-routing-policy.json"
            policy_bytes = policy_document.model_dump_json().encode()
            policy_path.write_bytes(policy_bytes)
            policy_path.chmod(0o600)
            routing_policy = FrozenRoutingPolicyResolver.from_document(source_path=policy_path,
                content_sha256=hashlib.sha256(policy_bytes).hexdigest(), policy=policy_document)
            settings = StrategyLiveRuntimeSettings.model_validate(settings.model_dump(mode="python") | {
                "calendar_path": calendar_authority_path, "calendar_expected_commit": commit,
                "calendar_content_sha256": calendar.content_sha256, "signal_bus_path": bus.path,
                "routing_policy_fingerprint": routing_policy.routing_policy_fingerprint,
                "producer_instance_id": "synthetic-native-forward:" + native_id,
                "producer_version": "synthetic-original-runtime-v1",
                "strategy_spec_fingerprint": native.spec.spec_fingerprint,
                "evaluator_contract_fingerprint": native.executable_fingerprint,
            })
            signing_authority = create_shadow_ed25519_test_authority(root / "synthetic-completion-keys")
        manifest = RuntimeServiceManifest(service_id=original.binding.role_id, service_kind=RuntimeServiceKind.STRATEGY_LIVE,
            plane=RuntimeServicePlane.LIVE, interval_seconds=2, stale_after_seconds=20,
            producer_commit=commit, settings=settings.model_dump(mode="json"))
        configuration = original.model_copy(update={"binding": original.binding.model_copy(update={
            "ledger_id": head["ledger_generation"], "manifest_fingerprint": manifest.manifest_fingerprint})})
        captured = []

        def forward_source(actual_runner: StrategyRunnerStore, actual_manifest: RuntimeServiceManifest) -> NativeMinuteForwardViewSource:
            state = NativeMinuteForwardState(owner, configuration)
            runtime = NativeMinuteForwardRuntime(state=state, broker=broker, queue=queue, runner=actual_runner,
                quote=quote, calendar=calendar, manifest=actual_manifest)
            source = NativeMinuteForwardViewSource(runtime)
            captured.append(source)
            return source

        strategy_step = strategy_live_builder(clock=lambda: clock[0], native_forward_source_factory=forward_source,
            completion_attestation_signer=None if signing_authority is None else signing_authority.signer,
            completion_attestation_active_key_id=None if signing_authority is None else signing_authority.keyring.active_key_id)(manifest)
        assert len(captured) == 1
        source = captured[0]
        runtime = source.runtime
        close = datetime(2026, 10, 7, 7, tzinfo=UTC)
        if not completion_authority:
            _publish(raw, sequence=0, available_at=close, rows=[_minute_row(trade_time=close)], producer_commit=commit)
            clock[0] = close + timedelta(seconds=5)
    except BaseException:
        broker_step.close()
        raise
    return SimpleNamespace(source=source, runtime=runtime, owner=owner, configuration=configuration,
        clock=clock, roles_path=roles_path, raw=raw, close=close,
        evaluator=BuiltinStrategyEvaluatorRegistry(producer_commit=commit).load_binding(native_id, 1).evaluator,
        broker_step=broker_step, strategy_step=strategy_step, close_owner=broker_step.close,
        bus=bus, route=route, calendar=calendar, signing_authority=signing_authority, routing_policy=routing_policy)


def drive_native_forward_session(fixture: Any) -> Any:
    """Synthetic live clock; every signal, fill, completion and NAV uses its original owner."""
    from contextlib import closing
    from rquant.feature_spool import FeatureBatchSpool
    from rquant.live_contracts import LiveChannel
    from rquant.paper_execution_constraints import PaperExecutionConstraintBatch, PaperExecutionConstraintSnapshot, PaperExecutionConstraintPublisher
    from rquant.signal_contracts import SignalAction
    from rquant.signal_router_runtime import ReadonlyStrategyRunnerSignalSource, SignalRouteCursorStore, route_runner_signals
    from rquant.signal_route_spool import publish_signal_bus_prefix
    from tests.paper_cost_fixtures import paper_instrument_context
    from tests.unit.test_runtime_builder_strategy import _publish as publish_features, _publish_candidates
    from tests.unit.test_runtime_paper_quote import _publish as publish_market, _minute_row

    if fixture.signing_authority is None or fixture.routing_policy is None:
        raise ValueError("original complete native forward authority is not installed")
    runtime, settings = fixture.runtime, fixture.runtime.manifest.settings
    day = fixture.close.date()
    entry = fixture.close.replace(hour=1, minute=40)
    native_id, commit = runtime.runner.spec.strategy_id, runtime.producer_commit
    features = FeatureBatchSpool(Path(settings["feature_spool_root"]))
    _publish_candidates(Path(settings["candidate_snapshot_root"]), strategy_id=native_id,
        producer_commit=commit, trade_date=day, captured_at=entry,
        definition_fingerprint=settings["strategy_registration_fingerprint"],
        executable_fingerprint=settings["strategy_executable_fingerprint"],
        candidate_schema_fingerprint=settings["candidate_schema_fingerprint"])
    constraint_body = {"ts_code": "600000.SH", "trade_date": day,
        "available_at": entry - timedelta(minutes=1), "expires_at": fixture.close + timedelta(minutes=1),
        "suspended": False, "buy_limit_locked": False, "sell_limit_locked": False, "risk_rejected": False,
        "instrument_context": paper_instrument_context("600000.SH"),
        "source_snapshot_ids": {"synthetic_native_definition": runtime.runner.spec.spec_fingerprint},
        "producer_commit": commit}
    constraint = PaperExecutionConstraintSnapshot.model_validate(constraint_body | {"content_hash": canonical_sha256(constraint_body)})
    batch_body = {"schema_version": 1, "sequence": 0, "producer_commit": commit, "records": (constraint,)}
    PaperExecutionConstraintPublisher(root=runtime.quote.config.execution_constraint_root,
        producer_commit=commit, clock=lambda: entry - timedelta(minutes=1)).publish(
            PaperExecutionConstraintBatch.model_validate(batch_body | {"content_hash": canonical_sha256(batch_body)}))
    publish_market(fixture.raw, sequence=0, available_at=entry,
        rows=[_minute_row(trade_time=entry, close=11.2)], producer_commit=commit)
    publish_features(features, sequence=0, strategy_id=native_id, available_at=entry,
        source_event_time=entry, decision_cutoff=entry)
    fixture.clock[0] = entry + timedelta(seconds=1)
    fixture.strategy_step()
    if native_id == "auction_gap":
        publish_features(features, sequence=1, strategy_id=native_id,
            available_at=entry + timedelta(seconds=1), source_event_time=entry + timedelta(seconds=1),
            decision_cutoff=entry + timedelta(seconds=1))
        fixture.clock[0] = entry + timedelta(seconds=2)
        fixture.strategy_step()
    runner_source = ReadonlyStrategyRunnerSignalSource(source_id=runtime.manifest.service_id,
        path=runtime.runner.path, expected_strategy_spec_fingerprint=runtime.runner.spec.spec_fingerprint,
        expected_evaluator_contract_fingerprint=runtime.runner.evaluator_contract_fingerprint)
    route_runner_signals(source_id=runtime.manifest.service_id, source=runner_source, bus=fixture.bus,
        cursors=SignalRouteCursorStore(fixture.bus.path,
            routing_policy_fingerprint=fixture.routing_policy.routing_policy_fingerprint),
        routed_at=fixture.clock[0], target_resolver=fixture.routing_policy, limit=100)
    publish_signal_bus_prefix(bus=fixture.bus, spool=fixture.route, limit=100)
    entries = tuple(record.signal for record in runtime.runner.signals_after(sequence=0)
        if record.signal.action is SignalAction.B_INTENT)
    if not entries:
        raise ValueError("original native runner did not produce its entry signal")
    execution = max(max(signal.available_at, signal.event_time + runtime.queue.policy.execution_lag)
        for signal in entries)
    publish_market(fixture.raw, sequence=1, available_at=execution,
        rows=[_minute_row(trade_time=execution, close=11.0)], producer_commit=commit)
    fixture.clock[0] = execution
    fixture.broker_step()
    last_market = publish_market(fixture.raw, sequence=2, available_at=fixture.close,
        rows=[_minute_row(trade_time=fixture.close, close=11.1)], producer_commit=commit)
    publish_features(features, sequence=2 if native_id == "auction_gap" else 1,
        strategy_id=native_id, available_at=fixture.close, source_event_time=fixture.close,
        decision_cutoff=fixture.close, latest_close=11.1)
    fixture.clock[0] = fixture.close + timedelta(seconds=1)
    fixture.strategy_step()
    route_runner_signals(source_id=runtime.manifest.service_id, source=runner_source, bus=fixture.bus,
        cursors=SignalRouteCursorStore(fixture.bus.path,
            routing_policy_fingerprint=fixture.routing_policy.routing_policy_fingerprint),
        routed_at=fixture.clock[0], target_resolver=fixture.routing_policy, limit=100)
    publish_signal_bus_prefix(bus=fixture.bus, spool=fixture.route, limit=100)
    fixture.clock[0] = fixture.close + timedelta(seconds=5)
    features.publish_session_close_marker(trade_date=day, session_close_at=fixture.close,
        produced_at=fixture.clock[0], calendar_generation_id=fixture.calendar.content_sha256,
        complete_through=fixture.close,
        upstream_source_generation_id=fixture.raw.source_descriptor(LiveChannel.MARKET_MINUTE).generation_id,
        upstream_final_sequence=last_market.sequence, upstream_final_batch_id=last_market.batch_id,
        upstream_final_content_hash=last_market.content_sha256)
    fixture.strategy_step()
    receipt = runtime.runner.session_close_receipt(day)
    if receipt is None or receipt.completion_attestation is None:
        raise ValueError("original installed native daily completion was not produced")
    if not fixture.signing_authority.keyring.verify(receipt.completion_attestation):
        raise ValueError("original native completion signature did not verify")
    frame = fixture.source.read(as_of=fixture.clock[0]).frame
    with closing(runtime.queue._connect()) as connection:
        pending = connection.execute("SELECT COUNT(*) FROM paper_signal_queue WHERE status IN ('pending','prepared')").fetchone()[0]
    nav = fixture.source.views.nav_series()
    if not nav:
        raise ValueError(f"original signed native close has no NAV; actual pending queue={pending}")
    return SimpleNamespace(receipt=receipt, frame=frame, nav=nav[-1],
        pending_queue=pending, signals=runtime.runner.signals_after(sequence=0),
        orders=tuple(row.order for row in frame.history), fills=tuple(fill for row in frame.history for fill in row.fills))
