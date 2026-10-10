"""Private installation of the native minute adapter in the original Lab."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, SerializerFunctionWrapHandler, field_validator, model_serializer, model_validator

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.experiment_registry import ExperimentRegistry, ExperimentRegistryReadonlyReader, IncompleteHypothesisFamilyError
from rquant.job_center_authority import JobCenterAuthorityManifest, resolve_current_job_center_authority_binding
from rquant.lab_daemon import load_lab_job_center_authority_manifest
from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandSpool
from rquant.lab_jobs import LabJobReader
from rquant.lab_artifacts import _ensure_private_directory
from rquant.lab_worker import build_builtin_shard_runtime_manifest
from rquant.lab_worker_registry import builtin_lab_shard_configuration, resolve_builtin_adapter_registry
from rquant.metadata_catalog import ImmutableDuckDBMetadataCatalog, MetadataCatalogDescriptor
from rquant.minute_backtest_contracts import MinuteReplayModel, Sha256
from rquant.minute_backtest_formal import PreparedMinuteRequest, build_minute_plan, register_minute_plan
from rquant.minute_backtest_parameter_formal import (
    PreparedMinuteParameterRequest, build_minute_parameter_plan, register_minute_parameter_plan,
)
from rquant.minute_backtest_parameter_producer import MinuteParameterReplayCatalog, MinuteParameterResolvedReadUnit
from rquant.minute_backtest_producer import (
    MinutePrivateFileReference, MinuteReplayCatalog, PublishedMinuteInput,
    _secure_private_bytes, _strict_json, open_gated_minute_store,
)
from rquant.minute_backtest_publication_contracts import MAX_MINUTE_CONTROL_BYTES
from rquant.research_gate import ResearchGateRequest
from rquant.research_run_spec import DatasetSnapshotIdentity
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry


MINUTE_INSTALLATION_ENV = "RQUANT_MINUTE_REPLAY_INSTALLATION"


class MinuteReplayInstallation(MinuteReplayModel):
    contract: Literal["minute-replay-installation/v1"] = "minute-replay-installation/v1"
    code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    deployment_profile_id: Sha256
    deployment_generation_hash: Sha256
    authority_manifest_hash: Sha256
    runtime_deployment_root: Path
    runtime_root: Path
    lab_jobs_path: Path
    command_spool_path: Path
    final_artifact_root: Path
    metadata_identity: MetadataCatalogDescriptor
    forbidden_paths: tuple[Path, ...] = ()
    snapshot_root: Path
    research_lake_root: Path
    catalog: MinuteReplayCatalog | None = None
    parameter_catalog: MinuteParameterReplayCatalog | None = None

    @model_serializer(mode="wrap")
    def preserve_native_serialization(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        data = handler(self)
        if self.parameter_catalog is None:
            data.pop("parameter_catalog", None)
        if self.catalog is None:
            data.pop("catalog", None)
        return data

    @field_validator("runtime_deployment_root", "runtime_root", "lab_jobs_path", "command_spool_path",
        "final_artifact_root", "snapshot_root", "research_lake_root")
    @classmethod
    def normalized_path(cls, value: Path) -> Path:
        if not value.is_absolute() or value != Path(os.path.abspath(value)):
            raise ValueError("minute installation paths must be absolute and normalized")
        return value

    @model_validator(mode="after")
    def closed_installation(self) -> Self:
        self.normalized_path(self.metadata_identity.source_path)
        for path in self.forbidden_paths:
            self.normalized_path(path)
        if len(self.model_dump_json().encode()) > MAX_MINUTE_CONTROL_BYTES:
            raise ValueError("minute installation exceeds original 1 MiB control budget")
        if self.metadata_identity.source_path in self.forbidden_paths:
            raise ValueError("minute metadata cannot be an operational authority")
        if self.catalog is None and self.parameter_catalog is None:
            raise ValueError("minute installation requires a complete native or parameter catalog")
        if self.parameter_catalog is not None and self.parameter_catalog.fact_sources:
            parameter = self.parameter_catalog
            if (parameter.prepared_root, parameter.snapshot_root, parameter.research_lake_root, parameter.forbidden_paths) != (
                self.runtime_root / "minute-parameter-prepared", self.snapshot_root, self.research_lake_root, self.forbidden_paths):
                raise ValueError("parameter installed private producer/snapshot/lake roots differ")
        return self


def _current_authority(profile: MinuteReplayInstallation) -> JobCenterAuthorityManifest:
    binding = resolve_current_job_center_authority_binding(profile.runtime_deployment_root,
        expected_code_sha=profile.code_sha, runtime_root=profile.runtime_root, lab_jobs_path=profile.lab_jobs_path,
        command_spool_path=profile.command_spool_path, final_artifact_root=profile.final_artifact_root)
    if (binding.deployment_profile_id, binding.deployment_generation_hash) != (
        profile.deployment_profile_id, profile.deployment_generation_hash):
        raise PermissionError("minute installed current deployment generation differs")
    authority = load_lab_job_center_authority_manifest(binding.runtime_root / "job-center-authority.json",
        expected_code_sha=profile.code_sha, expected_research_root=binding.runtime_root,
        expected_lab_jobs_path=binding.lab_jobs_path, expected_command_spool_path=binding.command_spool_path,
        expected_final_artifact_root=binding.final_artifact_root, expected_runtime_deployment_root=binding.runtime_deployment_root,
        expected_deployment_profile_id=binding.deployment_profile_id,
        expected_deployment_generation_hash=binding.deployment_generation_hash)
    if authority.manifest_hash != profile.authority_manifest_hash:
        raise PermissionError("minute installed full Job Center authority differs")
    return authority


class InstalledMinuteReplay:
    def __init__(self, profile: MinuteReplayInstallation, reference: MinutePrivateFileReference,
        authority: JobCenterAuthorityManifest, *, writable: bool, clock: Callable[[], datetime]) -> None:
        self.profile, self.reference, self.authority, self.clock = profile, reference, authority, clock
        self.writable = writable
        self.reader = LabJobReader(authority.lab_jobs_path)
        self.definitions = ImmutableDefinitionRegistry(authority.definition_registry_root,
            execution_registry=BuiltinStrategyEvaluatorRegistry(producer_commit=profile.code_sha).trusted_executable_registry())
        if writable:
            # Fixed writer-owned directories precede the original registry's root
            # binding; later ZIP publication changes only their private children.
            for path in (profile.runtime_root / "exports", profile.runtime_root / "exports" / "minute-base",
                profile.runtime_root / "exports" / "minute-reports",
                *((profile.runtime_root / "minute-parameter-prepared",) if profile.parameter_catalog is not None else ())):
                _ensure_private_directory(path, manage_existing=False, require_private_existing=True)
        self.experiments = (ExperimentRegistry(authority.experiment_registry_path, managed_trust_root=authority.runtime_root)
            if writable else ExperimentRegistryReadonlyReader(authority.experiment_registry_path, managed_trust_root=authority.runtime_root))
        self.commands = LabCommandSubmissionFacade(reader=self.reader, spool=LabCommandSpool(authority.command_spool_path),
            experiment_registry=self.experiments, definition_registry=self.definitions,
            minute_parameter_catalog=profile.parameter_catalog, clock=clock)
        configuration = builtin_lab_shard_configuration(catalog_path=profile.metadata_identity.source_path,
            forbidden_paths=profile.forbidden_paths, snapshot_root=profile.snapshot_root,
            research_lake_root=profile.research_lake_root, minute_catalog=profile.catalog,
            parameter_catalog=profile.parameter_catalog, minute_registry_mode="installed")
        self.registry = resolve_builtin_adapter_registry(configuration)
        self.shard_manifest = build_builtin_shard_runtime_manifest(catalog_path=profile.metadata_identity.source_path,
            forbidden_paths=profile.forbidden_paths, snapshot_root=profile.snapshot_root,
            research_lake_root=profile.research_lake_root, minute_catalog=profile.catalog,
            parameter_catalog=profile.parameter_catalog, minute_registry_mode="installed")
        self.verify_current()

    def verify_current(self) -> None:
        data, _ = _secure_private_bytes(self.reference.path, self.reference)
        if MinuteReplayInstallation.model_validate_json(data) != self.profile or _current_authority(self.profile) != self.authority:
            raise PermissionError("minute installed profile or current authority changed")

    def guard(self, original: Callable[[], str]) -> Callable[[], str]:
        def verify() -> str:
            code_sha = original()
            self.verify_current()
            if code_sha != self.profile.code_sha:
                raise PermissionError("minute running code changed from installed code")
            return code_sha
        return verify

    @contextmanager
    def _metadata_store(self) -> Iterator[DuckDBStore]:
        profile = self.profile
        with ImmutableDuckDBMetadataCatalog.open(profile.metadata_identity.source_path,
            forbidden_paths=profile.forbidden_paths, snapshot_root=profile.snapshot_root) as catalog:
            if catalog.descriptor != profile.metadata_identity:
                raise PermissionError("minute installed complete metadata physical identity differs")
            with DuckDBStore(catalog.snapshot_path, read_only=True) as store:
                yield store

    def publication(self, *, source_key: str, source_version: int, owner_id: str,
        full_input_hash: str | None = None) -> PublishedMinuteInput:
        self.verify_current()
        profile = self.profile
        if profile.catalog is None:
            raise PermissionError("minute installed native catalog is unavailable")
        receipt = profile.catalog.resolve(source_key=source_key, source_version=source_version, owner_id=owner_id)
        frozen = receipt.frozen
        if frozen.runtime.producer_commit != profile.code_sha or (full_input_hash is not None and full_input_hash != frozen.full_input_hash):
            raise PermissionError("minute selected complete source/code hash differs")
        now = self.clock()
        if frozen.provenance.published_at > now:
            raise PermissionError("minute actual source publication is not yet available")
        request = ResearchGateRequest(mode="formal", strategy_name="minute_runtime_replay",
            start_date=frozen.runtime.start_date, end_date=frozen.runtime.end_date,
            audit_run_id=frozen.runtime.audit_run_id, dataset_snapshot_id=frozen.runtime.dataset_snapshot_id,
            dataset_binding_hash=receipt.binding.binding_hash, code_commit=profile.code_sha)
        with open_gated_minute_store(request, metadata_store_factory=self._metadata_store, lake_root=profile.research_lake_root,
            catalog=profile.catalog, source_key=source_key, source_version=source_version, owner_id=owner_id) as (_, decision):
            reference = next(x for x in profile.catalog.entries if (x.source_key, x.source_version, x.owner_id) == (
                source_key, source_version, owner_id))
            published = PublishedMinuteInput(receipt=receipt, reference=reference,
                identity=DatasetSnapshotIdentity(snapshot_id=receipt.snapshot.snapshot_id,
                    binding_hash=receipt.binding.binding_hash, audit_run_id=receipt.audit.audit_run_id), gate_decision=decision)
        if (self.definitions.read_strategy_spec(frozen.runtime.strategy.registration_fingerprint, as_of=now),
            self.definitions.latest_strategy_spec("minute_runtime_replay", as_of=now)) != (
            frozen.native_registration, frozen.wrapper_registration):
            raise PermissionError("minute installed complete native/wrapper registrations differ")
        self.verify_current()
        return published

    def parameter_submission_facade(self, spec: object, *,
        resolved_read_unit: MinuteParameterResolvedReadUnit | None = None,
        _minute_input_read: object | None = None) -> LabCommandSubmissionFacade:
        from rquant.research_run_spec import ResearchRunSpec

        self.verify_current()
        checked = ResearchRunSpec.model_validate(spec)
        if _minute_input_read is not None:
            from rquant.minute_backtest_parameter_study_projection import _MinuteStudyVerifiedInputRead

            if type(_minute_input_read) is not _MinuteStudyVerifiedInputRead:
                raise TypeError("minute installation needs its actual authenticated lexical input read")
            _minute_input_read._assert_active(reader=self.reader, catalog=self.profile.parameter_catalog, spec=checked)
            if _minute_input_read._projection.installation is not self:
                raise PermissionError("minute input read belongs to a different installation instance")
        definitions = (self.commands.parameter_definitions_for_spec(checked, _minute_input_read=_minute_input_read)
            if _minute_input_read is not None else self.commands.parameter_definitions_for_spec(checked) if resolved_read_unit is None
            else self.commands.parameter_definitions_for_spec(checked, resolved_read_unit=resolved_read_unit))
        facade = LabCommandSubmissionFacade(reader=self.reader, spool=LabCommandSpool(self.authority.command_spool_path),
            experiment_registry=self.experiments, definition_registry=definitions,
            minute_parameter_catalog=self.profile.parameter_catalog, clock=self.clock,
            resolved_read_unit=resolved_read_unit, _minute_input_read=_minute_input_read)
        self.verify_current()
        return facade

    def _prepare_parameter(self, config: object, *, owner_id: str, operation_key: str | None) -> PreparedMinuteParameterRequest:
        from rquant.minute_backtest_commands import MinuteParameterRunConfig
        from rquant.minute_backtest_parameter_definition import minute_parameter_research_registry
        from rquant.minute_backtest_parameter_preparation import prepare_minute_parameter_publication

        checked = MinuteParameterRunConfig.model_validate(config)
        catalog = self.profile.parameter_catalog
        if catalog is None or operation_key is None or not self.writable:
            raise PermissionError("parameter preparation requires its original writer, complete installed facts and command")
        publication = prepare_minute_parameter_publication(checked, owner_id=owner_id, operation_key=operation_key,
            catalog=catalog, definitions_root=self.authority.definition_registry_root, now=self.clock(),
            expected_code_sha=self.profile.code_sha)
        published, carrier = publication.published, publication.prepared_publication
        frozen = published.receipt.frozen
        definitions = ImmutableDefinitionRegistry(self.authority.definition_registry_root,
            execution_registry=minute_parameter_research_registry(frozen.runtime.parameters,
                producer_commit=self.profile.code_sha))
        now = self.clock()
        candidate = build_minute_parameter_plan(frozen, published, prepared_publication=carrier,
            catalog=catalog, definitions=definitions, protocol=checked.protocol, now=now,
            deadline=checked.deadline, random_seed=checked.random_seed)
        spec = candidate.formal_plan.spec
        keys = ("strategy_spec_fingerprint", "strategy_executable_fingerprint", "candidate_schema_fingerprint",
            "dataset_snapshot_id", "code_commit", "parameter_fingerprint", "cost_model_fingerprint", "execution_model_fingerprint", "seed")
        try:
            existing = self.experiments.resolve_formal_plan(**{key: getattr(spec, key) for key in keys}, as_of=now)
        except IncompleteHypothesisFamilyError:
            prepared = register_minute_parameter_plan(frozen, published, prepared_publication=carrier,
                catalog=catalog, definitions=definitions, experiments=self.experiments,
                protocol=checked.protocol, now=now, deadline=checked.deadline, random_seed=checked.random_seed)
        else:
            if existing.spec != spec or existing.hypothesis_variant != candidate.formal_plan.hypothesis_variant:
                raise PermissionError("parameter original registered plan has conflicting full protocol")
            prepared = PreparedMinuteParameterRequest.model_validate(
                candidate.model_dump(mode="python", exclude_computed_fields=True) | {"formal_plan": existing})
        self.verify_current()
        return prepared

    def prepare(self, config: object, *, owner_id: str, operation_key: str | None = None) -> PreparedMinuteRequest | PreparedMinuteParameterRequest:
        from rquant.minute_backtest_commands import MinuteRunConfig, MinuteParameterRunConfig

        if isinstance(config, MinuteParameterRunConfig):
            return self._prepare_parameter(config, owner_id=owner_id, operation_key=operation_key)

        checked = MinuteRunConfig.model_validate(config)
        if not isinstance(self.experiments, ExperimentRegistry):
            raise PermissionError("minute plan registration belongs to the writer")
        published = self.publication(source_key=checked.source_key, source_version=checked.source_version,
            owner_id=owner_id, full_input_hash=checked.full_input_hash)
        frozen = published.receipt.frozen
        if (checked.native_id, checked.native_version) != (frozen.runtime.strategy.strategy_id, frozen.runtime.strategy.strategy_version):
            raise PermissionError("minute source is bound to another native strategy/version")
        now = self.clock()
        candidate = build_minute_plan(frozen, published, catalog=self.profile.catalog, definitions=self.definitions,
            protocol=checked.protocol, now=now, deadline=checked.deadline, random_seed=checked.random_seed)
        spec = candidate.formal_plan.spec
        keys = ("strategy_spec_fingerprint", "strategy_executable_fingerprint", "candidate_schema_fingerprint",
            "dataset_snapshot_id", "code_commit", "parameter_fingerprint", "cost_model_fingerprint", "execution_model_fingerprint", "seed")
        try:
            existing = self.experiments.resolve_formal_plan(**{key: getattr(spec, key) for key in keys}, as_of=now)
        except IncompleteHypothesisFamilyError:
            prepared = register_minute_plan(frozen, published, catalog=self.profile.catalog, definitions=self.definitions,
                experiments=self.experiments, protocol=checked.protocol, now=now, deadline=checked.deadline,
                random_seed=checked.random_seed)
        else:
            if existing.spec != spec or existing.hypothesis_variant != candidate.formal_plan.hypothesis_variant:
                raise PermissionError("minute original registered plan has conflicting protocol")
            prepared = PreparedMinuteRequest.model_validate(candidate.model_dump(mode="python") | {"formal_plan": existing})
        self.verify_current()
        return prepared


def load_minute_replay_installation(path: Path, *, expected_code_sha: str | None = None,
    writable: bool = False, clock: Callable[[], datetime] | None = None) -> InstalledMinuteReplay:
    data, reference = _secure_private_bytes(path)
    if len(data) > MAX_MINUTE_CONTROL_BYTES:
        raise PermissionError("minute installation exceeds original 1 MiB control budget")
    _strict_json(data)
    profile = MinuteReplayInstallation.model_validate_json(data)
    if expected_code_sha is not None and expected_code_sha != profile.code_sha:
        raise PermissionError("minute installed runtime code is not the actual attested code")
    authority = _current_authority(profile)
    _secure_private_bytes(path, reference)
    return InstalledMinuteReplay(profile, reference, authority, writable=writable, clock=clock or (lambda: datetime.now(UTC)))
