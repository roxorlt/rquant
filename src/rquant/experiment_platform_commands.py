"""One bounded experiment producer and original PageControl effects."""

from __future__ import annotations

import hashlib
import os
import stat
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, Protocol
from uuid import UUID, uuid4

import duckdb
from pydantic import Field, JsonValue, TypeAdapter

from rquant.data_metadata import DataAuditRun, DatasetSnapshot
from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.experiment_platform import (
    MAX_FAMILY_INPUT_BYTES,
    ExperimentChildAdmission,
    ExperimentChildRegistration,
    ExperimentFamilyRecord,
    ExperimentOuterGrant,
    ExperimentPhaseRead,
    ExperimentPlatformStore,
    ExperimentPreparationReceipt,
    ExperimentPreparationReservation,
    ExperimentSearchRequest,
    ExperimentSourceProfile,
    Owner,
    Sha256,
    holdout_cutoff,
    stable_experiment_interaction,
    stable_experiment_job,
    validate_experiment_dates,
)
from rquant.experiment_platform_projection import ExperimentPrivateResultAuthority
from rquant.experiment_platform_template_models import (
    ExperimentTemplateBaseline,
    PreparedExperimentTemplate,
)
from rquant.experiment_registry import DateRange
from rquant.lab_job_center import CommandSubmissionReceipt, LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandEnvelope
from rquant.portfolio_backtest_adapter import read_portfolio_input_table
from rquant.portfolio_backtest_artifact import PortfolioResultReader
from rquant.portfolio_backtest_source import (
    PortfolioSourceData,
    PublishedPortfolioInput,
    build_portfolio_plan,
    freeze_portfolio_config,
    publish_portfolio_input,
    require_portfolio_gate,
    verify_bound_portfolio_input,
)
from rquant.research_catalog import ResearchCatalog
from rquant.research_gate import ResearchGateRequest
from rquant.research_run_spec import DatasetSnapshotIdentity
from rquant.research_snapshot import ResearchExecutionSession
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader

if TYPE_CHECKING:
    from rquant.experiment_platform_evidence import ExperimentIndependenceEvidence
    from rquant.experiment_platform_projection import (
        ExperimentAttemptFact,
        ExperimentFamilyFact,
        ExperimentPrivateProjectionReader,
    )
    from rquant.experiment_platform_templates import ExperimentTemplateBinding
    from rquant.lab_job_center import ExperimentLifecycleCoordinator
    from rquant.portfolio_backtest_models import FrozenPortfolioInput, PortfolioBacktestConfig
    from rquant.promotions_serving_authority import PromotionsSourceReader
    from rquant.web.experiment_platform_service import ExperimentWebService
    from rquant.web.lab_control_gateway import LabControlGateway


class _ExperimentCommand(RuntimeContractModel):
    command_id: str = Field(pattern=r"^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$")
    requested_at: AwareUtcDatetime
    actor_id: Owner


class RegisterExperimentFamily(_ExperimentCommand):
    kind: Literal["register_experiment_family"] = "register_experiment_family"
    request: ExperimentSearchRequest


class CancelExperimentFamily(_ExperimentCommand):
    kind: Literal["cancel_experiment_family"] = "cancel_experiment_family"
    family_id: str = Field(max_length=100)


class SetExperimentNote(_ExperimentCommand):
    kind: Literal["set_experiment_note"] = "set_experiment_note"
    family_id: str = Field(max_length=100)
    expected_version: int = Field(strict=True, ge=0)
    text: str = Field(max_length=1024)


class UnsealExperimentOuterTest(_ExperimentCommand):
    kind: Literal["unseal_experiment_outer_test"] = "unseal_experiment_outer_test"
    family_id: str = Field(max_length=100)
    experiment_id: Sha256
    result_hash: Sha256
    confirmed: Literal[True]


class SetExperimentHoldoutPolicy(_ExperimentCommand):
    kind: Literal["set_experiment_holdout_policy"] = "set_experiment_holdout_policy"
    expected_version: int = Field(strict=True, ge=0)
    months: int = Field(strict=True, ge=0, le=36)


ExperimentCommand = Annotated[
    RegisterExperimentFamily
    | CancelExperimentFamily
    | SetExperimentNote
    | UnsealExperimentOuterTest
    | SetExperimentHoldoutPolicy,
    Field(discriminator="kind"),
]
EXPERIMENT_COMMAND_TYPES = (
    RegisterExperimentFamily,
    CancelExperimentFamily,
    SetExperimentNote,
    UnsealExperimentOuterTest,
    SetExperimentHoldoutPolicy,
)


class ExperimentEffect(RuntimeContractModel):
    contract: Literal["experiment-admission/v1"] = "experiment-admission/v1"
    command_hash: Sha256
    owner: Owner
    action: Literal[
        "register_experiment_family",
        "cancel_experiment_family",
        "set_experiment_note",
        "unseal_experiment_outer_test",
        "set_experiment_holdout_policy",
    ]
    family_id: str | None = None
    grant_id: Sha256 | None = None
    version: int | None = None


class ExperimentCommandResult(RuntimeContractModel):
    contract: Literal["experiment-command-result/v1"] = "experiment-command-result/v1"
    command_id: UUID
    owner: Owner
    action: str
    family_id: str | None = None
    job_ids: tuple[UUID, ...] = Field(default=(), max_length=64)
    status: Literal[
        "registered",
        "cancellation_pending",
        "cancelled",
        "already_completed",
        "already_finished",
        "note_saved",
        "policy_saved",
        "outer_admitted",
    ]
    planned_count: int = Field(default=0, ge=0, le=64)
    version: int | None = None


class ExperimentPageControlBackend(Protocol):
    def freeze(self, command: ExperimentCommand) -> JsonValue: ...
    def submit(self, command: ExperimentCommand, marker: JsonValue) -> JsonValue: ...
    def recover(self, command: ExperimentCommand, marker: JsonValue) -> JsonValue | None: ...


class ExperimentPreparationUncertainError(RuntimeError):
    """A persisted exact admission still needs original preparation recovery."""


def _input_digest(path: Path) -> tuple[tuple[int, int, int, int], str]:
    before = path.lstat()
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) != 0o600
    ):
        raise PermissionError("experiment input file is not private and owned")
    if before.st_size > MAX_FAMILY_INPUT_BYTES:
        raise ValueError("experiment input exceeds 512 MiB")
    digest = hashlib.sha256()
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise ValueError("experiment input identity changed")
        while raw := os.read(descriptor, 1024 * 1024):
            digest.update(raw)
        after = os.fstat(descriptor)
        current = path.lstat()
        identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        if any(
            (v.st_dev, v.st_ino, v.st_size, v.st_mtime_ns) != identity for v in (after, current)
        ):
            raise ValueError("experiment input changed while reading")
        return identity, digest.hexdigest()
    finally:
        os.close(descriptor)


class ExperimentFamilyPreparer:
    """The installed provider reads an exact phase before returning any price rows."""

    def __init__(
        self,
        *,
        store: ExperimentPlatformStore,
        definitions: ImmutableDefinitionRegistry,
        profiles: tuple[ExperimentSourceProfile, ...],
        phase_provider: Callable[[ExperimentPhaseRead], PortfolioSourceData],
        metadata_store_factory: Callable[[], AbstractContextManager[DuckDBStore]],
        catalog: ResearchCatalog,
        lake_root: Path,
        input_root: Path,
        clock: Callable[[], datetime],
        max_task_seconds: int = 3600,
        template_binding: ExperimentTemplateBinding | None = None,
    ) -> None:
        self.store, self.definitions = store, definitions
        self.profiles = tuple(
            ExperimentSourceProfile.model_validate(p.model_dump(mode="python")) for p in profiles
        )
        if len(self.profiles) > 100 or len(
            {(p.source_key, p.source_version) for p in self.profiles}
        ) != len(self.profiles):
            raise ValueError("experiment source profiles exceed their budget or repeat")
        identity = input_root.lstat()
        if (
            not stat.S_ISDIR(identity.st_mode)
            or stat.S_IMODE(identity.st_mode) != 0o700
            or identity.st_uid != os.geteuid()
        ):
            raise PermissionError("experiment input root must be private and owned")
        if type(max_task_seconds) is not int or not 1 <= max_task_seconds <= 86400:
            raise ValueError("experiment task deadline policy is invalid")
        self.phase_provider, self.metadata_store_factory = phase_provider, metadata_store_factory
        self.catalog, self.lake_root, self.input_root = catalog, lake_root, input_root
        self._input_root_identity = (identity.st_dev, identity.st_ino)
        self.clock, self.max_task_seconds = clock, max_task_seconds
        self.template_binding = template_binding

    def template_baseline(
        self, owner: str, request: ExperimentSearchRequest
    ) -> ExperimentTemplateBaseline | None:
        if request.template is None:
            return None
        if self.template_binding is None:
            raise ValueError("original template source is not installed")
        return self.template_binding.baseline(owner=owner, request=request)

    def profile(self, record: ExperimentFamilyRecord) -> ExperimentSourceProfile:
        matches = tuple(
            p
            for p in self.profiles
            if (p.source_key, p.source_version)
            == (record.request.base_config.source_key, record.request.base_config.source_version)
        )
        if len(matches) != 1 or not matches[0].phase_slice_available:
            raise ValueError("source cannot provide a protected phase slice")
        return matches[0]

    def _recover_publication(
        self, reservation: ExperimentPreparationReservation, record: ExperimentFamilyRecord
    ) -> tuple[FrozenPortfolioInput, PublishedPortfolioInput]:
        path = Path(reservation.source_path)
        if (
            path.parent.parent != self.input_root
            or path.name != "input.duckdb"
            or len(path.parent.name) != 32
        ):
            raise PermissionError("reserved input is outside the installed private root")
        parent = path.parent.lstat()
        if (
            not stat.S_ISDIR(parent.st_mode)
            or parent.st_uid != os.geteuid()
            or stat.S_IMODE(parent.st_mode) != 0o700
        ):
            raise PermissionError("reserved input directory is not private and owned")
        _input_digest(path)
        with duckdb.connect(
            str(path),
            read_only=True,
            config={"enable_external_access": False, "threads": 1, "temp_directory": ""},
        ) as connection:
            frozen = read_portfolio_input_table(connection, require_primary_key=True)
        if (
            frozen.input_hash != reservation.input_hash
            or frozen.config != record.actual_configurations[reservation.index]
        ):
            raise ValueError("original interrupted input differs from its reservation")
        audit = DataAuditRun.create(
            as_of_date=record.registered_at.date(),
            range_start=frozen.config.start_date,
            range_end=frozen.config.end_date,
            observed_at=record.registered_at,
            rule_set_version=f"portfolio-source/v1:{frozen.input_hash}",
        )
        snapshot = DatasetSnapshot.create(
            strategy_name="portfolio_backtest",
            manifest_id=frozen.input_hash,
            as_of_time=record.registered_at,
            code_commit=frozen.request.producer_commit,
            origin="trusted-portfolio-producer",
            created_at=record.registered_at,
        )
        with self.metadata_store_factory() as metadata:
            binding = metadata.get_dataset_snapshot_binding(snapshot.snapshot_id)
            if binding is None:
                raise ValueError(
                    "interrupted preparation is incomplete; its owned reservation is retained"
                )
            request = ResearchGateRequest(
                mode="formal",
                strategy_name="portfolio_backtest",
                start_date=frozen.config.start_date,
                end_date=frozen.config.end_date,
                code_commit=frozen.request.producer_commit,
                audit_run_id=audit.audit_run_id,
                dataset_snapshot_id=snapshot.snapshot_id,
                dataset_binding_hash=binding.binding_hash,
            )
            with ResearchExecutionSession(binding=binding, lake_root=self.lake_root) as session:
                verify_bound_portfolio_input(metadata, request, session)
            decision = require_portfolio_gate(metadata, request, binding_verified=True)
        published = PublishedPortfolioInput(
            config_hash=frozen.config.config_hash,
            input_hash=frozen.input_hash,
            identity=DatasetSnapshotIdentity(
                snapshot_id=snapshot.snapshot_id,
                binding_hash=binding.binding_hash,
                audit_run_id=audit.audit_run_id,
            ),
            gate_decision=decision,
        )
        return frozen, published

    def __call__(
        self, record: ExperimentFamilyRecord, *, grant: ExperimentOuterGrant | None = None
    ) -> ExperimentFamilyRecord:
        root = self.input_root.lstat()
        if (
            not stat.S_ISDIR(root.st_mode)
            or stat.S_IMODE(root.st_mode) != 0o700
            or root.st_uid != os.geteuid()
            or (root.st_dev, root.st_ino) != self._input_root_identity
        ):
            raise PermissionError("installed private input root changed")
        checked = self.store.get_family(record.owner, record.family_id)
        if checked != record:
            raise ValueError("formal family preparation changed")
        if record.state == "ready":
            return record
        if record.state == "cancelled":
            raise ValueError("original preparing family was cancelled")
        profile = self.profile(record)
        now = self.clock()
        if record.phase == "search":
            if grant is not None or self.store.policy().version != record.policy.version:
                raise ValueError("holdout policy changed before search admission")
            cutoff = min(
                profile.latest_complete,
                holdout_cutoff(now, record.policy.months, calendar=profile.calendar.dates),
            )
            validate_experiment_dates(
                record.request, calendar=profile.calendar.dates, latest_complete=cutoff
            )
        else:
            if (
                grant is None
                or grant.owner != record.owner
                or grant.request_id != record.request_id
                or "experiment-outer:" + grant.grant_id != record.family_id
                or grant not in self.store.list_outer_grants(record.owner)
            ):
                raise PermissionError("outer source read has no exact persisted grant")
            if grant.source_identity != profile.source_identity:
                raise PermissionError("outer source identity differs from the admitted grant")
        window = DateRange(
            start_date=record.actual_configurations[0].start_date,
            end_date=record.actual_configurations[0].end_date,
        )
        if (
            window.start_date < profile.coverage.start_date
            or window.end_date > profile.coverage.end_date
        ):
            raise ValueError("phase exceeds actual source coverage")
        read = ExperimentPhaseRead(
            owner=record.owner,
            family_id=record.family_id,
            source_identity=profile.source_identity,
            source_key=profile.source_key,
            source_version=profile.source_version,
            phase=record.phase,
            window=window,
            outer_grant_id=None if grant is None else grant.grant_id,
        )
        if record.template_baseline is not None:
            if self.template_binding is None:
                raise ValueError("original template preparation is unavailable")
            return self.template_binding.prepare_family(self, record, read=read, profile=profile)
        children = []
        input_bytes = 0
        source = None
        for index, cfg in enumerate(record.actual_configurations):
            previous = self.store.preparation(record.owner, record.family_id, index)
            if previous is not None:
                if previous.source_identity != profile.source_identity:
                    raise ValueError("original protected source identity changed")
                identity, digest = _input_digest(Path(previous.source_path))
                if (identity, digest) != (previous.file_identity, previous.file_sha256):
                    raise ValueError("original prepared input changed")
                prepared = previous.prepared
                input_bytes += identity[2]
            else:
                reservation = self.store.preparation_reservation(
                    record.owner, record.family_id, index
                )
                recovered = reservation is not None and Path(reservation.source_path).exists()
                if (
                    reservation is not None
                    and reservation.source_identity != profile.source_identity
                ):
                    raise PermissionError("reserved original source identity changed")
                if not recovered and source is None:
                    source = PortfolioSourceData.model_validate(
                        self.phase_provider(read).model_dump(mode="python")
                    )
                    expected = tuple(
                        d
                        for d in profile.calendar.dates
                        if window.start_date <= d <= window.end_date
                    )
                    if (
                        (
                            source.source_key,
                            source.source_version,
                            source.sources,
                            source.template.producer_commit,
                        )
                        != (
                            profile.source_key,
                            profile.source_version,
                            profile.sources,
                            profile.producer_commit,
                        )
                        or tuple(d.trade_date for d in source.template.days) != expected
                        or source.template.calendar != profile.calendar
                    ):
                        raise PermissionError(
                            "phase provider returned another source or unbounded rows"
                        )
                    first = profile.calendar.dates.index(window.start_date)
                    if first == 0:
                        raise ValueError("phase has no actual previous trading date")
                    baseline = profile.calendar.dates[first - 1]
                    if any(
                        d < baseline or d > window.end_date
                        for rows in source.benchmarks.values()
                        for d, _ in rows
                    ):
                        raise PermissionError("phase provider exposed out-of-phase prices")
                    # A newly materialized private slice has its own honest generation;
                    # source prices and their authority hashes remain the original inputs.
                    template = source.template.model_copy(
                        update={
                            "input_generation_id": canonical_sha256(
                                {
                                    "contract": "private-experiment-phase/v1",
                                    "source": profile.source_identity,
                                    "family": record.family_id,
                                    "phase": record.phase,
                                    "window": window,
                                }
                            )
                        }
                    )
                    source = PortfolioSourceData.model_validate(
                        source.model_dump(mode="python")
                        | {"template": template, "material_hash": None}
                    )
                if recovered:
                    frozen, published = self._recover_publication(reservation, record)
                    directory = Path(reservation.source_path).parent
                else:
                    frozen = freeze_portfolio_config(source, cfg)
                    if reservation is None:
                        directory = self.input_root / uuid4().hex
                        reservation = self.store.reserve_preparation(
                            ExperimentPreparationReservation(
                                owner=record.owner,
                                family_id=record.family_id,
                                index=index,
                                source_identity=profile.source_identity,
                                source_path=str(directory / "input.duckdb"),
                                input_hash=frozen.input_hash,
                                created_at=record.registered_at,
                            )
                        )
                    else:
                        directory = Path(reservation.source_path).parent
                        if reservation.input_hash != frozen.input_hash:
                            raise ValueError("reserved original input changed")
                    directory.mkdir(mode=0o700, exist_ok=True)
                    with self.metadata_store_factory() as metadata:
                        published = publish_portfolio_input(
                            frozen,
                            metadata_store=metadata,
                            source_path=directory / "input.duckdb",
                            catalog=self.catalog,
                            lake_root=self.lake_root,
                            now=record.registered_at,
                        )
                prepared = build_portfolio_plan(
                    frozen,
                    published,
                    definitions=self.definitions,
                    protocol=record.request.protocol,
                    now=record.registered_at,
                    deadline=record.registered_at + timedelta(seconds=self.max_task_seconds),
                    random_seed=record.request.seed,
                    family_id=record.family_id,
                    hypothesis_variant=f"configuration-{index}",
                )
                if (
                    prepared.registration.logical_id != "portfolio_backtest"
                    or prepared.registration.version != 1
                ):
                    raise PermissionError("formal source requires exact portfolio_backtest@1")
                identity, digest = _input_digest(directory / "input.duckdb")
                input_bytes += identity[2]
                if input_bytes > MAX_FAMILY_INPUT_BYTES:
                    raise ValueError("complete family input exceeds 512 MiB")
                self.store.save_preparation(
                    ExperimentPreparationReceipt(
                        owner=record.owner,
                        family_id=record.family_id,
                        index=index,
                        source_identity=profile.source_identity,
                        source_path=str(directory / "input.duckdb"),
                        file_identity=identity,
                        file_sha256=digest,
                        prepared=prepared,
                    )
                )
            if input_bytes > MAX_FAMILY_INPUT_BYTES:
                raise ValueError("complete family input exceeds 512 MiB")
            envelope = LabCommandEnvelope(
                request_id=LabCommandSubmissionFacade._request_id(
                    stable_experiment_interaction(record.owner, record.request_id, index)
                ),
                command=prepared.submission(
                    job_id=stable_experiment_job(record.owner, record.request_id, index)
                ).command,
            )
            intent = LabCommandSubmissionFacade._experiment_submission_intent(envelope)
            assert intent is not None
            children.append(
                ExperimentChildRegistration(
                    config=cfg,
                    plan=prepared.formal_plan,
                    intent=intent,
                    published=prepared.published,
                )
            )
        return self.store.register_family_submission(
            owner=record.owner, request_id=record.request_id, children=tuple(children)
        )


class ExperimentCommandWriter:
    def __init__(
        self,
        *,
        store: ExperimentPlatformStore,
        commands: LabCommandSubmissionFacade,
        prepare: ExperimentFamilyPreparer,
        results: PortfolioResultReader | None = None,
        template_results: StrategyTemplateSealedResultReader | None = None,
        enabled: bool = False,
        owners: frozenset[str] = frozenset(),
        administrators: frozenset[str] = frozenset(),
        private_authority: ExperimentPrivateResultAuthority | None = None,
    ) -> None:
        if not administrators <= owners:
            raise ValueError("holdout administrators must have experiment permission")
        self.store, self.commands, self.prepare, self.results = store, commands, prepare, results
        self.enabled, self.owners, self.administrators = enabled, owners, administrators
        self.private_authority = private_authority
        self.template_results = template_results

    def _owner(self, command: ExperimentCommand) -> None:
        if command.actor_id not in self.owners:
            raise PermissionError("experiment owner is not authorized")

    def _prepare_admitted(
        self,
        command: RegisterExperimentFamily | UnsealExperimentOuterTest,
        record: ExperimentFamilyRecord,
        *,
        grant: ExperimentOuterGrant | None = None,
    ) -> ExperimentFamilyRecord:
        try:
            return self.prepare(record, grant=grant)
        except PermissionError:
            raise
        except (OSError, RuntimeError) as exc:
            actual = self.store.get_request(command.actor_id, UUID(command.command_id))
            if actual is None or actual.model_dump(exclude={"state"}) != record.model_dump(
                exclude={"state"}
            ):
                raise
            if isinstance(command, RegisterExperimentFamily):
                if grant is not None or actual.body_hash != canonical_sha256(command):
                    raise
            elif (
                grant is None
                or grant not in self.store.list_outer_grants(command.actor_id)
                or actual.body_hash != canonical_sha256(grant)
                or (grant.family_id, grant.experiment_id, grant.body_hash, grant.result_hash)
                != (
                    command.family_id,
                    command.experiment_id,
                    canonical_sha256(command),
                    command.result_hash,
                )
            ):
                raise
            raise ExperimentPreparationUncertainError(
                "original admitted experiment preparation needs recovery"
            ) from exc

    def freeze(self, command: ExperimentCommand) -> JsonValue:
        command = TypeAdapter(ExperimentCommand).validate_python(command)
        self._owner(command)
        original = self.store.get_request(command.actor_id, UUID(command.command_id))
        if isinstance(command, RegisterExperimentFamily):
            if original is None and not self.enabled:
                raise PermissionError("formal experiment writes are disabled")
            record = self.store.begin_request(
                owner=command.actor_id,
                request_id=UUID(command.command_id),
                body_hash=canonical_sha256(command),
                request=command.request,
                registered_at=self.prepare.clock(),
                template_baseline=None
                if original is not None
                else self.prepare.template_baseline(command.actor_id, command.request),
            )
            if record.state != "cancelled":
                record = self._prepare_admitted(command, record)
            effect = ExperimentEffect(
                command_hash=canonical_sha256(command),
                owner=command.actor_id,
                action=command.kind,
                family_id=record.family_id,
            )
        elif isinstance(command, UnsealExperimentOuterTest):
            previous = next(
                (
                    g
                    for g in self.store.list_outer_grants(command.actor_id)
                    if g.request_id == UUID(command.command_id)
                ),
                None,
            )
            if previous is None and not self.enabled:
                raise PermissionError("formal experiment writes are disabled")
            if previous is None:
                parent = self.store.get_family(command.actor_id, command.family_id)
                attempts = self.store.registry.list_family_attempts(command.family_id)
                attempt = next(
                    (a for a in attempts if a.spec.experiment_id == command.experiment_id), None
                )
                if attempt is None or self.results is None or self.private_authority is None:
                    raise ValueError("selected complete result is unavailable")
                with self.store.registry._connect() as connection:
                    row = connection.execute(
                        "SELECT payload_json FROM experiment_child_admission WHERE experiment_id=?",
                        (command.experiment_id,),
                    ).fetchone()
                if row is None:
                    raise ValueError("selected original child is unavailable")
                from rquant.experiment_platform import ExperimentChildAdmission

                child = ExperimentChildAdmission.model_validate_json(row[0])
                if parent.template_baseline is not None:
                    if self.template_results is None:
                        raise ValueError("original template result reader is unavailable")
                    result = self.template_results.read_private(
                        child.job_id,
                        expected_result_hash=command.result_hash,
                        private_owner=command.actor_id,
                        private_authority=self.private_authority,
                    )
                    job = self.commands.reader.get_job(child.job_id)
                    preparation = self.private_authority.authorize(job, command.actor_id)
                    if not isinstance(preparation.prepared, PreparedExperimentTemplate) or (
                        preparation.configuration not in parent.actual_configurations
                    ):
                        raise ValueError("selected template lacks its full exact sealed result")
                else:
                    result = self.results.read(
                        child.job_id,
                        expected_result_hash=command.result_hash,
                        private_owner=command.actor_id,
                        private_authority=self.private_authority,
                    )
                    if (
                        result.bundle.result.status != "complete"
                        or result.bundle.frozen.config not in parent.actual_configurations
                    ):
                        raise ValueError("selected candidate has no full exact sealed result")
                profile = self.prepare.profile(parent)
                policy = self.store.policy()
                cutoff = min(
                    profile.latest_complete,
                    holdout_cutoff(
                        self.prepare.clock(), policy.months, calendar=profile.calendar.dates
                    ),
                )
                if parent.request.protocol.frozen_outer_test_range.end_date > cutoff:
                    raise ValueError("outer interval exceeds the current policy cutoff")
            grant = previous or self.store.admit_outer(
                owner=command.actor_id,
                family_id=command.family_id,
                request_id=UUID(command.command_id),
                experiment_id=command.experiment_id,
                now=self.prepare.clock(),
                body_hash=canonical_sha256(command),
                result_hash=command.result_hash,
                source_identity=profile.source_identity,
                expected_policy_version=policy.version,
                cutoff=cutoff,
            )
            if (grant.family_id, grant.experiment_id, grant.body_hash, grant.result_hash) != (
                command.family_id,
                command.experiment_id,
                canonical_sha256(command),
                command.result_hash,
            ):
                raise ValueError("original outer request content conflicts")
            record = self._prepare_admitted(
                command, self.store.begin_outer_request(grant), grant=grant
            )
            effect = ExperimentEffect(
                command_hash=canonical_sha256(command),
                owner=command.actor_id,
                action=command.kind,
                family_id=record.family_id,
                grant_id=grant.grant_id,
            )
        else:
            with self.store.registry._connect() as connection:
                previous_operation = connection.execute(
                    "SELECT owner FROM experiment_platform_receipt WHERE request_id=?",
                    (command.command_id,),
                ).fetchone()
            if not self.enabled and previous_operation is None:
                raise PermissionError("formal experiment writes are disabled")
            if isinstance(command, CancelExperimentFamily):
                self.store.cancel_family(
                    owner=command.actor_id,
                    family_id=command.family_id,
                    request_id=UUID(command.command_id),
                    now=self.prepare.clock(),
                )
                effect = ExperimentEffect(
                    command_hash=canonical_sha256(command),
                    owner=command.actor_id,
                    action=command.kind,
                    family_id=command.family_id,
                )
            elif isinstance(command, SetExperimentNote):
                note = self.store.set_note(
                    owner=command.actor_id,
                    family_id=command.family_id,
                    request_id=UUID(command.command_id),
                    expected_version=command.expected_version,
                    text=command.text,
                    now=self.prepare.clock(),
                )
                effect = ExperimentEffect(
                    command_hash=canonical_sha256(command),
                    owner=command.actor_id,
                    action=command.kind,
                    family_id=command.family_id,
                    version=note.version,
                )
            else:
                policy = self.store.set_policy(
                    owner=command.actor_id,
                    request_id=UUID(command.command_id),
                    months=command.months,
                    expected_version=command.expected_version,
                    now=self.prepare.clock(),
                    administrators=self.administrators,
                )
                effect = ExperimentEffect(
                    command_hash=canonical_sha256(command),
                    owner=command.actor_id,
                    action=command.kind,
                    version=policy.version,
                )
        return effect.model_dump(mode="json")

    def submit(self, command: ExperimentCommand, marker: JsonValue) -> JsonValue:
        self._owner(command)
        effect = ExperimentEffect.model_validate(marker)
        if (effect.owner, effect.action, effect.command_hash) != (
            command.actor_id,
            command.kind,
            canonical_sha256(command),
        ):
            raise PermissionError("original experiment effect binding changed")
        jobs: tuple[UUID, ...] = ()
        count = 0
        if isinstance(command, (RegisterExperimentFamily, UnsealExperimentOuterTest)):
            if effect.family_id is None:
                raise ValueError("original formal family is missing")
            record = self.store.get_family(command.actor_id, effect.family_id)
            if record.state == "cancelled":
                count, status = len(record.actual_configurations), "cancelled"
            else:
                if record.state != "ready":
                    raise ValueError("complete formal family is not ready")
                ids = []
                for index in range(len(record.actual_configurations)):
                    job_id = stable_experiment_job(record.owner, record.request_id, index)
                    intent = self.store.registry.get_submission_intent_for_job(job_id)
                    if intent is None:
                        raise ValueError("complete formal child intent is missing")
                    child = self.store.child(job_id)
                    # Keep planned identities even when cancellation preceded publication.
                    ids.append(job_id)
                    if child is not None and child.cancel_state == "before_publication":
                        continue
                    envelope = LabCommandEnvelope.model_validate_json(intent.envelope_json)
                    result = self.commands.submit_create(
                        envelope.command,
                        interaction_key=stable_experiment_interaction(
                            record.owner, record.request_id, index
                        ),
                    )
                    if not isinstance(result, CommandSubmissionReceipt):
                        raise ValueError("original child publication needs recovery")
                jobs, count = tuple(ids), len(record.actual_configurations)
                status = "outer_admitted" if record.phase == "outer" else "registered"
        elif isinstance(command, CancelExperimentFamily):
            self.commands.recover_private_experiment_cancellations(observed_at=self.prepare.clock())
            record = self.store.get_family(command.actor_id, command.family_id)
            count = len(record.actual_configurations)
            with self.store.registry._connect() as connection:
                rows = connection.execute(
                    "SELECT payload_json FROM experiment_child_admission "
                    "WHERE owner=? AND hypothesis_family=?",
                    (command.actor_id, command.family_id),
                ).fetchall()
            children = tuple(ExperimentChildAdmission.model_validate_json(row[0]) for row in rows)
            if not children and record.state == "cancelled":
                status = "cancelled"
            elif len(children) != count or any(
                (child.owner, child.family_id) != (command.actor_id, command.family_id)
                for child in children
            ):
                raise ValueError("complete cancellation receipt lost its original children")
            elif any(child.cancel_state in ("none", "pending") for child in children):
                status = "cancellation_pending"
            elif all(child.cancel_state == "already_completed" for child in children):
                status = "already_completed"
            elif any(
                child.cancel_state in ("before_publication", "confirmed") for child in children
            ):
                status = "cancelled"
            else:
                status = "already_finished"
        elif isinstance(command, SetExperimentNote):
            status = "note_saved"
        else:
            status = "policy_saved"
        return ExperimentCommandResult(
            command_id=UUID(command.command_id),
            owner=command.actor_id,
            action=command.kind,
            family_id=effect.family_id,
            job_ids=jobs,
            planned_count=count,
            status=status,
            version=effect.version,
        ).model_dump(mode="json")

    def recover(self, command: ExperimentCommand, marker: JsonValue) -> JsonValue | None:
        return self.submit(command, marker)


@dataclass(frozen=True)
class ExperimentRuntimeBinding:
    preparer: ExperimentFamilyPreparer
    command_backend: ExperimentCommandWriter
    lifecycle: ExperimentLifecycleCoordinator
    private_projection: ExperimentPrivateProjectionReader
    promotion_reader: PromotionsSourceReader
    web_service: ExperimentWebService


def bind_experiment_platform(
    *,
    store: ExperimentPlatformStore,
    commands: LabCommandSubmissionFacade,
    prepare: ExperimentFamilyPreparer,
    results: PortfolioResultReader,
    default_config: PortfolioBacktestConfig,
    gateway: LabControlGateway | None = None,
    owners: frozenset[str] = frozenset(),
    administrators: frozenset[str] = frozenset(),
    enabled: bool = False,
    independence_resolver: Callable[
        [ExperimentFamilyFact, tuple[ExperimentAttemptFact, ...]],
        ExperimentIndependenceEvidence | None,
    ]
    | None = None,
) -> ExperimentRuntimeBinding:
    """Compose original workers, source, PageControl and Web; install no schema or process."""
    from rquant.experiment_platform_evidence import ExperimentEvidencePublisher
    from rquant.experiment_platform_projection import ExperimentPrivateProjectionReader
    from rquant.experiment_registry import ExperimentRegistryReadonlyReader
    from rquant.lab_job_center import ExperimentLifecycleCoordinator
    from rquant.promotions_serving_authority import PromotionsSourceReader
    from rquant.web.experiment_platform_service import ExperimentWebService

    if (
        prepare.store is not store
        or commands.experiment_registry is not store.registry
        or commands.definition_registry is not prepare.definitions
        or results.reader is not commands.reader
    ):
        raise ValueError("experiment composition must use the exact original authorities")
    store.policy()
    readonly = ExperimentRegistryReadonlyReader(
        store.registry.path, managed_trust_root=store.registry._path_authority._managed_trust_root
    )
    projection = ExperimentPrivateProjectionReader(
        registry=readonly, jobs=commands.reader, owners=owners
    )
    template_results = None
    if prepare.template_binding is not None:
        from rquant.lab_artifact_preview import ArtifactPreviewReader

        template_results = StrategyTemplateSealedResultReader(
            reader=commands.reader,
            artifact_reader=ArtifactPreviewReader(
                reader=commands.reader, artifact_root=results.previews.artifact_root
            ),
        )
    writer = ExperimentCommandWriter(
        store=store,
        commands=commands,
        prepare=prepare,
        results=results,
        template_results=template_results,
        enabled=enabled,
        owners=owners,
        administrators=administrators,
        private_authority=projection.authority,
    )
    evidence = ExperimentEvidencePublisher(
        store=store,
        projection=projection,
        results=results,
        template_results=template_results,
        independence_resolver=independence_resolver,
    )
    return ExperimentRuntimeBinding(
        preparer=prepare,
        command_backend=writer,
        lifecycle=ExperimentLifecycleCoordinator(commands, evidence_sink=evidence),
        private_projection=projection,
        promotion_reader=PromotionsSourceReader(
            registry=readonly, include_experiments=True, private_experiment_reader=projection
        ),
        web_service=ExperimentWebService(
            results=results,
            template_results=template_results,
            private_authority=projection.authority,
            gateway=gateway,
            profiles=prepare.profiles,
            default_config=default_config,
            owners=owners,
            administrators=administrators,
            enabled=enabled,
            template_available=prepare.template_binding is not None
            and prepare.template_binding.phase_provider is not None,
        ),
    )
