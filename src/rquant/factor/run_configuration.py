"""Explicit local configuration and sealed prepared-source files; never read .env."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, Field, model_validator

from rquant.data_metadata import DatasetSnapshot, DatasetSnapshotBinding
from rquant.factor.daily_feature_source import FactorDailyFeatureSource
from rquant.factor.job_ledger import FactorEvaluationJobLedger, FactorLedgerIdentity
from rquant.factor.member_archive import (
    FactorMemberArchiveReference,
    _bytes,
    _check_identities,
    _publish_bytes,
    _read_file,
    _sha,
)
from rquant.factor.neutralization_context import FactorNeutralizationContext
from rquant.factor.registry import FactorRegistryIdentity
from rquant.factor.result_artifact import _open_private_root, _root_path
from rquant.factor.run_request import RUN_IMMUTABLE
from rquant.factor.source_prepare import FactorPreparedStreamSource
from rquant.factor.universe import UniverseSelection
from rquant.strict_json import strict_canonical_json_loads

_MAX_SOURCE_BYTES = 16 * 1024 * 1024
_MAX_CONFIG_BYTES = 256 * 1024


class FactorRunFileReference(BaseModel):
    model_config = RUN_IMMUTABLE

    kind: str = Field(
        pattern=r"^factor-((prepared-source|run-configuration|neutralization-context)-v1|daily-feature-source-v[12])$"
    )
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    filename: str
    byte_count: int = Field(gt=0, le=_MAX_SOURCE_BYTES)

    @model_validator(mode="after")
    def _name(self) -> FactorRunFileReference:
        if self.filename != f"{self.kind}-{self.sha256}.json":
            raise ValueError("配置文件名与摘要不符")
        return self


class FactorRunMemberBinding(BaseModel):
    model_config = RUN_IMMUTABLE

    selection: UniverseSelection
    archive: FactorMemberArchiveReference


class FactorRunConfiguration(BaseModel):
    model_config = RUN_IMMUTABLE

    schema_version: int = Field(default=1, ge=1, le=1)
    enabled: bool = False
    factor_run_users: tuple[str, ...] = Field(default=(), max_length=16)
    registry_identity: FactorRegistryIdentity
    ledger_identity: FactorLedgerIdentity
    prepared_source: FactorRunFileReference
    lake_root: Path
    member_root: Path
    artifact_root: Path
    members: tuple[FactorRunMemberBinding, ...] = Field(default=(), max_length=4)
    code_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    deadline_seconds: int = Field(default=3600, ge=60, le=86400)
    neutralization_context: FactorRunFileReference | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    daily_feature_source: FactorRunFileReference | None = Field(
        default=None, exclude_if=lambda v: v is None
    )

    @model_validator(mode="after")
    def _fixed_paths(self) -> FactorRunConfiguration:
        if self.prepared_source.kind != "factor-prepared-source-v1":
            raise ValueError("配置缺少来源包")
        if (
            self.neutralization_context is not None
            and self.neutralization_context.kind != "factor-neutralization-context-v1"
        ):
            raise ValueError("配置缺少中性化上下文包")
        if self.daily_feature_source is not None and self.daily_feature_source.kind not in (
            "factor-daily-feature-source-v1",
            "factor-daily-feature-source-v2",
        ):
            raise ValueError("配置缺少库存日线事实包")
        for path in (
            self.lake_root,
            self.member_root,
            self.artifact_root,
            Path(self.registry_identity.path),
            Path(self.ledger_identity.path),
        ):
            _root_path(path)
        if len({item.selection for item in self.members}) != len(self.members):
            raise ValueError("股票池配置重复")
        if len(set(self.factor_run_users)) != len(self.factor_run_users) or any(
            not user or user.strip() != user or len(user) > 128 or not user.isprintable()
            for user in self.factor_run_users
        ):
            raise ValueError("运行用户配置不正确")
        return self


def _save(root: Path, model: BaseModel, kind: str, limit: int) -> FactorRunFileReference:
    root = _root_path(root)
    descriptor = _open_private_root(root)
    try:
        data = _bytes(model)
        reference = FactorRunFileReference(
            kind=kind, sha256=_sha(data), filename=f"{kind}-{_sha(data)}.json", byte_count=len(data)
        )
        _publish_bytes(root, descriptor, reference.filename, data, limit)
        return reference
    finally:
        os.close(descriptor)


def save_factor_prepared_source(
    root: Path,
    prepared: FactorPreparedStreamSource,
) -> FactorRunFileReference:
    return _save(
        root,
        FactorPreparedStreamSource.model_validate(prepared),
        "factor-prepared-source-v1",
        _MAX_SOURCE_BYTES,
    )


def save_factor_run_configuration(
    root: Path,
    configuration: FactorRunConfiguration,
) -> FactorRunFileReference:
    return _save(
        root,
        FactorRunConfiguration.model_validate(configuration),
        "factor-run-configuration-v1",
        _MAX_CONFIG_BYTES,
    )


def save_factor_neutralization_context(
    root: Path, context: FactorNeutralizationContext
) -> FactorRunFileReference:
    return _save(
        root,
        FactorNeutralizationContext.model_validate(context),
        "factor-neutralization-context-v1",
        _MAX_SOURCE_BYTES,
    )


def save_factor_daily_feature_source(
    root: Path, source: FactorDailyFeatureSource
) -> FactorRunFileReference:
    return _save(
        root,
        FactorDailyFeatureSource.model_validate(source),
        f"factor-daily-feature-source-v{source.schema_version}",
        _MAX_SOURCE_BYTES,
    )


def _load(
    descriptor: int,
    reference: FactorRunFileReference,
    model: type[BaseModel],
    limit: int,
) -> tuple[BaseModel, tuple[int, ...]]:
    data, identity = _read_file(descriptor, reference.filename, limit, reference.sha256)
    strict_canonical_json_loads(data)
    parsed = model.model_validate_json(data)
    if len(data) != reference.byte_count or _bytes(parsed) != data:
        raise ValueError("配置文件内容不符")
    return parsed, identity


class PreparedFactorMetadata:
    """The original ready records are supplied by a verified prepared-source package."""

    def __init__(self, source: FactorPreparedStreamSource) -> None:
        self.source = source

    def get_dataset_snapshot(self, snapshot_id: str) -> DatasetSnapshot | None:
        return self.source.snapshot if snapshot_id == self.source.snapshot.snapshot_id else None

    def get_dataset_snapshot_binding(self, snapshot_id: str) -> DatasetSnapshotBinding | None:
        return self.source.binding if snapshot_id == self.source.snapshot.snapshot_id else None


class LoadedFactorRunConfiguration:
    def __init__(
        self,
        root: Path,
        descriptor: int,
        identities: dict[str, tuple[int, ...]],
        configuration: FactorRunConfiguration,
        source: FactorPreparedStreamSource,
        context: FactorNeutralizationContext | None = None,
        daily_features: FactorDailyFeatureSource | None = None,
    ) -> None:
        self.root, self.descriptor, self.identities = root, descriptor, identities
        self.configuration, self.source = configuration, source
        self.metadata = PreparedFactorMetadata(source)
        self.context = context
        self.daily_features = daily_features

    def recheck(self) -> None:
        _check_identities(self.root, self.descriptor, self.identities)

    def open_ledger(self, *, clock: Callable[[], datetime]) -> FactorEvaluationJobLedger:
        return FactorEvaluationJobLedger.open_existing(
            self.configuration.ledger_identity,
            clock=clock,
        )


@contextmanager
def open_factor_run_configuration(
    root: Path,
    reference: FactorRunFileReference,
) -> Iterator[LoadedFactorRunConfiguration]:
    root = _root_path(root)
    reference = FactorRunFileReference.model_validate(reference)
    if reference.kind != "factor-run-configuration-v1":
        raise ValueError("缺少运行配置")
    descriptor = _open_private_root(root)
    try:
        config, config_identity = _load(
            descriptor, reference, FactorRunConfiguration, _MAX_CONFIG_BYTES
        )
        assert isinstance(config, FactorRunConfiguration)
        source, source_identity = _load(
            descriptor, config.prepared_source, FactorPreparedStreamSource, _MAX_SOURCE_BYTES
        )
        assert isinstance(source, FactorPreparedStreamSource)
        identities = {
            reference.filename: config_identity,
            config.prepared_source.filename: source_identity,
        }
        context = None
        if config.neutralization_context is not None:
            context, identity = _load(
                descriptor,
                config.neutralization_context,
                FactorNeutralizationContext,
                _MAX_SOURCE_BYTES,
            )
            assert isinstance(context, FactorNeutralizationContext)
            context.require_prepared(source)
            identities[config.neutralization_context.filename] = identity
        daily_features = None
        if config.daily_feature_source is not None:
            daily_features, identity = _load(
                descriptor, config.daily_feature_source, FactorDailyFeatureSource, _MAX_SOURCE_BYTES
            )
            assert isinstance(daily_features, FactorDailyFeatureSource)
            if (
                config.daily_feature_source.kind
                != f"factor-daily-feature-source-v{daily_features.schema_version}"
            ):
                raise ValueError("daily source reference version differs from its mode")
            daily_features.require_prepared(source)
            identities[config.daily_feature_source.filename] = identity
        loaded = LoadedFactorRunConfiguration(
            root,
            descriptor,
            identities,
            config,
            source,
            context,
            daily_features,
        )
        loaded.recheck()
        yield loaded
        loaded.recheck()
    finally:
        os.close(descriptor)


def run_configured_factor_worker(
    root: Path,
    reference: FactorRunFileReference,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> BaseModel:
    from rquant.factor.job_worker import run_one_factor_job

    with open_factor_run_configuration(root, reference) as loaded:
        if not loaded.configuration.enabled:
            raise PermissionError("运行入口尚未开启")
        loaded.recheck()
        ledger = loaded.open_ledger(clock=clock)
        config = loaded.configuration
        return run_one_factor_job(
            ledger,
            metadata_store=loaded.metadata,
            lake_root=config.lake_root,
            artifact_root=config.artifact_root,
            member_root=config.member_root,
            runner_now=clock,
        )
