"""Complete native minute results from the original accepted and sealed Lab graph."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Self
from uuid import UUID

import pandas as pd
from pydantic import computed_field, model_validator

from rquant.experiment_registry import FormalExperimentPlan
from rquant.lab_artifact_preview import (
    ArtifactCompleteTableBudget, ArtifactCompleteTables, ArtifactPreviewIntegrityError,
    ArtifactPreviewReader, ArtifactPreviewUnavailableError,
)
from rquant.lab_artifacts import LabJobArtifactManifest
from rquant.lab_finalizer import LabFinalizerMetrics
from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandEnvelope
from rquant.lab_jobs import COMPLETE_RESULT_CONTRACT_VERSION, LabJobReader, ShardStatus
from rquant.minute_backtest_contracts import MinuteReplayModel, Sha256
from rquant.minute_backtest_formal_adapter import MinuteFormalParameters, MinuteFormalReplayAdapter, MinuteFormalReplayResult, minute_formal_adapter_registry
from rquant.minute_backtest_producer import MinuteReplayCatalog
from rquant.minute_backtest_runner import MinuteRuntimeReplayResult, minute_runtime_result_tables
from rquant.research_run_spec import ResearchRunSpec
from rquant.runtime_contracts import AwareUtcDatetime, canonical_sha256, normalize_aware_utc
from rquant.strict_json import canonical_json_bytes, strict_model_validate_json
from rquant.strategy_job_adapters import StrategyJobAdapterRegistry

MINUTE_RESULT_TABLE_NAMES = (
    "signals", "orders", "fills", "paper_queue", "account", "daily_valuations",
    "execution_profile", "replay_summary",
)


class MinuteSealedReplayIntegrityError(ArtifactPreviewIntegrityError):
    """The sealed bytes conflict with the accepted native minute authorities."""


class MinuteSealedReplayResult(MinuteReplayModel):
    kind: Literal["minute_runtime_replay"] = "minute_runtime_replay"
    job_id: UUID
    shard_id: UUID
    owner_id: str
    spec_hash: Sha256
    payload_hash: Sha256
    plan_hash: Sha256
    manifest_hash: Sha256
    complete_result_hash: Sha256
    completed_at: AwareUtcDatetime
    accepted_spec: ResearchRunSpec
    formal_plan: FormalExperimentPlan
    manifest: LabJobArtifactManifest
    result: MinuteFormalReplayResult

    @model_validator(mode="after")
    def sealed_binding(self) -> Self:
        experiment = self.accepted_spec.experiment
        if (self.job_id, self.spec_hash, self.plan_hash, self.manifest_hash, self.complete_result_hash) != (
            self.manifest.job_id, self.accepted_spec.spec_hash, self.manifest.plan_hash,
            self.manifest.manifest_hash, self.manifest.complete_result_hash):
            raise ValueError("minute sealed result differs from its accepted physical identity")
        if self.manifest.spec_hash != self.spec_hash or self.owner_id != self.result.publication.frozen.runtime.owner_id:
            raise ValueError("minute sealed result differs from accepted owner/spec")
        if experiment is None or experiment.formal_plan_id != self.formal_plan.plan_id or experiment.spec != self.formal_plan.spec:
            raise ValueError("minute sealed result differs from complete formal plan")
        return self

    @computed_field
    @property
    def full_input_hash(self) -> Sha256:
        return self.result.full_input_hash

    @computed_field
    @property
    def core_input_hash(self) -> Sha256:
        return self.result.core_input_hash

    @computed_field
    @property
    def seed_hash(self) -> Sha256:
        return self.result.seed_hash

    @computed_field
    @property
    def result_hash(self) -> Sha256:
        return canonical_sha256(self.result.model_dump(mode="json", exclude_computed_fields=True))

    @computed_field
    @property
    def source_kind(self) -> Literal["captured", "reconstructed"]:
        return self.result.publication.frozen.provenance.source_kind


class MinuteSealedReplayReader:
    def __init__(
        self,
        *,
        reader: LabJobReader,
        artifact_reader: ArtifactPreviewReader,
        submission_facade: LabCommandSubmissionFacade,
        catalog: MinuteReplayCatalog,
    ) -> None:
        if artifact_reader.reader is not reader or submission_facade.reader is not reader:
            raise ValueError("minute sealed reader requires one original Lab authority")
        if submission_facade.experiment_registry is None or submission_facade.definition_registry is None:
            raise ValueError("minute sealed reader requires original experiment and definition registries")
        self.reader = reader
        self.artifact_reader = artifact_reader
        self.submission_facade = submission_facade
        self.catalog = self._catalog_model().model_validate(catalog.model_dump(mode="python"))

    @staticmethod
    def _catalog_model() -> type[MinuteReplayCatalog]:
        return MinuteReplayCatalog

    @staticmethod
    def _parameter_model() -> type[MinuteFormalParameters]:
        return MinuteFormalParameters

    @staticmethod
    def _formal_result_model() -> type[MinuteFormalReplayResult]:
        return MinuteFormalReplayResult

    @staticmethod
    def _sealed_result_model() -> type[MinuteSealedReplayResult]:
        return MinuteSealedReplayResult

    @staticmethod
    def _result_tables(replay: MinuteRuntimeReplayResult) -> dict[str, pd.DataFrame]:
        return minute_runtime_result_tables(replay)

    def _adapter(self) -> MinuteFormalReplayAdapter:
        return MinuteFormalReplayAdapter(self.catalog)

    def _registry(self) -> StrategyJobAdapterRegistry:
        return minute_formal_adapter_registry(self.catalog)

    @classmethod
    def _complete_result(cls, tables: ArtifactCompleteTables) -> MinuteFormalReplayResult:
        observed = {item.parquet.table_name: item for item in tables.tables}
        if set(observed) != set(MINUTE_RESULT_TABLE_NAMES) or len(tables.tables) != len(MINUTE_RESULT_TABLE_NAMES):
            raise MinuteSealedReplayIntegrityError("minute sealed result requires exactly eight complete tables")
        summary = observed["replay_summary"]
        if len(summary.rows) != 1 or "payload" not in summary.parquet.columns:
            raise MinuteSealedReplayIntegrityError("minute sealed result has no unique complete summary")
        payload = summary.rows[0][summary.parquet.columns.index("payload")]
        if not isinstance(payload, str):
            raise MinuteSealedReplayIntegrityError("minute sealed complete payload is not JSON text")
        try:
            result = strict_model_validate_json(cls._formal_result_model(), payload)
        except Exception as exc:
            raise MinuteSealedReplayIntegrityError("minute sealed complete payload is invalid") from exc
        expected = cls._result_tables(result.replay)
        expected["replay_summary"]["full_input_hash"] = result.full_input_hash
        expected["replay_summary"]["core_input_hash"] = result.core_input_hash
        expected["replay_summary"]["seed_hash"] = result.seed_hash
        expected["replay_summary"]["payload"] = result.model_dump_json(exclude_computed_fields=True)
        for name, frame in expected.items():
            actual = observed[name]
            rows = tuple(tuple(row) for row in frame.itertuples(index=False, name=None))
            if (actual.parquet.columns, actual.parquet.row_count) != (tuple(frame.columns), len(frame)) or canonical_json_bytes(actual.rows) != canonical_json_bytes(rows):
                raise MinuteSealedReplayIntegrityError(f"minute sealed canonical table conflicts: {name}")
        return result

    def read(
        self,
        job_id: UUID,
        *,
        owner_id: str,
        native_id: str,
        native_version: int,
        as_of: datetime,
    ) -> MinuteSealedReplayResult | None:
        visible_at = normalize_aware_utc(as_of)
        authority = self.reader.get_artifact_preview_authority(job_id)
        if authority is None or authority.evidence.indexed_at > visible_at or authority.job.updated_at > visible_at:
            return None
        spec = authority.job.spec
        selected = self._parameter_model().model_validate({item.name: item.value for item in spec.parameters.arguments})
        if type(native_version) is not int or (owner_id, native_id, native_version) != (
            selected.owner_id, selected.native_strategy_id, selected.native_strategy_version):
            raise PermissionError("minute sealed result differs from requested owner/native version")
        adapter = self._adapter()
        parameters = adapter.parameters(spec)
        expected = adapter.expected(parameters)
        frozen = expected.frozen
        facade = self.submission_facade
        assert facade.experiment_registry is not None and facade.definition_registry is not None
        intent = facade.experiment_registry.get_submission_intent_for_job(job_id)
        if intent is None:
            raise MinuteSealedReplayIntegrityError("minute sealed job has no original accepted submission intent")
        try:
            envelope = strict_model_validate_json(LabCommandEnvelope, intent.envelope_json)
            if envelope.command.job_id != job_id or envelope.command.spec != spec:
                raise MinuteSealedReplayIntegrityError("minute sealed job differs from full accepted request")
            facade.validate_prepared_experiment_submission(envelope, observed_at=visible_at)
            native = facade.definition_registry.read_strategy_spec(frozen.native_registration.fingerprint, as_of=visible_at)
            if native != frozen.native_registration:
                raise MinuteSealedReplayIntegrityError("minute sealed native registration differs from original registry")
            assert spec.experiment is not None and spec.experiment.formal_plan_id is not None
            plan = facade.experiment_registry.resolve_formal_plan_by_id(spec.experiment.formal_plan_id, as_of=visible_at)
        except MinuteSealedReplayIntegrityError:
            raise
        except Exception as exc:
            raise MinuteSealedReplayIntegrityError("minute sealed accepted request/registry authority is invalid") from exc
        shards = self.reader.list_shards(job_id)
        definition, = self._registry().plan(spec)
        if len(shards) != 1 or shards[0].status is not ShardStatus.SUCCEEDED:
            raise MinuteSealedReplayIntegrityError("minute sealed original shard is incomplete")
        shard = shards[0]
        if (shard.shard_id, shard.shard_index, shard.plan_hash, shard.payload_hash, shard.payload_json,
            shard.adapter_id, shard.adapter_version, shard.work_units) != (
            definition.shard_id, definition.shard_index, definition.plan_hash, definition.payload_hash,
            definition.payload_json, definition.adapter_id, definition.adapter_version, parameters.work_units):
            raise MinuteSealedReplayIntegrityError("minute sealed original shard differs from full accepted plan")
        budget = frozen.result_budget
        try:
            complete = self.artifact_reader.read_complete_tables(job_id, table_names=MINUTE_RESULT_TABLE_NAMES,
                budget=ArtifactCompleteTableBudget(max_table_count=budget.table_count,
                    max_table_bytes=budget.table_bytes, max_total_bytes=budget.total_bytes))
        except ArtifactPreviewUnavailableError:
            return None
        if complete.authority != authority or complete.spec != spec:
            raise MinuteSealedReplayIntegrityError("minute sealed Lab authority changed during full read")
        manifest = complete.manifest
        if (manifest.plan_hash, manifest.adapter_id, manifest.adapter_version, manifest.result_contract_version) != (
            shard.plan_hash, adapter.adapter_id, adapter.adapter_version, COMPLETE_RESULT_CONTRACT_VERSION):
            raise MinuteSealedReplayIntegrityError("minute sealed physical adapter/plan identity conflicts")
        try:
            metrics = strict_model_validate_json(LabFinalizerMetrics, canonical_json_bytes(complete.metrics))
        except Exception as exc:
            raise MinuteSealedReplayIntegrityError("minute sealed original finalizer metrics are invalid") from exc
        if (metrics.job_id, metrics.spec_hash, metrics.plan_hash, metrics.adapter_id, metrics.adapter_version,
            metrics.result_contract_version, metrics.finalizer_code_sha, metrics.shard_count) != (
            job_id, authority.job.spec_hash, shard.plan_hash, adapter.adapter_id, adapter.adapter_version,
            COMPLETE_RESULT_CONTRACT_VERSION, spec.code_sha, 1) or metrics.shards[0].shard_id != shard.shard_id:
            raise MinuteSealedReplayIntegrityError("minute sealed original finalizer identity conflicts")
        result = self._complete_result(complete)
        if result.publication != expected:
            raise MinuteSealedReplayIntegrityError("minute sealed complete publication differs from independent receipt")
        after = self.reader.get_artifact_preview_authority(job_id)
        if after != authority:
            raise MinuteSealedReplayIntegrityError("minute sealed authority changed after complete validation")
        return self._sealed_result_model()(job_id=job_id, shard_id=shard.shard_id, owner_id=owner_id,
            spec_hash=authority.job.spec_hash, payload_hash=shard.payload_hash, plan_hash=shard.plan_hash,
            manifest_hash=authority.evidence.manifest_hash, complete_result_hash=authority.evidence.complete_result_hash,
            completed_at=authority.evidence.indexed_at, accepted_spec=spec, formal_plan=plan, manifest=manifest, result=result)
