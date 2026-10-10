"""Derive a full recipe from independently installed, complete minute facts."""

from __future__ import annotations

import base64
import hashlib
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.minute_backtest_contracts import MinuteReplayExecutionProfile, MinuteReplayMaterial
from rquant.minute_backtest_parameter_contracts import (
    FrozenMinuteParameterResearchInput, MinuteParameterFormalWork,
    MinuteParameterRuntimeContent, MinuteParameterSourceSeed, MinuteParameterStrategyBinding,
)
from rquant.minute_backtest_parameter_definition import (
    bootstrap_minute_parameter_definition, bootstrap_minute_parameter_research_definition,
    build_minute_parameter_definition, minute_parameter_research_registry,
)
from rquant.minute_backtest_parameter_features import PARAMETER_CANDIDATE_FEATURE, MinuteParameterCandidate
from rquant.minute_backtest_parameter_source import measure_minute_parameter_work
from rquant.minute_backtest_parameter_study import MinuteParameterStudyBinding, MinuteParameterStudySettings
from rquant.minute_backtest_study_protocols import MinuteStudyHead, MinuteStudyProtocol, MinuteStudySource, MinuteStudySplit
from rquant.minute_backtest_parameters import MinuteParameterSet
from rquant.minute_backtest_producer import measure_minute_formal_work
from rquant.minute_backtest_publication_contracts import (
    MinuteCodeFile, MinuteDerivation, MinutePublicationEvidence, MinuteVisibilityPolicy,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_candidate_snapshot import StrategyCandidateRecord, StrategyCandidateSnapshot, StrategyCandidateSnapshotSpool

if TYPE_CHECKING:
    from rquant.minute_backtest_formal import MinuteExperimentProtocol


def _candidate_facts(value: FrozenMinuteParameterResearchInput) -> dict[tuple[object, ...], MinuteParameterCandidate]:
    facts = {}
    for item in value.runtime.materials:
        if not item.relative_path.startswith("candidates/generations/"):
            continue
        snapshot = StrategyCandidateSnapshot.model_validate_json(item.payload())
        for record in snapshot.rows:
            raw = record.static_features.get(PARAMETER_CANDIDATE_FEATURE)
            if not isinstance(raw, str):
                raise PermissionError("parameter complete facts omit an original candidate record")
            candidate = MinuteParameterCandidate.model_validate_json(raw)
            key = (record.candidate_id, record.variant, record.decision_at, record.available_at,
                record.effective_trade_date, record.reference_trade_date, record.price_basis,
                tuple(sorted(record.reference_snapshot_ids.items())))
            if key in facts:
                raise PermissionError("parameter complete facts repeat an original candidate occurrence")
            facts[key] = candidate
    return facts


def _parameter_execution_profile(
    profile: MinuteReplayExecutionProfile, parameters: MinuteParameterSet,
) -> MinuteReplayExecutionProfile:
    slippage = type(profile.execution_costs.slippage).model_validate(
        profile.execution_costs.slippage.model_dump(mode="python", exclude_computed_fields=True) | {
            "buy_bps": Decimal(str(parameters.parameters.paper.entry_slippage_pct)) * 10000})
    # The original v3 owner derives its identity from the full canonical costs;
    # carrying the baseline identity after changing slippage is invalid.
    costs = type(profile.execution_costs).model_validate(
        profile.execution_costs.model_dump(mode="python", exclude={"cost_spec_id"}, exclude_computed_fields=True)
        | {"slippage": slippage})
    return type(profile).model_validate(profile.model_dump(mode="python") | {
        "execution_costs": costs})


def _derived_profile(baseline: FrozenMinuteParameterResearchInput, parameters: MinuteParameterSet) -> MinuteReplayExecutionProfile:
    return _parameter_execution_profile(baseline.runtime.execution_profile, parameters)


def verify_minute_parameter_derivation(
    value: FrozenMinuteParameterResearchInput, baseline: FrozenMinuteParameterResearchInput,
) -> None:
    current, original = value.runtime, baseline.runtime
    unchanged = ("owner_id", "producer_commit", "available_at", "start_date", "end_date", "complete_through",
        "warmup_available_at", "warmup_complete", "holding_tail_complete", "market_calendar",
        "source_frequency", "session_facts", "tick_times", "result_budget")
    if any(getattr(current, field) != getattr(original, field) for field in unchanged):
        raise PermissionError("parameter derivative changed original raw/runtime basis or facts")
    if current.parameters.parameters.family != original.parameters.parameters.family:
        raise PermissionError("parameter derivative family is not present in its installed full facts")
    original_raw = {item.relative_path: item for item in original.materials if not item.relative_path.startswith("candidates/")}
    current_raw = {item.relative_path: item for item in current.materials if not item.relative_path.startswith("candidates/")}
    if current_raw != original_raw or current.execution_profile != _derived_profile(baseline, current.parameters):
        raise PermissionError("parameter derivative changed original raw/archive or broker profile")
    original_facts, current_facts = _candidate_facts(baseline), _candidate_facts(value)
    if original_facts.keys() != current_facts.keys():
        raise PermissionError("parameter derivative filtered or added original candidate facts")
    for key, candidate in current_facts.items():
        if candidate.parameter_hash != current.parameters.fingerprint or candidate.model_dump(
                mode="python", exclude={"parameter_hash", "study_binding"}) != original_facts[key].model_dump(
                mode="python", exclude={"parameter_hash", "study_binding"}):
            raise PermissionError("parameter derivative changed original candidate values/visibility")
        if candidate.study_binding != current.study_binding:
            raise PermissionError("parameter derivative candidate differs from its complete runtime study")
    if current.study_binding is not None:
        source = current.study_binding.protocol.source
        if (source.source_key, source.source_version, source.owner_id, source.full_input_hash,
                source.dataset_snapshot_id, source.frequency, source.start_date, source.end_date,
                source.published_at) != (
                original.source_key, original.source_version, original.owner_id, baseline.full_input_hash,
                original.dataset_snapshot_id, original.source_frequency, original.start_date, original.end_date,
                baseline.provenance.published_at):
            raise PermissionError("study derivative does not bind its complete independent baseline")
    if value.origin_materials != baseline.origin_materials or value.provenance.capture_lineage != baseline.provenance.capture_lineage:
        raise PermissionError("parameter derivative changed full original bytes or acquisition lineage")
    if (value.provenance.source_kind != "reconstructed" or value.provenance.visibility_policy is None
            or value.provenance.extracted_at < baseline.provenance.published_at):
        raise PermissionError("parameter derivative lacks its explicit research visibility policy")


def build_minute_parameter_source_seed(
    baseline: FrozenMinuteParameterResearchInput, *, parameters: MinuteParameterSet,
    definitions_root: Path, candidate_root: Path, source_key: str, now: datetime,
    visibility_policy: MinuteVisibilityPolicy,
    study: MinuteParameterStudySettings | None = None,
    formal_protocol: MinuteExperimentProtocol | None = None,
    random_seed: int = 0, request_hash: str | None = None,
) -> MinuteParameterSourceSeed:
    baseline = FrozenMinuteParameterResearchInput.model_validate(baseline.model_dump(mode="python"))
    parameters = MinuteParameterSet.model_validate(parameters.model_dump(mode="python"))
    original = baseline.runtime
    if parameters.parameters.family != original.parameters.parameters.family or parameters.parameters.freq != original.source_frequency:
        raise PermissionError("parameter recipe family/frequency differs from the full physical facts")
    if now < baseline.provenance.published_at or visibility_policy.timestamp_semantics != original.execution_profile.timestamp_semantics:
        raise PermissionError("parameter derivative time or explicit visibility policy differs")
    native = bootstrap_minute_parameter_definition(definitions_root, parameters,
        producer_commit=original.producer_commit, registered_at=now, available_at=now)
    wrapper = bootstrap_minute_parameter_research_definition(definitions_root, parameters,
        producer_commit=original.producer_commit, now=now)
    registry = ImmutableDefinitionRegistry(definitions_root,
        execution_registry=minute_parameter_research_registry(parameters, producer_commit=original.producer_commit))
    feature = registry.read_feature_contract(native.feature_contract_fingerprint, as_of=now)
    definition = build_minute_parameter_definition(parameters, producer_commit=original.producer_commit)
    study_binding = None
    if study is not None:
        if formal_protocol is None or request_hash is None:
            raise PermissionError("study source requires the full formal protocol and original request hash")
        study = MinuteParameterStudySettings.model_validate(study)
        protocol = MinuteStudyProtocol(source=MinuteStudySource(source_key=original.source_key,
            source_version=original.source_version, owner_id=original.owner_id,
            full_input_hash=baseline.full_input_hash, dataset_snapshot_id=original.dataset_snapshot_id,
            frequency=original.source_frequency, start_date=original.start_date, end_date=original.end_date,
            published_at=baseline.provenance.published_at),
            head=MinuteStudyHead(definition_id=native.logical_id, definition_version=native.version,
                evaluator_semantic_version=parameters.evaluator_semantic_version,
                parameter_fingerprint=parameters.fingerprint, registration_fingerprint=native.fingerprint,
                spec_fingerprint=native.spec.spec_fingerprint, executable_fingerprint=native.executable_fingerprint,
                producer_commit=original.producer_commit), parameters=parameters,
            split=MinuteStudySplit(train_start=formal_protocol.train_range.start_date,
                train_end=formal_protocol.train_range.end_date,
                test_start=formal_protocol.frozen_outer_test_range.start_date,
                test_end=formal_protocol.frozen_outer_test_range.end_date),
            score_profile=study.score_profile, top_n=study.top_n, min_trades=study.min_trades,
            random_seed=random_seed, requested_at=now)
        study_binding = MinuteParameterStudyBinding.from_formal_protocol(protocol=protocol,
            formal_protocol=formal_protocol, request_hash=request_hash)
    spool = StrategyCandidateSnapshotSpool(candidate_root)
    parents = tuple(item.origin_object_keys[0] for item in baseline.derivations
        if item.material_path.startswith("candidates/") and len(item.origin_object_keys) == 1)
    if not parents:
        raise PermissionError("parameter baseline has no full original candidate derivation parents")
    snapshots = sorted((StrategyCandidateSnapshot.model_validate_json(item.payload())
        for item in original.materials if item.relative_path.startswith("candidates/generations/")),
        key=lambda item: item.sequence)
    for snapshot in snapshots:
        rows = []
        for record in snapshot.rows:
            raw = record.static_features.get(PARAMETER_CANDIDATE_FEATURE)
            if not isinstance(raw, str):
                raise PermissionError("parameter baseline lacks its complete original candidate values")
            candidate = MinuteParameterCandidate.model_validate_json(raw).model_copy(update={
                "parameter_hash": parameters.fingerprint, "study_binding": study_binding})
            rows.append(StrategyCandidateRecord.model_validate(record.model_dump(mode="python") | {
                "strategy_id": definition.strategy_id, "strategy_version": "1",
                "static_features": {PARAMETER_CANDIDATE_FEATURE: candidate.model_dump_json()}}))
        spool.publish_strategy_records(strategy_id=definition.strategy_id, strategy_version="1",
            definition_fingerprint=native.fingerprint, executable_fingerprint=native.executable_fingerprint,
            candidate_schema_fingerprint=definition.candidate_schema_fingerprint,
            static_feature_schema={name: field.contract_payload() for name, field in definition.static_feature_schema.items()},
            source_snapshot_ids=snapshot.source_snapshot_ids, trade_date=snapshot.trade_date,
            captured_at=snapshot.captured_at, producer_commit=original.producer_commit, rows=tuple(rows))
    materials = [item for item in original.materials if not item.relative_path.startswith("candidates/")]
    for path in sorted(spool.root.rglob("*")):
        if not path.is_file() or path.name.startswith("."):
            continue
        data = path.read_bytes()
        materials.append(MinuteReplayMaterial(relative_path="candidates/"+path.relative_to(spool.root).as_posix(),
            content_base64=base64.b64encode(data).decode("ascii"), content_sha256=hashlib.sha256(data).hexdigest()))
    materials = tuple(sorted(materials, key=lambda item: item.relative_path))
    work = measure_minute_parameter_work(parameters, materials=materials, tick_times=original.tick_times,
        warmup_available_at=original.warmup_available_at, runtime_work=original.work,
        session_facts=original.session_facts, study_binding=study_binding)
    runtime = MinuteParameterRuntimeContent.model_validate(original.content.model_dump(mode="python") | {
        "source_key": source_key, "source_version": 1, "parameters": parameters,
        "study_binding": study_binding,
        "strategy": MinuteParameterStrategyBinding.from_registration(native,
            parameters=parameters, producer_commit=original.producer_commit),
        "materials": materials, "execution_profile": _derived_profile(baseline, parameters), "parameter_work": work})
    transformation = canonical_sha256({"contract": "minute-parameter-fact-derivative/v1",
        "baseline": baseline.full_input_hash, "parameters": parameters.fingerprint,
        "executable": native.executable_fingerprint, "policy": visibility_policy.fingerprint})
    if study_binding is not None:
        transformation = canonical_sha256({"base_transformation": transformation,
            "complete_study": study_binding.model_dump(mode="json")})
    derivations = [item for item in baseline.derivations if not item.material_path.startswith("candidates/")]
    derivations.extend(MinuteDerivation(material_path=item.relative_path, origin_object_keys=parents,
        method="research_derivative", transformation_fingerprint=transformation, time_basis="modeled")
        for item in materials if item.relative_path.startswith("candidates/"))
    proofs = [item for item in baseline.provenance.publication_evidence if item.kind != "candidate"]
    for item in materials:
        if item.relative_path.startswith("candidates/generations/"):
            snapshot = StrategyCandidateSnapshot.model_validate_json(item.payload())
            proofs.append(MinutePublicationEvidence(kind="candidate", sequence=snapshot.sequence,
                material_path=item.relative_path, origin_object_key=parents[0],
                published_at=snapshot.captured_at, time_basis="modeled"))
    code = MinuteCodeFile(logical_name="minute_parameter_fact_derivative",
        content_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    code_files = tuple(item for item in baseline.provenance.code_files if item.logical_name != code.logical_name) + (code,)
    provenance = type(baseline.provenance).model_validate(baseline.provenance.model_dump(mode="python") | {
        "source_kind": "reconstructed", "publication_evidence": tuple(proofs), "extracted_at": now,
        "published_at": now, "extractor_fingerprint": transformation, "code_files": code_files,
        "source_index_sha256": canonical_sha256(code_files), "visibility_policy": visibility_policy})
    measured = measure_minute_formal_work(work.runtime_work, origins=baseline.origin_materials,
        provenance=provenance, derivations=tuple(derivations))
    return MinuteParameterSourceSeed(runtime=runtime, native_registration=native, wrapper_registration=wrapper,
        feature_registration=feature, provenance=provenance, origin_materials=baseline.origin_materials,
        derivations=tuple(derivations), formal_work=MinuteParameterFormalWork(
            **measured.model_dump(mode="python"), parameter_work=work), result_budget=original.result_budget)
