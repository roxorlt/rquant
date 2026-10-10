"""Explicit synthetic parameter source; historical visibility is modeled."""

from __future__ import annotations

import base64
import hashlib
from datetime import UTC, date, datetime
from pathlib import Path

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.minute_backtest_parameter_contracts import MinuteParameterFormalWork, MinuteParameterSourceSeed
from rquant.minute_backtest_parameter_definition import (
    bootstrap_minute_parameter_research_definition, minute_parameter_research_registry,
)
from rquant.minute_backtest_parameters import MinuteNShapeParameters, MinuteParameterSet
from rquant.minute_backtest_producer import measure_minute_formal_work
from rquant.minute_backtest_publication_contracts import (
    MinuteCaptureLineage, MinuteCodeFile, MinuteDerivation, MinuteOriginMaterial,
    MinuteProvenance, MinutePublicationEvidence, MinuteVisibilityPolicy,
)
from rquant.live_contracts import BatchEnvelope
from rquant.paper_execution_constraints import PaperExecutionConstraintPointer
from rquant.strategy_candidate_snapshot import StrategyCandidateSnapshot
from tests.support.minute_parameter_runtime_fixture import parameter_runtime_fixture


def parameter_source_seed(root: Path, parameters: MinuteParameterSet | None = None, *,
    days: tuple[date, ...] | None = None, sparse: bool = False,
    study_facts: bool = False) -> MinuteParameterSourceSeed:
    parameters = parameters or MinuteParameterSet(parameters=MinuteNShapeParameters(
        paper={"stop_loss_pct": 0.012345, "entry_slippage_pct": 0.0002}))
    value, _ = parameter_runtime_fixture(root / "runtime-fixture", parameters, days=days, sparse=sparse,
        study_facts=study_facts)
    now = datetime.now(UTC)
    definitions = root / "runtime-fixture/actual-parameter-definitions"
    registry = ImmutableDefinitionRegistry(definitions,
        execution_registry=minute_parameter_research_registry(parameters, producer_commit=value.producer_commit))
    native = registry.read_strategy_spec(value.strategy.registration_fingerprint, as_of=now)
    feature = registry.read_feature_contract(native.feature_contract_fingerprint, as_of=now)
    wrapper = bootstrap_minute_parameter_research_definition(definitions, parameters,
        producer_commit=value.producer_commit, now=now)
    policy = MinuteVisibilityPolicy(policy_id="explicit-synthetic-parameter-prefix", version=1,
        timestamp_semantics=value.execution_profile.timestamp_semantics,
        market_event_basis="original synthetic envelope event_time",
        market_visibility_basis="original synthetic available_at, modeled historical visibility",
        candidate_visibility_basis="synthetic T-1 static facts and original candidate captured_at",
        constraint_visibility_basis="original synthetic per-publication published_at",
        native_definition_basis="synthetic private historical bootstrap; actual wrapper registered at publication",
        limitations="Synthetic validation only. No claim of real retained capture, official close or full real facts.")
    origins = [MinuteOriginMaterial(object_key=f"archive:{index}", content_base64=item.content_base64,
        content_sha256=item.content_sha256,
        format="parquet" if item.relative_path.endswith((".payload", ".parquet")) else "json")
        for index, item in enumerate(value.materials)]
    derivations = tuple(MinuteDerivation(material_path=item.relative_path,
        origin_object_keys=(origin.object_key,), method="retained_research_archive",
        transformation_fingerprint=policy.fingerprint, time_basis="modeled")
        for item, origin in zip(value.materials, origins, strict=True))
    proofs = []
    for item, origin in zip(value.materials, origins, strict=True):
        if item.relative_path.startswith("market/batches/") and item.relative_path.endswith(".json"):
            batch = BatchEnvelope.model_validate_json(item.payload())
            proofs.append(MinutePublicationEvidence(kind="market", sequence=batch.sequence,
                material_path=item.relative_path, origin_object_key=origin.object_key,
                published_at=batch.available_at, time_basis="modeled"))
        elif item.relative_path.startswith("constraint-publications/"):
            pointer = PaperExecutionConstraintPointer.model_validate_json(item.payload())
            proofs.append(MinutePublicationEvidence(kind="constraint", sequence=pointer.sequence,
                material_path=item.relative_path, origin_object_key=origin.object_key,
                published_at=pointer.published_at, time_basis="modeled"))
        elif item.relative_path.startswith("candidates/generations/"):
            candidate = StrategyCandidateSnapshot.model_validate_json(item.payload())
            proofs.append(MinutePublicationEvidence(kind="candidate", sequence=candidate.sequence,
                material_path=item.relative_path, origin_object_key=origin.object_key,
                published_at=candidate.captured_at, time_basis="modeled"))
    for index, fact in enumerate(value.session_facts):
        data = fact.source_payload()
        origins.append(MinuteOriginMaterial(object_key=f"session-fact:{index}",
            content_base64=base64.b64encode(data).decode("ascii"),
            content_sha256=hashlib.sha256(data).hexdigest(), format="json"))
    provenance = MinuteProvenance(source_kind="reconstructed",
        capture_lineage=tuple(MinuteCaptureLineage(object_key=item.object_key,
            content_sha256=item.content_sha256, acquisition_commit=value.producer_commit,
            captured_at=None, timing_evidence_object_key=None) for item in origins),
        publication_evidence=tuple(proofs), extracted_at=now, published_at=now,
        extractor_code_commit=value.producer_commit, extractor_fingerprint=policy.fingerprint,
        research_code_commit=value.producer_commit,
        source_index_sha256=hashlib.sha256(b"explicit-synthetic-parameter-fixture-v1").hexdigest(),
        code_files=(MinuteCodeFile(logical_name="explicit_synthetic_parameter_fixture",
            content_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()),),
        replay_start=value.tick_times[0], replay_end=value.tick_times[-1],
        native_definition_replay_available_at=value.available_at, visibility_policy=policy)
    work = measure_minute_formal_work(value.work, origins=tuple(origins), provenance=provenance, derivations=derivations)
    return MinuteParameterSourceSeed(runtime=value.content, native_registration=native,
        feature_registration=feature, wrapper_registration=wrapper, provenance=provenance,
        origin_materials=tuple(origins), derivations=derivations,
        formal_work=MinuteParameterFormalWork(**work.model_dump(mode="python"), parameter_work=value.parameter_work),
        result_budget=value.result_budget)
