from __future__ import annotations

import hashlib
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from rquant.feature_contracts import (
    FeatureAvailability,
    FeatureBatchEnvelope,
    FeatureFieldStatus,
    FeatureRequirement,
    RequirementLevel,
)
from rquant.feature_spool import FeatureBatchSpool
from rquant.runtime_builder_strategy import (
    StrategyEvaluatorBinding,
    strategy_live_builder,
)
from rquant.runtime_candidate_universe import RuntimeCandidateUniverseIntegrityError
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.signal_contracts import SignalAction
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
    StrategyCandidateSnapshot,
    StrategyCandidateSnapshotSpool,
)
from rquant.strategy_runner import (
    StrategyCandidateState,
    StrategyDecision,
    canonical_feature_payload,
)
from rquant.strategy_spec import (
    StateTransition,
    StrategyLifecycleState,
    StrategyRunMode,
    StrategySpec,
)

NOW = datetime(2026, 7, 31, 1, 40, 2, tzinfo=UTC)
COMMIT = "a" * 40
EVALUATOR_FINGERPRINT = "b" * 64


def _spec(*, strategy_id: str = "n-shape-live", version: int = 3) -> StrategySpec:
    return StrategySpec(
        strategy_id=strategy_id,
        version=version,
        feature_contract_id="intraday-pit",
        min_feature_contract_version=2,
        required_features=(
            FeatureRequirement(
                name="rel_same_minute",
                level=RequirementLevel.REQUIRED,
                min_contract_version=2,
            ),
        ),
        optional_features=(),
        initial_state=StrategyLifecycleState.IDLE,
        transitions=(
            StateTransition(
                from_state=StrategyLifecycleState.IDLE,
                event="entry_ready",
                to_state=StrategyLifecycleState.ARMED,
            ),
        ),
        parameters={"threshold": 1.4},
        allowed_actions=(SignalAction.B_INTENT.value,),
        run_mode=StrategyRunMode.SHADOW,
        producer_commit=COMMIT,
    )


def _evaluator(
    spec: StrategySpec,
    state: StrategyCandidateState,
    features: dict[str, object],
) -> StrategyDecision | None:
    if state.state is not StrategyLifecycleState.IDLE or float(
        features["rel_same_minute"]
    ) <= float(spec.parameters["threshold"]):
        return None
    return StrategyDecision(
        event="entry_ready",
        expected_from_state=state.state,
        expected_to_state=StrategyLifecycleState.ARMED,
        expected_action=SignalAction.B_INTENT,
        action=SignalAction.B_INTENT,
        reason_codes=("same_minute_volume",),
        evidence={"rel_same_minute": float(features["rel_same_minute"])},
        expires_after=timedelta(minutes=2),
    )


def _binding(*, strategy_id: str = "n-shape-live", version: int = 3) -> StrategyEvaluatorBinding:
    return StrategyEvaluatorBinding(
        strategy_id=strategy_id,
        strategy_version=version,
        contract_fingerprint=EVALUATOR_FINGERPRINT,
        evaluator=_evaluator,
    )


def _write_spec(path: Path, spec: StrategySpec | None = None) -> None:
    path.write_text((spec or _spec()).model_dump_json(), encoding="utf-8")
    path.chmod(0o600)


def _manifest(
    tmp_path: Path,
    *,
    batch_limit: int = 10,
    plane: RuntimeServicePlane = RuntimeServicePlane.LIVE,
    kind: RuntimeServiceKind = RuntimeServiceKind.STRATEGY_LIVE,
) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id="strategy.n-shape-live.v3",
        service_kind=kind,
        plane=plane,
        interval_seconds=1,
        stale_after_seconds=10,
        producer_commit=COMMIT,
        settings={
            "feature_spool_root": str(tmp_path / "features"),
            "runner_state_path": str(tmp_path / "runner.sqlite3"),
            "strategy_spec_path": str(tmp_path / "strategy.json"),
            "candidate_snapshot_root": str(tmp_path / "candidates"),
            "candidate_max_age_seconds": 60,
            "strategy_id": "n-shape-live",
            "strategy_version": 3,
            "batch_limit": batch_limit,
        },
    )


def _publish(spool: FeatureBatchSpool, *, sequence: int, value: float = 2.0) -> None:
    frame = pd.DataFrame([{"ts_code": "600000.SH", "rel_same_minute": value}])
    payload = canonical_feature_payload(frame, schema_version=2)
    available_at = NOW + timedelta(seconds=sequence)
    spool.publish(
        FeatureBatchEnvelope(
            schema_version=2,
            batch_id=f"feature-{sequence}",
            contract_id="intraday-pit",
            contract_version=2,
            input_batch_ids=(f"minute-{sequence}", "history-snapshot"),
            sequence=sequence,
            event_time=available_at,
            available_at=available_at,
            row_count=1,
            content_hash=hashlib.sha256(payload).hexdigest(),
            field_statuses=(
                FeatureFieldStatus(
                    name="rel_same_minute",
                    status=FeatureAvailability.AVAILABLE,
                    available_at=available_at,
                ),
            ),
            producer_commit=COMMIT,
        ),
        payload,
    )


def _publish_candidates(
    root: Path,
    *,
    producer_commit: str = COMMIT,
) -> None:
    decision_at = NOW - timedelta(days=1)
    StrategyCandidateSnapshotSpool(root.resolve()).publish(
        StrategyCandidateSnapshot.build(
            sequence=0,
            trade_date=date(2026, 7, 31),
            captured_at=NOW,
            producer_commit=producer_commit,
            rows=(
                StrategyCandidateRecord(
                    strategy_id="n-shape-live",
                    strategy_version="3",
                    candidate_id="600000.SH",
                    variant="default",
                    decision_at=decision_at,
                    available_at=decision_at + timedelta(minutes=1),
                    effective_trade_date=date(2026, 7, 31),
                    reference_trade_date=date(2026, 7, 30),
                    price_basis=StrategyCandidatePriceBasis.QFQ_PIT,
                    static_features={"candidate_score": 0.9},
                    reference_snapshot_ids={"daily": "d" * 64},
                ),
            ),
        )
    )


def test_strategy_builder_maps_sequences_generation_backlog_and_replay(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path, batch_limit=1)
    _write_spec(tmp_path / "strategy.json")
    feature_spool = FeatureBatchSpool(tmp_path / "features")
    _publish(feature_spool, sequence=0)
    _publish(feature_spool, sequence=1)
    _publish_candidates(tmp_path / "candidates")
    load_calls: list[tuple[str, int]] = []

    def load_evaluator(strategy_id: str, version: int) -> StrategyEvaluatorBinding:
        load_calls.append((strategy_id, version))
        return _binding()

    step = strategy_live_builder(
        evaluator_loader=load_evaluator,
        clock=lambda: NOW + timedelta(minutes=1),
    )(manifest)

    first = step()
    second = step()
    replay = step()

    assert load_calls == [("n-shape-live", 3)]
    assert first.input_sequence == 0
    assert first.output_sequence == 1
    assert first.processed_count == 1
    assert first.backlog_count == 1
    assert set(first.source_generations) == {"feature_spool", "runner_signal"}
    assert (
        first.source_generations["feature_spool"] == feature_spool.source_descriptor().generation_id
    )
    assert len(first.source_generations["runner_signal"]) == 64
    assert second.input_sequence == 1
    assert second.output_sequence == 1
    assert second.processed_count == 1
    assert second.backlog_count == 0
    assert replay.input_sequence == 1
    assert replay.output_sequence == 1
    assert replay.processed_count == 0
    assert replay.backlog_count == 0


def test_strategy_builder_defers_future_feature_and_reports_exact_backlog(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    _write_spec(tmp_path / "strategy.json")
    feature_spool = FeatureBatchSpool(tmp_path / "features")
    _publish(feature_spool, sequence=0)

    result = strategy_live_builder(
        evaluator_loader=lambda *_args: _binding(),
        clock=lambda: NOW - timedelta(seconds=1),
    )(manifest)()

    assert result.input_sequence == -1
    assert result.output_sequence == 0
    assert result.processed_count == 0
    assert result.backlog_count == 1
    assert result.degraded_reasons == ()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("feature_spool_root", "relative/features"),
        ("runner_state_path", "runner.sqlite3"),
        ("strategy_spec_path", "strategy.json"),
        ("candidate_snapshot_root", "candidates"),
    ],
)
def test_strategy_builder_requires_absolute_runtime_paths(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    manifest_payload = _manifest(tmp_path).model_dump(mode="json")
    manifest_payload["settings"][field] = value
    manifest = RuntimeServiceManifest.model_validate(manifest_payload)

    with pytest.raises(ValidationError, match="absolute"):
        strategy_live_builder(
            evaluator_loader=lambda *_args: _binding(),
            clock=lambda: NOW,
        )(manifest)


def test_strategy_builder_requires_normalized_candidate_root_and_positive_age(
    tmp_path: Path,
) -> None:
    payload = _manifest(tmp_path).model_dump(mode="json")
    payload["settings"]["candidate_snapshot_root"] = os.path.join(
        str(tmp_path), "nested", "..", "candidates"
    )
    traversal = RuntimeServiceManifest.model_validate(payload)
    with pytest.raises(ValidationError, match="normalized"):
        strategy_live_builder(
            evaluator_loader=lambda *_args: _binding(),
            clock=lambda: NOW,
        )(traversal)

    payload = _manifest(tmp_path).model_dump(mode="json")
    payload["settings"]["candidate_max_age_seconds"] = 0
    invalid_age = RuntimeServiceManifest.model_validate(payload)
    with pytest.raises(ValidationError, match="greater than 0"):
        strategy_live_builder(
            evaluator_loader=lambda *_args: _binding(),
            clock=lambda: NOW,
        )(invalid_age)


def test_strategy_builder_binds_candidate_authority_to_manifest_commit(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    _write_spec(tmp_path / "strategy.json")
    feature_spool = FeatureBatchSpool(tmp_path / "features")
    _publish(feature_spool, sequence=0)
    _publish_candidates(tmp_path / "candidates", producer_commit="f" * 40)
    step = strategy_live_builder(
        evaluator_loader=lambda *_args: _binding(),
        clock=lambda: NOW,
    )(manifest)

    with pytest.raises(RuntimeCandidateUniverseIntegrityError, match="producer commit"):
        step()

    assert feature_spool.load_cursor("strategy:n-shape-live:3") is None


def test_strategy_builder_rejects_wrong_kind_plane_and_dynamic_import_setting(
    tmp_path: Path,
) -> None:
    _write_spec(tmp_path / "strategy.json")
    builder = strategy_live_builder(
        evaluator_loader=lambda *_args: _binding(),
        clock=lambda: NOW,
    )

    with pytest.raises(ValueError, match="kind"):
        builder(_manifest(tmp_path, kind=RuntimeServiceKind.FEATURE_LIVE))
    with pytest.raises(ValueError, match="live plane"):
        builder(_manifest(tmp_path, plane=RuntimeServicePlane.RESEARCH))

    payload = _manifest(tmp_path).model_dump(mode="json")
    payload["settings"]["evaluator_import"] = "unsafe.module:evaluate"
    dynamic_import = RuntimeServiceManifest.model_validate(payload)
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        builder(dynamic_import)


def test_strategy_builder_freezes_and_validates_spec_and_evaluator_identity(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "strategy.json"
    _write_spec(spec_path)
    manifest = _manifest(tmp_path)

    with pytest.raises(ValueError, match="evaluator identity"):
        strategy_live_builder(
            evaluator_loader=lambda *_args: _binding(strategy_id="other"),
            clock=lambda: NOW,
        )(manifest)

    with pytest.raises(ValueError, match="strategy spec identity"):
        strategy_live_builder(
            evaluator_loader=lambda *_args: _binding(),
            clock=lambda: NOW,
        )(
            RuntimeServiceManifest.model_validate(
                {
                    **manifest.model_dump(mode="json"),
                    "settings": {
                        **manifest.model_dump(mode="json")["settings"],
                        "strategy_version": 4,
                    },
                }
            )
        )

    wrong_commit = _spec().model_copy(update={"producer_commit": "c" * 40})
    _write_spec(spec_path, wrong_commit)
    with pytest.raises(ValueError, match="producer commit"):
        strategy_live_builder(
            evaluator_loader=lambda *_args: _binding(),
            clock=lambda: NOW,
        )(manifest)


def test_strategy_builder_rejects_strategy_spec_through_symlinked_parent(
    tmp_path: Path,
) -> None:
    actual = tmp_path / "actual"
    actual.mkdir()
    _write_spec(actual / "strategy.json")
    linked = tmp_path / "linked"
    linked.symlink_to(actual, target_is_directory=True)
    payload = _manifest(tmp_path).model_dump(mode="json")
    payload["settings"]["strategy_spec_path"] = str(linked / "strategy.json")

    with pytest.raises(ValueError, match="symlink|unsafe"):
        strategy_live_builder(
            evaluator_loader=lambda *_args: _binding(),
            clock=lambda: NOW,
        )(RuntimeServiceManifest.model_validate(payload))


def test_strategy_builder_rejects_symlinked_strategy_spec(tmp_path: Path) -> None:
    original = tmp_path / "original.json"
    _write_spec(original)
    (tmp_path / "strategy.json").symlink_to(original)

    with pytest.raises(ValueError, match="symlink"):
        strategy_live_builder(
            evaluator_loader=lambda *_args: _binding(),
            clock=lambda: NOW,
        )(_manifest(tmp_path))


def test_strategy_builder_rejects_non_regular_strategy_spec(tmp_path: Path) -> None:
    (tmp_path / "strategy.json").mkdir()

    with pytest.raises(ValueError, match="regular file|unavailable"):
        strategy_live_builder(
            evaluator_loader=lambda *_args: _binding(),
            clock=lambda: NOW,
        )(_manifest(tmp_path))


def test_strategy_builder_requires_current_uid_for_strategy_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_spec(tmp_path / "strategy.json")
    monkeypatch.setattr(os, "getuid", lambda: os.stat(tmp_path / "strategy.json").st_uid + 1)

    with pytest.raises(ValueError, match="current uid"):
        strategy_live_builder(
            evaluator_loader=lambda *_args: _binding(),
            clock=lambda: NOW,
        )(_manifest(tmp_path))


def test_strategy_builder_rejects_strategy_spec_with_group_or_world_permissions(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "strategy.json"
    _write_spec(spec_path)
    spec_path.chmod(0o666)

    with pytest.raises(ValueError, match="0600"):
        strategy_live_builder(
            evaluator_loader=lambda *_args: _binding(),
            clock=lambda: NOW,
        )(_manifest(tmp_path))


def test_strategy_builder_rejects_hardlinked_strategy_spec(tmp_path: Path) -> None:
    original = tmp_path / "original.json"
    _write_spec(original)
    os.link(original, tmp_path / "strategy.json")

    with pytest.raises(ValueError, match="hardlink|link count"):
        strategy_live_builder(
            evaluator_loader=lambda *_args: _binding(),
            clock=lambda: NOW,
        )(_manifest(tmp_path))


def test_strategy_builder_rejects_strategy_spec_replaced_while_opening(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec_path = tmp_path / "strategy.json"
    _write_spec(spec_path)
    original_stat = os.stat
    final_stat_calls = 0

    def changing_stat(
        path: str | bytes | int | os.PathLike[str] | os.PathLike[bytes],
        *args: object,
        **kwargs: object,
    ) -> os.stat_result:
        nonlocal final_stat_calls
        observed = original_stat(path, *args, **kwargs)
        if path == "strategy.json" and kwargs.get("dir_fd") is not None:
            final_stat_calls += 1
            if final_stat_calls == 2:
                changed = list(observed)
                changed[1] += 1
                return os.stat_result(changed)
        return observed

    monkeypatch.setattr(os, "stat", changing_stat)

    with pytest.raises(ValueError, match="identity changed"):
        strategy_live_builder(
            evaluator_loader=lambda *_args: _binding(),
            clock=lambda: NOW,
        )(_manifest(tmp_path))
