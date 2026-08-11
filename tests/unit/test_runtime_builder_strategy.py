from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

import rquant.strategy_evaluators as strategy_evaluators_module
from rquant.definition_registry import (
    DefinitionIntegrityError,
    ImmutableDefinitionRegistry,
    StrategySpecRegistration,
)
from rquant.feature_contracts import (
    FeatureAvailability,
    FeatureBatchEnvelope,
    FeatureContract,
    FeatureDefinition,
    FeatureFieldStatus,
)
from rquant.feature_spool import FeatureBatchSpool
from rquant.paper_broker import (
    BrokerCostPolicy,
    BrokerExecutionContext,
    PaperBrokerStore,
)
from rquant.paper_contracts import PaperOrderIntent, PaperOrderType, PaperSide
from rquant.runtime_builder_strategy import (
    StrategyEvaluatorBinding,
    strategy_live_builder,
)
from rquant.runtime_candidate_universe import RuntimeCandidateUniverseIntegrityError
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_shadow_validation import HmacCompletionAttestationAuthority
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_router_runtime import (
    ReadonlyStrategyRunnerSignalSource,
    RoutingDecision,
    SignalRouteCursorStore,
    route_runner_signals,
)
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
    StrategyCandidateSnapshotSpool,
)
from rquant.strategy_runner import (
    StrategyBatchResult,
    StrategyCandidateState,
    StrategyDecision,
    canonical_feature_payload,
)
from rquant.strategy_spec import StrategyLifecycleState, StrategySpec
from tests.shadow_ed25519_support import (
    create_rotating_shadow_ed25519_test_authority,
    create_shadow_ed25519_test_authority,
)

NOW = datetime(2026, 7, 31, 1, 40, 2, tzinfo=UTC)
COMMIT = "a" * 40
EVALUATOR_FINGERPRINT = "b" * 64
SESSION_CLOSE = datetime(2026, 7, 31, 7, 0, tzinfo=UTC)


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


def _replacement_runtime_dispatcher(
    _spec: StrategySpec,
    _state: StrategyCandidateState,
    _features: dict[str, object],
) -> StrategyDecision | None:
    return None


def _replacement_session_geometry(
    *,
    session_low: float,
    latest_close: float,
    session_high: float,
) -> None:
    del session_low, latest_close, session_high


def _binding(*, strategy_id: str = "n_shape", version: int = 1) -> StrategyEvaluatorBinding:
    return StrategyEvaluatorBinding(
        strategy_id=strategy_id,
        strategy_version=version,
        contract_fingerprint=EVALUATOR_FINGERPRINT,
        evaluator=_evaluator,
    )


def _manifest(
    tmp_path: Path,
    *,
    batch_limit: int = 10,
    plane: RuntimeServicePlane = RuntimeServicePlane.LIVE,
    kind: RuntimeServiceKind = RuntimeServiceKind.STRATEGY_LIVE,
    producer_commit: str = COMMIT,
    strategy_id: str = "n_shape",
    strategy_version: int = 1,
    registration_fingerprint: str | None = None,
    definition_registry_root: Path | None = None,
    executable_fingerprint: str | None = None,
    candidate_schema_fingerprint: str | None = None,
    completion_authority: bool = False,
) -> RuntimeServiceManifest:
    if registration_fingerprint is None or definition_registry_root is None:
        definition_registry_root, registration = _publish_builtin_registration(
            tmp_path,
            strategy_id=strategy_id,
            producer_commit=producer_commit,
        )
        registration_fingerprint = registration.fingerprint
        executable_fingerprint = registration.executable_fingerprint
        candidate_schema_fingerprint = registration.candidate_schema_fingerprint
    if executable_fingerprint is None or candidate_schema_fingerprint is None:
        from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

        definition = BuiltinStrategyEvaluatorRegistry(
            producer_commit=producer_commit
        ).load_definition(strategy_id, strategy_version)
        executable_fingerprint = executable_fingerprint or definition.executable_fingerprint
        candidate_schema_fingerprint = (
            candidate_schema_fingerprint or definition.candidate_schema_fingerprint
        )
    paper_broker_path = tmp_path / "paper-broker.sqlite3"
    if not paper_broker_path.exists():
        PaperBrokerStore(
            paper_broker_path,
            account_id="paper-main",
            initial_cash=Decimal("100000"),
            cost_policy=BrokerCostPolicy(
                commission_rate=Decimal("0.0003"),
                minimum_commission=Decimal("5"),
                sell_stamp_tax_rate=Decimal("0.001"),
            ),
        )
    settings: dict[str, object] = {
        "feature_spool_root": str(tmp_path / "features"),
        "runner_state_path": str(tmp_path / "runner.sqlite3"),
        "definition_registry_root": os.path.abspath(definition_registry_root),
        "strategy_registration_fingerprint": registration_fingerprint,
        "strategy_executable_fingerprint": executable_fingerprint,
        "candidate_schema_fingerprint": candidate_schema_fingerprint,
        "candidate_snapshot_root": str(tmp_path / "candidates"),
        "paper_broker_path": str(paper_broker_path),
        "paper_account_id": "paper-main",
        "candidate_max_age_seconds": 60,
        "strategy_id": strategy_id,
        "strategy_version": strategy_version,
        "batch_limit": batch_limit,
    }
    if completion_authority:
        from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

        definition = BuiltinStrategyEvaluatorRegistry(
            producer_commit=producer_commit
        ).load_definition(strategy_id, strategy_version)
        calendar = MarketCalendarAuthority.create(
            schema_version=1,
            exchange="SSE",
            producer_commit=COMMIT,
            coverage_start=date(2026, 7, 1),
            coverage_end=date(2026, 8, 31),
            open_dates=(date(2026, 7, 31),),
            generated_at=datetime(2026, 7, 1, tzinfo=UTC),
        )
        calendar_path = tmp_path / "calendar.json"
        calendar_path.write_text(
            json.dumps(
                calendar.model_dump(mode="json"),
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        calendar_path.chmod(0o600)
        settings.update(
            {
                "calendar_path": str(calendar_path.resolve()),
                "calendar_expected_commit": COMMIT,
                "calendar_content_sha256": calendar.content_sha256,
                "signal_bus_path": str((tmp_path / "signal-bus.sqlite3").resolve()),
                "routing_policy_fingerprint": "9" * 64,
                "producer_instance_id": "test-strategy-primary",
                "producer_version": "test-runtime-v1",
                "strategy_spec_fingerprint": definition.spec.spec_fingerprint,
                "evaluator_contract_fingerprint": definition.executable_fingerprint,
            }
        )
    return RuntimeServiceManifest(
        service_id=f"strategy.{strategy_id}.v{strategy_version}",
        service_kind=kind,
        plane=plane,
        interval_seconds=1,
        stale_after_seconds=10,
        producer_commit=producer_commit,
        settings=settings,
    )


def _publish(
    spool: FeatureBatchSpool,
    *,
    sequence: int,
    strategy_id: str = "n_shape",
    value: float = 2.0,
    latest_close: float = 11.2,
    session_high: float | None = None,
    available_at: datetime | None = None,
    source_event_time: datetime | None = None,
    decision_cutoff: datetime | None = None,
) -> None:
    if strategy_id == "n_shape":
        values: dict[str, object] = {
            "ts_code": "600000.SH",
            "latest_close": latest_close,
            "session_low": min(10.05, latest_close),
            "session_high": session_high or max(11.3, latest_close),
            "price_over_vwap": 1.02,
            "rel_same_minute": value,
            "rel_cumulative": None,
            "amount_accel_5m": None,
            "amount_accel_10m": None,
            "tick_rule_buy_sell_ratio_proxy": None,
            "historical_sessions": None,
        }
    elif strategy_id == "growth_board_surge":
        values = {
            "ts_code": "600000.SH",
            "latest_close": latest_close,
            "session_high": session_high or max(11.3, latest_close),
            "opening_bar_high": 10.6,
            "opening_bar_low": 10.2,
            "rel_cumulative": 1.5,
            "price_over_vwap": 1.01,
            "historical_sessions": 20,
            "rel_same_minute": 2.2,
            "amount_accel_5m": 2.1,
            "amount_accel_10m": 1.5,
            "tick_rule_buy_sell_ratio_proxy": 0.2,
            "minute_volume": 1000.0,
            "cumulative_volume": 5000.0,
        }
    elif strategy_id == "auction_gap":
        values = {
            "ts_code": "600000.SH",
            "latest_close": latest_close,
            "session_low": min(10.4, latest_close),
            "session_high": session_high or max(10.9, latest_close),
            "price_over_vwap": 1.01,
            "rel_same_minute": 1.2,
            "rel_cumulative": 1.3,
            "amount_accel_5m": 1.2,
            "amount_accel_10m": 1.1,
            "tick_rule_buy_sell_ratio_proxy": 0.3,
        }
    else:
        raise ValueError(f"unsupported test strategy: {strategy_id}")
    frame = pd.DataFrame([values])
    payload = canonical_feature_payload(frame, schema_version=2)
    resolved_available_at = available_at or NOW + timedelta(seconds=sequence)
    resolved_source_event_time = source_event_time or resolved_available_at
    resolved_decision_cutoff = decision_cutoff or resolved_available_at
    actual_delay_seconds = (resolved_available_at - resolved_source_event_time).total_seconds()
    spool.publish(
        FeatureBatchEnvelope(
            schema_version=2,
            batch_id=f"feature-{sequence}",
            contract_id="intraday-pit",
            contract_version=3,
            input_batch_ids=(f"minute-{sequence}", "history-snapshot"),
            sequence=sequence,
            event_time=resolved_source_event_time,
            available_at=resolved_available_at,
            decision_cutoff=resolved_decision_cutoff,
            actual_delay_seconds=actual_delay_seconds,
            row_count=1,
            content_hash=hashlib.sha256(payload).hexdigest(),
            field_statuses=tuple(
                FeatureFieldStatus(
                    candidate_id="600000.SH",
                    name=name,
                    status=(
                        FeatureAvailability.UNAVAILABLE
                        if pd.isna(frame.iloc[0][name])
                        else FeatureAvailability.AVAILABLE
                    ),
                    source_event_time=resolved_source_event_time,
                    available_at=resolved_available_at,
                    decision_cutoff=resolved_decision_cutoff,
                    actual_delay_seconds=actual_delay_seconds,
                    reason=(
                        "not applicable to entry state" if pd.isna(frame.iloc[0][name]) else None
                    ),
                )
                for name in frame.columns
                if name != "ts_code"
            ),
            producer_commit=COMMIT,
        ),
        payload,
    )


def _publish_candidates(
    root: Path,
    *,
    strategy_id: str = "n_shape",
    producer_commit: str = COMMIT,
    trade_date: date = date(2026, 7, 31),
    captured_at: datetime = NOW,
    definition_fingerprint: str,
    executable_fingerprint: str,
    candidate_schema_fingerprint: str,
) -> None:
    from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

    decision_at = captured_at - timedelta(days=1)
    static_features_by_strategy: dict[str, dict[str, object]] = {
        "n_shape": {
            "candidate_price_basis": "raw_session",
            "limit_pct": 0.2,
            "limit_up_price_session_raw": 12.0,
            "t_close_session_raw": 10.0,
            "t_high_session_raw": 11.0,
        },
        "growth_board_surge": {
            "candidate_price_basis": "raw_session",
            "session_pre_close_raw": 10.0,
            "limit_up_price_session_raw": 12.0,
            "board_type": "gem",
            "ma_alignment": True,
            "large_net_vol_t1": 1.0,
        },
        "auction_gap": {
            "candidate_price_basis": "raw_session",
            "auction_price_raw": 10.5,
            "auction_vol_ratio_5d": 0.2,
            "gap_pct_close": 0.05,
            "limit_up_price_session_raw": 12.0,
        },
    }
    try:
        static_features = static_features_by_strategy[strategy_id]
    except KeyError as exc:
        raise ValueError(f"unsupported test strategy: {strategy_id}") from exc
    definition = BuiltinStrategyEvaluatorRegistry(producer_commit=producer_commit).load_definition(
        strategy_id, 1
    )
    StrategyCandidateSnapshotSpool(root.resolve()).publish_strategy_records(
        strategy_id=strategy_id,
        strategy_version="1",
        definition_fingerprint=definition_fingerprint,
        executable_fingerprint=executable_fingerprint,
        candidate_schema_fingerprint=candidate_schema_fingerprint,
        static_feature_schema={
            name: semantic.contract_payload()
            for name, semantic in definition.static_feature_schema.items()
        },
        source_snapshot_ids={"candidate_input": "e" * 64},
        trade_date=trade_date,
        captured_at=captured_at,
        producer_commit=producer_commit,
        rows=(
            StrategyCandidateRecord(
                strategy_id=strategy_id,
                strategy_version="1",
                candidate_id="600000.SH",
                variant="default",
                decision_at=decision_at,
                available_at=decision_at + timedelta(minutes=1),
                effective_trade_date=trade_date,
                reference_trade_date=date(2026, 7, 30),
                price_basis=StrategyCandidatePriceBasis.RAW,
                static_features=static_features,
                reference_snapshot_ids={"daily": "d" * 64},
            ),
        ),
    )


def _publish_builtin_registration(
    tmp_path: Path,
    *,
    strategy_id: str = "n_shape",
    producer_commit: str = COMMIT,
) -> tuple[Path, StrategySpecRegistration]:
    from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

    builtin = BuiltinStrategyEvaluatorRegistry(producer_commit=producer_commit)
    execution_registry = builtin.trusted_executable_registry()
    root = tmp_path / "definitions"
    definitions = ImmutableDefinitionRegistry(root, execution_registry=execution_registry)
    feature_names = sorted(
        {
            requirement.name
            for definition in builtin.definitions.values()
            for requirement in (
                *definition.spec.required_features,
                *definition.spec.optional_features,
            )
        }
    )
    parent = None
    for version in (1, 2, 3):
        contract = FeatureContract(
            contract_id="intraday-pit",
            version=version,
            features=tuple(
                FeatureDefinition(
                    name=name,
                    dtype="object",
                    source_datasets=("market_minute",),
                    lookback=90,
                    pit_rule="available_at <= decision_time",
                    price_basis="raw",
                    availability_contract={
                        "source_available_at_basis": "max_source_available_at",
                        "max_delay_seconds": 60,
                        "missing_policy": "mark_unavailable",
                        "late_policy": "mark_stale",
                        "decision_visibility_gate": "available_at_lte_decision_time",
                    },
                )
                for name in feature_names
            ),
            producer_commit=producer_commit,
        )
        parent = definitions.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=producer_commit,
            expected_fingerprint=contract.contract_fingerprint,
            parent_fingerprint=None if parent is None else parent.fingerprint,
            supersedes=None if parent is None else parent.version,
            replacement_reason=None if parent is None else "contract evolution",
        )
    assert parent is not None
    spec = builtin.load_spec(strategy_id, 1)
    registration = definitions.register_strategy_spec(
        spec,
        feature_contract_fingerprint=parent.fingerprint,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=producer_commit,
        expected_fingerprint=spec.spec_fingerprint,
    )
    return root, registration


def test_strategy_builder_maps_sequences_generation_backlog_and_replay(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path, batch_limit=1)
    feature_spool = FeatureBatchSpool(tmp_path / "features")
    _publish(feature_spool, sequence=0)
    _publish(feature_spool, sequence=1)
    _publish_candidates(
        tmp_path / "candidates",
        definition_fingerprint=str(manifest.settings["strategy_registration_fingerprint"]),
        executable_fingerprint=str(manifest.settings["strategy_executable_fingerprint"]),
        candidate_schema_fingerprint=str(manifest.settings["candidate_schema_fingerprint"]),
    )
    step = strategy_live_builder(clock=lambda: NOW + timedelta(minutes=1))(manifest)

    first = step()
    second = step()
    replay = step()

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


def test_strategy_builder_publishes_completion_only_after_router_drain(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path, completion_authority=True)
    feature_spool = FeatureBatchSpool(tmp_path / "features")
    _publish(
        feature_spool,
        sequence=0,
        available_at=SESSION_CLOSE,
        source_event_time=SESSION_CLOSE,
        decision_cutoff=SESSION_CLOSE,
    )
    feature_spool.publish_session_close_marker(
        trade_date=date(2026, 7, 31),
        session_close_at=SESSION_CLOSE,
        produced_at=SESSION_CLOSE + timedelta(seconds=1),
        calendar_generation_id=str(manifest.settings["calendar_content_sha256"]),
        complete_through=SESSION_CLOSE,
        upstream_source_generation_id="f" * 64,
        upstream_final_sequence=0,
        upstream_final_batch_id="raw-0",
        upstream_final_content_hash="e" * 64,
    )
    _publish_candidates(
        tmp_path / "candidates",
        captured_at=SESSION_CLOSE,
        definition_fingerprint=str(manifest.settings["strategy_registration_fingerprint"]),
        executable_fingerprint=str(manifest.settings["strategy_executable_fingerprint"]),
        candidate_schema_fingerprint=str(manifest.settings["candidate_schema_fingerprint"]),
    )
    bus_path = Path(str(manifest.settings["signal_bus_path"]))
    bus = SignalBusStore(bus_path)
    observed = [SESSION_CLOSE + timedelta(seconds=3)]
    attestation_authority = create_shadow_ed25519_test_authority(
        tmp_path / "completion-keys"
    )
    step = strategy_live_builder(
        clock=lambda: observed[0],
        completion_attestation_signer=attestation_authority.signer,
        completion_attestation_active_key_id=attestation_authority.keyring.active_key_id,
    )(manifest)

    first = step()

    assert first.output_sequence == 1
    source = ReadonlyStrategyRunnerSignalSource(
        source_id=manifest.service_id,
        path=Path(str(manifest.settings["runner_state_path"])),
        expected_strategy_spec_fingerprint=(
            strategy_evaluators_module.BuiltinStrategyEvaluatorRegistry(producer_commit=COMMIT)
            .load_spec("n_shape", 1)
            .spec_fingerprint
        ),
        expected_evaluator_contract_fingerprint=str(
            manifest.settings["strategy_executable_fingerprint"]
        ),
    )
    with pytest.raises(ValueError, match="completion receipt"):
        source.read_completion_receipt(trade_date=date(2026, 7, 31))

    route_runner_signals(
        source_id=manifest.service_id,
        source=source,
        bus=bus,
        cursors=SignalRouteCursorStore(
            bus_path,
            routing_policy_fingerprint=str(manifest.settings["routing_policy_fingerprint"]),
        ),
        routed_at=SESSION_CLOSE + timedelta(seconds=4),
        target_resolver=lambda _signal: RoutingDecision.no_target(
            routing_policy_fingerprint=str(manifest.settings["routing_policy_fingerprint"]),
            reason_code="shadow_only",
        ),
        limit=10,
    )
    observed[0] = SESSION_CLOSE + timedelta(seconds=5)

    second = step()
    receipt = source.read_completion_receipt(trade_date=date(2026, 7, 31))

    assert second.output_sequence == 1
    assert receipt.source_id == manifest.service_id
    assert receipt.producer_service_id == manifest.service_id
    assert receipt.producer_instance_id == "test-strategy-primary"
    assert receipt.calendar_generation_id == manifest.settings["calendar_content_sha256"]
    assert receipt.completion_attestation is not None
    assert attestation_authority.keyring.verify(receipt.completion_attestation)
    assert (
        receipt.completion_attestation.claims.strategy_registration_fingerprint
        == manifest.settings["strategy_registration_fingerprint"]
    )
    assert (
        receipt.completion_attestation.claims.producer_manifest_fingerprint
        == manifest.manifest_fingerprint
    )


def test_strategy_builder_rejects_completion_authority_without_attestation_signer(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="attestation signer"):
        strategy_live_builder(clock=lambda: NOW)(_manifest(tmp_path, completion_authority=True))


def test_strategy_builder_rejects_hmac_completion_attestation_for_production(
    tmp_path: Path,
) -> None:
    hmac_authority = HmacCompletionAttestationAuthority(
        key_id="test-runtime-completion",
        secret=b"runtime-completion-test-secret-32b",
    )

    with pytest.raises(ValueError, match="Ed25519"):
        strategy_live_builder(
            clock=lambda: NOW,
            completion_attestation_signer=hmac_authority,
            completion_attestation_active_key_id=hmac_authority.key_id,
        )(_manifest(tmp_path, completion_authority=True))


def test_strategy_builder_rejects_previous_completion_signer_for_new_receipts(
    tmp_path: Path,
) -> None:
    authority = create_rotating_shadow_ed25519_test_authority(tmp_path / "rotating-completion")

    with pytest.raises(ValueError, match="active"):
        strategy_live_builder(
            clock=lambda: NOW,
            completion_attestation_signer=authority.previous_signer,
            completion_attestation_active_key_id=authority.keyring.active_key_id,
        )(_manifest(tmp_path, completion_authority=True))


@pytest.mark.parametrize(
    "strategy_id",
    ("n_shape", "growth_board_surge", "auction_gap"),
)
def test_strategy_builder_uses_real_paper_fill_for_holding_and_t_plus_one_exit(
    tmp_path: Path,
    strategy_id: str,
) -> None:
    manifest_payload = _manifest(
        tmp_path,
        batch_limit=1,
        strategy_id=strategy_id,
    ).model_dump(mode="json")
    manifest_payload["settings"]["candidate_max_age_seconds"] = 120
    manifest = RuntimeServiceManifest.model_validate(manifest_payload)
    feature_spool = FeatureBatchSpool(tmp_path / "features")
    _publish(feature_spool, sequence=0, strategy_id=strategy_id)
    _publish_candidates(
        tmp_path / "candidates",
        strategy_id=strategy_id,
        definition_fingerprint=str(manifest.settings["strategy_registration_fingerprint"]),
        executable_fingerprint=str(manifest.settings["strategy_executable_fingerprint"]),
        candidate_schema_fingerprint=str(manifest.settings["candidate_schema_fingerprint"]),
    )
    current_time = [NOW + timedelta(seconds=1)]
    step = strategy_live_builder(clock=lambda: current_time[0])(manifest)

    first = step()
    assert first.output_sequence == 1
    entry_feature_sequence = 0
    expected_signal_sequence = 1
    if strategy_id == "auction_gap":
        with sqlite3.connect(tmp_path / "runner.sqlite3") as connection:
            watch_payload = connection.execute(
                "SELECT payload_json FROM runner_signal ORDER BY sequence DESC LIMIT 1"
            ).fetchone()[0]
        assert SignalEnvelope.model_validate_json(watch_payload).action is SignalAction.WATCH
        _publish(feature_spool, sequence=1, strategy_id=strategy_id)
        current_time[0] = NOW + timedelta(seconds=2)
        armed = step()
        assert armed.output_sequence == 2
        entry_feature_sequence = 1
        expected_signal_sequence = 2
    with sqlite3.connect(tmp_path / "runner.sqlite3") as connection:
        payload = connection.execute(
            "SELECT payload_json FROM runner_signal ORDER BY sequence DESC LIMIT 1"
        ).fetchone()[0]
    entry_signal = SignalEnvelope.model_validate_json(payload)
    assert entry_signal.action is SignalAction.B_INTENT

    broker = PaperBrokerStore(
        tmp_path / "paper-broker.sqlite3",
        account_id="paper-main",
        initial_cash=Decimal("100000"),
        cost_policy=BrokerCostPolicy(
            commission_rate=Decimal("0.0003"),
            minimum_commission=Decimal("5"),
            sell_stamp_tax_rate=Decimal("0.001"),
        ),
    )
    intent_available = current_time[0] + timedelta(seconds=1)
    broker.submit_intent(
        PaperOrderIntent(
            signal_id=entry_signal.signal_id,
            account_id="paper-main",
            ts_code="600000.SH",
            side=PaperSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=1000,
            event_time=current_time[0],
            available_at=intent_available,
            expires_at=intent_available + timedelta(minutes=5),
            earliest_execution_at=intent_available,
            price_snapshot_id="f" * 64,
            producer_commit=COMMIT,
        ),
        decision_time=intent_available,
        trade_date=date(2026, 7, 31),
        quote=BrokerExecutionContext(
            executable_price=Decimal("11.00"),
            acquisition_available_date=date(2026, 8, 3),
        ),
    )

    next_trade_time = datetime(2026, 8, 3, 1, 40, tzinfo=UTC)
    _publish_candidates(
        tmp_path / "candidates",
        strategy_id=strategy_id,
        trade_date=date(2026, 8, 3),
        captured_at=next_trade_time,
        definition_fingerprint=str(manifest.settings["strategy_registration_fingerprint"]),
        executable_fingerprint=str(manifest.settings["strategy_executable_fingerprint"]),
        candidate_schema_fingerprint=str(manifest.settings["candidate_schema_fingerprint"]),
    )
    holding_feature_sequence = entry_feature_sequence + 1
    _publish(
        feature_spool,
        sequence=holding_feature_sequence,
        strategy_id=strategy_id,
        available_at=next_trade_time,
        session_high=14.0,
    )
    current_time[0] = next_trade_time + timedelta(seconds=1)
    holding = step()
    assert holding.output_sequence == expected_signal_sequence
    with sqlite3.connect(tmp_path / "runner.sqlite3") as connection:
        state = connection.execute(
            "SELECT state, eligible_high_price_raw FROM candidate_state "
            "ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        lifecycle_result = connection.execute(
            "SELECT result_json FROM processed_batch WHERE feature_sequence = ?",
            (holding_feature_sequence,),
        ).fetchone()[0]
    assert state[0] == StrategyLifecycleState.HOLDING.value
    assert state[1] == pytest.approx(14.0)
    assert (
        len(
            StrategyBatchResult.model_validate_json(lifecycle_result).lifecycle_feature_fingerprints
        )
        == 1
    )

    exit_time = next_trade_time + timedelta(seconds=30)
    _publish(
        feature_spool,
        sequence=holding_feature_sequence + 1,
        strategy_id=strategy_id,
        latest_close=9.50,
        available_at=exit_time,
    )
    current_time[0] = exit_time + timedelta(seconds=1)
    exited = step()

    assert exited.output_sequence == expected_signal_sequence + 1
    with sqlite3.connect(tmp_path / "runner.sqlite3") as connection:
        payload = connection.execute(
            "SELECT payload_json FROM runner_signal ORDER BY sequence DESC LIMIT 1"
        ).fetchone()[0]
        terminal_high = connection.execute(
            "SELECT eligible_high_price_raw FROM candidate_state ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()[0]
    exit_signal = SignalEnvelope.model_validate_json(payload)
    assert exit_signal.action is SignalAction.S_INTENT
    assert exit_signal.reason_codes == ("t_plus_one_stop",)
    assert terminal_high == pytest.approx(14.0)


@pytest.mark.parametrize(
    ("strategy_id", "strategy_version"),
    [
        ("n_shape", 1),
        ("growth_board_surge", 1),
        ("auction_gap", 1),
    ],
)
def test_strategy_builder_default_binds_each_builtin_at_manifest_commit(
    tmp_path: Path,
    strategy_id: str,
    strategy_version: int,
) -> None:
    producer_commit = "c" * 40
    manifest = _manifest(
        tmp_path,
        producer_commit=producer_commit,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
    )

    step = strategy_live_builder(clock=lambda: NOW)(manifest)

    assert callable(step)


def test_strategy_builder_loads_published_executable_registration(tmp_path: Path) -> None:
    root, registration = _publish_builtin_registration(tmp_path)
    manifest = _manifest(
        tmp_path,
        strategy_id="n_shape",
        strategy_version=1,
        definition_registry_root=root,
        registration_fingerprint=registration.fingerprint,
    )

    step = strategy_live_builder(clock=lambda: NOW + timedelta(seconds=1))(manifest)

    assert callable(step)


def test_strategy_builder_rejects_runtime_dispatcher_not_bound_by_registration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

    original = BuiltinStrategyEvaluatorRegistry.load_definition

    def replaced_definition(
        self: BuiltinStrategyEvaluatorRegistry,
        strategy_id: str,
        strategy_version: int,
    ) -> object:
        definition = original(self, strategy_id, strategy_version)
        return replace(definition, evaluator=_replacement_runtime_dispatcher)

    monkeypatch.setattr(BuiltinStrategyEvaluatorRegistry, "load_definition", replaced_definition)

    with pytest.raises(ValueError, match="evaluator fingerprint"):
        strategy_live_builder(clock=lambda: NOW)(_manifest(tmp_path))


def test_strategy_builder_rejects_rebound_internal_evaluator_helper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, registration = _publish_builtin_registration(tmp_path)
    manifest = _manifest(
        tmp_path,
        definition_registry_root=root,
        registration_fingerprint=registration.fingerprint,
    )
    monkeypatch.setattr(
        strategy_evaluators_module,
        "_validate_session_geometry",
        _replacement_session_geometry,
    )

    with pytest.raises(ValueError, match="evaluator fingerprint"):
        strategy_live_builder(clock=lambda: NOW)(manifest)


def test_strategy_builder_requires_readonly_paper_lifecycle_source(tmp_path: Path) -> None:
    payload = _manifest(tmp_path).model_dump(mode="json")
    payload["settings"]["paper_broker_path"] = str(tmp_path / "paper-broker.sqlite3")
    payload["settings"]["paper_account_id"] = "paper-main"

    step = strategy_live_builder(clock=lambda: NOW)(RuntimeServiceManifest.model_validate(payload))

    assert callable(step)


def test_strategy_builder_default_fails_closed_for_unknown_builtin_identity(
    tmp_path: Path,
) -> None:
    payload = _manifest(tmp_path).model_dump(mode="json")
    payload["settings"]["strategy_registration_fingerprint"] = "0" * 64
    manifest = RuntimeServiceManifest.model_validate(payload)

    with pytest.raises(ValueError, match="published strategy registration"):
        strategy_live_builder(clock=lambda: NOW)(manifest)


def test_strategy_builder_default_rejects_builtin_spec_mismatch(tmp_path: Path) -> None:
    payload = _manifest(tmp_path).model_dump(mode="json")
    payload["settings"]["strategy_id"] = "growth_board_surge"
    manifest = RuntimeServiceManifest.model_validate(payload)

    with pytest.raises(ValueError, match="registration identity"):
        strategy_live_builder(clock=lambda: NOW)(manifest)


def test_strategy_builder_rejects_injected_evaluator_loader(
    tmp_path: Path,
) -> None:
    with pytest.raises(TypeError, match="not trusted"):
        strategy_live_builder(
            evaluator_loader=lambda *_args: _binding(),
            clock=lambda: NOW,
        )(_manifest(tmp_path))


def test_strategy_builder_defers_future_feature_and_reports_exact_backlog(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    feature_spool = FeatureBatchSpool(tmp_path / "features")
    _publish(feature_spool, sequence=0)
    current_time = [NOW + timedelta(seconds=1)]
    step = strategy_live_builder(clock=lambda: current_time[0])(manifest)
    current_time[0] = NOW - timedelta(seconds=1)

    result = step()

    assert result.input_sequence == -1
    assert result.output_sequence == 0
    assert result.processed_count == 0
    assert result.backlog_count == 1
    assert result.degraded_reasons == ()


def test_strategy_builder_rejects_feature_cutoff_after_runner_decision(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    feature_spool = FeatureBatchSpool(tmp_path / "features")
    _publish(
        feature_spool,
        sequence=0,
        available_at=NOW,
        decision_cutoff=NOW + timedelta(seconds=2),
    )
    _publish_candidates(
        tmp_path / "candidates",
        definition_fingerprint=str(manifest.settings["strategy_registration_fingerprint"]),
        executable_fingerprint=str(manifest.settings["strategy_executable_fingerprint"]),
        candidate_schema_fingerprint=str(manifest.settings["candidate_schema_fingerprint"]),
    )
    step = strategy_live_builder(clock=lambda: NOW + timedelta(seconds=1))(manifest)

    with pytest.raises(ValueError, match="decision_cutoff.*future"):
        step()


def test_strategy_builder_rejects_available_feature_over_contract_max_delay(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    feature_spool = FeatureBatchSpool(tmp_path / "features")
    _publish(
        feature_spool,
        sequence=0,
        available_at=NOW,
        source_event_time=NOW - timedelta(seconds=61),
    )
    _publish_candidates(
        tmp_path / "candidates",
        definition_fingerprint=str(manifest.settings["strategy_registration_fingerprint"]),
        executable_fingerprint=str(manifest.settings["strategy_executable_fingerprint"]),
        candidate_schema_fingerprint=str(manifest.settings["candidate_schema_fingerprint"]),
    )
    step = strategy_live_builder(clock=lambda: NOW + timedelta(seconds=1))(manifest)

    with pytest.raises(ValueError, match="max_delay_seconds"):
        step()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("feature_spool_root", "relative/features"),
        ("runner_state_path", "runner.sqlite3"),
        ("definition_registry_root", "definitions"),
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
        strategy_live_builder(clock=lambda: NOW)(manifest)


@pytest.mark.parametrize(
    "field",
    (
        "feature_spool_root",
        "runner_state_path",
        "definition_registry_root",
        "candidate_snapshot_root",
    ),
)
def test_strategy_builder_requires_normalized_runtime_paths(
    tmp_path: Path,
    field: str,
) -> None:
    payload = _manifest(tmp_path).model_dump(mode="json")
    payload["settings"][field] = os.path.join(str(tmp_path), "nested", "..", "value")
    traversal = RuntimeServiceManifest.model_validate(payload)
    with pytest.raises(ValidationError, match="normalized"):
        strategy_live_builder(clock=lambda: NOW)(traversal)


def test_strategy_builder_requires_positive_candidate_age(tmp_path: Path) -> None:
    payload = _manifest(tmp_path).model_dump(mode="json")
    payload["settings"]["candidate_max_age_seconds"] = 0
    invalid_age = RuntimeServiceManifest.model_validate(payload)
    with pytest.raises(ValidationError, match="greater than 0"):
        strategy_live_builder(clock=lambda: NOW)(invalid_age)


def test_strategy_builder_binds_candidate_authority_to_manifest_commit(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    feature_spool = FeatureBatchSpool(tmp_path / "features")
    _publish(feature_spool, sequence=0)
    _publish_candidates(
        tmp_path / "candidates",
        producer_commit="f" * 40,
        definition_fingerprint=str(manifest.settings["strategy_registration_fingerprint"]),
        executable_fingerprint=str(manifest.settings["strategy_executable_fingerprint"]),
        candidate_schema_fingerprint=str(manifest.settings["candidate_schema_fingerprint"]),
    )
    step = strategy_live_builder(clock=lambda: NOW)(manifest)

    with pytest.raises(RuntimeCandidateUniverseIntegrityError, match="producer commit"):
        step()

    assert feature_spool.load_cursor("strategy:n_shape:1") is None


def test_strategy_builder_rejects_wrong_kind_plane_and_dynamic_import_setting(
    tmp_path: Path,
) -> None:
    builder = strategy_live_builder(clock=lambda: NOW)

    with pytest.raises(ValueError, match="kind"):
        builder(_manifest(tmp_path, kind=RuntimeServiceKind.FEATURE_LIVE))
    with pytest.raises(ValueError, match="live plane"):
        builder(_manifest(tmp_path, plane=RuntimeServicePlane.RESEARCH))

    payload = _manifest(tmp_path).model_dump(mode="json")
    payload["settings"]["evaluator_import"] = "unsafe.module:evaluate"
    dynamic_import = RuntimeServiceManifest.model_validate(payload)
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        builder(dynamic_import)


def test_strategy_builder_rejects_registration_from_another_manifest_commit(
    tmp_path: Path,
) -> None:
    root, registration = _publish_builtin_registration(tmp_path)
    payload = _manifest(
        tmp_path,
        definition_registry_root=root,
        registration_fingerprint=registration.fingerprint,
    ).model_dump(mode="json")
    payload["producer_commit"] = "c" * 40

    with pytest.raises(ValueError, match="allowlist|producer commit|registration"):
        strategy_live_builder(clock=lambda: NOW)(RuntimeServiceManifest.model_validate(payload))


def test_strategy_builder_rejects_manifest_candidate_schema_fingerprint_forgery(
    tmp_path: Path,
) -> None:
    payload = _manifest(tmp_path).model_dump(mode="json")
    payload["settings"]["candidate_schema_fingerprint"] = "0" * 64
    forged = RuntimeServiceManifest.model_validate(payload)

    with pytest.raises(ValueError, match="candidate schema fingerprint"):
        strategy_live_builder(clock=lambda: NOW)(forged)


def test_strategy_builder_rejects_manifest_executable_fingerprint_forgery(
    tmp_path: Path,
) -> None:
    payload = _manifest(tmp_path).model_dump(mode="json")
    payload["settings"]["strategy_executable_fingerprint"] = "0" * 64
    forged = RuntimeServiceManifest.model_validate(payload)

    with pytest.raises(ValueError, match="evaluator fingerprint|executable fingerprint"):
        strategy_live_builder(clock=lambda: NOW)(forged)


def test_strategy_builder_rejects_symlinked_definition_registry_root(tmp_path: Path) -> None:
    root, registration = _publish_builtin_registration(tmp_path)
    linked = tmp_path / "linked-definitions"
    linked.symlink_to(root, target_is_directory=True)
    manifest = _manifest(
        tmp_path,
        definition_registry_root=linked,
        registration_fingerprint=registration.fingerprint,
    )

    with pytest.raises(DefinitionIntegrityError, match="symlink|unsafe|registry"):
        strategy_live_builder(clock=lambda: NOW)(manifest)
