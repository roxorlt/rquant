from __future__ import annotations

from pathlib import Path

import pytest


def test_condition_builder_uses_current_source_contract_and_original_borrowed_ledger() -> None:
    import pytest
    from pydantic import ValidationError

    from rquant.runtime_builder_condition_alert import (
        ConditionEvaluationSettings,
        condition_evaluation_contract_sha256,
        condition_routing_contract_sha256,
    )

    assert len(condition_evaluation_contract_sha256()) == 64
    assert len(condition_routing_contract_sha256()) == 64
    with pytest.raises(ValidationError):
        ConditionEvaluationSettings(
            scope_serving_root=Path("relative"),
            calendar_path=Path("relative"),
            calendar_expected_commit="a" * 40,
            calendar_content_sha256="b" * 64,
        )


@pytest.mark.parametrize("current_source", ["same", "changed", "incomplete"])
def test_published_consumer_proof_binds_actual_source_after_new_serving_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, current_source: str
) -> None:
    from datetime import timedelta
    from hashlib import sha256

    from rquant.condition_alert_runtime_projection import (
        ConditionConsumerProof,
        condition_consumer_ready,
    )
    from rquant.runtime_builder_condition_alert import (
        condition_evaluation_contract_sha256,
        condition_routing_contract_sha256,
    )
    from rquant.runtime_contracts import canonical_sha256
    from rquant.screen.intraday_reference import IntradayReferenceSnapshot
    from rquant.screen.intraday_source import intraday_projections
    from rquant.serving_contracts import (
        FreshnessStatus,
        ServingDatasetWatermark,
        ServingGenerationManifest,
    )
    from rquant.serving_publisher import ServingPublisher, ServingReader
    from rquant.serving_read_models import (
        SERVING_TABLE_SPECS,
        ServingProjectionInput,
        ServingProjectionPayload,
        ServingReadModelInput,
        build_serving_read_models,
    )
    from rquant.web.serving import BorrowedGeneration
    from tests.unit.test_serving_screen_intraday import _source_world

    reader, cutoff = _source_world(tmp_path, monkeypatch)
    original = IntradayReferenceSnapshot.model_validate_json(
        reader.config.reference_snapshot_path.read_bytes()
    )

    def use_reference(*, complete: bool, changed: bool = False) -> None:
        value = IntradayReferenceSnapshot.model_validate(
            {
                **original.model_dump(mode="python"),
                "universe_codes": ("600000.SH",) if complete else original.universe_codes,
                "universe_source_id": "7" * 64 if changed else original.universe_source_id,
                "identity": None,
            }
        )
        path = tmp_path / f"reference-{complete}-{changed}.json"
        path.write_bytes(value.model_dump_json().encode())
        path.chmod(0o600)
        reader.config = reader.config.model_copy(
            update={
                "reference_snapshot_path": path,
                "reference_snapshot_sha256": sha256(path.read_bytes()).hexdigest(),
            }
        )

    # Only installed-schema evidence remains the explicit source-world double.
    use_reference(complete=True)
    first_source = reader(cutoff)
    root = tmp_path / "actual-serving"
    publisher = ServingPublisher(root, producer_commit="a" * 40, table_specs=SERVING_TABLE_SPECS)

    def publish(
        projections: tuple[ServingProjectionPayload, ...], sequence: int
    ) -> ServingGenerationManifest:
        observed = cutoff + timedelta(seconds=sequence)
        owner_generation = canonical_sha256({"signals": sequence})
        tables = build_serving_read_models(
            ServingReadModelInput(
                observed_at=observed,
                projections=tuple(
                    ServingProjectionInput.bind(
                        item, owner_dataset_id="signals", owner_generation_id=owner_generation
                    )
                    for item in projections
                ),
            )
        )
        return publisher.publish(
            tables,
            watermarks=(
                ServingDatasetWatermark(
                    dataset_id="signals",
                    generation_id=owner_generation,
                    event_time=observed,
                    published_at=observed,
                    sequence=sequence,
                    status=FreshnessStatus.FRESH,
                ),
            ),
            source_generations={"signals": owner_generation},
            built_at=observed,
        )

    first = publish(intraday_projections(first_source), 0)
    proof = ConditionConsumerProof(
        producer_manifest_sha256="a" * 64,
        notifier_manifest_sha256="b" * 64,
        evaluation_contract_sha256=condition_evaluation_contract_sha256(),
        routing_contract_sha256=condition_routing_contract_sha256(),
        frequency_policy_sha256="c" * 64,
        source_epoch="d" * 64,
        producer_generation_id="e" * 64,
        source_identity=first_source.source.source_identity,
        serving_generation_id=first.generation_id,
        inspected_at=cutoff,
        source_cutoff=cutoff,
        full_source_ready=True,
        feature_contract_version=4,
    )
    use_reference(complete=current_source != "incomplete", changed=current_source == "changed")
    actual = reader(cutoff)
    second = publish(
        (
            *intraday_projections(actual),
            ServingProjectionPayload(
                table_name="condition_alert_runtime_state",
                available_at=cutoff,
                rows=({"snapshot_key": "current", "body_json": proof.wire_bytes().decode()},),
            ),
            ServingProjectionPayload(table_name="condition_alert_runtime", available_at=cutoff),
            ServingProjectionPayload(
                table_name="condition_alert_runtime_event", available_at=cutoff
            ),
        ),
        1,
    )
    assert second.generation_id != proof.serving_generation_id
    with ServingReader(root).acquire_generation() as lease:
        cursor = lease.connection.cursor()
        try:
            borrowed = BorrowedGeneration(
                manifest=lease.manifest, pointer=lease.pointer, cursor=cursor, fallback_detail=None
            )
            assert condition_consumer_ready(borrowed, now=cutoff + timedelta(seconds=1)) is (
                current_source == "same"
            )
        finally:
            cursor.close()


def test_composed_original_price_builder_keeps_one_writer_cadence_and_independent_unknown_receipt(
    tmp_path: Path,
) -> None:
    import json
    from datetime import timedelta
    from hashlib import sha256
    from unittest.mock import patch

    from rquant.condition_alert_runtime import ConditionFrequencyPolicy
    from rquant.price_alert_runtime_contracts import verify_price_alert_activation
    from rquant.price_alert_runtime_store import PriceAlertRuntimeStore
    from rquant.runtime_builder_condition_alert import (
        condition_evaluation_contract_sha256,
        condition_routing_contract_sha256,
    )
    from rquant.runtime_builder_price_alert import price_alert_runtime_builder
    from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
    from tests.unit.test_price_alert_runtime_builders import runtime_fixture

    now, path, _, _, _, _, serving, calendar = runtime_fixture(tmp_path)
    value = json.loads(path.read_text())
    ledger = tmp_path / "composed.sqlite3"
    value["settings"]["ledger_path"] = str(ledger)
    value["settings"]["condition_alert_runtime_manifest_path"] = str(path)
    value["settings"]["condition_alert_runtime"] = {
        "source_id": "condition-runtime",
        "source_epoch": "b" * 64,
        "ledger_id": "d" * 64,
        "generation_id": "e" * 64,
        "evaluation_contract_sha256": condition_evaluation_contract_sha256(),
        "routing_policy_sha256": condition_routing_contract_sha256(),
        "frequency_policy_sha256": ConditionFrequencyPolicy().sha256,
        "recipient_policy_sha256": "c" * 64,
        "evaluation_enabled": True,
        "event_write_enabled": True,
    }
    value["settings"]["condition_alert"] = {
        "scope_serving_root": str(serving),
        "calendar_path": str(tmp_path / "calendar.json"),
        "calendar_expected_commit": "b" * 40,
        "calendar_content_sha256": calendar.content_sha256,
        "install_namespace": True,
    }
    path.write_text(json.dumps(value))
    path.chmod(0o600)
    manifest = RuntimeServiceManifest.model_validate_json(path.read_bytes())
    cap = verify_price_alert_activation(
        path,
        runtime_root=tmp_path,
        expected_manifest_sha256=sha256(path.read_bytes()).hexdigest(),
        expected_commit="b" * 40,
        expected_kind=RuntimeServiceKind.PRICE_ALERT_RUNTIME,
    )
    original = PriceAlertRuntimeStore.install(ledger, activation=cap)
    original.close()
    clock = [now]
    step = price_alert_runtime_builder(clock=lambda: clock[0], runtime_root=tmp_path)(manifest)
    try:
        assert step.condition_store.ledger is step.store
        first = step()
        saved = step.condition_store.latest_round()
        assert saved.input.source is None and saved.evaluated_at == now
        clock[0] = now + timedelta(seconds=1)
        assert step().degraded_reasons == ("price_alert:waiting_cadence",)
        assert step.condition_store.latest_round() == saved
        clock[0] = now + timedelta(seconds=5)
        assert step().processed_count == first.processed_count
        assert step.condition_store.latest_round().evaluated_at == clock[0]
        clock[0] = now + timedelta(seconds=10)
        with patch.object(
            step.condition_store,
            "record_unavailable",
            side_effect=RuntimeError("condition transaction failed"),
        ):
            failed = step()
        assert "condition_alert:evaluation_unavailable" in failed.degraded_reasons
        assert failed.processed_count == first.processed_count
        assert step.store.runtime_snapshot(observed_at=clock[0]).round.evaluated_at == clock[0]
    finally:
        step.close()
    reopened = PriceAlertRuntimeStore(ledger, activation=cap)
    reopened.close()


def test_original_paper_builder_condition_flag_consumes_nontrading_mixed_prefix_on_same_database(
    tmp_path: Path,
) -> None:
    import sqlite3

    from rquant.runtime_builder_paper import paper_consumer_builder
    from rquant.runtime_service_control import RuntimeServicePlane
    from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
    from tests.integration.test_screen_alert_consumer_chain import AT, condition_delivery_fixture

    state, _, _, routed = condition_delivery_fixture(tmp_path)
    manifest = RuntimeServiceManifest(
        service_id="paper.condition-test",
        service_kind=RuntimeServiceKind.PAPER_CONSUMER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=1,
        stale_after_seconds=10,
        producer_commit="a" * 40,
        settings={
            "signal_bus_path": str(tmp_path / "condition-bus.sqlite3"),
            "queue_path": str(tmp_path / "paper-queue.sqlite3"),
            "consumer_state_path": str(tmp_path / "paper-state.sqlite3"),
            "condition_history_enabled": True,
            "account_id": "paper-main",
            "execution_lag_seconds": 60,
            "buy_quantity": 1000,
            "reduce_quantity": 500,
            "sell_quantity": 1000,
            "limit": 10,
        },
    )
    step = paper_consumer_builder(clock=lambda: AT)(manifest)
    result = step()
    assert result.processed_count == 1
    assert step().processed_count == 0
    restarted = paper_consumer_builder(clock=lambda: AT)(manifest)
    assert restarted().processed_count == 0
    with sqlite3.connect(tmp_path / "paper-state.sqlite3") as connection:
        assert (
            connection.execute(
                "SELECT count(*) FROM paper_condition_non_trading_receipt"
            ).fetchone()[0]
            == 1
        )
    with sqlite3.connect(tmp_path / "paper-queue.sqlite3") as connection:
        assert connection.execute("SELECT count(*) FROM paper_signal_queue").fetchone()[0] == 0


def test_original_broker_keeps_bound_portfolio_control_and_condition_receipt_without_orders(
    tmp_path: Path,
) -> None:
    import sqlite3
    from contextlib import closing
    from datetime import timedelta
    from decimal import Decimal
    from uuid import uuid4

    from rquant.paper_broker import PaperBrokerStore
    from rquant.paper_operator import PaperOperatorControlStore
    from rquant.paper_operator_commands import SetPaperAccountPaused
    from rquant.paper_portfolio_models import PaperPortfolioConfiguration
    from rquant.paper_portfolio_runtime import PaperPortfolioRuntime, PaperPortfolioRuntimeCatalog
    from rquant.paper_portfolio_source import PaperPortfolioMaterialStore
    from rquant.paper_portfolio_state import PaperPortfolioStateStore
    from rquant.runtime_builder_paper import PaperBrokerSettings, paper_broker_builder
    from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
    from tests.paper_cost_fixtures import paper_cost_policy
    from tests.unit.test_paper_portfolio_core import config_data
    from tests.unit.test_runtime_builder_paper import _manifest
    from tests.unit.test_paper_signal_worker import _quote

    data = _manifest(tmp_path, RuntimeServiceKind.PAPER_BROKER).model_dump(mode="python")
    data["service_id"] = "paper.main.v1"
    data["settings"]["initial_cash"] = "1000"
    data["settings"]["signal_spool_root"] = str(tmp_path / "spool")
    data["settings"]["condition_history_enabled"] = True
    manifest = RuntimeServiceManifest.model_validate(data)
    settings = PaperBrokerSettings.model_validate(dict(manifest.settings))
    from tests.integration.test_screen_alert_consumer_chain import AT, condition_delivery_fixture

    condition_delivery_fixture(tmp_path)
    configured = config_data()
    configured["binding"]["manifest_fingerprint"] = manifest.manifest_fingerprint
    configured["configured_at"] = AT - timedelta(days=1)
    configuration = PaperPortfolioConfiguration.model_validate(configured)
    portfolio = PaperPortfolioStateStore(tmp_path / "portfolio.sqlite", configuration=configuration)
    operator = PaperOperatorControlStore(
        portfolio, root=tmp_path / "operator", clock=lambda: AT
    )
    value = SetPaperAccountPaused(
        command_id=str(uuid4()), requested_at=AT, generation_id="generation-a",
        account_id=configuration.binding.account_id,
        configuration_fingerprint=configuration.fingerprint,
        expected_sequence=0, expected_paused=True, paused=False,
    )
    control = operator.commit_confirmed_control(
        value, authenticated_actor_id="alice", original_command_id=value.command_id
    )
    operator.publish(control)
    runtime = PaperPortfolioRuntime(
        portfolio, operator=operator, materials=PaperPortfolioMaterialStore(portfolio),
        producer_commit=manifest.producer_commit,
    )
    catalog = PaperPortfolioRuntimeCatalog((runtime,))
    step = paper_broker_builder(
        clock=lambda: AT,
        quote_resolver=lambda signal, cutoff: _quote(price="1", available_at=cutoff),
        trade_date_resolver=lambda cutoff: cutoff.date(),
        portfolio_catalog=catalog,
    )(manifest)
    result = step()
    assert result.processed_count == 0
    assert result.watermark_advanced
    assert operator.current().status == "applied" and not operator.current().paused
    assert result.source_generations["paper_operator_control"] == control.fingerprint
    assert step().processed_count == 0
    restarted = paper_broker_builder(
        clock=lambda: AT,
        quote_resolver=lambda signal, cutoff: _quote(price="1", available_at=cutoff),
        trade_date_resolver=lambda cutoff: cutoff.date(),
        portfolio_catalog=catalog,
    )(manifest)
    assert not restarted().watermark_advanced
    with closing(sqlite3.connect(settings.consumer_state_path)) as connection:
        assert connection.execute(
            "SELECT count(*) FROM paper_condition_non_trading_receipt"
        ).fetchone()[0] == 1
    with closing(sqlite3.connect(settings.queue_path)) as connection:
        assert connection.execute("SELECT count(*) FROM paper_signal_queue").fetchone()[0] == 0
    with PaperBrokerStore.open_readonly(
        settings.broker_path, account_id=settings.account_id,
        initial_cash=Decimal("1000"), cost_policy=paper_cost_policy(),
    ) as broker:
        assert broker.reconcile().order_count == 0
