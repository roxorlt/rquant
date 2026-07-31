from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget, OutboxRecord, OutboxStatus
from rquant.paper_contracts import PaperAccountSnapshot, PaperHolding
from rquant.runtime_service_control import (
    RuntimeServiceHealth,
    RuntimeServicePlane,
    RuntimeServiceStatus,
)
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_publisher import ServingPublisher
from rquant.serving_read_models import (
    SERVING_TABLE_SPECS,
    ServingReadModelInput,
    ServingSignalRecord,
    build_serving_read_models,
)
from rquant.signal_bus import RouteReceiptDisposition, SignalRouteReceipt
from rquant.signal_contracts import SignalAction, SignalEnvelope

NOW = datetime(2026, 7, 31, 2, 31, tzinfo=UTC)


def _signal() -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="1",
        parameter_fingerprint="a" * 64,
        dataset_snapshot_id="b" * 64,
        feature_snapshot_id="c" * 64,
        event_time=NOW - timedelta(seconds=5),
        available_at=NOW,
        candidate_id="600000.SH",
        action=SignalAction.B_INTENT,
        reason_codes=("strong_support",),
        evidence={"score": 0.8},
        expires_at=NOW + timedelta(minutes=5),
        producer_commit="d" * 40,
    )


def _account() -> PaperAccountSnapshot:
    holding = PaperHolding(
        code="600000.SH",
        quantity=1_000,
        available_quantity=0,
        frozen_quantity=1_000,
        average_cost=Decimal("10"),
        market_price=Decimal("10.50"),
    )
    return PaperAccountSnapshot(
        account_id="paper-main",
        as_of_time=NOW,
        cash=Decimal("90000"),
        available_cash=Decimal("90000"),
        frozen_cash=Decimal("0"),
        holdings=(holding,),
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("500"),
        nav=Decimal("100500"),
    )


def test_builds_deterministic_page_tables_and_publishes_readonly_generation(tmp_path) -> None:
    signal = _signal()
    target = DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER)
    route = SignalRouteReceipt(
        source_id="n-shape-v1",
        source_sequence=1,
        signal_id=signal.signal_id,
        decision_fingerprint="e" * 64,
        disposition=RouteReceiptDisposition.ROUTED,
        target_manifest_hash="f" * 64,
        targets=(target,),
        target_count=1,
        routed_at=NOW,
    )
    delivery = OutboxRecord(
        signal_id=signal.signal_id,
        target=target,
        status=OutboxStatus.PENDING,
        expires_at=signal.expires_at,
        attempt_count=0,
        created_at=NOW,
        updated_at=NOW,
    )
    source = ServingReadModelInput(
        observed_at=NOW,
        signals=(ServingSignalRecord(global_sequence=1, signal=signal),),
        routes=(route,),
        deliveries=(delivery,),
        paper_accounts=(_account(),),
        runtime_services=(
            RuntimeServiceHealth(
                service_id="feature-live",
                plane=RuntimeServicePlane.LIVE,
                status=RuntimeServiceStatus.MISSING,
                stale=True,
                observed_at=NOW,
            ),
        ),
    )

    tables = build_serving_read_models(source)

    assert set(tables) == set(SERVING_TABLE_SPECS)
    assert tables["serving_status"].iloc[0]["signal_count"] == 1
    assert tables["signals"].iloc[0]["candidate_id"] == "600000.SH"
    assert tables["paper_holdings"].iloc[0]["frozen_quantity"] == 1_000
    assert tables["runtime_services"].iloc[0]["service_id"] == "feature-live"
    publisher = ServingPublisher(
        tmp_path / "serving",
        producer_commit="1" * 40,
        table_specs=SERVING_TABLE_SPECS,
    )
    manifest = publisher.publish(
        tables,
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="signal_bus",
                generation_id="2" * 64,
                event_time=NOW,
                published_at=NOW,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
            ServingDatasetWatermark(
                dataset_id="paper",
                generation_id="3" * 64,
                event_time=NOW,
                published_at=NOW,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"signal_bus": "2" * 64, "paper": "3" * 64},
        built_at=NOW,
    )

    with publisher.open_current_readonly() as connection:
        assert connection.execute("SELECT count(*) FROM signals").fetchone()[0] == 1
        assert connection.execute("SELECT nav FROM paper_accounts").fetchone()[0] == Decimal(
            "100500"
        )
    assert manifest.row_counts["lab_jobs"] == 0
    assert manifest.row_counts["promotions"] == 0


def test_serving_snapshot_rejects_evidence_from_the_future() -> None:
    payload = _signal().model_dump(mode="python", exclude={"signal_id"})
    payload["available_at"] = NOW + timedelta(seconds=1)
    signal = SignalEnvelope.model_validate(payload)

    try:
        ServingReadModelInput(
            observed_at=NOW,
            signals=(ServingSignalRecord(global_sequence=1, signal=signal),),
        )
    except ValueError as error:
        assert "future" in str(error)
    else:
        raise AssertionError("future serving evidence must be rejected")
