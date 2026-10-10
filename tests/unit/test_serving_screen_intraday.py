from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from rquant.screen.intraday_contracts import (
        IntradayFieldFact,
        IntradayScreenSnapshot,
        IntradayStockSnapshot,
    )


def test_v4_definitions_register_original_math_without_moving_legacy_strategies() -> None:
    from rquant.runtime_definition_bootstrap import plan_builtin_definitions

    plan = plan_builtin_definitions(producer_commit="a" * 40)
    assert plan.feature_contract_versions == (1, 2, 3, 4)
    assert len(plan.strategies) == 3


def _source_world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    import rquant.screen.intraday_source as source
    from rquant.feature_spool import FeatureBatchSpool
    from rquant.intraday_feature_engine import IntradayFeatureConfig, live_compute
    from rquant.live_contracts import LiveChannel
    from rquant.live_spool import LiveBatchSpool
    from rquant.market_minute_gateway import MarketMinuteGateway, MarketMinuteGatewayConfig
    from rquant.runtime_definition_bootstrap import (
        bootstrap_builtin_definitions,
        plan_builtin_definitions,
    )
    from rquant.schema_compatibility import (
        ConsumerCapabilityReceipt,
        ProductionConsumerCapability,
        ProductionConsumerRegistry,
    )
    from rquant.screen.intraday_contracts import IntradaySchemaEvidence
    from rquant.screen.intraday_reference import (
        IntradayReferenceObservation,
        IntradayReferenceSnapshot,
    )
    from tests.unit.test_intraday_feature_engine import _current_minutes, _historical_minutes

    cutoff = datetime(2026, 7, 31, 1, 40, 2, tzinfo=UTC)
    raw = LiveBatchSpool(tmp_path / "raw")
    gateway = MarketMinuteGateway(
        spool=raw,
        fetcher=lambda: _current_minutes().drop(columns="available_at"),
        config=MarketMinuteGatewayConfig(producer_version="offline-unit", producer_commit="a" * 40),
    )
    gateway.capture_once(received_at=cutoff)
    batch = raw.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)[0].envelope
    history_path = tmp_path / "history.parquet"
    history = _historical_minutes()
    history.to_parquet(history_path, index=False)
    history_path.chmod(0o600)
    history_id = hashlib.sha256(history_path.read_bytes()).hexdigest()
    feature_spool = FeatureBatchSpool(tmp_path / "features")
    current = _current_minutes()
    current["available_at"] = cutoff
    result = live_compute(
        current,
        history,
        decision_time=cutoff,
        input_available_at=cutoff,
        input_batch_ids=(history_id, batch.batch_id),
        sequence=0,
        config=IntradayFeatureConfig(
            producer_commit="a" * 40, contract_version=4, schema_version=3, lookback_sessions=2
        ),
    )
    feature_spool.publish(result.envelope, result.payload_bytes)
    definitions = tmp_path / "definitions"
    plan = plan_builtin_definitions(producer_commit="a" * 40)
    bootstrap_builtin_definitions(
        definitions,
        producer_commit="a" * 40,
        registered_at=cutoff - timedelta(days=1),
        available_at=cutoff - timedelta(days=1),
        expected_plan_id=plan.plan_id,
    )
    reference = IntradayReferenceSnapshot(
        trade_date=date(2026, 7, 31),
        available_at=cutoff,
        producer_commit="a" * 40,
        universe_source_id="b" * 64,
        universe_available_at=cutoff,
        universe_codes=("600000.SH", "600001.SH"),
        calendar_source_id="c" * 64,
        calendar_available_at=cutoff - timedelta(days=1),
        open_dates=(date(2026, 7, 30), date(2026, 7, 31)),
        observations=(
            IntradayReferenceObservation(
                ts_code="600000.SH",
                trade_date=date(2026, 7, 31),
                source_id="d" * 64,
                source_event_time=cutoff - timedelta(hours=1),
                available_at=cutoff - timedelta(hours=1),
                pre_close=10,
                up_limit=15,
                no_price_limit=False,
                float_shares=11000,
            ),
        ),
    )
    reference_path = tmp_path / "reference.json"
    reference_path.write_text(reference.model_dump_json())
    reference_path.chmod(0o600)
    capability = ProductionConsumerCapability(
        consumer_id="unit-notifier",
        service_id="unit-notifier",
        dataset_id="runtime.intraday_feature.batch-envelope",
        contract_fingerprint="e" * 64,
        code_commit="a" * 40,
        min_readable_schema_version=1,
        max_readable_schema_version=1,
        required_fields=(),
    )
    receipt = ConsumerCapabilityReceipt(
        consumer_id=capability.consumer_id,
        service_id=capability.service_id,
        dataset_id=capability.dataset_id,
        code_commit=capability.code_commit,
        min_readable_schema_version=1,
        max_readable_schema_version=1,
        required_fields=(),
        serving_physical_schema_fingerprint="e" * 64,
        observed_generation_id="f" * 64,
        available_at=cutoff - timedelta(days=1),
    )
    gate = source.IntradaySchemaGate(
        store_path=tmp_path / "schema.sqlite3",
        plan_id="f" * 64,
        registry=ProductionConsumerRegistry(registry_id="unit", consumers=(capability,)),
        consumer_id=capability.consumer_id,
        consumer_commit="a" * 40,
        installed_generation_id="f" * 64,
        declaration_fingerprint="e" * 64,
        declaration_version=1,
    )
    # Only the installed-schema dependency is a double; all spool and math evidence is real.
    monkeypatch.setattr(
        source,
        "read_intraday_schema_evidence",
        lambda gate, cutoff: IntradaySchemaEvidence(
            plan_id=gate.plan_id, revision=1, phase="cutover", consumer_receipt=receipt
        ),
    )
    config = source.IntradaySourceConfig(
        raw_spool_root=raw.root,
        feature_spool_root=feature_spool.root,
        cursor_root=tmp_path / "reader-cursors",
        definition_root=definitions,
        feature_definition_fingerprint=plan.feature_contract_fingerprints[-1],
        expected_feature_generation_id=feature_spool.source_descriptor().generation_id,
        expected_raw_generation_id=raw.source_descriptor(LiveChannel.MARKET_MINUTE).generation_id,
        feature_producer_commit="a" * 40,
        raw_producer_commit="a" * 40,
        reference_snapshot_path=reference_path,
        reference_snapshot_sha256=hashlib.sha256(reference_path.read_bytes()).hexdigest(),
        historical_snapshot_path=history_path,
        historical_snapshot_id=history_id,
        units_contract_id="1" * 64,
        minute_volume_unit="shares",
        minute_amount_unit="CNY",
        schema_gate=gate,
    )
    return source.IntradayScreenProjectionSource(config), cutoff


def test_original_spools_bind_same_cutoff_full_universe_and_observed_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.feature_contracts import FeatureAvailability

    reader, cutoff = _source_world(tmp_path, monkeypatch)
    snapshot = reader(cutoff)
    assert snapshot.source.missing_codes == ("600001.SH",)
    assert len(snapshot.stocks) == 2 and len(snapshot.market_rows) == 1
    facts = {item.name: item for item in snapshot.stocks[0].fields}
    assert facts["INTRADAY_SPEED_5M[0]"].value == pytest.approx(100 * (14 / 11 - 1))
    assert facts["INTRADAY_PCT_CHG[0]"].value == pytest.approx(40)
    assert facts["INTRADAY_TURNOVER_RATE[0]"].value == pytest.approx(10)
    assert facts["INTRADAY_LIMIT_DISTANCE[0]"].value == pytest.approx(100 / 15)
    assert all(item.status is FeatureAvailability.UNAVAILABLE for item in snapshot.stocks[1].fields)


def test_later_publisher_cutoff_does_not_refresh_old_minute_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.feature_contracts import FeatureAvailability

    reader, cutoff = _source_world(tmp_path, monkeypatch)
    snapshot = reader(cutoff + timedelta(seconds=61))
    facts = {item.name: item for item in snapshot.stocks[0].fields}
    assert facts["INTRADAY_PRICE[0]"].status is FeatureAvailability.STALE
    assert snapshot.market_rows[0].price is None


def test_changed_feature_generation_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader, cutoff = _source_world(tmp_path, monkeypatch)
    reader.config = reader.config.model_copy(update={"expected_feature_generation_id": "0" * 64})
    with pytest.raises(ValueError, match="generation"):
        reader(cutoff)


def test_original_signal_authority_publishes_three_tables_and_drops_failed_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.notification_state import NotificationStateStore
    from rquant.serving_page_projection_source import (
        DuckDBSignalPageProjectionSource,
        SignalPageProjectionProducer,
    )
    from tests.unit.test_serving_page_projection_source import _signal_projection_database

    reader, cutoff = _source_world(tmp_path, monkeypatch)
    database = tmp_path / "readonly-unit.duckdb"
    _signal_projection_database(database)
    store = NotificationStateStore(tmp_path / "notifications.sqlite3")
    producer = SignalPageProjectionProducer(
        source=DuckDBSignalPageProjectionSource(database), store=store, intraday_source=reader
    )
    result = producer.publish(cutoff)
    assert result.written
    published = {
        item.table_name: item
        for item in store.serving_snapshot(observed_at=cutoff, history_limit=1).payload.projections
    }
    assert len(published["intraday_screen_source"].rows) == 1
    assert len(published["intraday_feature_snapshot"].rows) == 2
    assert len(published["market_snapshot"].rows) == 1
    assert published["market_snapshot"].available_at == cutoff
    reader.config = reader.config.model_copy(update={"expected_feature_generation_id": "0" * 64})
    later = cutoff + timedelta(seconds=1)
    assert producer.publish(later).written
    failed = {
        item.table_name: item
        for item in store.serving_snapshot(observed_at=later, history_limit=1).payload.projections
    }
    assert all(
        not failed[name].rows
        for name in ("intraday_screen_source", "intraday_feature_snapshot")
    )
    assert failed["market_snapshot"] == published["market_snapshot"]


def _quote_world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    import pandas as pd

    import rquant.screen.intraday_source as source
    from rquant.live_contracts import LiveChannel
    from rquant.live_spool import LiveBatchSpool
    from rquant.watchlist_quote_gateway import WatchlistQuoteGateway, WatchlistQuoteGatewayConfig

    reader, cutoff = _source_world(tmp_path, monkeypatch)
    spool = LiveBatchSpool(tmp_path / "quote-spool")

    def provider(codes, *, timeout_seconds, on_started):
        on_started(cutoff)
        return pd.DataFrame(
            [
                {
                    "ts_code": code,
                    "price": 15.0,
                    "open": 10.0,
                    "high": 15.0,
                    "low": 10.0,
                    "volume": 2000.0,
                    "amount": 25000.0,
                    "source_observed_at": cutoff - timedelta(seconds=1),
                    "pre_close": 12.0,
                    "pct_chg": 25.0,
                    "up_limit": 16.0,
                    "no_price_limit": False,
                    "turnover_rate": 1.5,
                }
                for code in codes
            ]
        )

    gateway = WatchlistQuoteGateway(
        spool=spool,
        provider=provider,
        clock=lambda: cutoff,
        config=WatchlistQuoteGatewayConfig(
            producer_version="offline-unit",
            producer_commit="a" * 40,
            schema_version=3,
            rollout_mode="published",
            units_contract_id="2" * 64,
            volume_unit="shares",
            amount_unit="CNY",
        ),
    )
    gateway.capture_once(
        codes=("600000.SH", "600001.SH"),
        scheduled_at=cutoff,
        universe_as_of=cutoff,
        trade_date=cutoff.date(),
    )
    gate = reader.config.schema_gate.model_copy(
        update={"dataset_id": "runtime.watchlist_quote.batch-envelope"}
    )
    original = source.read_intraday_schema_evidence

    def schema(gate, *, cutoff):
        evidence = original(gate, cutoff=cutoff)
        return evidence.model_copy(
            update={
                "consumer_receipt": evidence.consumer_receipt.model_copy(
                    update={"dataset_id": gate.dataset_id}
                )
            }
        )

    monkeypatch.setattr(source, "read_intraday_schema_evidence", schema)
    reader.config = reader.config.model_copy(
        update={
            "quote_source": source.IntradayQuoteSourceConfig(
                spool_root=spool.root,
                producer_commit="a" * 40,
                expected_generation_id=spool.source_descriptor(
                    LiveChannel.WATCHLIST_QUOTE
                ).generation_id,
                units_contract_id="2" * 64,
                schema_gate=gate,
            )
        }
    )
    return reader, cutoff


def test_v3_quote_spool_binds_full_request_and_keeps_original_minute_math(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader, cutoff = _quote_world(tmp_path, monkeypatch)
    snapshot = reader(cutoff)
    assert snapshot.source.quote_kind == "quote_snapshot"
    facts = {item.name: item for item in snapshot.stocks[0].fields}
    assert facts["INTRADAY_PRICE[0]"].value == 15
    assert facts["INTRADAY_PCT_CHG[0]"].value == 25
    assert facts["INTRADAY_TURNOVER_RATE[0]"].value == 1.5
    assert facts["INTRADAY_SPEED_5M[0]"].value == pytest.approx(100 * (14 / 11 - 1))
    assert facts["INTRADAY_LIMIT_DISTANCE[0]"].value == pytest.approx(6.25)
    assert snapshot.market_rows[0].as_of == cutoff - timedelta(seconds=1)


def expanded_wire_snapshot(
    snapshot: IntradayScreenSnapshot, count: int = 8000
) -> IntradayScreenSnapshot:
    """Size/roundtrip fixture only; expanded codes are not a real authority or market claim."""
    from rquant.runtime_contracts import canonical_sha256
    from rquant.screen.intraday_contracts import IntradayScreenSnapshot, IntradayScreenSource

    codes = tuple(f"{600000 + index:06}.SH" for index in range(count))

    def stock_for(index: int, code: str) -> IntradayStockSnapshot:
        stock = snapshot.stocks[index % len(snapshot.stocks)]

        def facts(values: tuple[IntradayFieldFact, ...]) -> tuple[IntradayFieldFact, ...]:
            return tuple(
                fact.model_copy(
                    update={
                        "original_feature_status": fact.original_feature_status.model_copy(
                            update={"candidate_id": code}
                        )
                        if fact.original_feature_status
                        else None
                    }
                )
                for fact in values
            )

        return stock.model_copy(
            update={
                "ts_code": code,
                "fields": facts(stock.fields),
                "closed_bar": stock.closed_bar.model_copy(
                    update={"fields": facts(stock.closed_bar.fields)}
                )
                if stock.closed_bar
                else None,
            }
        )

    stocks = tuple(stock_for(index, code) for index, code in enumerate(codes))
    market = tuple(
        snapshot.market_rows[index % len(snapshot.market_rows)].model_copy(
            update={"ts_code": code, "price": 10 + index / 10000}
        )
        for index, code in enumerate(codes)
    )
    source = IntradayScreenSource.model_validate(
        snapshot.source.model_dump(exclude={"source_identity"})
        | {
            "universe_codes": codes,
            "universe_digest": canonical_sha256(codes),
            "missing_codes": (),
            "coverage_digest": canonical_sha256({"universe": codes, "missing": ()}),
            "stock_digest": canonical_sha256(stocks),
            "market_digest": canonical_sha256(market),
        }
    )
    return IntradayScreenSnapshot(source=source, stocks=stocks, market_rows=market)


def test_full_8000_wire_keeps_provenance_and_original_owner_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.screen.intraday_source import intraday_projections
    from rquant.notification_state import NotificationStateStore
    from rquant.serving_page_projection_source import (
        DuckDBSignalPageProjectionSource,
        SignalPageProjectionProducer,
    )
    from rquant.serving_read_models import (
        ServingProjectionInput,
        ServingReadModelInput,
        _projection_json_bytes,
    )
    from rquant.web.screen_intraday import read_intraday_screen_snapshot
    from tests.unit.test_web_screen_intraday import _borrow
    from tests.unit.test_serving_page_projection_source import _signal_projection_database

    reader, cutoff = _quote_world(tmp_path, monkeypatch)
    snapshot = expanded_wire_snapshot(reader(cutoff))
    projections = intraday_projections(snapshot)
    bound = tuple(
        ServingProjectionInput.bind(item, owner_dataset_id="signals", owner_generation_id="0" * 64)
        for item in projections
    )
    ServingReadModelInput(observed_at=cutoff, projections=bound)
    meter = sum(_projection_json_bytes(item) for item in bound)
    assert meter < 7 * 1024 * 1024
    assert (
        len(
            next(
                item for item in projections if item.table_name == "intraday_feature_snapshot"
            ).rows
        )
        == 8000
    )
    borrowed = _borrow(snapshot)
    try:
        actual = read_intraday_screen_snapshot(borrowed, now=cutoff)
        assert actual == snapshot
        assert actual.source.stock_digest == snapshot.source.stock_digest
        assert all(
            left.fields == right.fields
            for left, right in zip(actual.stocks, snapshot.stocks, strict=True)
        )
    finally:
        borrowed.cursor.close()
    print(f"size-only full 8000 source/stock/market owner bytes: {meter}")
    database = tmp_path / "publisher-readonly.duckdb"
    _signal_projection_database(database)
    store = NotificationStateStore(tmp_path / "publisher-notifications.sqlite3")
    producer = SignalPageProjectionProducer(
        source=DuckDBSignalPageProjectionSource(database),
        store=store,
        intraday_source=lambda _: snapshot,
    )
    producer.publish(cutoff)
    published = store.serving_snapshot(observed_at=cutoff, history_limit=1)
    owned = tuple(
        ServingProjectionInput.bind(
            item,
            owner_dataset_id="signals",
            owner_generation_id=published.projection_generation_id,
        )
        for item in published.payload.projections
    )
    ServingReadModelInput(observed_at=cutoff, projections=owned)
    actual_meter = sum(_projection_json_bytes(item) for item in owned)
    assert actual_meter < 7 * 1024 * 1024
    assert len(next(item for item in owned if item.table_name == "intraday_feature_snapshot").rows) == 8000
    assert next(item for item in owned if item.table_name == "market_snapshot").rows == projections[2].rows
    print(f"size-only full 8000 with all {len(owned)} original shared owner tables: {actual_meter}")


def test_source_wire_keeps_plain_v1_and_rejects_bad_source_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json

    from rquant.screen.intraday_contracts import (
        decode_intraday_source_wire,
        encode_intraday_source_wire,
    )

    reader, cutoff = _quote_world(tmp_path, monkeypatch)
    source = reader(cutoff).source
    assert decode_intraday_source_wire(source.model_dump_json()) == source
    encoded = encode_intraday_source_wire(source)
    assert decode_intraday_source_wire(encoded) == source
    invalid = json.loads(encoded)
    invalid["contract"] = "unknown/v1"
    with pytest.raises(ValueError):
        decode_intraday_source_wire(json.dumps(invalid))


def test_new_domain_overflow_preserves_old_market_and_shared_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.notification_state import NotificationStateStore
    from rquant.runtime_contracts import canonical_sha256
    from rquant.screen.intraday_contracts import IntradayScreenSnapshot, IntradayScreenSource
    from rquant.serving_page_projection_source import (
        DuckDBSignalPageProjectionSource,
        SignalPageProjectionProducer,
        _COMPANION_SIGNAL_TABLES,
    )
    from rquant.serving_read_models import ServingProjectionPayload
    from tests.unit.test_serving_page_projection_source import _signal_projection_database

    reader, cutoff = _quote_world(tmp_path, monkeypatch)
    snapshot = expanded_wire_snapshot(reader(cutoff))

    def crowded(
        values: tuple[IntradayFieldFact, ...], code: str, kind: str
    ) -> tuple[IntradayFieldFact, ...]:
        return tuple(
            fact.model_copy(update={"source_id": canonical_sha256((code, kind, fact.name))})
            for fact in values
        )

    stocks = tuple(
        stock.model_copy(
            update={
                "fields": crowded(stock.fields, stock.ts_code, "latest"),
                "closed_bar": stock.closed_bar.model_copy(
                    update={"fields": crowded(stock.closed_bar.fields, stock.ts_code, "bar")}
                )
                if stock.closed_bar
                else None,
            }
        )
        for stock in snapshot.stocks
    )
    source = IntradayScreenSource.model_validate(
        snapshot.source.model_dump(exclude={"source_identity"})
        | {"stock_digest": canonical_sha256(stocks)}
    )
    snapshot = IntradayScreenSnapshot(
        source=source, stocks=stocks, market_rows=snapshot.market_rows
    )
    database = tmp_path / "readonly.duckdb"
    _signal_projection_database(database)
    store = NotificationStateStore(tmp_path / "notifications.sqlite3")
    old_market = ServingProjectionPayload(
        table_name="market_snapshot",
        available_at=cutoff - timedelta(seconds=5),
        rows=(
            {
                **snapshot.market_rows[0].model_dump(mode="json"),
                "as_of": (cutoff - timedelta(seconds=5)).isoformat(),
                "price": 9,
            },
        ),
    )
    old_results = ServingProjectionPayload(
        table_name="screen_result",
        available_at=cutoff - timedelta(seconds=5),
        rows=(
            {
                "trade_date": cutoff.date().isoformat(),
                "ts_code": "600000.SH",
                "preset_name": "old",
                "name": "原记录",
                "close": 9,
                "pct_chg": -1,
            },
        ),
    )
    prior = {"market_snapshot": old_market, "screen_result": old_results}
    companion = tuple(
        prior.get(name)
        or ServingProjectionPayload(
            table_name=name, available_at=cutoff - timedelta(seconds=5), rows=()
        )
        for name in sorted(_COMPANION_SIGNAL_TABLES)
    )
    producer = SignalPageProjectionProducer(
        source=DuckDBSignalPageProjectionSource(database),
        store=store,
        companion_projections=companion,
    )
    producer.publish(cutoff)
    before = {
        item.table_name: item
        for item in store.serving_snapshot(observed_at=cutoff, history_limit=1).payload.projections
    }
    producer.intraday_source = lambda _: snapshot
    producer.publish(cutoff + timedelta(seconds=1))
    after = {
        item.table_name: item
        for item in store.serving_snapshot(
            observed_at=cutoff + timedelta(seconds=1), history_limit=1
        ).payload.projections
    }
    assert not after["intraday_screen_source"].rows and not after["intraday_feature_snapshot"].rows
    assert after["market_snapshot"] == old_market and after["screen_result"] == old_results
    for name, projection in before.items():
        assert after[name] == projection


def test_quote_and_exact_closed_bar_keep_all_original_values_and_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader, cutoff = _quote_world(tmp_path, monkeypatch)
    snapshot = reader(cutoff)
    bar = snapshot.stocks[0].closed_bar
    assert bar is not None and bar.bar_end == cutoff - timedelta(seconds=2)
    facts = {fact.name: fact for fact in bar.fields}
    from tests.unit.test_intraday_feature_engine import _current_minutes

    frame = _current_minutes()
    assert facts["INTRADAY_PRICE[0]"].value == frame.iloc[-1].close == 14
    assert facts["INTRADAY_VOLUME[0]"].value == frame.vol.sum()
    assert facts["INTRADAY_AMOUNT[0]"].value == frame.amount.sum()
    assert facts["INTRADAY_HIGH[0]"].value == frame.high.max()
    assert facts["INTRADAY_LOW[0]"].value == frame.low.min()
    assert facts["INTRADAY_TURNOVER_RATE[0]"].value == 100 * frame.vol.sum() / 11000
    assert facts["INTRADAY_LIMIT_DISTANCE[0]"].value == pytest.approx(100 / 15)
    assert all(fact.source_event_time in (None, bar.bar_end) for fact in bar.fields)
    assert facts["INTRADAY_PRICE[0]"].source_id == snapshot.source.feature_payload_sha256
    assert facts["INTRADAY_PRICE[0]"].original_feature_status.candidate_id == "600000.SH"
    assert facts["INTRADAY_PRICE[0]"].available_at == cutoff
    assert snapshot.stocks[1].closed_bar is None


@pytest.mark.parametrize("has_float", [True, False])
def test_closed_bar_does_not_retimestamp_an_earlier_observed_turnover(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, has_float: bool
) -> None:
    from rquant.feature_contracts import FeatureAvailability
    from rquant.screen.intraday_reference import IntradayReferenceSnapshot
    from tests.unit.test_intraday_feature_engine import _current_minutes

    reader, cutoff = _quote_world(tmp_path, monkeypatch)
    path = reader.config.reference_snapshot_path
    original = IntradayReferenceSnapshot.model_validate_json(path.read_bytes())
    observations = tuple(
        observation.model_copy(
            update={
                "turnover_rate": 0,
                "float_shares": observation.float_shares if has_float else None,
            }
        )
        for observation in original.observations
    )
    reference = IntradayReferenceSnapshot.model_validate(
        original.model_dump(exclude={"identity"}) | {"observations": observations}
    )
    path.write_text(reference.model_dump_json())
    reader.config = reader.config.model_copy(
        update={"reference_snapshot_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    )
    stock = reader(cutoff).stocks[0]
    assert stock.closed_bar is not None
    fact = next(
        item for item in stock.closed_bar.fields if item.name == "INTRADAY_TURNOVER_RATE[0]"
    )
    if has_float:
        assert fact.status is FeatureAvailability.AVAILABLE
        assert fact.value == pytest.approx(100 * _current_minutes().vol.sum() / 11000)
        assert fact.source_event_time == stock.closed_bar.bar_end
    else:
        assert fact.status is FeatureAvailability.UNAVAILABLE and fact.value is None
        assert fact.reason


def test_changed_quote_prefix_during_combined_publication_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.screen.intraday_source as source
    from rquant.live_contracts import LiveChannel
    from rquant.live_spool import LiveBatchSpool

    reader, cutoff = _quote_world(tmp_path, monkeypatch)
    read = source.read_intraday_quotes
    descriptor = LiveBatchSpool.source_descriptor

    def change_after_quote_read(config, *, cursor_root, reference, cutoff):
        result = read(config, cursor_root=cursor_root, reference=reference, cutoff=cutoff)

        def changed(spool, channel):
            value = descriptor(spool, channel)
            return (
                value.model_copy(update={"high_watermark": value.high_watermark + 1})
                if channel is LiveChannel.WATCHLIST_QUOTE
                else value
            )

        monkeypatch.setattr(LiveBatchSpool, "source_descriptor", changed)
        return result

    monkeypatch.setattr(source, "read_intraday_quotes", change_after_quote_read)
    with pytest.raises(ValueError, match="changed during publication"):
        reader(cutoff)


def test_history_file_change_during_bounded_digest_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    import rquant.screen.intraday_source as source

    reader, cutoff = _source_world(tmp_path, monkeypatch)
    read = os.read
    changed = False

    def mutate(fd: int, size: int) -> bytes:
        nonlocal changed
        payload = read(fd, size)
        if not changed:
            changed = True
            with reader.config.historical_snapshot_path.open("ab") as file:
                file.write(b"changed")
        return payload

    monkeypatch.setattr(os, "read", mutate)
    with pytest.raises(ValueError, match="historical.*changed"):
        source._read_history_digest(reader.config.historical_snapshot_path)


def test_intraday_gate_requires_real_cutover_and_original_consumer_receipt(tmp_path: Path) -> None:
    from rquant.schema_compatibility import (
        ConsumerCapabilityReceipt,
        LiveSchemaRolloutPlan,
        ProductionConsumerCapability,
        ProductionConsumerRegistry,
        RolloutPhase,
        SchemaDeclaration,
        SchemaField,
        SchemaParticipant,
        SchemaRolloutStore,
        validate_dual_write_values,
    )
    from rquant.screen.intraday_source import IntradaySchemaGate, read_intraday_schema_evidence

    dataset = "runtime.intraday_feature.batch-envelope"
    now = datetime(2026, 7, 31, 1, 40, tzinfo=UTC)
    fields = (
        SchemaField(name="ts_code", type_name="string", required=True, introduced_in=1),
        SchemaField(name="close", type_name="float64", required=True, introduced_in=1),
    )
    old = SchemaDeclaration(
        dataset_id=dataset,
        schema_name="unit_intraday",
        current_version=1,
        min_reader_version=1,
        fields=fields,
        producer_commit="a" * 40,
    )
    new = SchemaDeclaration(
        dataset_id=dataset,
        schema_name="unit_intraday",
        current_version=2,
        min_reader_version=1,
        fields=(
            *fields,
            SchemaField(name="amount", type_name="float64", required=False, introduced_in=2),
        ),
        producer_commit="a" * 40,
    )
    capability = ProductionConsumerCapability(
        consumer_id="unit-notifier",
        service_id="unit-notifier",
        dataset_id=dataset,
        contract_fingerprint="e" * 64,
        code_commit="a" * 40,
        min_readable_schema_version=1,
        max_readable_schema_version=2,
        required_fields=("close", "ts_code"),
    )
    registry = ProductionConsumerRegistry(registry_id="unit", consumers=(capability,))
    plan = LiveSchemaRolloutPlan(
        dataset_id=dataset,
        old_declaration_fingerprint=old.schema_fingerprint,
        new_declaration_fingerprint=new.schema_fingerprint,
        producers=(
            SchemaParticipant(participant_id="unit-producer", contract_fingerprint="b" * 64),
        ),
        consumers=(
            SchemaParticipant(
                participant_id=capability.consumer_id,
                contract_fingerprint=capability.contract_fingerprint,
            ),
        ),
        production_consumer_registry_fingerprint=registry.registry_fingerprint,
        serving_physical_schema_fingerprint="5" * 64,
        target_generation_id="4" * 64,
        target_schema_version=2,
        consumer_ack_max_age_seconds=300,
        started_at=now,
        deadline=now + timedelta(hours=2),
    )
    path = tmp_path / "rollout.sqlite3"
    store = SchemaRolloutStore(path, production_consumer_registry=registry)
    state = store.create_plan(plan, now=now, operation_id="unit-create")
    gate = IntradaySchemaGate(
        store_path=path,
        plan_id=plan.plan_id,
        registry=registry,
        consumer_id=capability.consumer_id,
        consumer_commit="a" * 40,
        installed_generation_id="4" * 64,
        declaration_fingerprint=new.schema_fingerprint,
        declaration_version=2,
    )
    with pytest.raises(ValueError, match="not installed"):
        read_intraday_schema_evidence(gate, cutoff=now)
    state = store.acknowledge(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        phase=RolloutPhase.PREPARE,
        participant_id="unit-producer",
        participant_fingerprint="b" * 64,
        declaration_fingerprint=new.schema_fingerprint,
        now=now + timedelta(seconds=1),
        operation_id="unit-prepare",
    )
    state = store.advance(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.DUAL_WRITE,
        now=now + timedelta(seconds=2),
        operation_id="unit-dual",
    )
    evidence = validate_dual_write_values(
        old_declaration=old,
        new_declaration=new,
        old_values={"ts_code": "600000.SH", "close": 10.5},
        new_values={"ts_code": "600000.SH", "close": 10.5, "amount": 1.0},
        generation_id="4" * 64,
        observed_at=now + timedelta(seconds=3),
    )
    state = store.record_dual_write_evidence(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        evidence=evidence,
        operation_id="unit-evidence",
    )
    state = store.advance(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.CONSUMER_ACK,
        now=now + timedelta(seconds=4),
        operation_id="unit-ack-phase",
    )
    receipt = ConsumerCapabilityReceipt(
        consumer_id=capability.consumer_id,
        service_id=capability.service_id,
        dataset_id=dataset,
        code_commit="a" * 40,
        min_readable_schema_version=1,
        max_readable_schema_version=2,
        required_fields=("close", "ts_code"),
        serving_physical_schema_fingerprint="5" * 64,
        observed_generation_id="4" * 64,
        available_at=now + timedelta(seconds=5),
    )
    state = store.acknowledge_consumer(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        receipt=receipt,
        now=now + timedelta(seconds=5),
        operation_id="unit-consumer",
    )
    state = store.advance(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.CUTOVER,
        now=now + timedelta(seconds=6),
        operation_id="unit-cutover",
    )
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    actual = read_intraday_schema_evidence(gate, cutoff=now + timedelta(seconds=6))
    assert actual.consumer_receipt == receipt and actual.revision == state.revision
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    for update in ({"installed_generation_id": "0" * 64}, {"consumer_commit": "0" * 40}):
        with pytest.raises(ValueError, match="capability"):
            read_intraday_schema_evidence(
                gate.model_copy(update=update), cutoff=now + timedelta(seconds=6)
            )
    with pytest.raises(ValueError, match="not installed"):
        read_intraday_schema_evidence(gate, cutoff=now + timedelta(seconds=5))
