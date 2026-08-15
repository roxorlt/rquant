from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pandas as pd
import pytest

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
    PAGE_PROJECTION_CONTRACTS,
    SERVING_TABLE_SPECS,
    NlScreenPageError,
    ServingProjectionContract,
    ServingProjectionInput,
    ServingProjectionPayload,
    ServingReadModelInput,
    ServingSignalRecord,
    build_serving_read_models,
    decode_nl_screen_cursor,
    encode_nl_screen_cursor,
    paginate_nl_screen_projection,
    screen_nl_projection,
    serving_physical_table_specs_fingerprint,
)
from rquant.signal_bus import RouteReceiptDisposition, SignalRouteReceipt
from rquant.signal_contracts import SignalAction, SignalEnvelope

NOW = datetime(2026, 7, 31, 2, 31, tzinfo=UTC)


def test_serving_physical_table_specs_have_a_canonical_fingerprint() -> None:
    first = serving_physical_table_specs_fingerprint()
    second = serving_physical_table_specs_fingerprint()

    assert first == second
    assert first == "78cbded6cb27a09532d0cf253bcebcf6e4f24c29cfa96c59652a479a4b9c6fd5"


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
        evidence={
            "score": 0.8,
            "runner_transition": {"from_state": "idle", "to_state": "armed"},
        },
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


def _projection(
    table_name: str,
    rows: tuple[dict[str, object], ...],
    *,
    owner_dataset_id: str,
    available_at: datetime = NOW,
) -> ServingProjectionInput:
    return ServingProjectionInput.bind(
        ServingProjectionPayload(
            table_name=table_name,
            available_at=available_at,
            rows=rows,
        ),
        owner_dataset_id=owner_dataset_id,
        owner_generation_id="9" * 64,
    )


def test_page_projections_build_typed_bounded_tables_and_explicit_availability() -> None:
    dashboard = _projection(
        "dashboard_summary",
        (
            {
                "snapshot_key": "current",
                "latest_daily_bar": "2026-07-31",
                "latest_screen": "2026-07-31",
                "daily_bar_rows": 123456,
                "monitor_event_rows": 7,
                "minute_bar_rows": 8000000,
                "minute_codes": 321,
                "minute_min_time": "2026-04-01T01:30:00Z",
                "minute_max_time": "2026-07-31T02:30:00Z",
                "host_name": "rquant-test",
                "monitor_state": "running",
                "monitor_substate": "running",
                "monitor_next_at": None,
                "monitor_last_at": "2026-07-31T01:25:00Z",
                "daily_state": "inactive",
                "daily_exec_status": "0",
                "daily_next_at": None,
                "daily_last_at": "2026-07-30T09:00:00Z",
                "dashboard_state": "running",
                "backup_snapshot_at": "2026-07-31T02:00:00Z",
                "backup_source_bytes": 1000,
                "backup_compressed_bytes": 500,
                "backup_last_download_at": None,
                "backup_last_download_ip": None,
                "backup_last_download_bytes": None,
            },
        ),
        owner_dataset_id="runtime_health",
    )
    kline = _projection(
        "daily_bar",
        (
            {
                "ts_code": "600000.SH",
                "trade_date": "2026-07-31",
                "open": 10.0,
                "high": 10.8,
                "low": 9.9,
                "close": 10.6,
                "vol": 100000,
            },
        ),
        owner_dataset_id="reference_slow_authority",
    )
    source = ServingReadModelInput(
        observed_at=NOW,
        projections=(dashboard, kline),
    )

    tables = build_serving_read_models(source)

    assert set(tables) == set(SERVING_TABLE_SPECS)
    assert tables["daily_bar"].iloc[0]["trade_date"].isoformat() == "2026-07-31"
    assert tables["dashboard_summary"].iloc[0]["daily_bar_rows"] == 123456
    status = tables["projection_status"].set_index("table_name")
    assert bool(status.loc["daily_bar", "available"]) is True
    assert status.loc["daily_bar", "owner_generation_id"] == "9" * 64
    assert bool(status.loc["market_overview", "available"]) is False
    assert status.loc["market_overview", "reason"] == "projection_not_published"
    for table_name in ("pulse_history", "pulse_alert", "surge_runtime_config"):
        assert bool(status.loc[table_name, "available"]) is False
        assert status.loc[table_name, "owner_dataset_id"] == "signals"
        assert status.loc[table_name, "reason"] == "projection_not_published"


def test_projection_contract_rejects_duplicate_columns_and_unknown_sort_keys() -> None:
    with pytest.raises(ValueError, match="column names"):
        ServingProjectionContract(
            owner_dataset_id="signals",
            columns=(("ts_code", "string"), ("ts_code", "string")),
            sort_keys=("ts_code",),
            max_rows=1,
            max_bytes=1024,
        )

    with pytest.raises(ValueError, match="sort keys"):
        ServingProjectionContract(
            owner_dataset_id="signals",
            columns=(("ts_code", "string"),),
            sort_keys=("missing",),
            max_rows=1,
            max_bytes=1024,
        )

    with pytest.raises(ValueError, match="column identifier"):
        ServingProjectionContract(
            owner_dataset_id="signals",
            columns=(("price; DROP TABLE signals", "float"),),
            sort_keys=("price; DROP TABLE signals",),
            max_rows=1,
            max_bytes=1024,
        )


def test_projection_contracts_reject_wrong_owner_schema_future_and_row_budget() -> None:
    with pytest.raises(ValueError, match="owner"):
        _projection(
            "daily_bar",
            (),
            owner_dataset_id="signals",
        )

    with pytest.raises(ValueError, match="columns"):
        ServingProjectionPayload(
            table_name="stock_basic",
            available_at=NOW,
            rows=({"ts_code": "600000.SH", "name": "浦发银行"},),
        )

    with pytest.raises(ValueError, match="future"):
        ServingReadModelInput(
            observed_at=NOW,
            projections=(
                _projection(
                    "stock_basic",
                    (
                        {
                            "ts_code": "600000.SH",
                            "name": "浦发银行",
                            "industry": "银行",
                        },
                    ),
                    owner_dataset_id="reference_slow_authority",
                    available_at=NOW + timedelta(microseconds=1),
                ),
            ),
        )

    contract = PAGE_PROJECTION_CONTRACTS["stock_basic"]
    oversized = tuple(
        {
            "ts_code": f"{index:06d}.SH",
            "name": "样本",
            "industry": "行业",
        }
        for index in range(contract.max_rows + 1)
    )
    with pytest.raises(ValueError, match="row budget"):
        ServingProjectionPayload(
            table_name="stock_basic",
            available_at=NOW,
            rows=oversized,
        )


def test_page_isolation_projections_are_typed_bounded_and_owned() -> None:
    expected = {
        "trade_calendar": ("reference_slow_authority", 5_000),
        "screen_bounds": ("signals", 256),
        "minute_coverage": ("signals", 128),
        "research_gate_metadata": ("lab_jobs", 512),
        "canvas_diagnostic": ("signals", 20_000),
        "canvas_latest_trade_date": ("signals", 1),
        "canvas_hit": ("signals", 20_000),
        "canvas_definition": ("signals", 512),
    }

    for table_name, (owner, max_rows) in expected.items():
        contract = PAGE_PROJECTION_CONTRACTS[table_name]
        assert contract.owner_dataset_id == owner
        assert contract.max_rows == max_rows
        assert contract.max_bytes <= 6 * 1024 * 1024
        assert table_name in SERVING_TABLE_SPECS


def test_nl_projection_allows_bounded_feature_columns_but_rejects_oversized_cells() -> None:
    projection = ServingProjectionPayload(
        table_name="nl_screen_universe",
        available_at=NOW,
        rows=(
            {
                "trade_date": "2026-07-31",
                "ts_code": "600000.SH",
                "name": "浦发银行",
                "is_st": False,
                "is_bj": False,
                "board_type": "main",
                "CLOSE[0]": 10.6,
                "PCT_CHG[0]": 6.0,
                "MA5[0]": 10.0,
            },
        ),
    )
    assert projection.rows[0]["MA5[0]"] == 10.0

    with pytest.raises(ValueError, match="byte budget|cell"):
        ServingProjectionPayload(
            table_name="nl_screen_universe",
            available_at=NOW,
            rows=(
                {
                    "trade_date": "2026-07-31",
                    "ts_code": "600000.SH",
                    "name": "x" * (2 * 1024 * 1024),
                    "is_st": False,
                    "is_bj": False,
                    "board_type": "main",
                    "CLOSE[0]": 10.6,
                    "PCT_CHG[0]": 6.0,
                },
            ),
        )


def test_nl_projection_screen_runs_rules_in_memory_with_diagnostics() -> None:
    universe = pd.DataFrame(
        {
            "trade_date": [date(2026, 7, 31), date(2026, 7, 31)],
            "ts_code": ["600000.SH", "600001.SH"],
            "name": ["甲", "乙"],
            "is_st": [False, False],
            "is_bj": [False, False],
            "board_type": ["main", "main"],
            "CLOSE[0]": [10.6, 8.0],
            "PCT_CHG[0]": [6.0, -1.0],
            "MA5[0]": [10.0, 8.5],
        }
    )
    rules = (
        lambda frame: frame["is_st"].eq(False),
        lambda frame: frame["CLOSE[0]"] > frame["MA5[0]"],
    )

    result, diagnostics = screen_nl_projection(
        universe,
        trade_date="2026-07-31",
        rules=rules,
        rule_labels=("not_st()", "gt(CLOSE[0], MA5[0])"),
        include_columns=("MA5[0]",),
    )

    assert result["ts_code"].tolist() == ["600000.SH"]
    assert list(result.columns) == [
        "ts_code",
        "name",
        "CLOSE[0]",
        "PCT_CHG[0]",
        "MA5[0]",
    ]
    assert diagnostics == (("not_st()", 2), ("gt(CLOSE[0], MA5[0])", 1))


def test_nl_projection_screen_fails_closed_when_feature_is_not_projected() -> None:
    universe = pd.DataFrame(
        {
            "trade_date": [date(2026, 7, 31)],
            "ts_code": ["600000.SH"],
            "name": ["甲"],
            "CLOSE[0]": [10.6],
            "PCT_CHG[0]": [6.0],
        }
    )

    with pytest.raises(ValueError, match=r"projection.*MA20\[0\]"):
        screen_nl_projection(
            universe,
            trade_date="2026-07-31",
            rules=(lambda frame: frame["CLOSE[0]"] > frame["MA20[0]"],),
            rule_labels=("gt(CLOSE[0], MA20[0])",),
        )


def test_nl_projection_paginates_only_after_full_rule_validation() -> None:
    universe = pd.DataFrame(
        {
            "trade_date": [date(2026, 7, 31)] * 3,
            "ts_code": ["600000.SH", "600001.SH", "600002.SH"],
            "name": ["无效", "乙", "丙"],
            "CLOSE[0]": [1.0, 2.0, 3.0],
            "PCT_CHG[0]": [0.0, 1.0, 2.0],
        }
    )
    kwargs = {
        "generation_id": "a" * 64,
        "trade_date": "2026-07-31",
        "rules": (lambda frame: frame["CLOSE[0]"] > 1.0,),
        "rule_labels": ("gt(CLOSE[0], 1)",),
        "normalized_plan": {"trade_date": "2026-07-31", "rule": "close_gt_1"},
        "page_size": 1,
    }

    first = paginate_nl_screen_projection(universe, **kwargs)
    second = paginate_nl_screen_projection(universe, cursor=first.next_cursor, **kwargs)

    assert first.rows["ts_code"].tolist() == ["600001.SH"]
    assert second.rows["ts_code"].tolist() == ["600002.SH"]
    assert first.next_cursor is not None
    assert second.next_cursor is None


def test_nl_projection_cursor_is_deterministic_and_rejects_query_mismatch() -> None:
    universe = pd.DataFrame(
        {
            "trade_date": [date(2026, 7, 31)] * 2,
            "ts_code": ["600000.SH", "600001.SH"],
            "name": ["甲", "乙"],
            "CLOSE[0]": [1.0, 2.0],
            "PCT_CHG[0]": [0.0, 1.0],
        }
    )
    kwargs = {
        "generation_id": "a" * 64,
        "trade_date": "2026-07-31",
        "rules": (),
        "rule_labels": (),
        "normalized_plan": {"trade_date": "2026-07-31", "rule": "all"},
        "page_size": 1,
    }

    first = paginate_nl_screen_projection(universe, **kwargs)
    repeat = paginate_nl_screen_projection(universe, **kwargs)
    cursor_repeat = paginate_nl_screen_projection(
        universe,
        cursor=first.next_cursor,
        **kwargs,
    )
    assert first.next_cursor == repeat.next_cursor
    assert first.rows.to_dict("records") == repeat.rows.to_dict("records")
    assert cursor_repeat.rows.to_dict("records") == [
        {"ts_code": "600001.SH", "name": "乙", "CLOSE[0]": 2.0, "PCT_CHG[0]": 1.0}
    ]
    assert first.next_cursor is not None
    decoded = decode_nl_screen_cursor(first.next_cursor)
    assert decoded.generation_id == "a" * 64
    assert encode_nl_screen_cursor(decoded) == first.next_cursor

    with pytest.raises(NlScreenPageError, match="requires rerun"):
        paginate_nl_screen_projection(
            universe,
            cursor=first.next_cursor,
            **{**kwargs, "normalized_plan": {"trade_date": "2026-07-31", "rule": "none"}},
        )
    with pytest.raises(NlScreenPageError, match="requires rerun"):
        paginate_nl_screen_projection(
            universe,
            cursor=first.next_cursor,
            **{**kwargs, "generation_id": "b" * 64},
        )


def test_nl_projection_rejects_duplicate_snapshot_keys_and_has_no_phantom_page() -> None:
    universe = pd.DataFrame(
        {
            "trade_date": [date(2026, 7, 31)],
            "ts_code": ["600000.SH"],
            "name": ["无效"],
            "CLOSE[0]": [1.0],
            "PCT_CHG[0]": [0.0],
        }
    )
    kwargs = {
        "generation_id": "a" * 64,
        "trade_date": "2026-07-31",
        "rules": (lambda frame: frame["CLOSE[0]"] > 1.0,),
        "rule_labels": ("gt(CLOSE[0], 1)",),
        "normalized_plan": {"trade_date": "2026-07-31"},
        "page_size": 1,
    }

    page = paginate_nl_screen_projection(universe, **kwargs)
    assert page.rows.empty
    assert page.next_cursor is None

    with pytest.raises(NlScreenPageError, match="duplicate"):
        paginate_nl_screen_projection(pd.concat([universe, universe]), **kwargs)
