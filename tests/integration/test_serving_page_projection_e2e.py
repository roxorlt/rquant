from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from rquant.config import settings
from rquant.dashboard.runtime_console_data import (
    ServingFrameState,
    query_serving_frame,
)
from rquant.dashboard.serving_page_data import ServingPageRenderContext
from rquant.lab_jobs import LabJobReader, LabJobStore
from rquant.lab_jobs_serving_authority import LabJobsServingSourceReader
from rquant.notification_state import NotificationStateStore
from rquant.page_control import (
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    PageControlStatus,
    SaveCanvas,
)
from rquant.panorama_data import (
    load_board_members,
    load_intraday_kline_projection,
    load_market_overview_projection,
    load_market_snapshot_projection,
    load_pulse_alerts,
    load_pulse_history,
    load_surge_log,
    load_surge_runtime_config,
    open_panorama_serving_generation,
)
from rquant.paper_contracts import PaperAccountSnapshot
from rquant.reference_slow_publisher import (
    ReferenceDailyFact,
    ReferenceSecurityFact,
    ReferenceSlowPublishReceipt,
    ReferenceSlowSourceSnapshot,
    build_reference_slow_serving_result,
)
from rquant.research_gate import ResearchGateRequest
from rquant.runtime_builder_serving import serving_publisher_builder
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_health_authority import (
    RuntimeHealthControlSource,
    RuntimeHealthSourceReader,
)
from rquant.runtime_service_control import (
    RuntimeServicePlane,
    RuntimeServiceSpec,
)
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
from rquant.runtime_serving_snapshot import (
    LAB_JOBS_DATASET_ID,
    PAPER_ACCOUNTS_DATASET_ID,
    PROMOTIONS_DATASET_ID,
    REFERENCE_SLOW_AUTHORITY_DATASET_ID,
    RUNTIME_HEALTH_DATASET_ID,
    SIGNALS_DATASET_ID,
    PaperAccountsPayload,
    PromotionsPayload,
    ReferenceSlowPayload,
    SourceReadResult,
)
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_page_projection_source import (
    DuckDBLabPageProjectionSource,
    DuckDBSignalPageProjectionSource,
    SignalPageProjectionProducer,
)
from rquant.serving_publisher import ServingPublisher, ServingReader
from rquant.serving_read_models import (
    SERVING_TABLE_SPECS,
    ServingProjectionPayload,
    screen_nl_projection,
)
from rquant.storage.duckdb import DuckDBStore
from tests.canvas_ed25519_support import create_canvas_ed25519_test_authority

NOW = datetime(2026, 8, 2, 3, 0, tzinfo=UTC)
COMMIT = "a" * 40


def _projection(
    table_name: str,
    rows: tuple[dict[str, object], ...],
    *,
    available_at: datetime = NOW - timedelta(seconds=3),
) -> ServingProjectionPayload:
    return ServingProjectionPayload(
        table_name=table_name,
        available_at=available_at,
        rows=rows,
    )


def _signal_projections() -> tuple[ServingProjectionPayload, ...]:
    observed = NOW - timedelta(seconds=3)
    return (
        _projection(
            "screen_result",
            (
                {
                    "trade_date": "2026-07-31",
                    "ts_code": "600000.SH",
                    "preset_name": "n-shape-pool1",
                    "name": "浦发银行",
                    "close": 10.6,
                    "pct_chg": 6.0,
                },
            ),
        ),
        _projection(
            "pool2_watch",
            (
                {
                    "ts_code": "600000.SH",
                    "entry_date": "2026-07-31",
                    "body_lower": 10.0,
                    "body_upper": 10.4,
                    "level_40": 10.2,
                    "level_30": 10.1,
                    "level_20": 10.0,
                    "stop_strong": 9.8,
                    "stop_weak": 9.5,
                    "status": "active",
                },
            ),
        ),
        _projection(
            "monitor_event",
            (
                {
                    "trade_date": "2026-07-31",
                    "trigger_time": "2026-07-31T02:00:00Z",
                    "ts_code": "600000.SH",
                    "level": "strong",
                    "trigger_price": 10.5,
                    "level_price": 10.4,
                    "trigger_type": "support",
                    "pool": "pool2",
                },
            ),
        ),
        _projection(
            "surge_event",
            (
                {
                    "trade_date": "2026-07-31",
                    "confirmed_at": "10:00",
                    "ts_code": "600000.SH",
                    "name": "浦发银行",
                    "theme": "银行",
                    "price": 10.6,
                    "pct_chg": 6.0,
                    "cum_amount": 100000000.0,
                    "rel_cum": 2.0,
                    "room_to_limit_pct": 4.0,
                    "status": "confirmed",
                },
            ),
        ),
        _projection(
            "market_snapshot",
            (
                {
                    "as_of": observed.isoformat(),
                    "ts_code": "600000.SH",
                    "name": "浦发银行",
                    "price": 10.6,
                    "open": 10.1,
                    "high": 10.8,
                    "low": 10.0,
                    "pre_close": 10.0,
                    "pct_chg": 6.0,
                    "volume": 1000000.0,
                    "amount": 100000000.0,
                },
            ),
        ),
        _projection(
            "market_overview",
            (
                {
                    "as_of": observed.isoformat(),
                    "system": "东财行业",
                    "board_code": "BK0001.DC",
                    "board_name": "银行",
                    "amount": 100000000.0,
                    "main_net_amount": 10000000.0,
                    "main_net_rate": 10.0,
                    "pct_chg_median": 3.0,
                    "limit_up_count": 1,
                    "broken_count": 0,
                    "stock_count": 10,
                    "limit_up_ratio_pct": 10.0,
                    "leading_stock": "浦发银行",
                },
            ),
        ),
        _projection(
            "intraday_kline",
            (
                {
                    "ts_code": "600000.SH",
                    "trade_time": "2026-07-31T02:00:00Z",
                    "open": 10.5,
                    "high": 10.7,
                    "low": 10.4,
                    "close": 10.6,
                    "vol": 100000.0,
                },
            ),
        ),
        _projection(
            "screen_bounds",
            (
                {
                    "preset_name": "n-shape-pool1",
                    "min_date": "2026-07-01",
                    "max_date": "2026-07-31",
                    "candidate_count": 1,
                },
            ),
        ),
        _projection(
            "minute_coverage",
            (
                {
                    "is_total": True,
                    "source": "all",
                    "rows_count": 240,
                    "codes_count": 1,
                    "trade_dates": 1,
                    "min_time": "2026-07-31T01:30:00Z",
                    "max_time": "2026-07-31T07:00:00Z",
                },
            ),
        ),
        _projection(
            "canvas_diagnostic",
            (
                {
                    "trade_date": "2026-07-31",
                    "preset_name": "n-shape-pool1",
                    "step_index": 0,
                    "rule_label": "all",
                    "remaining_count": 1,
                },
            ),
        ),
        _projection(
            "canvas_latest_trade_date",
            ({"snapshot_key": "current", "trade_date": "2026-07-31"},),
        ),
        _projection(
            "canvas_hit",
            (
                {
                    "trade_date": "2026-07-31",
                    "preset_name": "n-shape-pool1",
                    "ts_code": "600000.SH",
                    "row_json": '{"ts_code":"600000.SH","name":"浦发银行"}',
                },
            ),
        ),
        _projection("canvas_definition", ()),
    )


def _signal_page_source_database(path: Path) -> None:
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            """
            CREATE TABLE screen_result (
                trade_date DATE, preset_name VARCHAR, ts_code VARCHAR, name VARCHAR,
                close DOUBLE, pct_chg DOUBLE, extra JSON, created_at TIMESTAMP
            );
            INSERT INTO screen_result VALUES
              ('2026-07-01', 'n-shape-pool1', '000001.SZ', '平安银行', 10, 1, '{}',
               '2026-07-01 15:05:00'),
              ('2026-07-31', 'n-shape-pool1', '600000.SH', '浦发银行', 10.6, 6, '{}',
               '2026-07-31 15:05:00');
            CREATE TABLE minute_bar (
                ts_code VARCHAR, trade_time TIMESTAMP, freq VARCHAR, open DOUBLE,
                high DOUBLE, low DOUBLE, close DOUBLE, vol DOUBLE, amount DOUBLE,
                source VARCHAR, created_at TIMESTAMP
            );
            INSERT INTO minute_bar
            SELECT '600000.SH', TIMESTAMP '2026-07-31 09:30:00'
                       + range * INTERVAL 1 MINUTE,
                   '1min', 10, 10.6, 10, 10.6, 100, 1000, 'tushare',
                   TIMESTAMP '2026-07-31 15:05:00'
            FROM range(240);
            """
        )
    finally:
        connection.close()


def _reference_projections(
    *,
    stock_name: str = "浦发银行",
    available_at: datetime = NOW - timedelta(seconds=3),
) -> tuple[ServingProjectionPayload, ...]:
    values = (
        _projection(
            "stock_basic",
            ({"ts_code": "600000.SH", "name": stock_name, "industry": "银行"},),
            available_at=available_at,
        ),
        _projection(
            "risk_blacklist",
            (
                {
                    "ts_code": "600001.SH",
                    "list_label": "测试风险",
                    "expires_at": "2026-08-31",
                    "imported_at": "2026-07-31T08:00:00Z",
                },
            ),
            available_at=available_at,
        ),
        _projection(
            "dc_board",
            ({"ts_code": "BK0001.DC", "name": "银行", "idx_type": "行业板块"},),
            available_at=available_at,
        ),
        _projection(
            "dc_board_member",
            ({"board_code": "BK0001.DC", "con_code": "600000.SH"},),
            available_at=available_at,
        ),
        _projection(
            "kpl_concept_member",
            ({"board_code": "KPL1", "board_name": "金融", "con_code": "600000.SH"},),
            available_at=available_at,
        ),
        _projection(
            "market_liquidity",
            ({"ts_code": "600000.SH", "circ_mv": 1000000.0, "avg_amount_5d": 80000000.0},),
            available_at=available_at,
        ),
        _projection(
            "daily_bar",
            (
                {
                    "ts_code": "600000.SH",
                    "trade_date": "2026-07-31",
                    "open": 10.1,
                    "high": 10.8,
                    "low": 10.0,
                    "close": 10.6,
                    "vol": 1000000.0,
                },
            ),
            available_at=available_at,
        ),
        _projection(
            "trade_calendar",
            (
                {
                    "trade_date": "2026-07-31",
                    "exchange": "SSE",
                    "is_open": True,
                },
                {
                    "trade_date": "2026-08-03",
                    "exchange": "SSE",
                    "is_open": True,
                },
            ),
            available_at=available_at,
        ),
        _projection(
            "nl_screen_universe",
            (
                {
                    "trade_date": "2026-07-31",
                    "ts_code": "600000.SH",
                    "name": stock_name,
                    "is_st": False,
                    "is_bj": False,
                    "board_type": "main",
                    "CLOSE[0]": 10.6,
                    "PCT_CHG[0]": 6.0,
                    "MA5[0]": 10.0,
                },
            ),
            available_at=available_at,
        ),
    )
    return values


def _source_result(
    dataset_id: str,
    payload: object,
    *,
    sequence: int = 1,
    observed_at: datetime = NOW - timedelta(seconds=2),
) -> SourceReadResult:
    values: dict[str, object] = {
        "dataset_id": dataset_id,
        "sequence": sequence,
        "event_time": observed_at,
        "published_at": observed_at,
        "status": FreshnessStatus.FRESH,
        "reason": None,
        "payload": payload,
    }
    values["generation_id"] = canonical_sha256(values)
    return SourceReadResult.model_validate(values)


def _publish_authority(
    root: Path,
    dataset_id: str,
    payload: object,
    *,
    sequence: int = 1,
    observed_at: datetime = NOW - timedelta(seconds=2),
    clock: datetime = NOW,
) -> None:
    ServingSourceAuthorityPublisher(
        root=root,
        producer_commit=COMMIT,
        dataset_id=dataset_id,
        payload_kind=payload.payload_kind,
        clock=lambda: clock,
    ).publish(
        _source_result(
            dataset_id,
            payload,
            sequence=sequence,
            observed_at=observed_at,
        )
    )


def test_page_projections_are_atomic_bounded_and_independent_from_operational_db(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root = tmp_path / "runtime"
    (runtime_root / "authorities").mkdir(parents=True)
    roots = {
        SIGNALS_DATASET_ID: runtime_root / "authorities" / SIGNALS_DATASET_ID,
        PAPER_ACCOUNTS_DATASET_ID: runtime_root / "authorities" / PAPER_ACCOUNTS_DATASET_ID,
        RUNTIME_HEALTH_DATASET_ID: runtime_root / "authorities" / RUNTIME_HEALTH_DATASET_ID,
        LAB_JOBS_DATASET_ID: runtime_root / "authorities" / LAB_JOBS_DATASET_ID,
        PROMOTIONS_DATASET_ID: runtime_root / "authorities" / PROMOTIONS_DATASET_ID,
        REFERENCE_SLOW_AUTHORITY_DATASET_ID: (
            runtime_root / "authorities" / REFERENCE_SLOW_AUTHORITY_DATASET_ID
        ),
    }
    strategy_summary = _projection(
        "strategy_summary",
        (
            {
                "run_id": "run-1",
                "computed_at": "2026-07-31T08:00:00Z",
                "start_date": "2026-07-01",
                "end_date": "2026-07-31",
                "max_hold_days": 1,
                "entry_mode": "first_break",
                "profile_variant": "baseline",
                "candidates": 1,
                "trades": 1,
                "trigger_rate_pct": 100.0,
                "mean_ret_pct": 2.0,
                "median_ret_pct": 2.0,
                "win_rate_pct": 100.0,
                "best_ret_pct": 2.0,
                "worst_ret_pct": 2.0,
                "gap_stop_rate_pct": 0.0,
            },
        ),
    )
    strategy_trade = _projection(
        "strategy_trade",
        (
            {
                "run_id": "run-1",
                "trade_id": "trade-1",
                "entry_mode": "first_break",
                "profile_variant": "baseline",
                "signal_date": "2026-07-31",
                "ts_code": "600000.SH",
                "name": "浦发银行",
                "entry_time": "2026-07-31T01:31:00Z",
                "entry_price_raw": 10.0,
                "entry_price": 10.0,
                "stop_loss_basis": 9.5,
                "take_profit_basis": 11.0,
                "volume_profile_lookbacks": "90",
                "volume_profile_rr": 2.0,
                "exit_time": "2026-07-31T07:00:00Z",
                "exit_price": 10.2,
                "exit_reason": "close",
                "ret_pct": 2.0,
            },
        ),
    )
    account = PaperAccountSnapshot(
        account_id="paper-main",
        as_of_time=NOW - timedelta(seconds=3),
        cash=Decimal("100000"),
        available_cash=Decimal("100000"),
        frozen_cash=Decimal("0"),
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        nav=Decimal("100000"),
    )

    notification_store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    signal_page_database = tmp_path / "signal-page-source.duckdb"
    _signal_page_source_database(signal_page_database)
    page_data_dir = tmp_path / "page-data"
    page_authority = create_canvas_ed25519_test_authority(tmp_path / "page-keys")
    page_outbox = PageControlOutbox(tmp_path / "page-control.sqlite3")
    page_service = PageControlService(
        outbox=page_outbox,
        consumer=PageControlConsumer(
            outbox=page_outbox,
            data_dir=page_data_dir,
            log_dir=tmp_path / "page-logs",
            consumer_service_id="page-control-e2e",
            consumer_id="page-control-e2e-instance",
            canvas_publication_signer=page_authority.signer,
            canvas_publication_keyring=page_authority.keyring,
        ),
    )
    canvas_command = SaveCanvas(
        command_id="serving-page-e2e-breakout",
        requested_at=NOW - timedelta(days=1),
        name="breakout",
        description="突破候选",
        pool_refs=("n-shape-pool1",),
        source="page_control",
    )
    canvas_receipt = page_service.submit(canvas_command)
    assert canvas_receipt.status is PageControlStatus.SUCCEEDED
    assert isinstance(canvas_receipt.result, dict)
    assert not (tmp_path / "canvas-catalog" / "breakout.json").exists()
    canvas_catalog = page_data_dir / "canvases"
    surge_live_root = tmp_path / "surge_live"
    surge_live_root.mkdir()
    (surge_live_root / "pulse-2026-08-02.jsonl").write_text(
        json.dumps(
            {
                "t": "09:31",
                "limit_up": 20,
                "limit_down": 2,
                "broken": 1,
                "up": 2600,
                "down": 2400,
                "up_ratio_pct": 50.0,
                "total": 5400,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (surge_live_root / "pulse_alerts-2026-08-02.jsonl").write_text(
        json.dumps(
            {
                "t": "10:15",
                "kind": "broken_surge",
                "kind_label": "炸板潮",
                "before": 2.0,
                "after": 6.0,
                "window_minutes": 10,
                "message": "炸板异动",
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    (surge_live_root / "runtime_config.json").write_text(
        json.dumps(
            {
                "day": "2026-08-02",
                "boards": ["main", "gem"],
                "k_rough": 1.2,
                "k_cum": 2.5,
                "ratio_cap": 8.0,
                "skip_first_minutes": 0,
                "tushare_rate_per_min": 2,
                "require_price_strength": True,
                "max_room_to_limit_pct": 1.0,
            }
        ),
        encoding="utf-8",
    )
    surge_source_timestamp = (NOW - timedelta(seconds=3)).timestamp()
    for path in surge_live_root.iterdir():
        os.utime(path, (surge_source_timestamp, surge_source_timestamp))
    page_table_names = {
        "screen_bounds",
        "minute_coverage",
        "canvas_diagnostic",
        "canvas_latest_trade_date",
        "canvas_hit",
        "canvas_definition",
    }
    SignalPageProjectionProducer(
        source=DuckDBSignalPageProjectionSource(
            signal_page_database,
            surge_live_root=surge_live_root,
            canvas_catalog_root=canvas_catalog,
            canvas_receipt_root=page_data_dir / "canvas-publication-receipts",
            canvas_publication_keyring=page_authority.keyring,
            page_control_outbox=page_outbox,
        ),
        store=notification_store,
        companion_projections=tuple(
            item for item in _signal_projections() if item.table_name not in page_table_names
        ),
    ).publish(NOW - timedelta(seconds=2))
    signal_payload = notification_store.serving_snapshot(
        observed_at=NOW - timedelta(seconds=2),
        history_limit=10,
    ).payload

    lab_store = LabJobStore(tmp_path / "lab-jobs.sqlite3")
    lab_store.initialize()
    research_database = tmp_path / "research-ro.duckdb"
    with DuckDBStore(research_database):
        pass
    lab_page_source = DuckDBLabPageProjectionSource(research_database)

    def read_strategy_projections(
        _summaries: object,
        _observed_at: datetime,
    ) -> tuple[ServingProjectionPayload, ...]:
        return strategy_summary, strategy_trade

    def read_lab_page_projections(
        observed_at: datetime,
    ) -> tuple[ServingProjectionPayload, ...]:
        return lab_page_source(observed_at).projections

    lab_result = LabJobsServingSourceReader(
        reader=LabJobReader(lab_store.path),
        strategy_projection_reader=read_strategy_projections,
        page_projection_reader=read_lab_page_projections,
    )(NOW - timedelta(seconds=2))

    health_result = RuntimeHealthSourceReader(
        sources=(
            RuntimeHealthControlSource(
                control_root=tmp_path / "control" / "monitor",
                spec=RuntimeServiceSpec(
                    service_id="monitor",
                    plane=RuntimeServicePlane.LIVE,
                    stale_after=timedelta(seconds=60),
                    producer_commit=COMMIT,
                ),
            ),
        ),
        serving_service_id="serving-publisher",
    )(NOW - timedelta(seconds=2))

    reference_snapshot = ReferenceSlowSourceSnapshot.create(
        target_trade_date=date(2026, 7, 31),
        captured_at=NOW - timedelta(seconds=3),
        producer_commit=COMMIT,
        source_snapshot_ids={
            "daily": "3" * 64,
            "security": "4" * 64,
            "suspension": "5" * 64,
            "calendar": "6" * 64,
        },
        daily_facts=(
            ReferenceDailyFact(
                ts_code="600000.SH",
                trade_date=date(2026, 7, 31),
                close_raw=10.6,
                prior_adj_factor=1.0,
                adj_factor=1.0,
            ),
        ),
        security_facts=(
            ReferenceSecurityFact(
                ts_code="600000.SH",
                name="浦发银行",
                is_st=False,
                list_date=date(1999, 11, 10),
                market="主板",
            ),
        ),
        projections=_reference_projections(),
    )
    reference_result = build_reference_slow_serving_result(
        snapshot=reference_snapshot,
        receipt=ReferenceSlowPublishReceipt(
            target_trade_date=date(2026, 7, 31),
            generation_id="f" * 64,
            source_snapshot_id=reference_snapshot.content_sha256,
            inserted_record_count=4,
            security_count=1,
            revision=1,
            available_at=NOW - timedelta(seconds=3),
        ),
    )
    payloads = {
        SIGNALS_DATASET_ID: signal_payload,
        PAPER_ACCOUNTS_DATASET_ID: PaperAccountsPayload(paper_accounts=(account,)),
        PROMOTIONS_DATASET_ID: PromotionsPayload(),
    }
    for dataset_id, payload in payloads.items():
        _publish_authority(roots[dataset_id], dataset_id, payload)
    for result in (health_result, reference_result, lab_result):
        ServingSourceAuthorityPublisher(
            root=roots[result.dataset_id],
            producer_commit=COMMIT,
            dataset_id=result.dataset_id,
            payload_kind=result.payload.payload_kind,
            clock=lambda: NOW,
        ).publish(result)

    clock = [NOW]
    manifest = RuntimeServiceManifest(
        service_id="serving-publisher",
        service_kind=RuntimeServiceKind.SERVING_PUBLISHER,
        plane=RuntimeServicePlane.SERVING,
        interval_seconds=15,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings={
            "serving_root": str(runtime_root / "serving"),
            "schema_version": 4,
            "source_authorities": [
                {"dataset_id": dataset_id, "root": str(root)} for dataset_id, root in roots.items()
            ],
        },
    )
    step = serving_publisher_builder(snapshot_loader=None, clock=lambda: clock[0])(manifest)
    step()
    reader = ServingReader(runtime_root / "serving")
    first = reader.current_manifest()

    operational = tmp_path / "rquant.duckdb"
    replica = tmp_path / "rquant_ro.duckdb"
    monkeypatch.setattr(settings, "duckdb_path", operational)
    monkeypatch.setattr(settings, "duckdb_readonly_path", replica)
    writer = duckdb.connect(str(operational))
    replica_writer = duckdb.connect(str(replica))
    for connection in (writer, replica_writer):
        connection.execute("CREATE TABLE production_only(value INTEGER)")
        connection.execute("BEGIN TRANSACTION")
        connection.execute("INSERT INTO production_only VALUES (1)")
    monkeypatch.setenv("RQUANT_SERVING_ROOT", str(runtime_root / "serving"))
    try:
        dashboard_result = query_serving_frame(
            runtime_root / "serving",
            "SELECT latest_daily_bar, daily_bar_rows FROM dashboard_summary",
            now=NOW,
            required_projections=("dashboard_summary",),
        )
        paper_result = query_serving_frame(
            runtime_root / "serving",
            "SELECT account_id, nav FROM paper_accounts LIMIT 10",
            now=NOW,
        )
        strategy_result = query_serving_frame(
            runtime_root / "serving",
            "SELECT run_id, mean_ret_pct FROM strategy_summary LIMIT 2000",
            now=NOW,
            required_projections=("strategy_summary",),
        )
        with open_panorama_serving_generation() as panorama_store:
            snapshot = load_market_snapshot_projection(panorama_store)
            overview = load_market_overview_projection("东财行业", panorama_store)
            intraday = load_intraday_kline_projection(
                "600000.SH",
                store=panorama_store,
            )
            members = load_board_members(panorama_store)
            surge = load_surge_log(
                day=datetime(2026, 7, 31, tzinfo=UTC).date(),
                store=panorama_store,
            )
            pulse_history = load_pulse_history(date(2026, 8, 2), store=panorama_store)
            pulse_alerts = load_pulse_alerts(date(2026, 8, 2), store=panorama_store)
            runtime_config = load_surge_runtime_config(store=panorama_store)
        nl_result = query_serving_frame(
            runtime_root / "serving",
            "SELECT * FROM nl_screen_universe WHERE trade_date = CAST(? AS DATE) LIMIT 8000",
            ("2026-07-31",),
            now=NOW,
            max_rows=8000,
            max_result_bytes=6 * 1024 * 1024,
            required_projections=("nl_screen_universe",),
        )
        with ServingPageRenderContext.open(
            runtime_root / "serving",
            now=NOW,
        ) as canvas_page:
            canvas_latest_result = canvas_page.canvas_latest_trade_date()
            canvas_definitions_result = canvas_page.canvas_definitions()
            canvas_result = canvas_page.canvas_diagnostic(
                "n-shape-pool1",
                "2026-07-31",
            )
            canvas_latest = canvas_latest_result.value
            canvas_hits, canvas_diagnostics = canvas_result.value
            canvas_generation = canvas_page.generation_id
        with ServingPageRenderContext.open(
            runtime_root / "serving",
            now=NOW,
        ) as lab_page:
            lab_calendar_result = lab_page.trading_calendar()
            lab_bounds_result = lab_page.screen_bounds("n-shape-pool1")
            lab_minute_result = lab_page.minute_coverage()
            lab_gate_result = lab_page.research_gate(
                ResearchGateRequest(
                    mode="formal",
                    strategy_name="n_shape",
                    start_date=date(2026, 7, 1),
                    end_date=date(2026, 7, 31),
                    code_commit=COMMIT,
                )
            )
            lab_calendar = lab_calendar_result.value
            lab_bounds = lab_bounds_result.value
            lab_minute_coverage = lab_minute_result.value
            lab_gate = lab_gate_result.value
            lab_generation = lab_page.generation_id
    finally:
        writer.rollback()
        writer.close()
        replica_writer.rollback()
        replica_writer.close()

    assert dashboard_result.state is ServingFrameState.DEGRADED
    assert "runtime_health" in dashboard_result.detail
    assert paper_result.rows == (("paper-main", Decimal("100000.000")),)
    assert strategy_result.rows == (("run-1", 2.0),)
    assert snapshot["ts_code"].tolist() == ["600000.SH"]
    assert overview["board_code"].tolist() == ["BK0001.DC"]
    assert intraday["price"].tolist() == [10.6]
    assert members["con_code"].tolist() == ["600000.SH"]
    assert surge["ts_code"].tolist() == ["600000.SH"]
    assert pulse_history["t"].tolist() == ["09:31"]
    assert pulse_alerts["kind"].tolist() == ["broken_surge"]
    assert runtime_config.config is not None
    assert runtime_config.config.boards == ("main", "gem")
    assert nl_result.state is ServingFrameState.READY
    nl_frame = nl_result.dataframe()
    assert isinstance(nl_frame, pd.DataFrame)
    selected, diagnostics = screen_nl_projection(
        nl_frame,
        trade_date="2026-07-31",
        rules=(lambda frame: frame["CLOSE[0]"] > frame["MA5[0]"],),
        rule_labels=("gt(CLOSE[0], MA5[0])",),
    )
    assert selected["ts_code"].tolist() == ["600000.SH"]
    assert diagnostics == (("gt(CLOSE[0], MA5[0])", 1),)
    assert canvas_latest == "2026-07-31"
    assert canvas_hits["ts_code"].tolist() == ["600000.SH"]
    assert canvas_diagnostics == [("final", 1)]
    assert canvas_definitions_result.value is not None
    assert canvas_definitions_result.value["name"].tolist() == ["breakout"]
    assert canvas_definitions_result.value["pool_refs_json"].tolist() == ['["n-shape-pool1"]']
    assert canvas_definitions_result.value["command_id"].tolist() == ["serving-page-e2e-breakout"]
    assert canvas_definitions_result.value["record_hash"].tolist() == [
        canvas_receipt.result["record_hash"]
    ]
    assert lab_calendar == (date(2026, 7, 31), date(2026, 8, 3))
    assert lab_bounds == (date(2026, 7, 1), date(2026, 7, 31), 2)
    assert lab_minute_coverage is not None
    assert lab_minute_coverage[["source", "rows_count"]].values.tolist() == [
        ["all", 240],
        ["tushare", 240],
    ]
    assert lab_gate.allowed is False
    assert lab_gate.audit_run_id is None
    assert [failure.code for failure in lab_gate.failures] == ["metadata_unavailable"]
    assert canvas_latest_result.state is ServingFrameState.STALE
    assert canvas_result.state is ServingFrameState.STALE
    assert canvas_definitions_result.state is ServingFrameState.STALE
    assert lab_calendar_result.state is ServingFrameState.READY
    assert lab_bounds_result.state is ServingFrameState.STALE
    assert lab_minute_result.state is ServingFrameState.STALE
    assert lab_gate_result.state is ServingFrameState.STALE
    observed_generations = {
        dashboard_result.generation_id,
        paper_result.generation_id,
        strategy_result.generation_id,
        snapshot.attrs["serving_generation_id"],
        overview.attrs["serving_generation_id"],
        intraday.attrs["serving_generation_id"],
        members.attrs["serving_generation_id"],
        surge.attrs["serving_generation_id"],
        pulse_history.attrs["serving_generation_id"],
        pulse_alerts.attrs["serving_generation_id"],
        runtime_config.generation_id,
        nl_result.generation_id,
        canvas_generation,
        lab_generation,
    }
    assert observed_generations == {first.generation_id}
    assert replica.exists()
    database = runtime_root / "serving" / "generations" / first.generation_id / "serving.duckdb"
    assert database.stat().st_size <= 256 * 1024 * 1024
    assert first.row_counts["nl_screen_universe"] <= 8000
    assert first.source_generations["reference_slow"] == "f" * 64

    revised_at = NOW + timedelta(seconds=1)
    revised = ReferenceSlowPayload(
        reference_generation_id="e" * 64,
        revision=2,
        price_basis="raw_session",
        adjustment_basis="tushare_adj_factor",
        available_at=revised_at,
        projections=_reference_projections(
            stock_name="浦发银行修订",
            available_at=revised_at,
        ),
    )
    _publish_authority(
        roots[REFERENCE_SLOW_AUTHORITY_DATASET_ID],
        REFERENCE_SLOW_AUTHORITY_DATASET_ID,
        revised,
        sequence=2,
        observed_at=revised_at,
        clock=NOW + timedelta(seconds=2),
    )
    clock[0] = NOW + timedelta(seconds=3)
    step()
    second = reader.current_manifest()

    assert second.generation_id != first.generation_id
    assert second.source_generations["reference_slow"] == "e" * 64
    with reader.acquire_historical_generation(first.generation_id) as historical:
        old_name = historical.connection.execute(
            "SELECT name FROM stock_basic WHERE ts_code = '600000.SH'"
        ).fetchone()[0]
    with reader.acquire_generation() as current:
        new_name = current.connection.execute(
            "SELECT name FROM stock_basic WHERE ts_code = '600000.SH'"
        ).fetchone()[0]
        tables = {
            table_name: current.connection.execute(f'SELECT * FROM "{table_name}"').fetchdf()
            for table_name in second.row_counts
        }
    assert old_name == "浦发银行"
    assert new_name == "浦发银行修订"

    publisher = ServingPublisher(
        runtime_root / "serving",
        producer_commit=COMMIT,
        schema_version=4,
        table_specs=SERVING_TABLE_SPECS,
    )

    def fail_after_switch(stage: str) -> None:
        if stage == "after_pointer_switch":
            raise RuntimeError("injected pointer failure")

    with pytest.raises(RuntimeError, match="pointer failure"):
        publisher.publish(
            tables,
            watermarks=second.watermarks,
            source_generations=second.source_generations,
            built_at=clock[0] + timedelta(seconds=1),
            failure_hook=fail_after_switch,
        )
    assert reader.current_manifest().generation_id == second.generation_id
