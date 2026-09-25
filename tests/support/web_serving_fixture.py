"""Synthetic Serving generations for the web API and the browser tests.

Everything here is invented: stock codes 600001-600030.SH named 样本01-样本30, prices and
board members from ``rquant.panorama_data``'s test-only fixtures. No real market data.
The one real thing is the trade calendar: the published 2026 SSE schedule (weekdays minus
the exchange holidays, e.g. 2026-09-25 中秋 is closed and the next open day is 09-28),
because the top bar and the overview read it and a weekday rule would call a holiday a
trading day. Like the production calendar it lists open dates only and ends 2026-12-31.

Generations go through the production path — ``ServingReadModelInput`` validation,
``build_serving_read_models`` and a ``ServingPublisher`` over ``SERVING_TABLE_SPECS`` — so
what the API reads has exactly the shape a real generation has.

Scenarios:

* ``baseline``: runtime services running / degraded / missing, two signals with route
  receipts and deliveries, one paper account with holdings, ``dashboard_summary``,
  ``minute_coverage``, the latest daily screen (``canvas_hit``,
  ``canvas_latest_trade_date``, ``screen_bounds``), ``trade_calendar`` and
  ``stock_basic``; every watermark fresh.
* ``panorama``: ``baseline`` plus every table the market panorama reads.
* ``degraded``: ``baseline`` with degraded / unavailable watermarks and two page
  projections left unpublished.

``sequence=n`` publishes the n-th generation of a scenario one minute after the previous
one, for generation-switch tests.

The content is fixed (``built_at``, producer commit, every value); the ``generation_id`` is
not, because it binds the SHA-256 of the DuckDB file and DuckDB does not write
byte-identical files for identical content. Tests compare content, never generation ids
across builds.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget, OutboxRecord, OutboxStatus
from rquant.panorama_data import (
    _FAKE_CODES,
    _fake_board_members,
    _fake_daily_kline,
    _fake_kpl_members,
    _fake_liquidity_baseline,
    _fake_pulse_history,
    _fake_sector_fund_flow,
    _fake_snapshot,
    _fake_surge_log,
    _session_minute_stamps,
)
from rquant.paper_contracts import PaperAccountSnapshot, PaperHolding
from rquant.runtime_service_control import (
    RuntimeServiceHealth,
    RuntimeServiceHeartbeatProjection,
    RuntimeServicePlane,
    RuntimeServiceStatus,
)
from rquant.serving_contracts import (
    FreshnessStatus,
    ServingDatasetWatermark,
    ServingGenerationManifest,
)
from rquant.serving_publisher import ServingPublisher
from rquant.serving_read_models import (
    SERVING_TABLE_SPECS,
    ServingProjectionInput,
    ServingProjectionPayload,
    ServingReadModelInput,
    ServingSignalRecord,
    build_serving_read_models,
)
from rquant.signal_bus import RouteReceiptDisposition, SignalRouteReceipt
from rquant.signal_contracts import SignalAction, SignalEnvelope

SCENARIOS = ("baseline", "panorama", "degraded")
FIXTURE_PRODUCER_COMMIT = "0e5b0e5b0e5b0e5b0e5b0e5b0e5b0e5b0e5b0e5b"
#: Schema version the production publisher writes (manifest ``schema_version`` 3).
FIXTURE_SCHEMA_VERSION = 3
#: 2026-09-24 15:31 in Shanghai: after the close, so a whole session of minute bars fits.
FIXTURE_TRADE_DATE = date(2026, 9, 24)
FIXTURE_BUILT_AT = datetime(2026, 9, 24, 7, 31, tzinfo=UTC)
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_REFERENCE_AVAILABLE_AT = datetime(2026, 9, 24, 1, 25, tzinfo=UTC)
_UNAVAILABLE_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_CHART_CODES = tuple(_FAKE_CODES[:5])
_DATASETS = (
    "lab_jobs",
    "paper_accounts",
    "promotions",
    "reference_slow",
    "reference_slow_authority",
    "reference_slow_contract",
    "runtime_health",
    "signals",
)


def fixture_built_at(sequence: int) -> datetime:
    return FIXTURE_BUILT_AT + timedelta(minutes=sequence)


def _digest(*parts: object) -> str:
    return hashlib.sha256(":".join(str(part) for part in parts).encode()).hexdigest()


def _generation_ids(scenario: str, sequence: int) -> dict[str, str]:
    return {dataset: _digest("web-fixture", scenario, dataset, sequence) for dataset in _DATASETS}


def _utc_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _cst(day: date, hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=_SHANGHAI)


#: Weekday closures of the 2026 SSE calendar (the exchange's published holiday schedule;
#: the same dates the production ``trade_calendar`` projection leaves out).
SSE_2026_WEEKDAY_CLOSURES = frozenset(
    date.fromisoformat(day)
    for day in (
        "2026-01-01",
        "2026-01-02",
        "2026-02-16",
        "2026-02-17",
        "2026-02-18",
        "2026-02-19",
        "2026-02-20",
        "2026-02-23",
        "2026-04-06",
        "2026-05-01",
        "2026-05-04",
        "2026-05-05",
        "2026-06-19",
        "2026-09-25",
        "2026-10-01",
        "2026-10-02",
        "2026-10-05",
        "2026-10-06",
        "2026-10-07",
    )
)
CALENDAR_START = date(2026, 1, 1)
CALENDAR_END = date(2026, 12, 31)


def _trading_days(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5 and current not in SSE_2026_WEEKDAY_CLOSURES:
            days.append(current)
        current += timedelta(days=1)
    return days


# ------------------------------------------------------------------ runtime state


def _runtime_services(observed_at: datetime) -> tuple[RuntimeServiceHealth, ...]:
    # Service ids shaped like production's (``<role>.<instance>.v1``).
    rows = (
        ("feature.intraday-pit.v1", RuntimeServicePlane.LIVE, RuntimeServiceStatus.RUNNING, False),
        (
            "signal-router.all-strategies.v1",
            RuntimeServicePlane.LIVE,
            RuntimeServiceStatus.RUNNING,
            False,
        ),
        (
            "notifier.admin.shadow.v1",
            RuntimeServicePlane.LIVE,
            RuntimeServiceStatus.DEGRADED,
            False,
        ),
        (
            "paper-broker.shadow-main.v1",
            RuntimeServicePlane.LIVE,
            RuntimeServiceStatus.RUNNING,
            False,
        ),
        (
            "auction-match.source.v1",
            RuntimeServicePlane.LIVE,
            RuntimeServiceStatus.MISSING,
            True,
        ),
        (
            "serving-publisher.primary.v1",
            RuntimeServicePlane.SERVING,
            RuntimeServiceStatus.RUNNING,
            False,
        ),
        ("lab-jobs.serving.v1", RuntimeServicePlane.RESEARCH, RuntimeServiceStatus.MISSING, True),
    )
    services = [
        RuntimeServiceHealth(
            service_id=service_id,
            plane=plane,
            status=status,
            stale=stale,
            observed_at=observed_at,
        )
        for service_id, plane, status, stale in rows
    ]
    # The reference publisher after 09:25 once today's generation is out: it refuses every
    # round by design (#301 S-1), so it is degraded with a long failure streak.
    reference_id = "reference-slow.publisher.v1"
    services.append(
        RuntimeServiceHealth(
            service_id=reference_id,
            plane=RuntimeServicePlane.LIVE,
            status=RuntimeServiceStatus.DEGRADED,
            stale=False,
            observed_at=observed_at,
            heartbeat=RuntimeServiceHeartbeatProjection(
                service_id=reference_id,
                spec_fingerprint=_digest("spec", reference_id),
                run_id=_digest("run", reference_id),
                generation=1,
                status=RuntimeServiceStatus.DEGRADED,
                started_at=observed_at - timedelta(hours=6),
                heartbeat_at=observed_at - timedelta(seconds=10),
                input_sequence=0,
                output_sequence=0,
                consecutive_failures=239,
                degraded_reasons=("reference_slow:ReferenceSlowRuntimeError",),
                last_error=(
                    "ReferenceSlowRuntimeError: reference slow publisher started after 09:25"
                ),
            ),
        )
    )
    return tuple(services)


def _signal(
    *,
    strategy_id: str,
    candidate_id: str,
    action: SignalAction,
    available_at: datetime,
) -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id=strategy_id,
        strategy_version="1",
        parameter_fingerprint=_digest("parameters", strategy_id),
        dataset_snapshot_id=_digest("dataset", strategy_id),
        feature_snapshot_id=_digest("features", strategy_id),
        event_time=available_at - timedelta(seconds=5),
        available_at=available_at,
        candidate_id=candidate_id,
        action=action,
        reason_codes=("synthetic_fixture",),
        evidence={"score": 0.8},
        expires_at=available_at + timedelta(hours=1),
        producer_commit=FIXTURE_PRODUCER_COMMIT,
    )


def _signal_bundle(
    built_at: datetime,
) -> tuple[
    tuple[ServingSignalRecord, ...],
    tuple[SignalRouteReceipt, ...],
    tuple[OutboxRecord, ...],
]:
    target = DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER)
    # Strategy ids as production writes them (``strategy.<id>.v1`` sources).
    specs = (
        ("n_shape", "600001.SH", SignalAction.B_INTENT, _cst(FIXTURE_TRADE_DATE, 9, 47)),
        ("auction_gap", "600003.SH", SignalAction.WATCH, _cst(FIXTURE_TRADE_DATE, 9, 29)),
    )
    signals: list[ServingSignalRecord] = []
    routes: list[SignalRouteReceipt] = []
    deliveries: list[OutboxRecord] = []
    for index, (strategy_id, code, action, available_at) in enumerate(specs, start=1):
        signal = _signal(
            strategy_id=strategy_id,
            candidate_id=code,
            action=action,
            available_at=available_at.astimezone(UTC),
        )
        routed_at = signal.available_at + timedelta(seconds=1)
        signals.append(ServingSignalRecord(global_sequence=index, signal=signal))
        routes.append(
            SignalRouteReceipt(
                source_id=f"strategy.{strategy_id}.v1",
                source_sequence=1,
                signal_id=signal.signal_id,
                decision_fingerprint=_digest("decision", strategy_id),
                disposition=RouteReceiptDisposition.ROUTED,
                target_manifest_hash=_digest("targets", "admin"),
                targets=(target,),
                target_count=1,
                routed_at=routed_at,
            )
        )
        deliveries.append(
            OutboxRecord(
                signal_id=signal.signal_id,
                target=target,
                status=OutboxStatus.SUCCEEDED if index == 1 else OutboxStatus.PENDING,
                expires_at=signal.expires_at,
                attempt_count=1 if index == 1 else 0,
                created_at=routed_at,
                updated_at=min(routed_at + timedelta(seconds=2), built_at),
            )
        )
    return tuple(signals), tuple(routes), tuple(deliveries)


def _paper_account(as_of: datetime) -> PaperAccountSnapshot:
    holdings = (
        PaperHolding(
            code="600001.SH",
            quantity=100,
            available_quantity=0,
            frozen_quantity=100,
            average_cost=Decimal("8.80"),
            market_price=Decimal("8.80"),
        ),
        PaperHolding(
            code="600005.SH",
            quantity=100,
            available_quantity=100,
            frozen_quantity=0,
            average_cost=Decimal("15.00"),
            market_price=Decimal("15.42"),
        ),
    )
    cash = Decimal("97620.00")
    unrealized = sum(
        ((holding.market_price - holding.average_cost) * holding.quantity for holding in holdings),
        Decimal("0"),
    )
    value = sum((holding.market_price * holding.quantity for holding in holdings), Decimal("0"))
    return PaperAccountSnapshot(
        account_id="shadow-main",
        as_of_time=as_of,
        cash=cash,
        available_cash=cash,
        frozen_cash=Decimal("0"),
        holdings=holdings,
        realized_pnl=Decimal("0"),
        unrealized_pnl=unrealized,
        nav=cash + value,
    )


# ------------------------------------------------------------------ projections


def _projection(
    table_name: str,
    rows: Sequence[Mapping[str, object]],
    *,
    owner: str,
    generations: Mapping[str, str],
    available_at: datetime,
) -> ServingProjectionInput:
    return ServingProjectionInput.bind(
        ServingProjectionPayload(
            table_name=table_name,
            available_at=available_at,
            rows=tuple(dict(row) for row in rows),
        ),
        owner_dataset_id=owner,
        owner_generation_id=generations[owner],
    )


def _dashboard_summary(built_at: datetime) -> list[dict[str, object]]:
    return [
        {
            "snapshot_key": "current",
            "latest_daily_bar": FIXTURE_TRADE_DATE.isoformat(),
            "latest_screen": FIXTURE_TRADE_DATE.isoformat(),
            "daily_bar_rows": 1_234_567,
            "monitor_event_rows": 42,
            "minute_bar_rows": 8_400_000,
            "minute_codes": 5_120,
            "minute_min_time": "2025-04-28T01:30:00Z",
            "minute_max_time": _utc_iso(_cst(FIXTURE_TRADE_DATE, 14, 59)),
            "host_name": "rquant-fixture",
            "monitor_state": "running",
            "monitor_substate": "running",
            "monitor_next_at": None,
            "monitor_last_at": _utc_iso(built_at - timedelta(minutes=2)),
            "daily_state": "inactive",
            "daily_exec_status": "0",
            "daily_next_at": None,
            "daily_last_at": "2026-09-23T09:00:00Z",
            "dashboard_state": "running",
            "backup_snapshot_at": "2026-09-24T02:00:00Z",
            "backup_source_bytes": 3_000_000_000,
            "backup_compressed_bytes": 900_000_000,
            "backup_last_download_at": None,
            "backup_last_download_ip": None,
            "backup_last_download_bytes": None,
        }
    ]


def _minute_coverage() -> list[dict[str, object]]:
    last = _utc_iso(_cst(FIXTURE_TRADE_DATE, 14, 59))
    return [
        {
            "is_total": True,
            "source": "all",
            "rows_count": 8_400_000,
            "codes_count": 5_120,
            "trade_dates": 352,
            "min_time": "2025-04-28T01:30:00Z",
            "max_time": last,
        },
        {
            "is_total": False,
            "source": "tushare",
            "rows_count": 8_400_000,
            "codes_count": 5_120,
            "trade_dates": 352,
            "min_time": "2025-04-28T01:30:00Z",
            "max_time": last,
        },
    ]


def _trade_calendar() -> list[dict[str, object]]:
    # Open dates only, like the production projection.
    return [
        {"trade_date": day.isoformat(), "exchange": "SSE", "is_open": True}
        for day in _trading_days(CALENDAR_START, CALENDAR_END)
    ]


#: The latest daily screen: selected after the 2026-09-23 close for the 09-24 session.
_SCREEN_DATE = date(2026, 9, 23)
_SCREEN_MEMBERS = (
    ("n-shape-pool1", "600002.SH"),
    ("n-shape-pool1", "600004.SH"),
    ("n-shape-pool1", "600006.SH"),
    ("n-shape-pool2", "600008.SH"),
    ("n-shape-pool2", "600010.SH"),
)


def _canvas_hits() -> list[dict[str, object]]:
    frame = _snapshot_frame().set_index("ts_code")
    rows: list[dict[str, object]] = []
    for preset, code in _SCREEN_MEMBERS:
        quote = frame.loc[code]
        rows.append(
            {
                "trade_date": _SCREEN_DATE.isoformat(),
                "preset_name": preset,
                "ts_code": code,
                "row_json": json.dumps(
                    {
                        "close": float(quote["pre_close"]),
                        "name": str(quote["name"]),
                        "pct_chg": round(float(quote["pct_chg"]) / 2, 2),
                        "ts_code": code,
                    },
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            }
        )
    return rows


def _screen_bounds() -> list[dict[str, object]]:
    counts: dict[str, int] = {}
    for preset, _code in _SCREEN_MEMBERS:
        counts[preset] = counts.get(preset, 0) + 1
    return [
        {
            "preset_name": preset,
            "min_date": "2026-09-10",
            "max_date": _SCREEN_DATE.isoformat(),
            "candidate_count": count * 8,
        }
        for preset, count in sorted(counts.items())
    ]


def _stock_basic() -> list[dict[str, object]]:
    industries = ("半导体", "计算机", "通信", "电子", "机械设备")
    return [
        {"ts_code": code, "name": f"样本{index:02d}", "industry": industries[index % 5]}
        for index, code in enumerate(_FAKE_CODES, start=1)
    ]


def _snapshot_frame() -> pd.DataFrame:
    return _fake_snapshot()


def _market_snapshot(as_of: datetime) -> list[dict[str, object]]:
    frame = _snapshot_frame()
    return [
        {
            "as_of": _utc_iso(as_of),
            "ts_code": str(row.ts_code),
            "name": str(row.name),
            "price": float(row.price),
            "open": float(row.open),
            "high": float(row.high),
            "low": float(row.low),
            "pre_close": float(row.pre_close),
            "pct_chg": float(row.pct_chg),
            "volume": float(row.volume),
            "amount": float(row.amount),
        }
        for row in frame.itertuples(index=False)
    ]


def _market_overview(as_of: datetime) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for system, sector_type in (("东财行业", "行业资金流"), ("东财概念", "概念资金流")):
        flows = _fake_sector_fund_flow(sector_type)
        for index, flow in enumerate(flows.itertuples(index=False)):
            rows.append(
                {
                    "as_of": _utc_iso(as_of),
                    "system": system,
                    "board_code": f"{flow.board_code}.DC",
                    "board_name": str(flow.board_name),
                    "amount": float(4.2e10 - index * 1.5e10),
                    "main_net_amount": float(flow.main_net_amount),
                    "main_net_rate": float(flow.main_net_rate),
                    "pct_chg_median": float(flow.pct_chg),
                    "limit_up_count": 2 - index,
                    "broken_count": 1 - index,
                    "stock_count": 12,
                    "limit_up_ratio_pct": round((2 - index) / 12 * 100, 2),
                    "leading_stock": str(flow.leading_stock),
                }
            )
    kpl = _fake_kpl_members()
    for index, (board_code, members) in enumerate(kpl.groupby("board_code", sort=True)):
        rows.append(
            {
                "as_of": _utc_iso(as_of),
                "system": "开盘啦题材",
                "board_code": str(board_code),
                "board_name": str(members["board_name"].iloc[0]),
                "amount": float(1.8e10 - index * 0.6e10),
                "main_net_amount": None,
                "main_net_rate": None,
                "pct_chg_median": round(3.1 - index * 1.4, 2),
                "limit_up_count": 2 - 2 * index,
                "broken_count": 1 - index,
                "stock_count": len(members),
                "limit_up_ratio_pct": round((2 - 2 * index) / len(members) * 100, 2),
                "leading_stock": "样本01" if index == 0 else "样本11",
            }
        )
    return rows


def _dc_boards() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    members = _fake_board_members()
    boards = (
        members[["board_code", "board_name", "idx_type"]]
        .drop_duplicates()
        .sort_values("board_code")
        .itertuples(index=False)
    )
    board_rows = [
        {"ts_code": str(row.board_code), "name": str(row.board_name), "idx_type": str(row.idx_type)}
        for row in boards
    ]
    member_rows = [
        {"board_code": str(row.board_code), "con_code": str(row.con_code)}
        for row in members.itertuples(index=False)
    ]
    return board_rows, member_rows


def _kpl_members() -> list[dict[str, object]]:
    return [
        {
            "board_code": str(row.board_code),
            "board_name": str(row.board_name),
            "con_code": str(row.con_code),
        }
        for row in _fake_kpl_members().itertuples(index=False)
    ]


def _market_liquidity() -> list[dict[str, object]]:
    return [
        {
            "ts_code": str(row.ts_code),
            "circ_mv": float(row.circ_mv),
            "avg_amount_5d": float(row.avg_amount_5d),
        }
        for row in _fake_liquidity_baseline().itertuples(index=False)
    ]


def _daily_bars() -> list[dict[str, object]]:
    curve = _fake_daily_kline(_CHART_CODES[0])
    days = _trading_days(CALENDAR_START, FIXTURE_TRADE_DATE)[-len(curve) :]
    pre_close = dict(zip(_snapshot_frame()["ts_code"], _snapshot_frame()["pre_close"], strict=True))
    rows: list[dict[str, object]] = []
    for code in _CHART_CODES:
        scale = float(pre_close[code]) / 20.0
        for day, bar in zip(days, curve.itertuples(index=False), strict=True):
            rows.append(
                {
                    "ts_code": code,
                    "trade_date": day.isoformat(),
                    "open": round(float(bar.open) * scale, 2),
                    "high": round(float(bar.high) * scale, 2),
                    "low": round(float(bar.low) * scale, 2),
                    "close": round(float(bar.close) * scale, 2),
                    "vol": float(bar.volume),
                }
            )
    return rows


def _intraday_bars() -> list[dict[str, object]]:
    stamps = _session_minute_stamps(pd.Timestamp(FIXTURE_TRADE_DATE))
    x = np.arange(len(stamps))
    base = np.round(20.0 + 2.0 * np.sin(x / 30.0) + x * 0.001, 2)
    volume = np.round(10_000.0 + (np.sin(x / 10.0) + 1.0) * 5_000.0, 0)
    pre_close = dict(zip(_snapshot_frame()["ts_code"], _snapshot_frame()["pre_close"], strict=True))
    rows: list[dict[str, object]] = []
    for code in _CHART_CODES:
        scale = float(pre_close[code]) / 20.0
        previous = round(float(base[0]) * scale, 2)
        for stamp, price, vol in zip(stamps, base, volume, strict=True):
            close = round(float(price) * scale, 2)
            trade_time = stamp.to_pydatetime().replace(tzinfo=_SHANGHAI)
            rows.append(
                {
                    "ts_code": code,
                    "trade_time": _utc_iso(trade_time),
                    "open": previous,
                    "high": round(max(previous, close) + 0.01, 2),
                    "low": round(min(previous, close) - 0.01, 2),
                    "close": close,
                    "vol": float(vol),
                }
            )
            previous = close
    return rows


def _surge_events() -> list[dict[str, object]]:
    return [
        {
            "trade_date": FIXTURE_TRADE_DATE.isoformat(),
            "confirmed_at": str(row.confirmed_at),
            "ts_code": str(row.ts_code),
            "name": str(row.name),
            "theme": str(row.theme),
            "price": float(row.price),
            "pct_chg": float(row.pct_chg),
            "cum_amount": float(row.cum_amount),
            "rel_cum": float(row.rel_cum),
            "room_to_limit_pct": float(row.room_to_limit_pct),
            "status": str(row.status),
        }
        for row in _fake_surge_log().itertuples(index=False)
    ]


def _pulse_history() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in _fake_pulse_history().itertuples(index=False):
        hour, minute = (int(part) for part in str(row.t).split(":"))
        rows.append(
            {
                "trade_date": FIXTURE_TRADE_DATE.isoformat(),
                "as_of": _utc_iso(_cst(FIXTURE_TRADE_DATE, hour, minute)),
                "t": str(row.t),
                "limit_up": int(row.limit_up),
                "limit_down": int(row.limit_down),
                "broken": int(row.broken),
                "up": int(row.up),
                "down": int(row.down),
                "up_ratio_pct": float(row.up_ratio_pct),
                "total": int(row.total),
            }
        )
    return rows


def _pulse_alerts() -> list[dict[str, object]]:
    return [
        {
            "trade_date": FIXTURE_TRADE_DATE.isoformat(),
            "as_of": _utc_iso(_cst(FIXTURE_TRADE_DATE, 14, 20)),
            "t": "14:20",
            "kind": "broken_surge",
            "kind_label": "炸板潮",
            "before": 2.0,
            "after": 6.0,
            "window_minutes": 10,
            "message": "炸板 10 分钟 2 → 6（+4）",
        }
    ]


def _surge_runtime_config(as_of: datetime) -> list[dict[str, object]]:
    return [
        {
            "snapshot_key": "current",
            "trade_date": FIXTURE_TRADE_DATE.isoformat(),
            "as_of": _utc_iso(as_of),
            "boards_json": json.dumps(["主板", "创业板", "科创板"], ensure_ascii=False),
            "k_rough": 2.0,
            "k_cum": 3.0,
            "ratio_cap": 12.0,
            "skip_first_minutes": 5,
            "tushare_rate_per_min": 200,
            "require_price_strength": True,
            "max_room_to_limit_pct": 9.0,
        }
    ]


def _projections(
    scenario: str,
    *,
    built_at: datetime,
    generations: Mapping[str, str],
) -> tuple[ServingProjectionInput, ...]:
    signals_at = built_at - timedelta(seconds=30)

    def reference(table: str, rows: Sequence[Mapping[str, object]]) -> ServingProjectionInput:
        return _projection(
            table,
            rows,
            owner="reference_slow_authority",
            generations=generations,
            available_at=_REFERENCE_AVAILABLE_AT,
        )

    def signal_owned(table: str, rows: Sequence[Mapping[str, object]]) -> ServingProjectionInput:
        return _projection(
            table, rows, owner="signals", generations=generations, available_at=signals_at
        )

    projections = [
        reference("trade_calendar", _trade_calendar()),
        reference("stock_basic", _stock_basic()),
    ]
    if scenario != "degraded":
        projections.append(
            _projection(
                "dashboard_summary",
                _dashboard_summary(built_at),
                owner="runtime_health",
                generations=generations,
                available_at=built_at - timedelta(seconds=10),
            )
        )
        projections.append(signal_owned("minute_coverage", _minute_coverage()))
        projections.extend(
            (
                signal_owned("canvas_hit", _canvas_hits()),
                signal_owned(
                    "canvas_latest_trade_date",
                    [{"snapshot_key": "current", "trade_date": _SCREEN_DATE.isoformat()}],
                ),
                signal_owned("screen_bounds", _screen_bounds()),
            )
        )
    if scenario == "panorama":
        as_of = _cst(FIXTURE_TRADE_DATE, 15, 0, 3).astimezone(UTC)
        board_rows, member_rows = _dc_boards()
        projections.extend(
            (
                signal_owned("market_snapshot", _market_snapshot(as_of)),
                signal_owned("market_overview", _market_overview(as_of)),
                reference("dc_board", board_rows),
                reference("dc_board_member", member_rows),
                reference("kpl_concept_member", _kpl_members()),
                reference("market_liquidity", _market_liquidity()),
                reference("daily_bar", _daily_bars()),
                signal_owned("intraday_kline", _intraday_bars()),
                signal_owned("surge_event", _surge_events()),
                signal_owned("pulse_history", _pulse_history()),
                signal_owned("pulse_alert", _pulse_alerts()),
                signal_owned("surge_runtime_config", _surge_runtime_config(as_of)),
            )
        )
    return tuple(sorted(projections, key=lambda item: item.table_name))


# ------------------------------------------------------------------ watermarks


def _watermarks(
    scenario: str,
    *,
    built_at: datetime,
    generations: Mapping[str, str],
    sequence: int,
) -> tuple[ServingDatasetWatermark, ...]:
    degraded = {
        "runtime_health": (
            FreshnessStatus.DEGRADED,
            "degraded:notifier.admin.shadow.v1,missing:lab-jobs.serving.v1",
        ),
        "lab_jobs": (
            FreshnessStatus.UNAVAILABLE,
            "ServingSourceAuthorityUnavailableError: current pointer is unavailable",
        ),
    }
    watermarks: list[ServingDatasetWatermark] = []
    for dataset in _DATASETS:
        status, reason = FreshnessStatus.FRESH, None
        if scenario == "degraded" and dataset in degraded:
            status, reason = degraded[dataset]
        if dataset.startswith("reference_slow"):
            event_time = published_at = _REFERENCE_AVAILABLE_AT
        elif status is FreshnessStatus.UNAVAILABLE:
            event_time = published_at = _UNAVAILABLE_EPOCH
        else:
            event_time = built_at - timedelta(seconds=40)
            published_at = built_at - timedelta(seconds=20)
        watermarks.append(
            ServingDatasetWatermark(
                dataset_id=dataset,
                generation_id=generations[dataset],
                event_time=event_time,
                published_at=published_at,
                sequence=sequence + 1,
                status=status,
                reason=reason,
            )
        )
    return tuple(watermarks)


# ------------------------------------------------------------------ public entry


def build_web_fixture(
    root: str | Path,
    scenario: str,
    *,
    sequence: int = 0,
) -> ServingGenerationManifest:
    """Publish generation ``sequence`` of ``scenario`` into ``root`` and select it."""

    if scenario not in SCENARIOS:
        raise ValueError(f"unknown web fixture scenario: {scenario}")
    if type(sequence) is not int or sequence < 0:
        raise ValueError("sequence must be a non-negative integer")
    built_at = fixture_built_at(sequence)
    generations = _generation_ids(scenario, sequence)
    signals, routes, deliveries = _signal_bundle(built_at)
    source = ServingReadModelInput(
        observed_at=built_at,
        signals=signals,
        routes=routes,
        deliveries=deliveries,
        paper_accounts=(_paper_account(built_at - timedelta(seconds=30)),),
        runtime_services=_runtime_services(built_at - timedelta(seconds=5)),
        projections=_projections(scenario, built_at=built_at, generations=generations),
    )
    publisher = ServingPublisher(
        root,
        producer_commit=FIXTURE_PRODUCER_COMMIT,
        schema_version=FIXTURE_SCHEMA_VERSION,
        table_specs=SERVING_TABLE_SPECS,
    )
    return publisher.publish(
        build_serving_read_models(source),
        watermarks=_watermarks(
            scenario, built_at=built_at, generations=generations, sequence=sequence
        ),
        source_generations=generations,
        built_at=built_at,
    )


__all__ = [
    "CALENDAR_END",
    "FIXTURE_BUILT_AT",
    "FIXTURE_PRODUCER_COMMIT",
    "FIXTURE_TRADE_DATE",
    "SCENARIOS",
    "SSE_2026_WEEKDAY_CLOSURES",
    "build_web_fixture",
    "fixture_built_at",
]
