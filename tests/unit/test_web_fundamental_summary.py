"""The financial summary counts only fully checked receipts in one replica."""

from __future__ import annotations

import os
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from rquant.fundamental_daily import (
    FundamentalDailyQuery,
    FundamentalDailyVersion,
    _version_identity,
)
from rquant.replica_generation import replica_generation_path
from rquant.storage.duckdb import DuckDBStore
from rquant.web.app import create_app
from rquant.web.models.fundamentals import FundamentalSummaryData
from rquant.web.settings import WebSettings
from tests.unit.test_screen_fundamental_projection import _insert_version, _version
from tests.unit.test_web_screen_replica import _publish

_DAY = date(2026, 4, 15)
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_PATH = "/api/v1/data/fundamentals/summary"


def _now(day: date = _DAY, hour: int = 17, minute: int = 1) -> datetime:
    return datetime.combine(day, time(hour, minute), tzinfo=_SHANGHAI).astimezone(UTC)


def _calendar(store: DuckDBStore) -> None:
    for offset in (2, 1, 0):
        day = _DAY - timedelta(days=offset)
        store._conn.execute(
            "INSERT INTO trade_calendar "
            "(exchange, cal_date, is_open, pretrade_date, source, updated_at) "
            "VALUES ('SSE', ?, TRUE, ?, 'tushare', ?)",
            [day, day - timedelta(days=1), _now()],
        )


def _version_for(
    *,
    code: str = "600001.SH",
    day: date = _DAY,
    revision: int = 1,
    missing: dict[str, str] | None = None,
    pe: float = 10,
) -> FundamentalDailyVersion:
    base = _version(revision=revision, pe=None if missing and "pe_ttm" in missing else pe)
    decision = datetime.combine(day, time(17), tzinfo=_SHANGHAI)
    fields = {
        name: field.model_copy(
            update={
                "decision_at": decision,
                "reason": (missing or {}).get(name, field.reason),
                "status": "unknown" if name in (missing or {}) else field.status,
                "value": None if name in (missing or {}) else field.value,
            }
        )
        for name, field in base.fields.items()
    }
    identity = _version_identity(
        FundamentalDailyQuery(ts_code=code, trade_date=day),
        decision,
        base.target_report_period,
        base.target_period_reason,
        base.financial_source,
        base.valuation_source,
        fields,
    )
    return base.model_copy(
        update={
            "version_id": identity,
            "ts_code": code,
            "trade_date": day,
            "decision_at": decision,
            "fields": fields,
        }
    )


def _insert_head(store: DuckDBStore, version: FundamentalDailyVersion) -> None:
    _insert_version(store, version)
    store._conn.execute(
        "INSERT INTO fundamental_daily_head VALUES (?, ?, ?, ?)",
        [version.ts_code, version.trade_date, version.version_id, version.revision],
    )


def _world(tmp_path: Path, *, record: bool = True) -> tuple[Path, Path]:
    primary, replica = tmp_path / "main.duckdb", tmp_path / "readonly.duckdb"
    with DuckDBStore(primary) as store:
        _calendar(store)
        if record:
            _insert_head(store, _version_for(missing={"pe_ttm": "missing_value"}))
    _publish(primary, replica)
    return primary, replica


def _client(
    tmp_path: Path,
    primary: Path | None,
    replica: Path | None,
    *,
    now: datetime | None = None,
) -> TestClient:
    return TestClient(
        create_app(
            WebSettings(
                serving_root=tmp_path / "serving",
                screen_primary_path=primary,
                screen_replica_path=replica,
            ),
            clock=lambda: now or _now(),
            background=False,
        )
    )


def test_summary_counts_all_six_fields_without_daily_bar_and_binds_source(tmp_path: Path) -> None:
    primary, replica = _world(tmp_path)
    old_mtime = datetime(2026, 1, 1, tzinfo=UTC).timestamp()
    os.utime(primary, (old_mtime, old_mtime))
    _publish(primary, replica)
    with _client(tmp_path, primary, replica) as client:
        response = client.get(_PATH)
        assert response.status_code == 200
        body = response.json()
        source = body["source"]
        assert len(source["identity"]) == 64
        assert source["updated_at"]
        synced_at = datetime.fromtimestamp(
            replica_generation_path(replica).stat().st_mtime_ns / 1e9, tz=UTC
        )
        assert (
            abs((datetime.fromisoformat(source["updated_at"]) - synced_at).total_seconds()) < 0.01
        )
        again = client.get(_PATH, params={"expected_identity": source["identity"]})
        assert again.status_code == 200
    assert body["status"] == "ready"
    assert body["decision_date"] == "2026-04-15"
    assert body["record_count"] == 1
    assert body["coverage_note"] == "全市场覆盖尚未核验"
    assert len(body["fields"]) == 6
    fields = {item["key"]: item for item in body["fields"]}
    assert fields["pe_ttm"]["known_count"] == 0
    assert fields["pe_ttm"]["unknown_count"] == 1
    assert fields["pe_ttm"]["reasons"] == [{"label": "字段缺值", "count": 1}]
    assert all(
        item["known_count"] + item["unknown_count"] == body["record_count"]
        for item in body["fields"]
    )
    assert "missing_value" not in str(body)


def test_zero_heads_do_not_claim_zero_coverage_or_fall_back_to_old_day(tmp_path: Path) -> None:
    primary, replica = _world(tmp_path, record=False)
    with DuckDBStore(primary) as store:
        _insert_head(store, _version_for(day=_DAY - timedelta(days=1)))
    _publish(primary, replica)
    with _client(tmp_path, primary, replica) as client:
        response = client.get(_PATH)
    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "no_records"
    assert body["decision_date"] == "2026-04-15"
    assert body["record_count"] is None
    assert all(
        field["known_count"] is None and field["unknown_count"] is None for field in body["fields"]
    )


@pytest.mark.parametrize(
    ("now", "expected_date", "waiting"),
    [
        (_now(hour=10), "2026-04-14", True),
        (_now(hour=15, minute=30), "2026-04-14", True),
        (_now(hour=17, minute=0), "2026-04-15", False),
        (_now(date(2026, 4, 16), hour=10), None, False),
    ],
)
def test_decision_day_is_fixed_by_injected_shanghai_clock(
    tmp_path: Path,
    now: datetime,
    expected_date: str | None,
    waiting: bool,
) -> None:
    primary, replica = _world(tmp_path, record=False)
    with _client(tmp_path, primary, replica, now=now) as client:
        response = client.get(_PATH)
    assert response.status_code == 200
    body = response.json()
    assert body["decision_date"] == expected_date
    assert body["waiting_for_today"] is waiting
    assert body["status"] == ("calendar_unavailable" if expected_date is None else "no_records")


@pytest.mark.parametrize("change", ["drop_today", "drop_middle", "bad_source", "bad_chain"])
def test_incomplete_calendar_has_no_date_or_counts(tmp_path: Path, change: str) -> None:
    primary, replica = _world(tmp_path)
    with DuckDBStore(primary) as store:
        if change == "drop_today":
            store._conn.execute("DELETE FROM trade_calendar WHERE cal_date = ?", [_DAY])
        elif change == "drop_middle":
            store._conn.execute(
                "DELETE FROM trade_calendar WHERE cal_date = ?", [_DAY - timedelta(days=1)]
            )
        elif change == "bad_source":
            store._conn.execute(
                "UPDATE trade_calendar SET source = 'fixture' WHERE cal_date = ?", [_DAY]
            )
        else:
            store._conn.execute(
                "UPDATE trade_calendar SET pretrade_date = ? WHERE cal_date = ?",
                [_DAY - timedelta(days=2), _DAY],
            )
    _publish(primary, replica)
    with _client(tmp_path, primary, replica) as client:
        response = client.get(_PATH)
    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "calendar_unavailable"
    assert body["decision_date"] is None
    assert body["record_count"] is None


def test_no_config_is_distinct_from_empty_day(tmp_path: Path) -> None:
    with _client(tmp_path, None, None) as client:
        response = client.get(_PATH)
    assert response.status_code == 200
    assert response.json()["status"] == "not_configured"
    assert response.json()["source"] is None
    assert response.json()["record_count"] is None


def test_older_head_revision_cannot_hide_newer_version(tmp_path: Path) -> None:
    primary, replica = _world(tmp_path)
    with DuckDBStore(primary) as store:
        _insert_version(store, _version_for(revision=2))
    _publish(primary, replica)
    with _client(tmp_path, primary, replica) as client:
        response = client.get(_PATH)
    assert response.status_code == 503
    assert "record_count" not in response.text


@pytest.mark.parametrize(
    "damage", ["missing_version", "wrong_decision", "bad_number", "bad_json", "extra_bad_head"]
)
def test_one_bad_head_invalidates_the_whole_summary(tmp_path: Path, damage: str) -> None:
    primary, replica = _world(tmp_path)
    with DuckDBStore(primary) as store:
        if damage == "missing_version":
            store._conn.execute("UPDATE fundamental_daily_head SET version_id = repeat('0', 64)")
        elif damage == "wrong_decision":
            store._conn.execute(
                "UPDATE fundamental_daily_version SET decision_at = ?",
                [datetime.combine(_DAY, time(18), tzinfo=_SHANGHAI)],
            )
        elif damage == "bad_number":
            store._conn.execute("UPDATE fundamental_daily_version SET pb = 999")
        elif damage == "bad_json":
            store._conn.execute("UPDATE fundamental_daily_version SET fields_json = '[]'")
        else:
            store._conn.execute(
                "INSERT INTO fundamental_daily_head VALUES ('600002.SH', ?, repeat('0', 64), 1)",
                [_DAY],
            )
    _publish(primary, replica)
    with _client(tmp_path, primary, replica) as client:
        response = client.get(_PATH)
    assert response.status_code == 503
    assert "record_count" not in response.text


def test_unknown_reasons_are_closed_chinese_categories_with_exact_total(tmp_path: Path) -> None:
    primary, replica = _world(tmp_path, record=False)
    reasons = (
        "no_facts",
        "no_visible_version",
        "missing_value",
        "invalid_calendar",
        "unrepresentable_numeric",
        "future_new_reason",
    )
    with DuckDBStore(primary) as store:
        for index, reason in enumerate(reasons, start=1):
            _insert_head(
                store,
                _version_for(code=f"6000{index:02d}.SH", missing={"pe_ttm": reason}),
            )
    _publish(primary, replica)
    with _client(tmp_path, primary, replica) as client:
        response = client.get(_PATH)
    assert response.status_code == 200
    body = response.json()
    pe = next(field for field in body["fields"] if field["key"] == "pe_ttm")
    assert pe["known_count"] == 0
    assert pe["unknown_count"] == 6
    assert sum(item["count"] for item in pe["reasons"]) == 6
    assert pe["reasons"] == [
        {"label": "其他原因", "count": 3},
        {"label": "尚无来源记录", "count": 1},
        {"label": "披露尚未可见", "count": 1},
        {"label": "字段缺值", "count": 1},
    ]
    assert all(reason not in str(body) for reason in reasons)


def test_negative_pe_is_a_known_value_not_a_missing_record(tmp_path: Path) -> None:
    primary, replica = _world(tmp_path, record=False)
    with DuckDBStore(primary) as store:
        _insert_head(store, _version_for(pe=-3))
    _publish(primary, replica)
    with _client(tmp_path, primary, replica) as client:
        response = client.get(_PATH)
    assert response.status_code == 200
    pe = next(field for field in response.json()["fields"] if field["key"] == "pe_ttm")
    assert (pe["known_count"], pe["unknown_count"]) == (1, 0)


def test_api_contract_rejects_mismatched_counts_and_raw_reason(tmp_path: Path) -> None:
    primary, replica = _world(tmp_path)
    with _client(tmp_path, primary, replica) as client:
        body = client.get(_PATH).json()
    body["record_count"] = 2
    with pytest.raises(ValidationError):
        FundamentalSummaryData.model_validate(body)
    body["record_count"] = 1
    body["fields"][0]["reasons"][0]["label"] = "missing_value"
    with pytest.raises(ValidationError):
        FundamentalSummaryData.model_validate(body)


def test_closed_weekend_uses_previous_open_day_when_calendar_is_complete(tmp_path: Path) -> None:
    primary, replica = _world(tmp_path, record=False)
    for day, is_open, previous in (
        (date(2026, 4, 16), True, _DAY),
        (date(2026, 4, 17), True, date(2026, 4, 16)),
        (date(2026, 4, 18), False, date(2026, 4, 17)),
        (date(2026, 4, 19), False, date(2026, 4, 17)),
    ):
        with DuckDBStore(primary) as store:
            store._conn.execute(
                "INSERT INTO trade_calendar "
                "(exchange, cal_date, is_open, pretrade_date, source, updated_at) "
                "VALUES ('SSE', ?, ?, ?, 'tushare', ?)",
                [day, is_open, previous, _now()],
            )
    _publish(primary, replica)
    with _client(tmp_path, primary, replica, now=_now(date(2026, 4, 19), 12)) as client:
        response = client.get(_PATH)
    assert response.status_code == 200
    assert response.json()["status"] == "no_records"
    assert response.json()["decision_date"] == "2026-04-17"
    assert response.json()["waiting_for_today"] is False


def test_head_limit_is_checked_before_ignoring_bad_rows(tmp_path: Path) -> None:
    primary, replica = _world(tmp_path, record=False)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "INSERT INTO fundamental_daily_head "
            "SELECT lpad(i::VARCHAR, 6, '0') || '.SH', ?, repeat('0', 64), 1 "
            "FROM generate_series(1, 8001) AS g(i)",
            [_DAY],
        )
    _publish(primary, replica)
    with _client(tmp_path, primary, replica) as client:
        response = client.get(_PATH)
    assert response.status_code == 503
    assert "record_count" not in response.text


def test_old_identity_conflicts_and_read_in_rotation_discards_counts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, replica = _world(tmp_path)
    with _client(tmp_path, primary, replica) as client:
        source = client.get(_PATH).json()["source"]
        with DuckDBStore(primary) as store:
            store._conn.execute(
                "UPDATE trade_calendar SET updated_at = ? WHERE cal_date = ?",
                [_now() + timedelta(seconds=1), _DAY],
            )
        _publish(primary, replica)
        stale = client.get(_PATH, params={"expected_identity": source["identity"]})
        assert stale.status_code == 409
        assert "record_count" not in stale.text

        replica_source = client.app.state.web.screen_service.replica
        assert replica_source is not None
        finish = replica_source._finish

        def rotate(descriptor: int, before: object) -> None:
            with DuckDBStore(primary) as store:
                store._conn.execute(
                    "UPDATE trade_calendar SET updated_at = ? WHERE cal_date = ?",
                    [_now() + timedelta(seconds=2), _DAY],
                )
            _publish(primary, replica)
            finish(descriptor, before)

        monkeypatch.setattr(replica_source, "_finish", rotate)
        changed = client.get(_PATH)
    assert changed.status_code == 503
    assert "record_count" not in changed.text
