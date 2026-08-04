"""市场全景爆量历史检索的纯展示与历史详情单测。"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from rquant.dashboard import market_panorama as panorama


def _surge_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "confirmed_at": "10:15",
        "ts_code": "688255.SH",
        "name": "芯片先锋",
        "theme": "半导体",
        "price": 12.34,
        "pct_chg": 5.67,
        "rel_cum": 3.2,
        "cum_amount": 120_000_000,
        "room_to_limit_pct": 4.3,
        "status": "confirmed",
    }
    row.update(overrides)
    return row


def test_surge_history_display_inserts_normalized_trade_date() -> None:
    rows = pd.DataFrame([
        _surge_row(trade_date=date(2026, 7, 29)),
        _surge_row(trade_date="2026-07-28"),
    ])

    display = panorama._surge_history_display(rows)

    assert list(display.columns)[0] == "日期"
    assert list(display["日期"]) == ["2026-07-29", "2026-07-28"]
    assert "trade_date" not in display.columns


def test_daily_surge_display_does_not_add_date_column() -> None:
    display = panorama._surge_log_display(pd.DataFrame([_surge_row()]))

    assert "日期" not in display.columns


def test_cached_surge_history_strips_query_before_loading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        panorama,
        "search_surge_history",
        lambda query: calls.append(query) or pd.DataFrame(),
    )
    panorama.cached_surge_history.clear()

    try:
        panorama.cached_surge_history("  芯片  ")
    finally:
        panorama.cached_surge_history.clear()

    assert calls == ["芯片"]


def test_surge_history_table_key_tracks_query_and_ordered_result_identity() -> None:
    rows = pd.DataFrame([
        _surge_row(trade_date="2026-07-29", confirmed_at="10:15"),
        _surge_row(trade_date="2026-07-28", confirmed_at="09:45"),
    ])
    duplicate = rows.copy()
    inserted = pd.concat(
        [pd.DataFrame([_surge_row(trade_date="2026-07-30", confirmed_at="09:31")]), rows],
        ignore_index=True,
    )
    reordered = rows.iloc[::-1].reset_index(drop=True)

    original_key = panorama._surge_history_table_key("  芯片  ", rows)

    assert original_key == panorama._surge_history_table_key("芯片", duplicate)
    assert original_key != panorama._surge_history_table_key("半导体", rows)
    assert original_key != panorama._surge_history_table_key("芯片", inserted)
    assert original_key != panorama._surge_history_table_key("芯片", reordered)
    assert "芯片" not in panorama._surge_history_table_key("芯片" * 80, rows)


def test_historical_surge_detail_loads_day_trend_and_all_event_marks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    day = date(2026, 7, 29)
    calls: list[tuple[str, object]] = []
    trend = pd.DataFrame({
        "dt": pd.to_datetime(["2026-07-29 09:30", "2026-07-29 10:15"]),
        "price": [10.0, 10.5],
        "avg_price": [10.0, 10.25],
        "volume": [100.0, 200.0],
    })
    marks = pd.DataFrame([{
        "date": day, "confirmed_at": "10:15", "rel_cum": 3.2,
    }])
    monkeypatch.setattr(
        panorama,
        "cached_historical_intraday_trend",
        lambda ts_code, day_key: calls.append(("trend", (ts_code, day_key))) or trend,
    )
    monkeypatch.setattr(
        panorama,
        "cached_surge_event_marks",
        lambda ts_code, day_key: calls.append(("marks", (ts_code, day_key))) or marks,
    )
    monkeypatch.setattr(panorama.st, "markdown", lambda text: calls.append(("title", text)))
    monkeypatch.setattr(
        panorama.st, "altair_chart", lambda chart, **_: calls.append(("chart", chart))
    )
    monkeypatch.setattr(panorama.st, "caption", lambda text: calls.append(("caption", text)))

    panorama.render_historical_surge_detail("688255.SH", "芯片先锋", day)

    assert ("trend", ("688255.SH", "2026-07-29")) in calls
    assert ("marks", ("688255.SH", "2026-07-29")) in calls
    assert any(kind == "chart" for kind, _ in calls)


def test_historical_surge_detail_explains_unavailable_minute_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages: list[str] = []
    monkeypatch.setattr(
        panorama, "cached_historical_intraday_trend", lambda *_: pd.DataFrame()
    )
    monkeypatch.setattr(panorama.st, "markdown", lambda _: None)
    monkeypatch.setattr(panorama.st, "info", messages.append)

    panorama.render_historical_surge_detail("688255.SH", "芯片先锋", date(2026, 7, 29))

    assert "该日分钟数据未入库/暂不可用" in messages[0]
    assert "只读副本" in messages[0]
