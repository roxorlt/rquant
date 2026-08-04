"""panorama_data 新增 loader 单测：pulse 历史 / 异动 / runtime_config / 爆量标记 + fake 覆盖。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from rquant.panorama_data import (
    load_pulse_alerts,
    load_pulse_history,
    load_surge_log,
    load_surge_marks,
    load_surge_runtime_config,
    search_surge_history,
    surge_mark_positions,
    volume_directions,
)


def _write_lines(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows),
                    encoding="utf-8")


DAY = date(2026, 7, 29)


class TestPulseHistory:
    def test_reads_and_skips_bad_lines(self, tmp_path: Path) -> None:
        p = tmp_path / f"pulse-{DAY.isoformat()}.jsonl"
        _write_lines(p, [
            {"t": "09:31", "limit_up": 20, "limit_down": 2, "broken": 1,
             "up": 2600, "down": 2400, "up_ratio_pct": 50.0, "total": 5400},
            {"t": "09:32", "limit_up": 21, "limit_down": 2, "broken": 1,
             "up": 2610, "down": 2390, "up_ratio_pct": 50.2, "total": 5400},
        ])
        with p.open("a", encoding="utf-8") as f:
            f.write("BROKEN\n{\"no_t\": 1}\n")
        df = load_pulse_history(DAY, live_dir=tmp_path)
        assert list(df["t"]) == ["09:31", "09:32"]
        assert df.iloc[1]["limit_up"] == 21

    def test_missing_file_empty_with_columns(self, tmp_path: Path) -> None:
        df = load_pulse_history(DAY, live_dir=tmp_path)
        assert df.empty and "limit_up" in df.columns


class TestPulseAlerts:
    def test_reads_alerts(self, tmp_path: Path) -> None:
        p = tmp_path / f"pulse_alerts-{DAY.isoformat()}.jsonl"
        _write_lines(p, [{
            "t": "10:15", "kind": "broken_surge", "kind_label": "炸板潮",
            "before": 2, "after": 6, "window_minutes": 10,
            "message": "炸板 10 分钟 2 → 6（+4）",
        }])
        df = load_pulse_alerts(DAY, live_dir=tmp_path)
        assert len(df) == 1 and df.iloc[0]["kind_label"] == "炸板潮"

    def test_missing_file_empty(self, tmp_path: Path) -> None:
        assert load_pulse_alerts(DAY, live_dir=tmp_path).empty


class TestRuntimeConfig:
    def test_reads_config(self, tmp_path: Path) -> None:
        (tmp_path / "runtime_config.json").write_text(
            json.dumps({"boards": ["main", "gem"], "k_cum": 2.5, "ratio_cap": 8.0}),
            encoding="utf-8",
        )
        cfg = load_surge_runtime_config(live_dir=tmp_path)
        assert cfg is not None and cfg["boards"] == ["main", "gem"]

    def test_missing_or_broken_none(self, tmp_path: Path) -> None:
        assert load_surge_runtime_config(live_dir=tmp_path) is None
        (tmp_path / "runtime_config.json").write_text("nope", encoding="utf-8")
        assert load_surge_runtime_config(live_dir=tmp_path) is None


class TestSurgeMarks:
    def test_earliest_per_day_and_missing_days_skipped(self, tmp_path: Path) -> None:
        d1, d2, d3 = date(2026, 7, 27), date(2026, 7, 28), date(2026, 7, 29)
        _write_lines(tmp_path / f"events-{d1.isoformat()}.jsonl", [
            {"ts_code": "688255.SH", "confirmed_at": "10:31", "rel_cum": 4.0},
            {"ts_code": "688255.SH", "confirmed_at": "09:47", "rel_cum": 3.2},
            {"ts_code": "300409.SZ", "confirmed_at": "09:40", "rel_cum": 2.8},
        ])
        _write_lines(tmp_path / f"events-{d3.isoformat()}.jsonl", [
            {"ts_code": "688255.SH", "confirmed_at": "13:05", "rel_cum": 5.5},
        ])
        df = load_surge_marks("688255.SH", [d1, d2, d3], live_dir=tmp_path)
        assert list(df["confirmed_at"]) == ["09:47", "13:05"]  # d2 无文件跳过
        assert list(df["date"]) == [d1, d3]
        assert df.iloc[0]["rel_cum"] == pytest.approx(3.2)

    def test_no_hit_empty(self, tmp_path: Path) -> None:
        df = load_surge_marks("000001.SZ", [DAY], live_dir=tmp_path)
        assert df.empty and list(df.columns) == ["date", "confirmed_at", "rel_cum"]


class TestFakeMode:
    def test_fake_covers_all_new_loaders(self, monkeypatch) -> None:
        monkeypatch.setenv("RQUANT_PANORAMA_FAKE", "1")
        hist = load_pulse_history()
        assert len(hist) >= 60 and hist.iloc[0]["t"] == "09:30"
        alerts = load_pulse_alerts()
        assert len(alerts) == 1 and alerts.iloc[0]["kind"] == "broken_surge"
        cfg = load_surge_runtime_config()
        assert cfg is not None and set(cfg["boards"]) == {"main", "gem", "star", "bj"}
        log = load_surge_log()
        assert len(log) == 3 and "600001.SH" in set(log["ts_code"])
        marks = load_surge_marks("600001.SH", [date.today()])
        assert len(marks) == 1


class TestSurgeLogHistoricalDay:
    """爆量记录历史日期回看（UI 新增日期选择器所依赖的 loader 契约）：不同日期各读各的
    ``events-<day>.jsonl``，互不串台。"""

    def test_reads_own_day_file_per_day(self, tmp_path: Path) -> None:
        d1, d2 = date(2026, 7, 28), date(2026, 7, 29)
        _write_lines(tmp_path / f"events-{d1.isoformat()}.jsonl", [
            {"ts_code": "600001.SH", "confirmed_at": "09:31", "rel_cum": 3.0},
        ])
        _write_lines(tmp_path / f"events-{d2.isoformat()}.jsonl", [
            {"ts_code": "300002.SZ", "confirmed_at": "10:00", "rel_cum": 4.0},
        ])
        df1 = load_surge_log(d1, live_dir=tmp_path)
        df2 = load_surge_log(d2, live_dir=tmp_path)
        assert list(df1["ts_code"]) == ["600001.SH"]
        assert list(df2["ts_code"]) == ["300002.SZ"]


class TestSurgeHistorySearch:
    def test_searches_code_and_name_across_days_in_reverse_chronological_order(
        self, tmp_path: Path
    ) -> None:
        d1, d2, d3 = date(2026, 7, 27), date(2026, 7, 28), date(2026, 7, 29)
        _write_lines(tmp_path / f"events-{d1.isoformat()}.jsonl", [
            {"ts_code": "688255.SH", "name": "芯片先锋", "confirmed_at": "10:31"},
            {"ts_code": "688255.SH", "name": "芯片先锋", "confirmed_at": "09:47"},
        ])
        _write_lines(tmp_path / f"events-{d2.isoformat()}.jsonl", [
            {"ts_code": "688255.SH", "name": "芯片先锋", "confirmed_at": "10:00"},
            {"ts_code": "300409.SZ", "name": "算力股份", "confirmed_at": "09:40"},
        ])
        _write_lines(tmp_path / f"events-{d3.isoformat()}.jsonl", [
            {"ts_code": "688255.SH", "name": "芯片先锋", "confirmed_at": "09:35"},
        ])

        by_code = search_surge_history("  688255  ", live_dir=tmp_path)
        assert list(by_code["trade_date"]) == [d3, d2, d1]
        assert list(by_code["confirmed_at"]) == ["09:35", "10:00", "09:47"]

        by_name = search_surge_history("芯片", live_dir=tmp_path)
        assert list(by_name["ts_code"]) == ["688255.SH", "688255.SH", "688255.SH"]

    def test_casefolds_query_and_skips_bad_event_filenames_and_records(
        self, tmp_path: Path
    ) -> None:
        _write_lines(tmp_path / "events-2026-07-29.jsonl", [
            {"ts_code": "600001.SH", "name": "Alpha Tech", "confirmed_at": "09:31"},
        ])
        _write_lines(tmp_path / "events-not-a-date.jsonl", [
            {"ts_code": "600002.SH", "name": "Alpha Invalid", "confirmed_at": "09:32"},
        ])
        _write_lines(tmp_path / "events-2026-07-28.jsonl.bak", [
            {"ts_code": "600003.SH", "name": "Alpha Backup", "confirmed_at": "09:33"},
        ])
        _write_lines(tmp_path / "events-20260729.jsonl", [
            {"ts_code": "600004.SH", "name": "Alpha Compact", "confirmed_at": "09:34"},
        ])
        _write_lines(tmp_path / "events-2026-W31-3.jsonl", [
            {"ts_code": "600005.SH", "name": "Alpha Week", "confirmed_at": "09:35"},
        ])
        (tmp_path / "events-2026-07-27.jsonl").write_text("{bad json}\n", encoding="utf-8")

        df = search_surge_history("  alpha  ", live_dir=tmp_path)
        assert list(df["ts_code"]) == ["600001.SH"]

    def test_empty_query_no_match_and_empty_directory_return_stable_empty_columns(
        self, tmp_path: Path
    ) -> None:
        empty_query = search_surge_history("   ", live_dir=tmp_path)
        empty_directory = search_surge_history("688255", live_dir=tmp_path)
        _write_lines(tmp_path / "events-2026-07-29.jsonl", [
            {"ts_code": "600001.SH", "name": "芯片先锋", "confirmed_at": "09:31"},
        ])
        no_match = search_surge_history("不存在", live_dir=tmp_path)
        expected = [
            "trade_date", "confirmed_at", "ts_code", "name", "theme", "pct_chg",
            "cum_amount", "rel_cum", "room_to_limit_pct", "status",
        ]
        assert empty_query.empty and list(empty_query.columns) == expected
        assert empty_directory.empty and list(empty_directory.columns) == expected
        assert no_match.empty and list(no_match.columns) == expected


def _trend(day: str, times: list[str], prices: list[float]) -> pd.DataFrame:
    return pd.DataFrame({
        "dt": pd.to_datetime([f"{day} {t}" for t in times]),
        "price": prices,
        "avg_price": [float("nan")] * len(times),
        "volume": [100.0] * len(times),
    })


class TestSurgeMarkPositions:
    def test_exact_minute_hit(self) -> None:
        trend = _trend("2026-07-29", ["09:46", "09:47", "09:48"], [10.0, 10.5, 10.6])
        marks = pd.DataFrame([{"date": date(2026, 7, 29), "confirmed_at": "09:47",
                               "rel_cum": 3.2}])
        pos = surge_mark_positions(trend, marks)
        assert len(pos) == 1
        assert pos.iloc[0]["idx"] == 1 and pos.iloc[0]["price"] == pytest.approx(10.5)
        assert pos.iloc[0]["label"] == "09:47 首次爆量确认 · 3.2×"

    def test_missing_minute_falls_back_to_prior_bar(self) -> None:
        trend = _trend("2026-07-29", ["09:46", "09:49"], [10.0, 10.6])
        marks = pd.DataFrame([{"date": date(2026, 7, 29), "confirmed_at": "09:47",
                               "rel_cum": float("nan")}])
        pos = surge_mark_positions(trend, marks)
        assert pos.iloc[0]["idx"] == 0
        assert pos.iloc[0]["label"] == "09:47 首次爆量确认"  # rel_cum 缺失不带倍数

    def test_day_absent_skipped_and_empty_inputs(self) -> None:
        trend = _trend("2026-07-29", ["09:46"], [10.0])
        marks = pd.DataFrame([{"date": date(2026, 7, 28), "confirmed_at": "09:47",
                               "rel_cum": 2.0}])
        assert surge_mark_positions(trend, marks).empty
        assert surge_mark_positions(trend, pd.DataFrame()).empty
        assert surge_mark_positions(pd.DataFrame(), marks).empty

    def test_multiple_marks_same_day(self) -> None:
        trend = _trend("2026-07-29", ["09:45", "09:47", "10:15"], [10.0, 10.5, 11.0])
        marks = pd.DataFrame([
            {"date": date(2026, 7, 29), "confirmed_at": "09:47", "rel_cum": 3.2},
            {"date": date(2026, 7, 29), "confirmed_at": "10:15", "rel_cum": 4.5},
        ])
        pos = surge_mark_positions(trend, marks)
        assert len(pos) == 2
        assert pos.iloc[0]["idx"] == 1 and pos.iloc[0]["price"] == pytest.approx(10.5)
        assert pos.iloc[1]["idx"] == 2 and pos.iloc[1]["price"] == pytest.approx(11.0)
        assert "3.2×" in pos.iloc[0]["label"]
        assert "4.5×" in pos.iloc[1]["label"]


class TestVolumeDirections:
    def test_directions(self) -> None:
        prices = pd.Series([10.0, 10.2, 10.2, 10.1])
        assert list(volume_directions(prices)) == ["flat", "up", "flat", "down"]

    def test_none_input_returns_empty(self) -> None:
        result = volume_directions(None)
        assert result.empty and result.dtype == "object"

    def test_empty_series_returns_empty(self) -> None:
        result = volume_directions(pd.Series(dtype="float64"))
        assert result.empty and result.dtype == "object"
