"""panorama_data 新增 loader 单测：pulse 历史 / 异动 / runtime_config / 爆量标记 + fake 覆盖。"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest

import rquant.panorama_data as panorama_data
from rquant.dashboard.serving_only_page_data import ServingFrameResult, ServingFrameState
from rquant.panorama_data import (
    load_historical_intraday_trend,
    load_pulse_alerts,
    load_pulse_history,
    load_surge_event_marks,
    load_surge_log,
    load_surge_marks,
    load_surge_runtime_config,
    search_surge_history,
    surge_mark_positions,
    volume_directions,
)


def _write_lines(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows), encoding="utf-8"
    )


DAY = date(2026, 7, 29)


class TestPulseHistory:
    def test_reads_and_skips_bad_lines(self, tmp_path: Path) -> None:
        p = tmp_path / f"pulse-{DAY.isoformat()}.jsonl"
        _write_lines(
            p,
            [
                {
                    "t": "09:31",
                    "limit_up": 20,
                    "limit_down": 2,
                    "broken": 1,
                    "up": 2600,
                    "down": 2400,
                    "up_ratio_pct": 50.0,
                    "total": 5400,
                },
                {
                    "t": "09:32",
                    "limit_up": 21,
                    "limit_down": 2,
                    "broken": 1,
                    "up": 2610,
                    "down": 2390,
                    "up_ratio_pct": 50.2,
                    "total": 5400,
                },
            ],
        )
        with p.open("a", encoding="utf-8") as f:
            f.write('BROKEN\n{"no_t": 1}\n')
        df = load_pulse_history(DAY, live_dir=tmp_path)
        assert list(df["t"]) == ["09:31", "09:32"]
        assert df.iloc[1]["limit_up"] == 21

    def test_missing_file_empty_with_columns(self, tmp_path: Path) -> None:
        df = load_pulse_history(DAY, live_dir=tmp_path)
        assert df.empty and "limit_up" in df.columns


class TestPulseAlerts:
    def test_reads_alerts(self, tmp_path: Path) -> None:
        p = tmp_path / f"pulse_alerts-{DAY.isoformat()}.jsonl"
        _write_lines(
            p,
            [
                {
                    "t": "10:15",
                    "kind": "broken_surge",
                    "kind_label": "炸板潮",
                    "before": 2,
                    "after": 6,
                    "window_minutes": 10,
                    "message": "炸板 10 分钟 2 → 6（+4）",
                }
            ],
        )
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


def test_production_pulse_and_runtime_loaders_use_one_injected_serving_generation() -> None:
    generation_id = "a" * 64

    class _Result:
        def __init__(self, frame: pd.DataFrame) -> None:
            self._frame = frame

        def fetchdf(self) -> pd.DataFrame:
            return self._frame.copy()

    class _Connection:
        queries: list[str] = []

        def execute(self, sql: str, _parameters: object) -> _Result:
            self.queries.append(sql)
            if "FROM pulse_history" in sql:
                return _Result(
                    pd.DataFrame(
                        {
                            "trade_date": [DAY],
                            "as_of": ["2026-07-29T01:31:00Z"],
                            "t": ["09:31"],
                            "limit_up": [20],
                            "limit_down": [2],
                            "broken": [1],
                            "up": [2600],
                            "down": [2400],
                            "up_ratio_pct": [50.0],
                            "total": [5400],
                        }
                    )
                )
            if "FROM pulse_alert" in sql:
                return _Result(
                    pd.DataFrame(
                        {
                            "trade_date": [DAY],
                            "as_of": ["2026-07-29T02:15:00Z"],
                            "t": ["10:15"],
                            "kind": ["broken_surge"],
                            "kind_label": ["炸板潮"],
                            "before": [2.0],
                            "after": [6.0],
                            "window_minutes": [10],
                            "message": ["炸板异动"],
                        }
                    )
                )
            if "FROM surge_runtime_config" in sql:
                return _Result(
                    pd.DataFrame(
                        {
                            "snapshot_key": ["current"],
                            "trade_date": [DAY],
                            "as_of": ["2026-07-29T01:25:00Z"],
                            "boards_json": ['["main","gem"]'],
                            "k_rough": [1.2],
                            "k_cum": [2.5],
                            "ratio_cap": [8.0],
                            "skip_first_minutes": [5],
                            "tushare_rate_per_min": [2],
                            "require_price_strength": [True],
                            "max_room_to_limit_pct": [3.0],
                        }
                    )
                )
            raise AssertionError(f"unexpected serving query: {sql}")

    class _Store:
        _conn = _Connection()

        def close(self) -> None:
            return None

        def serving_health(self) -> None:
            return None

        generation_id = "a" * 64

    store = _Store()
    history = load_pulse_history(DAY, store=store)
    alerts = load_pulse_alerts(DAY, store=store)
    runtime = load_surge_runtime_config(store=store)

    assert history["t"].tolist() == ["09:31"]
    assert alerts["kind"].tolist() == ["broken_surge"]
    assert runtime.config is not None
    assert runtime.config.boards == ("main", "gem")
    assert {
        history.attrs["serving_generation_id"],
        alerts.attrs["serving_generation_id"],
        runtime.generation_id,
    } == {generation_id}
    assert len(store._conn.queries) == 3


def test_missing_serving_pulse_projections_are_explicit_without_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = "b" * 64
    generated_at = datetime(2026, 7, 29, 1, 30, tzinfo=UTC)

    class _Connection:
        def execute(self, sql: str, _parameters: object) -> object:
            raise RuntimeError(f"projection not published in generation: {sql.split('FROM')[1]}")

    class _Store:
        _conn = _Connection()
        generation_id = "b" * 64
        evidence = ServingFrameResult(
            state=ServingFrameState.UNAVAILABLE,
            detail="serving projection not published",
            generation_id=generation_id,
            generated_at=generated_at,
        )

        def close(self) -> None:
            return None

        def serving_health(self) -> ServingFrameResult:
            return self.evidence

    def fallback_called(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("operational or source fallback must not be called")

    monkeypatch.setattr(panorama_data, "_open_serving_store", fallback_called)
    store = _Store()

    history = load_pulse_history(DAY, store=store)
    alerts = load_pulse_alerts(DAY, store=store)
    runtime = load_surge_runtime_config(store=store)

    assert history.empty and history.attrs["serving_state"] == "unavailable"
    assert alerts.empty and alerts.attrs["serving_state"] == "unavailable"
    assert runtime.state.value == "unavailable" and runtime.config is None
    assert {
        history.attrs["serving_generation_id"],
        alerts.attrs["serving_generation_id"],
        runtime.generation_id,
    } == {generation_id}
    assert {
        history.attrs["serving_generated_at"],
        alerts.attrs["serving_generated_at"],
        runtime.generated_at,
    } == {generated_at}


class TestSurgeMarks:
    def test_earliest_per_day_and_missing_days_skipped(self, tmp_path: Path) -> None:
        d1, d2, d3 = date(2026, 7, 27), date(2026, 7, 28), date(2026, 7, 29)
        _write_lines(
            tmp_path / f"events-{d1.isoformat()}.jsonl",
            [
                {"ts_code": "688255.SH", "confirmed_at": "10:31", "rel_cum": 4.0},
                {"ts_code": "688255.SH", "confirmed_at": "09:47", "rel_cum": 3.2},
                {"ts_code": "300409.SZ", "confirmed_at": "09:40", "rel_cum": 2.8},
            ],
        )
        _write_lines(
            tmp_path / f"events-{d3.isoformat()}.jsonl",
            [
                {"ts_code": "688255.SH", "confirmed_at": "13:05", "rel_cum": 5.5},
            ],
        )
        df = load_surge_marks("688255.SH", [d1, d2, d3], live_dir=tmp_path)
        assert list(df["confirmed_at"]) == ["09:47", "13:05"]  # d2 无文件跳过
        assert list(df["date"]) == [d1, d3]
        assert df.iloc[0]["rel_cum"] == pytest.approx(3.2)

    def test_no_hit_empty(self, tmp_path: Path) -> None:
        df = load_surge_marks("000001.SZ", [DAY], live_dir=tmp_path)
        assert df.empty and list(df.columns) == ["date", "confirmed_at", "rel_cum"]


class TestSurgeEventMarks:
    def test_reads_all_valid_events_for_one_code_in_stable_time_order(self, tmp_path: Path) -> None:
        _write_lines(
            tmp_path / f"events-{DAY.isoformat()}.jsonl",
            [
                {"ts_code": "688255.SH", "confirmed_at": "10:31", "rel_cum": "4.0"},
                {"ts_code": "688255.SH", "confirmed_at": "09:47", "rel_cum": 3.2},
                {"ts_code": "688255.SH", "confirmed_at": "09:47", "rel_cum": 3.3},
                {"ts_code": "688255.SH", "confirmed_at": "10:45"},
                {"ts_code": "688255.SH", "confirmed_at": "not-a-time", "rel_cum": 9.0},
                {"ts_code": "688255.SH", "confirmed_at": "9:05", "rel_cum": 9.0},
                {"ts_code": "688255.SH", "confirmed_at": "12:99", "rel_cum": 9.0},
                {"ts_code": "688255.SH", "confirmed_at": "24:00", "rel_cum": 9.0},
                {"ts_code": "300409.SZ", "confirmed_at": "09:40", "rel_cum": 2.8},
                {"ts_code": "688255.SH", "rel_cum": 5.0},
            ],
        )
        with (tmp_path / f"events-{DAY.isoformat()}.jsonl").open("a", encoding="utf-8") as f:
            f.write("{bad json}\n")

        df = load_surge_event_marks("688255.SH", DAY, live_dir=tmp_path)

        assert list(df.columns) == ["date", "confirmed_at", "rel_cum"]
        assert list(df["date"]) == [DAY, DAY, DAY, DAY]
        assert list(df["confirmed_at"]) == ["09:47", "09:47", "10:31", "10:45"]
        assert list(df["rel_cum"].iloc[:3]) == pytest.approx([3.2, 3.3, 4.0])
        assert pd.isna(df.iloc[-1]["rel_cum"])

    def test_empty_file_and_no_match_have_stable_columns(self, tmp_path: Path) -> None:
        empty = load_surge_event_marks("688255.SH", DAY, live_dir=tmp_path)
        _write_lines(
            tmp_path / f"events-{DAY.isoformat()}.jsonl",
            [
                {"ts_code": "300409.SZ", "confirmed_at": "09:40", "rel_cum": 2.8},
            ],
        )
        no_match = load_surge_event_marks("688255.SH", DAY, live_dir=tmp_path)
        assert empty.empty and list(empty.columns) == ["date", "confirmed_at", "rel_cum"]
        assert no_match.empty and list(no_match.columns) == ["date", "confirmed_at", "rel_cum"]

    def test_file_exists_oserror_returns_stable_empty_with_warning(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        warnings: list[str] = []

        def fail_exists(_: Path) -> bool:
            raise OSError("filesystem unavailable")

        monkeypatch.setattr(Path, "exists", fail_exists)
        monkeypatch.setattr(
            panorama_data.logger, "warning", lambda message: warnings.append(str(message))
        )

        df = load_surge_event_marks("688255.SH", DAY, live_dir=tmp_path)

        assert df.empty and list(df.columns) == ["date", "confirmed_at", "rel_cum"]
        assert any(
            "OSError" in message and f"events-{DAY}.jsonl" in message for message in warnings
        )

    def test_unicode_decode_error_returns_stable_empty_with_warning(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = tmp_path / f"events-{DAY.isoformat()}.jsonl"
        path.write_bytes(b"\xff")
        warnings: list[str] = []
        monkeypatch.setattr(
            panorama_data.logger, "warning", lambda message: warnings.append(str(message))
        )

        df = load_surge_event_marks("688255.SH", DAY, live_dir=tmp_path)

        assert df.empty and list(df.columns) == ["date", "confirmed_at", "rel_cum"]
        assert any("UnicodeDecodeError" in message and path.name in message for message in warnings)


class TestHistoricalIntradayTrend:
    def test_reads_from_an_injected_serving_reader_when_operational_db_is_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class _Result:
            def fetchdf(self) -> pd.DataFrame:
                return pd.DataFrame(
                    {
                        "dt": ["2026-07-29 09:30", "2026-07-29 09:31"],
                        "price": [10.0, 11.0],
                        "volume": [100.0, 200.0],
                        "amount": [1_000.0, 2_200.0],
                    }
                )

        class _Connection:
            def execute(self, _sql: str, _parameters: object) -> _Result:
                return _Result()

        class _ServingReader:
            _conn = _Connection()
            generation_id = "serving-generation-1"

        def operational_fallback_called(*_args: object, **_kwargs: object) -> object:
            raise AssertionError("operational database fallback must not be called")

        monkeypatch.setattr(panorama_data, "_open_serving_store", operational_fallback_called)

        trend = load_historical_intraday_trend("688255.SH", DAY, store=_ServingReader())

        assert list(trend["price"]) == pytest.approx([10.0, 11.0])
        assert trend.attrs["serving_generation_id"] == "serving-generation-1"

    def test_empty_serving_projection_returns_stable_empty_columns(self) -> None:
        class _Result:
            def fetchdf(self) -> pd.DataFrame:
                return pd.DataFrame(columns=["dt", "price", "volume", "amount"])

        class _Connection:
            def execute(self, _sql: str, _parameters: object) -> _Result:
                return _Result()

        class _ServingReader:
            _conn = _Connection()
            generation_id = "serving-generation-empty"

        trend = load_historical_intraday_trend("688255.SH", DAY, store=_ServingReader())
        assert trend.empty and list(trend.columns) == ["dt", "price", "avg_price", "volume"]

    def test_open_failure_returns_stable_empty_columns(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def boom(*args, **kwargs):
            raise OSError("readonly replica unavailable")

        monkeypatch.setattr(panorama_data, "_open_serving_store", boom)
        trend = load_historical_intraday_trend("688255.SH", DAY)
        assert trend.empty and list(trend.columns) == ["dt", "price", "avg_price", "volume"]


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
        trend = load_historical_intraday_trend("600001.SH", date(2026, 7, 29))
        assert list(trend.columns) == ["dt", "price", "avg_price", "volume"]
        assert len(trend) == 240 and set(trend["dt"].dt.date) == {date(2026, 7, 29)}


class TestSurgeLogHistoricalDay:
    """爆量记录历史日期回看（UI 新增日期选择器所依赖的 loader 契约）：不同日期各读各的
    ``events-<day>.jsonl``，互不串台。"""

    def test_reads_own_day_file_per_day(self, tmp_path: Path) -> None:
        d1, d2 = date(2026, 7, 28), date(2026, 7, 29)
        _write_lines(
            tmp_path / f"events-{d1.isoformat()}.jsonl",
            [
                {"ts_code": "600001.SH", "confirmed_at": "09:31", "rel_cum": 3.0},
            ],
        )
        _write_lines(
            tmp_path / f"events-{d2.isoformat()}.jsonl",
            [
                {"ts_code": "300002.SZ", "confirmed_at": "10:00", "rel_cum": 4.0},
            ],
        )
        df1 = load_surge_log(d1, live_dir=tmp_path)
        df2 = load_surge_log(d2, live_dir=tmp_path)
        assert list(df1["ts_code"]) == ["600001.SH"]
        assert list(df2["ts_code"]) == ["300002.SZ"]


class TestSurgeHistorySearch:
    def test_searches_code_and_name_across_days_in_reverse_chronological_order(
        self, tmp_path: Path
    ) -> None:
        d1, d2, d3 = date(2026, 7, 27), date(2026, 7, 28), date(2026, 7, 29)
        _write_lines(
            tmp_path / f"events-{d1.isoformat()}.jsonl",
            [
                {"ts_code": "688255.SH", "name": "芯片先锋", "confirmed_at": "10:31"},
                {"ts_code": "688255.SH", "name": "芯片先锋", "confirmed_at": "09:47"},
            ],
        )
        _write_lines(
            tmp_path / f"events-{d2.isoformat()}.jsonl",
            [
                {"ts_code": "688255.SH", "name": "芯片先锋", "confirmed_at": "10:00"},
                {"ts_code": "300409.SZ", "name": "算力股份", "confirmed_at": "09:40"},
            ],
        )
        _write_lines(
            tmp_path / f"events-{d3.isoformat()}.jsonl",
            [
                {"ts_code": "688255.SH", "name": "芯片先锋", "confirmed_at": "09:35"},
                {"ts_code": "688999.SH", "name": "芯片龙头", "confirmed_at": "10:30"},
            ],
        )

        by_code = search_surge_history("  688255  ", live_dir=tmp_path)
        assert list(by_code["trade_date"]) == [d3, d2, d1]
        assert list(by_code["confirmed_at"]) == ["09:35", "10:00", "09:47"]

        by_name = search_surge_history("芯片", live_dir=tmp_path)
        assert list(by_name["ts_code"]) == ["688999.SH", "688255.SH", "688255.SH", "688255.SH"]
        assert list(by_name["confirmed_at"]) == ["10:30", "09:35", "10:00", "09:47"]

    def test_preserves_price_through_history_normalization(self, tmp_path: Path) -> None:
        _write_lines(
            tmp_path / "events-2026-07-29.jsonl",
            [
                {
                    "ts_code": "688255.SH",
                    "name": "芯片先锋",
                    "confirmed_at": "09:31",
                    "price": 12.34,
                }
            ],
        )

        df = search_surge_history("688255", live_dir=tmp_path)

        assert list(df.columns) == [
            "trade_date",
            "confirmed_at",
            "ts_code",
            "name",
            "theme",
            "price",
            "pct_chg",
            "cum_amount",
            "rel_cum",
            "room_to_limit_pct",
            "status",
        ]
        assert df.iloc[0]["price"] == pytest.approx(12.34)

    def test_casefolds_query_and_skips_bad_event_filenames_and_records(
        self, tmp_path: Path
    ) -> None:
        _write_lines(
            tmp_path / "events-2026-07-29.jsonl",
            [
                {"ts_code": "600001.SH", "name": "Alpha Tech", "confirmed_at": "09:31"},
            ],
        )
        _write_lines(
            tmp_path / "events-not-a-date.jsonl",
            [
                {"ts_code": "600002.SH", "name": "Alpha Invalid", "confirmed_at": "09:32"},
            ],
        )
        _write_lines(
            tmp_path / "events-2026-07-28.jsonl.bak",
            [
                {"ts_code": "600003.SH", "name": "Alpha Backup", "confirmed_at": "09:33"},
            ],
        )
        _write_lines(
            tmp_path / "events-20260729.jsonl",
            [
                {"ts_code": "600004.SH", "name": "Alpha Compact", "confirmed_at": "09:34"},
            ],
        )
        _write_lines(
            tmp_path / "events-2026-W31-3.jsonl",
            [
                {"ts_code": "600005.SH", "name": "Alpha Week", "confirmed_at": "09:35"},
            ],
        )
        (tmp_path / "events-2026-07-27.jsonl").write_text("{bad json}\n", encoding="utf-8")

        df = search_surge_history("  alpha  ", live_dir=tmp_path)
        assert list(df["ts_code"]) == ["600001.SH"]

    def test_empty_query_no_match_and_empty_directory_return_stable_empty_columns(
        self, tmp_path: Path
    ) -> None:
        empty_query = search_surge_history("   ", live_dir=tmp_path)
        empty_directory = search_surge_history("688255", live_dir=tmp_path)
        _write_lines(
            tmp_path / "events-2026-07-29.jsonl",
            [
                {"ts_code": "600001.SH", "name": "芯片先锋", "confirmed_at": "09:31"},
            ],
        )
        no_match = search_surge_history("不存在", live_dir=tmp_path)
        expected = [
            "trade_date",
            "confirmed_at",
            "ts_code",
            "name",
            "theme",
            "price",
            "pct_chg",
            "cum_amount",
            "rel_cum",
            "room_to_limit_pct",
            "status",
        ]
        assert empty_query.empty and list(empty_query.columns) == expected
        assert empty_directory.empty and list(empty_directory.columns) == expected
        assert no_match.empty and list(no_match.columns) == expected

    @pytest.mark.parametrize("yield_path", [False, True], ids=["before-first", "after-first"])
    def test_scan_oserror_returns_stable_empty_columns(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, yield_path: bool
    ) -> None:
        valid_path = tmp_path / "events-2026-07-29.jsonl"
        _write_lines(
            valid_path,
            [
                {"ts_code": "600001.SH", "name": "芯片先锋", "confirmed_at": "09:31"},
            ],
        )

        def flaky_glob(_: Path, pattern: str):
            assert pattern == "events-*.jsonl"

            def paths():
                if yield_path:
                    yield valid_path
                raise OSError("directory scan failed")

            return paths()

        monkeypatch.setattr(Path, "glob", flaky_glob)
        df = search_surge_history("芯片", live_dir=tmp_path)
        expected = [
            "trade_date",
            "confirmed_at",
            "ts_code",
            "name",
            "theme",
            "price",
            "pct_chg",
            "cum_amount",
            "rel_cum",
            "room_to_limit_pct",
            "status",
        ]
        assert df.empty and list(df.columns) == expected


def _trend(day: str, times: list[str], prices: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "dt": pd.to_datetime([f"{day} {t}" for t in times]),
            "price": prices,
            "avg_price": [float("nan")] * len(times),
            "volume": [100.0] * len(times),
        }
    )


class TestSurgeMarkPositions:
    def test_marks_every_same_day_event_with_neutral_labels(self, tmp_path: Path) -> None:
        _write_lines(
            tmp_path / f"events-{DAY.isoformat()}.jsonl",
            [
                {"ts_code": "688255.SH", "confirmed_at": "09:47", "rel_cum": 3.2},
                {"ts_code": "688255.SH", "confirmed_at": "10:15", "rel_cum": 4.5},
            ],
        )
        marks = load_surge_event_marks("688255.SH", DAY, live_dir=tmp_path)
        trend = _trend("2026-07-29", ["09:47", "10:15"], [10.5, 11.0])

        positions = surge_mark_positions(trend, marks)

        assert len(positions) == 2
        assert list(positions["label"]) == [
            "09:47 爆量确认 · 3.2×",
            "10:15 爆量确认 · 4.5×",
        ]

    def test_groups_same_minute_events_without_losing_ordered_rel_cum_values(self) -> None:
        trend = _trend("2026-07-29", ["09:47", "10:15"], [10.5, 11.0])
        marks = pd.DataFrame(
            [
                {"date": DAY, "confirmed_at": "09:47", "rel_cum": 3.2},
                {"date": DAY, "confirmed_at": "09:47", "rel_cum": 3.3},
                {"date": DAY, "confirmed_at": "10:15", "rel_cum": 4.5},
            ]
        )

        positions = surge_mark_positions(trend, marks)

        assert len(positions) == 2
        assert positions.iloc[0]["idx"] == 0
        assert positions.iloc[0]["trigger_count"] == 2
        assert positions.iloc[0]["rel_cum_values"] == "3.2× / 3.3×"
        assert positions.iloc[0]["label"] == "09:47 爆量确认 2次 · 3.2× / 3.3×"
        assert positions.iloc[1]["label"] == "10:15 爆量确认 · 4.5×"

    def test_exact_minute_hit(self) -> None:
        trend = _trend("2026-07-29", ["09:46", "09:47", "09:48"], [10.0, 10.5, 10.6])
        marks = pd.DataFrame([{"date": date(2026, 7, 29), "confirmed_at": "09:47", "rel_cum": 3.2}])
        pos = surge_mark_positions(trend, marks)
        assert len(pos) == 1
        assert pos.iloc[0]["idx"] == 1 and pos.iloc[0]["price"] == pytest.approx(10.5)
        assert pos.iloc[0]["label"] == "09:47 爆量确认 · 3.2×"

    def test_missing_minute_falls_back_to_prior_bar(self) -> None:
        trend = _trend("2026-07-29", ["09:46", "09:49"], [10.0, 10.6])
        marks = pd.DataFrame(
            [{"date": date(2026, 7, 29), "confirmed_at": "09:47", "rel_cum": float("nan")}]
        )
        pos = surge_mark_positions(trend, marks)
        assert pos.iloc[0]["idx"] == 0
        assert pos.iloc[0]["label"] == "09:47 爆量确认"  # rel_cum 缺失不带倍数

    def test_day_absent_skipped_and_empty_inputs(self) -> None:
        trend = _trend("2026-07-29", ["09:46"], [10.0])
        marks = pd.DataFrame([{"date": date(2026, 7, 28), "confirmed_at": "09:47", "rel_cum": 2.0}])
        assert surge_mark_positions(trend, marks).empty
        assert surge_mark_positions(trend, pd.DataFrame()).empty
        assert surge_mark_positions(pd.DataFrame(), marks).empty

    def test_multiple_marks_same_day(self) -> None:
        trend = _trend("2026-07-29", ["09:45", "09:47", "10:15"], [10.0, 10.5, 11.0])
        marks = pd.DataFrame(
            [
                {"date": date(2026, 7, 29), "confirmed_at": "09:47", "rel_cum": 3.2},
                {"date": date(2026, 7, 29), "confirmed_at": "10:15", "rel_cum": 4.5},
            ]
        )
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
