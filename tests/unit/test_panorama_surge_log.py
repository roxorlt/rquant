"""panorama_data.load_surge_log 单测（全离线，读 tmp events jsonl，不碰 settings/网络）。

覆盖：每标的取当日最早识别时刻（去重）、缺文件/空文件空态、坏行跳过、按时间升序。
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from rquant.panorama_data import load_surge_log
from rquant.surge_watch import SurgeConfirmed, append_events

DAY = date(2026, 7, 6)


def _events_path(live_dir: Path) -> Path:
    return live_dir / f"events-{DAY.isoformat()}.jsonl"


def test_dedup_keeps_earliest_confirmed_at(tmp_path: Path) -> None:
    live = tmp_path / "surge_live"
    # 同一 ts_code 两条不同时间（09:45 先 append、09:32 后 append），验证取最早 09:32
    confirmed = [
        SurgeConfirmed(
            ts_code="300001.SZ",
            name="特锐德",
            theme="充电桩",
            confirmed_at="09:45",
            pct_chg=6.1,
            cum_amount=3.2e8,
            rel_cum=3.4,
            room_to_limit_pct=4.2,
            status="confirmed",
        ),
        SurgeConfirmed(
            ts_code="300001.SZ",
            name="特锐德",
            theme="充电桩",
            confirmed_at="09:32",
            price=18.66,
            pct_chg=4.0,
            cum_amount=2.1e8,
            rel_cum=2.8,
            room_to_limit_pct=6.0,
            status="confirmed",
        ),
        SurgeConfirmed(
            ts_code="300750.SZ",
            name="宁德时代",
            theme="锂电池",
            confirmed_at="10:15",
            pct_chg=9.9,
            cum_amount=8.0e8,
            rel_cum=5.0,
            room_to_limit_pct=0.3,
            status="unbuyable",
        ),
    ]
    append_events(_events_path(live), confirmed)

    df = load_surge_log(DAY, live_dir=live)

    # 去重后每标的一行，按 confirmed_at 升序
    assert list(df["ts_code"]) == ["300001.SZ", "300750.SZ"]
    row = df[df["ts_code"] == "300001.SZ"].iloc[0]
    assert row["confirmed_at"] == "09:32"  # 取最早那行
    assert row["rel_cum"] == 2.8
    assert row["price"] == 18.66  # 推送价（入场价）随台账记录
    # unbuyable 行保留、字段完整
    unb = df[df["ts_code"] == "300750.SZ"].iloc[0]
    assert unb["status"] == "unbuyable"
    assert unb["room_to_limit_pct"] == 0.3


def test_missing_file_returns_empty(tmp_path: Path) -> None:
    df = load_surge_log(DAY, live_dir=tmp_path / "surge_live")
    assert df.empty
    assert "ts_code" in df.columns  # 空态仍带标准列


def test_empty_file_returns_empty(tmp_path: Path) -> None:
    live = tmp_path / "surge_live"
    live.mkdir(parents=True)
    _events_path(live).write_text("", encoding="utf-8")
    assert load_surge_log(DAY, live_dir=live).empty


def test_skips_bad_lines(tmp_path: Path) -> None:
    live = tmp_path / "surge_live"
    live.mkdir(parents=True)
    good = SurgeConfirmed(ts_code="300002.SZ", name="蓝色光标", confirmed_at="09:40", rel_cum=3.0)
    with _events_path(live).open("w", encoding="utf-8") as f:
        f.write("{bad json line\n")  # 非法 JSON
        f.write("42\n")  # 合法 JSON 但非 dict
        f.write(json.dumps({"name": "无代码"}, ensure_ascii=False) + "\n")  # dict 但缺 ts_code
        f.write(json.dumps(good.model_dump(), ensure_ascii=False) + "\n")
        f.write("\n")  # 空行

    df = load_surge_log(DAY, live_dir=live)
    assert len(df) == 1
    assert df.iloc[0]["ts_code"] == "300002.SZ"


def test_sorted_ascending_by_time(tmp_path: Path) -> None:
    live = tmp_path / "surge_live"
    confirmed = [
        SurgeConfirmed(ts_code="300003.SZ", name="a", confirmed_at="10:30", rel_cum=3.0),
        SurgeConfirmed(ts_code="300004.SZ", name="b", confirmed_at="09:35", rel_cum=3.0),
        SurgeConfirmed(ts_code="300005.SZ", name="c", confirmed_at="09:50", rel_cum=3.0),
    ]
    append_events(_events_path(live), confirmed)

    df = load_surge_log(DAY, live_dir=live)
    assert list(df["confirmed_at"]) == ["09:35", "09:50", "10:30"]
