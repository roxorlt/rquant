"""盘中脉搏 + 午间战报单测（U1-U9，全离线，tmp_path 落盘）。"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from rquant import midday_briefing as mb
from rquant.midday_briefing import (
    CandidateStock,
    DigestView,
    LadderStock,
    PositionCheck,
    PulseView,
    ThemeHeat,
    build_candidate_pool,
    check_positions,
    compute_board_ladder,
    compute_pulse_view,
    compute_theme_ladder_summaries,
    render_digest,
    render_pulse,
    resolve_slot,
    run_morning_pulse,
)

_SNAP_COLS = ["ts_code", "name", "price", "high", "pre_close", "pct_chg",
              "volume", "amount", "limit_up_price", "limit_down_price"]


def mk_snapshot(rows: list[dict]) -> pd.DataFrame:
    """构造带涨跌停价的快照（直控列，绕过 add_limit_prices 便于精确断言）。"""
    filled = []
    for r in rows:
        row = {c: r.get(c) for c in _SNAP_COLS}
        if row.get("name") is None:
            row["name"] = row["ts_code"]
        if row.get("high") is None:
            row["high"] = row["price"]
        if row.get("limit_down_price") is None and row.get("pre_close") is not None:
            row["limit_down_price"] = round(row["pre_close"] * 0.9, 2)
        filled.append(row)
    return pd.DataFrame(filled, columns=_SNAP_COLS)


# ── U1 槽位归属 ─────────────────────────────────────────────────────────────────


class TestU1SlotResolution:
    def test_on_time_and_slight_late(self) -> None:
        assert resolve_slot(datetime(2026, 7, 6, 10, 3))[0] == "1000"
        assert resolve_slot(datetime(2026, 7, 6, 10, 36))[0] == "1030"

    def test_late_over_10min_skips(self) -> None:
        tag, reason = resolve_slot(datetime(2026, 7, 6, 10, 44))
        assert tag is None
        assert reason

    def test_no_pulse_window(self) -> None:
        tag, _ = resolve_slot(datetime(2026, 7, 6, 11, 58))
        assert tag is None

    def test_early_tolerance(self) -> None:
        # 09:57（早到 3min）应归 10:00 槽
        assert resolve_slot(datetime(2026, 7, 6, 9, 57))[0] == "1000"

    def test_explicit_slot(self) -> None:
        assert resolve_slot(datetime(2026, 7, 6, 15, 0), explicit="11:00")[0] == "1100"

    def test_explicit_invalid_raises(self) -> None:
        with pytest.raises(ValueError):
            resolve_slot(datetime(2026, 7, 6, 10, 0), explicit="09:45")


# ── U2 落盘幂等 + 去重 ──────────────────────────────────────────────────────────


class TestU2Idempotency:
    def _run(self, tmp_path, **kwargs):
        now = datetime(2026, 7, 6, 10, 0, tzinfo=mb.CST)
        return run_morning_pulse(slot="10:00", now=now, base_dir=tmp_path, **kwargs)

    def test_pushed_flag_dedup_and_force(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setenv("RQUANT_PANORAMA_FAKE", "1")
        spy = []
        with patch.object(mb, "is_trading_day", return_value=True), \
             patch.object(mb, "notify", side_effect=lambda *a, **k: spy.append((a, k))):
            self._run(tmp_path)                    # 首推
            assert len(spy) == 1
            self._run(tmp_path)                    # 已推送 → 跳过
            assert len(spy) == 1
            self._run(tmp_path, force=True)        # --force 绕过
            assert len(spy) == 2

        mdir = tmp_path / "midday" / "2026-07-06"
        # 覆盖重跑不产生副本：snapshot_1000 仍只有一份
        assert len(list(mdir.glob("snapshot_1000*"))) == 1
        meta = mb._read_meta(mdir)
        assert meta["1000"].pushed is True

    def test_dry_run_does_not_mark_pushed(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setenv("RQUANT_PANORAMA_FAKE", "1")
        spy = []
        with patch.object(mb, "is_trading_day", return_value=True), \
             patch.object(mb, "notify", side_effect=lambda *a, **k: spy.append(1)):
            self._run(tmp_path, dry_run=True)
        assert spy == []
        meta = mb._read_meta(tmp_path / "midday" / "2026-07-06")
        assert meta["1000"].pushed is False  # dry-run 只落 parquet，不标记


# ── U3 增量计算 ─────────────────────────────────────────────────────────────────


class TestU3Delta:
    def _theme_map(self) -> dict[str, str]:
        return {"600001.SH": "题材A", "600003.SH": "题材C"}

    def test_new_limit_ups_and_delta(self) -> None:
        prev = mk_snapshot([
            {"ts_code": "600001.SH", "price": 11.0, "pre_close": 10.0, "pct_chg": 10,
             "volume": 1e6, "amount": 1e7, "limit_up_price": 11.0},
            {"ts_code": "600002.SH", "price": 9.0, "pre_close": 10.0, "pct_chg": -10,
             "volume": 1e6, "amount": 1e7, "limit_up_price": 11.0},
        ])
        curr = mk_snapshot([
            {"ts_code": "600001.SH", "price": 11.0, "pre_close": 10.0, "pct_chg": 10,
             "volume": 1e6, "amount": 1e7, "limit_up_price": 11.0},
            {"ts_code": "600003.SH", "price": 11.0, "pre_close": 10.0, "pct_chg": 10,
             "volume": 1e6, "amount": 1e7, "limit_up_price": 11.0},
            {"ts_code": "600002.SH", "price": 9.0, "pre_close": 10.0, "pct_chg": -10,
             "volume": 1e6, "amount": 1e7, "limit_up_price": 11.0},
        ])
        view = compute_pulse_view("10:30", curr, pd.DataFrame(), prev, pd.DataFrame(),
                                  self._theme_map(), pd.DataFrame())
        assert view.has_prev is True
        assert view.limit_up_delta == 1  # 2 - 1
        new_codes = {n.ts_code for n in view.new_limit_ups}
        assert new_codes == {"600003.SH"}
        assert view.new_limit_ups[0].theme == "题材C"

    def test_first_slot_no_prev(self) -> None:
        curr = mk_snapshot([
            {"ts_code": "600001.SH", "price": 11.0, "pre_close": 10.0, "pct_chg": 10,
             "volume": 1e6, "amount": 1e7, "limit_up_price": 11.0},
        ])
        view = compute_pulse_view("10:00", curr, pd.DataFrame(), None, None, {}, pd.DataFrame())
        assert view.has_prev is False
        assert view.limit_up_delta is None
        assert view.new_limit_ups == []
        assert view.limit_up_count == 1  # 绝对值仍在


# ── U4 连板现算 ─────────────────────────────────────────────────────────────────


class TestU4BoardLadder:
    def test_four_paths(self) -> None:
        snapshot = mk_snapshot([
            {"ts_code": "600001.SH", "price": 11.0, "pre_close": 10.0, "pct_chg": 10,
             "volume": 1e6, "amount": 1e7, "limit_up_price": 11.0},   # 昨2板 → 今涨停
            {"ts_code": "600002.SH", "price": 11.0, "pre_close": 10.0, "pct_chg": 10,
             "volume": 1e6, "amount": 1e7, "limit_up_price": 11.0},   # 昨1板 → 今涨停
            {"ts_code": "600003.SH", "price": 11.0, "pre_close": 10.0, "pct_chg": 10,
             "volume": 1e6, "amount": 1e7, "limit_up_price": 11.0},   # 昨无 → 今涨停
            {"ts_code": "600004.SH", "price": 10.5, "pre_close": 10.0, "pct_chg": 5,
             "volume": 1e6, "amount": 1e7, "limit_up_price": 11.0},   # 今未涨停
        ])
        prev_limit = pd.DataFrame([
            {"ts_code": "600001.SH", "name": "样本01", "limit_times": 2},
            {"ts_code": "600002.SH", "name": "样本02", "limit_times": 1},
        ])
        ladder = {s.ts_code: s.boards for s in compute_board_ladder(snapshot, prev_limit, {})}
        assert ladder == {"600001.SH": 3, "600002.SH": 2, "600003.SH": 1}
        assert "600004.SH" not in ladder  # 未涨停不入梯队


class TestThemeLadderSummaries:
    def test_theme_ladder_missing_snapshot_amount_fails_soft(self) -> None:
        snapshot = mk_snapshot([
            {"ts_code": "600001.SH", "name": "样本", "price": 11.0, "pre_close": 10.0,
             "pct_chg": 10, "volume": 1e6, "amount": 50.0, "limit_up_price": 11.0},
        ]).drop(columns="amount")
        kpl_members = pd.DataFrame([{"board_name": "题材A", "con_code": "600001.SH"}])

        assert compute_theme_ladder_summaries(snapshot, pd.DataFrame(), kpl_members, {}) == []

    def test_theme_ladder_multi_membership_keeps_continuous_rungs(self) -> None:
        snapshot = mk_snapshot([
            {"ts_code": "600001.SH", "name": "共用龙头", "price": 11.0, "pre_close": 10.0,
             "pct_chg": 10, "volume": 1e6, "amount": 30.0, "limit_up_price": 11.0},
            {"ts_code": "600002.SH", "name": "题材A首板", "price": 11.0, "pre_close": 10.0,
             "pct_chg": 10, "volume": 1e6, "amount": 20.0, "limit_up_price": 11.0},
            {"ts_code": "600003.SH", "name": "题材B二板", "price": 11.0, "pre_close": 10.0,
             "pct_chg": 10, "volume": 1e6, "amount": 20.0, "limit_up_price": 11.0},
        ])
        prev_limit = pd.DataFrame([
            {"ts_code": "600001.SH", "limit_times": 2},
            {"ts_code": "600003.SH", "limit_times": 1},
        ])
        kpl_members = pd.DataFrame([
            {"board_name": "题材B", "con_code": "600001.SH"},
            {"board_name": "题材A", "con_code": "600001.SH"},
            {"board_name": "题材A", "con_code": "600001.SH"},  # 重复成员不可重复计数
            {"board_name": "题材A", "con_code": "600002.SH"},
            {"board_name": "题材B", "con_code": "600003.SH"},
        ])

        summaries = compute_theme_ladder_summaries(
            snapshot, prev_limit, kpl_members, {}, top_n=5
        )

        assert [summary.theme for summary in summaries] == ["题材A", "题材B"]
        assert [(r.boards, [s.name for s in r.stocks]) for r in summaries[0].rungs] == [
            (3, ["共用龙头"]),
            (2, []),
            (1, ["题材A首板"]),
        ]
        assert [(r.boards, [s.name for s in r.stocks]) for r in summaries[1].rungs] == [
            (3, ["共用龙头"]),
            (2, ["题材B二板"]),
            (1, []),
        ]

    def test_theme_ladder_stable_sorts_equal_counts_and_amounts_by_name(self) -> None:
        snapshot = mk_snapshot([
            {"ts_code": "600001.SH", "name": "甲", "price": 11.0, "pre_close": 10.0,
             "pct_chg": 10, "volume": 1e6, "amount": 50.0, "limit_up_price": 11.0},
            {"ts_code": "600002.SH", "name": "乙", "price": 11.0, "pre_close": 10.0,
             "pct_chg": 10, "volume": 1e6, "amount": 50.0, "limit_up_price": 11.0},
        ])
        kpl_members = pd.DataFrame([
            {"board_name": "题材B", "con_code": "600001.SH"},
            {"board_name": "题材A", "con_code": "600002.SH"},
        ])

        summaries = compute_theme_ladder_summaries(
            snapshot, pd.DataFrame(), kpl_members, {}, top_n=5
        )

        assert [summary.theme for summary in summaries] == ["题材A", "题材B"]
        assert [summary.limit_up_count for summary in summaries] == [1, 1]
        assert [summary.amount for summary in summaries] == [50.0, 50.0]

    def test_theme_ladder_invalid_previous_board_counts_fall_back_to_first_board(self) -> None:
        codes = [f"60000{i}.SH" for i in range(1, 9)]
        snapshot = mk_snapshot([
            {"ts_code": code, "name": code, "price": 11.0, "pre_close": 10.0,
             "pct_chg": 10, "volume": 1e6, "amount": 10.0, "limit_up_price": 11.0}
            for code in codes
        ])
        prev_limit = pd.DataFrame([
            {"ts_code": codes[0], "limit_times": -1},
            {"ts_code": codes[1], "limit_times": float("inf")},
            {"ts_code": codes[2], "limit_times": "2.0"},
            {"ts_code": codes[3], "limit_times": 2.0},
            {"ts_code": codes[4], "limit_times": 99},
            {"ts_code": codes[5], "limit_times": float("nan")},
            {"ts_code": codes[6], "limit_times": "2.5"},
            {"ts_code": codes[7], "limit_times": "非整数"},
        ])
        kpl_members = pd.DataFrame([
            {"board_name": "题材A", "con_code": code} for code in codes
        ])

        summaries = compute_theme_ladder_summaries(snapshot, prev_limit, kpl_members, {})

        stocks = {
            stock.ts_code: stock.boards
            for rung in summaries[0].rungs
            for stock in rung.stocks
        }
        assert stocks[codes[2]] == 3
        assert stocks[codes[3]] == 3
        invalid_codes = (codes[0], codes[1], codes[4], codes[5], codes[6], codes[7])
        assert {stocks[code] for code in invalid_codes} == {1}
        assert [rung.boards for rung in summaries[0].rungs] == [3, 2, 1]


class TestPulseThemeLeaders:
    @staticmethod
    def _snapshot_for_theme_counts(
        counts: dict[str, int], *, start_code: int = 600001
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        rows: list[dict[str, object]] = []
        members: list[dict[str, str]] = []
        for theme, count in counts.items():
            for index in range(count):
                code = f"{len(rows) + start_code:06d}.SH"
                rows.append({
                    "ts_code": code,
                    "name": f"{theme}{index + 1}",
                    "price": 11.0,
                    "pre_close": 10.0,
                    "pct_chg": 10,
                    "volume": 1e6,
                    "amount": 10.0,
                    "limit_up_price": 11.0,
                })
                members.append({"board_name": theme, "con_code": code})
        return mk_snapshot(rows), pd.DataFrame(members)

    def test_pulse_theme_rank_change_uses_same_ladder_compute(self) -> None:
        previous, previous_members = self._snapshot_for_theme_counts(
            {"题材B": 5, "题材C": 4, "题材A": 3, "题材D": 2, "题材E": 1}
        )
        current, current_members = self._snapshot_for_theme_counts(
            {"题材A": 6, "题材C": 5, "题材B": 4, "题材F": 3, "题材G": 2},
            start_code=600101,
        )
        members = pd.concat([previous_members, current_members]).drop_duplicates()

        view = compute_pulse_view(
            "10:30",
            current,
            pd.DataFrame(),
            previous,
            pd.DataFrame(),
            {},
            pd.DataFrame(),
            kpl_members=members,
            prev_limit=pd.DataFrame(),
        )

        assert [summary.theme for summary in view.theme_ladders] == [
            "题材A", "题材C", "题材B", "题材F", "题材G"
        ]
        assert [summary.rank_change for summary in view.theme_ladders] == [2, 0, -2, None, None]
        assert [summary.rank_state for summary in view.theme_ladders] == [
            "changed", "changed", "changed", "new", "new"
        ]

    def test_pulse_first_slot_hides_theme_rank_change(self) -> None:
        snapshot, members = self._snapshot_for_theme_counts({"题材A": 1})

        view = compute_pulse_view(
            "10:00",
            snapshot,
            pd.DataFrame(),
            None,
            None,
            {},
            pd.DataFrame(),
            kpl_members=members,
            prev_limit=pd.DataFrame(),
        )

        assert view.theme_ladders[0].rank_change is None
        assert view.theme_ladders[0].rank_state == "hidden"
        _, body = render_pulse(view)
        theme_section = body.split("## 题材", maxsplit=1)[1]
        assert "↑" not in theme_section
        assert "↓" not in theme_section
        assert "持平" not in theme_section
        assert "新晋" not in theme_section

    def test_pulse_theme_highest_board_mobile_limit(self) -> None:
        leaders = [
            LadderStock(ts_code=f"60000{index}.SH", name=name, boards=3, theme="题材A")
            for index, name in enumerate(("甲", "乙", "丙"), 1)
        ]
        first_board = [
            LadderStock(ts_code=f"6001{index:02d}.SH", name=f"首板{index}", boards=1, theme="题材A")
            for index in range(1, 8)
        ]
        view = PulseView(
            slot_hhmm="10:30",
            has_prev=True,
            limit_up_count=10,
            broken_count=0,
            limit_down_count=0,
            up_count=3,
            down_count=0,
            theme_ladders=[
                mb.ThemeLadderSummary(
                    theme="题材A",
                    limit_up_count=10,
                    amount=100.0,
                    rungs=[
                        mb.ThemeLadderRung(boards=3, stocks=leaders),
                        mb.ThemeLadderRung(boards=2, stocks=[]),
                        mb.ThemeLadderRung(boards=1, stocks=first_board),
                    ],
                    rank_change=2,
                    rank_state="changed",
                )
            ],
        )

        _, body = render_pulse(view)

        assert "## 题材梯队 Top5" in body
        assert "1. 题材A ↑2" in body
        assert "   涨停 10 ｜ 最高 3板：甲、乙等3只" in body
        assert "   梯队：3板 3 ｜ 2板 0 ｜ 首板 7" in body
        assert "丙" not in body

    def test_pulse_theme_rank_labels_render_all_visible_states(self) -> None:
        def summary(
            theme: str, rank_change: int | None, rank_state: str
        ) -> mb.ThemeLadderSummary:
            return mb.ThemeLadderSummary(
                theme=theme,
                limit_up_count=1,
                amount=10.0,
                rungs=[
                    mb.ThemeLadderRung(
                        boards=1,
                        stocks=[LadderStock(ts_code="600001.SH", name="甲", boards=1)],
                    )
                ],
                rank_change=rank_change,
                rank_state=rank_state,
            )

        view = PulseView(
            slot_hhmm="10:30",
            has_prev=True,
            limit_up_count=4,
            broken_count=0,
            limit_down_count=0,
            up_count=4,
            down_count=0,
            theme_ladders=[
                summary("上升", 2, "changed"),
                summary("下降", -2, "changed"),
                summary("持平", 0, "changed"),
                summary("新晋", None, "new"),
            ],
        )

        _, body = render_pulse(view)

        assert "1. 上升 ↑2" in body
        assert "2. 下降 ↓2" in body
        assert "3. 持平 持平" in body
        assert "4. 新晋 新晋" in body


# ── U5 候选池 ───────────────────────────────────────────────────────────────────


class TestU5CandidatePool:
    def test_unit_conversion_1000x_trap(self) -> None:
        # amount=1e8 元、avg_amount_20d=5e5 千元(=5e8 元) → 正确量比 0.2 < 0.8 应排除；
        # 若漏掉 ×1000（拿 5e5 当元）量比会变 200 被错误纳入 → 借此验证换算生效
        snap = mk_snapshot([
            {"ts_code": "300001.SZ", "price": 11.0, "pre_close": 10.0, "pct_chg": 10,
             "volume": 1e7, "amount": 1e8, "limit_up_price": 12.0},
        ])
        avg20 = pd.DataFrame([{"ts_code": "300001.SZ", "avg_amount_20d": 5e5}])
        assert build_candidate_pool(snap, avg20, {}) == []

    def test_threshold_boundary_and_exclusions(self) -> None:
        snap = mk_snapshot([
            # A: 量比恰好 0.8（1e8 / (1.25e5*1000)=0.8）、未涨停、price>=vwap → 纳入
            {"ts_code": "300001.SZ", "name": "创A", "price": 10.0, "pre_close": 9.0,
             "pct_chg": 11.1, "volume": 1e7, "amount": 1e8, "limit_up_price": 10.8},
            # B: 涨停（price==limit_up）→ 排除
            {"ts_code": "300002.SZ", "name": "创B", "price": 12.0, "pre_close": 10.0,
             "pct_chg": 20, "volume": 1e7, "amount": 2e8, "limit_up_price": 12.0},
            # C: VWAP 下方（price 9 < amount/volume=10）→ 排除
            {"ts_code": "688003.SH", "name": "科C", "price": 9.0, "pre_close": 9.0,
             "pct_chg": 0.5, "volume": 1e7, "amount": 1e8, "limit_up_price": 10.8},
        ])
        avg20 = pd.DataFrame([
            {"ts_code": "300001.SZ", "avg_amount_20d": 1.25e5},
            {"ts_code": "300002.SZ", "avg_amount_20d": 1.0e5},
            {"ts_code": "688003.SH", "avg_amount_20d": 1.25e5},
        ])
        pool = build_candidate_pool(snap, avg20, {"300001.SZ": "题材A"})
        assert [c.ts_code for c in pool] == ["300001.SZ"]
        c = pool[0]
        assert c.vol_ratio == pytest.approx(0.8, abs=1e-6)
        assert c.theme == "题材A"
        # 距涨停空间 = (10.8/10 - 1)*100 = 8%
        assert c.room_to_limit_pct == pytest.approx(8.0, abs=1e-6)

    def test_top20_truncation(self) -> None:
        rows, avg = [], []
        for i in range(25):
            code = f"3000{i:02d}.SZ"
            rows.append({"ts_code": code, "name": code, "price": 10.5, "pre_close": 10.0,
                         "pct_chg": 5, "volume": 1e7, "amount": 1.05e8, "limit_up_price": 12.0})
            avg.append({"ts_code": code, "avg_amount_20d": 1.0e5})  # ratio=2.0
        pool = build_candidate_pool(mk_snapshot(rows), pd.DataFrame(avg), {})
        assert len(pool) == 20

    def test_main_board_excluded(self) -> None:
        snap = mk_snapshot([
            {"ts_code": "600001.SH", "price": 10.5, "pre_close": 10.0, "pct_chg": 5,
             "volume": 1e7, "amount": 2e8, "limit_up_price": 11.0},
        ])
        avg20 = pd.DataFrame([{"ts_code": "600001.SH", "avg_amount_20d": 1.0e5}])
        assert build_candidate_pool(snap, avg20, {}) == []  # 主板不进候选池


# ── U6 持仓体检 ─────────────────────────────────────────────────────────────────


class TestU6PositionCheck:
    def test_pnl_and_stop_distance(self) -> None:
        positions = pd.DataFrame([
            {"ts_code": "600001.SH", "name": "样本01", "entry_price": 10.0,
             "stop_loss_price": 9.0, "run_mode": "live"},
        ])
        snap = mk_snapshot([
            {"ts_code": "600001.SH", "price": 11.0, "pre_close": 10.0, "pct_chg": 10,
             "volume": 1e6, "amount": 1e7, "limit_up_price": 11.0},
        ])
        checks = check_positions(positions, snap, {}, pd.DataFrame())
        assert len(checks) == 1
        assert checks[0].pnl_pct == pytest.approx(10.0)          # 11/10-1
        assert checks[0].dist_stop_pct == pytest.approx(22.22, abs=0.01)  # 11/9-1

    def test_empty_positions_section_omitted(self) -> None:
        assert check_positions(pd.DataFrame(), mk_snapshot([]), {}, pd.DataFrame()) == []
        view = DigestView(day=datetime(2026, 7, 6).date(), limit_up_count=1, broken_count=0,
                          limit_down_count=0, up_count=5, down_count=3, positions=[])
        _, body = render_digest(view)
        assert "持仓午间体检" not in body  # 空仓整节省略


# ── U7 报文渲染 ─────────────────────────────────────────────────────────────────


class TestU7Rendering:
    def test_pulse_groups_sections_for_mobile(self) -> None:
        view = PulseView(
            slot_hhmm="10:30",
            has_prev=True,
            limit_up_count=72,
            broken_count=19,
            limit_down_count=2,
            up_count=5022,
            down_count=434,
            limit_up_delta=14,
            broken_delta=6,
            new_limit_ups=[
                mb.NewLimitUp(
                    ts_code="600683.SH",
                    name="京投发展",
                    theme="银发经济",
                )
            ],
            theme_ladders=[
                mb.ThemeLadderSummary(
                    theme="AI硬件",
                    limit_up_count=10,
                    amount=100.0,
                    rungs=[
                        mb.ThemeLadderRung(
                            boards=1,
                            stocks=[LadderStock(ts_code="600001.SH", name="甲", boards=1)],
                        )
                    ],
                )
            ],
            new_anomalies=[
                mb.VolAnomaly(
                    ts_code="300039.SZ",
                    name="上海凯宝",
                    vol_ratio=1.26,
                )
            ],
        )

        _, body = render_pulse(view)

        expected = (
            "## 市场温度\n"
            "- 涨停：72（+14） ｜ 炸板：19（+6） ｜ 跌停：2\n"
            "- 上涨：5022 ｜ 下跌：434\n\n"
            "## 新晋涨停\n"
            "- 京投发展 ｜ 银发经济\n\n"
            "## 题材梯队 Top5\n"
            "1. AI硬件\n"
            "   涨停 10 ｜ 最高 1板：甲\n"
            "   梯队：首板 1\n\n"
            "## 放量异动新增\n"
            "- 上海凯宝：量比 1.26"
        )
        assert expected in body
        assert "新晋涨停：京投发展(银发经济)" not in body

    def test_pulse_lines(self) -> None:
        view = PulseView(
            slot_hhmm="10:30", has_prev=True, limit_up_count=47, broken_count=6,
            limit_down_count=2, up_count=2871, down_count=2130,
            limit_up_delta=9, broken_delta=2,
            theme_ladders=[
                mb.ThemeLadderSummary(
                    theme="人形机器人",
                    limit_up_count=5,
                    amount=50.0,
                    rungs=[
                        mb.ThemeLadderRung(
                            boards=1,
                            stocks=[LadderStock(ts_code="600001.SH", name="甲", boards=1)],
                        )
                    ],
                )
            ],
        )
        title, body = render_pulse(view)
        assert "涨停47(+9)" in title
        assert "- 上涨：2871 ｜ 下跌：2130" in body
        assert "1. 人形机器人" in body
        assert "   涨停 5 ｜ 最高 1板：甲" in body

    def test_pulse_legacy_theme_heat_is_not_rendered(self) -> None:
        view = PulseView(
            slot_hhmm="10:30",
            has_prev=True,
            limit_up_count=5,
            broken_count=0,
            limit_down_count=0,
            up_count=5,
            down_count=0,
            theme_heat=[ThemeHeat(theme="旧题材", limit_up_count=5, delta=2)],
        )

        _, body = render_pulse(view)

        assert "题材热度" not in body
        assert "旧题材" not in body

    def test_digest_groups_theme_ladders_and_candidates_for_mobile(self) -> None:
        view = DigestView(
            day=datetime(2026, 7, 6).date(), limit_up_count=47, broken_count=6,
            limit_down_count=2, up_count=2871, down_count=2130, broken_ratio_pct=11.3,
            slot_limit_up_series=[("10:00", 20), ("10:30", 30), ("11:00", 40), ("11:30", 47)],
            prev_day_limit_up=52,
            theme_ladders=[
                mb.ThemeLadderSummary(
                    theme="人形机器人",
                    limit_up_count=3,
                    amount=234_000_000.0,
                    slot_series=[1, 1, 2, 3],
                    rungs=[
                        mb.ThemeLadderRung(
                            boards=3,
                            stocks=[
                                LadderStock(
                                    ts_code="600001.SH",
                                    name="样本01",
                                    boards=3,
                                    theme="人形机器人",
                                )
                            ],
                        ),
                        mb.ThemeLadderRung(boards=2, stocks=[]),
                        mb.ThemeLadderRung(
                            boards=1,
                            stocks=[
                                LadderStock(
                                    ts_code="600002.SH",
                                    name="样本02",
                                    boards=1,
                                    theme="人形机器人",
                                )
                            ],
                        ),
                    ],
                )
            ],
            candidates=[CandidateStock(ts_code="300001.SZ", name="创A", theme="存储",
                                       vol_ratio=2.1, pct_chg=8.0, room_to_limit_pct=5.0)],
            positions=[PositionCheck(ts_code="600001.SH", name="样本01", pnl_pct=7.3,
                                     dist_stop_pct=12.8, board_note="人形 +2%")],
        )
        _, body = render_digest(view)
        headers = ("① 情绪温度", "② 最强题材·连板梯队 Top5",
                   "③ 下午候选观察池", "④ 持仓风险")
        for header in headers:
            assert header in body
        assert "炸板率 11.3%" in body
        assert "昨日终值：涨停 52 家" in body
        theme_block = (
            "1. 人形机器人 ｜ 涨停 3 ｜ 半日额 2.3亿  \n"
            "   上午：1 / 1 / 2 / 3  \n"
            "   - 3板（1）：样本01  \n"
            "   - 2板（0）：暂无  \n"
            "   - 首板（1）：样本02"
        )
        assert theme_block in body
        assert "## ② 连板梯队" not in body
        assert "## ③ 最强题材" not in body
        assert "| 300001.SZ | 创A |" not in body
        candidate_block = (
            "- 创A（300001.SZ）  \n"
            "  题材：存储 ｜ 半日量比：2.10  \n"
            "  涨幅：+8.0% ｜ 距涨停：5.0%"
        )
        assert candidate_block in body
        assert "## ⑤ 持仓午间体检" not in body

    def test_digest_empty_theme_and_candidate_sections_are_explicit(self) -> None:
        view = DigestView(
            day=datetime(2026, 7, 6).date(),
            limit_up_count=0,
            broken_count=0,
            limit_down_count=0,
            up_count=1,
            down_count=1,
        )

        _, body = render_digest(view)

        assert "## ② 最强题材·连板梯队 Top5\n- 暂无" in body
        assert "## ③ 下午候选观察池（创业/科创 半日量能预筛）\n- 暂无" in body


# ── U8 守卫 ─────────────────────────────────────────────────────────────────────


class TestU8Guards:
    def test_non_trading_day_exits(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setenv("RQUANT_PANORAMA_FAKE", "1")
        called = []
        with patch.object(mb, "is_trading_day", return_value=False), \
             patch.object(mb, "fetch_slot_frames",
                          side_effect=lambda *a, **k: called.append(1) or (None, None, None)), \
             patch.object(mb, "notify", side_effect=lambda *a, **k: called.append(1)):
            rc = run_morning_pulse(slot="10:00", now=datetime(2026, 7, 6, 10, 0, tzinfo=mb.CST),
                                   base_dir=tmp_path)
        assert rc == 0
        assert called == []                       # 不拉快照不推送
        assert not (tmp_path / "midday").exists()  # 不落盘

    def test_snapshot_failure_degrades(self, tmp_path, monkeypatch) -> None:
        empty = pd.DataFrame()
        empty.attrs["route"] = "none"
        monkeypatch.setattr(mb, "_sleep", lambda s: None)  # 自拉重试间隔不真等 60s
        spy = []
        with patch.object(mb, "is_trading_day", return_value=True), \
             patch.object(mb, "fetch_market_snapshot", return_value=empty), \
             patch.object(mb, "notify", side_effect=lambda scene, **k: spy.append((scene, k))):
            rc = run_morning_pulse(slot="10:00", now=datetime(2026, 7, 6, 10, 0, tzinfo=mb.CST),
                                   base_dir=tmp_path)
        assert rc == 0
        assert len(spy) == 1
        assert "快照不可用" in spy[0][1]["title"]
        mdir = tmp_path / "midday" / "2026-07-06"
        assert list(mdir.glob("snapshot_*")) == []  # 降级不落 snapshot parquet


# ── U9 notify ──────────────────────────────────────────────────────────────────


class TestU9Notify:
    def test_dry_run_no_push(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setenv("RQUANT_PANORAMA_FAKE", "1")
        spy = []
        with patch.object(mb, "is_trading_day", return_value=True), \
             patch.object(mb, "notify", side_effect=lambda *a, **k: spy.append(1)):
            run_morning_pulse(slot="10:00", now=datetime(2026, 7, 6, 10, 0, tzinfo=mb.CST),
                              base_dir=tmp_path, dry_run=True)
        assert spy == []

    def test_scene_and_kwargs(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setenv("RQUANT_PANORAMA_FAKE", "1")
        captured = {}
        with patch.object(mb, "is_trading_day", return_value=True), \
             patch.object(mb, "notify",
                          side_effect=lambda scene, **k: captured.update(scene=scene, **k)):
            run_morning_pulse(slot="10:00", now=datetime(2026, 7, 6, 10, 0, tzinfo=mb.CST),
                              base_dir=tmp_path)
        assert captured["scene"] == "morning_pulse"
        assert captured["title"].startswith("脉搏 10:00")
        assert "涨停" in captured["body"]


# ── 共享 drop（全机单一取数者：poller 落盘，midday 优先读） ─────────────────────


def _write_drop_fixture(
    base: Path, as_of_iso: str, snapshot: pd.DataFrame, route: str = "em_direct"
) -> None:
    """手工构造 data/panorama_live/ drop（snapshot + live_meta.json）。"""
    drop = base / "panorama_live"
    drop.mkdir(parents=True, exist_ok=True)
    snapshot.to_parquet(drop / "snapshot.parquet", index=False)
    meta = {
        "snapshot": {"as_of_iso": as_of_iso, "route": route, "written_at": as_of_iso},
    }
    (drop / "live_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False), encoding="utf-8"
    )


def _drop_snapshot() -> pd.DataFrame:
    return mk_snapshot([
        {"ts_code": "600001.SH", "name": "样本01", "price": 11.0, "pre_close": 10.0,
         "pct_chg": 10, "volume": 1e6, "amount": 1e7, "limit_up_price": 11.0},
        {"ts_code": "600002.SH", "name": "样本02", "price": 9.5, "pre_close": 10.0,
         "pct_chg": -5, "volume": 1e6, "amount": 1e7, "limit_up_price": 11.0},
    ])


class TestSharedDrop:
    NOW = datetime(2026, 7, 6, 10, 0, tzinfo=mb.CST)

    def test_fresh_drop_skips_self_fetch(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setenv("RQUANT_PANORAMA_FAKE", "1")  # kpl/avg20 等本地读仍走 fake
        _write_drop_fixture(
            tmp_path, self.NOW.isoformat(timespec="seconds"), _drop_snapshot()
        )
        fetch_spy: list[int] = []
        captured: dict = {}
        with patch.object(mb, "is_trading_day", return_value=True), \
             patch.object(mb, "fetch_market_snapshot",
                          side_effect=lambda *a, **k: fetch_spy.append(1) or pd.DataFrame()), \
             patch.object(mb, "notify",
                          side_effect=lambda scene, **k: captured.update(scene=scene, **k)):
            rc = run_morning_pulse(slot="10:00", now=self.NOW, base_dir=tmp_path)
        assert rc == 0
        assert fetch_spy == []  # 共享读命中 → 零自拉
        assert captured["title"].startswith("脉搏 10:00")
        assert "数据源：共享:em_direct" in captured["body"]
        meta = mb._read_meta(tmp_path / "midday" / "2026-07-06")
        assert meta["1000"].route == "共享:em_direct"

    def test_stale_drop_falls_back_to_self_fetch(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setenv("RQUANT_PANORAMA_FAKE", "1")
        stale_iso = (self.NOW - timedelta(seconds=600)).isoformat(timespec="seconds")
        _write_drop_fixture(tmp_path, stale_iso, _drop_snapshot())
        calls: list[int] = []

        def counting_fetch(*a, **k) -> pd.DataFrame:
            calls.append(1)
            df = _drop_snapshot().drop(columns=["limit_up_price", "limit_down_price"])
            df.attrs["route"] = "em_socks"
            return df

        with patch.object(mb, "is_trading_day", return_value=True), \
             patch.object(mb, "fetch_market_snapshot", side_effect=counting_fetch), \
             patch.object(mb, "notify", side_effect=lambda *a, **k: None):
            rc = run_morning_pulse(slot="10:00", now=self.NOW, base_dir=tmp_path)
        assert rc == 0
        assert len(calls) == 1  # 陈旧 drop 被拒 → 自拉一次成功
        meta = mb._read_meta(tmp_path / "midday" / "2026-07-06")
        assert meta["1000"].route == "em_socks"  # 无「共享:」前缀

    def test_self_fetch_retry_then_degrade(self, tmp_path, monkeypatch) -> None:
        # 无 drop；自拉两次全空 → sleep(60) 恰一次 → 降级短讯
        calls: list[int] = []
        sleeps: list[float] = []
        monkeypatch.setattr(mb, "_sleep", lambda s: sleeps.append(s))

        def failing_fetch(*a, **k) -> pd.DataFrame:
            calls.append(1)
            df = pd.DataFrame()
            df.attrs["route"] = "none"
            return df

        captured: dict = {}
        with patch.object(mb, "is_trading_day", return_value=True), \
             patch.object(mb, "fetch_market_snapshot", side_effect=failing_fetch), \
             patch.object(mb, "notify",
                          side_effect=lambda scene, **k: captured.update(scene=scene, **k)):
            rc = run_morning_pulse(slot="10:00", now=self.NOW, base_dir=tmp_path)
        assert rc == 0
        assert len(calls) == 2          # 首拉 + 重试恰一次
        assert sleeps == [60]           # 重试间隔 60s（注入不真等）
        assert "快照不可用" in captured["title"]
