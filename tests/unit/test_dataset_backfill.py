"""统一数据集回补层（dataset_backfill）测试。

覆盖：注册表完整性（DDL / MERGE 语义）、by_date 回补（幂等 / 故障隔离 /
dry_run）、snapshot 模式（整表替换 / 空快照保护）、代表性 normalize 列映射、
CLI 解析、adapter 分页取数逻辑。全部 mock，不触网。
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from rquant.adapter.tushare import TushareAdapter
from rquant.cli import build_parser
from rquant.dataset_backfill import (
    DATASETS,
    _norm_hm_list,
    _norm_kpl_list,
    _norm_moneyflow,
    _norm_ths_index,
    _norm_top_inst,
    backfill_dataset,
)
from rquant.research_sync import MERGE_TABLES, REPLACE_TABLES
from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path: Path) -> Iterator[DuckDBStore]:
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


# ── 注册表完整性 ──


def test_registry_covers_expected_datasets() -> None:
    expected = {
        "ths_daily", "dc_daily",
        "ths_index", "ths_member", "dc_index", "dc_member",
        "moneyflow", "moneyflow_dc", "moneyflow_ths", "moneyflow_ind_ths",
        "moneyflow_ind_dc", "moneyflow_cnt_ths", "moneyflow_mkt_dc",
        "top_list", "top_inst",
        "kpl_list", "daily_info", "hm_list", "index_daily",
    }
    assert set(DATASETS) == expected


def test_registry_tables_have_ddl_and_merge_semantics(
    store: DuckDBStore,
) -> None:
    """每个数据集的目标表：DDL 真实可建（store 初始化已跑 ALL_DDL）、
    在 MERGE_TABLES（本地回补权威，整表替换会抹历史）、不在 REPLACE_TABLES。"""
    existing = {
        r[0]
        for r in store._conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()
    }
    for name, spec in DATASETS.items():
        assert spec.name == name
        assert spec.table in existing, f"{spec.table} 缺 DDL"
        assert spec.table in MERGE_TABLES, f"{spec.table} 必须 MERGE 语义"
        assert spec.table not in REPLACE_TABLES, f"{spec.table} 不允许整表替换"


def test_moneyflow_daily_has_full_field_columns(store: DuckDBStore) -> None:
    """moneyflow 全 20 字段迁移列已建齐（原表只有大单/特大单窄口径）。"""
    cols = set(store._dataset_table_columns("moneyflow_daily"))
    for c in ("buy_sm_vol", "sell_md_amount", "buy_lg_amount", "sell_elg_amount"):
        assert c in cols, f"moneyflow_daily 缺迁移列 {c}"


# ── by_date 回补 ──


def _mf_dc_row(trade_date: date, ts_code: str) -> dict:
    return {
        "trade_date": trade_date,
        "ts_code": ts_code,
        "name": "测试股",
        "pct_change": 5.2,
        "close": 21.45,
        "net_amount": 235127.4,
        "net_amount_rate": 11.62,
        "buy_elg_amount": 202142.41,
        "buy_elg_amount_rate": 9.99,
        "buy_lg_amount": 32984.99,
        "buy_lg_amount_rate": 1.63,
        "buy_md_amount": -103910.32,
        "buy_md_amount_rate": -5.13,
        "buy_sm_amount": -131217.07,
        "buy_sm_amount_rate": -6.48,
    }


class _FakeByDateAdapter:
    def __init__(self) -> None:
        self.trade_cal_calls: list[tuple[date, date]] = []
        self.fetch_calls: list[date] = []

    def trade_cal(self, start: date, end: date) -> list[date]:
        self.trade_cal_calls.append((start, end))
        all_dates = [date(2024, 1, 2), date(2024, 1, 3)]
        return [d for d in all_dates if start <= d <= end]

    def moneyflow_dc_by_date(self, trade_date: date) -> pd.DataFrame:
        self.fetch_calls.append(trade_date)
        return pd.DataFrame([
            _mf_dc_row(trade_date, "300059.SZ"),
            _mf_dc_row(trade_date, "001399.SZ"),
        ])


class _FailingDayAdapter(_FakeByDateAdapter):
    def moneyflow_dc_by_date(self, trade_date: date) -> pd.DataFrame:
        if trade_date == date(2024, 1, 3):
            raise RuntimeError("rate limit")
        return super().moneyflow_dc_by_date(trade_date)


def test_by_date_writes_rows(store: DuckDBStore) -> None:
    adapter = _FakeByDateAdapter()

    summary = backfill_dataset(
        "moneyflow_dc", "2024-01-01", "2024-01-05", store, adapter,
        api_sleep=0.0,
    )

    assert summary["dataset"] == "moneyflow_dc"
    assert summary["table"] == "moneyflow_dc_daily"
    assert summary["mode"] == "by_date"
    assert summary["trading_dates_count"] == 2
    assert summary["planned_requests"] == 2
    assert summary["executed_dates"] == 2
    assert summary["failed_dates"] == []
    assert summary["rows"] == 4
    assert adapter.fetch_calls == [date(2024, 1, 2), date(2024, 1, 3)]

    df = store.query("SELECT * FROM moneyflow_dc_daily ORDER BY trade_date, ts_code")
    assert len(df) == 4
    assert set(df["ts_code"]) == {"300059.SZ", "001399.SZ"}


def test_by_date_idempotent(store: DuckDBStore) -> None:
    adapter = _FakeByDateAdapter()
    backfill_dataset(
        "moneyflow_dc", "2024-01-01", "2024-01-05", store, adapter, api_sleep=0.0
    )
    backfill_dataset(
        "moneyflow_dc", "2024-01-01", "2024-01-05", store, adapter, api_sleep=0.0
    )

    df = store.query("SELECT * FROM moneyflow_dc_daily")
    assert len(df) == 4


def test_by_date_isolates_single_day_failure(store: DuckDBStore) -> None:
    summary = backfill_dataset(
        "moneyflow_dc", "2024-01-01", "2024-01-05", store,
        _FailingDayAdapter(), api_sleep=0.0,
    )

    assert summary["executed_dates"] == 2
    assert summary["failed_dates"] == ["2024-01-03"]
    assert summary["rows"] == 2
    df = store.query("SELECT DISTINCT trade_date FROM moneyflow_dc_daily")
    assert len(df) == 1


def test_by_date_dry_run_only_calls_trade_cal(store: DuckDBStore) -> None:
    adapter = _FakeByDateAdapter()

    summary = backfill_dataset(
        "moneyflow_dc", "2024-01-01", "2024-01-05", store, adapter, dry_run=True
    )

    assert summary["dry_run"] is True
    assert summary["planned_requests"] == 2
    assert summary["executed_dates"] == 0
    assert adapter.trade_cal_calls == [(date(2024, 1, 1), date(2024, 1, 5))]
    assert adapter.fetch_calls == []
    assert store.query("SELECT * FROM moneyflow_dc_daily").empty


def test_unknown_dataset_raises(store: DuckDBStore) -> None:
    with pytest.raises(ValueError, match="未知数据集"):
        backfill_dataset(
            "nope", "2024-01-01", "2024-01-05", store, _FakeByDateAdapter()
        )


def test_reversed_range_raises(store: DuckDBStore) -> None:
    with pytest.raises(ValueError, match="start_date"):
        backfill_dataset(
            "moneyflow_dc", "2024-01-05", "2024-01-01", store,
            _FakeByDateAdapter(),
        )


def test_by_date_normalize_applied_moneyflow(store: DuckDBStore) -> None:
    """moneyflow 数据集：net_mf_* 改名 large_net_* + source 补齐后落库。"""

    class _MoneyflowAdapter(_FakeByDateAdapter):
        def moneyflow_full_by_date(self, trade_date: date) -> pd.DataFrame:
            return pd.DataFrame([{
                "ts_code": "600000.SH",
                "trade_date": trade_date,
                "buy_sm_vol": 110065.0,
                "buy_lg_vol": 98462.0,
                "sell_lg_vol": 92355.0,
                "buy_elg_vol": 35486.0,
                "sell_elg_vol": 46818.0,
                "buy_elg_amount": 7893.27,
                "net_mf_vol": -10492.0,
                "net_mf_amount": -2371.73,
            }])

    summary = backfill_dataset(
        "moneyflow", "2024-01-02", "2024-01-02", store,
        _MoneyflowAdapter(), api_sleep=0.0,
    )

    assert summary["rows"] == 1
    df = store.query("SELECT * FROM moneyflow_daily")
    row = df.iloc[0]
    assert row["source"] == "tushare"
    assert row["large_net_vol"] == -10492.0
    assert row["large_net_amount"] == -2371.73
    assert row["buy_sm_vol"] == 110065.0


# ── snapshot 模式 ──


def _ths_board_row(ts_code: str, name: str) -> dict:
    return {
        "ts_code": ts_code,
        "name": name,
        "count": 159.0,
        "exchange": "A",
        "list_date": "20070808",
        "type": "N",
    }


class _FakeSnapshotAdapter:
    def __init__(self, boards: list[dict] | None = None) -> None:
        self.boards = boards if boards is not None else [
            _ths_board_row("885573.TI", "猪肉"),
            _ths_board_row("885808.TI", "养鸡"),
        ]

    def trade_cal(self, start: date, end: date) -> list[date]:
        all_dates = [date(2024, 1, 2), date(2024, 1, 3)]
        return [d for d in all_dates if start <= d <= end]

    def ths_index_snapshot(self) -> pd.DataFrame:
        return pd.DataFrame(self.boards)


def test_snapshot_writes_and_normalizes(store: DuckDBStore) -> None:
    summary = backfill_dataset(
        "ths_index", "2024-01-05", "2024-01-05", store, _FakeSnapshotAdapter()
    )

    assert summary["mode"] == "snapshot"
    # as_of 解析到 end_date 往前最近交易日
    assert summary["start_date"] == "2024-01-03"
    assert summary["failed_dates"] == []
    assert summary["rows"] == 2

    df = store.query("SELECT * FROM ths_board ORDER BY ts_code")
    assert set(df["board_type"]) == {"N"}
    assert df.iloc[0]["member_count"] == 159
    assert "count" not in df.columns


def test_snapshot_replaces_stale_rows(store: DuckDBStore) -> None:
    """整表替换语义：第二次快照缺失的板块（成分调出/板块下线）被删掉。"""
    backfill_dataset(
        "ths_index", "2024-01-05", "2024-01-05", store, _FakeSnapshotAdapter()
    )
    backfill_dataset(
        "ths_index", "2024-01-05", "2024-01-05", store,
        _FakeSnapshotAdapter([_ths_board_row("885573.TI", "猪肉")]),
    )

    df = store.query("SELECT * FROM ths_board")
    assert list(df["ts_code"]) == ["885573.TI"]


def test_snapshot_empty_fetch_keeps_existing(store: DuckDBStore) -> None:
    """源抽风返回空：记失败、保留现有快照，不清表。"""
    backfill_dataset(
        "ths_index", "2024-01-05", "2024-01-05", store, _FakeSnapshotAdapter()
    )
    summary = backfill_dataset(
        "ths_index", "2024-01-05", "2024-01-05", store,
        _FakeSnapshotAdapter([]),
    )

    assert summary["failed_dates"] == ["2024-01-03"]
    assert summary["rows"] == 0
    assert len(store.query("SELECT * FROM ths_board")) == 2


def test_snapshot_dry_run_does_not_fetch(store: DuckDBStore) -> None:
    class _ExplodingAdapter(_FakeSnapshotAdapter):
        def ths_index_snapshot(self) -> pd.DataFrame:
            raise AssertionError("dry_run 不应取数")

    summary = backfill_dataset(
        "ths_index", "2024-01-05", "2024-01-05", store,
        _ExplodingAdapter(), dry_run=True,
    )
    assert summary["planned_requests"] == 1
    assert summary["executed_dates"] == 0


# ── 代表性 normalize 列映射 ──


def test_norm_kpl_list_pk_and_numeric_coercion() -> None:
    df = pd.DataFrame([{
        "ts_code": "300852.SZ",
        "trade_date": date(2026, 7, 1),
        "tag": None,
        "bid_amount": None,
        "bid_turnover": "3.5",
        "pct_chg": None,
        "lu_time": "14:54:12",
    }])
    out = _norm_kpl_list(df)
    assert out.iloc[0]["tag"] == ""
    assert pd.isna(out.iloc[0]["bid_amount"])
    assert out.iloc[0]["bid_turnover"] == 3.5
    assert out.iloc[0]["lu_time"] == "14:54:12"


def test_norm_top_inst_fills_pk_columns() -> None:
    df = pd.DataFrame([{
        "trade_date": date(2026, 7, 1),
        "ts_code": "000004.SZ",
        "exalter": None,
        "side": "0",
        "reason": None,
        "buy": 479638.0,
    }])
    out = _norm_top_inst(df)
    assert out.iloc[0]["exalter"] == ""
    assert out.iloc[0]["reason"] == ""
    assert out.iloc[0]["side"] == "0"


def test_norm_ths_index_renames_keywords() -> None:
    out = _norm_ths_index(pd.DataFrame([_ths_board_row("882001.TI", "安徽")]))
    assert list(out.columns) == [
        "ts_code", "name", "member_count", "exchange", "list_date", "board_type",
    ]
    assert out.iloc[0]["list_date"] == date(2007, 8, 8)


def test_norm_moneyflow_renames_and_adds_source() -> None:
    out = _norm_moneyflow(pd.DataFrame([{
        "ts_code": "600000.SH", "net_mf_vol": 1.0, "net_mf_amount": 2.0,
    }]))
    assert out.iloc[0]["large_net_vol"] == 1.0
    assert out.iloc[0]["large_net_amount"] == 2.0
    assert out.iloc[0]["source"] == "tushare"


def test_norm_hm_list_renames_desc() -> None:
    out = _norm_hm_list(pd.DataFrame([{
        "name": "龙飞虎", "desc": "说明", "orgs": '["某营业部"]',
    }]))
    assert "description" in out.columns
    assert "desc" not in out.columns


# ── 通用落库 ──


def test_upsert_dataset_dedupes_within_batch(store: DuckDBStore) -> None:
    """批内主键重复保留末行，不炸整日。"""
    d = date(2024, 1, 2)
    df = pd.DataFrame([
        {**_mf_dc_row(d, "300059.SZ"), "close": 1.0},
        {**_mf_dc_row(d, "300059.SZ"), "close": 2.0},
    ])
    assert store.upsert_dataset("moneyflow_dc_daily", df) == 1
    out = store.query("SELECT close FROM moneyflow_dc_daily")
    assert out.iloc[0]["close"] == 2.0


def test_upsert_dataset_unknown_table_raises(store: DuckDBStore) -> None:
    with pytest.raises(ValueError, match="未知数据表"):
        store.upsert_dataset("no_such_table", pd.DataFrame([{"a": 1}]))


def test_replace_dataset_rejects_empty(store: DuckDBStore) -> None:
    with pytest.raises(ValueError, match="拒绝空快照"):
        store.replace_dataset("ths_board", pd.DataFrame())


# ── CLI 解析 ──


class TestDataBackfillParser:
    def test_range_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "data-backfill", "--dataset", "moneyflow_dc",
            "--start-date", "2024-01-01", "--end-date", "2024-06-30",
        ])
        assert args.command == "data-backfill"
        assert args.dataset == "moneyflow_dc"
        assert args.start_date == "2024-01-01"
        assert args.end_date == "2024-06-30"
        assert not args.dry_run
        assert not args.today

    def test_today_and_dry_run_flags(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "data-backfill", "--dataset", "all", "--today", "--dry-run",
        ])
        assert args.dataset == "all"
        assert args.today
        assert args.dry_run
        assert args.start_date is None


# ── TushareAdapter 数据集取数（stub _pro，不触网） ──


def _adapter_with_pro(pro: object) -> TushareAdapter:
    adapter = TushareAdapter.__new__(TushareAdapter)
    adapter._pro = pro
    return adapter


class _StubPagedPro:
    """按 offset 出页的 stub：pages 用完返回空页。"""

    def __init__(self, pages: list[pd.DataFrame]) -> None:
        self._pages = pages
        self.calls: list[dict] = []

    def dc_member(self, **kwargs: object) -> pd.DataFrame:
        self.calls.append(dict(kwargs))
        idx = int(kwargs["offset"]) // int(kwargs["limit"])
        if idx < len(self._pages):
            return self._pages[idx]
        return pd.DataFrame()


def _member_page(n: int, start: int = 0) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "trade_date": "20260701",
            "ts_code": "BK0145.DC",
            "con_code": f"{300000 + start + i:06d}.SZ",
            "name": f"股{start + i}",
        }
        for i in range(n)
    ])


def test_dataset_paged_concatenates_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("rquant.adapter.tushare._PAGE_SLEEP", 0.0)
    pro = _StubPagedPro([_member_page(2), _member_page(2, 2), _member_page(1, 4)])
    adapter = _adapter_with_pro(pro)

    out = adapter._dataset_paged(
        "dc_member",
        fields="trade_date,ts_code,con_code,name",
        page_size=2,
        trade_date="20260701",
    )

    assert len(out) == 5
    # 末页 1 行 < page_size，第 4 页不再请求
    assert [c["offset"] for c in pro.calls] == [0, 2, 4]
    assert all(c["limit"] == 2 for c in pro.calls)
    assert all(c["trade_date"] == "20260701" for c in pro.calls)
    # trade_date 归一化为 date
    assert out.iloc[0]["trade_date"] == date(2026, 7, 1)


def test_dataset_paged_exact_page_boundary_stops_on_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """总行数恰为 page_size 整数倍：靠下一页空返回终止。"""
    monkeypatch.setattr("rquant.adapter.tushare._PAGE_SLEEP", 0.0)
    pro = _StubPagedPro([_member_page(2), _member_page(2, 2)])
    adapter = _adapter_with_pro(pro)

    out = adapter._dataset_paged(
        "dc_member", fields="trade_date,ts_code,con_code,name", page_size=2
    )

    assert len(out) == 4
    assert [c["offset"] for c in pro.calls] == [0, 2, 4]


def test_dataset_paged_max_pages_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("rquant.adapter.tushare._PAGE_SLEEP", 0.0)
    pro = _StubPagedPro([_member_page(2) for _ in range(5)])
    adapter = _adapter_with_pro(pro)

    with pytest.raises(RuntimeError, match="分页超过"):
        adapter._dataset_paged(
            "dc_member",
            fields="trade_date,ts_code,con_code,name",
            page_size=2,
            max_pages=3,
        )


class _StubByDatePro:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df
        self.calls: list[dict] = []

    def moneyflow_dc(self, **kwargs: object) -> pd.DataFrame:
        self.calls.append(dict(kwargs))
        return self._df


def test_dataset_by_date_passes_fields_and_normalizes_date() -> None:
    raw = pd.DataFrame([{
        "trade_date": "20260701", "ts_code": "300059.SZ", "net_amount": 1.0,
    }])
    pro = _StubByDatePro(raw)
    adapter = _adapter_with_pro(pro)

    out = adapter.moneyflow_dc_by_date(date(2026, 7, 1))

    assert pro.calls[0]["trade_date"] == "20260701"
    assert "net_amount" in pro.calls[0]["fields"]
    assert out.iloc[0]["trade_date"] == date(2026, 7, 1)


def test_dataset_by_date_empty_returns_empty_frame() -> None:
    adapter = _adapter_with_pro(_StubByDatePro(pd.DataFrame()))
    assert adapter.moneyflow_dc_by_date(date(2026, 7, 1)).empty
