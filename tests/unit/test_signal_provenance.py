"""模拟盘 B 信号溯源：FactorSpec 键名锁死、命中组装、落库与复盘查询。"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest

from rquant.paper import PaperPosition
from rquant.signal_provenance import (
    AUCTION_GAP_MINUTE_STRATEGY,
    AUCTION_GAP_V1,
    AUCTION_GAP_V1_FACTORS,
    GROWTH_SURGE_V1,
    GROWTH_SURGE_V1_FACTORS,
    N_SHAPE_MINUTE_STRATEGY,
    N_SHAPE_V1,
    N_SHAPE_V1_FACTORS,
    FactorSpec,
    SignalProvenance,
    build_signal_factors,
    make_snapshot_id,
    persist_position_with_provenance,
)
from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path) -> Iterator[DuckDBStore]:
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


# ── FactorSpec 键名锁死（与设计文档 2.3 / 全景页因子矩阵对拍）──


def test_auction_gap_v1_factor_keys_locked() -> None:
    """键名是 replay/live/全景页三方契约，改名必须三方同步 + factor_set 升版本。"""
    assert [spec.name for spec in AUCTION_GAP_V1_FACTORS] == [
        "auction_vol_ratio_5d",
        "gap_pct_close",
        "vwap_position",
        "limit_progress",
        "support_ok",
        "rel_amount_same_minute_20d",
        "amount_accel_5m",
        "market_temperature_high60",
    ]


def test_n_shape_v1_factor_keys_locked() -> None:
    """N 字 B 点确认因子键名三端同构（replay/live/全景页），改名必须升 factor_set 版本。"""
    assert N_SHAPE_MINUTE_STRATEGY == "n_shape_minute"
    assert N_SHAPE_V1 == "n_shape_v1"
    assert [spec.name for spec in N_SHAPE_V1_FACTORS] == [
        "auction_vol_ratio_5d",
        "auction_gap_pct",
        "seal_open_times_t",
        "seal_fd_to_circ_pct_t",
        "price_percentile_250d",
        "ma_alignment",
        "vwap_position",
        "rel_amount_same_minute_20d",
        "market_above_ma20_ratio_pct",
    ]
    # 已证伪方向的因子锁死为观察位（op=None），不得悄悄参与判定
    by_name = {spec.name: spec for spec in N_SHAPE_V1_FACTORS}
    assert by_name["rel_amount_same_minute_20d"].op is None
    assert by_name["market_above_ma20_ratio_pct"].op is None


def test_growth_surge_v1_factor_keys_locked() -> None:
    """科创/创业放量因子键名锁死；观察位与用户三条件的判定方向不得悄悄改动。"""
    assert GROWTH_SURGE_V1 == "growth_surge_v1"
    assert [spec.name for spec in GROWTH_SURGE_V1_FACTORS] == [
        "rel_cum_amount_asof",
        "rel_amount_same_minute",
        "amount_accel_5m",
        "classic_volume_ratio",
        "inner_outer_ratio",
        "large_net_vol_t1",
        "price_percentile_250d",
        "market_above_ma20_ratio_pct",
        "board_gap_up_ratio",
        "board_auction_amount_ratio",
    ]
    by_name = {spec.name: spec for spec in GROWTH_SURGE_V1_FACTORS}
    # 经典量比与市场温度只观察不判定
    assert by_name["classic_volume_ratio"].op is None
    assert by_name["market_above_ma20_ratio_pct"].op is None
    # 板块竞价强度为闸门方向（越高越强），竞价快照 09:25 已知
    assert by_name["board_gap_up_ratio"].op == ">="
    assert by_name["board_auction_amount_ratio"].op == ">="
    assert by_name["board_auction_amount_ratio"].basis == "signal_date_auction"
    # 用户口径：内盘>外盘（tick-rule 近似）判多，方向不得翻转
    assert by_name["inner_outer_ratio"].op == ">"
    assert by_name["inner_outer_ratio"].threshold == 1.0
    # 大单净量为 T-1 口径（防未来函数）
    assert by_name["large_net_vol_t1"].op == ">"
    assert by_name["large_net_vol_t1"].basis == "t_minus_1"
    assert by_name["price_percentile_250d"].op == "<="


def test_build_signal_factors_supports_n_shape_v1() -> None:
    factors = build_signal_factors(
        {
            "auction_vol_ratio_5d": 2.0,
            "auction_gap_pct": 3.1,
            "seal_open_times_t": 0,
            "seal_fd_to_circ_pct_t": 4.2,
            "price_percentile_250d": 0.18,
            "ma_alignment": 1,
            "vwap_position": 1.006,
            "rel_amount_same_minute_20d": 2.8,
            "market_above_ma20_ratio_pct": 41.0,
        },
        factor_set=N_SHAPE_V1,
    )

    assert factors.factor_set == N_SHAPE_V1
    assert set(factors.factors) == {spec.name for spec in N_SHAPE_V1_FACTORS}
    assert factors.factors["seal_open_times_t"].hit is True  # 0 <= 1
    assert factors.factors["price_percentile_250d"].hit is True  # 0.18 <= 0.5
    assert factors.factors["ma_alignment"].hit is True
    assert factors.factors["rel_amount_same_minute_20d"].hit is None
    assert factors.factors["market_above_ma20_ratio_pct"].hit is None
    assert factors.hit_count == 7
    assert factors.evaluated_count == 7


def test_factor_spec_threshold_validation() -> None:
    with pytest.raises(ValueError, match="未定义 threshold"):
        FactorSpec(name="x", label="x", tier="minute", op=">=")
    with pytest.raises(ValueError, match="无比较符却定义了"):
        FactorSpec(name="x", label="x", tier="minute", threshold=1.0)


# ── build_signal_factors 组装 ──


def _full_raw_values() -> dict[str, object]:
    return {
        "auction_vol_ratio_5d": 1.82,
        "gap_pct_close": 3.4,
        "vwap_position": 1.004,
        "limit_progress": 0.35,
        "support_ok": 1,
        "rel_amount_same_minute_20d": 3.1,
        "amount_accel_5m": 2.4,
        "market_temperature_high60": 12.4,
    }


def test_build_signal_factors_hits_and_counts() -> None:
    factors = build_signal_factors(_full_raw_values())

    assert factors.factor_set == AUCTION_GAP_V1
    assert set(factors.factors) == {spec.name for spec in AUCTION_GAP_V1_FACTORS}
    assert factors.factors["auction_vol_ratio_5d"].hit is True
    assert factors.factors["support_ok"].hit is True
    assert factors.factors["amount_accel_5m"].hit is None  # 观察因子不参与判定
    assert factors.hit_count == 7
    assert factors.evaluated_count == 7


def test_build_signal_factors_miss_and_bool_false() -> None:
    raw = _full_raw_values()
    raw["limit_progress"] = 0.1  # < 0.2 未命中
    raw["support_ok"] = 0  # 布尔因子假值
    factors = build_signal_factors(raw)

    assert factors.factors["limit_progress"].hit is False
    assert factors.factors["support_ok"].hit is False
    assert factors.hit_count == 5
    assert factors.evaluated_count == 7


def test_build_signal_factors_omits_missing_and_nan() -> None:
    raw = _full_raw_values()
    del raw["market_temperature_high60"]  # 无数据的因子直接不出现
    raw["rel_amount_same_minute_20d"] = None
    raw["amount_accel_5m"] = float("nan")
    factors = build_signal_factors(raw)

    assert "market_temperature_high60" not in factors.factors
    assert "rel_amount_same_minute_20d" not in factors.factors
    assert "amount_accel_5m" not in factors.factors
    assert factors.evaluated_count == 5


def test_build_signal_factors_threshold_overrides() -> None:
    factors = build_signal_factors(
        _full_raw_values(),
        threshold_overrides={"limit_progress": 0.5},
    )
    reading = factors.factors["limit_progress"]
    assert reading.threshold == 0.5
    assert reading.hit is False

    with pytest.raises(ValueError, match="spec 之外的键"):
        build_signal_factors(_full_raw_values(), threshold_overrides={"nope": 1.0})
    with pytest.raises(ValueError, match="未知 factor_set"):
        build_signal_factors(_full_raw_values(), factor_set="nope_v9")


def test_signal_factors_json_shape_matches_design() -> None:
    payload = build_signal_factors(_full_raw_values()).to_json_payload()

    assert payload["schema_version"] == 1
    assert payload["factor_set"] == AUCTION_GAP_V1
    factors = payload["factors"]
    # 布尔因子：只有 value/hit，不带 op/threshold
    assert factors["support_ok"] == {"value": 1, "hit": True}
    # 观察因子：hit 显式为 null（区别于被 exclude 掉）
    assert factors["amount_accel_5m"] == {"value": 2.4, "hit": None}
    # 比较因子：op/threshold 全带；market 因子带 basis
    assert factors["gap_pct_close"] == {
        "value": 3.4, "hit": True, "op": ">", "threshold": 0.0,
    }
    assert factors["market_temperature_high60"]["basis"] == "prev_day"
    # 整体可 JSON 序列化
    assert json.loads(json.dumps(payload))["hit_count"] == 7


# ── schema 迁移 ──

_OLD_PAPER_POSITION_DDL = """
CREATE TABLE paper_position (
    position_id          VARCHAR PRIMARY KEY,
    trade_date           DATE      NOT NULL,
    ts_code              VARCHAR   NOT NULL,
    name                 VARCHAR,
    pool                 VARCHAR,
    entry_time           TIMESTAMP NOT NULL,
    entry_price          DOUBLE    NOT NULL,
    entry_signal         VARCHAR   NOT NULL,
    candidate_id         VARCHAR   NOT NULL,
    earliest_exit_date   DATE      NOT NULL,
    stop_loss_price      DOUBLE    NOT NULL,
    stop_loss_basis      VARCHAR   NOT NULL,
    stop_loss_pct        DOUBLE    NOT NULL,
    status               VARCHAR   NOT NULL,
    feature_snapshot_id  VARCHAR,
    param_payload        JSON
);
"""


def test_schema_migration_adds_columns_and_backfills_run_mode(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(_OLD_PAPER_POSITION_DDL)
    conn.execute(
        "INSERT INTO paper_position (position_id, trade_date, ts_code, entry_time, "
        "entry_price, entry_signal, candidate_id, earliest_exit_date, stop_loss_price, "
        "stop_loss_basis, stop_loss_pct, status) VALUES ('p-old', DATE '2026-06-25', "
        "'600000.SH', TIMESTAMP '2026-06-25 09:33:00', 10.5, 'attack', 'baseline', "
        "DATE '2026-06-26', 10.2, 'pct_3', 0.03, 'open')"
    )
    conn.close()

    store = DuckDBStore(db_path)  # _init_schema 跑 ALL_DDL（含 4 条 ALTER 迁移）
    try:
        cols = set(
            store.query(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'paper_position'"
            )["column_name"]
        )
        assert {"strategy_name", "signal_factors", "run_mode", "run_id"} <= cols
        row = store.query("SELECT run_mode, run_id FROM paper_position").iloc[0]
        assert row["run_mode"] == "live"  # 历史行按 DEFAULT 回填
        assert row["run_id"] is None
    finally:
        store.close()


def test_schema_migration_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "fresh.duckdb"
    DuckDBStore(db_path).close()
    store = DuckDBStore(db_path)  # 第二次开库重跑全部 DDL，不应报错
    try:
        assert (
            store.query(
                "SELECT COUNT(*) AS n FROM information_schema.columns "
                "WHERE table_name = 'paper_position' AND column_name = 'run_mode'"
            ).iloc[0]["n"]
            == 1
        )
    finally:
        store.close()


# ── 落库 / 查询 / 清理 ──


def _make_position(
    position_id: str,
    *,
    ts_code: str = "600000.SH",
    trade_date: date = date(2026, 6, 25),
    status: str = "closed",
    pnl_pct: float | None = 1.5,
    max_drawdown_pct: float | None = -1.0,
) -> PaperPosition:
    closed = status == "closed"
    return PaperPosition(
        position_id=position_id,
        trade_date=trade_date,
        ts_code=ts_code,
        name="测试标的",
        pool="auction_gap",
        entry_time=datetime.combine(trade_date, datetime.min.time()).replace(
            hour=9, minute=33
        ),
        entry_price=10.53,
        entry_price_raw=10.53,
        entry_signal="auction_gap_vwap_push",
        candidate_id="baseline",
        earliest_exit_date=date(2026, 6, 26),
        stop_loss_price=10.2,
        stop_loss_basis="pct_3",
        stop_loss_pct=0.03,
        take_profit_price=11.06,
        take_profit_pct=0.05,
        trailing_stop_pct=0.025,
        status=status,
        exit_time=datetime(2026, 6, 26, 9, 30) if closed else None,
        exit_price=10.85 if closed else None,
        exit_reason="next_auction_weak" if closed else None,
        holding_trading_days=1 if closed else None,
        pnl_pct=pnl_pct if closed else None,
        max_drawdown_pct=max_drawdown_pct if closed else None,
    )


def _make_provenance(
    raw_values: dict[str, object] | None = None,
    *,
    as_of_time: datetime = datetime(2026, 6, 25, 9, 32),
) -> SignalProvenance:
    return SignalProvenance(
        strategy_name=AUCTION_GAP_MINUTE_STRATEGY,
        signal_factors=build_signal_factors(raw_values or _full_raw_values()),
        raw_payload={
            "auction_price": 10.4,
            "entry_signal_time": as_of_time,  # datetime 走 _json_default
            "entry_signal_vwap": 10.47,
        },
        as_of_time=as_of_time,
        lookback_days=20,
    )


def test_persist_position_live_writes_snapshot_then_position(store: DuckDBStore) -> None:
    position = _make_position("20260625-600000.SH-auction_gap_vwap_push")
    provenance = _make_provenance()

    snapshot_id = persist_position_with_provenance(store, position, provenance)

    assert snapshot_id == make_snapshot_id(
        position.trade_date, position.ts_code, "auction_gap_minute_entry",
        provenance.as_of_time,
    )
    row = store.query("SELECT * FROM paper_position").iloc[0]
    assert row["strategy_name"] == AUCTION_GAP_MINUTE_STRATEGY
    assert row["run_mode"] == "live"
    assert row["run_id"] is None
    assert row["feature_snapshot_id"] == snapshot_id
    factors = json.loads(row["signal_factors"])
    assert factors["factor_set"] == AUCTION_GAP_V1
    assert set(factors["factors"]) <= {spec.name for spec in AUCTION_GAP_V1_FACTORS}

    snapshot = store.query("SELECT * FROM intraday_feature_snapshot").iloc[0]
    assert snapshot["snapshot_id"] == snapshot_id
    assert snapshot["feature_set"] == "auction_gap_minute_entry"
    assert snapshot["source"] == "live"
    assert snapshot["lookback_days"] == 20
    payload = json.loads(snapshot["payload"])
    assert payload["auction_price"] == 10.4
    assert payload["entry_signal_time"] == "2026-06-25T09:32:00"


def test_persist_position_replay_requires_run_id(store: DuckDBStore) -> None:
    position = _make_position("20260625-600000.SH-auction_gap_vwap_push")
    with pytest.raises(ValueError, match="run_id"):
        persist_position_with_provenance(
            store, position, _make_provenance(), run_mode="replay"
        )


def test_persist_replay_and_delete_by_run_id(store: DuckDBStore) -> None:
    for idx, run in ((1, "run-A"), (2, "run-A"), (3, "run-B")):
        persist_position_with_provenance(
            store,
            _make_position(f"p-{idx}", ts_code=f"60000{idx}.SH"),
            _make_provenance(),
            run_mode="replay",
            run_id=run,
            param_payload={"stop_loss_pct": 0.03},
        )
    persist_position_with_provenance(
        store, _make_position("p-live", ts_code="000001.SZ"), _make_provenance()
    )
    assert len(store.query("SELECT * FROM paper_position")) == 4
    assert len(store.query("SELECT * FROM intraday_feature_snapshot")) == 4

    assert store.delete_paper_positions_by_run_id("run-A") == 2

    remaining = store.query("SELECT position_id, run_mode FROM paper_position")
    assert sorted(remaining["position_id"]) == ["p-3", "p-live"]
    # run-A 的入场快照一并清理，run-B 与 live 的保留
    assert len(store.query("SELECT * FROM intraday_feature_snapshot")) == 2


def test_query_paper_positions_filters(store: DuckDBStore) -> None:
    persist_position_with_provenance(
        store,
        _make_position("p-live-1", trade_date=date(2026, 6, 25)),
        _make_provenance(),
    )
    persist_position_with_provenance(
        store,
        _make_position("p-live-2", ts_code="000001.SZ", trade_date=date(2026, 7, 1)),
        _make_provenance(),
    )
    persist_position_with_provenance(
        store,
        _make_position("p-replay", ts_code="000002.SZ", trade_date=date(2026, 6, 25)),
        _make_provenance(),
        run_mode="replay",
        run_id="run-X",
    )

    assert len(store.query_paper_positions()) == 2  # 默认只看 live
    assert len(store.query_paper_positions(run_mode="replay")) == 1
    assert len(store.query_paper_positions(run_mode=None)) == 3
    assert (
        store.query_paper_positions(
            date(2026, 6, 25), date(2026, 6, 30)
        )["position_id"].tolist()
        == ["p-live-1"]
    )
    assert len(
        store.query_paper_positions(strategy_name=AUCTION_GAP_MINUTE_STRATEGY)
    ) == 2
    assert store.query_paper_positions(strategy_name="nshape_attack").empty
    assert len(store.query_paper_positions(status="closed")) == 2


def test_aggregate_paper_positions_by_factor_hits(store: DuckDBStore) -> None:
    cases = [
        ("p-1", 0.35, 3.0, 2.0),   # progress hit, rel hit, +2.0
        ("p-2", 0.30, 2.5, -1.0),  # progress hit, rel hit, -1.0
        ("p-3", 0.25, 1.0, 0.5),   # progress hit, rel miss, +0.5
        ("p-4", 0.10, 2.2, -2.0),  # progress miss, rel hit, -2.0
    ]
    for idx, (pid, progress, rel, pnl) in enumerate(cases):
        raw = _full_raw_values()
        raw["limit_progress"] = progress
        raw["rel_amount_same_minute_20d"] = rel
        persist_position_with_provenance(
            store,
            _make_position(pid, ts_code=f"60000{idx}.SH", pnl_pct=pnl),
            _make_provenance(raw),
        )

    out = store.aggregate_paper_positions_by_factor_hits(
        ["limit_progress", "rel_amount_same_minute_20d"],
        strategy_name=AUCTION_GAP_MINUTE_STRATEGY,
    )

    assert out[["limit_progress", "rel_amount_same_minute_20d"]].to_numpy().tolist() == [
        ["false", "true"],
        ["true", "false"],
        ["true", "true"],
    ]
    assert out["trades"].tolist() == [1, 1, 2]
    assert out["mean_ret"].tolist() == [-2.0, 0.5, 0.5]
    assert out["win_rate_pct"].tolist() == [0.0, 100.0, 50.0]

    with pytest.raises(ValueError, match="非法因子键名"):
        store.aggregate_paper_positions_by_factor_hits(["bad-name; DROP"])
    with pytest.raises(ValueError, match="不能为空"):
        store.aggregate_paper_positions_by_factor_hits([])


# ── replay 写入路径（auction_gap_strategy 集成）──


def test_auction_gap_replay_persist_flag(store: DuckDBStore) -> None:
    from rquant.auction_gap_strategy import (
        AuctionGapMinuteReplayConfig,
        run_auction_gap_minute_replay,
    )
    from tests.unit.test_auction_gap_minute_replay import _seed_base

    _seed_base(store)
    config = AuctionGapMinuteReplayConfig(
        start_date="2026-06-25",
        end_date="2026-06-25",
        max_hold_days=1,
    )

    # 默认不落库：反复实验不刷表
    trades = run_auction_gap_minute_replay(store, config)
    assert len(trades) == 1
    assert store.query("SELECT * FROM paper_position").empty

    trades = run_auction_gap_minute_replay(
        store, config, persist_positions=True, run_id="lab-run-1"
    )
    assert len(trades) == 1
    row = store.query("SELECT * FROM paper_position").iloc[0]
    assert row["strategy_name"] == AUCTION_GAP_MINUTE_STRATEGY
    assert row["run_mode"] == "replay"
    assert row["run_id"] == "lab-run-1"
    assert row["status"] == "closed"
    assert row["pnl_pct"] == pytest.approx(round((10.85 / 10.53 - 1) * 100, 4))

    factors = json.loads(row["signal_factors"])
    spec_names = {spec.name for spec in AUCTION_GAP_V1_FACTORS}
    assert set(factors["factors"]) <= spec_names  # replay 键名与 live 同一份 spec
    # 入场门槛因子在成交仓里必然命中
    for name in ("auction_vol_ratio_5d", "gap_pct_close", "vwap_position",
                 "limit_progress", "support_ok"):
        assert factors["factors"][name]["hit"] is True, name
    # 无历史分钟基准 → 相对放量因子缺席而非记为未命中
    assert "rel_amount_same_minute_20d" not in factors["factors"]

    snapshot = store.query("SELECT * FROM intraday_feature_snapshot").iloc[0]
    assert snapshot["snapshot_id"] == row["feature_snapshot_id"]
    assert snapshot["source"] == "replay"
    payload = json.loads(snapshot["payload"])
    assert payload["auction_price"] == pytest.approx(10.4)

    # 同一 run_id 重跑走 INSERT OR REPLACE，不产生重复行
    run_auction_gap_minute_replay(
        store, config, persist_positions=True, run_id="lab-run-1"
    )
    assert len(store.query("SELECT * FROM paper_position")) == 1
    assert store.delete_paper_positions_by_run_id("lab-run-1") == 1


# ── research_sync 兼容（新列走列交集自动兼容）──


def test_restore_research_tables_merges_paper_position_from_old_schema_backup(
    tmp_path: Path,
) -> None:
    from rquant.research_sync import restore_research_tables

    backup_path = tmp_path / "cloud_backup.duckdb"
    conn = duckdb.connect(str(backup_path))
    conn.execute(_OLD_PAPER_POSITION_DDL)  # 云端老 schema：没有溯源新列
    conn.execute(
        "INSERT INTO paper_position (position_id, trade_date, ts_code, entry_time, "
        "entry_price, entry_signal, candidate_id, earliest_exit_date, stop_loss_price, "
        "stop_loss_basis, stop_loss_pct, status) VALUES ('p-cloud', DATE '2026-07-01', "
        "'600519.SH', TIMESTAMP '2026-07-01 09:40:00', 1500.0, 'attack', 'baseline', "
        "DATE '2026-07-02', 1450.0, 'pct_3', 0.03, 'open')"
    )
    conn.close()

    db_path = tmp_path / "local.duckdb"
    local = DuckDBStore(db_path)
    persist_position_with_provenance(
        local,
        _make_position("p-local", ts_code="000001.SZ"),
        _make_provenance(),
        run_mode="replay",
        run_id="run-L",
    )
    local.close()

    report = restore_research_tables(
        backup_path,
        db_path,
        tables=["paper_position"],
        refresh_replica=False,
    )
    assert not report.has_errors
    paper_result = next(t for t in report.tables if t.table == "paper_position")
    assert paper_result.mode == "merge"
    assert "restore" in paper_result.detail
    assert paper_result.rows == 1

    check = duckdb.connect(str(db_path), read_only=True)
    rows = check.execute(
        "SELECT position_id, run_mode, run_id FROM paper_position ORDER BY position_id"
    ).fetchall()
    check.close()
    # 旧研究备份按列交集恢复，新列吃 DEFAULT；本地 replay 行不丢
    assert rows == [("p-cloud", "live", None), ("p-local", "replay", "run-L")]
