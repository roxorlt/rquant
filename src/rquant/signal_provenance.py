"""模拟盘 B 信号溯源：FactorSpec 因子清单、命中矩阵组装与统一落库入口。

核心原则（docs/plans/2026-07-02-paper-signal-provenance.md）：replay 回测与
未来 live 模拟买入写同一套溯源结构——同一份 FactorSpec、同一个组装函数、
同一个落库入口，因子键名与全景页因子矩阵完全一致。

职责分层：
- ``paper_position.signal_factors``（JSON）只放入场判定因子的命中矩阵，小而精；
- ``intraday_feature_snapshot.payload`` 放触发时刻的全量中间量（原始特征 dict）；
- ``persist_position_with_provenance`` 是唯一落库入口：事务内先写 snapshot
  再写 position。谁调用谁负责写者串行（live 只能是 monitor 进程；replay 落库
  必须在非盘中时段跑，否则撞 monitor 写锁）。
"""

from __future__ import annotations

import json
import math
import operator
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, time
from typing import Literal

import pandas as pd
from loguru import logger
from pydantic import BaseModel, ConfigDict, model_validator

from rquant.paper import PaperPosition
from rquant.storage.duckdb import DuckDBStore

RunMode = Literal["live", "replay"]
FactorOp = Literal[">=", ">", "<=", "<", "bool"]
FactorTier = Literal["snapshot", "minute", "market"]

SIGNAL_FACTORS_SCHEMA_VERSION = 1

AUCTION_GAP_MINUTE_STRATEGY = "auction_gap_minute"
AUCTION_GAP_V1 = "auction_gap_v1"
N_SHAPE_MINUTE_STRATEGY = "n_shape_minute"
N_SHAPE_V1 = "n_shape_v1"
GROWTH_SURGE_V1 = "growth_surge_v1"

_COMPARE_OPS: dict[str, Callable[[float, float], bool]] = {
    ">=": operator.ge,
    ">": operator.gt,
    "<=": operator.le,
    "<": operator.lt,
}


class FactorSpec(BaseModel):
    """单个判定因子的定义：键名 + 展示名 + 判定口径。

    - ``op`` 为比较符时必须给 ``threshold``；
    - ``op='bool'`` 表示布尔因子（value 真值即命中，输出 JSON 不带 op/threshold）；
    - ``op=None`` 表示观察因子：记录取值但不参与判定（hit 固定为 null）。
    """

    model_config = ConfigDict(frozen=True)

    name: str
    label: str
    tier: FactorTier
    op: FactorOp | None = None
    threshold: float | None = None
    basis: str | None = None

    @model_validator(mode="after")
    def _check_threshold(self) -> FactorSpec:
        if self.op in _COMPARE_OPS and self.threshold is None:
            msg = f"因子 {self.name} 使用比较符 {self.op} 但未定义 threshold"
            raise ValueError(msg)
        if self.op not in _COMPARE_OPS and self.threshold is not None:
            msg = f"因子 {self.name} 无比较符却定义了 threshold"
            raise ValueError(msg)
        return self


# 与全景页因子矩阵共用的键名（文档一 3.1 / 文档二 2.3），改阈值必须递增 factor_set 版本号
AUCTION_GAP_V1_FACTORS: tuple[FactorSpec, ...] = (
    FactorSpec(
        name="auction_vol_ratio_5d", label="竞价量比(5日)",
        tier="snapshot", op=">=", threshold=0.15,
    ),
    FactorSpec(
        name="gap_pct_close", label="竞价跳空幅度(%)",
        tier="snapshot", op=">", threshold=0.0,
    ),
    FactorSpec(
        name="vwap_position", label="VWAP位置(价/价格底线)",
        tier="minute", op=">=", threshold=1.0,
    ),
    FactorSpec(
        name="limit_progress", label="涨停进度",
        tier="minute", op=">=", threshold=0.2,
    ),
    FactorSpec(name="support_ok", label="回撤承接", tier="minute", op="bool"),
    FactorSpec(
        name="rel_amount_same_minute_20d", label="同分钟相对放量(20日)",
        tier="minute", op=">=", threshold=2.0,
    ),
    FactorSpec(name="amount_accel_5m", label="分钟放量加速(5m)", tier="minute"),
    FactorSpec(
        name="market_temperature_high60", label="市场温度(60日新高占比)",
        tier="market", op=">=", threshold=8.0, basis="prev_day",
    ),
)

# N 字分钟级 B 点多因子确认因子组（docs/plans/2026-07-03-nshape-factor-confirm-design.md 2 节）。
# 日期口径：T = 池筛选日（非涨停日），B = 次日买入日，N 字首板在 T-1（pool1）/ T-2（pool2）。
# 竞价因子 B 日 09:26 已知；封板质量为首板日（[T-3,T] 最近 'U' 行）收盘后官方数据；
# 低位/均线/温度取 T-1 收盘（B 日无未来函数）。rel_amount 与温度为已证伪方向，
# 只观察不判定（op=None）。命中阈值按 N 字池实测分布标定（竞价量比中位数 ~0.013）。
N_SHAPE_V1_FACTORS: tuple[FactorSpec, ...] = (
    FactorSpec(
        name="auction_vol_ratio_5d", label="竞价量比(5日)",
        tier="snapshot", op=">=", threshold=0.02, basis="b_open_auction",
    ),
    FactorSpec(
        name="auction_gap_pct", label="竞价跳空幅度(%)",
        tier="snapshot", op=">", threshold=0.0, basis="b_open_auction",
    ),
    FactorSpec(
        name="seal_open_times_t", label="首板开板次数",
        tier="snapshot", op="<=", threshold=1.0, basis="board_day",
    ),
    FactorSpec(
        name="seal_fd_to_circ_pct_t", label="首板封单/流通市值(%)",
        tier="snapshot", op=">=", threshold=1.0, basis="board_day",
    ),
    FactorSpec(
        name="price_percentile_250d", label="250日收盘百分位",
        tier="snapshot", op="<=", threshold=0.5, basis="t_minus_1",
    ),
    FactorSpec(name="ma_alignment", label="均线多头排列", tier="snapshot", op="bool",
               basis="t_minus_1"),
    FactorSpec(
        name="vwap_position", label="VWAP位置(价/VWAP)",
        tier="minute", op=">=", threshold=1.0,
    ),
    FactorSpec(
        name="rel_amount_same_minute_20d", label="同分钟相对放量(20日)",
        tier="minute",
    ),
    FactorSpec(
        name="market_above_ma20_ratio_pct", label="市场温度(MA20上方占比)",
        tier="market", basis="t_minus_1",
    ),
)

# 科创/创业板盘中放量追击多因子组（用户三条件 + 现有放量三件套 + 已验证低位因子）。
# 日期口径：T = 信号日（当日盘中入场），T-1 = 前一交易日。
# - 量比：rel_cum_amount_asof 是现有宽门的成交额口径（累计额/20 日同刻中位）；
#   classic_volume_ratio 为经典量比观察值（当日每分钟均量 / T-1 收盘可知的 5 日
#   每分钟均量），只记录不判定
# - 外盘>内盘：盘中真实内外盘无历史数据，用分钟 tick-rule 近似（close 对比前一
#   分钟 close：升=外盘/主动买，降=内盘/主动卖，平=均分；首分钟对比自身 open）。
#   标准口径按主动买盘占优判多：inner_outer_ratio < 1 命中
# - 大单净量：T 日盘中不可知，用 T-1 moneyflow_daily.large_net_vol > 0 防未来函数
#   （与用户口径「今日大单净量」存在 1 个交易日滞后）
GROWTH_SURGE_V1_FACTORS: tuple[FactorSpec, ...] = (
    FactorSpec(
        name="rel_cum_amount_asof", label="累计放量(相对同刻中位)",
        tier="minute", op=">=", threshold=1.4,
    ),
    FactorSpec(
        name="rel_amount_same_minute", label="同刻放量(相对同分钟中位)",
        tier="minute", op=">=", threshold=2.0,
    ),
    FactorSpec(
        name="amount_accel_5m", label="分钟放量加速(5m)",
        tier="minute", op=">=", threshold=2.0,
    ),
    FactorSpec(name="classic_volume_ratio", label="经典量比(观察)", tier="minute"),
    FactorSpec(
        name="inner_outer_ratio", label="内外盘比(tick-rule近似)",
        tier="minute", op="<", threshold=1.0,
    ),
    FactorSpec(
        name="large_net_vol_t1", label="大单净量(T-1)",
        tier="snapshot", op=">", threshold=0.0, basis="t_minus_1",
    ),
    FactorSpec(
        name="price_percentile_250d", label="250日收盘百分位",
        tier="snapshot", op="<=", threshold=0.5, basis="t_minus_1",
    ),
    FactorSpec(
        name="market_above_ma20_ratio_pct", label="市场温度(MA20上方占比)",
        tier="market", basis="t_minus_1",
    ),
    # T 日板块集合竞价强度（候选票所在题材，按 ≤T 最近打点还原成分）：
    # 高开占比与竞价资金相对历史都是「板块资金青睐度」的闸门方向（越高越强），
    # 竞价快照 09:25 已知，无未来函数（口径见 board_auction_strength）
    FactorSpec(
        name="board_gap_up_ratio", label="板块竞价高开占比",
        tier="snapshot", op=">=", threshold=0.5, basis="signal_date_auction",
    ),
    FactorSpec(
        name="board_auction_amount_ratio", label="板块竞价资金相对历史",
        tier="snapshot", op=">=", threshold=1.0, basis="signal_date_auction",
    ),
)

FACTOR_SETS: dict[str, tuple[FactorSpec, ...]] = {
    AUCTION_GAP_V1: AUCTION_GAP_V1_FACTORS,
    N_SHAPE_V1: N_SHAPE_V1_FACTORS,
    GROWTH_SURGE_V1: GROWTH_SURGE_V1_FACTORS,
}


class FactorReading(BaseModel):
    """单因子在入场时刻的读数：取值 + 是否命中 + 判定口径快照。"""

    model_config = ConfigDict(frozen=True)

    value: float | int | None
    hit: bool | None
    op: str | None = None
    threshold: float | None = None
    basis: str | None = None

    def to_payload(self) -> dict[str, object]:
        """输出 JSON 结构：hit 恒在（null=观察因子），op/threshold/basis 缺省不写。"""
        payload: dict[str, object] = {"value": self.value, "hit": self.hit}
        if self.op is not None:
            payload["op"] = self.op
        if self.threshold is not None:
            payload["threshold"] = self.threshold
        if self.basis is not None:
            payload["basis"] = self.basis
        return payload


class SignalFactors(BaseModel):
    """入场判定因子命中矩阵，落 ``paper_position.signal_factors``。"""

    model_config = ConfigDict(frozen=True)

    schema_version: int = SIGNAL_FACTORS_SCHEMA_VERSION
    factor_set: str
    factors: dict[str, FactorReading]
    hit_count: int
    evaluated_count: int

    def to_json_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "factor_set": self.factor_set,
            "factors": {
                name: reading.to_payload() for name, reading in self.factors.items()
            },
            "hit_count": self.hit_count,
            "evaluated_count": self.evaluated_count,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_json_payload(), ensure_ascii=False)


class SignalProvenance(BaseModel):
    """一次入场信号的完整溯源：策略归属 + 命中矩阵 + 全量现场。"""

    model_config = ConfigDict(frozen=True)

    strategy_name: str
    signal_factors: SignalFactors
    raw_payload: dict[str, object]
    as_of_time: datetime
    lookback_days: int | None = None


def _normalize_value(value: object) -> float | int | None:
    """把原始因子值归一成 JSON 可比较的标量；NaN/None 视为无数据。"""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return None
        return round(value, 4)
    item = getattr(value, "item", None)  # numpy 标量
    if callable(item):
        return _normalize_value(item())
    msg = f"因子值类型不支持: {type(value).__name__}"
    raise TypeError(msg)


def build_signal_factors(
    raw_values: Mapping[str, object],
    *,
    factor_set: str = AUCTION_GAP_V1,
    specs: Sequence[FactorSpec] | None = None,
    threshold_overrides: Mapping[str, float] | None = None,
) -> SignalFactors:
    """按 FactorSpec 清单把原始因子值组装成命中矩阵。

    约定（设计文档 2.3）：无数据的因子直接不出现在 factors 里（区别于未命中）；
    观察因子 hit=null；``threshold_overrides`` 允许参数化 run 记录实际生效阈值
    （键名仍锁死在 spec 上）。
    """
    if specs is None:
        try:
            specs = FACTOR_SETS[factor_set]
        except KeyError as e:
            msg = f"未知 factor_set: {factor_set}（已注册: {sorted(FACTOR_SETS)}）"
            raise ValueError(msg) from e
    overrides = dict(threshold_overrides or {})
    unknown = set(overrides) - {spec.name for spec in specs}
    if unknown:
        msg = f"threshold_overrides 含 spec 之外的键: {sorted(unknown)}"
        raise ValueError(msg)

    factors: dict[str, FactorReading] = {}
    for spec in specs:
        if spec.name not in raw_values:
            continue
        value = _normalize_value(raw_values[spec.name])
        if value is None:
            continue
        threshold = overrides.get(spec.name, spec.threshold)
        if spec.op in _COMPARE_OPS:
            assert threshold is not None  # FactorSpec 校验 + overrides 只允许 float
            hit = _COMPARE_OPS[spec.op](float(value), float(threshold))
            reading = FactorReading(
                value=value, hit=hit, op=spec.op, threshold=threshold, basis=spec.basis
            )
        elif spec.op == "bool":
            reading = FactorReading(value=value, hit=bool(value), basis=spec.basis)
        else:
            reading = FactorReading(value=value, hit=None, basis=spec.basis)
        factors[spec.name] = reading

    return SignalFactors(
        factor_set=factor_set,
        factors=factors,
        hit_count=sum(1 for reading in factors.values() if reading.hit is True),
        evaluated_count=sum(1 for reading in factors.values() if reading.hit is not None),
    )


def _json_default(value: object) -> object:
    if isinstance(value, datetime | date | time):
        return value.isoformat()
    item = getattr(value, "item", None)  # numpy 标量
    if callable(item):
        return item()
    return str(value)


def make_snapshot_id(
    trade_date: date,
    ts_code: str,
    feature_set: str,
    as_of_time: datetime,
) -> str:
    return f"{trade_date:%Y%m%d}-{ts_code}-{feature_set}-{as_of_time:%H%M%S}"


def persist_position_with_provenance(
    store: DuckDBStore,
    position: PaperPosition,
    provenance: SignalProvenance,
    *,
    run_mode: RunMode = "live",
    run_id: str | None = None,
    param_payload: Mapping[str, object] | None = None,
) -> str:
    """唯一落库入口：事务内先写入场快照再写模拟仓，返回 snapshot_id。

    replay 落库必须带 run_id（整批 DELETE 清理重跑）；live 由 monitor 进程调用
    （盘中唯一写者），replay 由收盘后 CLI 调用（与 monitor 错峰）。
    """
    if run_mode == "replay" and not run_id:
        msg = "replay 落库必须提供 run_id（用于整批清理重跑）"
        raise ValueError(msg)

    feature_set = f"{provenance.strategy_name}_entry"
    snapshot_id = make_snapshot_id(
        position.trade_date, position.ts_code, feature_set, provenance.as_of_time
    )
    snapshot_df = pd.DataFrame([{
        "snapshot_id": snapshot_id,
        "ts_code": position.ts_code,
        "trade_date": position.trade_date,
        "as_of_time": provenance.as_of_time,
        "feature_set": feature_set,
        "lookback_days": provenance.lookback_days,
        "payload": json.dumps(
            provenance.raw_payload, ensure_ascii=False, default=_json_default
        ),
        "source": run_mode,
    }])

    row = position.model_dump()
    row.pop("risk_payload", None)
    row["param_payload"] = (
        json.dumps(param_payload, ensure_ascii=False, default=_json_default)
        if param_payload is not None
        else None
    )
    row["feature_snapshot_id"] = snapshot_id
    row["strategy_name"] = provenance.strategy_name
    row["signal_factors"] = provenance.signal_factors.to_json()
    row["run_mode"] = run_mode
    row["run_id"] = run_id
    position_df = pd.DataFrame([row])

    conn = store._conn
    conn.execute("BEGIN")
    try:
        store.upsert_intraday_feature_snapshot(snapshot_df)
        store.upsert_paper_position(position_df)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    logger.info(
        f"溯源落库 {position.position_id}: strategy={provenance.strategy_name} "
        f"run_mode={run_mode} run_id={run_id or '-'} snapshot={snapshot_id}"
    )
    return snapshot_id
