# 模拟盘 B 信号溯源标记 设计文档

**目标：** 未来任何策略在盘中做模拟买入时，B 标记必须带上「是哪个策略、哪些因子、当时取值多少」的完整溯源，方便定期按策略/因子组合切片复盘胜率与收益。

**核心原则：** replay 回测与未来 live 模拟买入写**同一套溯源结构**（同一个组装函数、同一份因子 spec），否则「回测说好、实盘说坏」时无法归因是策略问题还是口径问题。

---

## 1. 现状盘点（设计前提，已核实）

| 事实 | 说明 |
|------|------|
| `paper_position` 已有 `entry_signal VARCHAR`（触发器名）、`candidate_id VARCHAR`（参数组）、`param_payload JSON`、`feature_snapshot_id VARCHAR` | 字段齐但 **`upsert_paper_position` 目前只有单测调用，没有生产写入方**——live monitor 只写 monitor_event，不开模拟仓 |
| `intraday_feature_snapshot` 表已建（snapshot_id PK / ts_code / trade_date / as_of_time / feature_set / lookback_days / payload JSON / source） | **无 writer**，是现成的溯源快照容器 |
| replay 路径（`auction_gap_strategy._find_auction_gap_entry`、`minute_replay._find_entry_snapshot`）已经在入场时刻组装 `signal_features` dict | 但只塞进 `PaperPosition.risk_payload` → 输出研究 DataFrame，不落 paper_position 表 |
| `monitor.check_attack_signals` 触发 `attack_*` 事件 | 是未来 live 模拟买入的挂载点（roadmap 阶段四） |

也就是说：**溯源不是给现有写入路径打补丁，而是在第一个生产写入方出现之前把结构定对**——成本最低的时机就是现在。

## 2. Schema 设计

### 2.1 决策：paper_position 加两列 + 复用 intraday_feature_snapshot（混合方案）

单独二选一都不够好：

- 只复用 `intraday_feature_snapshot`：复盘按因子切片每次都要 join + 解全量 JSON，而且 payload 是「当时所有可见特征」（几十个键），复盘只关心「策略判定用到的那几个因子命中与否」，语义混在一起。
- 只在 paper_position 加大 JSON：审计需要的完整现场（历史基准中位数、市场温度原始值、竞价原始行……）会把 position 行撑肥，且退出时刻的第二份快照没地方放。

因此**职责拆开**：

| 层 | 放哪 | 内容 | 用途 |
|----|------|------|------|
| 策略归属 | `paper_position.strategy_name`（新列） | 策略家族名 | GROUP BY 第一维 |
| 判定摘要 | `paper_position.signal_factors`（新列，JSON） | 入场判定因子的命中矩阵（每因子 value/hit/threshold），小而精 | 因子组合切片 SQL 直查 |
| 完整现场 | `intraday_feature_snapshot.payload` | 触发时刻全量可见特征（含未参与判定的观察因子） | 事后再归因 / 新因子回填验证 |
| 关联 | `paper_position.feature_snapshot_id`（已有列，启用） | 指向入场快照 | join 取现场 |

三层命名语义（避免和现有字段打架）：

```
strategy_name   策略家族      如 nshape_attack / auction_gap_minute / nshape_minute
entry_signal    触发器名(已有) 如 attack_break_high / auction_gap_vwap_push / minute_amount_surge
candidate_id    参数版本(已有) 如 baseline / grid_042
```

### 2.2 DDL 草案 + 迁移语句

`src/rquant/storage/schema.py` 追加（沿用现有 `ADD COLUMN IF NOT EXISTS` migration 模式，进 `ALL_DDL`）：

```sql
-- PAPER_POSITION_STRATEGY_NAME_MIGRATION_DDL
ALTER TABLE paper_position ADD COLUMN IF NOT EXISTS strategy_name VARCHAR;

-- PAPER_POSITION_SIGNAL_FACTORS_MIGRATION_DDL
ALTER TABLE paper_position ADD COLUMN IF NOT EXISTS signal_factors JSON;

-- PAPER_POSITION_RUN_MODE_MIGRATION_DDL（replay/live 同表共存，见 3.3）
ALTER TABLE paper_position ADD COLUMN IF NOT EXISTS run_mode VARCHAR DEFAULT 'live';
ALTER TABLE paper_position ADD COLUMN IF NOT EXISTS run_id VARCHAR;
```

历史行回填（一次性脚本，表目前无生产数据、实际是空跑保险）：

```sql
UPDATE paper_position SET strategy_name = CASE
    WHEN entry_signal LIKE 'auction_gap%' THEN 'auction_gap_minute'
    WHEN entry_signal LIKE 'attack%'      THEN 'nshape_attack'
    WHEN entry_signal LIKE 'minute_%'     THEN 'nshape_minute'
    ELSE 'unknown'
END
WHERE strategy_name IS NULL;
```

### 2.3 signal_factors JSON 格式

与全景页（文档一）的 `FactorSpec` 清单共用同一份因子定义，键名 = spec name：

```json
{
  "schema_version": 1,
  "factor_set": "auction_gap_v1",
  "factors": {
    "auction_vol_ratio_5d":          {"value": 1.82,  "hit": true,  "op": ">=", "threshold": 0.15},
    "gap_pct_close":                 {"value": 3.4,   "hit": true,  "op": ">",  "threshold": 0.0},
    "vwap_position":                 {"value": 1.004, "hit": true,  "op": ">=", "threshold": 1.0},
    "limit_progress":                {"value": 0.35,  "hit": true,  "op": ">=", "threshold": 0.2},
    "support_ok":                    {"value": 1,     "hit": true},
    "rel_amount_same_minute_20d":    {"value": 3.1,   "hit": true,  "op": ">=", "threshold": 2.0},
    "amount_accel_5m":               {"value": 2.4,   "hit": null},
    "market_temperature_high60":     {"value": 12.4,  "hit": true,  "op": ">=", "threshold": 8.0, "basis": "prev_day"}
  },
  "hit_count": 7,
  "evaluated_count": 7
}
```

约定：`hit: null` 表示观察因子（记录取值但不参与判定）；无数据的因子直接不出现在 factors 里（区别于「未命中」）。`schema_version` 留给未来结构演进。

`intraday_feature_snapshot` 侧约定：`snapshot_id = "{YYYYMMDD}-{ts_code}-{feature_set}-{HHMMSS}"`，`feature_set` 取 `{strategy_name}_entry` / `{strategy_name}_exit`，`source` 取 `live` / `replay`，`payload` 存组装因子时的全部中间量（历史基准中位数、竞价原始行、市场温度原始值等）。

## 3. 写入路径

### 3.1 统一组装函数（新模块 `src/rquant/signal_provenance.py`）

```python
class FactorReading(BaseModel):
    value: float | int | None
    hit: bool | None
    op: str | None = None
    threshold: float | None = None
    basis: str | None = None

class SignalProvenance(BaseModel):
    strategy_name: str
    factor_set: str
    factors: dict[str, FactorReading]
    raw_payload: dict[str, object]      # 进 intraday_feature_snapshot.payload
    as_of_time: datetime

def build_provenance(...) -> SignalProvenance      # 从因子原始 dict + FactorSpec 清单组装
def persist_position_with_provenance(
    store, position, provenance, *, run_mode, run_id,
) -> None                                          # 事务内先写 snapshot 再写 position
```

`persist_position_with_provenance` 是唯一落库入口：先 `upsert_intraday_feature_snapshot`，拿 snapshot_id 填 `feature_snapshot_id`，再 `upsert_paper_position`（补 strategy_name/signal_factors/run_mode/run_id 列）。谁写库由调用方决定——**live 场景只能是 monitor 进程**（唯一盘中写者），replay 场景是收盘后/研究时段的 CLI 进程（与 monitor 错峰，遵守写者串行约定）。

### 3.2 live 路径：monitor 的 check_attack_signals → 模拟买入

roadmap 阶段四落地时，`run_monitor` 主循环里在第一条 `attack_*` 事件后开模拟仓，因子组装来源：

| 因子 | live 数据来源 |
|------|-------------|
| attack 信号组（open_strength/strong_carry/break_high/near_limit） | `check_attack_signals` 返回的 events + item 的 t_close/t_high/limit_up_price_next |
| VWAP 位置 / 涨停进度 / 承接 | quote（akshare 快照含累计 amount/volume/最高/最低/今开）或 `IntradayMinuteQuoteProvider` 的分钟缓存累计 |
| 竞价强度组 | auction_bar 当日 `open_realtime` 行（9:26 后已可回补；monitor 开盘前预取一次缓存） |
| 同分钟相对放量 / 加速 | `stock_features.build_intraday_relative_volume_features`（watchlist 票有历史 minute_bar，基准可算） |
| 市场温度门控 | market_sentiment_daily **昨日**行（开盘前预取，无未来函数） |

关键点：这些输入 monitor 进程当下全部持有（写库 store + 分钟缓存 + quote），无需新增外部请求；组装耗时单票 ~10ms 量级，对 5s 主循环无压力。

### 3.3 replay 路径：与实盘同构

- `auction_gap_strategy._find_auction_gap_entry` 与 `minute_replay._find_entry_snapshot` 现有的 `signal_features` dict 改为先过 `build_provenance`（同一份 FactorSpec），再照旧塞 risk_payload 供研究 DataFrame 使用——**因子键名从此与 live 完全一致**。
- replay 落库默认关闭（反复实验会刷表），CLI 加 `--persist-positions` 开关；落库时 `run_mode='replay'`、`run_id` = Strategy Lab run id（`strategy_lab_runs` 已有 run 概念，直接引用），可整批 `DELETE WHERE run_id = ?` 清理重跑。
- 复盘查询默认 `WHERE run_mode = 'live'`，加 `--include-replay` 才把回测仓拉进来对比。

## 4. 复盘查询设计

### 4.1 SQL 示例（DuckDB JSON 函数直查 signal_factors）

按策略 × 参数版本的基础面板：

```sql
SELECT strategy_name,
       candidate_id,
       COUNT(*)                                             AS trades,
       ROUND(AVG(pnl_pct), 2)                               AS mean_ret,
       ROUND(MEDIAN(pnl_pct), 2)                            AS median_ret,
       ROUND(100.0 * SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                            AS win_rate_pct,
       ROUND(AVG(max_drawdown_pct), 2)                      AS avg_mdd_pct,
       ROUND(MIN(pnl_pct), 2)                               AS worst_ret
FROM paper_position
WHERE status = 'closed'
  AND run_mode = 'live'
  AND trade_date BETWEEN ? AND ?
GROUP BY 1, 2
ORDER BY trades DESC;
```

按因子命中组合切片（示例：涨停进度 × 相对放量两因子四象限）：

```sql
SELECT json_extract_string(signal_factors, '$.factors.limit_progress.hit')            AS f_progress,
       json_extract_string(signal_factors, '$.factors.rel_amount_same_minute_20d.hit') AS f_relvol,
       COUNT(*)                        AS trades,
       ROUND(AVG(pnl_pct), 2)          AS mean_ret,
       ROUND(100.0 * SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) / COUNT(*), 1)
                                       AS win_rate_pct,
       ROUND(AVG(max_drawdown_pct), 2) AS avg_mdd_pct
FROM paper_position
WHERE status = 'closed'
  AND run_mode = 'live'
  AND strategy_name = 'auction_gap_minute'
  AND trade_date BETWEEN ? AND ?
GROUP BY 1, 2
ORDER BY 1, 2;
```

命中数分布（信号「浓度」和收益的关系）：

```sql
SELECT CAST(json_extract(signal_factors, '$.hit_count') AS INTEGER) AS hits,
       COUNT(*) AS trades,
       ROUND(AVG(pnl_pct), 2) AS mean_ret,
       ROUND(100.0 * SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS win_rate_pct
FROM paper_position
WHERE status = 'closed' AND run_mode = 'live' AND trade_date BETWEEN ? AND ?
GROUP BY 1 ORDER BY 1;
```

回撤口径说明：per-trade 用已有 `max_drawdown_pct` 聚合；组合级 equity 回撤按入场时间排序在 CLI 内用 pandas 累乘算（个人量级足够，不进 SQL）。

### 4.2 CLI 输出形态：`rquant paper-review --start 2026-07-01 --end 2026-07-31`

```
== 模拟盘复盘 2026-07-01 ~ 2026-07-31（live，closed 仓）==

[按策略]
策略                参数组     笔数  胜率    均收益   中位数   均回撤   最差
auction_gap_minute  baseline   34   38.2%  +0.42%  -0.31%  -2.1%   -5.8%
nshape_attack       baseline   12   50.0%  +1.05%  +0.44%  -1.8%   -3.2%

[因子四象限 · auction_gap_minute]
limit_progress  rel_vol_20d   笔数  胜率    均收益
hit             hit           18   44.4%  +0.93%
hit             miss          9    33.3%  -0.12%
miss            hit           5    20.0%  -1.05%
miss            miss          2    0.0%   -2.40%

[命中数分布]
hits=4: 6 笔 / 胜率 16.7% / -1.2%   hits=6: 15 笔 / 40.0% / +0.5%   hits=7: 13 笔 / 46.2% / +1.1%

[组合曲线] 累计收益 +3.8% / 组合最大回撤 -4.2% / 未平仓 3 笔（不计入）
```

参数：`--strategy` 过滤策略、`--by strategy|candidate|factor|hits` 选切片维度、`--factors a,b` 指定四象限因子对、`--include-replay --run-id xxx` 拉回测仓对比、`--format md` 输出 Markdown（贴复盘笔记用）。

## 5. 分期交付

### P0 — schema + 溯源写入（前置：无，随时可做）

| 改动文件 | 内容 |
|---------|------|
| `src/rquant/storage/schema.py` | 4 条 migration DDL 常量 + 进 ALL_DDL |
| `src/rquant/signal_provenance.py`（新） | FactorReading/SignalProvenance 模型、build_provenance、persist_position_with_provenance |
| `src/rquant/storage/duckdb.py` | upsert_paper_position 列清单补 4 列 |
| `src/rquant/auction_gap_strategy.py` / `src/rquant/minute_replay.py` | signal_features 改走 build_provenance（键名统一），replay CLI 加 `--persist-positions` |
| `tests/unit/test_signal_provenance.py`（新） | 组装、落库、replay/live 键名一致性对拍 |

工作量：**1.5-2 天**。

### P1 — 复盘 CLI

| 改动文件 | 内容 |
|---------|------|
| `src/rquant/paper_review.py`（新） | 查询 + 聚合 + 组合曲线（只读副本连接） |
| `src/rquant/cli.py` | `paper-review` 子命令 |
| `tests/unit/test_paper_review.py`（新） | 切片 SQL 与四象限聚合单测（内存库造数） |

工作量：**1 天**。

### P2 — Lab「模拟盘复盘」页签

Strategy Lab 新增页签：策略面板表、因子四象限热力、单仓下钻（signal_factors 命中矩阵 + join intraday_feature_snapshot 展示完整现场）。复用 paper_review 的查询层，页面零新查询逻辑。

工作量：**1 天**。

### 依赖关系与顺序

P0 与 roadmap 阶段四（live 模拟买入接入 monitor）解耦：P0 先把结构和 replay 路径立住，live 接入时只是多一个 `persist_position_with_provenance` 调用方。P1/P2 在 replay 数据（`--persist-positions` 跑几个 run）上即可验收，不必等 live。

## 6. 风险与决策项

1. **DuckDB 写锁**：replay `--persist-positions` 必须在非盘中时段跑（monitor 持写锁期间任何新写连接失败）；CLI 里加盘中时段警告。
2. **因子 spec 漂移**：阈值调整后新旧仓 signal_factors 不可比——`factor_set` 版本号（`auction_gap_v1` → `v2`）随阈值变更递增，复盘默认按 factor_set 分组，跨版本对比显式声明。
3. **JSON 查询性能**：个人量级（每天几十仓）完全无压力；若未来到十万行级再考虑把高频切片因子物化成列，现在不预优化。
4. **不要用复盘结论直接改 live 阈值**：切片统计是归因线索，参数晋级仍必须走 walk-forward（对齐 2026-06-25 计划「No Hand-Written Strategy Conclusions」原则）。
