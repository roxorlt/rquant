# `src/rquant/vp/` — 三重锁相 VP 策略子包

规格是唯一权威：[`docs/vp-strategy/VP-STRATEGY-SPEC.md`](../../../docs/vp-strategy/VP-STRATEGY-SPEC.md)，
本目录对应其中的 §7（研究沙盒与权限边界）与 §9.1（目录规划）。

> **当前状态：纯新增，未接入任何生产调用链。** 没有 CLI 入口、没有 systemd unit、
> 没有 DuckDB 读写。合入 main 不改变任何既有行为。

## 两层权限边界

```
src/rquant/vp/
├── engine/     🔒 回测底座 · 研究沙盒只读
│   ├── config.py     参数总表（规格 §10.1）
│   ├── profile.py    分桶 + 算法 U 均摊 + POC/价值区快照（§3.3.1–§3.3.3）
│   ├── session.py    盘中 Session VP 增量累加器（§3.3.5）
│   ├── anchored.py   锚点选取 + Anchored VP（§3.3.5）
│   ├── composite.py  60 日滚动复合 VP（§3.3.5 / §8.2 增量、§2.3 除权重置）
│   └── lvn.py        HVN / LVN 检测（§3.3.3）
│
└── sandbox/    ✍️ 研究沙盒 · 仅以下 7 个文件可写
    ├── hypothesis.md        研究假设卡（每个实验一张，模板见 §7.1）
    ├── data_dictionary.md   本实验新增字段的登记（八列缺一不可，模板见 §2.1）
    ├── factor.py            因子/信号计算
    ├── test_factor.py       单元测试
    ├── config.yaml          策略参数（不含成交规则）
    ├── experiment_log.csv   所有实验，含失败
    └── audit_report.md      风险审计结果（清单见 §7.2）
```

`sandbox/__init__.py` 是打包脚手架，不属于那 7 个可写文件，沙盒工作不要改它。

## 规则（规格 §7 原文的工程落地）

1. **`engine/` 对研究沙盒只读。** 可以**提议**修改，但必须走人工 review；改动后
   **重跑全部历史实验**，并在 `experiment_log.csv` 的 `engine_version` 列标注引擎
   版本变更（版本号见 `engine/__init__.py` 的 `ENGINE_VERSION`）。
2. **任何一次「收益提升」的实验，先由审计角色回答：是不是成交规则被放松了？**
   `audit_report.md` 的清单每次实验后必跑。
3. **`config.yaml` 只放策略参数**（阈值、窗口长度）。成交价、手续费、滑点、涨跌停
   与停牌处理**永远不进** `config.yaml`——那是回测底座的一部分，放进参数文件就等于
   默许它被当成可调参数去优化。
4. **`va_pct` 固定 0.70**，`VPEngineConfig` 里有硬校验会拒绝其他取值（规格 §10.1
   把它列为「固定，行业标准，不调」）。

## 引擎的三条实现约束

- **价值区不重写。** POC 并列裁决与价值区连续扩展复用
  `rquant.volume_profile._poc_index` / `_value_area`——即 PR #140 修好的 G20 版本
  （从 POC 逐 bin 向量大一侧连续扩展至 70%），全仓只此一份实现。
- **分桶不写死。** 宽度 = `max(0.01, round(ref_price × bin_ratio, 2))`，`bin_ratio`
  由 `VPEngineConfig` 传入（规格 §3.3.1，0.002 待 P1 标定）。落桶用 `Decimal`，
  浮点除法在 0.02 这类非精确宽度上会掉到相邻桶。
- **分钟量分配固定算法 U**（均摊到 `[low, high]` 覆盖的所有 bin）。规格 §3.3.2 写死：
  P1 的 U/W/单点法对照实验做完之前，不得因为「W 回测收益更高」而切换——那是过拟合。
- **复合 VP 只能增量滚动**（规格 §8.2：全量 5400 × 60 × 240 ≈ 7.8 亿行）。
  `CompositeVP` 存每日直方图，加一天 / 挤一天都是直方图相加减；跨日的 bin 网格由
  窗口首日锚定并全程固定，`push_day` 收原始分钟集、用**本窗口的网格**现场分桶
  （不接受按别的参考价分好的单日 profile——二次量化的误差会在 60 天里累积歪 POC）。
