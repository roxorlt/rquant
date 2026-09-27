# 日线质量规则纯计算 Implementation Plan

**Goal:** 为 v2 C1.2 的日线数据审计补齐可复用的价格越界、零成交量和空值比例判断，并对证据不足给出明确的「无法判断」。这是审计计算的一段，不等于数据中心页面或定时任务已上线。

**Architecture:** 研究任务在已核验的同一快照中采集一日的日线、涨跌停价、停牌状态与空值计数，构造有界 Pydantic 输入；纯函数只根据这些证据产生稳定的异常描述和未覆盖计数。现有 `DataAuditRun` / `DataQualityIssue` 负责后续运行记录和问题生命周期，Serving 只发布最终只读摘要，网页不碰主库或副本。

**Tech Stack:** Python 3.11+、Pydantic、Decimal、pytest；后续接现有 DuckDB 只读研究输入、Lab 命令与 Serving 发布链。

## 范围、级别与验收

本纯计算增量是**普通任务**：只新增领域函数和聚焦测试，不修改生产数据、共享存储契约或发布流程。可验证行为：一天最多 10,000 只股票；输入按股票代码唯一；价格、成交量、涨跌停价为有限且非负的 Decimal；收盘价只有在同代的**权威**涨跌停上下界都存在时才判断越界，上市首日等无上下界情况计入未判定；成交量为零仅在权威停牌事实明确为「未停牌」时标为待核查，状态未知计入未判定；空值比例只针对显式指定的字段和阈值计算。异常输出有规则、日期、股票或字段、证据值，不把「缺证据」当作「正常」。规则不直接写库、不在网页请求中扫大表。

## Task 1：领域契约与价格/成交量规则

**Files:**

- Create: `src/rquant/data_audit_quality.py`
- Test: `tests/unit/test_data_audit_quality.py`

1. 先写聚焦失败用例：同一日两只股票，其中一只收盘价高于权威涨停价、一只在上市例外无界；分别得到一条异常和一条未判定。再覆盖下限、合法边界、零成交量且明确未停牌、停牌与状态未知、重复股票代码、无效数值和超过 10,000 行。
2. 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/unit/test_data_audit_quality.py -q` 确认新增用例先失败；`.env` 禁用并用临时离线环境变量满足仓库夹具。
3. 最小实现：一个冻结的 Pydantic 请求封装 `snapshot_id`、`trade_date`、同代证据的有界行；一个纯函数返回 `issues` 和按原因计数的 `unassessed`。规则 ID 稳定，股票异常排序稳定；不在此处复制 `DataAuditRun` 的存储职责。
4. 同一测试转绿，运行 Ruff 和 `git diff --check`。

## Task 2：显式字段空值比例

**Files:**

- Modify: `src/rquant/data_audit_quality.py`
- Test: `tests/unit/test_data_audit_quality.py`

1. 先写失败用例：字段的 `null_rows/observed_rows` 超过显式阈值时有一条异常，等于阈值不报；样本数为零返回未判定；重复字段、分子大于分母、阈值越界拒绝。比例用整数交叉相乘，避免浮点边界误差。
2. 实现显式字段统计输入和稳定排序输出；只接受上游已经限定日期、数据集和快照身份的计数，不从价格样本推算整张表的空值率。
3. 跑本文件聚焦测试、已有 `tests/unit/test_data_audit_coverage.py`，确认覆盖率与新质量规则可独立调用；自检 diff 后提交 `cdx/` 工作树候选。

## 后续接线边界（不属于此纯计算增量）

1. **同源采集**：研究面从一次固定且已核验的只读快照采集交易日历、日线、权威涨跌停价和停牌事实；快照换代就废弃本次结果。日线整日覆盖率复用 `src/rquant/data_audit_coverage.py`。大表按日期/批次离线读取并实测内存和耗时；不由网页或常驻服务扫描，也不混读主库与副本。
2. **审计任务与持久化**：现有 `src/rquant/data_quality.py`、`src/rquant/data_metadata.py` 和 `rquant data-audit` 已有运行/问题契约，但当前 Lab `ResearchRunSpec` 只覆盖策略回放等研究工作。新增 `data_audit` 命令须独立设计有类型的任务契约或显式扩展其联合类型，不能伪装成策略任务。命令幂等、审计报告产物、问题关闭与当前快照身份另冻结高风险不变量。运行时间以日线真正完成和发布证据为准，不把计划时刻 17:50 当作数据已齐。
3. **发布与页面**：`dataset_health`、`dataset_coverage_monthly`、问题摘要和产物索引需走一次原子 Serving 数据代，API 再提供 `/data/health`、`/data/coverage`、`/data/issues`；总览/健康页复用相同数据健康状态。Serving 表契约、Lab 命令、生产调度和权限属于高风险后续范围，必须在各自实现前冻结失败模型并按项目发布门禁验证。

本计划的纯计算验收只证明规则行为；C1.2 继续标「部分」，直到真实任务、报告发布、API、页面、受控手动运行及生产数据链均可用。
