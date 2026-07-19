# 空停牌时段修复实施计划

**目标：** 修正 Tushare 空停牌时段语义，在不降低 Stage 1 门槛的前提下重新生成三策略正式
证据。

**架构：** 在 `rquant.suspension` 统一归一化源事实，通过一份限定范围的 SQL 关系分类
`full_day / conflict / unknown`，历史全量刷新在一个 DuckDB 事务内发布。源事实或代码提交
改变后，为三种正式策略生成新的不可变制品，不修改或复用旧证据。

**技术栈：** Python 3.11+、pandas、Pydantic、DuckDB、pytest、Tushare `suspend_d` 和既有
受控生产发布器。

---

### 任务 1：证明并修正归一化

**文件：**
- 修改：`tests/unit/test_tushare_suspension.py`
- 修改：`src/rquant/suspension.py`

**步骤 1：** 先修改测试，使 `S + None` 期望 `full_day`，`R + None` 仍期望 `unknown`。

**步骤 2：** 用 `PYTHONPATH=src` 运行精确测试，确认先出现 `unknown != full_day`。

**步骤 3：** 只有先证明 `suspend_type == "S"`，空时段才返回 `full_day`。

**步骤 4：** 运行停牌、回补 manifest 和成长板策略测试，要求全部通过。

### 任务 2：统一证据并保证整段刷新原子性

**文件：**
- 新增：`src/rquant/suspension_evidence.py`
- 修改：`src/rquant/storage/duckdb.py`
- 修改：`src/rquant/data_quality.py`
- 修改：`src/rquant/growth_eligibility.py`
- 修改：`src/rquant/suspension.py`
- 修改相关停牌、规划器、执行器和质量审计测试

**步骤 1：** 证明同日日线或非零分钟成交会阻断全天停牌豁免，零量零额分钟占位不会。

**步骤 2：** 证明规划器、执行器、成长板资格和数据审计使用同一分类。

**步骤 3：** 在第二份快照写入前注入失败，证明事件表和覆盖表都保留完整旧批次。

**步骤 4：** 在分钟证据查询前限定共享 SQL 范围，再用 `transaction_mode="existing"` 将所有
已取回快照放进一个外层事务发布。

### 任务 3：版本与验证

**文件：**
- 修改：`pyproject.toml`
- 修改：`src/rquant/__init__.py`
- 修改：`CHANGELOG.md`
- 新增：`docs/plans/2026-07-19-suspension-empty-timing-design.md`
- 新增：`scripts/rollout-v0.25.1-stage1.sh`

**步骤 1：** 版本设为 `0.25.1`，记录源语义和生产迁移。

**步骤 2：** 运行 Ruff、`uv lock --check`、`git diff --check`、聚焦测试和完整测试。

**步骤 3：** 独立复审后提交、推送并创建 PR，只有 Python 3.11/3.12 CI 全绿才合并。

### 任务 4：部署并重建全部正式证据

**步骤 1：** 在合并后的精确 main SHA 创建 annotated tag `v0.25.1`。

**步骤 2：** 对同一精确 tag 先预演再部署：

```bash
bash scripts/deploy-production.sh --target v0.25.1 --dry-run
bash scripts/deploy-production.sh --target v0.25.1
```

**步骤 3：** 确认生产已经位于精确 tag/SHA 后，在交易保护窗口外运行唯一初始化脚本：

```bash
cd /home/lighthouse/rquant
RQUANT_STAGE1_EXPECTED_SHA=<v0.25.1合并后的40位SHA> \
  bash scripts/rollout-v0.25.1-stage1.sh
```

脚本固定执行以下链路，不再手工拼接中间变量：

1. 工作日 09:15 到 15:10（含两个端点）拒绝执行；
2. 记录原本 active 的 timers，停止相关 timer 和写服务，退出时只恢复原状态；
3. 停写后从主库生成、验证并保留操作前快照；
4. 从权威 SSE 日历和已有日线动态捕获 `REFRESH_END`；
5. `suspension-backfill --full-refresh --dry-run` 通过后，再原子刷新整段停牌事实；
6. 依次为 `n_shape`、`growth_board_surge`、`auction_gap` 创建移动截止日 manifest，执行
   回补、状态验收、分钟研究湖预演/修复、snapshot 预演/apply 和不可变 binding；
7. 运行一次覆盖完整区间且 P0=0 的 Stage 1 审计，再对三策略运行固定正式回放；
8. 保存操作后主库快照、刷新只读副本，核对主副摘要、schema v10、研究 authority、
   preflight、服务和 timers。

2026-07-19 部署前只读探针确认 Stage 1 区间已有 73/73 个竞价研究分区，故本次不预设任何
竞价修复日期。若将来 manifest 发现真实竞价缺口，`backfill-plan` 会失败关闭；应另行按
`research-repair-auction` 的计划 ID 流程修复，不能在 rollout 中猜日期或跳过门槛。

**步骤 4：** 成功后把日志里的精确 manifest ID、分钟修复 plan ID、snapshot ID、binding
hash、audit run ID、result hash、备份位置和回滚命令写入 `DEPLOY.md`，经 PR 合并。任何
中间步骤失败都停在首个失败点，原 timers 由 trap 恢复，旧 manifest 和旧正式证据保持不变。
