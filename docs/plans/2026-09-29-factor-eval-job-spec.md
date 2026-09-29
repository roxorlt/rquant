# 因子检验：独立的 Lab 作业输入合同

v2 C3.2 要求 `factor_eval` 作为研究面任务运行。现有 `ResearchRunSpec` 强制策略名、成交成本、特征执行合同与实验身份；将因子检验塞进去会制造没有真实含义的字段。本片只建立因子专用、可复算的**纯作业规格**，后续才接 PageControl 提交、队列、runner、产物回执、Serving 和页面。

**任务分级**：普通任务。只新增纯 Pydantic 合同与聚焦测试，不修改现有策略 Lab 合同、持久任务或生产基础设施。风险是来源范围、定义版本和执行口径不匹配，故最终候选做一次独立审查。

## 合同

1. `FactorEvaluationJobSpec` 固定 `job_type="factor_eval"`、`schema_version=1`、40 位小写 `code_revision`、`FactorSnapshotAdmissionRequest`、`HistoricalFactorAdapterRequest`、定义版本内容 SHA-256 和有时区的任务截止时刻。完整 `FactorDefinition` 已在 adapter request 内，不复制第二份；定义内容 SHA 使用因子注册表同一规范 JSON 算法核对，不信任调用方声称的版本摘要。
2. 准入起止日必须分别精确等于 adapter 查询起止日，两个请求都只接受 `historical_retrospective`；截止时刻应晚于研究 `as_of`，日期与股票池限制沿用现有 adapter 类型。宽于实际产物的准入范围也会改变规格身份，不能借用旧产物完成。结果永远是探索性回溯研究诊断，不在规格中放策略、成交成本、伪实验或实际 PIT/可交易声称。
3. 暴露规范规格摘要，包含完整定义、股票池、评估日、持有交易日、来源快照/binding、`as_of`、代码版本和截止时刻；同一逻辑内容序列化与摘要稳定，任一关键项变化摘要改变。是否有可用快照、真实历史成分和收益成熟由 runner 在已核验会话中判断，纯规格不猜。
4. 模型输入有界且拒绝 extra。调用方不得传文件路径、SQL、任意 Python 代码、`strategy_execution` 或 `execution_costs`。作业规格不执行 I/O，不凭它授予生产或 Web 写权限。

## 验收

先用现有小型因子定义/历史适配请求夹具写红测：正确组合可生成稳定摘要；换版本内容、快照/binding、股票池、评估日、holding sessions、`as_of` 或代码修订会改变摘要；准入范围窄于或宽于查询范围、截止早于研究时刻、假定义 SHA、额外策略字段都拒绝。随后实现纯合同；聚焦测试、Ruff、diff 通过后提交。测试清单和进度由累计分支统一更新。

后续单独冻结高风险任务存储与 runner 的失败模型：幂等提交、租约恢复、来源准入、产物已封存但回执未写时的恢复。此片不把作业类型加入现有策略 `ResearchJobType` 枚举或伪称已有可点击运行入口。
