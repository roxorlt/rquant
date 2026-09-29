# 因子检验：冻结来源到不可变产物的执行桥

承接 `FactorEvaluationJobSpec`、专属冻结来源租约、历史适配器与已审查的内容寻址产物，实现一个窄的因子域执行桥。它只负责「核准输入 → 同源计算 → 封存结果」，供后续 Lab 持久任务适配器调用；不修改现有策略 `ResearchRunSpec`，也不让浏览器直接运行长计算。原型里的「运行检验」在持久任务、只读结果索引、Serving 与权限入口都接通前保持不可用。

**任务分级：高风险。** 此桥跨冻结来源准入和持久产物写入，错配或提前报告成功会污染后续研究结果。先冻结以下失败模型，再由独立审查者核对本 SPEC。

## 冻结失败模型

- **资产与信任边界**：已验证的 `FactorEvaluationJobSpec`、专属快照 binding、只读租约、历史来源回执、纯计算结果、私有产物根和最终 SHA 回执。元数据仓库只作为准入查询；计算只能经 `FactorReadLease` 读取已绑定的副本工件。调用方负责提供可信的时钟、固定配置路径及代码修订；Web 请求、任意 JSON 路径和策略 Lab 的通用 source stage 不属于这个边界。
- **失败路径**：已过期限仍开始或封存；spec 的定义版本与适配请求不符；快照消失、binding 换代或准入失败后回退普通数据库；租约内只读数据变化、事实缺失或计算异常后留下「成功」回执；并发同一 spec 重试产生不同产物却被当成同一次结果；产物目录不存在/被替换、写入中断或同名冲突；代码版本或来源摘要未纳入产物身份；把探索性回溯结论描述为当时已采集、可成交或正式研究结论。
- **不变量**：入口重新验证 spec，不信任调用方构造的冻结模型；起算前和封存前使用显式 UTC 感知时钟检查 deadline，逾期拒绝且不调用产物发布器。仅以 spec 的 `admission_request` 打开一次专属已准入租约，生命周期内调用 `assemble_historical_factor_research`；不得直接打开 DuckDB/Parquet、复用策略源或在失败后用未经准入的来源补算。产物发布只接受该次内存结果与 spec 的 `code_revision`，复用既有 `publish_factor_research_artifact` 的完整内容校验、限额、原子发布和重试规则。完成回执须明确绑定 `spec_sha256`、产物 SHA、结果 SHA、快照 ID/binding hash 和 `historical_retrospective`/`exploratory` 口径；返回前按 SHA 重读已封存产物并与本次结果、代码修订、来源回执核对。相同 spec 与同一冻结事实重试应复用相同产物；跨调用的 spec→产物唯一性须由后续持久 Lab 索引约束，本函数不声称具备全局 exactly-once。任何失败不返回完成回执，不删除已有可信产物。
- **排除项**：本片没有 Lab 队列、并行分片、暂停/取消、进度、任务幂等索引、Parquet/Serving、Web/React 和生产路径配置；不证明历史数据当时实际采集、开盘可成交、交易费用或策略收益；不创建生产数据库/目录，不触碰 Streamlit。函数是后续持久 Lab runner 的可复用域边界，不是直接暴露给网页的运行接口。

## 合同与实现

1. `src/rquant/factor/job_runner.py` 提供单一同步函数，输入为完整 `FactorEvaluationJobSpec`、满足 `FactorSnapshotMetadataStore` 的只读元数据、显式 `lake_root` 与已创建私有 `artifact_root`、显式感知时钟。只在因子域组合四个已审查部件，不把因子 spec 塞进策略专属 `ResearchRunSpec`、`SubmitJobCommand` 或 worker。
2. 返回冻结的有类型 `FactorEvaluationCompletion`，字段最少覆盖 spec/产物/结果摘要、快照与 binding 身份、来源模式、研究状态和完成时刻；它不是持久化 Lab 回执。产物读取使用 `load_factor_research_artifact(root, receipt.sha256)`，不拼接任意用户路径。若相同 spec 在本次计算前存在先前持久结果的约束，应由未来 Lab adapter 在调用前检查；本函数不得无索引地声称全局 exactly-once。
3. 对时间的检查只防止明知逾期仍起算或开始封存；长计算的中途强制超时由后续 Lab worker 管。所有异常保留精确失败原因给内部任务记录，不把原始路径、异常栈或内部 ID 放到用户页面。此片不新增公共 Web 合同或 Serving 表。

## 红测与验收

- 用现有小型合成冻结日线/复权/日历夹具走一次真实准入、适配、纯结果和产物往返；核对完成回执中的 spec、结果、来源和文件摘要，重试同 spec 返回同一产物身份。
- 先做失败用例：起算前/封存前过期；错定义版本；缺失/换代/损坏 binding；只读查询中途变更；适配异常；不可信或缺失产物根；同名不同内容；封存后重读不一致。每例都不返回完成回执，且不借普通 DuckDB/旧策略源兜底；不会删除已存在可信产物。确认只调用一次专属准入会话，关闭后不能再查询。
- 限定到新 runner 与直接依赖的聚焦回归、Ruff、diff；用可注入的时钟和真实临时工件，不碰生产数据。独立 reviewer 先审 SPEC，最终候选一次覆盖冻结范围。发现阻断按项目高风险最多三轮定向修复；不扩张到 Lab 旧引擎审计。

后续顺序：持久 Lab `factor_eval` 任务接线与同 spec 结果索引 → 有界图表表及 Serving 同代发布 → 只读结果页 → 已认证提交按钮 → 增量跟踪。每一片只在其真实能力验证后更新页面和退出台账。
