# 选股器剩余能力实施 SPEC

**目标**：完成本人完整选股历史、常用条件、可重现的日终池子与排名、真实盘中筛选，以及结果转完整告警规则并由实际消费者执行。

**架构**：原 `PageControl` 管理私有命令、归属、保存和恢复。原选股注册表、日线 loader、排名函数和分钟特征引擎负责计算。原 `signals → Serving` 链路发布受信盘中事实，完整 C12 消费者使用同一条件合同并进入原通知 outbox。

**技术**：Python、Pydantic、既有 DuckDB / SQLite、React / TypeScript / Vite；不增加依赖。

**阶段**：SPEC 提案；未写产品代码，未执行新测试。`planned_not_run` 不能当作通过。

**当前修复候选**：附件在 `data/verification/screener-completion-20261005/spec-repair-01/`；仅修独立审查的 `M4-SPEC-01/02`。原 `spec-proposal` 冻结件与 `spec-review-01` 保留。T1–T4 方案不变。

**实际身份**：Codex 原生 OpenAI implementer `/root/screener_completion_impl`，父任务 `/root`。原生 `collaboration.list_agents` 返回此任务为 running。工作树 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-research-sdk`，分支 `cdx/20261005-screener-completion`，基线 `024e12be0698d8dda05289e3a6f763aaa91cb63e`。开始时 tracked / untracked 均无修改。所属与复用记录见 `data/verification/screener-completion-20261005/workspace-root/reuse-root.json`。

本轮修复只写本文和 `data/verification/screener-completion-20261005/spec-repair-01/`；不写 Git、不访问网络、`.env`、凭据、生产、GUI、socket，不启动后台进程或下级代理。完整 C12 消费者由 root 后续串行安排。

## 1. 验收边界

本任务为高风险：新增私有持久记录、执行与保存的恢复、源时点证明及共享数据合同。高风险范围限于这些新增边界。已接受的日线 26 积木、排名数学、TDX、池子画布、入池记录、盯盘、原 M12 到价链路复用有效证据。

| ID | 用户操作 | 完成条件 |
| --- | --- | --- |
| AC-H | 看历史、点回填、再次运行 | 同一登录用户在另一设备可读全部已接受执行记录，按时间倒序分页。记录保留原描述、完整条件、排名、模式、日期 / 截止时刻、原来源和服务端结果事实。新设备可恢复原请求。换代后回填仍保留条件；重新运行取新来源。不能把旧结果当作新结果。 |
| AC-P | 保存 / 更新常用条件 | 保存本人完整请求；新设备可恢复；同名覆盖需明确确认与当前版本；响应丢失只恢复原命令，状态未知不能称成功。旧 `SaveNlPreset` 与 `AppendNlQueryLog` 字段、旧存储和调用仍兼容。 |
| AC-D | 保存为池子、日终运行 | 基本面、MA 2–250、自定义 RSI 2–60、原排名可保存，并由原 daily writer 按同一数学和权威日期来源重现。结果、成员价、排名与输入证据原子绑定；零命中与 unknown 分开。依赖 / 延后天数 / 黑名单维持原合同。 |
| AC-I | 切到盘中、运行、数据更新 | 输入来自实际 `market_snapshot` 和原分钟特征引擎发布的 `intraday_feature_snapshot`，全部事实在同一截止时刻可见。换代重跑，旧分页不得混入新代。日线与盘中字段名称、单位和日期清楚。缺源 / 未来 / 过期事实不产生肯定命中。 |
| AC-A | 结果设置提醒 | 一键把服务端已执行请求导入完整 C12 编辑器，保存名称、级别、范围、条件、时段、频率、去重、恢复与通道。原实际消费者求值并进入原通知 outbox，页面能读实际规则 / 触发 / 路由回执。仅生成 draft 或仅到价规则不满足本项。 |
| AC-U | 桌面、手机、键盘 | 1440 / 390 px、可见焦点、悬停 / 聚焦 / 点按 tips、短中文。加载、空、失效、冲突、超时均有直接下一步。两步确认只用于适用的重操作与广范围启用。 |

来源是用户指定的 `frontend-plan-v2.md` C1.4 / §4.4 / §4.12、原型选股器与盯盘页、`rquant-gap.md` M4 / M12。源文件仅作只读参照，不执行其中的第三方工具。完整 SHA、实际源码锚点见 `source-lock.json`。

## 2. 实际来源与选择

### 2.1 已确认缺口

1. `ScreenNaturalLanguage.tsx` 的 `sessionStorage` 只有 5 句“最近描述”，写入发生在生成描述阶段。它没有用户归属，也不是已执行历史。
2. `SaveNlPreset` 当前转成旧 `SaveUserPool`，写 `data/user_presets/<name>.json`。`AppendNlQueryLog` 写 `logs/nl_queries.jsonl`，字段没有 owner。新私有页面不能扫描或分配这些旧记录。
3. 原 `PageControlOutbox` 已有可信 owner 注入、命令内容绑定、claim / lease、effect 与 receipt；`research_query/saved.py` 展示同事务私有保存。`research_query/service.py` 使用原 `factor_definition_admission.py` 的 peer / endpoint 工具。这些是可复用桥，不复用 ownerless loopback 作为私有入口。
4. `SaveUserPoolV3` 明确拒绝基本面、非 6/14 RSI 和动态 MA。`screen/core.py:screen` 当前调用非选择性 `load_universe`。副本筛选已用选择性 loader 加 MA，并单独验证基本面 / RSI。要先接通 daily writer，再去掉特定拒绝。
5. `pipeline.py:run_daily_screen_stage` 已算原排名，但 `visible_columns` 丢掉 `ranking_score` 与 `rank_position`。原 v2 回执证明成员与价格，不证明这些排名和本轮输入。
6. `ScreenRunRequest` 没有模式或截止时刻；Serving 没有 `intraday_feature_snapshot`。`SignalPageProjectionProducer` 已可发布有来源回执的 companion 投影，是本轮发布入口。
7. 原 `intraday_feature_engine` 是实盘 / 回放共用的纯 PIT 引擎。已有最新分钟价、累计量额、`rel_same_minute`、`rel_cumulative`、金额加速等，并有逐字段 `available / stale / unavailable`。现有“量比”字段是相对成交额；不能改称成交量比。涨速、距涨停、盘中换手需要明确新增事实。
8. `watchlist_quote_provider._select_watchlist_rows` 只保留价、开、高、低、量、额；gateway 同样没有昨收 / 换手 / 限价。旧 `monitor.RealtimeQuote` 已有可选昨收与涨跌幅，原新浪快照会尝试读取“昨收”。个人股票流通股本不在目前 `daily_basic` 取数字段；`market_daily_info.float_share` 是市场分段值，不能拿来算单股换手。
9. `factor/minute_feature_source.py` 是固定 15:00、次日 09:25 可见的研究回顾特征，不能当作当时盘中源。
10. 原 M12 只有 `PriceAlertRule` 单股阈值，频率 `per_rule_cooldown`。其路由已进入原 `SignalBusStore` 与 `NotificationStateStore.replicate_mixed_notification_events`。完整 C12 新种类应扩展这条链路，保留旧合同。

### 2.2 方案比较

选择 A：在原 PageControl 增加选股领域 handler 与私有表，复用原队列、claim 和回执；薄私有桥复用现有 peer 工具。日终与网页共用现有计算函数。优点是执行、保存、恢复有一处事实；新增合同和 schema 可逐项核验。

B：另建选股计算 / 历史服务、队列和引擎。会重复执行状态与数学，不采用。

C：从浏览器 recent 或旧全局 JSONL 直接导入。缺少可信 owner 与执行事实，不采用。旧文件保持原协议；没有归属证明的旧记录不迁成私人历史。

## 3. 冻结威胁与失败模型

| 资产 / 边界 | 直接失败路径 | 不变量 |
| --- | --- | --- |
| 登录身份 → 网页 → 私有 admission | 伪 user header、重复 header、owner 参数、其他 UID、同 UID 非隔离服务、错误 socket | owner 只来自原 `current_user` 的 proxy proof；私有桥校验既有 distinct UID / peer / 私有目录；公开请求模型 `extra=forbid`，不接收 owner。 |
| 私有请求 / 常用条件 / 结果 → SQLite | 跨 owner 枚举、命令 ID 冲突、分页串号、缓存泄漏 | 所有 lookup、list、resume、draft 都先带 owner；owner 不同返回相同“未找到”；私有响应 `Cache-Control: no-store`，无共享 Serving 私有正文。 |
| 执行 → 历史 | 浏览器提交 success / 数量，解析被算成执行，分页重复，执行后宕机 | 先持久登记原执行命令，再由服务端同一计算路径生成结果事实；历史源是已登记命令与实际完成回执。完成事实未提交时保留 unknown / 未确认，不能凭浏览器补写成功。 |
| 命令 → effect / receipt | 响应丢失、claim 过期、旧命令被编辑、新源替代旧源 | command_id + 完整 owner / payload hash 固定；lookup-first；resume 只能原 payload；不可复现原源时明确失效，不在同 ID 偷换来源。 |
| 数据 → 条件 / 排名 | 混代、未来公告、事后回补、错单位、缺日历、缺历史 / 复权、非有限值 | 所有来源、时间、值和版本可验证；现有 PIT / unknown 数学保持单一；未来或换代拒绝，缺个股事实计 unknown。 |
| 条件保存 → daily writer | 只放宽拒绝、直接用最新基本面、截断 RSI 历史、排名丢证 | 能力由真实 daily source handler 验证；先通过新参数的日终回归再开放保存。结果与原 v2 回执及新增输入 / 排名证据同事务。 |
| raw / feature → 盘中 Serving | 把日线当分钟、伪截止时间、过期字段变真、研究回顾伪 PIT | 引擎原 envelope、原 raw input IDs / 内容 hash、源 descriptor、字段状态一致；同截止时刻；运行时代变化可追溯。 |
| M4 draft → C12 保存 / 消费 | draft 冒充生效、浏览器改结果、范围失效、分钟频率混淆、未知时发恢复、新类型阻断共享 prefix | draft 只从本人实际执行回执取完整请求；最终规则由原 PageControl 保存，实际消费者与原通知 outbox 都有证据后才称生效 / 已触发。unknown 不等于 false，不产生恢复。原 mixed spool 校验新 exact 类型；paper 对条件告警原子提交非交易回执与 cursor，不产生新订单。 |
| schema / 启用 | 修改旧登记行、跳过注册 / ack、默认启用生产新规则 | 原 schema registry 与功能配置控制发布；新合同显式加入并按实际门禁 ack。此 SPEC 不授予生产迁移、服务切换或推送模式变更。 |

信任前提：原 proxy proof、已配置的可信 Web UID、PageControl UID、源 writer 与运行时 manifest 是平台信任边界。被授权服务自身已被攻陷、系统管理员改私有数据库 / 密钥、第三方行情的正确性不在本次防御范围；仍校验本轮收到的结构、身份、时间与内容。

明确排除：M11、生产数据库写入 / 修复、生产 unit / nginx / frp、真实推送、旧 monitor 停服、密钥、源账户开通、全仓审计、旧价规则重设计。上述外部动作按 root 已有授权边界处理。这里的代码和合成源验证继续推进。

阻断范围：本表不变量与 AC-H/P/D/I/A/U。来源未开放时不得宣称本项已完成；不把 source unavailable 当零命中。

## 4. 私有历史与常用条件合同

### 4.1 类型与存储

新增选股领域 Pydantic 合同，使用原 `RuntimeContractModel`。不另写前端接口类型。

| 类型 | 必须字段 / 约束 |
| --- | --- |
| `ScreenQueryDefinition` | schema 1；原描述 ≤500 字；`mode=daily/intraday`；trade_date；日线 source_kind / source_identity 或盘中 source binding；规范化原 `RuleCall` 1–26；原 `ScreenRankingPlan` 可选；完整日期 / 截止选择。source identity 属于执行上下文，不写成常用条件的永远固定来源。 |
| `ExecuteScreenQuery` | 原 PageControl command_id / requested_at；原请求 definition；page_size 1–100。无 owner、outcome、counts、rows。服务端补 `_OwnedExecuteScreenQuery` 的 owner。 |
| `ScreenQueryExecution` | owner（仅内部）；execution_id；server sequence；原 command hash、原请求 / normalized plan hash；来源 binding；started_at / completed_at；status；服务端 base/total/unknown/ranked_count、步骤、完整结果摘要与 rank/member digest。失败 / 未确认的数量为 null。结果行按实际成功结果存有界 artifact，私有正文不进共享 Serving。 |
| `ScreenPresetDefinition` | preset_id；本人显示名 1–80；可恢复的完整 definition（含排名 / 盘中模式）；version ≥1；updated_at；保存命令 hash。source binding 是原出处，重新运行必须解析当前源。 |
| `ScreenHistoryCursor` | 签名 schema；owner hash；截止 sequence；before sequence；过滤条件 digest。默认页 20、最大 100；倒序且固定分页视图。后续新执行不使正在翻页重复 / 丢失。 |

在原 PageControl SQLite 增加 `screen_query_execution` / `screen_query_preset` 领域表；后续draft表同属此数据库。命令队列仍为原 `page_control_command`，执行回执仍为原 `page_control_effect`。私有目录 / 文件沿用 0700 / 0600 和既有文件检查。

私有桥复用原 peer / endpoint helper 的实际权限合同：独立 PageControl UID、可信 Web UID、显式 shared GID；socket父目录 0710、socket 0660。数据库与结果 artifact仍在 PageControl UID独占的 0700目录 / 0600文件，不让 Web UID直接读这些文件。配置缺失、同 UID、错误 peer或端点身份变化时，功能关闭。

历史不按 5 条、30 天或设备静默删除。读端分页遍历全部本人已登记执行记录。单记录 / artifact、磁盘容量以既有输入预算及显式功能配置限制；容量不足在执行前拒绝，新失败仍有原命令事实，旧记录保留。本文不新增自动清理策略。

### 4.2 原协议兼容

`SaveNlPreset`、`AppendNlQueryLog` 的原字段、构造与原 loopback 行为保留；旧 global JSONL / user_presets 不重写。新增 trusted-owner handler 可以用内部 owned subclass 和新增领域上下文处理私人常用条件 / 执行日志，不能让公开 parser 接收内部 owner。

`SaveNlPreset` 原协议没有排名、模式、CAS。网页的新 `ScreenPresetSaveRequest` 以原 name / description / RuleCall 建立原命令，并在可信桥传入 typed `ScreenPresetDefinition` 上下文和 expected_version。新私有 handler 在原 outbox 同事务完成该领域效应，不调用旧共享 JSON 保存路径。旧 ownerless handler继续走原路径。两条协议必须分别回归。

内部 `_OwnedSaveNlPreset` 显式包含 owner、definition、expected_version；校验其 description / RuleCall与原命令一致。legacy name使用稳定preset_id，仍符合原`_validated_name`；私人显示名是独立字段，按NFC / strip / casefold形成owner内唯一名称键，同名覆盖必须指向原preset_id与当前version。内部 `_OwnedAppendNlQueryLog` 只由完成 handler构造并作为同事务的执行日志事实绑定。原公开 parser / enqueue / loopback拒绝这两个内部类型和新增私有执行类型；只有可信 admission可注入 owner。内部上下文进入原 command hash，不能在命令登记后更换。

`AppendNlQueryLog` 的 query / plan / outcome 只由实际执行 handler 生成。owned handler 在同一执行完成事务登记私人日志事实；它不从浏览器接受 outcome，也不把旧无 owner 日志当自己的事实。失败的 NL 生成可以作为生成错误显示，不能计入成功执行历史。

### 4.3 执行与恢复

1. 浏览器生成新的 UUID execution command_id，冻结当前条件、排名、模式和源。新 `/screen/query/execute` 经 require_current_user / require_csrf，严格 JSON 和大小限制。
2. admission 使用可信 owner 注入，先登记 `ExecuteScreenQuery`。只有已登记的可信 command claim 可以执行。计算使用原 `ScreenApplicationService` / 原规则与排名，不添加第二套计算逻辑。
3. handler按冻结 source 运行；成功后，完整服务端结果摘要、私有行 artifact 身份、owned `AppendNlQueryLog` 内容、effect 和完成 receipt 在原命令完成事务绑定。SQLite 与 artifact 跨资源时沿用既有文件 fence：先写不可变有界 artifact，fsync 并核对身份，事务只引用该确切 hash。未引用 artifact 可回收，不能作为成功证据。
4. 未提交结果时重启，只读原 command 与已提交事实。原 snapshot 仍存在且 hash 相同才可恢复原执行；原 replica 被替换、历史源不可恢复时记录 `source_expired`，保留原请求和“结果未确认”，不自动改用当前源。用户“重新运行”生成新 command_id。
5. 同 command_id 的另一 payload / owner拒绝，不能返回别人的 receipt；lease 过期的旧 claim不能完成。HTTP timeout进入“结果待确认”，只 lookup / resume 原命令。原结果分页读本人执行 artifact，换代不会改已执行结果，也不产生新历史条目。
6. 数据换代后的“恢复条件”回填完整原请求；UI展示“数据已更新，请重新运行”。点击运行新来源；不能带旧 cursor、旧成功 revision 或旧转提醒 draft。

一次执行保存同一 pinned source上原 service求得的完整有界结果，最多 8000行；page_size只控制读页。把现有 service中求值 / 排名与分页拆成可复用私有方法，原 `/screen/run` 和新 handler调用同一方法。不能靠浏览器逐页拼接、第一页行数或另一套计算器生成执行事实。

### 4.4 网页接口

新增 `GET /screen/query/history`、`GET /screen/query/presets`、`GET /screen/query/executions/{execution_id}`、`GET /screen/query/executions/{execution_id}/results`、`POST /screen/query/execute`、`POST /screen/query/lookup`、`POST /screen/query/resume`、`POST /screen/query/presets/save`。实际前缀为`/api/v1`。所有私有操作需 authenticated owner；请求无 owner。list / lookup 屏蔽跨 owner存在性。错误不泄漏私有路径 / SQL / 其他用户信息。

execution详情读取原请求与服务端事实；results只读已提交artifact，签名cursor绑定owner / execution / artifact hash，默认20、最大100。未提交结果不可分页。lookup / resume采用typed action union，包含原execute或presets-save的完整冻结请求，不能只凭command_id执行。错误码：输入无效422、本人payload冲突409、来源失效409、能力 / 来源不可用503；不存在与他人对象一致404。所有私有响应no-store，异常日志只记非正文的稳定ID。

私有请求最大64 KiB；执行artifact最大16 MiB、最多8000行，超限在提交成功前明确失败，不能截断后称完整。cursor复用原服务签名材料并加领域前缀，不新增密钥来源；重启后旧cursor无效可从首屏重新读，持久历史不丢。请求、artifact正文与自然语言不写运行日志。

私有list响应提供由服务端生成的owner_scope_tag，仅用于前端分隔本标签页的待确认命令，不能授权访问。跨设备恢复从服务端历史读取。前端不缓存完整历史到localStorage；本标签页必要的待确认原请求放owner_scope_tag命名空间的sessionStorage，登录人改变 / 注销即清除并忽略旧异步响应。服务端仍逐请求从proxy proof取owner，不信tag。

原 `/screen/run` 保持已接受的日线请求兼容；新 UI完整执行走上述可追溯入口。domain执行 handler 可直接复用 service.run 的纯行为；原 API 与新 API不能各算一套计数。缺私有功能配置时完整历史显示“暂不可用”，不能把 recent描述代替该功能。

## 5. 可重现的保存与日终执行

### 5.1 单一数据准备

新增 `screen/daily_inputs.py` 仅负责准备有类型输入与来源证据。规则求值仍在原 `screen`，排名仍在原 `rank_screen_results`。

- 依赖来自 `required_rule_columns` 与 `_collect_aggregates`，加展示和排名列；沿用副本预算：26 条、8000 股票、普通 lookback 90、aggregate 500、wide cells 1,000,000、aggregate facts 8,000,000；MA原上限 8000×281；RSI沿用原 full-history / 单股 50,000 / 2–60 / offset ≤30预算。超预算先拒绝，不先全量读表。
- 普通列使用现有选择性 loader；MA继续 `derive_requested_ma`，固定周期保留 daily_indicator。不改原均线复权 / 缺值口径。
- 自定义 RSI把 `publish_dynamic_rsi_projection` 中原完整历史算法抽成同一纯计算助手。daily writer 在自身原事务 / connection上用该助手，只读交易日 ≤目标日，原复权、缺行和暂停规则相同。副本发布器也用该助手。不能每日截取短 lookback当原 RSI。
- 基本面只读目标日已生成的 `fundamental_daily_head → immutable version`，用原 `checked_fundamental_version` / `_load_fundamental_wide` 验证 T17、股票 / 日期、最高 revision、字段值和 source hash。daily来源能力与 Web副本来源显式区分；不能加一个浏览器可控“trusted=True”跳过原检查。
- 财务值仍由原 financial PIT / daily valuation生成。公告可见时点、实际 first_observed、source revision继续生效。刚回补的旧日期资料不能变成过去可见。目标日缺 fundamental记录时该股 unknown；全部缺来源时本轮失败，不能报正常零命中。

`SaveUserPoolV3` 的参数能力检查调用同一日终支持验证器；定义并不要求当天某股有全量事实，但必须有实际 writer路径可重现。先实现并测通上述输入与结果，再移除针对新增受支持参数的拒绝。未知字段 / 假动态周期 / 未注册积木继续拒绝。

### 5.2 结果与排名证据

保持原 `ScreenRunReceipt` v1/v2语义和字节。新增 typed `ScreenRunEvidence`，与原 v2 result_version绑定，包含 definition_version、输入源 / 内容 digest、固定 decision_at、完整 unknown计数、ranking plan hash、实际排名行 digest、已存行 `extra` digest、completed_at。

原 `screen_result.extra` 存 `ranking_score` / `rank_position` 与原附加列，不丢排名。新增独立表 `screen_run_evidence` 与原结果 / v2 receipt在同一 DuckDB事务提交。新增可版本化 schema migration，不修改既有 migration 1–15的 checksum；迁移只在本任务合成库运行，生产迁移仍需 root独立授权。

发布器核对结果的原成员 / 价格回执，并从持久化 rows重算 rank / extra digest再发布新 `screen_run_evidence` 投影。浏览器、调用者或 draft不能填写 score/count证明。零命中有空结果与独立证据；全部unknown不伪装成完整lineage；失败不替换上轮成功结果。

日终页与池子结果可查看本次日期、命中、unknown、排名和当前定义对应状态；版本 / source hash放详情。旧池子结果保留原显示与契约，不能给旧结果补写新的来源证明。

## 6. 真实盘中来源与筛选

### 6.1 来源发布

新 `screen/intraday_source.py` 是原 raw / feature spool到投影的有界适配器。由原 `SignalPageProjectionProducer` 接入，使用同一 `NotificationProjectionSourceReceipt → NotificationProjectionAuthoritySnapshot → ServingSnapshotAssembler`。不增加 owner dataset来绕过原8 owner检查，不增加旁路 Serving writer。

新增 `intraday_feature_snapshot` + `intraday_screen_source` 两个 signals-owned投影。后一投影保存共同 trade_date / cutoff、feature source generation / sequence / batch id / contract fingerprint / payload hash、raw input receipts、quote源、reference source与schema fingerprint。snapshot每股一行，字段带原 FeatureFieldStatus。`market_snapshot` 一起绑定；同名投影不重复注入。原 companion fallback不能把上一轮数据标成新截止的 fresh。

来源还绑定本次 universe的股票集合 / digest、实际覆盖集合及缺失原因。全市场请求从受信的股票目录取完整有界代码集，经原quote gateway请求、原分钟批次检查覆盖；不能把个人盯盘集默认为全市场。部分缺股计 unknown，覆盖源完全失效时本轮失败。quote批次必须保留原单位与已登记转换：只有已证明成交量单位才能转成 shares，成交额才能转成 CNY。

publisher只读配置给定的原 FeatureBatchSpool、LiveBatchSpool和已经发布的参考事实。校验文件、原 descriptor、sequence / prefix、envelope内容、raw input_batch_ids、producer_commit、逐字段状态、交易日和source_available_at。不把浏览器 source_identity、row_count、时钟当来源证明。read-only spool cursors位于publisher自己的原control root，不能写producer目录。

### 6.2 同一截止时刻与数学

`ScreenRunRequest` 加可选 `mode`（默认 daily）、`decision_cutoff` 与 `intraday_source_identity`；daily旧请求行为保持。盘中目录只显示实际可用字段和单位。request source binding使用新综合源身份，并绑定Serving代、raw / feature / reference身份；分页签名含mode与cutoff。

截止时刻来自受信发布的source row；用户选择已发布时刻，不可指定不存在的历史时刻。全部 inputs event_time / available_at / published_at ≤cutoff；特征逐股字段delay沿用原60秒合同。事实晚到时不能把实际观察时间改成分钟结束时刻。市场报价和特征时间不同，只选在该cutoff已经可见且未过期的原事实，并分别保留实际时间；禁止做相等timestamp伪造。

| 盘中字段 | 来源 / 数学 | 缺失行为 |
| --- | --- | --- |
| 最新价、开、高、低、累计量额 | 同cutoff可信 `market_snapshot`，或由原分钟批次的最新价 / session值组成明确标为分钟快照；量统一 shares，额 CNY | 缺报价 / raw批次时unknown；不回退当日日线收盘 |
| 盘中涨跌幅 | 100×(price/pre_close−1)，pre_close须为当日已观察的真实昨收 / 除权参考；可直接保留受信来源pct_chg并核对 | 不能拿普通昨日close替代除权昨收；缺值unknown |
| 5分钟涨速 | 原纯分钟引擎新增 100×(C_t/C_{t−5}−1)，6根连续已收完分钟，同session，所有available_at≤cutoff | 缺分钟、跨午休、开盘不足、非有限值unknown |
| 累计相对成交额 / 同分钟相对成交额 | 原rel_cumulative / rel_same_minute，按原20日实际观察与金额中位数 | 不标为成交量比；缺 / 零基准沿用原reason |
| 盘中成交量比 | 原分钟引擎按同一历史选择新增累计量 / 历史同截止累计量中位数；1min股数同单位 | 缺历史 / 零基准unknown；日线 `volume_ratio_gte` 语义不改变 |
| 盘中换手率 | 真实已观察报价的turnover_rate；或cutoff前可验证的本人股票流通股数参考 ×100。来源和单位随同发布 | 市场分段float_share、今日事后daily_basic、浏览器输入均不得充当分母；缺真实参考unknown |
| 距涨停 | 100×(up_limit−price)/up_limit；up_limit来自同cutoff可验证限价事实，或原state/derive的规则、ROUND_HALF_UP与真实pre_close / PIT状态 / 上市资格 | IPO无价格限制 / 缺状态 / 缺上市资格 / 除权参考缺失均unknown |

新minute字段使原`intraday-pit`合同显式升为v4；保留旧v3源与消费者兼容，不能把旧version含义变更。原runtime_definition_bootstrap声明新字段、可用性和source依赖；按原runtime_schema_registry走实际schema / consumer ack。原引擎live / replay共用新增纯函数，一次手算及差分证据，网页不重复实现。

原26积木仍由原registry编译。盘中新的数值字段作为原gt/lt/gte/lte/between操作数；不重新定义日线CLOSE、VOL、MA等。盘中模式的原日线条件明确锚定cutoff前最近已收盘交易日，目录文案标“上个交易日”。当前日T17基本面在盘中不能使用；可用前一日已证明来源，缺 source_observed_at证明时unknown。各offset、日期日历和ranking字段同一锚定规则，复用原数学。

### 6.3 当前实际源缺口

`SCREEN-SOURCE-01`：已读代码中，正常quote gateway没有保留昨收 / 换手 / 限价；日线采集也没有个人流通股本的可见性证据。`market_snapshot`的默认 companion可能只是旧数据，现有盯盘请求也不能证明全市场覆盖。新增字段与全市场实时覆盖不是现有能力。可实施路径是沿原watchlist quote provider保留真实原始可选字段、单位、完整股票集与请求观察回执，由原gateway发布明确schema的新字段；原新版`market_snapshot`接入这一受信源。若源没有换手，沿原数据源采集个人流通股本参考及不可变观察回执，由`screen/intraday_reference.py`仅验证 / 适配事实。新增参考采集的实际路径、源权限与可返回列由root串行接线；不让quote gateway写主库。本implementer本轮不联网。

该依赖不缩减AC-I / AC-A：先完成已有raw / feature源、纯函数、unknown与界面；在新字段真实来源接通并回放之前，对应能力不得记完成。不得用一张合成CSV或日终回填声明真实盘中源已开放。

## 7. M4 → 完整 C12 的串行合同

### 7.1 M4提供什么

新增 `POST /screen/query/alert-draft`，只接 execution_id、原command hash、draft命令id；无conditions/counts/outcome/owner。私有handler lookup本人已确认的实际执行，验证完整原请求、mode与source binding，输出typed `ScreenAlertDraft`。

`ScreenAlertDraft` 字段：schema 1、draft_id、origin execution / definition / result digest、原规范化RuleCall、原ranking、日线锚定政策或盘中字段合同、trade_date / cutoff出处、preferred_scope、可用capabilities / message。scope最终在编辑器明确选择。默认“全市场，按这些条件持续筛选”，不能把当前结果几只股票暗示成永远固定股票；选择盯盘 / 池子 / 板块时由C12服务验证实际成员版本。排名存在时一起传递，C12仍用原rank_screen_results在scope内筛选后选前N，不丢排名语义。

draft只读本人私有请求，不替用户保存 / 启用规则，不通知。跳转到盯盘页时传短draft_id，不把私有条件放URL query或共享localStorage。消费页通过私有接口读draft；source换代只影响出处，不自动改已冻结条件。用户修改后需作为新的明确规则定义确认。

`GET /screen/query/alert-drafts/{draft_id}`只返回当前owner的草稿。draft内容固定存于原PageControl SQLite的`screen_alert_draft`表，绑定owner / 原执行hash / 内容hash；同draft命令id重复只返回原draft，不在换代时改内容。draft明确24小时过期；过期只提示重新带入，不影响完整执行历史。生成与取回都不改变执行成功事实。

### 7.2 C12必须实现的共享类型

在 `alert_rule_contracts.py` 定义完整新规则，旧 `PriceAlertRule`与所有到价协议保持原字节与含义。M4 / C12使用同一Pydantic模块，前端由同一OpenAPI生成。

| 合同部分 | 定义 |
| --- | --- |
| 定义 | owner仅内部；rule_id；version；name 1–80；priority P0–P3；enabled；原RuleCall 1–26；ranking可选；`condition_semantics_version`；origin draft / executed result hash |
| 范围 | discriminated union `pool / watchlist / sector / market`；池子定义/结果版本、本人盯盘成员版本、板块系统/代码与成分源版本必须可核验。失效 / 过期 / unknown范围不求肯定结果。 |
| 时段 | Asia/Shanghai交易日连续时段，start≤end、不跨午夜，校验盘中session与日历；日线条件锚定上一已收盘日。 |
| 频率 | `every_evaluation`：每个新的可信evaluation身份一次，重放不重复；`per_symbol_minutes`：整数N 1–60，按(rule版本,ts_code)真实trigger时间，Δ≥N×60秒才可新触发；`bar_close`：1min，每根已结束且可见K线一次，同bar重复不发。 |
| 治理 | dedup_window_seconds 0–3600；notify_recovery boolean；channels非空且在原服务允许集合。结果true→false且来源完全known才可发恢复；unknown / stale / 断源不更新为false。 |
| 保存 | `SaveAlertRule / SetAlertRuleEnabled / DeleteAlertRule`沿原PageControl新增typed种类与可信owner handler；expected_version CAS；两步确认适用全市场启用等重操作；lookup/resume原命令。 |
| 来源回执 | 本人规则当前版本 / body hash、scope版本、input source身份 / cutoff / field状态、evaluation_id、member / ranking digest，actual server times |
| 触发与路由 | 新typed条件告警事件；原bus内追加种类；原 mixed spool 增加独立 condition wrapper/schema codec；同原global_sequence与连续prefix；原NotificationStateStore混合replication增加此exact类型；进入原delivery_outbox / 原通道provider。新种类不得扮成threshold_reached，paper 按可信非交易事件续原 cursor。 |

规则采用逐股真值状态。只有本次 scope / source完整可判的股票，才可由true转false；缺股、缺字段、失效scope和中断来源保持unknown。频率在实际evaluation / trigger提交时绑定(rule版本,股票,event_kind)，重放同evaluation不能推进冷却。去重以同一键和服务端event time计算；恢复为独立event_kind，但必须有此前已确认true。scope变化与规则修改生成新版本，不继承旧版本的肯定状态。排名只在原scope内用原数学选前N，保留出处与完整排名证据。

私人历史 / 常用条件 / artifact仍只在私有桥读取。用户明确保存后的C12规则才进入原规则authority路径，原owner检查覆盖规则列表、当前设置、状态、触发与推送详情。此authority不携带原自然语言描述、历史结果行或私人artifact路径；origin只含不可反查正文的ID / hash。旧Serving规则读取的权限检查沿用，新种类不能退回无owner接口。

### 7.3 实际消费接线与分工

root串行安排C12消费者作者，在本任务最终候选之前完成：

1. 共享rule合同与原规则存储增加condition-kind namespace，原价格规则保持独立type；同PageControl queue / effect / receipt。不是另一registry。
2. 在原runtime_service_entrypoint / runtime_service_builtin注册实际条件评估role，薄builder位于`runtime_builder_condition_alert.py`。配置显式给定已发布Serving source、原feature合同、市场日历与原runtime ledger；启用证据绑定实际producer_commit / role manifest及rule / evaluation / routing合同hash。没有配置时capability=false。
3. 新role调用本SPEC同一输入准备、registry求值、ranking数学；范围服务解析真实pool/watchlist/sector/market。频率、truth状态与触发事件原子写原alert runtime ledger的新typed命名空间，不添加第二个任务队列。
4. 原signal_bus / price_alert_route的现有global sequence入口增加新typed事件适配；原 `signal_route_spool.py` 的 writer → readonly reader 接三类完整连续 prefix，再由原notification_state的mixed复制进入原outbox。路由继续原允许的recipient_policy、dedup/merge、上游断开抑制与正式/影子状态；不新增notifier / provider。原notification_worker / runtime_notification_providers增加完整条件事件的exact类型处理与发送前许可：同原lease / 当前规则与scope / 可信activation / 原outbox绑定，禁止套用旧price prepared/admitted类型。保留原未知发送状态、取消与重试语义；未知不能称已推送。
5. 盯盘规则编辑器复用选股条件行，完整scope / frequency / governance输入与当前head CAS；读取原实际rule、scope、eval/trigger/route回执。M4按钮只有这些真实capabilities开放才显示可保存；draft中间态文案为“已带入条件”，最终有save回执与当前published row才为“已保存”，actual消费者activation验证后才为“已启用”。“已推送”必须有原delivery实际成功事实。
6. 本轮合成/历史回放源验证真实Python consumer→原bus→原NotificationStateStore→原delivery_outbox链路，provider注入离线测试double；不发真实推送、不停旧monitor。旧四类monitor迁移与production停服依原goal其余轨道，不在M4私自切换。

`M4-SPEC-01`：在原 `signal_route_spool.py` 新增 exact `ConditionAlertRouteSpoolRecord`，schema 5、`record_schema=rquant.condition-alert-route-record/v1`，只封装 exact `ConditionAlertBusRoutedRecord`。header hash 使用 canonical header（不含 record / record_hash），payload_hash 绑定原新事件 sha256，previous_record_hash 续同一全局链。扩展两个原 TypeAlias、`_decode_notification_spool_record`、`publish_mixed_notification_bus_prefix`、`ReadonlyNotificationEventRouteSpool` 的刷新 / 路由读 / 事件转换 / observed prefix。写前核验有界批次的 exact 类型、source generation、序号、canonical 内容与可见性；reader 再核验链、pointer及内容。原 legacy v2 / price v4 codec、bytes、hash-chain公式保持；current v3、未知 schema / 类型、替换对象仍拒绝。不增加 relay，不过滤或跳过全局记录。

`M4-SPEC-02`：原 `paper_signal_consumer.py` 的混合分派增加 exact `ConditionAlertBusEventRecord`。先校验本批次连续序号、原source generation、canonical事件及 received_at / available_at可见性，再 observe / complete。新增显式 `PaperConditionNonTradingReceipt`（`paper-condition-non-trading/v1`）沿原 `PaperSignalConsumerStateStore` 安装 / 查询 / 重放路径，与同一原cursor在单一SQLite事务提交；旧price receipt / schema保持。失败或冲突不前移cursor，不新建 queue记录 / paper订单，不调用交易委派。重开 / 重放返回同一非交易回执，随后legacy仍按原路径消费。`runtime_builder_paper.py` 锁为只读直接依赖；它已构造同一原readonly mixed spool，当前不提案修改，实际新增需要再按最小before边界提案。这是共享消费者兼容，M11仍排除。

交接端点、字段、准确文件边界及未接通状态见`handoff.json`。M4完成清单必须等C12实际消费回证才可关闭AC-A，不能在draft成功时关闭。

## 8. 实施写集与步骤

机器可检查写集在`proposed-write-boundary.json`，每个既有文件包含新base before_sha256；新文件before=null。当前仅SPEC范围可写，下表都是待root接线授权的产品范围。不得顺便改已接受子系统。

| 阶段 | 新增领域文件 | 直接修改 |
| --- | --- | --- |
| T1 私有执行 / 历史 | screen/query_contracts.py、query_history.py、query_admission.py；web/models/screen_history.py、web/routes/screen_history.py；ScreenQueryHistory.tsx、screenQueryCommandSession.ts | page_control.py、page_control_service.py（薄配置接线）、web/app.py、web/settings.py、web/screen_service.py、screener index / NL面板；复用原安全helper，不重写helper |
| T2 保存 / daily证据 | screen/daily_inputs.py | screen/core.py、loader.py、dynamic_rsi.py、replica_source.py、pipeline.py、page_control.py的V3支持验证；pool_result_receipt.py、storage/schema.py / migrations.py / duckdb.py、serving_page_projection_source.py、serving_read_models.py；ScreenPoolSave.tsx真实capability与反馈 |
| T3 盘中 | screen/intraday_contracts.py、intraday_source.py、intraday_reference.py；web/screen_intraday.py；shared/ScreenConditionEditor.tsx | 原intraday_feature_engine / runtime_definition_bootstrap / feature builder；必要的原quote provider / gateway字段；SignalPageProjectionProducer / runtime_builder_signal；screen服务 / API / catalog / typed请求 / 主页面 |
| T4 M4导入 | screen/alert_draft.py；web/models/screen_alert_draft.py、routes/screen_alert_draft.py；ScreenAlertImport.tsx | screener index / ScreenResults，原Web路由注册与body限额；共享alert_rule_contracts.py由两端冻结同一份 |
| T5 C12串行消费者 | 共享alert_rule_contracts.py、condition_alert_rule_store.py、condition_alert_runtime.py、condition_alert_runtime_contracts.py、condition_alert_route.py、condition_alert_runtime_projection.py、runtime_builder_condition_alert.py、web/condition_alert_commands.py / read.py / models / routes | 原PageControl、price_alert_rule_store、price_alert_runtime_store、runtime_service_entrypoint / builtin、runtime_builder_signal、signal_bus、signal_route_spool、paper_signal_consumer、notification_state、notification_worker、runtime_notification_providers、盯盘页与新增ConditionAlertRules；runtime_builder_paper仅锁直接依赖。准确候选边界由root按handoff一次接线，原到价逻辑只复核新增适配回归 |
| 生成与说明 | 新API models生成openapi.json / schema.d.ts；必要原dist更新 | 仅相关README/说明与原gap报告，最后统一生成 / build，不按每步反复全build |

执行顺序：T1→T2→T3→T4→root串行T5→集中最终验收。每段先写直接失败测试、保存红测原始日志、最小实现、同一测试转绿，再运行相关回归。没有中间每段独立审查；SPEC独立审查一次，最终候选集中终审一次，真实finding按用户授权持续定向修复。

## 9. 直接失败矩阵与验证方式

`failure-matrix.json`列出每个稳定ID、实际代码锚点、输入、断言、拟测试节点、严重级别、验收关联和状态。所有新增测试均`planned_not_run`。它覆盖本SPEC风险表，不扩展全仓。

直接矩阵包含：身份伪造 / 跨owner；全历史分页与换设备；旧协议兼容；原命令恢复与版本冲突；先登记后执行的宕机矩阵；新增daily参数与unknown / PIT / 精确来源；排名行篡改和同事务失败；raw / feature混代、未来 / 过期 / 缺值；分钟字段手算、除权、午休；换代异步竞态；完整C12范围 / 频率 / 去重 / 恢复 / 原bus与outbox；1440 / 390 / 键盘及短文案。

`SC-A09/A10/A11` 定向补充 legacy v2 / price v4 / condition v5 三类交错：原 writer → readonly重开 → mixed复制 / outbox；同prefix的paper非交易回执与cursor；state重开 / 重放无重复效应；随后legacy继续；条件事件零新queue / 订单。缺序、改header/payload/hash、错source、未来时点、未知schema / 类型在 observe或提交前拒绝，pointer / outbox / paper receipt与cursor不部分前移。正常cutoff暂不可见记录保留原延后规则，不能跳过到后续序号。全部新增参数仍 `planned_not_run`。

离线Python后续使用已存在共同解释器：`/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration/.venv/bin/python`（root提供3.13.12，执行时再预检）。`-I -B`，本树src/root优先，`RQUANT_DISABLE_DOTENV=1`，各四个数据路径指向本树私有合成目录，0700 TMPDIR，BLAS线程1。原始日志和独立手算参考保存在本树项目目录。解释器仅验证本树代码，不把另一树代码作为实现证据。

新直接节点计划命令：

```text
pytest tests/unit/test_screen_query_history.py tests/unit/test_screen_query_admission.py tests/unit/test_web_screen_history.py -q
pytest tests/unit/test_daily_screen_reproducible.py tests/unit/test_screen_dynamic_rsi.py tests/unit/test_screen_dynamic_ma.py tests/unit/test_pool_definition_v2.py tests/unit/test_daily_pool_stage.py tests/unit/test_pool_result_publication.py -q
pytest tests/unit/test_screen_intraday.py tests/unit/test_serving_screen_intraday.py tests/unit/test_web_screen_intraday.py tests/unit/test_intraday_feature_engine.py -q
pytest tests/unit/test_web_screen_alert_draft.py tests/integration/test_screen_alert_consumer_chain.py -q
```

新文件未创建，以上不是已执行命令。Node / pnpm、依赖、socket、GUI与权限预检由root在实施授权时记录。此作者本轮不能运行socket / GUI；UI最终需root安排实际Playwright，不以Vitest替代。

最终按web/AGENTS实际门禁执行一次`pnpm -C web check`、`build`、`verify:dist`、相关新增e2e与API OpenAPI一致性。API变化跑规定`tests/unit/test_web_*.py`门禁；复用本goal仍有效证据，具体新增风险才扩大测试。skip / deselect / 环境失败逐项保留，不称通过。

新参考用例单独记录来源、算式与SHA；旧证据保留。收尾核对后续启动的测试server / child / 临时目录；本轮未启动任何server / child或新临时树。

## 10. 冻结与交接状态

本SPEC交付条件：source-lock里所有before文件来自本树新base；完整风险与失败模型；准确拟写集；AC-A完整C12消费者串行交接；failure-matrix新增项全部planned_not_run。本文完成只代表SPEC提案可审，不代表M4完成、测试通过或上线。

root独立SPEC审查通过后，给原作者明确产品写集与有限接线授权。实际来源或共享合同有实质变化时，先更新本SPEC、hash与矩阵，再实施受影响部分。只对真实阻断项补修 / 复核；本goal没有行政时长或次数上限。
