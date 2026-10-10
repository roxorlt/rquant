# 到价提醒运行链实施方案

**目标：** 把已接受的单股到价规则接到实际报价、原通知链和本人运行状态。

**做法：** 从当前同代规则和名单取得输入。复用纯价格求值；把频率状态和价格事件一次落盘。价格事件走独立、默认关闭的入口，再进入原 bus、spool、outbox 和 notifier。

**技术：** Python / Pydantic / SQLite，现有报价进程、Serving、FastAPI、React / TypeScript。由 Codex 原生代理实施。

**分级：高风险。** 本片涉及用户与收件人隔离、共享投递账本、重复执行和中断恢复。审查限本文不变量与 PAR-01～22。

**状态：两项 SPEC 发现已补入，待原审查者定向复核；尚未实施。** 基准 `6c793b47a97d3c4582f3f93f292ce86e8d95e253` 由 root 提供；本次没有运行 Git 或产品。原冻结和审查保留。当前合同、写集、附加来源及未执行矩阵在 `data/verification/price-alert-runtime-20261005/spec-proposal/repair-r01/`；使用该目录的 `contracts-final.json`、`acceptance-matrix-final.json`、`guard-map-final.json`、`write-boundaries-final.json` 和最终冻结索引。`before.md` 是原计划原字节，草稿和命令失败原件也保留。C12 配置已接受，本片不重审其 CAS、私有命令或恢复。

## 1. 本片与后续目标

本片只做本人有效盯盘股票的 `gte` / `lte` 价格规则。保持原价格、时间和精度。每条规则采用服务端频率合同 `price-alert-frequency/v1`：`per_rule_cooldown`，默认 300 秒，可由冻结运行配置设为 60～3,600 秒。用户页面显示「每 5 分钟最多一次」等实际值；本片不新增频率编辑字段。

第一次接通用已有历史报价或合成来源验证，并用隔离的假 provider 核对投递。合成通过只证明运行行为；历史通过只证明回放。生产身份安装、真实外发、切流和停旧服务均未新增授权。

完整 M12 还须完成通用条件、池子 / 板块 / 全市场、逐根 K 线频率、30 秒合并、恢复消息、旧四类规则对照、受控测试和模式切换。见第 10 节，不能把本片称为 M12 全部完成。

## 2. 现有边界与选定方案

[原信封](/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-research-sdk/src/rquant/signal_contracts.py:257)有策略、参数和数据身份。`CurrentSignalEnvelope` 目前只读。它不能承载价格规则，也不能用零哈希或规则哈希补策略字段。

选定方案：新增精确的价格 family 和来源绑定表，复用同一 bus 库的全局事件表及原投递表。生产库不迁移。专用表只通过显式版本入口在获准的私有验证库中建立；部署到真实库仍需原授权链。

两个备选不采用：借用 legacy 策略信封会伪造身份；重做通用通知框架会扩大范围。原 `SignalEnvelope`、`CurrentSignalEnvelope`、解析器、schema、身份哈希和所有 legacy-only 公共入口保持。新增入口只接受精确价格模型及对应准入能力。

## 3. 输入、时刻与容量

| 输入 / 边界 | 固定合同 |
|---|---|
| 规则与名单 | 每轮重借当前 Serving；四张原规则 / 名单表同代、摘要和计数正确，来源可见时刻不晚于本轮。代龄最多 30 秒。提交事件前再核指针，换代则整轮重取，不按旧 lease 继续 |
| owner | 来自规则权威行，完整计入本轮域；无收件映射不能静默漏掉此人的规则，须记录注意 / no_target。浏览器不能提交 owner、收件人、通道、路径或 source identity |
| 规则与绑定 | 每人原上限 100 条。精确 rule/version、membership_version、未删除、enabled、名单有效且未到期。名单重加后的新版本不复活旧规则 |
| 报价域 | 只取上述有效规则股票的排序去重并集。新增 `price_rules` 域入口；旧默认 `candidate` 域不变。请求前封存真实 scope 代、行摘要、codes、as_of 和原 request_id；报价必须能连回该请求 |
| 报价来源 | 复用 gateway / quota / circuit / spawn 清理。只接 `published` 批；缺批、candidate、坏 SHA、错来源、域外或重复代码都不可用。新有界只读适配读当前指针、具名批及原 payload；不 glob 历史，不用当前批声称历史完整 |
| 时刻 | 留下 scheduled/requested/response/observed/available/evaluated 时刻及原时间来源。无 provider 时刻时使用已记录的响应时刻，不能称交易所时刻。`available_at <= evaluated_at`；报价最多旧 15 秒；未来、错日和午休前的报价不触发 |
| 日历与求值 | 原已核验 SSE 日历，精确内容 SHA 和生产版本。原 `evaluate_price_rule` 三态及连续竞价、规则时间窗口不改。float 行情仅按实际 gateway 值的 `Decimal(str(value))` 比较，不舍入阈值，不提升原始精度保证 |
| 每轮 | 间隔 5 秒；最多 32 个已配置 owner、1,000 条有效规则、500 个报价代码；报价 payload 最多 4 MiB。超过任一边界拒绝整轮，不取前若干条假称完整 |
| 持久数据 | 生产者日志最多 100,000 个事件、512 MiB；每轮规则摘要最多 1 MiB。达到容量时暂停新提醒并保留既有历史；不自动删未路由事件或重建来源身份 |
| 路由与投递 | 每轮最多 100 个价格事件；每个 owner 最多两个已授权具体 target，按通道 / 收件人唯一，可包含同通道的两个设备。复用原重试、租约和投递数量上限。原 provider timeout / quota / backoff 不放宽 |
| 网页投影 | 每人最近 20 个事件，最多 640 个事件和 6,400 个 attempt；新投影合计最多 2 MiB，原 signals 所有投影合计 7 MiB 上限不改。容量不足时新统计不可用；可信截断有确切计数，不显示为全历史 |

空域只有在来源可信且计数确为零时才是空。来源缺失、未激活、字段不符、源换代、读中变化或容量不足分别记录，不改成空名单。

## 4. 精确价格事件与准入

新增 `PriceAlertEventEnvelope`，`envelope_schema = rquant.price-alert-event/v1`，精确类型、严格 JSON、额外字段拒绝。它含：

- `event_id`，`owner_id`，`rule_id`，`rule_version`，`membership_version`，`ts_code`，`trade_date`；
- 原规则完整规范正文 SHA，名单绑定 SHA，`frequency_policy_sha256`；
- `comparison`、精确 threshold / price 文本、`kind=threshold_reached`、原规则名与级别；
- 真实 scope 代 / manifest SHA、calendar SHA、quote source 代 / batch / sequence / revision / payload SHA、quote request binding SHA；
- `quote_observed_at`、时间来源、`quote_available_at`、`evaluated_at`、`available_at`、`expires_at`；
- 原运行 manifest SHA、实际 producer commit 和持久来源 epoch。所有 SHA 必须是实际计算的非零值。

`event_id` 是域版本、owner、rule/version、membership/version、交易日、频率 SHA、规则 SHA 和 `observation_key` 的严格规范 JSON SHA256。`observation_key` 绑定 quote source 代、代码、交易日、观察时刻、实际价格及时间来源。重读同一来源事实不因本轮时钟、spool 序号或新 Serving 发布而另造事件。提交时刻等其余事实第一次成功落盘后固定；同 ID 异正文拒绝。

事件 TTL 为 120 秒，并截到原规则有效结束和当前连续竞价结束。正可见时间必须早于过期时间。DB 旧 `signal_id` 列只作为兼容存储键保存 `event_id`；对外价格 JSON 不出现策略、参数或数据快照假身份。

`available_at` 是输入已可见且决策完成的实际逻辑时刻，不冒称物理 COMMIT 时刻。路由 reader 另记成功读取已提交原字节的 `source_inspected_at`；它不得晚于路由时刻。丢回执按日志恢复，不修改事件时间，也不伪造尚未完成的事务已可见。

新增 `PriceAlertRuntimeActivation`，缺省无能力。版本为 `price-alert-runtime/v1`；绑定精确 producer manifest、求值 / 频率合同、报价来源与域、日历、非秘密收件政策、生产者账本身份及 bus / spool / notifier 身份。builder 校验实际文件 SHA、可信目录与 UID、私有路径、角色和版本后才生成不可替代的 typed 能力。任意 bool、Web 字段、相近 family、子类或改内容的能力均不能授权写入。

新增价格求值、事件写入、路由及外发能力默认关闭。混合历史读取、已封存前缀发布及通知库复制使用固定版本合同，不受这些价格开关控制；它们只能搬运原受信来源已提交的事件和回执，不能生成事件、补 target 或取得外发权。`delivery_enabled=false` 不 claim 新的价格发送；原策略继续。隔离测试显式装配能力和假 provider，不能由测试开关开启真实 provider。

## 5. 频率、状态和中断恢复

生产者用自己的私有 SQLite，只有一个 runtime 写者。规则设置继续由原 PageControl 管理。

同一事务写入本轮回执、最后求值、观察身份、频率状态、事件日志和来源序号。事务前中断无事件；事务后回执丢失先按原观察回执读回，不重算正文。restart 保留 ledger identity、epoch、来源序号和冷却。缺账本但旧来源已注册、游标回退或钟回退时拒绝运行，不能新建空状态掩盖缺口。

冷却键为 owner / rule ID / membership_version / 频率合同。普通编辑和启停版本变化保留该键的 `next_allowed_at`，不能用反复启停绕开冷却。名单新版本是新绑定，原规则仍须重新合法绑定。跨日保留冷却时刻；不人工清空。

只有 fresh、triggered、不同观察且达到冷却时刻才建事件。持续超过阈值时每个冷却区间最多一次。未触发、不可用、来源中断和重复报价都不建事件、不重置冷却。它们仍更新实际求值事实；没有实现恢复消息。

价格来源是事件日志，不是第二个通知队列。路由只读它的有界连续前缀；所有通知重试、lease、unknown、attempt 仍在原通知状态库。

bus 的同库专用表是 `price_alert_route_activation`、`price_alert_route_source`、`price_alert_route_receipt`。source 存真实 ledger / epoch / manifest / 求值合同 / 收件政策 / first / high / last；receipt 存精确 owner、rule/version、membership/version、事件、冻结 target 与决定。它们不填 `strategy_spec_fingerprint`。不改旧策略 source 表和 rotation。

`commit_price_alert_route` 一次事务完成来源绑定、全局事件写入、专用 receipt、原 outbox 和价格来源游标。相同 source sequence / event / 正文 / targets 读回原结果；跳号、回退、改正文、换 owner 或改 target 拒绝。写后中断按原回执继续。生产者、路由、spool、通知库分别恢复各自已封存前缀；不假装跨库有原子事务。

## 6. 逐跳改点与旧入口

精确路径见本次 `repair-r01/write-boundaries-final.json`，原入口见 `guard-map-final.json`。下列新名称是实施合同，尚未存在。

| 位置 | 最小适配 |
|---|---|
| `signal_contracts.py` | 不改原 legacy / current schema、哈希和 parser |
| `signal_bus.py` | 保留 `require_legacy_signal_write`、`ingest`、`route`、`commit_source_route` 的旧拒绝。新增价格版本安装和 `commit_price_alert_route`。新增精确 `notification_event`、`notification_events_after_global_sequence`、`routed_notification_events_after_global_sequence` 历史 reader；不因读取价格而授予写权 |
| `signal_router_runtime.py` / 新 `price_alert_route.py` | 原 `route_runner_signals` 不接价格。独立价格 reader 与服务端 owner 路由只用获准价格来源 |
| `signal_route_spool.py` | 原 v2 / v3 codec、chain hash、公共 writer guard 不改。新增精确 v4 价格 record、`ReadonlyNotificationEventRouteSpool` 与 `publish_mixed_notification_bus_prefix`。复用同一 source、指针、全局序号、不可变条目和文件锁 |
| `runtime_builder_signal.py` | router 的新价格生产 / route 独立默认关闭。现有策略继续路由。价格关闭后仍选择 mixed 历史 publisher / reader，不退回会拒绝历史 v4 的旧 v2 reader；notifier 同样继续完整历史复制和策略外发 |
| `notification_state.py` | 原 `replicate` 仍 legacy-only。新增 `replicate_mixed_notification_events`：精确核完整 v2 / v4 前缀及原 routed receipt，复用唯一复制游标和 outbox SQL。复制原 target 不另授权或重路由。新增第 7 节的权威应用及发送准入表 / 事务入口；没有第二队列 |
| `notification_worker.py` | 原策略 delivery 保持。新增精确价格 delivery；价格在原 lease 下先完成 provider 静态准备，再取得持久准入。只有本次成功准入的新回执可开始一次外发。UNKNOWN 和租约收尾保持 |
| `runtime_notification_providers.py` | 新增价格短 formatter 及精确已准入 delivery 分支。核原 outbox / attempt / target 与准入回执；凭据和 endpoint 来自原能力。准入回执不表示已经调用 transport |
| `paper_signal_consumer.py` | 原 `consume_signal_bus_to_paper`、策略 receipt 和入队合同保持。新增 mixed consumer：v2 走原策略 bind / queue / complete；v4 写同一 consumer 库的非交易回执并连续推进原游标，从不进入 paper queue |
| `runtime_builder_paper.py` | `paper_consumer_builder` 使用 bus 的 mixed 历史 reader；`paper_broker_builder` 使用 mixed spool reader。两者调用新增 mixed consumer。选择只取决于已安装的固定历史协议，不取决于价格求值、路由、写入或外发开关；paused 原语义保持 |
| Serving | 价格只进新领域投影；原策略模型及 publisher 的 legacy-only guard 保持。新增投影带已同步权威代、实际准入和原 attempt 事实，不能将准入称为已调用 |
| 原 prefix 统计 | mixed 全局前缀用新增 typed 回执，`upstream_complete=false`。旧策略 prefix / S4 不能从筛出的策略行假证完整；无法表达时显示不可用。纯旧输入原字节和结果保持 |
| 网页 | 本人 GET runtime / events、短状态和最近提醒；原规则命令 / 恢复不改。生成 API / 类型 / dist 由 root 串行接线 |

### 6.1 历史协议与价格开关分开

新 mixed reader 只接精确 v2 和 v4，逐条核原字节、payload hash、record hash、全局连续序号、可见时刻、source 和原 receipt。v3 继续由原 R07 reader 处理；新 writer 仍拒绝 v3。未知 family 和宽松 fallback 均拒绝。原 v2 / v3 codec 和公共 legacy writer 不放宽。

mixed publisher 验证已有整个 mixed 前缀。随后可发布原策略和受信 bus 已提交的价格 routed record；后者必须有精确原价格来源 / route 回执，不能现场创建或修改。该历史搬运不要求当前价格 activation 仍有效。没有原回执的 v4、价格伪装 v2、跳号和错源仍拒绝。即使关闭全部价格开关，后续策略也能沿原全局顺序发布、复制和投递。历史 v4 不删除、不跳过，也不触发价格外发。

记录落盘而指针未推进时，只核同一字节并恢复指针；复制回执丢失时只核原 payload / receipt / target 后继续同一游标。固定协议安装和价格能力开关是两回事。新增表仅在获准的私有验证库走显式版本安装；没有在本片迁移生产库。

### 6.2 paper 的连续非交易处理

`consume_notification_events_to_paper` 读取精确的策略 / 价格 record 联合类型，不先筛掉价格。整个有界批先核 family、来源、字节、可见时刻和连续范围；再按全局顺序消费。策略仍交原队列和 broker，不重新解释其动作。价格没有策略字段，不交 `PaperSignalQueueStore.ingest` 或 broker。

同一原 consumer SQLite 增加 `paper_consumer_non_trading_receipt`：保存 source / generation、global_sequence、event_id、价格 schema、原 payload bytes / size / SHA、原记录证明、`ignored_non_trading` 和实际核验 / 消费时刻。`complete_non_trading` 用原 `BEGIN IMMEDIATE`：检查全局序号恰为原 cursor + 1、两种 receipt 无序号冲突，写非交易回执并推进原 cursor，一次 COMMIT。原内部 `last_signal_id` 列仅存最后事件键；价格不伪造 `SignalEnvelope` 或策略 receipt。

COMMIT 前中断不推进；COMMIT 后回执丢失只核原非交易回执，不入队、不造新价格动作。重放要求 event / 原字节 / SHA / source / sequence 相同；缺口、改正文、来源回退、两表冲突均拒绝。新增 typed mixed receipt / summary 读两种回执；原策略 receipt reader 不混入价格。新增表用独立固定版本 marker 安装，原 consumer ID / fingerprint、source identity 和已存策略回执不重置。

`v2(1) → v4(2) → v2(3)` 必须仅入队两个策略，价格记录产生零订单；consumer 最后为 3，序号 2 有真实非交易回执。关价格后再追加 `v2(4)`，publisher、notifier、paper 仍推进到 4。每条策略只入队一次，原订单只由原策略决定。

## 7. 收件、排队和消息

`PriceAlertRecipientPolicy` 是服务端非秘密映射：owner → 具体 recipient_id / PushDeer 或 PushPlus。精确 SHA、已安装政策代和原 route receipt 留存；最多两个唯一 target，不接自动展开别名。已有事件不得换 owner 或 target 后再发。Web 和求值进程不接触 push 凭据。

### 7.1 取消界线是持久发送准入

取消保证以 notifier 库中**发送准入事务成功 COMMIT**为界，不以 provider 开始为界。设置保存、Serving 发布、notifier 应用新权威和发送准入是不同事实；它们不跨库原子。

运行端先从实际同代规则 / 名单及受信服务配置核得 `PriceAlertDeliveryAuthoritySnapshot`。其完整域、精确 source / manifest / 行摘要、源原版本、实际读取时刻、owner-policy 代 / SHA / targets 和 delivery 开关一次应用到同一 notifier 库的 `price_alert_delivery_authority`。应用用原 `BEGIN IMMEDIATE` 和 expected 本地 revision 的 CAS；本地 revision 成功后加一。旧读取不能覆盖已经应用的新代；同代异正文、源原版本回退或不可信 / 不完整域均拒绝。不能按哈希大小判断版本先后。无法取得可信 scope 时，运行端另持久记录 unavailable；不将旧代补成当前有效。

authority 保存完整有类型规则 / 名单行及实际摘要，不只有 hash head；准入在事务内读这些真实行核对。原 1 MiB 完整域预算不放宽，超界拒绝整轮。当前 authority 是「运行端已同步」事实，不冒称远端物理发布时刻。未应用的外部变更仍在等待同步。原 30 秒 scope 上限与精确规则有效时刻继续限制准入；不能用旧 cache 或只有相同哈希的自述证明 fresh。

worker 在原 lease 下完成有界 formatter、target / 凭据能力准备；最终核验后调用 `admit_price_alert_delivery`。该入口在同一 `BEGIN IMMEDIATE` 内复核当前 authority revision / generations 与完整 rule/version/member/owner/target、delivery 开关、freshness、TTL、原 outbox 和实际 worker / attempt / lease。成功写 `price_alert_send_admission` 并 COMMIT。authority 应用与准入因此有唯一先后顺序；再增加一次外部重读不能替代这个事务。

准入回执含原 outbox_id / global_sequence / event_id / payload SHA、owner / rule / member、冻结 target、实际 scope 代 / source 版本 / manifest / 行摘要、owner-policy 代 / SHA、本地 authority revision、原 lease worker / claimed attempt_no / 起止时刻和实际 admitted_at。回执只证明该次发送获准；它没有 provider 调用或送达的含义。`admitted_at` 是事务内实际核验 / insert 的逻辑时刻，不冒称物理 COMMIT 时刻；只有成功 COMMIT 后读回才取得调用能力。调用前原 TTL 和 lease 仍须有效。

新 authority 先应用：expected revision 失配，旧准入不提交；重取当前可信行后，停用 / 删除 / 版本或名单失效 / target 撤销的旧事件终止。保留原死信及 typed 取消证明。未 claim 时 attempt 不变；已 claim 且从未准入 / 调用时，原计数回退一，和死信 / 取消回执同事务完成，原已有 attempt 不删除。来源未知则不准入；未尝试 lease 沿原 `release_unattempted` 释放，不作为 provider 失败。

准入先 COMMIT：随后停用、换版本、名单变化或收件人撤销不能撤销该次准入，即使 transport 尚未开始，也可能发送一次。更新仍阻止之后的新准入；没有承诺保护核验到 provider 开始的原子区间。若本次 transport 明确未接受而按原链重试，新的 attempt 须按最新 authority 重新准入，不能复用旧准入绕过撤销。

### 7.2 中断和文案

store 只给本次成功 COMMIT 的新结果生成单次 `PriceAlertAdmittedDelivery` 能力，绑定实际 store、原 outbox / lease / attempt 和回执；旧结果不生成能力。worker / provider 拒绝自构造模型、任意 fresh bool、反序列化回执替代能力及同能力二次调用。只有新能力、原 lease 和 TTL 均有效时可调用一次 transport；读回旧准入不是再次调用的许可。准入前事务失败 / 回执未知须先查原回执；不得猜成功后调用。准入后中断或回执丢失，若无法证明 provider 未调用，沿原 UNKNOWN / 租约死信或过期恢复，不自动重发。准入本身不插入伪造 provider attempt；实际结果仍写原 delivery_attempt。外部是否已接受、实际调用数与 durable 准入分别记录。

| 实际事实 | 用户短文案 |
|---|---|
| 设置已保存，新权威尚未应用 | 等待同步，已排队的提醒可能发送 |
| 已应用，旧事件尚未准入且取消落盘 | 已取消 |
| 旧事件已准入，外部请求尚未确认 | 已准入发送，可能仍会发送 |
| 原 provider 明确接受 | 已提交 |
| 只有 shadow receipt | 仅记录 |
| 准入后中断或请求结果不明 | 结果未知，不会自动重发 |

Provider 失联、旧 client false、超时或成功回写不明继续用原 UNKNOWN；只有明确未接受才用原退避。不得清 UNKNOWN、自动换 target，或把「已提交」写成手机已送达。

短通知仍只含股票、规则短名称、到价方向、真实价格、阈值、上海观察时刻。名称最多 80 字，单行纯文本，清除控制符 / Markdown 主动结构；正文最多 1 KiB。不含 owner、ID、版本、哈希、服务名、准入证据、路径、target、凭据或 provider 原错误。页面两位价格的 Tip 保留精确值；外发用精确价格文本，超界拒绝，不偷偷改值。

## 8. 本人页面与状态

新增 `/monitor/price-rules/runtime` 和 `/monitor/price-rules/events` 两个私有 GET；复用受信 ingress / `current_user`，拒绝 owner 参数、裸身份头、TCP 绕过及额外 query。events 固定最近 20 条，不作全历史分页。Web 只读 Serving，不读 bus、producer SQLite、quote 文件或 `.env`。

投影包含实际最近轮次、规则版本、范围计数、报价来源 / 时刻、求值三态、触发 / 冷却、路由、已同步权威代、准入 / 取消及原 attempt 状态。准入和实际外部结果分开；「已取消」必须有准入前的持久取消回执。runtime 代、rule / member 代、来源 receipt、计数 / SHA 和时刻逐项核对。晚于本轮的事件与 attempt 不可见；混代或指针变化返回 409 / 不可用，并撤下旧统计。旧提醒是历史事实，不冒充当前规则结果。

| 实际事实 | 页面短词 |
|---|---|
| runtime 未开放或未运行 | 未运行 |
| 已知休市 / 开盘前 / 收盘后 | 等待开盘 / 已收盘 |
| 新鲜真实轮次、完整应评估域、价格和收件能力可用 | 正常 |
| 缺行情、范围失效、无收件能力、通知未开放 / shadow | 注意；一句原因进 Tip |
| 明确账本冲突或运行失败 | 异常 |

enabled 只是意愿。旧历史 prefix、bus 游标和零触发不证明当前覆盖。界面分别显示「最近核对」「最近触发」「通知记录」；缺事实显示「—」。正常等待不标红。没有真实可用输入时不能仅凭假 provider 成功显示真实正常。

沿用原 `DataTable`、`StatusBadge`、`Tip` 和相对时间。1440 / 390、键盘焦点、手机点按、owner 切换与迟返回均验收。内部字段只在必要详情 / Tip；规则抽屉和原命令恢复不受运行查询覆盖。

## 9. 实施、写集和验收

1. 先实现精确价格模型、激活和 PAR-01～05 红测；保存旧 schema / 原件字节基线。不要改原策略模型。
2. 实现 scope / 报价适配、生产者事务和频率；通过 PAR-06～08、19、22。所有失败保留原回执与未决记录。
3. root 串行接 bus、route、mixed spool、replicate、worker / provider；用 PAR-09～15、21 核对每一跳和旧行为，不建立第二队列。
4. 接有类型投影和本人 GET / React；通过 PAR-16～18。C12 仍有效证据复用，只验本次运行状态接线造成的影响。
5. 按 PAR-20 实际跑公共入口的完整隔离链；冻结完整 diff，再安排一次独立最终审查。修复只针对集中 finding 及受影响回归，不追加每层审查或全仓 sweep。

**owned：** 新建 `price_alert_runtime_contracts.py`、`price_alert_runtime_source.py`、`price_alert_runtime_store.py`、`price_alert_runtime.py`、`price_alert_route.py`、`price_alert_runtime_projection.py`、`runtime_builder_price_alert.py`；新 Web runtime 模型 / 读取 / routes，新 `PriceAlertRuntimeFacts.tsx` 及直接测试。

**root 共享写集：** 原 `signal_bus.py`、`signal_route_spool.py`、`notification_state.py`、`notification_worker.py`、`runtime_notification_providers.py`、`runtime_builder_signal.py`、新增直接接线 `paper_signal_consumer.py` / `runtime_builder_paper.py`，以及原 `runtime_service_entrypoint.py`、`runtime_service_builtin.py`、`serving_read_models.py`、`serving_page_projection_source.py`、Web app / 原 monitor 规则组件、生成 API / dist。混合 publisher 在原 spool 文件扩展，ServingPublisher 不加并发闸门。若实现者编写这些 patch，先冻结，只有 root 串行应用；不能并行改共享文件。精确接口、guard 和测试归属见写集文件。

**保持：** 原 rule store、PageControl、私有 UDS、C12 command / session、legacy/current 信封、原 strategy route / rotation、现有通知传输、旧 monitor / surge / pulse 业务数学。原 Serving 信号模型与 source payload schema 不扩成价格模型；只新增领域投影合同。

矩阵每项都有输入、中断点、明确拒绝 / 数量 / 字节断言和直接测试文件。当前均未运行。实施时复用共享 Python 环境、显式本树 src、禁 `.env`，先跑直接测试；按共享契约要求补相关回归。前端接线后核 OpenAPI、类型、构建、体积和 1440 / 390 浏览器。未启动、skip 或仅回放均不记真实运行通过。命令、版本、退出码、资源和自有临时目录 / FD / 子进程收尾须落项目证据目录。

## 10. 本 goal 后续必做片

| 后续片 | 精确依赖与本片边界 |
|---|---|
| 通用条件和其他范围 | 新严格配置版本；已有日线积木 / intraday feature 的实际可见性、昨收、流通和涨停事实。池 / 板块 / 全市场需完整实际成员来源与日期，不能最新名单回填历史 |
| 每次 / 每根 K 线 | 新频率 mode 版本；已收盘 bar ID、available_at、频率 state migration。未知 mode 本片拒绝，不按 cooldown 猜执行 |
| 30 秒合并、恢复、断源策略 | 保留原 trigger IDs、同 owner / recipient 授权、窗口端点、原始 trigger 与合并消息关系；恢复须有实际连续正常证据，不能把断源当条件恢复 |
| 旧四类对照与迁移 | monitor 四种 attack、surge 实际同刻累计、pulse 实际全市场计数语义及原价格 / 名单来源。先原语义多日回放对照，再取得切流 / 停服授权 |
| 受控测试推送 | C12.3 私有命令、较重确认、固定白名单入口、每 10 分钟最多一次、原 channels 和短回执；安装实际一次性 unit 另按生产授权。不用假价格事件充测试 |
| 模式切换及正式运行 | C12.4 两步确认、精确 mode 版本、paused / suppress_delivery 原合同、Linux 真实身份与配置证据。shadow 不标外发成功；不沿本片偷偷发真实通知 |

本次只补 PAR-SPEC-01 / 02，原冻结和失败记录保留。原 reviewer 定向复核后由 root 冻结实施。当前没有产品测试或真实运行通过；没有新增用户产品决策或行政时长门槛。
