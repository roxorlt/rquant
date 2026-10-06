# 数据中心完整接通规格

**状态：待独立规格审查。尚未写产品代码，尚未运行本片测试。**

**本次修订：** 集中处理 DC-SPEC-01 和 DC-SPEC-02。原审查记录和上一版 packet 保留。四个服务状态词和硬链接 ctime 规则一并澄清。仍待原审查者定向复核。

**目标：** 完成 C1.1–C1.4 的采集联动、日线回补执行和财务来源装配。用户从原数据中心页完成操作，并看到实际结果。

**实现方式：** 原生 Codex。在同一工作树实现。规格通过后授权写集。最终候选做一次独立审查，定向处理实际阻断项。

**架构：** 原采集器和原数据事务负责事实；原任务库负责受理和恢复；原审计器负责统计；原 Serving 和有类型网页 API 负责读取。页面不计算第二套数据口径。

**技术栈：** Python、Pydantic、DuckDB、现有 SQLite 任务库、Parquet、React、TypeScript。原财务 PIT、日历、指标和证券状态计算保持单一来源。

本片为高风险任务。原因是主库写集、崩溃恢复、来源证明和写者互斥。本次没有固定执行时长或修复次数上限；范围仍限于本规格。

## 1. 当前事实

下表是源码事实，不是新增能力的通过证据。逐文件校验值见 `spec/source-freeze.json`。

| 现有入口 | 已有能力 | 本片补充 |
|---|---|---|
| `data_audit_report_jobs.py` | 一个持久 SQLite 审计队列；完整请求；幂等键；租约；封存结果核验 | 同一队列接收受信任采集证明，保留旧请求 |
| `data_audit_report.py` | v1 日线、v2 的 24 类统计；固定副本；文件摘要；报告最多 8,000,000 字节 | v3 逐数据集采集证明；统计继续用原算法 |
| `DailyCanonicalPublisher.publish` | 原事实与 canonical 回执同一事务；来源、日历、数据库、各表内容和 fencing token 绑定 | 消费实际回执；公开复用原表内容摘要函数 |
| `cli.cmd_run_daily → ingest.ingest_daily` | 正式旧服务走这条入口；事实在一个原 DuckDB 事务提交 | 可选提交记录器写同事务来源回执，默认 `None` |
| `runtime_builder_daily_orchestrator.py` | 明确只运行 Shadow DAG | 保持影子身份。不能证明正式采集完成 |
| `BackfillPlanJobStore/Worker` | 原只读计划队列、精确空洞日期、不可变 v1 计划 | 原 `BackfillStateStore` 执行精确计划 |
| `market_backfill.backfill_market_daily` | 原取数、证券 PIT 状态、逐日事务 | 抽取逐日准备；受控入口不遍历区间、不覆盖正确既有行 |
| `FinancialArchive/acquire_financial_batches` | 七接口规范、不可变观察、首次观察时钟、单步恢复 | 实际客户端、有限任务清单、正式运行装配 |
| `import_financial_archive/derive_fundamental_daily` | 有界分页导入、原六字段计算、不可变修订和 head | 接到原维护执行入口；持久记录运行结果 |
| `workload arbiter` | research 与 maintenance 排斥；maintenance 之间可并发 | 不能代替主库 writer 排他锁 |
| `DuckDBStore/research-sync/backup/replica` | DuckDB 原文件锁；另有复制与直连入口 | 配置同一个主库 writer guard，启用前核验全部参与者 |

目录 24 类及其原覆盖、迟到、空值和质量统计继续使用原合同。未收到有效采集证明的类别显示“待核验”。事件类和具名研究分区不改成全市场每日采集。

## 2. 三条实际链路

```mermaid
flowchart LR
  A[原采集器] --> B[原事实事务和来源回执]
  B --> C[原副本刷新和固定内容证明]
  C --> D[原审计队列]
  D --> E[原审计 worker]
  E --> F[封存报告]
  F --> G[原 Serving 发布]
  G --> H[有类型 API]
  H --> I[React 数据中心]
  I --> J[原 PageControl Journal]
  J --> K[原 BackfillStateStore]
  K --> L[维护执行器]
  L --> A
```

### 2.1 采集完成联动

1. 原采集器完成实际取数。记录原请求、响应内容、证券/日期范围、来源版本和实际观察时刻。仅保存成功且已核验的结果。
2. 原事实写者提交事实。提交记录器与事实共用原 DuckDB 事务。`DailyCanonicalPublishReceipt` 不改格式；原 legacy ingest 和其他目录采集器可选增加 `IngestionCommitReceipt`。没有记录器时保持原行为，不能新增“已核验”状态。
3. 原副本刷新取得公共 writer guard。核对主库、WAL 和提交回执。继续原 cp/checkpoint/verify/atomic rename，绝不替换运行中的主库。
4. 为该实际副本发布 v2 采集来源证明。它是独立的伴随证明，不改写原 `ReplicaGenerationMetadata` v1 sidecar。旧读者继续使用原 sidecar，但它不能单独支持采集完成声明。新证明绑定原 sidecar 摘要、完整副本 SHA、物理身份、源库身份、SSE 日历、可见截止、目录合同及逐数据集回执。
5. 在固定证据目录保留该副本 inode 的只读硬链接。只有同文件系统才使用硬链接；不回退成无界复制。原审计请求固定这个名称与 inode，不能在重启后改读新副本。
6. `DataCollectionBridge` 提交原审计队列。键来自规范事件身份；完整请求仍由原队列持久保存。证明文件是封存输入，不是第二个任务队列。
7. 原 worker 同时核对请求、来源证明、原回执、固定副本内容与日历，再调用原统计函数。v3 报告按数据集记录“已核验 / 部分核验 / 待核验”。
8. 原 Serving 来源读取器同时核对成功任务和封存报告，再发布原统计及新的采集状态。网页只读这份发布结果。

**硬链接身份：** 新固定名称建立后，才封存提交时的文件身份。沿用原 `AuditReplicaFileIdentity` 和完整 SHA 核验。审计器内部创建、移除临时硬链接会改变同一 inode 的 ctime；这不是内容变化。首次执行仍按原身份严格检查；恢复只有原任务已耐久记录摘要时，才按原 `_same_pre_pin_identity` 和 `expected_file_sha256` 路径处理 ctime 差异。每次均须保持 device、inode、size、mtime 和完整 SHA 一致；活跃读取中仍按原 FD/path 身份检查。新来源核验不额外要求 ctime 永远等于证明发布时，也不单凭 ctime 放宽其他检查。没有耐久摘要或只有调用方解释时拒绝。

**恢复规则：** 审计受理的成功响应丢失时，先查原键，再读当前源。不能给同事件创建第二个请求。队列忙或冷却未结束时，原副本证明保持可续查；runner 每轮最多处理一份未受理证明。证明的顺序和任务承接位置保存在原审计 SQLite 文件。队列接收和承接位置推进共用一个 SQLite 事务。不是单凭扫描目录猜测完成。

**身份规则：** `event_id` 固定 collector、原运行/提交身份、目录合同、声明范围和源代。`binding_sha256` 固定实际内容、回执、日历和副本。相同 `event_id`、不同 binding 拒绝；完全相同返回原任务。例行复制相同内容不生成新采集事件。续查返回原固定文件，不把新 inode 写入旧请求。

**逐类规则：** canonical 回执只证明其实际表和日期。legacy 回执只证明原事务实际写入的表。原 `DatasetSnapshotBinding` 只证明它列出的湖分区和完整性范围。财务回执只证明列出的接口请求与观察。缺证券全集、分钟网格或历史完成证据时，原完整率结论仍未知。一次审计完成不等于 24 类都采集完成。

### 2.2 回补执行

1. 原计划继续是 v1，不改内容摘要、不把 `executable=False` 改成 True。执行资格由独立运行政策和执行受理决定。
2. 新生成计划的原 job request 追加可选 owner/原 command 身份。默认 `None`，旧计划仍可读。无 owner 证明的旧计划不能执行；用户可通过原生成入口获得当前 owner 的新计划。
3. 第一操作 `prepare_backfill_execution` 走原 PageControl Journal。服务端从原成功 plan task 读封存文件，核对 owner、plan hash、原副本证明、SSE 日历、真实额度和 writer 参与者。建立只读预览意图，不启动取数或写库。
4. 第二操作 `execute_backfill_plan` 也走原 Journal。请求只带原预览身份、原 plan task/hash、原命令身份和确认值。预览有效 5 分钟，绑定 owner、写集和运行政策代。受理时重新读取当前权益、政策和原额度，不把预览余额当保留额度。确认被使用一次；原命令重试返回同一执行，不再次消耗确认。
5. `BackfillStateStore` 在原 SQLite 事务建立受控 manifest、精确 day tasks 和唯一活动维护槽。重复绑定返回原 manifest；已有不同活动执行拒绝。状态槽是原任务库的事务索引，不是新队列。暂停/恢复命令也在原 Journal 受理，在同一原任务库按 owner、manifest、控制序号做 CAS；暂停请求在日期/派生批次边界生效，不能把“已请求暂停”写成“已暂停”。
6. 维护 worker 从原租约取一个 day task。首次执行和恢复均先核当前权益、政策及原 dispatch 账本，再装配必须绑定原 `QuotaBoundTransportObserver` 的原适配器。读取该日 daily、daily_basic、adj_factor 和原证券状态；每个实际 SDK dispatch 按 2.2.2 的同一协议受控。日期越界、重复主键、非有限数值、截断或缺关键来源时拒绝整日。
7. 获取公共 writer guard，再进入 2.2.1 的原 SQLite 提交保护区。重新核对 claim、维护槽和实际主库。日线整日必须仍为空，或已有本执行的原事务回执。外部进程补了同一天且无本任务回执时拒绝，不能冒认成果。
8. 只写封存计划的日期。正确既有关联行不更新；缺行插入；同键不同内容拒绝。不能直接调用原覆盖式区间 CLI。原取数和证券状态准备抽成共用函数；原 CLI 默认覆盖方式不变。
9. 原日事实、必要派生失效标记及 `BackfillDayCommitReceipt` 共用一个 DuckDB 事务。回执绑定 owner、执行、原 plan、day task、提交时 claim token、保护区维护槽版本、源响应、源库、日历及逐表内容。从核 token 到 DuckDB COMMIT 均保持原 SQLite 写事务，不能在 COMMIT 前放开接管。
10. DuckDB COMMIT 后，在同一受保护 SQLite 连接登记原 task 成功，再结束 SQLite 事务。主库提交成功但响应/SQLite 写入失败时，后继有效 claim 按 2.2.1 核实旧提交回执。回执中的旧 token 不要求等于接管后的新 token。完全一致才续记成功；回执丢失、I/O 错误或内容冲突保持待确认，不能重新覆盖。
11. 所有计划日期提交后，按原受影响证券和最早日期重算派生尾部。使用原 `derive_target_daily_indicators` 与 `recompute_daily_state(status_mode='verified_no_fetch')`，按固定小批次续跑。没有新的计算口径。
12. 释放所有主库连接后刷新原副本，再提交原审计任务。全部 dates、派生尾部、副本和新审计都成功，才写原 manifest 的完整终态。存在问题或缺来源时显示部分完成。

**时间窗：** 以注入时钟转换上海时间。周末允许；工作日 17:50 起允许，次日工作日 08:20 停止接新 day/派生批次，08:30 释放 writer guard。夜间窗口属于前一允许晚间，不用自然日期切换误判。工作日 08:20–17:50 不开始执行；可以生成计划和查看结果。

**截止处理：** 网络读取不持主库锁。每个原请求有有限超时和已有重试上界。剩余时间不足本批预算时暂停同一任务。维护 runner 在 08:28 请求 worker 停止并中断未完成 SQL；08:29:30 前未退出则终止该执行子进程，并回收进程与 FD。08:30 前核对实际锁已释放。中断中的事务结果按原回执续查，不能按退出码推断。此处是生产安全截止，不是限制本次开发时长。

**状态：** `queued → running → paused / partial / failed / verifying → completed`。暂停保留原 manifest 和原写集；恢复同一任务。失败不自动生成新计划。`completed` 要求完整终态证明，而不是“最后一次取数返回成功”。

### 2.2.1 原 claim 的提交保护区

选定最小协议：沿用原 `BackfillStateStore._write_transaction` 的 `BEGIN IMMEDIATE`，将其保持到本批 DuckDB 提交之后。新增 `commit_claim` 上下文和连接内成功登记助手；原 `mark_task_succeeded` 复用同一助手，不嵌套 SQLite 事务。不新增排他锁、队列或 claim 算法。

1. 所有取数、退避、响应校验和派生输入准备先完成。它们不持主库 gate 或 SQLite 写事务。接管后旧 worker 的材料不获得提交权。
2. 顺序是 workload admission → 公共 writer gate → 原状态库 `BEGIN IMMEDIATE` → 核验并延长当前 claim/维护槽 → DuckDB connect/事务。SQLite busy 使用有界超时，最多 5 秒且不得越过生产截止；取不到则释放 gate，在下一轮续查。禁止持 SQLite 写事务等待公共 gate。
3. 在同一 SQLite 连接核对 owner、manifest/hash、day task、当前 token、未到期租约、物理主库、政策代、维护槽版本及暂停/终止控制。错链、旧 token、已过期或暂停已生效时，不开始事实事务。进入时在本连接延长原租约，仍是 120 秒。
4. 保持此 SQLite 写事务。原 `claim_task`、renew/release、任务成功/失败、abandon 和新增维护槽/控制 CAS 都通过同一状态文件的原写事务。第二 worker 可等待或收到 busy，但不能在保护区内换 token、槽版本或终止该 manifest。只读查询不获得提交权。受控 manifest 不允许另配一个状态库。
5. 用已持有 gate 的原 writer factory 开库。日线事实和回执原子写入。COMMIT 前用同一 SQLite 连接再核 token/槽/政策、当前时钟和原租约；过期、跨截止或停止则回滚整批。保护区内不让另一条 SQLite 心跳写入自己阻塞；续租只使用当前连接。事实事务不跨网络请求，停滞仍由原截止监督器中断/终止并回收。
6. DuckDB COMMIT 成功后，事实成为可恢复结果。先在同一 SQLite 连接写原 task 成功，再 COMMIT SQLite；随后关闭主库连接并释放 gate。DuckDB 已成功而 SQLite 登记、COMMIT、连接关闭或返回失败时，不能删事实，也不能按异常类型猜测未提交。
7. 保护区释放后，新的有效 claim 可接管。它取得相同 gate 和 SQLite 保护区，读本执行、同 owner、同 plan、同 day 的原 DuckDB 回执，并逐表核对实际内容、源响应、日历和提交身份。旧提交 token 是原有效提交的证据；新 token 只授权本次续记。完全一致则在当前连接标成功，不重新取数或写日事实。不存在可核实回执才走原失败规则；I/O 失败或同身份异内容保持待确认。

该规则同样用于本维护清单内的派生/财务事实批次。批次保持原算法和原事务；提交保护只增加原 claim/维护槽核验与耐久结果绑定。原非受控 manifest/CLI 不自动加入新政策。

### 2.2.2 原额度与实际 SDK dispatch

预览只核条件，不预支额度。当前权益证明来自受信任运行配置；绑定实际账号身份、接口、参数范围、证明来源/摘要、有效截止、政策代和原 quota source/window/ledger 身份。网页不能提供这些值。缺失、过期、来源未知或无法证明实际额度时，受控执行关闭。不能把逻辑操作估计、另一接口成功或 SDK dispatch 数当供应商扣额证明。

1. execute 受理、claim 开始和每次恢复，都重读当前权益及政策，并续查原额度账本。排队后余额不足或证明过期时暂停原 task。预览身份、manifest、owner 和请求 ID 保持不变。
2. 每个取数请求封存 `BackfillSourceRequestBinding`。稳定 `logical_request_id` 由原 manifest/plan hash、固定 scope、API、规范参数和证券/日期范围计算。它不含 claim token、进程 ID、启动时刻、quota window 或临时剩余值。换 claim、跨窗口或重启不换请求 ID。
3. 原 TushareAdapter 显式绑定原 `QuotaBoundTransportObserver`；受控入口不接受 observer 为 `None` 的 adapter。一个有限请求范围对应一个原 observer scope。每个 dispatch 前的薄保护先核当前运行政策、该接口权益、请求范围、窗口和原 claim；随后调用原 `observer.observe`。原 `SourceQuotaStore.begin_transport_dispatch` 在自身事务中原子检查当前余额、扣记并绑定稳定 attempt，再调用 SDK。检查失败不调用 SDK。不能只在外层扣一次后放行 backoff。
4. 每次原 `_call_with_backoff` 重试均重复上一步，原额度失败/冲突继续直接抛出。原六次上限按同一请求的耐久 dispatch ordinal 累计，不能每次重启重新获得六次。没有额度不换 token、不换账号、不增 quota source、不清账本或另发 ID。政策/额度窗口与原账本不一致则拒绝，不用新上限重置已有窗口。
5. 覆盖实际 `daily_by_date → daily`、`daily_basic_by_date → daily_basic`、`adj_factor_by_date → adj_factor`，以及原证券状态实际所需 `namechange_raw → namechange`、`stock_st_raw → stock_st`。原日历若需刷新，则 `trade_cal_raw → trade_cal` 也必须经 observer；已经封存并核验的原 SSE 日历不再取数。原 helper 不需要的接口不额外调用；如实际选择原 suspend/证券基础入口，则先给该接口同等绑定，未绑定不得 dispatch。
6. `trade_cal_raw` 当前直调 SDK。最小共享改动是在已有 observer 时走 `_transport_call`，额度失败/冲突直接抛出，禁止落入备用 token 分支；受控 adapter 设置 `backup_token=''`。observer 为 `None` 的旧路径和原参数/归一化不变。其余三张日线和证券状态继续用已有 observer/backoff 接缝。
7. 原 scope 新增可选 `next_call_ordinal=1`、`resume_api_name=None`。默认路径不变。受控恢复先从原 ledger 核对同一 source/request/API 的连续旧 ordinal；只有全部既有尝试都是已确认 `FAILURE`、下一 ordinal 尚不存在，才从下一位置续试。observer 对该恢复位置再次校验，拒绝跳号、API 不同或遗漏尝试。受控入口另核下一位置不大于 6。成功响应及原材料已经耐久时直接复用，不重发。
8. dispatch 之后响应丢失、PENDING/UNKNOWN、成功但响应材料/归档丢失，均暂停同一 task，并保留账本消耗和原身份。原 observer 遇已存在 attempt 不再次调用 SDK。原 stale-attempt 恢复可以记 UNKNOWN，不能退款后复发。FAILURE 若实际是超时或传输响应不确定，也按未知响应阻止自动续试；只有可证明的拒绝/失败结果才允许第 7 步。原 six backoff 只在当次已知失败范围继续；未知异常在受控薄保护转成原 `SourceQuotaConflictError`，原 backoff 随即停止。不得为解除未知新建 request、manifest 或换 token。后续只能用原耐久响应/供应商证明解决同一身份的不确定性。

财务装配使用同一 dispatch 前政策核验和原 ledger 规则。财务 query ID、首次观察和原 archive 恢复规则仍保持原合同。HTTP 请求数和供应商真实扣额无法观测时继续为未知。

### 2.3 财务实际装配

1. `FinancialCollectionPlan` 固定 owner、原证券范围、报告期/公告窗口、接口权益证明、运行政策和 code commit。任务仍进原 `BackfillStateStore`。
2. 原 TushareAdapter 增加七个薄方法。仅白名单接口和已核验参数。复用 `_call_with_backoff` 和 `QuotaBoundTransportObserver`；每个可观测 SDK dispatch 在原 quota ledger 记账。没有新客户端、备用 token 绕行或第二账本。
3. 原 `acquire_financial_batches` 消费有界请求组。请求 ID 来自原 manifest+任务+query；恢复先读原 archive 回执，禁止因网络重试重置首次观察时刻。
4. 原 `FinancialArchive` 继续封存、单步恢复和 A→B→A 修订。空响应、可能截断、缺公告、权限失败分别保留实际结果，不能标全市场覆盖完成。
5. 取得公共 writer guard 后，原 `import_financial_archive` 按原 32 条分页导入。原 cursor、批记录、观察行同一原事务。只接收原 archive 的有效连续 successor。
6. 原六字段每日派生按证券/日期任务运行。日线估值观察仍经原 `daily_valuation_pit`，来源不完整时保持未知。原版本/head/回执检查不变；不得回退较旧报告期或补零。
7. 本批导入与派生结果形成可续查的运行证明；逐接口分别记录实际权限、额度、请求范围、记录数、缺数原因及最近可用时点。再接到 2.1 的原副本和审计链路。
8. 定期运行通过原维护 runner 的显式配置装配。旧默认没有财务任务；启用时建立有限且可续查的工作清单。历史请求按报告期、最多 31 自然日公告窗或 dividend 单日分组，不能自动无界翻页。

**权益和额度：** 未获得实际账号权益证明或额度未知时，政策保持关闭。合成测试只能证明链路。SDK dispatch 数和供应商 HTTP/扣额分别显示；无法观测的内部重试及真实扣额保持未知。一个接口成功不证明其余六个接口有权限。

## 3. 公共 writer 边界

公共 gate 配置默认为 `None`。新增回补/财务执行政策默认 `False`。生产启用需要固定主库身份、固定 gate 身份、参与者清单和实际安装证据。

| 参与者 | 最小接线 | 核验重点 |
|---|---|---|
| 日线、monitor、普通 CLI 和临时脚本 | 写模式 `DuckDBStore` 在原 connect 前取得 gate，原 close 后释放 | 原主库全部实例；读副本不取 gate |
| canonical publisher | 使用已接 gate 的原 writer factory；保留原 candidate 锁和 ledger fence | 不能用 candidate 目录锁代替公共 gate |
| research-sync / restore | 原 `_rescue_stale_wal` 和直连主库生命周期使用同一 gate | 保留原 backup 分家和 WAL 恢复，不换主文件 |
| backup / replica shell 入口 | 原 maintenance arbiter 之后取得同一 gate，覆盖源库复制和主库证明读取 | cp 也必须参与；不修改跨 plane 语义 |
| 新维护 worker | 原 writer factory；显式持有的 lease 可被 store 借用 | 禁止二次 flock 自己锁住自己 |

gate 使用固定普通文件和 pinned 父目录，`O_NOFOLLOW`、单链接、正确 owner/mode，并核对路径与 FD inode。`flock(LOCK_EX|LOCK_NB)`，忙时等待下一有界任务轮，不在主库连接内睡眠。不同路径指向同主库不能绕过 gate。释放、constructor/schema 失败、close 失败均在 `finally` 关闭已拥有 FD。

普通写者的锁顺序是原 workload admission → 公共 writer gate → 原 DuckDB connection/transaction。受控维护写者在 gate 和 DuckDB 之间加原状态库 `BEGIN IMMEDIATE`，按 2.2.1 持有至事实提交与任务登记。claim/admission/控制等短 SQLite 事务不得在事务内请求 gate。原 quota 事务在网络侧独立完成，不持上述主库/状态保护区。canonical 局部锁仍只管原领域重入，公共锁非阻塞，不在局部锁下无界等待。外部 raw `duckdb.connect` 不受应用 gate 管理；启用清单须覆盖所有已安装的实际写者，DuckDB 原锁作为最终拒绝。未知写者或未核验 wrapper 使新增执行政策关闭。

固定 guard 目录不是配置里随意指定的第二锁。运行配置必须绑定它的物理身份和 canonical DB；失配、符号链接、硬链接换名、目录换代或主库 inode 改变拒绝新写入。可信服务 UID 有权限整体改写资料的攻击不在本片对抗范围，但误配置和普通竞争必须拒绝。

## 4. 合同和容量

所有跨层数据使用冻结 Pydantic，`extra='forbid'`，严格日期、UTC 时刻、非有限数值拒绝。浏览器不能提交路径、table 名、额度、collector 身份、来源摘要或 SQL。命令 ID、owner、原 plan/版本和政策代都在服务端核验。

| 对象 | 上界与处理 |
|---|---|
| 原审计跨度/报告/队列 | 保留 3,660 自然日、8,000,000 字节、4,096 jobs、32 events；不扩大原冷却规则 |
| 原审计请求 | 保持 16 KiB；只增加固定证明引用，完整证明单独封存并核验 |
| 采集证明 | 最多 24 类、每类最多 16 个原回执引用、总计 256 KiB；大历史范围分成有限事件 |
| 固定副本 | 最多 2 个未完成 pin、每个最多 64 GiB；先核验实际大小及磁盘余量；硬链接不复制全库 |
| 已完成 pin | 原任务和报告都耐久、回执可读且无活动 claim 后释放；失败/未知 pin 保留，阻止新增超容量事件 |
| 回补计划/清单 | 保留 v1 的 8,000,000 字节和 3,660 日期；manifest 最多 8 MiB、4,096 任务；逐日一次只保留一份准备材料 |
| 原日响应 | 每表最多 8,000 行、128 列；当日原料累计 32 MiB；溢出/触顶的可能截断拒绝，不截掉后算完成 |
| 主库维护 | 每个物理主库只有一个活动受控维护执行；租约 120 秒，heartbeat 40 秒；短提交区间用原 SQLite 写事务排除接管，提交前核原 token/租约；提交后恢复允许核实旧 token 回执 |
| 回补 dispatch | 每个稳定请求最多 6 个耐久 SDK dispatch；先核权益/政策，原 ledger 每次原子扣记；未知响应暂停原身份，不重置计数 |
| 派生尾部 | 最多 250 只证券一批；精确受影响日期；材料有限分页，不将 10 年全市场载入内存 |
| 财务单组 | 沿用默认 32 requests、16 symbols、5,000 rows/response、8,000,000 bytes/batch、256 columns |
| 财务总清单 | 固定证券范围最多 8,000，只允许一个 3,660 自然日范围；最多 100,000 个原 query，按最多 32 个一组形成最多 4,096 个原 task；manifest 最多 8 MiB。task 固定原范围与查询位置，query ID 由封存 scope 和该位置确定，不由新响应扩张。超过容量时先拆成明确子范围；不能受理后静默删请求 |
| 财务保存/导入 | 保留原 64 MB snapshot、16 MB anchor、1 KiB anchor line、32 条分页；额度满则暂停，不能清空旧历史重跑 |
| 页面状态 | 每页最多 50 条任务，最近 20 条事件；owner+固定源代 cursor；旧代/跨用户请求拒绝 |
| 新 Serving 投影 | 24 行采集状态、1 行执行概况、50 行任务索引和 20 行事件；新增合计最多 512 KiB，仍计入原 owner/总预算 |
| 网页命令/回执 | 命令最多 4 KiB，回执最多 16 KiB；未知状态保留原 ID，不能新建命令猜测成功 |

上述数值是输入硬上界，不是“可在任何主机全部同时运行”的性能声明。容量用最大允许输入、独立峰值和实际主机资料核验；不调整用户已冻结的全平台资源合同。

## 5. 威胁与失败模型

**资产：** 原主库事实、不可变财务历史、原任务和命令身份、SSE 日历、固定副本、封存报告、完成状态。

**信任边界：** 浏览器 → 受保护 API → 原 PageControl Journal；受信任 collector → 原数据事务；原事务回执 → 固定副本证明；原审计任务 → 封存报告；只读 Serving → 网页。生产配置和凭据留在既有权限边界，不进网页。

**主要失败：** 重试换 ID、同身份换内容、早报完成、副本换代、回执读取失败、事务返回丢失、SQLite 续记失败、租约过期、两写者竞争、复制撞写、日历错链、跨窗、额度不明、后到修订提前可见。

必须保持以下不变量：

1. 未核验来源不能变成“已采集完成”；完成只覆盖证明里的数据集和范围。
2. 实际事实与提交回执原子提交；请求受理与任务身份耐久，重启续查同一身份。
3. 不可变计划和财务版本不被改写；同身份不同内容拒绝。
4. 回补不扩大日期，正确既有事实不被替换；无本任务回执的外部写入不能当本任务成功。
5. 同一物理主库的实际参与者共同排他；截止前停止新写入并释放锁。
6. 财务可见性同时服从原公告可见时刻和首次观察时刻；缺值保持未知。
7. 旧 v1/v2 报告、只读计划、原 CLI 和未启用配置保持兼容。
8. 页面 owner/源代改变后，不显示或恢复前用户的待处理命令及旧数字。
9. 从受控 claim 核验到事实 COMMIT，另一 claim 或维护槽不能接管。提交后当前 token 变化不能否定已核实的原提交。
10. 每个实际 SDK dispatch 都绑定当前有效权益、政策、稳定请求范围和原额度账本。未知响应不换身份重发，也不重新获得重试预算。

P0/P1 矩阵保存在 `spec/failure-acceptance-matrix.json`。此时每项状态均为“待实现/待验证”。仅标明拟用入口及预期，不能称已通过。

**排除：** 实盘下单、第二数据引擎/任务 broker、重写财务/指标算法、修改原 Shadow DAG 身份、全仓审计和无关重构。真实账号调用、生产写库、基础设施安装、切流和停 Streamlit 需要原有单独授权；本片必须准备可审阅的代码、命令、安装和回退材料，不能把这些必需工作移到 backlog 后宣布完成。

## 6. 页面与操作

页面沿用目录、审计、回补和财务区域。原目录/统计/图表复用既有验收。这里只补实际来源状态和执行动作。

| 用户看到 | 必须表达的事实 | 操作 |
|---|---|---|
| “采集待核验” | 没有可用回执；旧报告仍能读 | 查看缺失原因；核验现有来源 |
| “已核验 3 类” | 三类具体范围通过来源链 | 在原数据集详情看日期和计数 |
| “等待执行” | 已受理、窗口或 writer 未就绪 | 查看进度；同任务续查 |
| “正在回补 2 / 8 天” | 只统计本任务已核验事务 | 暂停/继续使用原命令 |
| “已暂停” | 当前日期边界停止，既有事实保留 | 同任务继续，不生成新计划 |
| “部分完成” | 日期、派生、副本或审计还有未完成项 | 一句话原因；原任务继续 |
| “回补完成” | 完整终态证明已通过 | 查看原新审计报告 |
| “财务来源待接入” | 七接口实际权益/装配未证明 | 显示需要处理的具体条件 |
| “最近可用日期 / 已核验记录” | 原财务派生核对结果 | 刷新；按字段看缺数原因 |

服务徽章只用“正常 / 注意 / 异常 / 未运行”四词，颜色、图标和文字一起表示。继续复用原 `StatusBadge`，不修改全站状态系统。原内部 `waiting` 仅在预期盘中服务的对应时段显示“等待开盘 / 已收盘”。回补排队、窗口等待、执行、暂停及部分完成用业务进度短文案；正常夜间等待不标红。加载用骨架；缺值写“—”；可信零行与未知区分。

回补第一步显示股票日线、计划日期、缺失天数、影响范围和执行条件。第二步明确“将写入缺失日线”，需要确认计划日期。财务运行也确认固定范围。操作按钮在受理时锁定；关闭、重新打开或刷新继续原请求。Esc 关闭后，按钮恢复可用时归还焦点。后台 owner 或代变化立即清除旧待提交状态。

页面正文只留名称、日期、数字、短状态和必要动作。服务代号、提交号、哈希、内部 reason 和阶段号留在详情或 Tip。Tip 支持 hover、键盘 focus、手机 tap；必须有可聚焦触发器。1440px 和 390px 都能完成两步确认、续查、查看进度和历史；次要列可隐藏，但关键日期、状态与操作保留。数字和时间使用原格式组件。

## 7. 写集与实施顺序

完整写图保存在 `spec/write-set.json`，包含每个现有路径的 before SHA、新路径、职责和 Root 共享授权项。本阶段只写本规格和证据。没有任何产品写入授权从本文件自动产生。

1. **来源记录与 v3 报告。** 先写失效来源、重复、换代、丢失响应红测。实现同原事务记录器、原队列扩展和逐类状态。复用原 24 类统计回归，不重复计算。
2. **公共 writer gate。** 先写两进程争锁、旧 writer、复制占用、别名和异常关闭用例。接入全部直接参与者。未获得实际参与者证据时，执行政策保持关闭。
3. **精确计划执行。** 先写 owner、二次确认、唯一执行、稀疏日期、同事务和回执 I/O 恢复用例。增加 token 核验后/COMMIT 前的实际接管竞争、有效提交后新 claim 续记、排队后额度耗尽、权益过期、重试前耗尽和 dispatch 响应丢失恢复用例。复用原 manifest/leases、原 quota observer/ledger 与逐日准备、派生函数。
4. **财务运行。** 先写七接口独立权限、原请求恢复、PIT 修订、导入 cursor 和派生来源用例。再装配原采集、导入和每日计算。
5. **真实发布和页面。** 原 PageControl → job → worker → report → Serving → typed API → React 必须走通。Root 生成 OpenAPI、schema 和 dist；作者不手改生成文件。
6. **有限真实验收与最终审查。** Root 执行原进程入口、独立进程争锁、浏览器和 Linux gate。保留失败记录，复用有效结果。最终候选一次独立审查；只复核修复和直接回归。
7. **安装与替代。** 准备 exact commit/profile、参与者清单、显式关闭的政策、安装命令、只读核验和关闭新执行入口的回退。生产写入和新 unit 获得具体授权后执行；实际来源和资源验证通过后才请求对应 Streamlit 停用。

每项顺序为相关红测 → 最小实现 → 同范围绿测 → 材料冻结。提交、生成文件、独立进程与生产操作由 Root 执行。不要逐小步骤追加独立审查或全量测试。

## 8. 证据和生产条件

本片至少保留以下实际材料：

- 原真实或隔离原采集入口的请求、原事实事务、原回执与副本绑定；不能只直接调用 bridge。
- 原审计队列、worker、成功回执、不可变报告、Serving 数据代及页面读数逐层一致。
- 非连续日期计划经过原 Journal 两步受理，再由原 BackfillStateStore claim 执行；进程重启后同身份恢复。
- 同一原主库的 daily/monitor/manual、research-sync、backup/replica 和维护 worker 的实际 gate 排斥证据。Linux 安装身份与 macOS 合成行为分别记录。
- 七财务接口分别走原客户端与 archive；原导入、原每日派生、原副本和页面来源一致。真实接口权益/额度未知就明确保留未知。
- 1440px/390px、键盘和 Tip 的实际浏览器操作；排队、暂停、部分完成、失败与完成来自实际后端状态。
- 进程、FD、连接和临时目录的回收记录；容量、实际耗时与资源。未运行、环境阻止、skip 不记通过。

生产所需单独动作是：安装/更新公共 gate 接线和对应 unit/profile；启用明确的主库写入政策；按原计划写入生产空洞日线；财务真实凭据/权益装配；最终入口迁移与旧服务停用。准备材料可继续，实际执行须遵守项目授权。应用代码普通发布仍走原受控部署器，不绕过它。

关闭新执行政策即可阻止新 claim。已持有 claim 在边界释放，原已提交事实和不可变历史保留。不得为了回退删除生产事实、清空 archive/anchor、替换主库或覆盖旧副本。

**规格冻结材料：** `data/verification/data-center-completion-20261006/spec/`。这些材料仅证明规格与来源固定，尚不证明功能完成。
