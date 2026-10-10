# rQuant 工作负载解耦端到端实施计划

**状态：** 执行中  
**日期：** 2026-07-23  
**目标：** 按顺序完成数据采集、权威发布、盘中特征、策略推理、信号消费、研究计算和页面查询七条链路的解耦，并补齐迟到修订、慢变参考数据、额度治理、注册表、实验留痕、样本外晋级、冷热存储、灾难恢复和 schema 演进等横向能力。

本计划落实[工作负载解耦与故障隔离架构设计](../architecture/2026-07-22-workload-isolation-design.md)，并与[可信研究平台与策略闭环计划](2026-07-22-rquant-trustworthy-research-platform-implementation.md)共用 Stage 1、PIT、walk-forward 和前瞻模拟盘阶段门。若两份计划出现顺序差异，以“先完成前置阶段门、一次只实施一个业务阶段”为准。

## 0. 全局规则

1. 每个可变数据库只有一个 writer；其他单元只读不可变文件、只读副本或原子发布代际。
2. 页面不执行长任务，不直接连接生产写库，不把结果只放在 `st.session_state`。
3. live 与 replay 共享版本化契约和纯计算核心；任一时刻只能使用 `available_at <= decision_time` 的输入。
4. 每个阶段先写失败测试，再做最小实现；阶段结束运行相关测试、全量测试和故障演练。
5. 工作日 `09:15-15:10` 不部署、不迁移 schema、不修改生产数据、不重启实时服务。
6. systemd、生产数据库、密钥、sudoers、nginx 和 destructive cleanup 仍需对应的单独明确授权。
7. 每次发布必须经 PR、Python 3.11/3.12 CI、精确 tag/SHA、dry-run、受控部署、preflight、备份、副本同步和 `DEPLOY.md` 记录。

## 1. 阶段与出口

| 阶段 | 交付 | 阶段出口 |
|---|---|---|
| P0 | Stage 1 资源止损与三策略收口 | N 字、科创/创业均为 `comparable`；集合竞价独立 B 已审计退役 |
| P1 | 持久 Strategy Lab Job Center | 四类回测关闭页面后继续；可暂停、恢复、取消、续跑和完整导出 |
| P2 | 数据源网关与不可变原始批次 | 同一接口只请求一次；额度、迟到、修订和降级状态可审计 |
| P3 | 统一实时/回放特征服务 | 逐分钟前缀不变，live/replay 同输入逐字段一致 |
| P4 | 独立策略 runner 与信号总线 | 单策略崩溃不影响其他策略；信号幂等且可恢复 |
| P5 | notifier、paper broker 与 serving | 通知故障不阻塞信号；模拟账户可对账；页面完全只读 |
| P6 | 混合研究 worker、资源隔离和容灾 | 研究计算不影响实时 SLA；故障演练全部通过 |
| P7 | 旧路径移除与整体验收 | 无双写、无页面长计算、无生产写库直读；文档与运行态一致 |

## 2. P0：先关闭当前 Stage 1

1. 修复全范围分钟审计的 OOM，生产只读范围内验证峰值内存、耗时、finding 数和 P0。
2. 基于已部署精确 SHA 为 `n_shape`、`growth_board_surge` 创建新 manifest；旧分钟湖内容按 hash 复用，只补缺失 session。
3. 每个策略依次执行 repair、snapshot、audit、formal smoke；首错即停并恢复 timers。
4. 保存 manifest、snapshot、binding、strategy spec 和 result hash；运行备份、副本同步和 preflight。

**2026-07-23 生产实测约束：** `backfill-plan` 在 4700 万行 `minute_bar` 上单次精确覆盖聚合耗时
约 26-29 分钟，峰值 RSS 约 1.0-1.9 GiB；`research-repair-minute` 即使只预演 N 字的
30 个缺失 session，也会在当前 2 核云主机上持续占满约 1 核。资源隔离完成前，交易保护窗口
不仅禁止写入、迁移和重启，也禁止此类重型只读扫描。P0 长任务必须在窗口外串行执行，或在
独立研究 worker 上读取不可变快照。

**2026-07-24 当前生产检查点：** 精确版本为 `v0.26.7` /
`9bb5235a8a2fd1d4d874a2c71858e99acb58f9fe`，Growth execution contract 已升级为
`stage1-v2` 并将派生的停牌 session evidence 固化进 binding。发布后两次 preflight、
备份和只读副本同步均成功，生产代码在当日 Stage 1 收口前保持冻结。

使用独立临时状态库完成了当前版本 Growth planner，范围为 2026-04-01 至 2026-07-09：
22,879 条资格记录、132 个缺口任务、预计补 136,406 行；baseline 覆盖率 99.9829%，
entry/exit 覆盖率 99.9146%。在 47,549,142 行 `minute_bar` 上实耗 1,989.97 秒、峰值约
1.47 GiB。临时 manifest `2c9bd7b023316c11f40cf8768e2de9e9d9f53d81abc3764ae47a24ac1b9ae58e`
没有写入生产 backfill state，不能用于正式验收。生产 manifest、repair、snapshot、audit 和
formal smoke 必须在当日 15:10 后重新生成并串行执行；09:10 前不再启动可能跨入交易保护
窗口的正式任务。N 字随后基于同一精确代码提交重建 manifest，旧版本 manifest 仅保留审计。

**出口测试：** `tests/unit/test_stage1_*.py`、formal reproducibility integration、生产固定回放；两策略均为 `comparable`。

## 3. P1：持久 Strategy Lab Job Center

### P1.1 冻结任务契约

**新增：** `src/rquant/research_run_spec.py`、`tests/unit/test_research_run_spec.py`

- Pydantic `ResearchRunSpec` 固定 job 类型、参数、代码 SHA、dataset snapshot、特征契约、成本/滑点、随机种子、资源等级和 deadline。
- canonical JSON 生成稳定 `spec_hash`；同 hash 幂等复用，缺少不可变 snapshot 的运行只能标记 `exploratory`。

### P1.2 SQLite 任务账本与单写调度器

**新增：** `src/rquant/lab_jobs.py`、`src/rquant/lab_job_protocol.py`、`src/rquant/lab_scheduler.py`  
**修改：** `src/rquant/config.py`、`.env.example`、`src/rquant/cli.py`

- 独立 `lab_jobs.sqlite3`，表包含 job、shard、event、command、lease 和 artifact。
- Streamlit 只原子投递 typed envelope；scheduler 是唯一 writer。
- request ID exactly-once；同 ID 不同内容 fail closed；合法状态为 queued/running/checkpointed/succeeded/failed/cancelled。

### P1.3 worker、分片和 fencing

**新增：** `src/rquant/lab_worker.py`、`src/rquant/strategy_job_adapters.py`

- claim token、lease、heartbeat、stale recovery；旧 worker 在 lease 失效后不能覆盖新结果。
- N 字先按 `hold_days` 分片；其后迁移 N 字对比、集合竞价和科创/创业放量。
- shard 结束后原子 checkpoint；暂停在当前 shard 后生效，恢复不重算已完成 shard，取消先协作停止再超时 SIGTERM。

### P1.4 动态 ETA 与完整 artifact

**新增：** `src/rquant/lab_artifacts.py`

- 每个 shard 记录 phase、work units、耗时和吞吐；前三片用静态估算，之后使用 EWMA 区间。
- 成功任务保存 `spec.json`、manifest、Markdown、完整指标 JSON、完整 Parquet 表和 SHA256；临时目录校验后原子 seal。
- 旧 JSON/Markdown 只读导入索引，不重写历史。

### P1.5 页面与守护进程

**新增：** `src/rquant/dashboard/lab/job_center.py`  
**修改：** `strategy_lab.py`、`strategy_lab_worker.py`、`strategy_lab_runs.py`

- 页面提供状态筛选、阶段、动态 ETA、首错、暂停/恢复/取消/重跑、历史和完整结果包导出。
- 移除四类任务的页面同步执行路径；Mac 先作为任务权威运行 launchd scheduler/worker，远程 worker 留到 P6。

**出口测试：** 关闭浏览器、杀 worker、重启 scheduler、租约过期、暂停恢复、取消竞争、完整导出、四类任务新旧结果逐行/逐 hash 等价；页面进程中不得出现回测调用。

## 4. P2：数据源网关与数据发布

### P2.1 版本化原始批次

**新增：** `src/rquant/raw_batch.py`、`src/rquant/source_gateway.py`、`src/rquant/source_quota.py`

- `RawBatchEnvelope` 包含 dataset/schema/source/request/batch/sequence、event/received/available 时间、行数、hash、质量和 producer 版本。
- `spool` 保存不可变批次，`current` 原子替换；每个消费者保存独立 cursor。
- 采集只保存原始数据，不算策略、不通知。

### P2.2 统一通道与额度治理

- 建立 `auction_match`、`watchlist_quote`、`market_minute`、`daily_close`、`reference_slow` 通道。
- 同一 endpoint 同一参数窗口只请求一次；策略共享批次。
- 实时额度保留，研究额度使用 lease；空响应、限流、网络失败和 stale 明确建模。

### P2.3 校验、修订与原子权威发布

**新增：** `src/rquant/data_publisher.py`、`src/rquant/data_revision.py`

- candidate 通过 schema、完整性、时点和跨源差异检查后才发布 canonical generation。
- 迟到或修订生成新 revision，不覆盖旧证据；已封存 snapshot 不自动漂移。
- 日终流水线拆成 capture/validate/publish/pool/serving/replica/research/backup 独立步骤，按输入 hash 幂等跳过。
- publisher 按每个 `(ts_code, trade_date, freq, source, revision)` 增量维护不可变
  `minute_session_coverage`，记录期望/实际分钟数、首末分钟、主键重复数、质量状态与输入 hash。
  planner、repair 和 snapshot 只扫描这张覆盖索引；发现 hash 或 revision 不一致时才回落原始分钟
  分区复核，避免每次策略计划重扫数千万行分钟事实。

**出口测试：** 数据源断开、重复批次、乱序、迟到、字段变化、额度耗尽和半批失败；旧 current 始终可读，消费者可按 sequence 补齐；覆盖索引与原始分区抽样/全量复核一致，常规 Stage 1 planner 不再扫描 `minute_bar`。

## 5. P3：统一盘中特征服务

### P3.1 特征注册表与上下文

**新增：** `src/rquant/feature_contract.py`、`src/rquant/feature_registry.py`、`src/rquant/feature_core.py`

- `FeatureContext` 固定 decision time、available-at cutoff、复权口径、市场日历、证券状态和输入批次 hash。
- 注册同刻放量、累计进度、5/10 分钟加速度、VWAP、内外盘方向、90 日价量分布、价格历史百分位、市场/板块情绪和涨跌停状态。
- 9:30 开盘段使用独立基准版本，不把全天数据或未来分钟混入。

### P3.2 live 与 replay

**新增：** `src/rquant/feature_live.py`、`src/rquant/feature_replay.py`

- 纯计算核心共享；live 增量状态和 replay 向量实现可以不同。
- 每分钟输出不可变 `FeatureSnapshot`，携带 input hash、contract version、available_at 和 quality。
- 缺失输入输出 degraded/unknown，不用 0 伪装。

**出口测试：** 任意时间前缀追加未来分钟后既有输出不变；同一冻结输入 live/replay 逐分钟、逐字段一致；除权、停牌、ST 和价格限制 PIT 正确。

## 6. P4：独立策略 runner 与信号总线

### P4.1 StrategySpec 与 runner

**新增：** `src/rquant/strategy_spec.py`、`src/rquant/strategy_runner.py`

- N 字、科创/创业放量、集合竞价特征用途分别注册版本化 StrategySpec。
- 每策略独立状态库和 runner；只读取 FeatureSnapshot，只输出信号 spool。
- runner 崩溃、降级或升级不改变其他策略。

### P4.2 SignalEnvelope 与 router

**新增：** `src/rquant/signal_envelope.py`、`src/rquant/signal_router.py`

- 内容确定的 `signal_id`，包含策略/参数/数据/特征快照、event/available time、action、证据、有效期和 commit。
- router 单写 `signal_bus.sqlite3`，负责 schema、全局 sequence、幂等和坏消息隔离。
- 重启从 cursor 续读，同一个信号不生成第二个全局事件。

**出口测试：** 杀任一 runner、重复投递、坏 envelope、router 重启、乱序和过期事件；其他 runner 和采集延迟不变。

## 7. P5：通知、模拟盘与页面 serving

### P5.1 notifier outbox

**新增：** `src/rquant/notification_outbox.py`、`src/rquant/notifier_service.py`

- 独立 `notification_state.sqlite3`；幂等键为 signal/recipient/channel。
- PushDeer/PushPlus 独立重试、冷却、过期和死信；API 超时不阻塞信号。
- 恢复后只补发仍有效且未成功的通知，从结构上消除重复 Push。

### P5.2 paper broker

**新增：** `src/rquant/paper_broker.py`、`src/rquant/paper_account.py`

- 独立账户事件账本，处理 A 股 T+1、涨跌停、停牌、手续费、滑点、部分止盈、移动止盈止损和不可成交。
- 每笔订单/成交引用 signal、feature snapshot 和报价批次；现金、冻结股数、持仓和收益逐笔可对账。

### P5.3 serving publisher

**新增：** `src/rquant/serving_publisher.py`

- 从各单元 sealed 摘要生成小型 `serving.duckdb` generation，验证后原子切换 `current.json`。
- Dashboard、Lab、Panorama、Canvas、NL Screen 只读 serving、只读任务账本和 sealed artifact；首页不扫原始分钟。

**出口测试：** notifier 停机后信号完整积压且恢复不重发；账户回放逐笔守恒；删除/锁住生产主库写路径后全部页面仍能打开并展示最近 generation。

## 8. P6：混合研究、横向治理与物理隔离

### P6.1 Mac/远程研究 worker

- worker 只领取绑定不可变 snapshot 的任务；通过 artifact 上传交换结果，不远程共享可写 DuckDB。
- 支持 resource class、deadline、抢占和 host capability；断网后本地 checkpoint，恢复后续传。

### P6.2 样本外晋级与实验留痕

**新增：** experiment/strategy registry，保存训练、验证、外层测试、费用、回撤、置信区间、消融和失败实验。

- 外层测试不得参与参数选择；多重比较惩罚和最小样本门槛显式记录。
- 晋级状态为 exploratory/backtest_candidate/paper_candidate/paper_active/retired，必须由证据 gate 转移。

### P6.3 慢变参考数据、冷热存储与 schema

- 交易日历、ST/退市、停牌、复权、板块和公司行为独立发布、带 effective/available 时间。
- 热数据保留最近盘中窗口；温数据按日 Parquet；冷 snapshot/artifact 压缩归档，catalog 始终保留 hash 和位置。
- schema 采用 additive-first、reader compatibility、回填、切换、旧字段退役流程；不原地破坏历史产物。

### P6.4 systemd slices、灾难恢复和审计

- 建立 live/serving/research slices；依据实测设置 CPUWeight、IOWeight、MemoryHigh/Max 和 Nice。
- 生产发布、数据发布、策略晋级分别保留独立审计链。
- 演练主库损坏、只读副本损坏、状态库损坏、artifact 丢失、磁盘逼近阈值、Tushare 限流和主机重启。

**出口测试：** 研究压力下 live 周期和告警延迟不超基线阈值；从备份和 manifest 恢复后 hash 一致；任一单元故障不产生跨库双写。

## 9. P7：旧路径移除与端到端验收

1. 关闭 monitor/surge-watch 内的数据源重复请求、策略内通知、页面 `Popen` 和同步回测。
2. 加静态门禁：禁止页面导入写 store，禁止 runner 调通知，禁止消费者调用外部 adapter。
3. 运行完整故障矩阵：浏览器关闭、worker SIGKILL、scheduler 重启、router 重启、notifier 断网、迟到分钟、研究 OOM、serving 发布失败和生产回滚。
4. 对三策略执行冻结 snapshot 的 replay/live/paper 贯通验收，核对信号、成交、通知和页面摘要 hash。
5. 更新 README、运维手册、数据源矩阵、灾难恢复手册、ELI25 Job Center 说明和 `DEPLOY.md`。
6. 全量测试、Python 3.11/3.12 CI、审查、精确 tag、生产 dry-run/部署、备份、副本同步、preflight 和次日盘中只读观察全部通过后，才删除旧路径和宣布目标完成。

## 10. 最终验收清单

- [ ] 七条流水线均有唯一 owner、独立状态和明确输入输出契约。
- [ ] 采集、特征、策略、通知、模拟盘、研究和页面可单独停止/恢复。
- [ ] 盘中和 replay 无未来函数，特征与策略版本可复现。
- [ ] 所有通知和信号具备持久幂等，重复 Push 故障演练通过。
- [ ] 所有回测有 snapshot、成本、样本外区间、完整 artifact 和失败留痕。
- [ ] 页面关闭或重启不会丢任务，长任务可分片、checkpoint、暂停和续跑。
- [ ] 研究资源耗尽不会影响 live SLA；大任务默认在 Mac/独立 worker。
- [ ] serving 页面不连接生产写库，不扫描全量分钟数据。
- [ ] 备份恢复、schema 演进、迟到修订和慢变参考数据均有自动测试与操作手册。
- [ ] 生产运行态、仓库文档、tag/SHA、systemd unit 和审计记录一致。
