# 盘后收盘数据与生产 DAG 隔离实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 把当前单体 `rquant run-daily` 拆成可恢复、可审计、可逐步切换的收盘数据采集与生产阶段链，同时在新链达到验收门前保留旧 daily 权威路径。

**Architecture:** 先新增只写不可变 `daily_close` spool 的影子 source，不触碰生产 DuckDB；再建立 SQLite 单写阶段账本，把验证、canonical publish、筛选/Pool、serving、副本、研究 ingest 和备份变成精确输入输出绑定的幂等阶段。新链通过逐日对账后才切换 canonical writer，旧 `rquant-daily` 最后退休。

**Tech Stack:** Python 3.11+、Pydantic、SQLite 状态账本、DuckDB 单写者、Parquet/JSON 不可变批次、systemd oneshot/timer、pytest、ruff。

---

## 强制边界

1. `daily_close` source 只请求外部接口并发布 raw batch，不打开生产 DuckDB 写连接。
2. 同一 Tushare 端点只能由一个 owner 调用；影子期旧 ingest 与新 source 不得同时消耗同一请求，必须由 source 结果喂给旧 ingest 或错开并明确记账。
3. canonical publisher 是生产 DuckDB 的唯一日终写者，发布事务不包含筛选、通知或研究计算。
4. 每个阶段都绑定上游 content hash、代码 commit、schema、运行日期和 `available_at`；输出一致时幂等跳过。
5. 失败只阻断依赖阶段，保留上游成功产物；不得把缺失或部分数据发布成成功日。
6. 新链影子验收前，旧 daily 继续权威运行；任何切换都必须有单独 preview/apply、回滚和生产授权。

### Task 1: 不可变 `daily_close` raw gateway

**Files:**
- Create: `src/rquant/daily_close_gateway.py`
- Create: `src/rquant/daily_close_source_service.py`
- Test: `tests/unit/test_daily_close_gateway.py`
- Test: `tests/unit/test_daily_close_source_service.py`

**Steps:**
1. 先写失败测试：全市场日线、日指标、复权因子、指数、证券/停牌状态必须归一化为一个 typed batch；缺字段、未来日期、NaN/Inf、重复主键 fail closed。
2. 运行聚焦测试，确认因模块缺失而失败。
3. 实现 `DailyCloseGatewayConfig`、`DailyCloseCapture` 和固定 payload schema；保存 source time、received/available time、质量状态、revision 与内容哈希。
4. 对 source 超时发布显式 stale 空批次；结构错误不得发布。
5. 补同日修订、迟到修订、重启恢复、配额耗尽、hard exit 和 immutable spool 测试。
6. 运行聚焦测试、ruff 和 format check。

### Task 2: 独立 source runtime 与最小权限 unit

**Files:**
- Modify: `src/rquant/runtime_service_entrypoint.py`
- Modify: `src/rquant/runtime_service_builtin.py`
- Modify: `src/rquant/runtime_capabilities.py`
- Modify: `src/rquant/runtime_deployment_bundle.py`
- Modify: `src/rquant/runtime_production_profile.py`
- Create: `deploy/systemd/rquant-runtime-daily-close@.service`
- Modify: `tests/unit/test_runtime_service_builtin.py`
- Modify: `tests/unit/test_runtime_deployment_bundle.py`
- Modify: `tests/unit/test_runtime_production_profile.py`
- Modify: `tests/unit/test_runtime_systemd_services.py`

**Steps:**
1. 写失败测试：profile 必须包含唯一 `DAILY_CLOSE_SOURCE`；只持 Tushare capability，只写 `live/daily-close` 与自己的 control root。
2. 确认测试失败后注册新 kind、builder、bundle 路径与 dedicated systemd unit。
3. source 读取精确 market-calendar generation，只在开放交易日收盘数据可用窗口运行，同日成功内容幂等。
4. unit 必须含固定 `--expected-kind`、最小 `ReadWritePaths`、加密 credential 和 live slice。
5. 运行 systemd 静态测试；发布前在云端原样执行 `systemd-analyze verify`。

### Task 3: 可恢复日终阶段账本

**Files:**
- Create: `src/rquant/daily_pipeline_ledger.py`
- Create: `tests/unit/test_daily_pipeline_ledger.py`

**Steps:**
1. 写失败测试定义固定阶段：`validate`、`canonical_publish`、`pool_build`、`serving_refresh`、`replica_sync`、`research_ingest`、`backup`。
2. 实现 typed `DailyRunSpec`、`DailyStageIntent`、`DailyStageReceipt`、lease/fencing token 和 SQLite 单写 store。
3. 阶段只能消费精确上游 receipt；输入 hash 改变时生成新 run，不可覆盖旧证据。
4. 实现 running lease、heartbeat、失败、重试、幂等成功和首错保留。
5. 增加 kill/restart、两个 orchestrator 竞争、SQLite busy、磁盘错误、stage receipt 篡改测试。

### Task 4: raw validation 与 canonical candidate

**Files:**
- Create: `src/rquant/daily_close_validation.py`
- Create: `src/rquant/daily_close_candidate.py`
- Create: `tests/unit/test_daily_close_validation.py`
- Create: `tests/unit/test_daily_close_candidate.py`

**Steps:**
1. 写失败测试覆盖行数、日期、市场代码、OHLCV、复权、状态覆盖、指数和 schema 质量门。
2. 实现流式验证和不可变 candidate generation；validation receipt 绑定 raw batch hash。
3. 不完整数据产生 degraded/quarantined，不创建 canonical current。
4. 同一 candidate 重跑内容一致；修订必须形成新 generation 并保留差异摘要。

### Task 5: 单一 canonical publisher

**Files:**
- Create: `src/rquant/daily_canonical_publisher.py`
- Create: `tests/unit/test_daily_canonical_publisher.py`
- Modify: `src/rquant/ingest.py`
- Modify: `tests/unit/test_ingest.py`

**Steps:**
1. 先测试 publisher 只从已验证 candidate 读取，禁止运行时调用 adapter。
2. 把现有 `ingest_daily` 的远端采集与事务写入拆开；旧公开函数保持兼容但内部复用 typed materialization。
3. canonical publish 在单个 DuckDB 事务内写 daily、状态、停牌、复权、daily basic、指标与 daily state，并生成精确 receipt。
4. 事务失败必须完整回滚；成功后 receipt 与数据库水位一致。
5. 增加同内容重跑、修订重发、部分表失败、DuckDB 锁冲突和 PIT 可见性测试。

### Task 6: 筛选、Pool 与通知职责拆分

**Files:**
- Modify: `src/rquant/pipeline.py`
- Create: `src/rquant/daily_pool_stage.py`
- Create: `src/rquant/daily_summary_stage.py`
- Modify: `tests/unit/test_pipeline.py`
- Create: `tests/unit/test_daily_pool_stage.py`
- Create: `tests/unit/test_daily_summary_stage.py`

**Steps:**
1. 写失败测试证明筛选失败不会把 Pool/退出检查伪记成功，通知失败也不会改变业务 receipt。
2. 将 preset screening、Pool2 sync、exit check 和 summary envelope 拆成显式 stage 函数。
3. summary 只写 signal/outbox，不直接调用 Push provider。
4. 所有 stage receipt 绑定 canonical generation 与实际写入计数。
5. 保留 `run_daily_pipeline` 兼容包装，影子期旧 timer 行为不变。

### Task 7: orchestration、恢复和服务阶段

**Files:**
- Create: `src/rquant/daily_pipeline_orchestrator.py`
- Create: `tests/unit/test_daily_pipeline_orchestrator.py`
- Modify: `src/rquant/cli.py`
- Modify: `tests/unit/test_cli.py`

**Steps:**
1. 写失败测试：默认命令只 preview；apply 必须绑定 `run_id` 与 plan hash。
2. 实现每次只推进一个可运行阶段的 orchestrator；支持 status、retry-stage 和 recover。
3. serving、副本、research ingest、backup 通过受控 adapter 调用并保存退出状态与输出 identity。
4. 下游失败时不得重跑 canonical publish；上游 identity 变化则拒绝续跑旧 run。
5. CLI 输出机器可读 JSON、阶段 ETA、首个失败点和回滚建议。

### Task 8: 影子对账与切换门

**Files:**
- Create: `src/rquant/daily_shadow_validation.py`
- Create: `tests/unit/test_daily_shadow_validation.py`
- Modify: `src/rquant/runtime_shadow_validation.py`
- Modify: `tests/unit/test_runtime_shadow_validation.py`

**Steps:**
1. 写失败测试比较旧/新链的行数、主键、OHLCVA、状态、指标、筛选、Pool 事件和 available time。
2. 差异按数据延迟、合法修订、口径错误和未知分类；未知差异阻断切换。
3. 连续真实交易日证据必须来自当日不可变批次，禁止盘后补造。
4. 切换门至少要求固定回放全绿、故障矩阵全绿及真实影子窗口达标。

### Task 9: systemd DAG 与受控退休

**Files:**
- Create: `deploy/systemd/rquant-daily-orchestrator.service`
- Modify: `deploy/systemd/rquant-daily.timer`
- Modify: `scripts/deploy-production.sh`
- Modify: `tests/unit/test_runtime_systemd_services.py`
- Modify: `tests/unit/test_deploy_production.py`

**Steps:**
1. 写失败测试证明新 orchestrator 使用独立 state、没有页面/通知密钥、超时后可续跑。
2. 先以 shadow mode 安装并保留旧 `rquant-daily.service` 权威。
3. 云端原样验证 unit 和 calendar；执行 dry-run、备份、固定 fixture 和一次人工 shadow run。
4. 真实影子达标并取得单独生产授权后，切换 timer 到新 orchestrator。
5. 保留一版回滚能力；确认新链稳定后再卸载旧 daily unit。

### Task 10: 总验收

1. 运行所有 daily、runtime、研究 ingest、serving、backup 和故障矩阵测试。
2. 无并发 agent 时重跑 hard-exit 测试，确保没有 pytest 临时目录警告或遗留进程。
3. 云端执行 `systemd-analyze verify`、profile dry-run、rollout dry-run 和只读 preflight。
4. 更新架构映射、README、CHANGELOG 和 DEPLOY；完成 SPEC review、quality review、CI、PR、tag 与受控部署。
5. 只有真实影子窗口通过后，才把旧 daily 标记为可退休；不得用历史回放代替真实天数。
