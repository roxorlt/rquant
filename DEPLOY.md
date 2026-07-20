# Deploy Log

> 每次部署到 82.156.0.68 时追加一条。日期 + tag + 备注 + 回滚命令。
> 最新在最上面。

---

## 2026-07-20 · v0.25.2 + PR #115 · 研究增量 candidate 与备份修复收尾

**状态**：生产应用继续冻结在 tag `v0.25.2`、精确 SHA
`f743fc46ece7c2677fd1bbbd6bdef47418ebf53b`，没有为基础设施修复重启应用服务。备份修复
PR #115 经 Python 3.11/3.12 CI 全绿后 squash merge，精确 SHA 为
`e4d14f06502c745101059f05382e401fd2dedf3b`。18:02 只从该 SHA 提取
`rquant-backup.service`，云端 `systemd-analyze verify` 通过且文件 SHA-256 为
`9a8bb5c92a479bccb076d992d8e2d478b2aff9a6f7c37595c8d35d6cae764003`，安装后
`TimeoutStartUSec=10min`。

**研究日增量首次 candidate**：

- 09:25:06 的不可变 watchlist snapshot 绑定应用 SHA `f743fc4`，共 4 只
  （pool1=2、pool2=2），与 monitor 运行清单一致；当天没有盘后补造或回填开盘前证据。
- 17:00 daily 成功，主动刷新只读副本后 readiness 返回 `ready`、`issues=[]`。首次手工
  ingest 退出码 0、状态 `candidate`：分钟 4/4 标的完整，覆盖率和观测精度均为 100%；
  竞价 5,522/5,524，覆盖率 99.9638%，观测精度 100%；authority、catalog、只读 catalog
  和 lake 全部验收通过。
- `rquant-research-ingest.timer` 随后启用。18:10 首次定时运行于 12 秒内成功退出，
  分钟与竞价分区均为 `unchanged`、`issues=[]`。最终 authority 仍为 `candidate`，
  `stable_trading_days=1`、`observation_count=6`，catalog 与只读 catalog 哈希一致；
  `eligible_for_promotion=false`，继续累计 10 个交易日证据，不提前晋级。

**备份修复与清理**：

- 生产 DuckDB 已增至 5,204,881,408 字节。旧 unit 的 `TimeoutStartSec=120` 会在复制、
  压缩和校验即将完成时终止任务。新 unit 恢复 timer 后因 `Persistent=true` 补触发一次，
  18:02:55 开始、18:06:07 成功，实测 192 秒，`Result=success`、
  `ExecMainStatus=0`。产物来自只读副本、源延迟 0、52 张表，压缩后
  1,529,612,162 字节，`gzip -t` 通过；timer 为 `enabled/active/waiting`，下一次触发
  2026-07-21 09:00。
- 删除前逐个核对 18 个 `.latest.duckdb.<pid>[.gz]` 私有代际文件：路径全部匹配固定格式，
  9 个 PID 均不存在，最新有效备份完整，备份服务 inactive。随后按文件数量和总字节数双门
  删除逻辑大小合计 57,077,510,144 字节；云盘可用空间由约 52 GiB 增至 63 GiB，实际
  回收约 11 GiB，说明这些临时文件的逻辑大小不等于独占物理块。

**最终验收**：

- 从精确合并 SHA `e4d14f0` 的临时 worktree 运行 preflight：
  `ok=5 warn=0 fail=0 skip=0`，28 个 unit 全部 verify，通过后临时 worktree 已删除。
- 主库与只读副本摘要完全一致：`daily_bar=1,650,869`、
  `daily_state=1,650,869`、`adj_factor=2,469,013`、
  `stock_status_daily=1,061,544`、`screen_result=856`、
  `minute_bar=46,992,269`；前五表最新日期均为 2026-07-20，分钟最新为
  2026-07-20 14:59。
- 修复分支本地全量 `2264 passed`，ruff、shell 语法和 `git diff --check` 通过；
  独立审查无 P0/P1。PR #115 的 Python 3.11/3.12 CI 分别通过。

**影响与回滚**：个人平台没有外部用户操作，管理员无需手工介入。若研究增量变为
`degraded`、catalog 哈希失配或 service 非 0，应立即
`sudo systemctl disable --now rquant-research-ingest.timer` 并保留 observation、catalog
和 lake 作审计证据，禁止补造 09:30 前 snapshot。若备份超过 10 分钟、完整性失败或新 unit
异常，应先停止 `rquant-backup.timer`，再用
`/tmp/rquant-backup-rollback-20260720T180254.service` 恢复旧 unit 并
`daemon-reload`；旧 unit 的 120 秒上限已知不适合当前库体积，因此回滚后 timer 必须保持
inactive，改用受控的 replica 直接备份，直到新的向前修复上线。

---

## 2026-07-17 · v0.21.1 · 云端研究日增量首次上线

**状态**：应用代码 PR #97 与 preflight 热修 PR #98 经 Python 3.11/3.12 CI 全绿后
squash merge；annotated tag `v0.21.1` 精确指向
`530bb8c489fb481da9a934220813c5ec02a65909`。15:19-15:20 由受控发布器从 `v0.20.2`
快进部署，状态 `deployed`。基础设施 PR #99 随后合并为精确 SHA
`39341f8145caa33c7355ca01983de9f65ab9f883`；生产 `main` 仅按白名单快进这 5 个文件，
安装 `rquant-research-ingest.service/.timer` 后保持 timer `disabled/inactive`。

**上线内容**：日终 runner 只在当日 daily 成功、主动刷新的只读副本包含完整日线后运行；
默认日期使用上海时区，普通增量和历史恢复都强制 observation 连续。分钟、竞价、catalog
和只读 catalog 先写隔离事务，完整审计后再原子发布。runner 固定交易日，最多尝试 4 次；
`degraded` 和开关关闭不重试。`auction_bar` 仍由研究 authority 验收，不再误列为没有每日
writer 的生产 DuckDB freshness 必选表。

**验证与生产验收**：

- 本地组合分支 `1981 passed`，核心质量检查和 `git diff --check` 通过；PR #97、#98、#99
  的 Python 3.11/3.12 CI 均通过。
- 云端原样 `systemd-analyze calendar` 将触发式规范为工作日 `18:10:00`，连续 5 次迭代
  正确；两个 unit `systemd-analyze verify` 退出码为 0。安装后 28 个 unit 全部通过
  preflight，timer 明确为 `disabled/inactive`。
- 生产代码 tag、包版本和应用 SHA 分别为 `v0.21.1`、`0.21.1`、`530bb8c`；安装基础设施后
  生产 `main` 为 `39341f8`。部署只重启当时 active 的 5 个 Web 服务，monitor 与
  surge-watch 保持正常收盘退出；备份与只读副本同步均为 `status=0/SUCCESS`。
- 17:00 daily 于 17:02 成功，写入 `daily_bar=5,522`、`stock_status_daily=5,522`，
  副本主动刷新后 readiness 返回 `ready`、`issues=[]`。

**首次 observation**：17:11 手工运行写入 2026-07-17 分钟 4,329 行（9/9 标的完整，
覆盖率 100%）和竞价 5,523 行（预期 5,522，覆盖率 99.9819%），catalog 与只读 catalog
哈希一致。结果按设计为 `degraded`、退出码 2，唯一问题是
`watchlist_snapshot_missing`：当天 09:25 monitor 启动时研究开关尚未启用，不能在盘后
伪造 09:30 前不可变清单。开关现保持 `true`，让下个交易日 monitor 留下真实清单；timer
继续禁用，待下个交易日收盘后手工得到 `candidate` 且退出码 0 才能启用。

**回滚**：发现异常时先执行
`sudo systemctl disable --now rquant-research-ingest.timer`，并将
`RESEARCH_CLOUD_INGEST_ENABLED=false`。已发布的 degraded observation、Parquet 和 catalog
必须保留作审计证据，不得手工删除或改写。代码或 unit 撤回必须创建 revert PR、合并后向前
发布新 SemVer / 精确基础设施 SHA；禁止生产机非快进回退。

---

## 2026-07-17 · v0.20.2 · 研究提交纯净度门修复

**状态**：PR #95 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag `v0.20.2`
精确指向 `0c1755e15b2f1a78f09ef18171010d0cf32e4f1f`。09:05 在交易保护窗口前由受控发布器从
`v0.20.1` 快进部署，状态 `deployed`。研究采集开关继续关闭，没有新增 systemd timer，
没有写生产 DuckDB 或研究 lake。

**修复内容**：根目录 `backup/` 明确作为云端定时恢复快照目录加入 `.gitignore`，避免受控
运行时工件让研究提交探测误报 `-dirty`；其他未提交或未跟踪文件仍会使可信度门 fail closed。
同时补齐 `v0.20.0` 和 `v0.20.1` 的生产部署审计记录。

**验证与生产验收**：

- 新回归测试先复现 `backup/snapshot.duckdb.gz` 导致 `-dirty`，修复后通过；本地聚焦测试
  `11 passed`、全量测试 `1958 passed`，核心质量检查与 `git diff --check` 通过。
- 部署后 tag、HEAD 和包版本均为 `v0.20.2` / `0c1755e` / `0.20.2`；包含未跟踪文件的
  `git status` 为空，`detect_code_commit()` 返回精确 40 位 SHA
  `0c1755e15b2f1a78f09ef18171010d0cf32e4f1f`。
- 原失败日期的 `research-ingest --date 2026-07-16 --dry-run` 返回 `status=planned`，结果内
  `code_commit` 为同一精确 SHA、`issues=[]`，且没有发布分区或调用网络补拉。
- 09:06 preflight 为 `ok=5 warn=0 fail=0 skip=0`，主副本工件延迟 0 分钟；五个前台服务
  `active/running`、`Result=success`、`NRestarts=0`。09:00 盘前检查已完成，monitor 与
  surge-watch 保持 inactive 等待 09:25 timer，原有 10 个 timers 全部正常。

**保留门**：本版本只消除正式研究增量的提交纯净度阻断。生产开关仍为 false/missing；安装
研究日增量 systemd 调度、打开开关并开始 10 个交易日 observation，仍属于独立基础设施与
生产研究数据写入变更，必须另行明确授权。

**回滚**：本版本没有 schema 或数据写入。部署失败由受控发布器自动回滚；成功后的撤回必须
对 `0c1755e` 创建 revert PR、合并并打新 SemVer tag 后向前发布，禁止生产机非快进回退旧 tag。

---

## 2026-07-17 · v0.20.1 · 研究日增量日期类型热修

**状态**：PR #94 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag `v0.20.1`
精确指向 `dc486566e356178596c7d917f0bf8fb42c38b055`。08:50 由受控发布器从
`v0.20.0` 快进部署，状态 `deployed`。研究采集开关继续保持关闭，没有安装或启用新的
systemd timer，也没有写生产 DuckDB 或研究 lake。

**修复内容**：研究 Parquet 返回的 `datetime.date` 与运营 DuckDB 返回的 pandas
`Timestamp` 在合并前统一规范化，再进入主键分组和排序；保留同主键最新业务值与最早
`created_at` 的既有语义。新增函数级测试和真实 Parquet + DuckDB dry-run 全链路回归测试。

**验证与生产验收**：

- 本地研究采集测试 `25 passed`、全量测试 `1957 passed`，核心质量检查与
  `git diff --check` 通过；GitHub Actions Python 3.11/3.12 分别通过。
- 部署后 tag、HEAD 和包版本均为 `v0.20.1` / `dc48656` / `0.20.1`；preflight 为
  `ok=5 warn=0 fail=0 skip=0`，无 DuckDB 锁，五个前台服务均为 `active/running`、
  `Result=success`、`NRestarts=0`，原有 10 个 timers 均有下一次触发。
- 对原失败日期重新执行 `research-ingest --date 2026-07-16 --dry-run` 成功返回
  `status=planned`：分钟计划 3,561 行、竞价计划 5,523 行，没有网络补拉或文件发布，日期类型
  异常未再出现。`research-authority-status` 仍为 `bootstrap_candidate`，bootstrap catalog
  哈希一致，尚无日增量 observation，符合开关未启用时的预期。

**启用前门槛**：部署目录存在长期未跟踪的 `backup/`，导致提交探测器在包含未跟踪文件时
返回 `dc48656-dirty`；tracked worktree 实际干净，tag 与 HEAD 准确。dry-run 允许该标记，
正式采集会 fail closed。该目录误判由后续 `v0.20.2` 修复；研究日增量仍须完成独立基础设施
授权与发布后才能启用。

**回滚**：本版本没有 schema 或数据写入。部署失败由受控发布器自动回滚；成功后的撤回必须
对 `dc48656` 创建 revert PR、合并并打新 SemVer tag 后向前发布，禁止生产机非快进退回旧 tag。

---

## 2026-07-17 · v0.20.0 · 云端研究日增量候选代码上线

**状态**：PR #93 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag `v0.20.0`
精确指向 `03d04c8acf8d9c3fc377432b4977d768506d87e9`。08:33-08:34 由受控发布器从
`v0.19.0` 快进部署，状态 `deployed`。本次只上线候选链路代码，生产开关
`RESEARCH_CLOUD_INGEST_ENABLED` 保持关闭，systemd 配置未发布。

**部署内容**：新增盘前不可变清单、日终分钟补齐、全市场竞价增量、隔离 lake/catalog 发布
事务、完整覆盖率审计、连续 observation 哈希链、只读权威状态检查和 10 个交易日晋级门。
正式增量与存量迁移共用发布锁，任一分区或证据失败都会回滚；生产 DuckDB 不进入写路径。

**验证与生产验收**：本地全量 `1956 passed`，PR 双版本 CI 通过；部署后 preflight 5/5、
五个前台服务和 10 个既有 timers 正常。`research-authority-status` 返回
`bootstrap_candidate`、bootstrap catalog 哈希一致、`stable_trading_days=0`，没有提前提升
研究权威。首次对 `2026-07-16` 执行只读 dry-run 暴露 Parquet 日期与 DuckDB 时间戳混合比较
异常；由于是 dry-run 且开关关闭，没有产生数据写入，随后由 `v0.20.1` 修复并复验。

**回滚**：本版本未启用调度且未改数据。部署失败由受控发布器自动回滚；成功后的撤回必须
对 `03d04c8` 创建 revert PR、合并并打新 SemVer tag 后向前发布。

---

## 2026-07-17 · v0.19.0 · 首次研究数据迁云候选发布

**状态**：用户明确授权首次迁云后，05:58-06:23 完成 Mac 冻结快照、迁移包、云端 staging
上传、candidate 发布和独立验收。snapshot ID 为
`research-20260716T215935Z-4e713ead`，全链路绑定代码
`4e713eada6596228f81f455a12fde3cca1111b30`。本次发布的是**研究权威候选**，没有切换现有
Dashboard/Lab/回测消费者，也没有删除 Mac 数据。

**迁移证据**：

- WAL-free 恢复快照 5,157,957,632 字节、51 张表，SHA-256
  `c5863c8e73606b84632eae336282df74315a1d816f5d084ac2f3c05f5a5cc6a2`；绑定 37 个
  Strategy Lab 文件，artifact inventory hash 为
  `990f0af8a675fe7627be88fc6aed9620e826f5ea41bc9cb99c12b3ad20332393`。
- 迁移包 1,392 个内容文件、317,323,241 字节、670 个分区、21,065,728 行；固定样本
  `200/200` 匹配，所有物理主键重复数为 0。bundle manifest SHA-256 为
  `db276d943b8810439c63dcd7e611eb21c5823db40955c8d2c0e57d25a4ac12d0`。
- 分钟线为 316 个分区、19,114,853 行，覆盖 `2025-03-28..2026-07-16`；集合竞价为
  354 个分区、1,950,875 行，覆盖 `2025-01-16..2026-07-16`。云端逐项核对 manifest 与
  version Parquet 数量均为 `316/316` 和 `354/354`。
- 7 张辅助研究表全部发布：`monitor_event=2434`、`data_quality_issue=9`，其余五张当前为
  0 行；物理 data/manifest 均为 7 个，Lab artifact 为 37 个。

**发布过程与安全门**：

- 原计划 15:12 发布；用户在 06:19 明确要求盘前立即执行后，先取消下午 timer，再使用
  “08:15 后拒绝启动 + CLI 30 分钟硬超时 + systemd 31 分 40 秒外层上限”执行，确保最迟
  08:45 结束，与 09:15 交易保护窗口保留 30 分钟。实际发布于 06:21:13 开始，06:23:28
  完成。
- 发布前 monitor inactive，远端空间门通过；发布只写独立 `data/lake/`、
  `data/research.duckdb`、`data/research_artifacts/` 和 candidate 标记。生产
  `data/rquant.duckdb` 发布前后 SHA-256 均为
  `53a76b354c838d6345aeadb345ad90573601b29219b4dd61b6b3bf712c73d73b`。
- candidate 的 catalog SHA-256 为
  `7700f28cc25aa6486d14391cb262cfa7bb9c3721963d61cb2e11cd55adce8b43`，与实际
  `research.duckdb` 一致。发布后再次执行 verify 通过，幂等重跑返回 `unchanged`。

**生产验收**：最终 preflight 为 `ok=5 warn=0 fail=0 skip=0`，无 DuckDB 读写锁；
dashboard active，monitor/surge-watch 按盘前日程保持 inactive，原有 10 个 rQuant timers
均有下一次触发。立即发布 transient service 为 `Result=success`、`ExecMainStatus=0`，下午
timer 已取消。

**保留与后续门**：candidate 要求 Mac 主库、恢复快照、迁移包和 staging 全部继续保留。
完成云端每日分钟/竞价/模拟盘增量、消费者候选验证入口、异机备份和至少 10 个交易日观察前，
不得删除本地研究库或把 candidate 提升为唯一权威。

**回滚**：生产 DuckDB 未变化，现有消费者尚未切换，因此不需要业务数据回滚。若候选后续
验收失败，应停止晋级并保留快照、staging、candidate 和日志作为证据，禁止手工覆盖生产库或
删除本地原始研究数据。

---

## 2026-07-17 · v0.19.0 · 研究数据迁云工具上线

**状态**：PR #90 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag `v0.19.0`
精确指向 `4e713eada6596228f81f455a12fde3cca1111b30`。01:03 在交易保护窗口外由受控发布器从
`v0.17.3` 快进部署，状态 `deployed`。本次只上线代码与操作手册，**没有上传、发布或切换
研究数据权威，也没有写生产 DuckDB**。

**部署内容**：

- 一并上线 v0.18.0 的研究湖导出契约：分钟/竞价按交易日导出校验过的不可变 Parquet，独立
  `research.duckdb` 保存 manifest、覆盖度和替换审计。
- 新增 `research-migration snapshot/prepare/verify/publish`：从同一个 WAL-free 恢复快照
  打包分钟、竞价、7 张辅助研究表和 Strategy Lab artifact，保存 PIT 截止、主键、聚合、
  固定样本与文件/内容哈希证据。
- 新增 `scripts/migrate-research-to-cloud.sh` 与中文操作手册，支持本地准备、checksum rsync
  续传、云端重验和 candidate-last 发布。中断恢复会校验 publish state、分区 export lock、
  inode 和 symlink 边界；生产 `rquant.duckdb` 不进入迁移写路径。

**发布与验收**：

- dry-run 识别前序 SHA `06c4eb0bc3a35a4749212b5b1c1e8960bde8d288`、目标 SHA
  `4e713ea`，changed files 不含 `deploy/systemd/`、nginx、sudoers 或生产数据修复路径。
- 发布器只重启部署前 active 的 canvas、dashboard、NL screen、panorama-auth、panorama；
  monitor 和 surge-watch 保持按日程 `inactive/dead`。五个 UI 服务均为 `active/running`、
  `Result=success`、`NRestarts=0`，10 个 rQuant timers 均有下一次触发。
- 最终 preflight 为 `ok=5 warn=0 fail=0 skip=0`：主副本工件延迟 0 分钟，日线最新
  `2026-07-16`，分钟最新 `2026-07-16 15:00`（87,709 行），竞价最新 `2026-07-16`
  （11,047 行），无 DuckDB 读写锁；Dashboard 健康端点返回 `ok`，新 CLI help 正常。
- 本地最终为 1,922 项全量测试和 134 项聚焦测试通过；独立审查未发现剩余 Critical、High、
  Medium 问题。GitHub Actions Python 3.11/3.12 分别通过。

**后续数据门**：真实迁移属于单独的生产数据写入变更，仍需明确授权后按操作手册执行。完成
云端逐表/逐分区验收、每日增量验证和至少 10 个交易日观察前，本地研究主库不得删除。

**回滚**：本版本没有 schema 或生产业务数据变更。部署中失败由受控发布器自动回滚；成功后
如需撤回，必须对 `4e713ea` 创建 revert PR、合并并打新 SemVer tag 后向前发布，禁止在生产
机向旧 tag 非快进回退或直接覆盖 DuckDB。

---

## 2026-07-16 · v0.17.3 · 通知事故治理与研究云化计划

**状态**：基础设施 PR #85 先行 squash merge 为
`03cb96468ef8983f4ea88f17b47704069ea36158`；通知代码与研究/Lab 计划 PR #86 squash merge
并以 annotated tag `v0.17.3` 精确指向
`06c4eb0bc3a35a4749212b5b1c1e8960bde8d288`。21:29-21:47 在交易保护窗口外完成两段发布。

**部署内容**：

- systemd monitor 自动重启限制为 30 分钟最多 3 次；Mac monitor LaunchAgent 显式设置
  `NOTIFY_ENABLED=false`，本地只保留研究分钟采集。
- 错误与运维告警使用 60 秒 pending 租约，至少一个通道成功后才进入 30 分钟冷却；文件锁
  状态为跨进程权威，SQLite 保存可查询镜像并作为降级。两种状态存储均不可用时 fail closed，
  不退回无状态重复发送。
- monitor/surge-watch 进程异常只交给 systemd `OnFailure`；watchdog 复用事故键，并在服务
  连续稳定 5 分钟后才关闭事故，避免短暂拉起重新打开 Push 风暴窗口。
- 新增研究数据云化、服务迁移和 Strategy Lab 六项改造实施计划；本次未迁移或删除研究主库。

**发布与验收**：

- 基础设施先在腾讯云 `/tmp` 通过 `systemd-analyze verify`，安装前后均复验；实际参数为
  `StartLimitIntervalUSec=30min`、`StartLimitBurst=3`、`RestartUSec=30s`。
- v0.17.3 dry-run 明确以 `03cb964` 为前序，changed files 不含 `deploy/systemd/` 等受保护
  路径。实际发布由 transient `rquant-v0173-code-rollout2.service` 托管，结果
  `success`、`ExecMainStatus=0`；只重启发布前 active 的五个 UI 服务，monitor/surge 保持
  inactive。
- 无 Push 的事故门 smoke 通过：claim、complete、冷却抑制、clear 和重新 claim 全部符合
  预期。发布前本地 160 项聚焦测试通过，GitHub Actions Python 3.11/3.12 全量 CI 通过。
- 副本同步与备份均 `Result=success`、`ExecMainStatus=0`；只读副本文件年龄归零。最终
  preflight 为 `ok=5 warn=0 fail=0 skip=0`，分钟最新 `2026-07-16 15:00`、竞价最新
  `2026-07-16`。
- 云端 tag、HEAD 和包版本均为 v0.17.3；dashboard/panorama active，monitor/surge
  inactive，10 个 rQuant timers 均有下一次触发。Mac 主运行时也已快进到相同 SHA，editable
  包重新绑定主 checkout，版本为 0.17.3。

**运行环境说明**：发布期间 sshd 多次返回 `Exceeded MaxStartups`，属于未认证连接队列限流，
不是 rQuant 故障。改用 systemd transient oneshot 后发布不再依赖 SSH 会话；未修改 sshd
配置。

**回滚**：本版本无 schema 和业务数据变更。部署过程中失败由受控发布器自动回滚；成功发布
后的回退必须对 `06c4eb0` 创建 revert PR、合并为新的 main 提交、打新 SemVer tag 后向前发布。
受控发布器会拒绝向旧 tag 非快进倒退，禁止在生产机使用 `git reset` 或直接覆盖 DuckDB。

---

## 2026-07-16 · v0.17.2 · Stage 1 生产数据修复验收

**状态**：PR #82、#83 已依次 squash merge；`v0.17.2` 精确指向
`aa3d4e378d2867303681a7a553bba752f6744a07`。16:12-16:17 在交易保护窗口外通过受控发布器
部署，Stage 1 生产数据 P0 已清零，调度已恢复。

**部署与修复内容**：

- 上线 v0.17.1 的历史状态对账、PIT 竞价修复和备份/副本原子发布加固，再以 v0.17.2
  识别 Tushare `namechange.change_reason=退市整理期`。
- 13 个退市整理期股票日继续 fail closed，不伪装为普通非 ST，也不进入策略；审计从真正的
  unknown/conflict 中分离，作为 `stock-status-intentional-exclusion` P2 证据保留。
- Stage 1 审计规则升级为 `stage1-v3`；真正缺失、未知、冲突和非法状态仍保持 P0。

**生产验收**：

- `data-audit`：区间 `2026-04-01..2026-07-15`，审计 ID
  `62485722f2daa4591189f88ac3d65db327ae9cef4d437f638ea9ce19cee55782`，
  `finding_count=4`、`p0_count=0`、状态 `completed`。
- `stock_status_daily`：资格分母与持久化均为 385,183，missing/unknown/conflict/invalid 均为 0，
  主动安全排除 13；schema migration 保持 v9。
- 主库与 `rquant_ro.duckdb` 摘要完全一致：日线最新 `2026-07-15`、1,628,806 行；
  状态覆盖、审计凭证、13 条排除和 migration 版本均一致。
- preflight 为 `ok=5 warn=0 fail=0 skip=0`；分钟最新 `2026-07-16 15:00`，竞价最新
  `2026-07-16`，停复牌覆盖最新 `2026-07-15`。dashboard、NL screen 等长驻服务完成重启，
  daily/monitor/surge-watch 按日程保持 inactive，全部 rQuant timers 已恢复下一次触发。
- 修复前恢复点：
  `/home/lighthouse/rquant/backup/v0.17.2-pre-apply-20260716T081302Z.duckdb.gz`；
  修复后恢复点：
  `/home/lighthouse/rquant/backup/v0.17.2-post-repair-20260716T081750Z.duckdb.gz`。

**剩余研究门**：生产数据 P0 清零不等于策略可晋级。全市场历史分钟缺口、策略 manifest 的
B/S 与基准覆盖率、不可变计算快照仍需完成；N 字、集合竞价和科创/创业放量继续保持
`exploratory`。

**回滚**：代码回滚使用
`bash scripts/deploy-production.sh --target v0.17.1`。v0.17.2 未新增 schema，13 条安全排除可
保留；只有确认必须回滚数据时，才在停止全部写服务后使用上述 pre-apply 恢复点，禁止运行期间
直接覆盖 DuckDB。

---

## 2026-07-15 · v0.15.0 · 阶段 1 PR-B PIT 质量守卫

**状态**：PR #78 已 squash merge 为
`22618768aaf6bf507eaeb7ed4c8c42813b19fe4b`。权威交易日历于 7 月 14 日完成初始化，
代码于 7 月 15 日 08:37 部署；云端与本地核验完成后恢复盘中调度。

**部署内容**：

- 历史证券名称/ST 状态、上市/重新上市边界改为 nullable、PIT 且 fail closed。
- 日线/分钟语义审计、分钟时间戳语义归一、复权价格可见性守卫与质量问题落库。
- 涨停池修复采用权威交易日历、稳定 plan id、CAS 重算和调用方事务所有权保护。
- 竞价与科创/创业策略回放必须使用匹配的历史状态，禁止回退当前证券快照。

**生产初始化与验证**：

- 首次部署在 preflight smoke 阶段因 `trade_calendar` 缺少 `2026-07-13` anchor 自动回滚到
  `v0.14.0`；根因是目标版本的 fail-closed 筛选先于部署后的日历 bootstrap 执行。
- 使用同一 `v0.15.0` 临时 worktree 执行幂等 bootstrap，Tushare 返回并验证
  `2020-01-01..2026-12-31` 共 2,557 个自然日、1,697 个交易日；随后正式发布成功。
- 发布后云端 preflight 为 `ok=5 warn=0 fail=0 skip=0`；主库和只读副本日历范围、行数、
  交易日计数完全一致。
- 09:05 重新生成 252M HTTP 生产快照，本地 `cloud_backup.duckdb`、主库和只读副本均完成
  合并并核验为 2,557/1,697；本地 preflight 为 `ok=3 warn=0 fail=0 skip=2`。
- 同时恢复多个 timer 时，monitor 首次启动与一次性备份任务争夺 DuckDB 写锁；backup 与
  replica 成功结束后 monitor 按 `RestartSec=30s` 自动恢复，`NRestarts=1`、10 只 watchlist
  进入盘前阶段。后续恢复顺序应先跑完一次性写任务，再启动 monitor/surge-watch。

**回滚基线**：`bb6141982d65f5ed78ed59c24c6c694d11cbd0c1`（`v0.14.0`）。
代码和依赖仍使用受控发布器自动回滚；已写入的交易日历和新增 schema 向后兼容，代码回滚时
可保留，不得在服务运行期间整文件覆盖 DuckDB。

---

## 2026-07-14 · v0.14.0 · 阶段 1 PR-A 数据可信底座

**状态**：PR #76 已 squash merge 为
`bb6141982d65f5ed78ed59c24c6c694d11cbd0c1`，01:01-01:05 部署并完成生产迁移。

**部署内容**：

- 新增 checksum 固定、事务执行的 DuckDB migration v1-v3；项目版本更新为 `0.14.0`。
- 新增数据集快照、覆盖率、质量问题、PIT 数据契约与权威交易日历基础能力。
- 研究同步改为跨表原子事务；只读副本发布、回滚和提交结果按真实状态报告。
- 此版本不修改策略触发、买卖规则或历史业务数据，不执行历史清理。

**生产迁移**：

- 迁移前在无写锁窗口生成 `backup/latest.duckdb.gz`：主库 264,515,584 bytes，
  压缩快照 110,463,165 bytes，完成时间 `2026-07-14 01:03:31 +08:00`。
- 创建 `schema_migration`、`dataset_snapshot`、`dataset_coverage`、
  `data_quality_issue`、`trade_calendar`；账本精确记录 v1-v3 及固定 checksum。
- 迁移后原表行数保持：`daily_bar=1,617,757`、`screen_result=848`、
  `monitor_event=2,337`；`trade_calendar` 初始为 0 行，留给后续权威日历回补。
- 原子刷新 `rquant_ro.duckdb`，主库与只读副本均为 264,515,584 bytes。

**验证**：

- 本地最终 HEAD：`1276 passed in 31.40s`；GitHub Actions Python 3.11/3.12 均通过。
- 发布器状态为 `deployed`，仅重启部署前 active 的 canvas、dashboard、nl-screen、
  panorama-auth 和 panorama；monitor、surge-watch 保持 inactive。
- 部署后 preflight：`ok=5 warn=0 fail=0 skip=0`，smoke screen 命中 8。
- 8501/8502/8504/8506 四个 Streamlit 健康端点均返回 `ok`。

**回滚基线**：`e3b48c0b358c4fd98748f4a57bb142c900294b4c`。代码/依赖使用受控发布器
自动回滚；新增空表为向后兼容 schema，代码回滚时可保留。如需文件级恢复，使用本次迁移前
`backup/latest.duckdb.gz`，不得在服务运行时直接覆盖主库。

---

## 2026-07-13 · v0.13.2 · 受控自动发布

**状态**：已于 15:42-15:45 部署到腾讯云，commit
`e3b48c0b358c4fd98748f4a57bb142c900294b4c`。

**部署内容**：

- 精确 tag/SHA、main 归属与快进校验；tracked 脏文件和并发部署拒绝。
- diff 自动计算服务重启；工作日 09:15-15:10 有重启需求时自动延期。
- 仅允许 7 个 rQuant 长驻服务走 `sudo -n systemctl restart`；基础设施变更自动拒绝。
- 依赖/preflight/服务健康失败自动回滚；审计写入 `logs/production-deploy.jsonl`。
- 包含尚未上云的 `v0.13.1` preflight 只读副本热修复。

**首次引导**：

1. SSH 22 端口恢复后，使用已授权密钥登录 `lighthouse@82.156.0.68`。
2. 从 `77f6ebf` 精确快进到 `v0.13.2`，执行 `uv sync --frozen`。
3. `visudo` 校验通过后，安装
   `/etc/sudoers.d/rquant-production-deploy`（root:root，0440）并复验授权。
4. 以后的日常发布全部调用
   `scripts/deploy-production.sh --target <exact-ref>`。

**验证**：

- 环境包版本为 `0.13.2`，tracked 工作区干净，仅保留 untracked `backup/`。
- 服务重启前后两次 preflight 均为 `ok=5 warn=0 fail=0 skip=0`。
- 只重启发布前 active 的 canvas、dashboard、nl-screen、panorama-auth 和 panorama；
  已按日程退出的 monitor 和 surge-watch 保持 inactive。
- Dashboard `127.0.0.1:8501/_stcore/health` 返回 `ok`。
- 受控入口复核返回 `already_current`，JSONL 审计时间为
  `2026-07-13T15:45:10+08:00`。

**回滚基线**：`77f6ebfb7782521e5c58ffc2e9226e20af9ac96c`；使用受控发布器的自动
回滚链路，不手工改生产数据。

---

## 2026-07-13 · v0.13.1 · preflight 只读副本热修复

**状态**：PR #73 已合并为 `347d57d`，annotated tag `v0.13.1` 已推送；修复已随
`v0.13.2` 于 2026-07-13 一并上云。

**候选内容**：

- `preflight` 的数据新鲜度与 smoke 筛选优先读取只读副本，避免盘中撞 monitor 主库写锁。
- `lsof` 只有 `mem` 等未分类 FD 时改报“无法判断”，不再误报 monitor 未运行。
- 修正 Stage 0 新增 CI 的上下文作用域，使 Python 3.11/3.12 矩阵能实际创建 job。
- 不改数据库 schema、systemd unit、策略逻辑或生产数据。

**部署后验证**：

1. `rquant.__version__` 输出 `0.13.1`。
2. monitor 运行期间执行 `.venv/bin/rquant preflight`，不再出现 DuckDB conflicting lock。
3. `duckdb_lock_detail` 可以预警未分类 FD，但不应因此令 preflight 失败。

**回滚命令**：`git checkout 77f6ebf && /home/lighthouse/.local/bin/uv sync --frozen`

---

## 2026-07-13 · v0.13.0 · 研究可信度阶段 0

**状态**：已部署到腾讯云，commit `77f6ebf`。

**部署内容**：

- Strategy Lab 研究记录增加四级可信度 manifest；旧记录自动降级为探索性。
- 页面增加 N 字、科创/创业、集合竞价三项当前可信度警示。
- 新增研究基线、中文总路线图和 GitHub Actions CI。
- 版本元数据和 README 对齐到当前活跃项目状态。

**验证**：

- `uv sync --frozen` 完成，包版本由 `0.1.0` 更新到 `0.13.0`。
- `git rev-parse --short HEAD` 为 `77f6ebf`，`rquant.__version__` 为 `0.13.0`。
- 26 个 systemd unit 验证通过，9 个生产 unit 状态正常，monitor 盘中 active/running。
- preflight 的数据新鲜度与 smoke 检查因直连主库撞 monitor 写锁而失败；部署本身正常，
  该问题由上方 `v0.13.1` 热修复处理。

**回滚命令**：`git checkout 20eadf9 && /home/lighthouse/.local/bin/uv sync --frozen`

---

## 2026-05-06 · v0.12.1 · hotfix：nl-screen 只读 DuckDB

**背景**：节后首日 09:30 开盘 monitor 启动失败，38 次 crash-loop + 持续 OnFailure
PushDeer 告警。根因：`rquant-nl-screen.service`（自 4/30 部署起常驻）持有
`/home/lighthouse/rquant/data/rquant.duckdb` 的写锁（PID 2597296），monitor 拿
不到锁就退出。

**部署内容**：

- `DuckDBStore.__init__` 新增 `read_only: bool=False` 参数；read_only=True 时跳过
  `_init_schema()` 的 DDL
- `dashboard/nl_screen.py` 改用 `DuckDBStore(settings.duckdb_path, read_only=True)`
  打开 DB（NL 选股是纯查询场景，与 `dashboard/app.py` 同模式）
- 不动 systemd unit、nginx、依赖

**入口**：无变化（NL screen 仍在 8502 / `/nl/`，monitor 仍 systemd 调度）

**验证**：
- 本地 smoke：read-only 开 DB 不锁、能读、写被拒 ✓
- 云端 hotfix 流程：先停 nl-screen 释放锁 → reset-failed monitor → 起 monitor（active running）→ pull fix 分支 → restart nl-screen ✓
- monitor PID 716834 自 09:49:58 起稳定，nl-screen restart 后 monitor PID 不变 ✓
- ⏳ 待验：浏览器跑 NL query 时 monitor 不被踢（read-only + 写锁理论上不冲突，等真请求覆盖）

**回滚命令**：

```bash
# 回到 v0.12.0
cd /home/lighthouse/rquant
git checkout v0.12.0
sudo systemctl restart rquant-nl-screen.service
# ⚠️ 回滚后会重现 nl-screen 占写锁问题，monitor 与 nl-screen 不能同时跑。
#   要么停 monitor 让 nl-screen 用，要么停 nl-screen 让 monitor 用。
```

**预防**：未来任何想跟 monitor 共存的 DB 消费者（dashboard / nl-screen / 新增 Streamlit / 临时 CLI 查询）必须用 `read_only=True` 打开 DuckDB。

---

## 2026-04-30 · v0.12.0 · Week 7 NL 选股

**部署内容**：

- 新 systemd 服务 `rquant-nl-screen.service`（端口 8502，独立 Streamlit）
- nginx 增加 `/nl/` 反代 → 8502，与 `/dashboard/` 共用 `.rquant-backup.htpasswd`
- `.env` 加入 `DEEPSEEK_API_KEY` / `DEEPSEEK_BASE_URL` / `DEEPSEEK_MODEL`
- `openai>=1.0` 依赖（实际安装 2.33.0）

**入口**：
- 浏览器 http://82.156.0.68:8081/nl/（用 `.rquant-backup.htpasswd` 凭据）
- 监控看板继续走 8501 / `/dashboard/`，无影响

**验证**：
- 内网 `127.0.0.1:8502/_stcore/health` ✓
- 外网 nginx /nl/ 反代 + auth ✓
- 浏览器 NL 流程跑通（解析 + Stage Cards + 运行 + 结果）

**已知遗留**：
- 前端 UX 与最终目标差距较大，下一迭代优化（Week 7.5 真画布会处理一部分）
- `/nl/` 与 `/dashboard/` 共用 htpasswd，未来想给协作者单独开放 NL 时
  按 `deploy/nl-screen.md` "未来：单独开放 NL" 一节切独立 htpasswd

**回滚命令**：

```bash
# 1. 停 systemd 服务
sudo systemctl stop rquant-nl-screen.service
sudo systemctl disable rquant-nl-screen.service
sudo rm /etc/systemd/system/rquant-nl-screen.service
sudo systemctl daemon-reload

# 2. nginx 摘掉 /nl/ location（手编辑 /www/server/panel/vhost/nginx/rquant-backup.conf 删 location /nl/ 块）
sudo nginx -t && sudo systemctl reload nginx

# 3. 代码回滚（main 上拉前一个 tag）
cd /home/lighthouse/rquant
sudo -u lighthouse git checkout v0.11.3
# 或回到 v0.12.0 上一个 commit：
# sudo -u lighthouse git reset --hard v0.11.3
sudo -u lighthouse /home/lighthouse/.local/bin/uv sync

# 4. .env 删除 DEEPSEEK_* 三行（手工编辑或 sed）
sudo -u lighthouse sed -i '/^DEEPSEEK_/d' /home/lighthouse/rquant/.env
sudo -u lighthouse sed -i '/^# ===== LLM (Week 7/d' /home/lighthouse/rquant/.env
```

监控看板（`rquant-dashboard.service`）不受回滚影响，继续运行。
