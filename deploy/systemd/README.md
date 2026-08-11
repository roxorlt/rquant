# systemd 部署（Linux 服务器）

腾讯云 / VPS / 任何 systemd Linux 主机用。macOS 用 `../com.roxor.rquant*.plist` launchd 那套。

## 核心调度 unit

| 文件 | 作用 | 触发时间 |
|------|------|---------|
| `rquant-daily.service` | 跑 `rquant run-daily`（ingest + screen + Pool 2） | 由 timer 触发 |
| `rquant-daily.timer` | 工作日 17:00 触发 daily.service | Mon-Fri 17:00 |
| `rquant-monitor.service` | 跑 `rquant monitor`（盘中实时） | 由 timer 触发，自然退出在 15:00 后 |
| `rquant-monitor.timer` | 工作日 09:25 触发 monitor.service | Mon-Fri 09:25 |
| `rquant-morning-pulse.service` | 读取云端只读副本与 Panorama 快照，生成30分钟脉搏 | 由 timer 触发 |
| `rquant-morning-pulse.timer` | 工作日上午四个槽位触发脉搏 | Mon-Fri 10:00/10:30/11:00/11:30 |
| `rquant-midday-report.service` | 读取云端只读副本与上午槽位，生成午间战报 | 由 timer 触发 |
| `rquant-midday-report.timer` | 工作日午间触发战报 | Mon-Fri 12:00 |
| `rquant-research-ingest.service` | daily/副本就绪后补齐并封存云端分钟/竞价研究分区 | 由 timer 触发，失败有限重试 |
| `rquant-research-ingest.timer` | 工作日 18:10 触发研究日增量 | Mon-Fri 18:10 |
| `rquant-artifact-retention.service` | 发布终态 artifact 回收 outbox、迁移热/温/冷分层并执行经恢复门禁批准的 GC | 由 timer 触发 |
| `rquant-artifact-retention.timer` | 触发 artifact retention 单步处理 | 每 5 分钟 |
| `rquant-daily-receipt-signer.socket` | root Daily receipt 签名入口，socket 为 `root:lighthouse 0660` | `active/waiting` |
| `rquant-daily-receipt-signer.service` | socket 激活的 root 签名进程，私钥和 nonce 状态只在此边界内可见 | 首次请求前可为 `inactive/dead` |

## Workload isolation cloud gate

`rquant-live.slice`、`rquant-serving.slice`、`rquant-research.slice` 和
`rquant-maintenance.slice` 是
`rquant.slice` 的子级资源边界。systemd 的 dash 命名会把它们解析为
`/rquant.slice/rquant-*.slice`，运行时验收始终读取 `ControlGroup`，不拼接假定路径。
这些 slice
不是业务 authority 的迁移工具。legacy unit 的 `ExecStart`、timer calendar、启停和写入权威
均保持不变；本轮仅为既有进程声明资源归属。

| 分类 | unit | Slice |
|---|---|---|
| live | monitor、monitor-watchdog、surge-watch、daily、daily-report、morning-pulse、midday-report、KPL snapshot、pre-market check、token reminder、OnFailure alert，以及实时 source/feature/strategy/router/notifier/paper runtime | `rquant-live.slice` |
| serving | dashboard、panorama、panorama-auth、NL screen、canvas、page-control、候选 workload sampler，以及 serving/runtime-health publisher | `rquant-serving.slice` |
| research | research-ingest、artifact retention、shadow、lab jobs、artifact catalog、promotions、recovery/rehearsal、daily-orchestrator | `rquant-research.slice` |
| maintenance | backup、replica-sync | `rquant-maintenance.slice`，并发预算求和；不修改各自 timer 或假定互斥 |
| root authority exception | daily-receipt-signer | `system.slice`，保持 root-only socket signing 边界并设独立进程上限 |

最低基线改为实测 2 CPU / 7.51 GiB 可见内存（8 GiB 标称主机）。已知生产高水位为
monitor current 2415 MiB、monitor peak 2814 MiB、backup peak 1303 MiB。证据不足以安全设置
live、serving、maintenance 或父级硬上限，因此父级/live/serving 只使用权重、`MemoryLow` 和
`MemoryHigh`，maintenance 暂时只使用低 CPU/IO 权重；仅 research 保留严格
`MemoryMax=768M`：

| 边界 | CPU / IO | MemoryLow | MemoryHigh | MemoryMax |
|---|---:|---:|---:|---:|
| `rquant.slice` | 100 / 100 | 3072 MiB | 6144 MiB | 不设 |
| live | 1000 / 1000 | 3072 MiB | 3840 MiB | 不设 |
| serving | 500 / 500 | 0 | 512 MiB | 不设 |
| research | 100 / 100，`CPUQuota=100%` | 0 | 512 MiB | 768 MiB |
| maintenance | 50 / 50 | 0 | **待校准，不设** | 不设 |

`MemoryHigh` 不是 reservation，不能用它证明 backup/replica 并发安全。正常 research 运行态的
静态上界为 live 3840 + serving 512 + research 768 + OS/其他 `system.slice` 1280 = 6400 MiB，
低于 7680 MiB。maintenance 没有足够证据形成静态内存上界，因此不能写出“总量不超”的绿色
结论。backup 与 replica 允许同类并发，证据必须同时记录独立峰值和 maintenance aggregate
峰值，不能假装互斥，也不对文件缓存设置 512 MiB hard cap。

research 与 maintenance 的跨 plane 排他由固定 root-owned
`/usr/local/libexec/rquant-workload-arbiter` 持锁覆盖整个子进程生命周期。多个 research 可并发，
多个 maintenance 也可并发；maintenance 取得意向锁后阻止新 research，并向已登记 research
wrapper 发 `SIGTERM`，限时等待其释放后执行。research 遇到 maintenance active/pending 立即以
75 拒绝，unit 将 75 视为正常准入拒绝，避免 restart storm；maintenance 等待超时 74 仍触发原
OnFailure。内核在 wrapper crash 时自动释放 flock。所有 timer calendar 和业务命令保持不变。
installer 同时发布 root-owned 0444 的
`/usr/local/libexec/rquant-workload-arbiter.sha256`；strict runtime gate 会重算固定 helper，环境变量和
`.env` 都不能改写路径。research registry 绑定 wrapper 的 PID、`/proc` starttime 与 boot ID，PID 已
退出或发生复用时只在持有 intent lock 内回收陈旧记录，绝不向身份不匹配的进程发信号。

`rquant-research-ingest.service` 通过同一 systemd start transaction 的
`Requires=rquant-replica-sync.service` 与 `After=rquant-replica-sync.service` 先运行 maintenance
oneshot；systemd 会合并与 timer 同时提交的同名 service job。required job 硬失败会阻止 ingest，
即使 replica 以既有可接受状态结束，runner 仍必须通过 generation readiness，否则只重试/失败，
不会在 research cgroup 内自行 refresh replica。

`rquant-morning-pulse.*` 与 `rquant-midday-report.*` 是本分支对 origin/main
`9699827be09ca22479f6741e820722399fe40244` 的临时整合，原始引入 commit 为
`5bb641ab23efa9595100070ff77282e18c14d170`。service 只增加 `Slice=rquant-live.slice`，
`ExecStart` 与 timer 内容逐字保留。后续正式合并 main 时必须执行 three-way/三方审计，确认上游
在这两个 commit 之后的变更没有被这次临时整合覆盖。

**必须在原始腾讯云 Linux 主机执行，macOS 的 pytest 只验证静态 fixture，不能替代此 gate。**
在 unit 文件安装、`daemon-reload` 已由明确部署操作完成后，执行下列只读验收；脚本不 reload、
不 start/restart/stop 服务，也不会制造 research 压力：

```bash
cd /home/lighthouse/rquant
bash /home/lighthouse/rquant/scripts/verify-workload-isolation.sh
```

该脚本使用固定安全 `PATH`、`/usr/bin/systemctl`、`/usr/bin/systemd-analyze` 和固定生产路径，
不接受环境变量覆盖。它逐项运行安装后的 `systemd-analyze verify`、每条原样 `OnCalendar` 的
`systemd-analyze calendar '<spec>' --iterations 5`、`systemctl show` 与 cgroup v2
`cpu/io/memory/pids` 控制器检查，并要求至少 2 CPU、7680 MiB 可见 RAM、2048 MiB 当前可用
内存和 8 GiB 数据盘可用空间，并核验 arbiter、其 SHA-256 文件、运行目录/四个 lock 的 root owner
与固定 mode。
它还严格读取至少 24 小时的
`/var/lib/rquant/workload-isolation/high-water.json`，验证 live/serving 并发峰值以及
backup+replica 峰值之和。任一失败时不得启动 research，也不得把 research 满载视为对
live SLO 的有效证明。

`rquant-workload-sample.service/.timer` 是无安装目标的候选采样器，只能由操作人显式 start，
不会随发布自动启用。它每次只读 `systemctl show` 和 `/proc/meminfo`，append+fsync canonical
hash-chain record 到 `samples.jsonl`。每个 schema v2 sample 记录 Linux boot ID、wall timestamp
与 `CLOCK_BOOTTIME` 纳秒值，外层 record 记录连续序号与前序 record SHA-256；不启动、停止或
reset 任何业务 unit。严格摘要只接受同一 boot 内 wall/boottime 都严格递增、单次 gap 不超过
450 秒的连续段。5 分钟 cadence 的 24 小时段至少需要 289 个端点样本，多个 boot 可以保留在
同一 raw 链中，但不能拼接成一个窗口。至少一个完整同 boot 窗口且 backup/replica 都有非零成功运行后，操作人显式运行
`scripts/summarize-workload-high-water.sh` 生成摘要。
gate 本身仍只读。字段单位均为 MiB，`observed_at` 最长允许 30 天：

```json
{
  "schema_version": 1,
  "observed_at": "2026-08-05T02:00:00+00:00",
  "observation_window_hours": 24,
  "host_mem_total_mib": 7690,
  "monitor_current_mib": 2415,
  "monitor_peak_mib": 2814,
  "live_concurrent_peak_mib": 3300,
  "serving_concurrent_peak_mib": 420,
  "backup_peak_mib": 1303,
  "replica_peak_mib": 300,
  "maintenance_concurrent_peak_mib": 1500,
  "backup_successful_runs": 3,
  "replica_successful_runs": 12,
  "backup_sample_count": 289,
  "replica_sample_count": 289,
  "backup_successful_runtime_seconds": 180,
  "replica_successful_runtime_seconds": 480,
  "raw_evidence_sha256": "<64 lowercase hex>",
  "os_system_slice_peak_mib": 910,
  "min_mem_available_mib": 2200
}
```

示例值不能作为生产证据。strict gate 会重新验证 raw canonical/hash chain、重新执行
`summarize_workload_samples` 并逐字段比对摘要；伪 raw 即使自声明匹配的文件 SHA 也会失败。缺任一
字段、链或 raw SHA 不匹配、同 boot 窗口不足 24h/289 样本、时钟倒退、gap 超过 450 秒，或
backup/replica 任一无成功 run/样本/持续时长都失败。两个相隔 24 小时的样本不能通过。
即使证据完整，当前 maintenance 阈值仍处于 pending
calibration，因此 relaxed health 只能是 warn，strict cloud/research admission 仍 fail closed；
后续必须用这批证据做独立阈值评审，不能由采样器自动决定资源上限。

### Research pressure SLO acceptance

生产机上的 `verify-workload-isolation.sh` 永远只读，**不得**自动生成 CPU、内存或 I/O 负载。
满载验证只能由操作人于 staging 或已批准的维护窗口手工执行，且不得读取/写入 rQuant 业务 authority。
建议使用一个临时、独立的 research slice scope（例如 `systemd-run --scope
--slice=rquant-research.slice` 运行已审查的压力工具），并在开始前、满载 15 分钟期间、结束后各运行一次：

```bash
cd /home/lighthouse/rquant
bash /home/lighthouse/rquant/scripts/verify-workload-isolation.sh
systemctl show rquant-monitor.service rquant-surge-watch.service --no-pager \
  --property=ActiveState,Result,NRestarts,Slice,ControlGroup
live_cgroup=$(systemctl show rquant-live.slice --value --property=ControlGroup)
research_cgroup=$(systemctl show rquant-research.slice --value --property=ControlGroup)
cat "/sys/fs/cgroup${live_cgroup}/memory.events"
cat "/sys/fs/cgroup${research_cgroup}/memory.events"
```

验收记录必须同时保存运行时 health authority 的 live p95（分钟批次 <10s、发布到信号 <5s、
信号到首次通知 <5s）、上述三次输出及压力 scope 的退出状态。只有全程 live 无 failed/restart、
live cgroup 无 `oom` 增量、p95 未越过目标且 research 保持其 `CPUQuota/MemoryMax` 时，才可写
“research 满载未拖垮 live”的结论。单机共享磁盘、网络和内核 OOM 仍是残余风险，重型回补和参数搜索
应继续留在 Mac/独立 research worker。

### Legacy runtime slice migration gate

安装主机若仍 loaded `rquant-runtime-live@*` 或 `rquant-runtime-research@*`，严格 cloud gate
会阻塞并明确列出实例。先运行只读 preview；它以退出码 3 表示仍有迁移待批准，绝不改变状态：

```bash
bash /home/lighthouse/rquant/scripts/migrate-legacy-runtime-slices.sh \
  --replacement rquant-runtime-live@svc-old.service=rquant-runtime-feature@svc-new.service
```

只有用户明确批准后才增加 `--accept`。replacement 必须是具体实例，任何 `@.service` template、
legacy template 派生实例或当前 `legacy_units` 成员都拒绝。已安装但当前无实例的旧模板也必须映射
到一个已运行并验证过的具体实例。maintenance 阈值 pending 期间 research replacement 不可 accept。
脚本会先验证所有 replacement 都是 loaded/active，且
其 `Slice` 与 `ControlGroup` 已落入 resolved live/serving/research cgroup；全部通过后才对旧实例
执行 `disable --now`，移除旧模板并 `daemon-reload`。accept 在 mutation 前原子发布持久 journal 到
`/var/lib/rquant/workload-isolation/migration/active`，保存旧 unit 文件及每个实例的 enabled/active
状态。从 startup recovery 到 journal 删除始终持有固定 root-owned `migration.lock`；第二个并发
调用以退出码 6 报 busy，不进入 discovery 或 mutation。phase/state 都在目标目录内写临时文件，
依次 fsync file、rename、fsync directory；phase 另有 `phase.last-good` 冗余。`ERR/TERM/INT/HUP`
共用同一个恢复器；SIGKILL/断电无法 trap，但下一次 preview/accept 会先恢复文件、reload、
enable/start 并逐项验证。单份或双份 phase 撕裂时按 last-good 或 fail-safe rollback 恢复；unit/state
journal 缺失或损坏仍 fail closed。mutation 前只接受可精确
恢复的 active/inactive 与 enabled/enabled-runtime/disabled
矩阵；mutation 后再次验证 replacement loaded/active/Slice/ControlGroup，漂移同样触发 rollback。
它不删除任何 replacement、不重启其他生产服务，也不替用户选择业务 replacement。

## Daily orchestrator timer 受控接受/启动（两阶段）

`rquant-runtime-daily-orchestrator@<instance>.timer` 走 `scripts/deploy.sh` 的两个独立 flag，语义不同：

```bash
# 阶段一：只显式接受并 enable 指定实例，不 start，也不立即校验 NEXT trigger
RQUANT_DAILY_ORCHESTRATOR_INSTANCE=<instance> \
  bash scripts/deploy.sh --accept-daily-orchestrator-timer

# 阶段二：只 start（不 enable），并校验 ActiveState=active/SubState=waiting + NEXT trigger 在 1 年内
RQUANT_DAILY_ORCHESTRATOR_INSTANCE=<instance> \
  bash scripts/deploy.sh --start-daily-orchestrator-timer
```

两个 flag 都要求 `RQUANT_DAILY_ORCHESTRATOR_INSTANCE` 合法（非法字符/路径分隔符直接 `exit 1`，不触发任何 `systemctl` 调用）。`--accept-daily-orchestrator-timer` 不检查 active/waiting 或 NEXT；`--start-daily-orchestrator-timer` 才检查 active/waiting 与 NEXT，任一不满足即 `exit 2`。

## 安装步骤

服务器跑：

```bash
# 1. 安装 root-owned workload wrapper/tmpfiles；不会改变 unit state
bash ~/rquant/scripts/install-workload-isolation-infra.sh

# 2. 复制 unit 到系统目录
sudo cp ~/rquant/deploy/systemd/*.service /etc/systemd/system/
sudo cp ~/rquant/deploy/systemd/*.timer /etc/systemd/system/
sudo cp ~/rquant/deploy/systemd/*.socket /etc/systemd/system/
sudo cp ~/rquant/deploy/systemd/*.slice /etc/systemd/system/

# 3. 让 systemd 重读配置
sudo systemctl daemon-reload

# 4. 启用并启动既有 timers（候选 workload sampler/research 不在此处启用）
sudo systemctl enable --now rquant-daily.timer
sudo systemctl enable --now rquant-monitor.timer
sudo systemctl enable --now rquant-morning-pulse.timer
sudo systemctl enable --now rquant-midday-report.timer
sudo systemctl enable --now rquant-daily-receipt-signer.socket

# 5. 验证
systemctl list-timers --no-pager | grep rquant
```

研究日增量必须按
[独立上线手册](../../docs/deploy/research-daily-ingest-rollout.md)先完成手工运行和候选验收，
不得随基础 unit 批量启用。

## Resource authority 权限域草案

`rquant-external-monotonic-root.service` 与 `rquant-resource-authority.service` 不属于上面的
批量安装范围，本轮只提交草案，不安装、不启动。正式安装前先运行：

```bash
sudo bash scripts/install-resource-authority-infra.sh \
  --dry-run \
  --release-sha <exact-40-character-release-sha>
```

审阅后才可显式改用 `--apply`。脚本从该精确 commit 的 `git archive` 构建 non-editable venv，
把所有 symlink 物化，生成逐文件 SHA-256 清单，并用预先配置的 root-only Ed25519 key 签名。
不可变 generation 位于 `/usr/local/libexec/rquant-authority-runtime/generations/<release-sha>`；
两个 unit 只执行 `current/venv/bin/rquant`，不读取 `lighthouse` 可写的 checkout 或 `.venv`。
脚本同时创建静态用户、共享 client group、独立目录和最小 EnvironmentFile 权限；不复制 unit、
不调用服务管理命令、不修改 sudoers。`--apply` 前必须单独预置：

- `/etc/rquant/keys/authority-runtime/runtime.private.pem`：`root:root 0400`；
- `/etc/rquant/keys/authority-runtime/runtime.public.pem`：`root:root 0444`。

- external root：用户 `rquant-external-root`，主组 `rquant-root-client`；runtime/state 分别为
  `/run/rquant-external-root`、`/var/lib/rquant-external-root`。
- resource authority：用户 `rquant-resource-authority`，主组 `rquant-resource-client`，并加入
  `rquant-root-client`；runtime/state 分别为 `/run/rquant-resource-authority`、
  `/var/lib/rquant-resource-authority`。
- `lighthouse` 只加入 `rquant-resource-client`，不加入 `rquant-root-client`。
- 两个 socket 目录为 `0750`、socket 为 `0660`；客户端可连接但不能写父目录或替换 socket。
- root 私钥为 `rquant-external-root: rquant-root-client 0400`；root backend 为该用户 `0600`。
  resource authority 只读 root 公钥并通过 root socket 请求，不能读取私钥或写 backend。
- 两个 unit 只读 `/etc/rquant/external-root.env` 与
  `/etc/rquant/resource-authority.env`；parser 拒绝 unknown/duplicate/export/插值/非 canonical 行。
  字段模板在 `deploy/env/`，禁止复制应用 `.env`，resource env 不引用主 `DATA_DIR/DUCKDB_PATH`。

Linux 上线 gate 必须另外验证 `systemd-analyze verify`、真实 `SO_PEERCRED`、目录 owner/mode、
resource 用户无法读取 root 私钥/写 root backend，以及 `lighthouse` 无法直连 root socket。
preflight 还必须重算 runtime 全文件 hash、验 manifest 签名和完整祖先链，并核对 unit 的实际
`EnvironmentFile` 精确路径。草案仍不安装、不启动，Linux gate 通过前不得部署。

## 验证 + 测试

```bash
# 看 timer 状态
systemctl status rquant-daily.timer
systemctl status rquant-monitor.timer
systemctl status rquant-morning-pulse.timer
systemctl status rquant-midday-report.timer
systemctl status rquant-research-ingest.timer
systemctl status rquant-artifact-retention.timer
systemctl status rquant-daily-receipt-signer.socket
systemctl status rquant-daily-receipt-signer.service

# Daily receipt signer 的健康形态是 socket active/waiting；service 在首次请求前 inactive/dead 正常。

# 新 retention unit 只能在云端 systemd 主机做语法验收
sudo systemd-analyze verify /etc/systemd/system/rquant-artifact-retention.service /etc/systemd/system/rquant-artifact-retention.timer
systemd-analyze calendar '*-*-* *:0/5' --iterations 5

# 查下次触发时间
systemctl list-timers --no-pager | grep rquant

# 手动触发一次 daily（不等到 17:00）
sudo systemctl start rquant-daily.service

# 看 daily 跑得怎么样
journalctl -u rquant-daily.service -n 100 --no-pager

# 看 monitor 跑得怎么样
journalctl -u rquant-monitor.service -n 100 --no-pager
```

## 节假日处理

A 股节假日 systemd timer 不知道，会照常 09:25 / 17:00 / 18:10 触发。但应用层在非交易日内部退出：

- `monitor` 启动后 `is_trading_day(today)` 检查（akshare 交易日历），非交易日立即 return 0
- `run-daily` ingest 在非交易日 Tushare 返回 0 行，pipeline 跳过
- `research-ingest` 默认日期读取权威 SSE 日历，明确休市时返回 `skipped`；日历缺口仍报错

所以节假日 timer 触发也不会出问题。

## 关闭/卸载

```bash
sudo systemctl disable --now rquant-daily.timer rquant-monitor.timer
sudo rm /etc/systemd/system/rquant-{daily,monitor}.{service,timer}
sudo systemctl daemon-reload
```
