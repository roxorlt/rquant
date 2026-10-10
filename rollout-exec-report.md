
## 第十三次执行（路线 A 第六窗口，协调者主会话，2026-09-09 02:50–03:50，v0.33.4，owner「不要等收盘」；主机已升 S5.LARGE16）

| 步 | 结果 |
|---|---|
| 0 前提 | PR #246（包 M）merge `64ea140`、PR #247（包 L）merge `7b98e80` → tag **v0.33.4**；包 M 的 20 个 unit 09-08 23:39 已装；主机 00:12 升为 4 vCPU / 16 GB；68 GB 孤儿已删（df 空闲 97 GB）；日线 09-08 已补跑 |
| 1–2 | `${WT}`→v0.33.4（已装 unit 与 tag 一致；制品输入零变化）；inputs `d39ae15a…`（`7b98e80`）；profile `540d35f5…`；deployment-profile dry-run/apply rc 0 ⇒ **bundle 第五代 `20d948d1…`**（previous `3cf6160c…`），credstore 5 代，计划 64 |
| 3 acknowledge | 16 新 PREPARE→DUAL_WRITE，48 旧 not_current；复跑 changed 0 |
| 4 stage/publish | `seventh`：**seq 4**、profile `7df0eb4b…`、generation `16572929…`、op `fc21c5b4…`、86 s、wrapper_preflight 32 |
| 5 #242 转换 | reference-slow-publisher 单起：注册表头字节 2→1、无 sidecar、心跳 running ⇒ **#242 生产验证通过**；paper-constraint 裸跑 rc 124 + unit running（沙箱内打开注册表成功） |
| 6 子 slice | `set-property --runtime rquant-live-runtime.slice MemoryHigh=4096M`；cgroup 真值 `cpu.max 60000 100000`、`memory.high 4 GiB` |
| 7 启动 | 裸跑全部 rc 124；health/serving/feature/auction-universe/candidate×3/watchlist-quote/broker/notifier active；**strategy×3 起后 2 min 失败** `strategy spec does not match persisted runner identity`（gen 4 的 `runner.sqlite3`）⇒ **3 条真实推送（03:04–03:05）** ⇒ 停；挪走三份 runner db（`rquant-runner-aside-20260909-030637/`）→ strategy×3 running；router 每轮 `SignalRouteConflictError … generation changed`（gen 4 spool）⇒ 停 notifier/broker/router，`live/signal-bus/` 挪走（`rquant-signal-bus-aside-20260909-031829/`）→ router running；credstore 其余 4 逐个 active（**5/5 常驻**）；research 4 退 0（#217）、oneshot 3 退 0 ⇒ **#248** |
| 8 分诊 | runtime-health 被 5 份 09-07 旧心跳卡住 ⇒ 挪走（`rquant-stale-heartbeats-aside-20260909-034453/`）→ running；candidate n_shape/growth 被 gen 2 `authority.json` 拒 ⇒ 两候选根挪走（`rquant-candidate-aside-20260909-034659/`）→ 三 candidate running；auction-universe `database_path` = 生产主库 + 0600 ⇒ **#249**（不可窗口内修）；serving/broker/paper-constraint 盘前 DEGRADED 属链条预期；notifier DEGRADED 0 错误（#241 已消） |
| 9 持续运行 | 03:42：**20 active / 0 failed / NRestarts 全 0**；内存 used 4.7 GB / avail 11 GB，live-runtime 3.06 GB（events 0）；load 0.6；03:09:30 后告警 0；常驻全 active、dashboard 200、生产 HEAD `e4e303b`、磁盘 96 GB。**20 unit 保持运行**（R-25 因升配放宽），持久监视每 60 s |

**判据核对**：`wrapper_preflight == 32` ✅；kind-backed ≥ 12 持续运行 ✅（20）；credstore ≥ 5 ✅；strategy×3 active ✅；rollout 新代 DUAL_WRITE ✅；serving generation 待 09:25 后（reference-slow-publisher 常驻）；auction-universe ❌（#249）。
**告警**：本窗口 3 条真实推送（#248 strategy 身份不匹配，03:04–03:05）。
**回滚**：`rollback --operation-id fc21c5b4c26f0bff3653f13bfc8379f0` 回 seq 3（先停 unit、挪心跳 D-2）；bundle `current` 切回 `3cf6160c…`；注意回滚会把注册表头转回 WAL（R-26）。
**DEPLOY.md 追加草稿（第十三版，第五条运行时变更记录）**：2026-09-09 · v0.33.4 · 路线 A 第五次安装（主机升配后）：bundle 第五代 `20d948d1…`、权威链 seq 4（generation `16572929…`，wrapper_preflight 32）、#242 转换完成、#241 消失；**20 unit 全部进主循环并保持运行**（credstore 5/5、策略链 6、paper-constraint 首次）；换代耐久状态四种形状被手工挪走（#248，包 N 修）；#249 auction-universe 待修；3 条告警推送；生产代码未切换（仍 v0.28.3）。
