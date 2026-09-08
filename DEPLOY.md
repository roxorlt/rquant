# Deploy Log

> 每次部署到 82.156.0.68 时追加一条。日期 + tag + 备注 + 回滚命令。
> 最新在最上面。

---

## 2026-09-08 · 待安装 · 主机资源包络（#243，owner 裁决 21）

**状态**：**尚未安装**。本条是安装说明，不是部署记录；真正装上去之后请在本条下面补
执行时间、`systemd-analyze` 输出和验收结果。

**为什么必须手工装**：改动落在 `deploy/systemd/`，受控发布器
（`scripts/deploy-production.sh`）按设计拒绝任何含该目录的 diff（见
`docs/production-release.md`「自动拒绝」），CLAUDE.md 第 7 条也把 `deploy/systemd/` 列为需要
owner 单独明确授权的高风险变更。因此代码可以照常走发布器，**unit 文件这部分要 owner 点头后
按下面的步骤人工安装**。

**改了哪 5 个文件**：`rquant-backup.timer`（盘中 5min → 30min）、`rquant-backup.service`
（`TimeoutStartSec` 20min、新增 `TimeoutStopSec=2min`）、`rquant-live.slice`（`CPUQuota=60%`）、
`rquant-serving.slice`（`CPUQuota=30%`）、`rquant-maintenance.slice`（`CPUWeight=300`）。
`rquant.slice` 与 `rquant-research.slice` 未改，但下面的 verify 一并跑一遍不吃亏。

### 1. 装之前先在云端验语法（mac 上验不了）

```bash
cd /home/lighthouse/rquant && git fetch --tags && git checkout <tag>
tmp="$(mktemp -d)"
cp deploy/systemd/rquant-backup.service deploy/systemd/rquant-backup.timer \
   deploy/systemd/rquant-live.slice deploy/systemd/rquant-serving.slice \
   deploy/systemd/rquant-maintenance.slice deploy/systemd/rquant.slice \
   deploy/systemd/rquant-research.slice "${tmp}/"
systemd-analyze verify "${tmp}"/rquant-backup.service "${tmp}"/rquant-backup.timer \
    "${tmp}"/rquant-live.slice "${tmp}"/rquant-serving.slice \
    "${tmp}"/rquant-maintenance.slice "${tmp}"/rquant.slice "${tmp}"/rquant-research.slice
echo "verify rc=$?"      # 期望 0，且不打印任何 warning
systemd-analyze calendar 'Mon..Fri *-*-* 9..15:0/30' --iterations 5
systemd-analyze calendar 'Mon..Fri 17:30' --iterations 5
rm -rf "${tmp}"
```

`9..15:0/30` 期望：`Normalized form: Mon..Fri *-*-* 09,10,11,12,13,14,15:00,30:00`，5 个
iteration **间隔 30 分钟**（不是 30 秒）。看到 `Invalid argument` 或秒级步进就**停下不要装**。

### 2. 安装

```bash
sudo cp deploy/systemd/rquant-backup.service deploy/systemd/rquant-backup.timer \
        deploy/systemd/rquant-live.slice deploy/systemd/rquant-serving.slice \
        deploy/systemd/rquant-maintenance.slice /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart rquant-backup.timer      # timer 必须重启才按新 calendar 排期
systemctl list-timers rquant-backup.timer       # 下一次触发应落在 :00 或 :30
```

### 3. slice 改动怎么对**已经在跑**的 unit 生效（重点）

`daemon-reload` 只让 systemd 重读文件，**不会把新的资源属性推给已经 active 的 slice**；
slice 的 cgroup 属性是在它被创建（第一个成员启动）时写下的。两条路，二选一：

- **推荐（不重启任何服务）**：用 `--runtime` 就地下发，重启后自然回落到 unit 文件的值。
  ```bash
  sudo systemctl set-property --runtime rquant-live.slice CPUQuota=60%
  sudo systemctl set-property --runtime rquant-serving.slice CPUQuota=30%
  sudo systemctl set-property --runtime rquant-maintenance.slice CPUWeight=300
  ```
  **一定要带 `--runtime`**：不带的话 systemd 会在 `/etc/systemd/system.control/` 里写永久
  drop-in，从此**盖住**仓库里的 unit 文件，以后改 git 不再生效，且悄悄与仓库分叉。
- **或者**：等下一次这些 slice 里的 unit 全部停过再起（盘后窗口），slice 重新创建时按文件生效。

核对（cgroup 里的真值，不看 systemd 自己的缓存）：

```bash
systemctl show -p CPUQuotaPerSecUSec -p CPUWeight rquant-live.slice rquant-serving.slice \
    rquant-maintenance.slice
cat /sys/fs/cgroup/rquant.slice/rquant-live.slice/cpu.max        # 期望 60000 100000
cat /sys/fs/cgroup/rquant.slice/rquant-serving.slice/cpu.max     # 期望 30000 100000
cat /sys/fs/cgroup/rquant.slice/rquant-maintenance.slice/cpu.weight   # 期望 300
```

**忘了这一步会被验收抓住**：`scripts/verify-workload-isolation.sh` 会把 cgroup 里的
`CPUWeight` 与仓库的 `WORKLOAD_SLICE_LIMITS` 逐字段对比，maintenance 还是 50 时会直接报
`rquant-maintenance.slice: cgroup CPUWeight='50', expected '300'`。

### 4. 装完的验收

```bash
sudo bash scripts/verify-workload-isolation.sh          # 只读，不改任何状态
sudo systemctl start rquant-backup.service              # 盘后手工跑一次
journalctl -u rquant-backup.service -n 40 --no-pager    # 期望 Result=success，无超时
tail -5 /home/lighthouse/rquant/logs/backup-snapshot.log
ls -lA /home/lighthouse/rquant/backup/                  # 期望没有新的 .latest.* 残留
```

### 5. 68 GB 孤儿文件是**另一件事**

`backup/` 里 2026-08-03..05 留下的 `.latest.duckdb.<pid>` / `.latest.duckdb.<pid>.gz`
共约 68 GB（磁盘 120 GB，只剩 16–18 GB）。**本次改动不删它们**：新脚本的开头清扫只在**下一次
备份运行时**才会碰到这些文件，而清理 68 GB 是一次单独的、需要 owner 明确点头的生产操作。
装完之后如果 owner 还没点头，请留意第一次备份运行会把它们一次性扫掉（它们都远超 1 天），
所以**要么先取得授权、要么在装之前把这批文件挪走留证**。删除前建议先记一份清单：

```bash
ls -lA /home/lighthouse/rquant/backup/.latest.* > /home/lighthouse/orphans-20260908.txt
du -ch /home/lighthouse/rquant/backup/.latest.* | tail -1
```

### 回滚

```bash
cd /home/lighthouse/rquant && git checkout <上一个 tag> -- deploy/systemd
sudo cp deploy/systemd/rquant-backup.{service,timer} deploy/systemd/rquant-{live,serving,maintenance}.slice \
        /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl restart rquant-backup.timer
sudo systemctl set-property --runtime rquant-live.slice CPUQuota=
sudo systemctl set-property --runtime rquant-serving.slice CPUQuota=
sudo systemctl set-property --runtime rquant-maintenance.slice CPUWeight=50
```

`CPUQuota=`（空值）就是取消限额。脚本改动（trap 与开头清扫）没有生产状态，回滚即回滚代码。

---

## 2026-09-07 · v0.32.2 · 路线 A 首次安装（权威链 sequence 3，生产代码仍未切换）

**状态**：路线 A——「由操作员产出生产 inputs 文档 + 生成一代真实画像」这条路——第一次真正装到
生产主机上。权威链发到第三代（sequence 3），`wrapper_preflight == 32`，`data/runtime/current`
第一次存在，credstore 密封了 7 个实例，**8 个 kind-backed unit 持续运行**（路线 B 下 live 平面
是 0 个）。**生产代码仍未切换**，还是 `e4e303b0a4c05d2a4deefbee502718053672fe6f`（v0.28.3）；
第二关（切代码 + 换 3.11 venv + 重启七个常驻服务）没做，而且**现在还排不了**，原因见下面
「第二关为什么还排不了」。

**tag**：`v0.32.2`（annotated）→ `695e952038aff8b426632233511d351fac2c7353`

**执行**：SSH `lighthouse@82.156.0.68`，2026-09-06 23:10 至 2026-09-07 06:50，分三段：

| 段 | 时间（CST） | 做了什么 | 当时 tag |
|---|---|---|---|
| 一 | 09-06 23:10–23:35 | 补两套凭证、发 completion 公钥环、换 sealer helper、生成生产 inputs；第 6 步撞 BLK-8 停下 | `v0.32.1`（`77ddb32`） |
| 二 | 09-07 00:48–00:56 | 换到 v0.32.2 重做 inputs；prerequisites 与 production-profile 落地 | `v0.32.2`（`695e952`） |
| 三 | 09-07 01:0x–06:50 | deployment-profile（写 `current` + credstore）、legacy stage、publish、探路、启动、收尾 | `v0.32.2` |

BLK-8 是 `rquant runtime-production-prerequisites` 在没有 `.env` 的 bootstrap worktree 里死在
`main()` 的 `get_settings()` fail-fast 上——那三条路线 A 命令自己一个配置都不读，只是没有被提前
分发。修复即 PR #213，合并打 tag `v0.32.2` 之后从第 6 步继续。

### 装了什么

**凭证（一律增量）**：`install-runtime-credential-keys.sh init --only-missing` 补出 completion 与
capabilities 两组（原有 9 个文件的 sha256 逐字节未变），`verify` 13 行 `OK` 外加
`consumer self-check passed (root form)`；`install-runtime-credential-infra.sh
--only-missing-keyrings` 只发布 `shadow-completion-trusted-keys.json`（`root:root 444`），
helper / unit / sudoers 一律未动。密封 helper
`/usr/local/libexec/rquant-runtime-credential-sealer` 按 #208 原子换代到 sha256
`2612a0c705d94617795197a39a953ec8145fbf27b1c230ce5b92d0569005beec`（与仓库一致），
旧版留在 `/root/rquant-helper-backup-20260906-024504/`。

**加密备份**：`/home/lighthouse/rquant-credentials-backup/etc-rquant-20260906-231708.tar.enc`
（0600，133,152 B，sha256
`0d30233a6b93b3e500582682f56d6193c28671a7bb322955787574475b2d5730`），口令文件同目录 0600，
解密往返自检 25 个条目逐条对上。**这份备份未离机**，口令内容不出服务器、也不写进本文件。
旧备份保留。

**生产 inputs 与真实画像**：

| 制品 | 位置与摘要 |
|---|---|
| inputs 文档 | `data/runtime-production-inputs.json`（0600），sha256 `173f38cdc0c8166f722f12139721cab6292075f759c49c8e4d73dbac55663b49`，`producer_commit 695e952…`；生成器两跑逐字节相同 |
| 市场日历 | coverage **2020-01-01..2026-12-31**，`open_dates` **1697**，`content_sha256 eceaa714…083f`；显式传了 `--calendar-coverage-floor 2026-12-31`，摘要与 stderr WARNING 都写着 `renew_by=2026-12-01`（#211） |
| 路由策略 | `signal-routing-policy.json` fingerprint `9f9b5a6e…33ae`（0444） |
| 生产画像 | `data/runtime-profiles/1e5b5fef….json`（0600，67,694 B，`service_count 26`），sha256 `4bc3142175f6bb5aa86afceb199b582fd60770c7d9b30f51dd7bacb3547178ff` |

`runtime-production-prerequisites --apply` 第一次退 1：`retention state root must be an owned
directory with mode 0700`——C-1 预建的 `data/runtime/research/**` 是 0755。13 个目录 `chmod 700`
之后 rc 0，三个 target 落地（市场日历 generation `31264c77…` 0600、`runtime-inputs/definitions`
0700、`research/artifact-retention/svc-248ba9b2…/catalog-authority/current.json` 0600）。

**`data/runtime/current` 与 credstore**（`runtime-deployment-profile --apply`，06:01 起）：

- `data/runtime/current` → `generations/7d572c7938e3def3d72828c1149049727393f2ccff3e7b05e4df0a31f6495dd6`，
  **相对 symlink**；`deployment-profile.json` 0600 / 67,694 B。这个指针正是路线 B 从不写的那一份，
  写出来之后原本走降级分支的角色才有 schema binding 可装。
- `/etc/credstore.encrypted/rquant-runtime/instances/` 下 **7 个 svc** 各有 `current.cred` 与
  `generations/`；`/var/lib/systemd/credential.secret` 由 systemd 自建为 **`root:root 0400`**
  （不是前置第 12 条原先写的 0600，该条已就地订正）。
- dry-run 首代必须带 `--schema-bootstrap-reason`；六个 `RQ_*` 能力变量与生产 `.env` 里的
  `TUSHARE_TOKEN_MAIN` / `PUSHDEER_*` / `PUSHPLUS_*` 都要在进程环境里。**读生产 `.env` 是 owner
  当场单独授权的**，设计上 deployer 就是带 `.env` 跑、把这些密封进 credstore。

**权威链第三代**：

| 项 | 值 |
|---|---|
| sequence | **3** |
| generation | `e850250e40da5b54696bc39f797da8fe2d48035010df49592f4c9ef7a2042b53` |
| operation_id | `9aa830e45819d8fb6ccad0e170ff89b5` |
| prior generation | `ff79b184…`（sequence 2） |
| profile_id | `d2206e53…7ea0`，**与前两代相同**，走「同 profile 换 generation」，#190 未触发 |
| producer commit | `695e952` |
| `wrapper_preflight` | **32** |
| 耗时 | stage apply 17 s；publish **96 s**（正式发布，不是 dry-run） |

stage 走 legacy 模式：`--legacy-runtime-root /home/lighthouse/rquant/data/runtime`（字面量）、
`--legacy-generation current`；`instance_mapping` 25 role / 29 label，28 份 manifest，
closure 584/50/105,668，`staged_files` 11,104；`legacy-binding.json`（224 B，`mode` 292，
sha `2c21a078…dc90`）里 `generation_id` 等于 `readlink current`、`runtime_root` 等于那个字面量，
并且出现在 `full-manifest.json` 的 `entries` 里；`plan.json` sha256
`8ee7e5a1b337c6b11af42b3e2aefc2a4ed0ad01ad3a2fc03766576bc635f2cdc`。publish 回执
`result committed` / `state active`，两个文件都是 `root:root 444 nlink=1`。

### 结果：8 个 unit 持续运行

06:45 复核：**8 个 unit active、`NRestarts` 全 0**，20 分钟 journal 里 0 条 traceback /
fail-closed / refused，runtime 进程的 `/proc/<pid>/fd` 里没有 duckdb 句柄，三个 slice 的
`memory.events` 全是 `high 0 / oom_kill 0`：

| 平面 | unit | 起始时刻 |
|---|---|---|
| live | `rquant-runtime-auction-universe@` | 06:14:12 |
| live | `rquant-runtime-candidate@` ×3 | 06:14:15 / 06:14:18 / 06:14:21 |
| live | `rquant-runtime-feature@` | 06:14:24 |
| live | `rquant-runtime-watchlist-quote@` | 06:19:10 |
| serving | `rquant-runtime-runtime-health@` | 06:22:53 |
| serving | `rquant-runtime-serving@` | 06:22:58 |

`daily_pipeline_orchestrator` 是 oneshot，`ExecMainStatus=0` 正常退出，不计在这 8 个里。
`data/runtime/current` 复核为相对 symlink，正确。

**换代残留心跳**：`runtime_health_publisher` 与 `serving_publisher` 第一次裸跑退 1，报
`runtime heartbeat does not match the requested service spec`——sequence 2 停掉的那两个实例在
`control/<role>/<svc>/heartbeats/<identity>.json` 留下了 `spec_fingerprint` 属于旧 spec 的心跳。
确认实例确已停（`pid` 为 `None`、`stopped_at` 有值）后把心跳文件移走，两个角色即启动成功。**#216**

**起不来的角色，按原因分类**：

| 原因 | 角色 | issue |
|---|---|---|
| research 平面被高水位证据门挡住：启动即打 `FAIL research blocked: high-water evidence unavailable or invalid: /var/lib/rquant/workload-isolation/high-water.json` 然后**退 0**（`Result=success`）。这是 workload arbiter 的资源门，不是崩溃 | `lab_artifact_catalog`、`promotions_publisher`、`shadow_session`、`lab_jobs_publisher` | **#217** |
| credstore 组：`reference_slow_source` / `market_minute_source` / `auction_match_source` 在 wrapper 白名单子环境里构造 `Settings` 缺 5 个字段；`daily_close_source` 报 `TUSHARE_TOKEN_MAIN capability is required`；`reference_slow_publisher` 报 `requires its isolated publication credential`；`notifier` 缺 route spool。全部 `stop` + `reset-failed` 防重启风暴 | 上述 6 个 | **#215**（`notifier` 另涉 #218） |
| completion signer / router / broker / recovery 这一串：`strategy_live` ×3 报 `completion signer profile contains invalid manifests`，`signal_router` 缺 runner source，`paper_broker` 缺 route spool，`runtime_recovery` 与 `rehearsal` 报 `profile generation is stale` | 7 个 | **#218** |
| 缺 reference registry，要等 `reference_slow_*` 先产出 | `paper_constraint_publisher` | #205 |
| `rquant-runtime-lab-jobs@` 的 `InaccessiblePaths=/etc/rquant/lab-claim-finalizer-runtime` 没有 `-` 前缀，路径不存在即 `226/NAMESPACE`；建一个空目录才能起（起来之后仍被上面那道 high-water 门挡住） | `lab_jobs_publisher` | #191 |

`page_control` 裸跑能起，但它不是 runtime unit，本次没有 start。

### 第二关为什么还排不了

**credstore 密封了 7 个实例，逐个 start 过的 6 个 role 一个都没能持续运行。**
`reference_slow_source` 与 `notifier` 在窗口中段曾各自重启一次后短暂 running（当时快照记的是
9 个 unit），到 06:45 复核时两个都已 failed 并被 stop + reset-failed；另外四个从头就起不来。
判据按整组记 **0/7**。

**没有 `reference_slow_publisher` 就没有 serving generation，第二关（包 D）不能排。**

判据核对：`wrapper_preflight == 32` ✅；`data/runtime/current` 是相对 symlink ✅；
credstore 7 份 `.cred` ✅；「≥ 12 个 kind-backed 持续运行」❌（实际 8 个）；
「credstore 组能起的都起」❌（0/7）。

### 一条告警推送

06:26:34–06:27:44，`notifier` 的 unit 失败触发 `OnFailure=rquant-alert@` 共 5 次；日志显示是同类
去重告警，owner 手机上大概率只收到 1 条。这是本次窗口唯一的推送。

### 现役零损伤

七个常驻服务 `NRestarts` 全 0；四个端口 `200/200/200/302`；生产 checkout 仍是 `e4e303b`，
`git status` 干净；生产主库 DuckDB 的 mtime 停在 2026-09-04 17:02:53 未变；bootstrap worktree 里
`.env` 不存在（第 8 步是把生产 `.env` 读进进程环境，没有拷贝落盘）；磁盘剩 19 GB。

### 回滚

```bash
# ① 停本次启动的 8 个 unit（模板 unit 没有 enable，stop 即回到未运行）
sudo systemctl stop 'rquant-runtime-auction-universe@*.service' \
     'rquant-runtime-candidate@*.service' 'rquant-runtime-feature@*.service' \
     'rquant-runtime-watchlist-quote@*.service' \
     'rquant-runtime-runtime-health@*.service' 'rquant-runtime-serving@*.service'

# ①b 回滚到「#231/#232 那一代之前」的任何一代之前，先把心跳文件挪走（见下方说明）
STAMP=$(date +%Y%m%d-%H%M%S)
sudo install -d -m 0700 "/home/lighthouse/rquant-heartbeats-aside-${STAMP}"
sudo find /home/lighthouse/rquant/data/runtime/control -mindepth 3 -maxdepth 4 \
     -path '*/heartbeats/*.json' \
     -exec mv -t "/home/lighthouse/rquant-heartbeats-aside-${STAMP}/" {} +

# ② 单级回到 sequence 2
sudo /usr/bin/python3.11 -I -S /usr/local/libexec/rquant-production-deploy.pyz \
     rollback --operation-id 9aa830e45819d8fb6ccad0e170ff89b5

# ③ credstore 的 current.cred 切回上一代

# ④ 删掉 current 指针；角色自动回到降级分支（路线 B），不需要再换 generation
sudo rm /home/lighthouse/rquant/data/runtime/current
```

**①b 为什么是必须的（#231/#232 那一包引入，与 #216 同一类）**：`RuntimeServiceHeartbeat` 是
`extra="forbid"` 的，心跳文件又**不按 generation 分目录**——`<control-root>/heartbeats/<sha256>.json`
一个 role 一份，换代之后还在原地。那一包给心跳加了 `waiting_for` / `waiting_since` /
`waited_seconds` 三个字段，方向是单向的：**新二进制读旧心跳**没问题（三个字段缺省为 `None`），
**旧二进制读新心跳**直接拒——`read_heartbeat` 把解析失败变成
`ValueError: runtime heartbeat is invalid: <service_id>`，打中的是 `start()`、健康面
（`runtime_health_authority` 的读取与 `inspect_runtime_health`）以及 rollout reader 三处。
所以**回滚到那一包之前的任何一代，都要先停 unit、再把 `$ROOT/control/*/*/heartbeats/*.json`
挪走**，挪走的文件留档不要删（前置第 21 条是同一类操作，那次是 #216 的 spec 变更）。

`generation` 目录是内容寻址的，三代全部保留，**永不删除**。代码本次未切换，无需代码回滚，
锚点 `e4e303b`（服务器上 `/home/lighthouse/rollback-code-sha.txt`）。
**⚠️ 私钥删了不可恢复**，回滚前先确认
`/home/lighthouse/rquant-credentials-backup/etc-rquant-20260906-231708.tar.enc` 能解开。

完整执行记录（每条命令与输出原文）在 Mac 本地的
`/Users/roxor/brain/30-projects/rQuant/.worktrees/release-a-cc/.superpowers/sdd/2026-09-03-release-a-rollout/rollout-exec-report.md`
的「第八次执行」「第八次执行续」「第八次执行终」三节，没有进仓库。

### 等你决策

1. **#215**：credstore 那 6 个 role 在 wrapper 子环境里跑不起来。这是第二关的硬前置，
   不解决就没有 serving generation。
2. **#216**：换代残留心跳目前靠手工移文件绕过，应该由发布链路自己作废旧代心跳。
3. **#217**：`/var/lib/rquant/workload-isolation/high-water.json` 由谁产出、什么时候产出还没定，
   research 平面四个角色一直被这道门挡着。
4. **#218**：completion signer 的 manifest 为什么无效，以及 `strategy_live` → `runner.sqlite3`
   → router → spool → broker/notifier 这条依赖链怎么接上。
5. **#211 的续期截止是 2026-12-01**：那之前必须把 `trade_calendar` 扩到 2027 年、重跑生成器与
   整条命令链、去掉 `--calendar-coverage-floor 2026-12-31`。往生产库写数据属高风险变更，需单独授权。
6. **凭证备份仍未离机**；**#191**（finalizer 的四份输入）与 **#192** 的 `[Install]` 段仍未做，
   服务器重启后这 8 个 unit 不会自动拉起。

---

## 2026-09-05 · v0.31.3 · Release A 第一关基础设施装机（生产代码未切换）

**状态**：第一关最终口径已达成——发布回执 `wrapper_preflight == 32`，serving 平面两个 kind-backed
服务持续运行。**生产代码未切换**，仍是 `e4e303b0a4c05d2a4deefbee502718053672fe6f`（v0.28.3）。
本次只装第一关的基础设施，第二关（切代码 + 换 venv + 重启七个常驻服务）未做。

**tag**：`v0.31.3`（annotated）→ `385aac88752a1fa63b2bc60cc4ec976e48e43a5b`（包版本仍 0.31.0）

**执行**：SSH `lighthouse@82.156.0.68`，2026-09-05 周末窗口分三次：

| 次 | 时间（CST） | 做了什么 | 当次 tag |
|---|---|---|---|
| 一 | 01:14–03:05 | A 段：三个 root 制品、arbiter、5 个 slice、19 个既有 unit | `v0.31.1`（`0fb7d95`） |
| 二 | 06:11–06:50 | B 段：四套凭证、credential 基础设施、权威链首发（sequence 1）；C 段装 unit、建目录 | `v0.31.2`（`a8beb44`） |
| 三 | 11:41–12:25 | 第二代权威链发布（sequence 2）与服务启动 | `v0.31.3`（`385aac8`） |

### 装了什么

**A 段（第一次执行）**：

- 三个 root 制品 `/usr/local/libexec/rquant-{runtime-exec,production-deploy,signal-family-verifier-v1}.pyz`，
  全部 `root:root 555 nlink=1`；签名族校验器的内容寻址树在
  `/usr/local/lib/rquant-signal-family-verifier/<content-id>/`（目录 0555 / 文件 0444 / root:root）。
- workload arbiter `/usr/local/libexec/rquant-workload-arbiter{,.sha256}`（`755` / `444`）
  与 `/etc/tmpfiles.d/rquant-workload-isolation.conf`。
- 5 个 slice（`rquant{,-live,-serving,-research,-maintenance}.slice`）与 19 个既有 unit：
  16 个纯配置改动，`backup` / `replica-sync` / `research-ingest` 三个的 ExecStart 改经 arbiter。
  `rquant-replica-sync.service` 经 arbiter 实测跑完 `Result=success`、`ExecMainStatus=0`，
  只读副本 mtime 已刷新。
- `/etc/sudoers.d/rquant-production-deploy` 换成 2,416 字节新版，旧版 652 字节备份在
  `/root/rquant-sudoers-backup-20260905`；32 个 unit 备份在 `/root/rquant-unit-backup-20260905/`。

**B 段（第二次执行）**：

- 四套 Ed25519 凭证（`lab-highwater` / `canvas-publication` / `shadow-report` / `daily-receipt`）：
  `/etc/rquant` 下 4 个私钥 `0600 root:root`、4 个 `*-trusted-keys.json` `0444 root:root`，
  `verify` 输出 9 行 `OK` 退 0。
- **加密备份**：`/home/lighthouse/rquant-credentials-backup/`（0700 lighthouse），
  密文包 `etc-rquant-20260905-061857.tar.enc`（0600，aes-256-cbc + pbkdf2），
  口令文件同目录 0600。**口令内容不出服务器，也不写进本文件。**
  已做解密往返自检，14 个条目与 `/etc/rquant` 逐条对上。**私钥删了不可恢复。**
- 五个 root helper（0755 root:root）：`runtime-credential-sealer`、`lab-highwater-authority`、
  `canvas-publication-signer`、`shadow-report-signer`、`daily-receipt-signer`；
  `rquant-daily-receipt-signer.socket` `active (listening)`。
- 26 个 protected unit + 3 个新 timer 装好（**一个都没 enable、没 start**）；
  C-1 按 `systemctl show -p ReadWritePaths` 解析出 35 个目录预建完，
  `data/runtime` 下共 62 个目录，`data/runtime/current` 确认不存在。

**权威链两代**：

| 代 | sequence | generation | operation_id | producer commit | `wrapper_preflight` | 事务耗时 |
|---|---|---|---|---|---|---|
| 一 | 1 | `2bb73c28…7ca6` | `12d1eea29db425d7f3f37673b2a59ceb` | `a8beb44` | **32** | 75 s |
| 二 | 2 | `ff79b184417a3b80a888a793ab2e8ed50e3aba212f2470e185d51cb25f6e92e4` | `39cf30b0ee2aa849fb5d69bc9417d2c8` | `385aac8` | **32** | 84 s |

两代 `profile_id` 相同（`d2206e53…7ea0`），发布器走的是「同 profile 换 generation」，
**issue #190 未触发**，没有走回退首发路径。`/etc/rquant/production-runtime-profile.json` 与
`/var/lib/rquant/runtime-authority/current.json` 都是 `root:root 444 nlink=1`，profile 声明 28 个 role。
第二代 `result=committed`、`state=active`、`prior_generation_id` = 第一代。

### 结果

- **第一关最终口径**（runbook R-12）：`wrapper_preflight == 32` **且** serving 平面
  ≥ 2 个 kind-backed 服务持续运行。两条都达成。原判据里的「三个 plane 各至少一个」
  在路线 B 下不可达（见 #204、#205），改动都在 builder 侧，属 owner 决策。
- **持续运行的两个 unit**：`rquant-runtime-runtime-health@svc-2a07f3ca….service` 与
  `rquant-runtime-serving@svc-63af0b41….service`，都在 `rquant-serving.slice`，
  观察到 12:24:34（分别已运行 18 分 20 秒与 17 分 27 秒），**`NRestarts=0`**、`Result=success`，
  journal 里各只有一条预期内的 degraded WARNING，没有 traceback 或 `exit-code`。
  `runtime_health_publisher` 已在 `data/runtime/control/authority-runtime-health/`
  实际产出 4 份 generation、4 份 publication 与 `current.json`。
- **issue #200 的修复在生产主机上确认有效**：28 份 service manifest 的 `plane` 已按角色派生
  （live 18 / serving 2 / research 6 / 两份占位无 plane），`settings` 除两份占位外全部非空。
- **live 与 research 两个平面 0 个服务**。15 个 kind-backed role 逐个用 wrapper 裸跑取证
  （不经 systemd，因而不触发告警），起不来的分类：

  | 分类 | 角色 | issue |
  |---|---|---|
  | 路线 B 的降级态不构造 artifact terminal lifecycle，research 平面结构性起不来 | 裸跑实测撞这一条的是 `lab_jobs_publisher` 与 `promotions_publisher`；`lab_artifact_catalog`、`artifact_retention` 同属这四个 artifact-terminal owner kind | **#204**（BLK-4） |
  | 缺 `data/runtime/authorities/reference-slow/` 的 reference registry，由推迟的 credstore 组产出，目录本身不存在 | `paper_constraint_publisher` | **#205**（BLK-5） |
  | 路线 B 从不写 `current/deployment-profile.json` | `daily_pipeline_orchestrator`、`strategy_live`（后者属设计性失败） | #201 |
  | 依赖明令不装的 `rquant-external-monotonic-root.service` | `lab_claim_finalizer` | #191 |
  | 缺装机阶段本来就不存在的操作员事实：交易日历、封存候选文档、历史分钟快照、路由策略指纹、存放地 / 失败域、签名授权 | 其余 9 个，其中 `paper_broker` 与 `notifier` 缺的是上游角色的产出 | — |

- **全程无退出码 78**（wrapper 一次都没拒绝）。
- #192 未做的那一半仍在：25 个 protected unit 没有 `[Install]` 段，**服务器重启后不会自动启动**。

### 两次事故

1. **02:51–02:57，三个页面 500，约 6 分钟**。A-1 把生产 `.venv` 换成 3.11，
   `mv .venv` 之后三个 Streamlit 页面立刻从 200 变 500
   （`FileNotFoundError: …/streamlit/static/index.html`）——Streamlit 每次 HTTP 请求都按绝对路径
   去 venv 里读前端静态资源，runbook 里「重建期间服务照常」这句不成立。进程本身没有退出
   （`NRestarts` 仍 0）。当场把 A-1 整体回滚，页面恢复 `200/200/200/302`，三个同窗口停掉的 timer
   也全部恢复。周六凌晨，无人访问。**A-1 已移出第一关**（runbook R-1），改到第二关的停服窗口做；
   3.11 的 venv 原地留作 `/home/lighthouse/rquant/.venv.new-3.11`，第二关改名即可用。
2. **06:33 与 06:35，两条告警推送到 owner 手机**。C-3 探路启动
   `rquant-runtime-runtime-health@` 与 `rquant-runtime-feature@` 失败，
   两个 unit 的 `OnFailure=rquant-alert@` 各推了一条 PushDeer「🚨 [RQ] … 失败 — 立即排查」。
   发现后改用 wrapper 裸跑做剩余角色的取证，此后没有再产生任何推送；
   第三次执行全程零推送，只 start 了裸跑证明能起的那 2 个。

### 校验器树被 root 写入 .pyc 后清理换代

第一次执行 A-5 的自检命令漏了 `verify` 子命令，以 root 跑出 8 个 `__pycache__` / 47 个 `.pyc`
（mtime 全为 02:54）写进了内容寻址的校验器树；第二次执行按 `verify` 复检时退 78，
报 `the artifact tree holds an unmanifested node`。处置：清掉污染，按「换代时旧树永不删、只换入口
pyz」装 v0.31.2 重建的新树，content_id `7db9c2a9…af771` → `bb571c69…5e1d`，
入口 pyz `263fd9a3…a3e0b` → `fbd14d60…de50`，旧入口 pyz 备份在
`/root/rquant-verifier-entry-backup-20260905-v0311.pyz`，旧树保留未删。
第三次执行没有再以 root 跑自检，未复发。

### 现役零损伤

七个常驻服务 `NRestarts` 全 0（5 个页面 active，`monitor` / `surge-watch` 周末 inactive，
与开工前逐条相同）；四个端口 `200/200/200/302`；failed unit 仍是基线那 5 个，无新增；
生产 checkout 仍是 `e4e303b`，`git status` 干净；`.env` 未改（644，1,722 字节，
也没拷进临时 worktree）；生产 DuckDB 未写（两个新进程 `/proc/<pid>/fd` 里没有 duckdb 句柄，
`fuser` 无输出）；`backup/` 未动；`data/runtime/current` 不存在；
三个 slice 的 `memory.events` 里 `oom` / `oom_kill` 全 0；磁盘剩 20 GB。

### 回滚（runbook §0.6）

```bash
# ① 停本次启动的两个 unit（模板 unit 没有 enable，stop 即回到未运行）
sudo systemctl stop 'rquant-runtime-runtime-health@*.service' 'rquant-runtime-serving@*.service'

# ② 单级回到第一代（第二代起才有 prior 可回）
sudo /usr/bin/python3.11 -I -S /usr/local/libexec/rquant-production-deploy.pyz \
     rollback --operation-id 39cf30b0ee2aa849fb5d69bc9417d2c8

# ③ 整链停用，回到「wrapper 全拒」的安全态
sudo rm -f /var/lib/rquant/runtime-authority/current.json
# generation 目录是内容寻址的，两代都保留，永不删除
```

A / B / C 三段逐层的完整回滚（unit → arbiter → 制品 → sudoers → 凭证）见 runbook §0.6 与本次执行记录。
**⚠️ 四套私钥删了不可恢复**，回滚前先确认 `/home/lighthouse/rquant-credentials-backup/` 的密文包能解开。
代码本次未切换，无需代码回滚，锚点 `e4e303b`（服务器上 `/home/lighthouse/rollback-code-sha.txt`）。

完整执行记录（三次窗口的每条命令与输出原文）在 Mac 本地的
`/Users/roxor/brain/30-projects/rQuant/.worktrees/release-a-cc/.superpowers/sdd/2026-09-03-release-a-rollout/rollout-exec-report.md`，
没有进仓库。

### 等你决策

1. **第二关**是否开窗口：切生产代码到 Release A、`mv` 换上 3.11 venv、重启七个常驻服务。
2. **#204（BLK-4）**：research 平面要么改降级分支给四个 artifact-terminal owner kind 一个只读
   lifecycle，要么走路线 A 补齐操作员事实。两条都要改代码或产出生产 inputs，本轮未做。
3. **#205（BLK-5）**：live 平面要有服务，先得让 `reference_slow_source` / `reference_slow_publisher`
   产出 reference registry，这两个 role 在推迟的 credstore 组里，属另一批工作。
4. **路线 A** 是否启动：由操作员产出生产 inputs 文档 + 生成一代真实画像，
   这是那 9 个「缺操作员事实」的角色唯一的出路。
5. **#192 的 `[Install]`**（重启自动启动）与 **#191 的 `external-monotonic-root`** 是否装，
   都属 `deploy/systemd/` 改动，需要单独授权。

---

## v0.31.0 Release A 上线目标（尚未部署）

**状态**：等你操作。本节写在装机之前，**不是部署记录**；真正部署完成后按本文件既有格式在它上面
追加一条 `## YYYY-MM-DD · v0.31.0 · 标题`。下一节（v0.30.0）是 Release A 的前置条件清单，逐条仍然
有效，只是 tag 号从 v0.30.0 顺延到了 v0.31.0——v0.30.0 已打 tag 但从未部署，最后一次真实部署是
2026-08-04 的 v0.28.3。

### tag 目标

`v0.31.0` 指向定版 PR（`chore(release): cut v0.31.0 (Release A toolchain)`）**合入 main 之后产生的
那个 merge commit**，不是定版分支上的任何 commit，也不是 main 的浮动 HEAD。tag 由协调者在 CI 全绿、
PR 合入之后创建。部署一律走：

```bash
bash scripts/deploy-production.sh --target v0.31.0     # 或该 merge commit 的完整 SHA
```

合版方式只能是 "Create a merge commit"，理由见下一节第 1 条（R07 证据的 merge-provenance 检查要求
候选 commit 恰有两个 parent；squash / rebase 会让这个 commit 永久失去部署资格）。

Release A 工具链本体是 PR #194，已于合入 main 时产生 merge commit
`9a42bdee5aa1e7be39329ea720d3801cd269540f`；定版 PR 的 R07 baseline 就冻结在它上面。

### 执行材料（不在仓库里）

- **runbook**：本机 Mac 上的
  `/Users/roxor/brain/30-projects/rQuant/.worktrees/release-a-cc/.superpowers/sdd/2026-09-03-release-a-rollout/release-a-runbook-v2.md`
  （v1 `release-a-runbook.md` 已被它取代，不要再用）。
- **给 owner 的一页摘要**：同目录 `go-no-go.md`。
- 两份都在 Mac 本地的 worktree 里，**没有进仓库**；服务器上不存在这两个文件。

### 第一关判据

- **真判据**：root 权威链就位——`/etc/rquant/production-runtime-profile.json` 与
  `/var/lib/rquant/runtime-authority/current.json` 都是 `root:root 0444`，profile 声明 28 个 role，
  发布回执里 **`wrapper_preflight == 32`**（wrapper 对全部 32 个「角色 × 实例」组合逐一预检通过）。
  Release A 的目标就是这一条，不是「26 个服务全绿」。
- **服务状态口径**（协调者 2026-09-04 已接受的应急口径，起因是 #191）：
  **13 个持续运行**（带降级警告，因为旧目录结构还不存在，这是设计）
  + **1 个 oneshot 跑完退 0**
  + ~~**`rquant-runtime-strategy@` failed（设计，协调者已裁定）**~~ **这条口径作废（#218 A、#232）**：
  那次 failed 的真实原因是 completion signer 把已冻结的 profile manifest 又验了一遍，报
  `strategy completion signer profile contains invalid manifests`，是代码缺陷不是设计。
  修好之后（再加上 #231/#232 那一包）`rquant-runtime-strategy@` **必须是 active**：它一启动就
  建自己的 `runner.sqlite3`、进服务循环，**不再需要「先起一轮失败」**（前置第 28 条），
  判据里它算**持续运行**。
  + **`rquant-lab-claim-finalizer` 受阻**（缺仓库外的 `/etc/rquant` 输入，journal 追加到 #191，
  留到影子窗口前修）
  + **10 个未启用**（7 个等 credstore 密钥、3 个硬依赖旧目录结构；预期状态是「未启用」，不是失败）。
- 七个常驻服务与四个端口与上线前基线逐项一致。
- **#200 修完之后，验收判据必须往前走一步：在生产机上真的 `systemctl start` 一个 kind-backed
  角色并看它持续运行，不能只看 `wrapper_preflight == 32`。** `wrapper_preflight` 只证明 wrapper
  对 32 个「角色 × 实例」组合逐一预检通过，它**看不到 service manifest 的内容对不对**——#200 就是
  这样溜过去的：manifest 里 `plane` 全写 `live`、`settings` 全是空对象，preflight 一路绿，
  角色一启动就被自己的构造器拒收。#200 修完后可用来做这条判据的角色有四个：
  `paper_constraint_publisher`、`runtime_health_publisher`、`serving_publisher`、`lab_jobs_publisher`
  （分属 live / serving / research 三个平面）。`daily_pipeline_orchestrator` 与 `strategy_live`
  这一轮仍然起不来，原因是路线 B 不产出 `<运行根>/current/deployment-profile.json`（issue #201），
  **不要拿它们当判据**。
- **明确不承诺**：26 个服务全绿、页面有数据。

### B-6' 的新前置检查（#200 引入，装机前逐条确认）

1. **顺序**：B-6'（`rquant runtime-authority-stage`）现在必须排在 **B-2 → B-3 之后**。stage 会读
   `/etc/rquant/daily-receipt-trusted-keys.json`，钥匙串不在就明确拒绝并整体失败（不会静默产出空授权）。
   现有 runbook 顺序本来就满足，这里只是把这条以前不存在的依赖写明。
2. **钥匙串本身**：`stat -c '%U:%G %a %h' /etc/rquant/daily-receipt-trusted-keys.json` 必须是
   `root:root 444 1`，且 `/etc/rquant`、`/etc`、`/` 每一级都是 root 属主、无 group/other 写位。
   打包侧用的就是生产画像那一个严格加载器，判据一分不宽：**mode 必须恰好 0444**，0440 也会被拒。
3. **openssl**：`openssl version` 需 **3.0 以上**，并且 `openssl pkeyutl -help 2>&1 | grep rawin`
   要有输出。stage 的 Ed25519 验签是 shell 出去跑 `openssl pkeyutl -verify -pubin -rawin`，
   查找顺序 `/opt/homebrew/bin/openssl` → `/usr/bin/openssl` → PATH。**缺 openssl 或版本太老时
   报出来的是 `signature is invalid`**，那是一句会把排查方向带偏的错，先确认这两条能省掉半天。
   （OpenCloudOS 9.2 自带 3.x，B-2 的凭证备份本来也在用 openssl，现场预期满足。）

### 路线 A 前置（生产 inputs 与真实画像，本轮 PR 引入，开工前逐条确认）

路线 A 就是「由操作员产出生产 inputs 文档 + 生成一代真实画像」这条路，也是那 9 个「缺操作员
事实」的角色唯一的出路（见本节末「等你决策」第 4 条）。本轮 PR 补齐了它缺的两个生产者
（`scripts/build_runtime_production_inputs.py` 与 `scripts/export_intraday_snapshot.py`）
和凭证侧的增量装法；另一个 PR（#207）修好了「权威 generation 与 legacy generation 是两个命名空间」
这个结构性阻塞，第 13 条起的六条就是它带来的新前置；第 19 条来自 #213，第 20 条起的八条是
2026-09-07 第一次真正跑完路线 A 之后的实战订正（runbook R-13…R-18），第 1、5、12 三条也按当时
的实测就地订正过；第 29 条来自 #218 的修复包（recovery 凭证的生成器），第 28 条原本也是
（#220 的启动顺序），2026-09-08 已被 #231/#232/#220 的修复包整条改写；第 30、31 两条来自
#227 的第二包（安装器代做的 PREPARE 承认与十六个 unit 的 rollout 写权限）；第 32 条来自 #230，
也就是 #215 的第三处断点（凭证的投递形状）；第 33 条来自 #237，也就是「v0.33.2 装不上第三代」
这件事本身。
下面三十三条是照着脚本敲命令时会踩到的东西，**不是部署记录**。

1. **市场日历的到期日与续期步骤**：生成器的 `--calendar-coverage-floor` 默认 `2027-12-31`，日历表
   覆盖不到这个下限就报错退出。跑完把实际的 `coverage_end` 与 `open_dates` 条数**记在本条下面**。
   续期的做法是：扩 `trade_calendar` 表 → 重跑生成器 → 重跑命令链 ①②③④。这是**换一代
   generation，不换 `profile_id`**。

   **首次装机显式传 `--calendar-coverage-floor 2026-12-31`**（#211）：生产库的 `trade_calendar`
   目前只到 `2026-12-31`，补 2027 年的日历要往生产库写数据，属于需要 owner 单独授权的高风险
   变更，所以首次装机改为显式下调这个下限，先把系统跑起来。下调只是放宽，不是取消——日历
   比传入值还短照样报错退出；成功时生成器会在 stderr 打一条 WARNING，并在 stdout 摘要里多一行
   `coverage_floor_override`，两处都写明必须在 `2026-12-01`（`coverage_end` 前 30 天）之前完成
   续期。续期步骤就是本条上面那一段，续期后**去掉这个参数**，让下限回到默认的 `2027-12-31`。

   **2026-09-07 首次装机实测**：`coverage` 2020-01-01..2026-12-31，也就是 `coverage_end` =
   `2026-12-31`；`open_dates` **1697** 条；日历文档 `content_sha256 eceaa714…083f`，
   `generated_at 2026-07-14T10:13:02Z`。摘要里的那行是
   `coverage_floor_override floor=2026-12-31 default=2027-12-31 renew_by=2026-12-01`。
   **续期截止 2026-12-01。**
2. **`/usr/local/libexec/rquant-runtime-credential-sealer` 必须随本次 tag 重装**（#208：旧版白名单
   只覆盖七种凭证种类里的两种，第一次真密封会整体中止）。重装前把旧版备份到
   `/root/rquant-helper-backup-<stamp>/`。
3. **凭证补装一律用增量模式，绝不裸 `init`**（主机 `/etc/rquant` 已有 B-2 装的四套，裸 `init` 会因
   5 个已存在文件整体退 3、一个字节不写）：

   ```bash
   sudo bash scripts/install-runtime-credential-keys.sh init --only-missing --dry-run   # 应只报 completion,capabilities
   sudo bash scripts/install-runtime-credential-keys.sh init --only-missing
   sudo bash scripts/install-runtime-credential-keys.sh verify                          # 13 行 OK
   ```

   **`--key-suffix` 必须等于主机上已装的那个**。「这一组装没装」是按组内文件在不在判的，而私钥
   文件名带 suffix、清单文件名不带，所以传错 suffix 会把四个装好的组全判成 `half installed` 并退 3
   ——那句报错指向一个不存在的文件，方向是错的。主机上 B-2 装的是 **`v1`**，也正是脚本默认值，
   **不传就是对的**；若某一套已经轮换过，先 `ls /etc/rquant/*/` 或读
   `/etc/rquant/<kind>-keys.json` 的 `active_private_key_path` 确认后显式传那个值。
4. **随后发布 completion 公钥环**：
   `sudo bash scripts/install-runtime-credential-infra.sh --only-missing-keyrings`。它只补还没发布的
   公钥环（本轮就 `shadow-completion-trusted-keys.json` 一份，`root:root 0444`），**不碰 helper、
   unit 与 sudoers**，因此不会重启 Daily authority。这一步用到的 `sudo /usr/bin/python3` /
   `/usr/bin/install` / `/bin/mv` **都不在** `deploy/sudoers/rquant-production-deploy` 的六个白名单
   别名里，所以**需要一个交互式 sudo 会话**，`sudo -n` 跑不了。第 3、4 步跑完立刻做离线加密备份。
5. **六个能力凭证的注入**：
   `eval "$(sudo … export-capabilities)" && rquant runtime-deployment-profile …`。这六个
   `RQ_*` 变量只在那条命令的进程环境里存在，**不落 env 文件**；执行前先 `set +o history`。
   轮换过凭证之后必须重跑命令链第 ④ 步。
   ⚠️ **`eval "$(...)"` 这个写法已被证伪（#214），改用下面第 22 条的逐行 `export`**；
   而且要注入的不止这六个变量，见第 23 条。
6. **分钟快照在云端只读副本上导出，`--ts-code-file` 是事实必需项**：导出器写盘前会用 `feature_live`
   自己的入场校验判一遍，而 A 股任何一个 20 交易日窗口里都必定有停牌标的、它们的分钟线在
   `minute_bar` 里是零价，**所以不带 universe 的全市场导出几乎一定退 2**（`OHLC prices must be
   strictly positive`）。正确做法是只导出实际订阅的标的；**「选哪些代码」是操作员决定，要写进
   runbook**。退 2 时脚本会打印第一条坏行，按它把对应标的剔掉或换一个窗口再跑。顺带的好处是体量：
   全市场 20 个交易日约 380 万行，而 `feature_live` 每次启动都会整份读进内存。
7. **封存候选与 `producer_commit` 绑定**：换代码就要重新产出那两份候选文档并重跑生成器。
8. **路由策略默认三条策略全放行到 admin 的 PushDeer**（裁决 7）。本轮封存候选是合法空清单，
   不会真推；**换成真候选之前，owner 需要确认是否收窄**。
9. **retention 的「不自动删除」是靠一条永不命中的哨兵 binding 实现的**（裁决 5）：真去 verify 某个
   artifact 会失败关闭，不是静默保留。
10. **回滚**：按 operation id 回到 sequence 2、把 `current.cred` 切回、删掉
    `data/runtime/current`。
11. **第 ⑥ 步之前先查软链**：`namei -l /home/lighthouse/rquant/data/runtime` 与
    `namei -l /home/lighthouse/rquant/data/runtime-production-inputs.json`。`_absolute_runtime_root`
    拒绝任何软链祖先，而**「这条路径上没有软链」这件事第一次被真正检验就在主机上**——本地测不出来，
    macOS 的 `/home` 本身就是 autofs 软链。
12. `/var/lib/systemd/credential.secret` 由第一次 `systemd-creds encrypt` 自动创建。
    第一次密封之后 `sudo stat` 确认一次并把结果记下来。
    **2026-09-07 首次密封实测是 `root:root 0400`，不是本条原先写的 0600**（systemd 自建的就是
    0400），按 0600 去核对会误判成异常。
13. **换 legacy 代必须重新 stage + publish 权威链**（#207 之后的硬约束）。只把
    `/home/lighthouse/rquant/data/runtime/current` 切到新的一代 legacy generation、不换权威 generation，
    全部 kind-backed 角色拒绝启动，原文
    `ValueError: runtime legacy generation binding does not match the current pointer`。
    这是刻意的：否则 `current` 一被挪动，角色就会拿一份和自己 manifest 无关的 bundle 去装 schema bindings。
14. **stage 时 `--legacy-runtime-root` 必须写字面量 `/home/lighthouse/rquant/data/runtime`**——绝对路径、
    路径上无软链分量、无尾斜杠。`legacy-binding.json` 记的是 `abspath` 之后的字符串，角色启动时比的
    也是 `abspath`，两边不一致就报 `... names another runtime root`。
15. **dry-run 与 apply 之后的核对点**：`plan.json` 的 `staged_files["generation/legacy-binding.json"]`
    要有摘要；apply 之后 `cat <staging>/generation/legacy-binding.json`，其中 `generation_id` 必须等于
    `readlink /home/lighthouse/rquant/data/runtime/current` 的目标，`runtime_root` 必须等于第 14 条那个
    字面量；再确认这份文档出现在 `full-manifest.json` 的 `entries` 里，`mode` 是 `292`（八进制 `0444`）。
16. **回滚含义**：路线 A 回退（删掉 `data/runtime/current`）之后，同一代权威 generation 的角色会自动
    回到降级分支（路线 B），**不需要**再换 generation。
17. **包 C 的两处顺序修正**（覆盖 runbook 原来的第 2、10 步）：
    - 第 2 步「换 `rquant-runtime-exec.pyz`」**不需要做**：它与 `rquant-production-deploy.pyz` 在 #207
      的改动前后逐字节相同（`a5d9b3ff…9c5e` / `a41db437…6757`），这一包没碰它们的输入。上面第 2 条
      那个密封 helper（#208）的重装仍然要做。
    - **两个 serving unit 要在第 8 步（`deployment-profile --apply`，写出 `current`）之前停**，不是第
      10 步之前。第 8 步之后它们一旦重启，跑旧代码就会以 `not current` 失败；第 10 步 publish 之后，
      若这一代不是用带 #207 改动的代码 stage 的，则会以
      `runtime legacy generation binding is unavailable or contains a symlink` 失败。
    - 第 11 步「任何 `not current` 出现即停下回报」继续有效，且判据现在更精确：`not current` = 角色跑的
      是旧代码；`binding is unavailable` = 这一代是旧代码 stage 的；`does not match the current pointer`
      = 指针与这一代对不上，该重新 stage。
    - **缺文档的报错措辞**：`runtime generation <64hex> carries no legacy-binding.json: it was staged
      before that document existed, so stage and publish this generation again with the current code`。
      它**只在文件真不存在时**出现；软链（哪怕是悬空的）或权限问题仍旧报
      `... is unavailable or contains a symlink`，不要拿后一句去找一份并不存在的软链。
18. **R07 与合版方式**：路线 A 这两个 PR（包 B 已合入为 `2238d9e`，包 A 即 #207 这一条）各自把 R07
    baseline 重冻结到自己合并时 `origin/main` 的 tip，重冻结是各自分支的最后一个 commit；
    **两个都只能用 "Create a merge commit" 合**
    ——R07 证据的 merge-provenance 检查要求候选恰有两个 parent，squash 与 rebase 拿不到部署证据。
    因此部署要取的 tag 指向的是合并后的那个 merge commit，不是分支 tip。
19. **`runtime-production-prerequisites` / `runtime-production-profile` / `runtime-deployment-profile`
    / `runtime-schema-rollout` 可以直接在没有 `.env` 的 bootstrap worktree
    （`/home/lighthouse/rquant-relA`）里跑**（#211，BLK-8；第四条来自 #227 的第二包）。
    这四条命令跟 `runtime-authority-stage` 一样，在 `main()` 构造 `Settings` 之前就被分发，
    命令自己也不读任何配置。**本条只对含这一改动的版本成立**：在此之前的版本里，同样的命令会以
    `ValidationError: 5 validation errors for Settings` 退出，当时的绕法是在命令前面临时导出五个
    环境变量（`DATA_DIR` / `DUCKDB_PATH` / `PARQUET_DIR` / `LOG_DIR` / `TUSHARE_TOKEN_MAIN`）；
    现在**不要再导**，这些变量指向的是生产库路径，在 bootstrap worktree 里给它们赋值只会误导。
    其余命令（含 `rquant --help`）在这个 worktree 里照旧 fail-closed，那是设计（T9-9）。
20. **C-1 预建的 `data/runtime/research/**` 必须全部 0700**（R-13，把 runbook R-7 里只针对
    `data/runtime/research` 那一层的要求扩到整棵子树）。`runtime-production-prerequisites --apply`
    的 retention catalog 那一步要求 state root（`research/artifact-retention/<svc>/`）是 0700 的
    属主目录，按默认 umask 建成 0755 会退 1，报
    `retention state root must be an owned directory with mode 0700`。补救：

    ```bash
    find /home/lighthouse/rquant/data/runtime/research -type d ! -perm 700 -exec chmod 700 {} +
    ```

    首次装机命中 13 个目录。C-1 每一条预建目录都要显式给 mode，不要依赖 umask，
    建完 `stat -c '%n %a'` 逐条复核。
21. **换代之前先把停掉实例的旧心跳移走**（R-14，#216）。停掉旧代实例之后
    `control/<role>/<svc>/heartbeats/<identity>.json` 还在，里面的 `spec_fingerprint` 属于旧 spec；
    新代同一角色启动会报 `runtime heartbeat does not match the requested service spec`。
    做法：确认该实例确已停（心跳文档里 `pid` 为 `None`、`stopped_at` 有值）之后，把这个文件移走
    再启动。首次装机在 `runtime_health_publisher` 与 `serving_publisher` 上各命中一次。
    **#216 修好之前这一步得手工做**，修法应当是发布链路自己作废旧代心跳。
22. **六个 `RQ_*` 能力变量要逐行 `export`，不能 `eval`**（R-18，#214，**取代第 5 条的写法**）。
    `export-capabilities` 的输出不是 eval-safe——公钥里含空格，`eval "$(...)"` 会当场炸。改用：

    ```bash
    while IFS= read -r line; do export "$line"; done \
      < <(sudo bash scripts/install-runtime-credential-keys.sh export-capabilities)
    ```

    导完先确认六个都在，再往下走。
23. **`deployment-profile` 的 dry-run 也要完整的 capability environment**（R-18），不是只有
    `--apply` 才要。除第 22 条那六个 `RQ_*` 之外，`CAPABILITY_KEYS` 还含 `TUSHARE_TOKEN_MAIN` /
    `TUSHARE_TOKEN_BACKUP` / `PUSHDEER_*` / `PUSHPLUS_*`，这些的来源只有生产 `.env`
    （设计上 deployer 就是带 `.env` 跑、把它们密封进 credstore），缺一个就报
    `runtime capability environment <NAME> is missing`。注入方式
    `set -a; . /home/lighthouse/rquant/.env; set +a`。**读生产 `.env` 需要 owner 单独授权**——
    首次装机是 owner 当场点头才做的，不要默认自己可以读，也不要把 `.env` 拷进 bootstrap worktree。
    另外**首代必须传 `--schema-bootstrap-reason`**（审计理由），不传连 dry-run 都过不去。
24. **stage 在 legacy 模式的参数与耗时**（R-18）：`--legacy-runtime-root` 写字面量
    `/home/lighthouse/rquant/data/runtime`（第 14 条），`--legacy-generation current`。
    首次装机 stage apply 约 **17 s**，随后的正式 publish 约 **96 s**（dry-run 不计，它在取部署锁
    之前就返回）。
25. **credstore 组的实际状态：密封 7 个实例，逐个 start 过的 6 个 role 一个都没起住**（R-15，#215）。
    `deployment-profile --apply` 会把 7 个实例密封进
    `/etc/credstore.encrypted/rquant-runtime/instances/<svc>/`。逐个 `systemctl start` 的结果：
    `reference_slow_source` 与 `notifier` 各自首次失败、重启一次后曾短暂 running，但到窗口收尾
    复核时两个都已 failed；`reference_slow_publisher`、`daily_close_source`、`market_minute_source`、
    `auction_match_source` 从头就起不来。判据记 **0/7**。起不来的立刻
    `systemctl stop` + `reset-failed` 防重启风暴——`notifier` 的 `OnFailure=rquant-alert@` 会推送。
    **没有 `reference_slow_publisher` 就没有 serving generation，第二关（包 D）不能排。**
26. **`rquant-runtime-lab-jobs@` 要先建一个空目录**（R-16，#191）。它的
    `InaccessiblePaths=/etc/rquant/lab-claim-finalizer-runtime` 没有 `-` 前缀，路径不存在就在挂载
    命名空间阶段 `226/NAMESPACE`。按 go-no-go 的绕过法：

    ```bash
    sudo install -d -o root -g root -m 755 /etc/rquant/lab-claim-finalizer-runtime
    ```

    建完能起，但紧接着会被第 27 条那道门挡住。
27. **research 平面被高水位证据门挡住**（R-17，#217）。`lab_artifact_catalog` /
    `promotions_publisher` / `shadow_session` / `lab_jobs_publisher` 启动即打
    `FAIL research blocked: high-water evidence unavailable or invalid:
    /var/lib/rquant/workload-isolation/high-water.json`，然后**退 0**（`Result=success`）。
    这是 workload arbiter 的资源门，不是崩溃、也不是 bug；谁在什么时候产出这份文件，
    仓库里还没有答案。判据里 research 平面按「已启动、被门挡住」记，**不计入持续运行数**。
28. **C-3 的 live 平面按固定顺序起，全部软依赖；「先起 strategy 一轮失败」这一步作废**
    （#231、#232、#220，2026-09-08 第三窗口的修复包）。第三窗口的实况是四个 role 一个都没起来：
    三个 `strategy_live` 报 `OSError: [Errno 30] Read-only file system:
    <ROOT>/live/features/.feature-spool.lock`（#231，消费者用写模式开 feature spool，锁在生产者
    目录里，而 strategy unit 的 `ReadWritePaths` 只有 `live/strategies/%i`），`signal_router`
    报五次 `runner source is unavailable: .../runner.sqlite3`（#232），`paper_broker` 与
    `notifier` 等 router 的 spool（#220）。每一次退出都触发一条 `OnFailure=rquant-alert@`。
    现在消费者只读打开 spool、游标放自己目录；`strategy_live` **一启动就建**
    `runner.sqlite3`，`signal_router` **一启动就建** bus 与 spool，缺对端制品的一方在主循环里
    等而不是退出。**两个方向都能起，顺序不再是硬约束**；下面这条顺序只是为了让每个 role
    一上来就是 RUNNING 而不是 DEGRADED。

    ```bash
    ROOT=/home/lighthouse/rquant/data/runtime
    ```

    ```
    ① rquant-runtime-runtime-health      （探路，不变）
    ② rquant-runtime-serving             （不变）
    ③ rquant-runtime-feature             ← 软前置：只有它会写 live/features/source-identity.json；
                                            先起它只是让 strategy 一上来就是 RUNNING 而不是 DEGRADED
       探针 0： test -f $ROOT/live/features/source-identity.json
    ④ rquant-runtime-strategy@ × 3
       探针 1： [ "$(ls -1 $ROOT/live/strategies/*/runner.sqlite3 2>/dev/null | wc -l)" -eq 3 ]
    ⑤ rquant-runtime-signal-router@
       探针 2： test -f $ROOT/live/signal-bus/spool/source.json
    ⑥ rquant-runtime-paper-broker@ → rquant-runtime-notifier@
    ⑦ 其余 unit（顺序无关）
    ```

    三条探针的产物在 `tests/integration/test_route_a_live_chain_idle_e2e.py` 里都是被断言过的
    真实文件，路径与生产画像 `runtime_production_profile.py` 一致，不是从代码推出来的。

    **探针要当判据用，不是提示。** 服务循环没有连续失败阈值——这正是本次要的，一个还没启动的
    对端不再让进程退出、不再触发 `OnFailure`——**代价是把「告警风暴」换成了「静默」**：等不到
    对端的 role 在面板上只是 DEGRADED，没有任何东西会主动找人。所以心跳多了
    `waiting_for` / `waiting_since` / `waited_seconds` 三个字段，每条探针都有对应的判据：

    ```bash
    CONTROL=$ROOT/control
    # 某个 role 在等谁、等了多久。心跳是 role 自己按
    # <control-root>/heartbeats/<sha256({"service_id":…})>.json 写的 0600 文件，一个 role 一份，
    # 所以按目录通配即可
    jq -r '[.service_id, .status, .waiting_for // "-", .waited_seconds // 0] | @tsv' \
      "$CONTROL"/strategies/*/heartbeats/*.json \
      "$CONTROL"/signal-routers/*/heartbeats/*.json \
      "$CONTROL"/paper-brokers/*/heartbeats/*.json \
      "$CONTROL"/notifiers/*/heartbeats/*.json
    ```

    - **放行判据**：本步涉及的 role 都是 `waiting_for == null`（即 RUNNING，或者 DEGRADED 但原因
      不是等对端）。
    - **卡住判据**：`status == "degraded"` **且** `waiting_for != null`，且在 `waiting_for` 不变的
      前提下 `waited_seconds` 持续增长——说明**那一份文件的拥有者没起来**，按文件名回到它的 unit，
      而不是重启正在等的这一个。**两个条件都要看**：崩溃停机的记录里 `waiting_for` 也会留着
      （`last_error` 描述的是那次崩溃），只看 `waiting_for` 会把人指向错误的文件。
    - 三个字段同组发布、换一份制品就重新计时、任何一次成功迭代清零，所以 `waited_seconds`
      回答的是「在这份文件上卡了多久」。

    **本轮不加阈值、不加告警规则**：「同一份对端制品等待超过 N 分钟要不要告警」是 **issue #235**，
    由它单独决定，那是 runbook 与告警面的事。

    **开窗前先看一眼旧游标**：消费者游标从 `live/features/cursors/` 搬到了
    `live/strategies/<svc>/feature-cursors/`，旧位置不会再被读到。

    ```bash
    ls -A "$ROOT"/live/features/cursors    # 期望：空
    ```

    丢游标本身不危险（从 sequence -1 重放，`runner.replay_source_batch` 会把已处理的批次认成重放、
    不重复发信号），但第三窗口三个 strategy 都死在构造期，理论上不该留下任何游标；非空说明更早的
    窗口里真的消费过，把那几份游标的语义交代清楚再往下走。

    **两个预期之内的 DEGRADED**，都在主循环里、都不是 #220 复发，不要当成回归：

    - `paper_broker`：在 `paper_constraint_publisher` 发布出 `authorities/paper-execution` 的
      current 指针之前，每一轮 `PaperExecutionConstraintUnavailableError: current pointer is
      unavailable`。**进程是活的**。整晚都不发布的话那是另一条要开的 issue。
    - `notifier`：`last_error` 是操作库路径的
      `FileNotFoundError: [Errno 2] No such file or directory: '…/rquant_ro.duckdb'`。
      主机上这一条取决于 serving 面自身的状态。


29. **bundle 装完、`current` 指向本代之后，在主机上生成 recovery 的两份文档**（#218 C）。
    `data/recovery/runtime-recovery.json` 与 `runtime-recovery-backup.json` 由已安装画像指定路径，
    但在此之前**仓库里没有任何脚本、CLI 或文档产出过它们**，两个 recovery oneshot 因此一直缺输入。
    前置是目录本身：

    ```bash
    sudo install -d -m 0700 -o lighthouse -g lighthouse /home/lighthouse/rquant/data/recovery
    ```

    然后跑生成器（replay 窗口是唯一要人判断的输入，**必须落在已发布生产数据集真实覆盖的范围内**）：

    ```bash
    /home/lighthouse/rquant/.venv/bin/python scripts/provision_runtime_recovery_credentials.py \
      --runtime-root /home/lighthouse/rquant/data/runtime \
      --replay-start-date <YYYY-MM-DD> --replay-end-date <YYYY-MM-DD> \
      --only-missing
    ```

    - **首次落 `runtime-recovery.json` 属新增生产密钥材料，需 owner 单独明确授权**
      （受控自动发布模式第 7 条），**不能走无人值守发布器**。HMAC 密钥由脚本现场生成，
      从不打印，也没有任何传入密钥的参数；两份文档都以 0600 经暂存改名原子落盘。
    - `--only-missing` 的语义是「已经有就保留」：已存在且是 0600 的普通文件原样不动。
      **已存在但权限被放宽（例如被 `chmod 0644`）或不是普通文件时，脚本报错退出，不覆盖**——
      静默换掉密钥会让 publication root 里已签的每一份 receipt 与 pointer 全部验不过。
      报错里带路径、实测 mode、期望 mode 和该敲的 `chmod 0600 <path>`。
      **不要为了绕过报错去掉 `--only-missing`**：不带这个参数就是明确要求重新生成，会真的换密钥。
    - 详细操作说明见 `docs/operations/runtime-recovery-credentials.md`。

30. **bundle 装完、`current` 指向本代之后、起 unit 之前，跑一次 schema rollout 的 acknowledge**
    （#227，owner 2026-09-07 授权）。装一代有前代的 bundle 会为每个「声明指纹变了」的 channel
    备一份 rollout 计划——生产画像上是**十六份**，每份都停在 PREPARE 等它的全部生产者各记一条
    承认。这一步不做的后果不是「慢一点」：两份计划各带三个生产者
    （`runtime.strategy_candidate.snapshot` 与 `runtime.strategy_signal.envelope`），头两个实例
    启动时必然各以 `schema producer startup is waiting for every producer PREPARE ACK` 失败，
    每次失败都会中继一条 `rquant-alert@` 告警。

    **命令做两件事，顺序固定**：

    - **先转换**：把 `control/schema-rollouts` 下**每一份** `state.sqlite3` 以写者身份打开一次，
      库头的 `journal_mode` 就从 WAL 变回回滚日志。**每一份，不分代**——生产上现存那十六份是
      v0.33.0 的写者留下的 WAL，而 `load_runtime_schema_service_bindings` 是**先打开每份计划的库、
      再判断是不是本代**，所以只要有一份旧代的 WAL 库留着，每个 kind-backed role 都会被它挡住，
      形状与 #227 一模一样。转换只改库头，不动阶段、不往哈希链上写任何东西。
    - **再承认**：对「目标是本代、阶段仍是 PREPARE」的计划，代每个生产者记一条 PREPARE 承认，
      然后推进到 **DUAL_WRITE 为止**（离开 DUAL_WRITE 要生产者真写过的双写一致性证据，
      CUTOVER 要可信消费者的回执，安装器都代签不了）。

    **位置：紧跟第 ④ 步 `runtime-deployment-profile`，在第 ⑤ 步 stage 之前。** 依据三条：

    - 它的全部前提就是「计划已落盘」加「`data/runtime/current` 指向计划的目标代」，两者都是
      第 ④ 步的产物；从第 ④ 步到起 unit 之间没有任何一步会动 `data/runtime/current`
      （publish 换的是 `/var/lib/rquant/runtime-authority/current.json`，是另一个文件）。
    - stage 只读 `<legacy root>/generations/<代>/manifests/*.json` 与 `current`，**不读也不写**
      `control/schema-rollouts`；所以这一步既动不了 stage/publish，stage/publish 也动不了它。
    - 把转换放在 96 s 的 root publish **之前**，是为了让「库能不能打开、能不能转换」这个问题
      在一条便宜的本地命令里得到答案，而不是在 root 事务跑完之后。

    ```bash
    cd "${WT}"                       # 无 .env 的 bootstrap worktree，本命令免配置（第 19 条）
    ./.venv/bin/rquant runtime-schema-rollout acknowledge \
      --runtime-root /home/lighthouse/rquant/data/runtime --dry-run
    ```

    dry-run **一个字节都不写，也不转换**（只读打开）。**生产上十六份现在是 WAL，只读打开读不了
    WAL 库，所以第一次 dry-run 会把它们全报成 `journal_mode_before: wal` +
    `skipped_reason: state_unreadable` + `phase_before: null`——这是预期形状，不是故障**：
    没有任何进程能在不往旁边建 wal-index 的前提下读 WAL 库。dry-run 此时能确认的是
    `plans` 等于 16、每份的 `target_generation_id` 是本代。

    ```bash
    ./.venv/bin/rquant runtime-schema-rollout acknowledge \
      --runtime-root /home/lighthouse/rquant/data/runtime
    ```

    apply 之后逐条核对输出：`converted` 是本次真正转过的份数；每份
    `journal_mode_after` 是 `rollback`、`phase_after` 是 `dual_write`；
    `control/schema-rollouts` 下没有残留 `state.sqlite3-wal` / `-shm`。
    **命令幂等**：再跑一次 `changed` 是 0，每份 `skipped_reason` 写 `past_prepare`。
    转换完之后**再跑一次 dry-run**，这次就能读出真实阶段了。

    **关于 deadline（必读）**：计划的 `deadline` 是 `started_at + schema_rollout_stage_timeout_seconds`，
    生产画像默认 **600 秒**。第 ④ 步到这一步之间超过十分钟是常态，所以：

    - 命令**在动任何东西之前**逐份判 deadline，dry-run 与 apply 判定完全一致；
    - 对「目标是本代、阶段是 PREPARE、已过期」的计划，安装器**重开一次窗口**
      （`now` 加上计划自己的那 600 秒），这条重开会作为 `deadline_reopen` 事件记进计划的哈希链，
      `operation_id` 是 `installer-deadline-reopen:<plan>`，输出里 `deadline_reopened: true`。
      **每份计划只有一次**；已越过 PREPARE 的计划一律不动 deadline；**窗口还没关的计划不许提前
      重开**（会白白花掉那一次），签名前缀不对也拒——这三条都由状态库自己守，命令绕不过去。
    - **重开一次之后，这份计划后续每个阶段的窗口也同步后移一个窗口长度**：
      `_validate_time` 管着这份计划**此后所有**的变更，所以 DUAL_WRITE 阶段生产者写双写记录、
      CONSUMER_ACK 阶段消费者写回执，用的都是重开之后的那个 deadline。换句话说重开是
      **把整份计划的时钟往后拨一个窗口**，不是只给承认这一步开口子。
      实务含义：起 unit、跑双写、收回执这几步的时间预算，从重开那一刻起重新计时 600 秒；
      超了就不是重开能解决的了（额度已用尽），要人工裁决。
    - 重开额度用尽还过期的计划报 `skipped_reason: deadline_expired`，**报告照样打完整、其余计划
      照样推进**，命令**退 2**。这时需要人工裁决（重新 `prepare` 是另一次生产写入，要 owner 单独授权）。

    - 这一步是**生产数据库写入**（往计划的哈希链上追加事件），按受控自动发布模式第 7 条
      需要 owner 单独明确授权，不走无人值守发布器。
    - 若窗口在这一步之后失败并把 `data/runtime/current` 回退到上一代：计划停在 DUAL_WRITE，
      但不再是当前代，之后任何一次 acknowledge 都会跳过它们（`skipped_reason: not_current_generation`），
      不会被误当成本代的进度；它们的库仍然会被转换，这正是要的。
    - 第 28 条那条启动顺序仍然照走。acknowledge 只消掉「等其他生产者承认」这一类失败；
      `strategy_live` ↔ `signal_router` 那个互等已经由 #231/#232/#220 那一包在代码里拆掉，
      两边现在都在主循环里等对端制品，不再退出。
    - **#228 仍然在**：只要 `changed_runtime_schema_channels` 的指纹里带 `producer_commit`，
      今后每一次纯代码发布都会凭空生出十六份计划，acknowledge 就得每次都跑一遍，
      `control/schema-rollouts` 下的目录数每发一版加十六（没有任何代码清理旧计划目录）。

31. **十六个 runtime unit 文件必须按 A-7 的做法重装一次，否则第 30 条做完 unit 还是写不了**
    （#227，owner 2026-09-07 授权 A）。第 30 条把计划推到了 DUAL_WRITE，而 DUAL_WRITE 阶段
    生产者要往计划的哈希链上写双写记录、`rquant-runtime-serving@` 要写 serving generation 回执，
    两样都要在 `state.sqlite3` 旁边建事务日志——那是**目录**写权限。生产上装着的还是
    2026-09-05 那一版 unit，`ReadWritePaths` 一个都不含 `control/schema-rollouts`，
    所以光装新代码不改 unit，DUAL_WRITE 一开始写就撞回 #227 的形状。

    **位置：第 ④ 步装 bundle 之后、第 28 条起 unit 之前**；与第 30 条谁先谁后都行
    （acknowledge 跑在无 `.env` 的 bootstrap worktree 里，不经过 unit 沙箱）。
    **这是 `deploy/systemd/` 改动，属高风险变更，要 owner 单独明确授权，不走无人值守发布器。**

    ```bash
    STAMP="$(date +%Y%m%d-%H%M%S)"
    sudo install -d -m 0700 "/root/rquant-unit-backup-${STAMP}"
    for f in "${WT}"/deploy/systemd/rquant-runtime-*@.service; do
      u="$(basename "$f")"
      grep -q 'control/schema-rollouts' "$f" || continue     # 只重装这次改过的那 16 个
      sudo cp -a "/etc/systemd/system/${u}" "/root/rquant-unit-backup-${STAMP}/"
      sudo cp "$f" "/etc/systemd/system/${u}"
      sudo systemd-analyze verify "/etc/systemd/system/${u}"
    done
    sudo systemctl daemon-reload
    grep -c control/schema-rollouts /etc/systemd/system/rquant-runtime-*@.service | grep -c ':1$'
    ```

    最后那行应当输出 **16**，且 `systemd-analyze verify` 逐个退 0。
    **云端语法已经先验过**：协调者 2026-09-07 在 82.156.0.68 的临时目录里对这 16 个文件跑过
    `systemd-analyze verify`，**16/16 通过**；装到 `/etc/systemd/system/` 之后仍要再验一遍，
    因为那时才会去解析 `Slice=` 与 drop-in。

    另外七个 runtime unit 一点都不给，不要顺手一起 `cp`——那会把授权面从十六个扩到二十三个。
    回滚就是 A-7 的回滚：`sudo cp -a /root/rquant-unit-backup-${STAMP}/* /etc/systemd/system/`
    加 `daemon-reload`。

32. **凭证的判据已换成 systemd 自己的投递形状，装机前先核一次那块挂载**（#230，#215 的第三处断点；
    裁决 19，**需 @roxorlt 知悉**）。#215 有三处断点，前两处（wrapper 白名单缺
    `CREDENTIALS_DIRECTORY`、七个 role 的环境面）在上一节；第三处是**读者的判据描述的不是 systemd
    真正投递的东西**。2026-09-08 02:04 第三窗口实测：`/run/credentials/<unit>/` 是一块
    `ro,nosuid,nodev,noexec` 的内存挂载，目录 `root:root 0550`，`capabilities.json` 是
    `root:root 0440` 外加一条放行 `lighthouse` 的 POSIX ACL；而旧判据要求
    `st_uid == os.geteuid()` 且 `mode & 0o077 == 0`，属主与 group 位两条都不满足，五个 unit
    因此全部起不来。包 E 的 e2e 用「当前用户属主 + 0400」造夹具，那是照着读者写的、不是照着
    systemd 写的，所以这条一路绿到生产。

    **新判据摘要**（每条都能指到 systemd 255 的出处，本地副本
    `src/core/exec-credential.c` 与 `src/shared/mount-util.c`）：

    | 面 | 要求 |
    |---|---|
    | 路径 | `/run/credentials/<本 unit>.service`；`/proc/self/cgroup` 读得出 `.service` 叶子时还要与本进程所属 unit 一致 |
    | 目录属主 | `root:root`，**或**本进程 `uid:gid`（ACL 放不下时 systemd 的属主 fallback，`exec-credential.c:735` 一带的 `fchown(dfd, uid, gid)`） |
    | 目录 mode | ∈ {0500, 0550, 0700} |
    | 挂载 | tmpfs 或 ramfs、带 `nosuid,nodev,noexec`、**且是只读的**；再拿目录自己的 `st_dev` 到 `/proc/self/mountinfo` 反查，确认选中的确实是这一条 |
    | 文件 | `O_NOFOLLOW` 打开、正规文件、nlink 1、属主 ∈ {0, 本进程 euid}、mode ∈ {0400, 0440} 且 `mode & 0o007 == 0`、带 group 读位时必须 `root:root`（ACL 投递就是这个形状）、读后 fstat 互校、1 MiB 上限 |

    两处属主 fallback（文件 `exec-credential.c:198-204`、目录 `:735`）共用同一个安全前提，而且
    是 systemd 自己写在注释里的：属主 fallback「only safe if we can then re-mount the whole thing
    read-only, so that the user can no longer chmod() the file to gain write access」。systemd 在
    把工作区移到最终位置之前**恒**重挂只读（`:869` 的 `MS_BIND|MS_REMOUNT` + `MS_MOVE`），所以
    `ro` 对**每一种**形状都是硬要求，不只对 fallback 那一支。这两条合起来就是裁决 19（D4 放宽 +
    `ro` 升硬要求），属 TCB 相邻变更，**向 owner @roxorlt 点名**。

    **为什么 fallback 那一支不是远端角落**：`mount_credentials_fs`（`mount-util.c:1648`）的挂载
    偏好是「tmpfs + `noswap`（需内核 ≥ 6.3）→ ramfs → 普通 tmpfs」，而 ramfs 根本不支持 POSIX ACL。
    只要主机内核 < 6.3，systemd 就会走 ramfs 加完整属主 fallback（文件与目录都 chown 给服务用户），
    当前判据接受这种形状——前提正是那块挂载只读。**换主机或换内核之后先核一次
    `/proc/self/mountinfo` 与 `uname -r`**：两支现在都被接受，所以这次核对是确认，不是排雷。
    （用瞬时 unit 探一次属生产写操作，**需 owner 单独授权**，本包没做。）

    **同 uid 的角色之间靠两层叠加隔离**，不要只记形状层：形状层是「目录必须是本 unit 自己的」
    （`/proc/self/cgroup` 读得出才施加），第二层是凭证内容里 `service_id` / `instance_name` /
    `bundle_generation` 三核。

    这一条**不改 `PRODUCTION_ROLE_POLICY`**，所以上一节那份角色策略摘要
    （`681151cb…`）与由它推出的换代要求不受影响，**没有新增换代理由**。

33. **v0.33.2 不要装在第三代之上；第五窗口直接以 v0.33.3 装第四代**（#237）。2026-09-08 在
    第三代（producer_commit `a0bbb4c`，也就是 v0.33.1）上跑 `rquant runtime-deployment-profile`，
    dry-run 与 `--apply` 都抛 `RuntimeSchemaCompatibilityError: new producer -> old consumer
    is incompatible on runtime.serving.runtime-health for serving.publisher.v1`，
    `runtime.serving.runtime-health` 这条 channel 的九个字段全部报「同一版本内出现语义变更」，
    而这九个字段没有一个被人动过（变的是被内嵌的心跳文件模型，报错为什么指错地方见 #238）。

    **直接在第三代上装 v0.33.3**：`bash scripts/deploy-production.sh --target v0.33.3`。
    v0.33.3 发布到 `runtime.serving.runtime-health` 的声明是**逐字段还原**的 v0.33.1 那一份，
    九个哈希与第三代逐个相同、序列化后的健康载荷与 v0.33.1 逐字节相同（都已实测），
    **所以服务健康这条 channel 不需要 rollout 计划、也不需要消费者回执**——第 30 条那趟
    acknowledge 的计划清单里不会多出它。

    **回滚目标是 v0.33.1，不要回滚到 v0.33.2**：v0.33.2 装不上第三代，也就不存在任何一代是
    由它装出来的。回滚本身仍按第 21 条先把已停实例的旧心跳文件移走。

    **快照的刷新责任在集成者**：`tests/fixtures/runtime-schema-contracts/` 下的那份
    `schema-contracts.json` **在每个发布 tag 上刷新一次**，节奏与 `tests/manifests/full-suite-v1`
    的全集清单一致；放进去的必须是那一刻**生产上实际装着**的那一代的
    `<runtime_root>/current/schema-contracts.json`，不是本地生成的 bundle。现在入库的
    `v0.33.1.json` 就是生产第三代的原件（producer_commit `a0bbb4c`、21 条 channel）。
    文件按它对应的 tag 命名，旧的那份可以删——闸门只读一份。规则同时写在 `tests/README.md`。
    装上 v0.33.3 之后，下一次刷新放的是 v0.33.3 那一代的文件；由于这条 channel 的九个哈希不变，
    那次刷新不会改变闸门的判定。

    `waiting_for` / `waiting_since` / `waited_seconds` 三个字段仍然**只在心跳文件里**，
    runbook 里用 jq 读心跳文件的探针照常工作（第 28 条的三条判据不受影响）；要让它们出现在
    `rquant runtime-health` 这类经由服务健康载荷的路径上，必须给这条 channel 升
    `schema_version` 并走完整 rollout，见 #239。

### 已知限制（装机前已登记的 issue，外加 2026-09-05 首次装机当场发现的 #198、路线 A 首次安装当场发现的 #215–#218，修 #218 时查出来的 #220，以及修 #237 时分出来的 #238、#239；末列写「已修」的条目已修，其余不修）

| 号 | 是什么 | 本次窗口怎么办 |
|---|---|---|
| #186 | recovery role 在 wrapper 下起不来：`runtime_recovery_backup` → coordinator → `formal_smoke_replay` → `dashboard/strategy_lab_runs` 这条 import 链在模块级构造 `Settings` | **已修（PR「fix(runtime): bind schema services to the legacy generation under route A」）**：`strategy_lab_runs.py` 与 `strategy_lab_data.py` 两处都惰性化。issue 写的链是四层，实测是六个模块——只改最深那一处修不好 |
| #187 | legacy `current` 的 generation id 与 `--expected-generation` 属不同名字空间，权威链下 schema binding 永远不可能是 current | **已修，但走的是 #207 的读法**（同一个 PR）：schema bindings 改按 `<root>/current` 解析出的 legacy generation 装载，权威 generation 那一层的绑定一字未改，另加一道运行期交叉核对。新前置见上面第 13–17 条 |
| #188 | recovery role 在 wrapper 子环境里 import `runtime_recovery_backup` 即死（同为 import 期 `Settings`） | **已修**，同 #186 一处改动。验收探针不是 grep import 语句，而是在白名单子环境里真 import 之后把 `sys.modules` 里所有 `rquant.*` 读回来断言，以后再多一条边同一条用例就会覆盖到 |
| #189 | `rquant/logging.py:15` 在 import 期构造 `Settings`，没有 `.env` 时 `rquant` console script 不可用 | **已修（本分支，PR「fix(rollout): prerequisites for the Release A window」）**：`logging.py` 已惰性化，`rquant runtime-authority-stage` 在无 `.env` 的 worktree 里可直接用；`python -m rquant.runtime_authority_stage` 仍然等价，runbook 两种入口都成立。`rquant --help` 按 T9-9 保持 fail-closed，未变 |
| #190 | 已有 `current.json` 时无法更换 profile（发布原语拿已安装 profile 校验 previous），profile / generation / R07 policy 三件套从第二代起换不了代 | 首次发布 `previous=None`，本次不受影响；第二代起要改 `profile_id` 需 owner 单独授权扩展原语 |
| #191 | `rquant-lab-claim-finalizer` 与 `rquant-runtime-lab-jobs@` 依赖仓库里根本没有产生者的四份 `/etc/rquant` 输入，外加一个文档明令「不安装」的草案 unit | `lab-jobs@` 用一个空目录绕过；finalizer 本次不启用，判据按上面的应急口径 |
| #192 | 26 个 protected unit：25 个没有 `[Install]` 段、`systemctl enable` 不了（重启机器不自动拉起）；16 个第一关 unit 的 `ReadWritePaths=` 原先全无 `-` 前缀，目录缺失即在挂载命名空间阶段 `226/NAMESPACE`（前缀已补，`[Install]` 仍未做） | 本次用 `systemctl start`；**预建 31 个目录这一步照旧必须做**——`-` 前缀只把 `226/NAMESPACE` 换成 wrapper 层可诊断的失败，不让服务自己建出目录（`ProtectSystem=strict` + `ProtectHome=read-only` 下被忽略的路径在命名空间里仍是只读，`mkdir` 得 `EROFS`）；且不能顺带创建 `data/runtime/current` |
| #193 | TP1 发布链路两处 `os.open` flag：`ldd` 输出里的 symlink 成员被 `O_NOFOLLOW` 拒（G-2）；`_copy_new_file` 缺 `O_NONBLOCK`，路径被换成 FIFO 可让 root publish 挂死（N-6） | **已修（本分支，同一 PR）**：闭包成员与 loader 按真实路径声明（`resolved_closure_member`），读侧 `_READ_FLAGS` 带 `O_NONBLOCK`。实测本机 `ldd` 报的三个成员都是普通文件，G-2 本来也不会命中；N-6 不再需要 Ctrl-C 兜底 |
| #195 | WP9 rendezvous poll 与它自己在等的 SQLite 锁争用，helper 首次续约可能输给 `database is locked` | CI 间歇性红，不影响生产行为；重跑前先按此条判因 |
| #198 | 2026-09-05 首次装机当场撞到的两处主机形状硬拒绝：**BLK-1** stage 报 `refused: standard library directory is missing: /usr/local/lib64/python3.11`（RHEL 系 `sysconfig` 把 `platstdlib` 指到一个发行版从不创建的目录）；**BLK-2** root publish 报 `RuntimeAuthorityPublishError: deployment lock ancestor / is unsafe`（可信祖先遍历要求 `/` 恰为 `root:root 0755`，OpenCloudOS 9.2 的 `/` 是发行版默认的 `0555`） | **已修（PR「fix(runtime): unblock the Release A first gate on a real RHEL host」，本条的修复分支）**：缺失的 `platstdlib` 移出闭包并在 `plan.json` 的 `closure_summary.skipped_stdlib_roots` 如实记录；`/`、`/etc`、`/var`、`/var/lib` 四个发行版自有目录改按「属主 root + 无 group/other 写位」判定，rQuant 自建目录与所有文件级校验不变（TCB 语义变更，详见 CHANGELOG 的 Security 一条）。**云端验收判据**：B-6' 的 `plan.json` 里 `closure_summary.stdlib_roots == ["/usr/lib64/python3.11"]` 且 `skipped_stdlib_roots == ["/usr/local/lib64/python3.11"]`；B-7 root publish 能取到部署锁；最终 `wrapper_preflight == 32`。**不要**拿 `publish --dry-run` 通过代替 B-7——dry-run 在取锁之前就返回 |
| #215 | credstore 密封了 7 个实例，路线 A 首次安装时逐个 start 过的 6 个 role 一个都没能持续运行：`reference_slow_source` / `market_minute_source` / `auction_match_source` 在 wrapper 白名单子环境里构造 `Settings` 缺 5 个字段（与 #189 同类，只是发生在子环境里）；`daily_close_source` 报 `TUSHARE_TOKEN_MAIN capability is required`；`reference_slow_publisher` 报 `requires its isolated publication credential`；`notifier` 缺 route spool（另涉 #218） | 首次安装时全部 `systemctl stop` + `reset-failed` 防重启风暴，判据记 0/7（前置第 25 条）。**这是第二关的硬前置**——没有 `reference_slow_publisher` 就没有 serving generation，包 D 排不了。已派单独一包修。**这个号下面一共三处断点**：wrapper 白名单缺 `CREDENTIALS_DIRECTORY`、七个 role 的环境面，以及**读者的凭证判据描述的不是 systemd 真正投递的形状**（另立 #230，见前置第 32 条） |
| #216 | 换代之后旧实例留下的心跳文件仍在，`spec_fingerprint` 属于旧 spec，新代同一角色启动即报 `runtime heartbeat does not match the requested service spec` | 手工把已停实例的心跳文件移走再启动（前置第 21 条），首次安装时在 `runtime_health_publisher` 与 `serving_publisher` 上各命中一次。正确修法是发布链路自己作废旧代心跳，与 #215 同一包 |
| #217 | research 平面四个角色（`lab_artifact_catalog` / `promotions_publisher` / `shadow_session` / `lab_jobs_publisher`）启动即 `FAIL research blocked: high-water evidence unavailable or invalid: /var/lib/rquant/workload-isolation/high-water.json` 并退 0。这是 workload arbiter 的资源门，不是崩溃，但这份高水位证据由谁产出、什么时候产出，仓库里没有答案 | 判据按「已启动、被门挡住」记，不计入持续运行数（前置第 27 条）。要让 research 平面真跑起来，得先定这份文件的产生者，本轮不做 |
| #218 | completion signer / router / broker / recovery 这一串起不来：`strategy_live` ×3 报 `completion signer profile contains invalid manifests`；`signal_router` 缺 runner source，`paper_broker` 与 `notifier` 缺 route spool（依赖 `strategy_live` → `runner.sqlite3` → router → spool 这条链）；`runtime_recovery` 与 `rehearsal` 报 `profile generation is stale` | **已修（PR「fix(runtime): unlock the live strategy chain and recovery units under route A」）**：A 完成签名器改成先 `model_dump(mode="json")` 再重验（冻结过的 manifest 不再被自己的 `JsonValue` 断言拒掉）；B 两个 recovery oneshot 改核自己命名空间里的 `recovery_profile_generation`，权威链那道绑定另走 `resolve_legacy_schema_generation`，两者都不给的调用方被拒；C 新增 `scripts/provision_runtime_recovery_credentials.py` 产出那两份从来没有生产者的文档（前置第 29 条）。router / broker / notifier 那条链不是单独的缺陷，是启动顺序，见 #220 与前置第 28 条（那条互等已由 #231/#232/#220 那一包修掉） |
| #220 | live 平面的启动顺序是代码层的循环依赖：`strategy_live` 要读 `live/signal-bus/signal_bus.sqlite3`，只有 `signal_router` 会建它，而 `signal_router` 又先要求三份 `live/strategies/*/runner.sqlite3` | **已修（PR「fix(runtime): let the live strategy chain start idle under the sandbox」，与 #231、#232 同一包）**：`strategy_live` 一启动就建自己的 `runner.sqlite3`、`signal_router` 一启动就建 bus 与 spool，缺对端制品的一方在主循环里等而不是退出，两个方向都能起。原来那条「起一轮失败留下 runner 数据库」的绕过法作废，前置第 28 条已整条改写；代价是等对端时只有 DEGRADED 没有告警（阈值见 #235） |
| #238 | `_field_schema_hashes` 给每个字段算哈希时把载荷**整个 `$defs`** 一起算进去，所以任何一个被内嵌的嵌套模型多一个字段，这条 channel 每个字段的哈希都会跟着变；报错却逐字段说「type changed / semantic meaning changed」，指向的是一个都没被改过的字段，真正的变更点（哪个嵌套模型、哪个字段）在消息里一个字都没有 | 不修。#237 的冻结投影只挡住 `runtime.serving.runtime-health` 这一条 channel；另外 20 条里凡是内嵌了「不是为发布而写」的模型的（`ServingProjectionPayload` 6 条、`BatchQualityStatus` 5 条、`LiveChannel` 5 条、`JsonValue` 4 条），同样的形状仍可能再来一次。跨版本快照闸会在装机前把这类改动拦在 CI 里，装机时按前置第 33 条处理 |
| #239 | #231 给心跳文件模型加的 `waiting_for` / `waiting_since` / `waited_seconds` 只在心跳文件里，`rquant runtime-health` 这类经服务健康载荷的路径看不到 | 不修。冻结投影上多一个字段就会让九个哈希全变，等于重演 #237；要发布这三个字段必须给 `runtime.serving.runtime-health` 升 `schema_version` 并走完整 rollout（PREPARE → 生产者承认 → DUAL_WRITE → 消费者回执 → CUTOVER），且在那次装机窗口里刷新跨版本快照。本轮照旧用 runbook 的 jq 探针直接读心跳文件 |

### ⚠️ 下一个装机窗口的强制前置：必须换一代 profile（#215 修复引入）

修 #215 要给七个 credstore role 的环境白名单加 `CREDENTIALS_DIRECTORY`
（`src/rquant/runtime_authority.py` 的 `_CAPABILITY_ROLE_ENVIRONMENT`）。**不加，凭据到不了角色**：
systemd 把解密后的 `capabilities.json` 放进 `$CREDENTIALS_DIRECTORY`，交给 unit 的 ExecStart 也就是
wrapper，而 wrapper 从空环境起、只复制 profile 白名单里的名字，没登记的名字被静默丢弃。
2026-09-07 窗口里 7 个 role 全起不来、进而没有 serving generation，根因就在这一条。

`environment_allowlist` 参与 `profile_id` 的哈希，所以这个改动**必然换 `profile_id`**：

| | 值 |
|---|---|
| 角色策略摘要（旧，生产在跑的 `v0.32.2` `695e952`；合并基 `origin/main` `a90f927` 的 `runtime_authority.py` 与它逐字节相同） | `6282aa50fca9cfca113a966379187202bdb975a04072b1beaf9ee5b8bb1ab102` |
| 角色策略摘要（新，含 `CREDENTIALS_DIRECTORY`） | `681151cbdfa310a83adb5ede906c1970913e1398960d8136b92c0b3114f44167` |
| 生产 `profile_id`（旧，2026-09-07 sequence 3 在用） | `d2206e53…7ea0` |
| 生产 `profile_id`（新） | 装机当场由 `runtime-authority-stage` 算出（含主机闭包与实例标签，本地算不了） |

「角色策略摘要」是 `PRODUCTION_ROLE_POLICY` 单独做 canonical JSON 之后的 sha256，只用来证明
「角色这一层确实变了」，不是 `profile_id` 本身（`profile_id` 还含主机闭包与实例标签，本地算不出）。
复算命令（在仓库根目录，任意 checkout）：

```bash
uv run python -c '
import hashlib, json
from rquant.runtime_authority import PRODUCTION_ROLE_POLICY
body = [
    {
        "name": e.name, "module": e.module,
        "environment_allowlist": list(e.environment_allowlist),
        "service_kind": e.service_kind, "control_root": e.control_root,
        "once": e.once, "module_arguments": list(e.module_arguments),
    }
    for e in PRODUCTION_ROLE_POLICY
]
print(hashlib.sha256(json.dumps(body, separators=(",", ":"), sort_keys=True).encode()).hexdigest())
'
```

**为什么不能直接发 sequence 4**：`#190`——已有 `current.json` 时，发布原语拿**已安装**的 profile
校验 record 的每一个 slot，`profile_id` 不同即 `RuntimeAuthorityRecordError: runtime slot profile id
is not active`；TP1 发布器在动任何 root 路径之前就显式拒绝。本包**不修 #190**（TCB 原语，需 owner
单独授权）。

**因此下一个窗口按 runbook §0.6「B-8 首次 publish」那条的逆过程走，重新首发。**

> ⚠️ **下面每条单独执行、每条看返回码**，不要整段粘贴（没有 `set -e`）。
> 第 ③ 步删掉 `current.json` 之后就回不到 sequence 3 了，**必须先看到第 ②c 步列出两个文件**再往下走。
> 不要用 `/root/rquant-profile-rollover-*/` 这种通配符：`/root` 在 OpenCloudOS 是 `dr-xr-x--- root root`，
> `lighthouse` 的 shell 展不开它，`cp` 会报 `No such file or directory`——而那正是备份没成的时刻。

```bash
# ① 停掉全部 runtime unit（模板 unit 无 [Install]，stop 即可；oneshot 等它自己退）
#    停之前先确认没有 daily/monitor 正在写库

# ②a 先把时间戳固定成一个变量，后面每条都用它，不再第二次调 date
STAMP=$(date +%Y%m%d-%H%M%S); echo "${STAMP}"

# ②b 建目录并备份两份 root 文档（换代出问题时靠它们回到 sequence 3）
sudo install -d -m 0700 "/root/rquant-profile-rollover-${STAMP}"
sudo cp -p /var/lib/rquant/runtime-authority/current.json  "/root/rquant-profile-rollover-${STAMP}/"
sudo cp -p /etc/rquant/production-runtime-profile.json     "/root/rquant-profile-rollover-${STAMP}/"

# ②c 确认两份都在，再往下走。看不到这两行就停在这里，不要执行 ③
sudo ls -la "/root/rquant-profile-rollover-${STAMP}/"
#    期望：current.json 与 production-runtime-profile.json 各一份，属主 root

# ③ 删掉 current.json —— 权威链回到「wrapper 全拒」的安全态，这一步之后没有服务能起
sudo rm -f /var/lib/rquant/runtime-authority/current.json

# ④ 用新代码 stage + publish，previous is None，走首发路径（sequence 回到 1）
#    判据仍是 wrapper_preflight == 32；不要拿 `publish --dry-run` 通过代替真 publish（#198 的教训，
#    dry-run 在取部署锁之前就返回）

# ⑤ 重启 C 段 unit
```

**回滚**（换代失败时）：

```bash
sudo cp -p "/root/rquant-profile-rollover-${STAMP}/production-runtime-profile.json" /etc/rquant/
sudo cp -p "/root/rquant-profile-rollover-${STAMP}/current.json" /var/lib/rquant/runtime-authority/
```

两份必须同时是旧的一代（先后顺序不重要，中间态没有服务在跑），再重启 unit——旧 generation
目录内容寻址、永不删除，所以旧一代随时可用。
credstore 的 `.cred` 不受影响：`current.cred` 按 bundle generation 指向，与 profile 无关。

**顺带两条**：
- `data/runtime` 的 legacy bundle 不必重装，本次改动不动 bundle generation，只有权威链那一层换代；
- runbook 的 **R-14（换代前人工把旧心跳文件移走）作废**——#216 已在代码里修掉，
  `read_heartbeat` 对「已 stopped 且单例锁无人持有」的旧心跳自动 supersede，判据比 R-14 更严。

### 2026-09-05 首次装机窗口的结果（决定下次从哪起跑）

窗口在 `v0.31.1`（`0fb7d95b16189af5763c8015c87b969ea69f7156`）上执行，**生产代码未切换**，第一关的真判据（`wrapper_preflight == 32`）未取得。

- **A 段通过验收，装好的东西全部保留**：三个 root 制品、workload arbiter、5 个 slice、19 个既有 unit 都在，`/home/lighthouse/rquant-relA` 的 worktree 与 `/home/lighthouse/rquant/.venv.new-3.11` 也在。**下个窗口不必重做 A 段**，省掉 A-0 那 88 分钟。
- **B 段停在 #198 的两处硬拒绝上，C 段未开始**；B-2（生成四套 Ed25519 私钥）与 B-3 是主动跳过的，没有任何密钥落地。
- **A-1（生产 venv 换 3.11）当场整体回滚**：`mv .venv` 会让 Streamlit 按绝对路径读静态资源的三个页面立刻 500，runbook 里「对现役零影响」这句不成立。生产 venv 仍是原来的解释器，3.11 的 venv 原地留作 `/home/lighthouse/rquant/.venv.new-3.11`，等第二关的停服窗口再改名启用。
- **#198 修复合入并重新打 tag 之后，装机从 B-2 起跑**，预算 B 段 1.5–2 h + C 段 1–1.5 h。
- 完整执行记录（含每条命令与输出原文）在 Mac 本地的 `/Users/roxor/brain/30-projects/rQuant/.worktrees/blk-cc/.superpowers/sdd/2026-09-05-host-blockers/rollout-exec-report.md`，没有进仓库。它的 P1 一节列了 runbook 的四处订正（A-1 移出第一关、A-8 移到 C-2 之后并以 root 跑、A-5 改用 `verify` 子命令、§0.4 的常驻服务基线口径），开工前先改掉。

（#194 是 Release A 工具链的 PR，不是 issue。）

---

## v0.30.0 Release A 上线前置条件（尚未部署，非部署记录）

**状态**：PR #155（`cc/workload-isolation-continuation`）已于 2026-08-28T12:51:28Z 以 merge
commit `2df97ed6045c4ab7efc676f31c742c97ae2193f4` 合入 main，**尚未打 tag、尚未部署**，
云服务器 82.156.0.68（lighthouse 用户）上没有发生任何变更。合入后那一次 main push CI 结论是
failure（`https://github.com/roxorlt/rquant/actions/runs/33172825610`）：R07 三个专用 job 全绿并
产出了 evidence artifact，红的是 full-suite 分片。本节记录的是这次发布**之前**必须逐条满足的
条件，不是一条部署记录；真正部署后再按本文件的既有格式追加
`## YYYY-MM-DD · v0.30.0 · 标题`。

1. **合版方式只能是 "Create a merge commit"**（技术强制，非约定）。R07 证据的 merge-provenance
   检查要求候选 commit 恰有两个 parent、第一 parent 等于冻结 baseline、且
   `git merge-tree --write-tree <parent1> <parent2>` 等于候选 tree；squash 与 rebase 只有一个
   parent，CI 的 push-to-main 路径会直接拒绝产出证据，Release B 也就永远拿不到部署证据。
   （实测：squash 提交的 tree 与 merge 提交完全相同，只有 parent 结构能区分两者。）
   Release A 这一步已经完成：`2df97ed` 恰有两个 parent，第一 parent `9699827b` 就是当时冻结的
   baseline。合并后实测 `git merge-base --is-ancestor 45d0b57c 2df97ed` 返回 0——
   `45d0b57c` 是 **historical baseline**，门禁仍单独要求它是每个候选的祖先。
2. **Release B 的冻结 baseline 是 commit `2df97ed6045c4ab7efc676f31c742c97ae2193f4` /
   tree `1e145e8a2b84ea43934bdf5a1cdca5b591445cab`**，即 Release A 合入 main 产生的那个
   merge commit。它不再由 checkout 里的 `merge_base(origin/main, HEAD)` 反推——那条语义在合入
   之后自我否定：`origin/main` 就是候选本身，merge-base 恒等于 HEAD，任何冻结常量都对不上。
   现在 baseline 由 `resolve_baseline_context()`（`src/rquant/signal_family_differential_gate.py`）
   从**显式端点**判定，来源优先级固定为：显式 CLI 参数 → GitHub 事件载荷
   (`pull_request.base.sha`/`head.sha`，push 的 `before`/`after`) → HEAD 自身的父结构；
   `origin/main`、`origin/HEAD` 与任何 remote-tracking ref 都不再被读取。
   allowlist 的条数随 baseline 变化：PR #155 合入时是 **792 条**（本文件旧版写的「764 条」是
   更早一版的数字，已过期），重冻结到 `2df97ed` 之后只剩 Release B 自己的改动，确切条数以
   `scripts/r07_policy_regenerate.py` 重生成的结果为准，不在文档里另记一个会过期的字面值。

   **纪律（Release B 起强制）：任何 PR 合入 main 之前，它的最后一个 commit 必须把 baseline
   重新冻结到当时的 `origin/main` tip，并重新生成 policy。** 理由是结构性的，不是流程洁癖：
   合并产生的 merge commit `M` 的第一 parent 是合并前的 main tip，push-to-main 的 R07 job 要求
   第一 parent 恰等于冻结 baseline；只要 baseline 停在更早的 commit，`M` 的 R07 job 就确定性
   失败，evidence job 因 `needs` 不满足而 skip，**该 commit 永远拿不到 evidence artifact，也就
   永远不能成为部署目标**。

   **不必等到 main push 才发现**：`tests/unit/test_r07_policy_regenerate.py`（`--check` 与幂等
   生成）和 `tests/unit/test_signal_family_differential_gate.py`（冻结常量、allowlist 等式）都在
   full-suite 分片的 manifest 里，而 PR 上 checkout 的是 GitHub 合成的合并 ref，它的第一 parent
   就是当时的 base tip——所以 baseline 一旦过期，**PR 阶段的 full-suite 分片就会先红**。
   看到这两个文件在 PR 上红，第一件事是查 baseline 是否过期，而不是查测试本身。
   main push 阶段才轮到 R07 三个 job 与 evidence 确定性失败。具体操作：

   ```bash
   # 在 PR 分支上，所有代码改动定稿之后
   #   1) 把 BASELINE_COMMIT_SHA / BASELINE_TREE_SHA 改成当时的 origin/main tip 与它的 tree
   #   2) 重生成 policy（必须用 Python 3.11 或 3.12，脚本会拒绝 3.13+）
   uv run --python 3.11 python scripts/r07_policy_regenerate.py --repo "$PWD"
   uv run --python 3.11 python scripts/r07_policy_regenerate.py --repo "$PWD" --check  # RC=0
   #   3) 同步 docs/architecture/production-interpreter-authority.md 里的字面 SHA
   ```

   **两个 PR 并发时 main 被串行化**：先合的那个让 main 前进，后合的那个的 baseline 立刻过期，
   必须重做第 1–3 步再合。忘记这一步的代价不是「CI 偶发红」，而是那个 commit 永久失去部署资格。

   **开放项（待用户/Codex 裁决，本轮不实现）：R07 差分门的退役条件。** 上面的串行代价只有在
   R07 门存在期间才需要承受。R07 本质上是一次性 cutover 门（v3 spool 切换 + Release A→B 的
   bootstrap 边），一旦 Release B 部署完成、v3 切换收尾，policy、三个 CI job、shard-3 那批测试
   与部署侧 evidence 消费可以整体退役。**是否退役、以什么条件退役，需要一次明确决策记录**；
   在那之前按上面的纪律执行。
3. **云服务器 82.156.0.68（lighthouse 用户）的 `/usr/bin/git --version` 必须 ≥ 2.38**。
   私有 verifier 用 `git merge-tree --write-tree` 离线重放 merge provenance，该子命令自 2.38
   起才可用；低于 2.38 时 Release B 的部署器会以
   `the deployment Git cannot replay merge provenance` fail closed（不降级为 warning）。
   部署前在云端执行 `git --version` 核对并把结果写进本节。
4. **第一次真实 push-to-main run 之后**核对 WP1-SPEC-06 / SPEC-12：evidence 里的
   `job.check_run_id` 与 artifact 内部路径必须与真实 GitHub API 返回一致（本地只用 fake
   transport 验证过）。
5. **服务器 `.env` 增加 `RQUANT_GITHUB_EVIDENCE_TOKEN`**。这属于生产密钥变更，需要用户单独
   明确授权；Release A 本身不消费证据，这个 token 是 Release B 才需要的。
6. **Release A 之后的下一次部署只能是 Release B**：`deployment_mode=enforced`，并且
   `bootstrap_predecessor` 精确声明 Release A 的 commit 与 tree SHA。中间不允许插入其他
   部署目标。
7. **云端只读核对**：对每一个 live generation 核对
   `sha256(full-manifest.json) == slot.full_manifest_hash`。只读操作，走
   `open_readonly_store()` / 只读副本，不碰主库写锁。
8. **云端 child 访问实验**：以真实 `lighthouse` 身份对 `0715` 的 child workspace 做
   `O_RDONLY | O_DIRECTORY` 打开，并确认 `id -g lighthouse != 0`——工作区的 group 位是
   `--x`，子进程一旦落进 group 类就会丢掉读权限（验证器现在会直接拒绝这种身份配对）。
9. **root verifier 必须从 root-owned 树运行**（Codex round-2 P1-4 已落地，安装仍待授权）。
   `scripts/signal-family-root-verifier.py` 现在直接拒绝运行并退出 78；生产入口改为固定的
   root-owned 制品对，由 `scripts/build-signal-family-verifier-artifact.py` 构建：

   ```bash
   # --source-venv 必须显式指向**目标解释器**的 venv：制品树里带原生扩展
   # （pydantic_core 的 .so），用别的 ABI/平台构建出来的树在生产 python3.11 上 import 不了，
   # 而 content-id 是这棵树的哈希，所以构建主机不同 → TCB 锚点不同。
   python scripts/build-signal-family-verifier-artifact.py \
     --output-root <staging> \
     --source-venv /home/lighthouse/rquant/.venv \
     --python-version "$(/usr/bin/python3.11 -c 'import platform;print(platform.python_version())')" \
     --target-platform linux
   # 打印 content_id / entry_sha256 / manifest_entries 及安装位置
   # 构建脚本现在自己 guard：源 venv 的解释器版本 / ABI tag / 平台与目标不一致就构建失败，
   # 不会再产出一棵装着 cpython-313-darwin 扩展却声称 3.11 的树。下面这条只是人工复核。
   find <staging>/<content-id> -name '*.so'   # 必须是 linux 的 ABI tag
   ```

   安装是**单独授权的 root 事务**，不能借受控发布器绕过。逐条核对：

   - 树装到 `/usr/local/lib/rquant-signal-family-verifier/<content-id>/`，`root:root`，
     目录 `0555`、文件 `0444`（源可执行的为 `0555`），每个文件 `nlink == 1`；
   - 入口装到 `/usr/local/libexec/rquant-signal-family-verifier-v1.pyz`，`root:root`，
     `0555`，`nlink == 1`，SHA-256 与构建输出一致（两次构建必须字节相同）；
   - 运行 `/usr/bin/python3.11 -I -S /usr/local/libexec/rquant-signal-family-verifier-v1.pyz`
     应能通过树校验；树里任何一个字节被改、多一个文件、少一个文件、mode 放宽或换了属主，
     都必须退 78 且不启动；
   - 回滚 = 把入口换回上一个 content id 的构建产物，旧树不删；两棵树可以共存。

10. **`/usr/bin/setpriv` 必须存在且进 TCB**（Codex round-2 P1-5）。root verifier 与
   `rquant-workload-arbiter` 的降权/父进程死亡信号都改由它执行，不再有任何 `preexec_fn`。
   OpenCloudOS 9.2 上部署前核对：

   ```bash
   /usr/bin/setpriv --version                 # util-linux ≥ 2.33（--pdeathsig 需要）
   stat -c '%U:%G %a %h' /usr/bin/setpriv     # root:root，无 g/o 写位，nlink 1
   sha256sum /usr/bin/setpriv                 # 记入 TCB 清单
   ```

   缺失或不满足属主/权限时，root verifier 会拒绝启动（fail closed），不会退回旧路径。

11. **`/usr/local/libexec/rquant-runtime-exec.pyz` 必须先安装，profile 必须先声明全部 role**
   （Codex round-2 P1-3）。本分支新增的 26 个受保护 runtime unit 已改为固定命令
   `/usr/bin/python3.11 -I -S /usr/local/libexec/rquant-runtime-exec.pyz --role <literal>`，
   不再读 `data/runtime/current/runtime.env`，也不再接受 `%i` 插值的 manifest 路径。部署前：

   ```bash
   python scripts/build-runtime-exec-pyz.py --output <staging>/rquant-runtime-exec.pyz
   # 装到 /usr/local/libexec/，root:root，0555；SHA-256 写进 profile 的 runtime_pyz
   for u in /etc/systemd/system/rquant-runtime-*.service \
            /etc/systemd/system/rquant-artifact-retention.service \
            /etc/systemd/system/rquant-page-control.service \
            /etc/systemd/system/rquant-lab-claim-finalizer.service; do
     systemd-analyze verify "$u"
     systemctl show -p ExecStart "$(basename "$u")"
   done
   systemctl show -p Environment rquant-lab-claim-finalizer   # 白名单补齐用
   ```

   **`PRODUCTION_ROLE_POLICY` 已在本轮扩到 28 个 role**（Codex round-2 P1-3 扩到 26，
   round-3 verdict RQ-WI-R2-P1-01 加 `workload_admission`，RQ-WI-R2-P1-02 加
   `lab_claim_finalizer`），`profile_id` 随之改变——这是刻意的 profile 版本演进，不是副作用。

   `workload_admission` **不属于任何 unit**：它由 `/usr/local/libexec/rquant-workload-arbiter`
   在取得 research 平面锁之后、exec 进 unit 自己的子进程之前调用，替换掉原先的
   `.venv/bin/python -m rquant.workload_isolation research-admission`。新版 profile 必须声明它，
   否则 10 个 research unit 全部起不来（admission 以 78 退出，被 arbiter 读成 research 被拒 →
   unit 退 75）。非 instanced role：`instances` 为空、`control_root` 为空串、`module_arguments`
   恰好是 `["research-admission"]`。

   `lab_claim_finalizer` 由 `deploy/systemd/rquant-lab-claim-finalizer.service` 直接命名
   （`--role lab_claim_finalizer`），该 unit 的 `ExecStartPre` 与旧的
   `.venv/bin/python … run-lab-daemon.py formal` 已一并删除。非 instanced role：`instances`
   为空、`control_root` 为空串、`module_arguments` 恰好是 `["lab-claim-finalizer"]`，
   `environment_allowlist` 为 `["APP_ENV","LANG","LC_ALL","RQUANT_DISABLE_DOTENV","TZ"]`。

   **⚠ 发布这一代 profile 之前必须做完的两件事**：

   1. **补齐环境白名单**。wrapper 的 `build_child_environment` 从空字典起，只复制 profile 白名单
      里的名字，未登记的名字**被静默丢弃**。`/etc/rquant/lab-claim-finalizer.env` 不在仓库里，
      发布前先在云服务器 `82.156.0.68`（`lighthouse` 用户）上跑
      `systemctl show -p Environment rquant-lab-claim-finalizer`，把 finalizer 真正需要的名字逐个
      补进 `src/rquant/runtime_authority.py` 的 `lab_claim_finalizer` role 再发布。
      **首次安装场景无从执行**：该 unit 从未装过，`systemctl show` 读不出任何值（S1 U-7）；
      第一代 profile 用现有五个名字发布，装上 unit 跑起来后若报缺变量，再按 §7 B-13 另开 PR
      补白名单并换代三件套。名字须排序去重、
      不得以 `PYTHON` / `LD_` 开头、不得是 `PATH`，总数 ≤ 32。`PYTHONDONTWRITEBYTECODE` 故意不在
      白名单里：两级都拒 `PYTHON*`，两级子进程都是 `-I -S`，generation 目录 0555，这个名字在旧
      unit 下也从未到达 daemon。
   2. **改 `/etc/rquant/runtime-code-migration.json`**：每个 `formal_services[]` 条目里的
      `wrapper_path` 字段**必须删掉**。unit 不再执行 `scripts/run-lab-daemon.py`，
      `RuntimeCodeFormalService` 已不再声明该字段，而请求模型是 `extra="forbid"`——留着它会让迁移
      gate 以 `extra_forbidden` 失败，finalizer 启动即非零退出并被 `Restart=on-failure` 反复拉起。

   **两条信任链的发布节奏现在是耦合的**（RQ-WI-R2-P1-02 的直接后果）。finalizer 先经
   runtime-authority 链（wrapper 校验 generation），再跑 runtime-code 链（ed25519 attestation +
   promotion receipt）。runtime-authority generation 没发布好——profile 与 `current.json` slot、
   generation full-manifest 不同代，或磁盘上的 pyz 与 profile 的 `runtime_pyz.sha256` 不一致——
   finalizer 就起不来。发布顺序必须是「安装新 pyz + 发布新 profile + 换代 generation」在同一次
   事务里完成，然后才 `systemctl restart rquant-lab-claim-finalizer`。
   bootstrap 绑定与迁移请求路径已经是 generation 常量，改它们同样要换代（R3B-SPEC-04）。

   **R3-A 与 R3-B 必须同一次发布**（R3B-SPEC-01）：两者共用同一代 pyz 与同一个 `profile_id`，
   分开发布会让先发的那一半带着后发那一半的 role 表或哈希，wrapper 直接 fail closed。

   **`rquant-runtime-exec.pyz` 换代**：本轮 `_verify.py` 有改动，SHA-256 由
   `5b903aeff9d5b8c44852825c54bd2531c0a994202bc575c08cce6d1b071a1aed` 变为
   **`a5d9b3fff7388f7aa35a951a6b6bc51e3e9faf69bf8b94b598c7c69b2c9c9c5e`**
   （`python scripts/build-runtime-exec-pyz.py --repository-root . --output <staging>/rquant-runtime-exec.pyz`
   重算，连续两次构建逐字节相同）。安装新 pyz 与发布新 profile 必须同一次事务完成：profile 的
   `runtime_pyz.sha256` 与磁盘上的 pyz 不一致时 wrapper 会 fail closed。

   **R07 policy 必须用 py3.11 或 py3.12 生成**：3.13 的 AST 摘要与前两者不同，生成出来的 policy
   在 CI 上对不上（Codex 非阻断项）。新版 profile 必须逐个 role 声明
   module / 环境白名单 / **instance 白名单**；instanced role 的 `instances` 至少一项且形如
   `svc-<64 hex>`，非 instanced role 必须为空。concrete 标签由
   `runtime_deployment_bundle` 从各 service manifest 派生，不冻结在代码里。
   `current.json` 的 slot 也必须声明同一组 role（`_validate_slot_against_profile` 要求相等）。
   profile 版本变更本身是单独授权的基础设施事务；`profile_id` 同时被 slot、generation
   full-manifest 与 R07 policy 冻结，三者必须一起换代。

12. **每个实例的 service manifest 必须落在 generation 里**。wrapper 不再从 unit 接收
   `--manifest`，而是从权威记录派生：`<generation>/manifests/<instance>.json`，并要求这条
   相对路径是该 generation full-manifest 里的一个 `file` 条目——于是它和其余代码一样被逐字节
   校验。`--control-root` 由 profile 的 per-role `control_root` 前缀拼上已授权的实例标签，
   `--expected-commit` / `--expected-generation` 来自 `current.json` 的 current slot；
   `--expected-kind` / `--once` 由 profile role 策略决定。既有的
   `rquant.runtime_service_main` **不需要任何改动**，它收到的正是原来由 unit 传的那组参数，
   只是来源换成了 root-owned 记录。

   发布 generation 时必须把 service manifest 写进 `<generation>/manifests/` 并纳入
   full-manifest（旧位置 `data/runtime/current/manifests/` 由应用自己可写，已不再被读取）。
   `current.json` 的 `current_commit` 必须是 40 位十六进制 commit sha，否则 wrapper 退 78——
   该值只是转发给既有模块的 `--expected-commit`，wrapper 自身仍不据它做任何判定。

   `rquant-runtime-recovery@` / `rquant-runtime-recovery-rehearsal@` 的
   `--expected-profile-generation %i` 已移除，generation 同样来自 `current.json`。

13. **`deploy/libexec/rquant-workload-arbiter` 的 pdeathsig 竞态**：`setpriv --pdeathsig`
   在 fork 与 exec 之间设置 `PR_SET_PDEATHSIG`，与旧实现一样存在「父进程恰在此窗口内死亡」
   的极小竞态；旧实现额外做的 `getppid()` 复查随 `preexec_fn` 一并移除。这是刻意的取舍：
   ruling D-6 优先消除 fork/exec 之间跑 Python 的死锁面。
14. **R07 证据缓存命中也需要网络与 token**：缓存命中不再跳过 GitHub run 身份核验，部署器仍会
   用 `RQUANT_GITHUB_EVIDENCE_TOKEN` 查一次 workflow runs 解析出当前的 `workflow_run_id` /
   `run_attempt`，因此**离线部署不可行**；GitHub 不可达、token 缺失、该 commit 没有唯一一个
   push-main run 时结果都是 blocked，不降级放行。缓存目录及其全部祖先必须由 root 或部署身份（lighthouse）拥有、无
   group/other 写位、无 symlink；缓存条目本身还必须由部署身份拥有、单链接、不超过 64 KiB，
   否则同样 blocked。
   **重跑 attempt 后旧缓存自动失效**：在 main 上对已部署 commit 点 Re-run all jobs 会产生新
   attempt，GitHub 的 run 列表只返回当前 attempt，旧缓存条目从此对不上。这种「身份不一致」不
   算失败，部署器会当作 cache miss 重新下载当前 attempt 的 artifact、全量重验后原子覆盖旧条目，
   无需人工 `rm` 缓存文件。
   **400 天以上历史不可核验 → blocked，且无补救**：GitHub 的 workflow run 历史保留 400 天
   （artifact 默认 90 天过期，两者独立设置），所以缓存在 artifact 过期后仍可核验；但 commit 超过
   400 天后 run 记录被归档删除，重下载与重核验都不再可能，该目标只能先在 main 上重新触发一次
   CI（或用一个更新的等价 commit）才能部署。
15. **规格 errata 未决**：family taxonomy 单元素域、bundle/overlay identity 语义、
   producer/consumer id 域、profile-service-manifests 文档绑定、WP5 Q1–Q4、wire schema
   在 3.11/3.12 的可见性、退休门的交易日数字，全部等 Codex 裁决。
   （`strategy-router` / `strategy-shadow` 五个 surface 的向量语义已在 R2-E 落地：13 个
   reader surface 全部经真实 production builder 产出，离线 harness 世界在 Linux 上产出
   五对 `READY`。这不改变「本轮不安装、不激活」——Phase C activation 仍不成立。）
16. **Phase C 在真实生产 generation 上能否跑通：谓词侧已由 PR-C 解决，云端实测仍未做**。
   原症状：`strategy-shadow` 的三个 reader 只能经 `FilesystemShadowSessionInputLoader` 读一份
   accepted legacy shadow export，而 `legacy_shadow_export._open_child_directory_at` 曾无条件
   要求 export 的 session 目录 `st_uid == os.geteuid()`（**不是**文件那样的 `{0, euid}`），
   `_ensure_private_root` 又把 export 根 `fchmod` 到 `0700`。合起来：**export 只能被与发布者
   同 uid 的进程读**。离线世界里发布者与 child 同 uid 所以能跑通；生产 generation 是
   root-owned、Phase C child 是非特权 lighthouse，这条路走不通。**更严重的是同一条谓词还挡住
   生产发布路径自身**：签名器 `deploy/libexec/rquant-shadow-report-signer` 经 `sudo -n` 以 root
   运行并 `fchown(dir, 0, -1)` + `fchmod(0o555)`，而发布者 `lighthouse`（`rquant-monitor` /
   `rquant-surge-watch`）在签名之后还要用同一谓词重开自己的 staging（`:1704` / `:2623` /
   `:3224`）⇒ **生产上根本产不出一份 accepted export**，不只是 Phase C 读不到。

   **裁决与落地（PR-C / TP5，TCB 变更）**：三条候选路径里取 ③「修改 `_open_child_directory_at`
   的 owner 谓词」，但不写死 `{0, euid}`，而是**按 `allowed_modes` 推导**——`allowed_modes` 含
   `_SESSION_MODE`（`0o555`，该模式只可能由 root 签名器产生，本模块自己从不生成）时 owner 集合
   为 `{0, euid}`，否则仍是 `{euid}`；同时补一条 `st_mode & (S_IWGRP | S_IWOTH)` 的**无条件拒绝**，
   把 g/o 可写目录从「靠 `allowed_modes` 间接排除」变成结构性拒绝。函数签名、模块内 **14 个调用点**
   与 `scripts/build-signal-family-shadow-fixture.py` 一行未动，离线策略（`allowed_modes = {0o700}`）
   下逐位不变。候选 ①（generation 内的 shadow export 改由 lighthouse 拥有）与 ②（Phase C child 以
   发布者 uid 运行）均未采纳。

   **仍未闭环 —— C7 是 Phase C activation 的前置**：云端真实发布验证还没做。它同时是唯一能证伪
   上面「生产发布路径自身走不通」这条代码阅读结论的实验；在它跑通之前，「Phase C 能在生产
   generation 上产出 READY」依然没有证据，本条仍不构成 activation 依据。

   **运维残留（须 root 手工清理）**：已经产生的 `root:root 0555` staging 目录**不会**被自动清理——
   `_discard_directory_at` 传的 `allowed_modes` 是 `{0o700}` ⇒ owner 推导仍为 `{euid}`，且
   lighthouse 对该目录没有写权限。这类残留必须由 root 手工 `rm -rf`。

17. **arbiter 不再执行 checkout 解释器**（Codex round-3 verdict RQ-WI-R2-P1-01）。
   `deploy/libexec/rquant-workload-arbiter` 在取得 research 平面锁之后、exec unit 自己的子进程
   之前，只启动两类进程：`/usr/bin/setpriv`（parent-death launcher）与
   `/usr/bin/python3.11 -I -S /usr/local/libexec/rquant-runtime-exec.pyz --role workload_admission`。
   这三个可执行文件全部 root-owned、全部已在 TCB 表里。arbiter 自己的解释器行也收紧为
   `#!/usr/bin/python3 -IS`，因此 `~/.local/lib/python3.11/site-packages/usercustomize.py` 与
   `EnvironmentFile` 里的 `PYTHONPATH` / `PYTHONHOME` 都不再能在 arbiter 起步阶段执行代码
   （独立评审 R3A-SPEC-02）。退出码契约不变：admission ≠ 0 → arbiter 退 75。

   **闭包白名单不是零豁免，还剩一项**：`rquant-research-ingest.service` 把一个 checkout shell
   脚本（`scripts/run-research-ingest-daily.sh`）交给 arbiter 当自有子进程，这个 unit 本体在
   `origin/main` 上、不属于本轮范围。`ExecStartPre` 一侧现在是**零豁免**——finalizer 的那条
   已随 RQ-WI-R2-P1-02 删除，所有 arbiter-fronted unit 都不得再声明 `ExecStartPre`。测试以两张
   精确豁免表登记（子进程 1 项、`ExecStartPre` 0 项），多一项或程序变了都会红。

   **`workload_admission` 的发布顺序有硬约束**：必须**先**装好新版 pyz 并发布声明了
   `workload_admission` 的新版 profile，**再**用 `scripts/install-workload-isolation-infra.sh`
   装新版 arbiter。顺序反了，10 个 research unit 的 admission 以 78 退出、unit 退 75——落在
   `SuccessExitStatus=0 75` 之内不触发 `Restart=on-failure`，现象是「unit 秒退成功、什么都没跑」。
   回滚同理：先回滚 arbiter，再回滚 pyz / profile。arbiter 的 `.sha256` 由安装脚本现算，
   代码与文档都不冻结 arbiter 哈希。

   **启动开销（生产口径）**。A1 为默认方案（协调者推荐，已向用户说明生产口径代价：每次研究
   服务启动约 +7 s、retention timer 折合约 34 min/天；截至本轮回执用户尚未明确答复，若改选
   A2 将以追加提交处理）。该探针让每次 research unit 启动多做 2 次完整 generation 校验（wrapper 父进程
   1 次 + 冻结 bootstrap 在 generation 解释器里 1 次），每次逐条 SHA-256 约 666 MiB。
   离线 arm64 容器、页缓存热的条件下 p95 = 0.889 s/次，**该数字不适用于生产**：生产机
   `82.156.0.68` 是 Intel Xeon Platinum 8255C（Cascade Lake，2 vCPU），`/proc/cpuinfo` 无
   `sha_ni`，实测单线程 SHA-256 吞吐 192–201 MB/s，据此推算单次校验的哈希下界 **≈ 3.5 s**，
   **每次 research 启动新增 ≈ 7 s CPU**；连同 unit 自身子进程的校验合计 ≈ 14 s。
   `rquant-artifact-retention.timer` 每 5 分钟一跳，按 288 次/天计新增 **≈ 34 min/天 CPU**。
   相关 unit 的 `TimeoutStartSec` 在 30–300 s，余量充足，但**首次部署后必须实测确认**：
   `systemd-analyze blame`，或
   `systemctl show -p ExecMainStartTimestamp,ActiveEnterTimestamp rquant-artifact-retention.service`，
   并观察一整个交易日的 1 分钟 load。若单次启动超过 30 s 或 load 明显抬升，按上面的回滚顺序
   先回滚 arbiter。

   **优化债务**：同一代 generation 在同一次启动里被校验两次，且每次都是全量逐文件 SHA-256。
   按 generation 缓存校验结果（例如以 generation id + full-manifest 摘要为键，把结果记在
   root-owned 的一次性文件里）可以把 ≈ 7 s 降到一次校验的量级。本轮不做，记为债务。

18. **root 权威链发布器（PR-A / TP1，S1 §1.3、§9）**。`/etc/rquant/production-runtime-profile.json`、
   `/var/lib/rquant/runtime-authority/current.json` 与 `generations/<id>/` 三样 root 持有产物从此
   有了仓库内的产生者，分两段、两种身份：

   ```bash
   # ① lighthouse：从 checkout 造 staging + plan.json（无特权，不碰 root 路径，不需要 .env）
   #   两种入口等价：`uv run rquant runtime-authority-stage …` 与下面的模块入口都可以，无 .env 的临时
   #   worktree 也一样——#189 修好后 `rquant.logging` 不再在 import 期构造 Settings
   uv run python -m rquant.runtime_authority_stage --bootstrap-from-checkout \
     --checkout-root /home/lighthouse/rquant-v0300 --commit <40 hex，须等于 HEAD> \
     --runtime-pyz /usr/local/libexec/rquant-runtime-exec.pyz \
     --deploy-pyz  /usr/local/libexec/rquant-production-deploy.pyz \
     --system-python /usr/bin/python3.11 --venv-source /home/lighthouse/rquant-v0300/.venv \
     --staging /home/lighthouse/rquant/var/authority-staging/first          # 先 dry-run：stdout 是 plan.json
   # 再加 --apply 落盘，stderr 打印 plan.json 的 sha256，人工抄给 ②
   # ② root：收进 inbox、逐文件重算哈希、装 profile、原子换代、wrapper 对 32 个 (role, instance) 预检、写 current.json
   sudo /usr/bin/python3.11 -I -S /usr/local/libexec/rquant-production-deploy.pyz publish \
     --staging /home/lighthouse/rquant/var/authority-staging/first --expect-plan-sha256 <①抄下的值> [--dry-run]
   ```

   `rquant-production-deploy.pyz` 由 `python scripts/build-production-deploy-pyz.py --repository-root . --output <path>`
   构建（stdlib-only 运行面，连续两次构建逐字节相同，stdout 打印 sha256），装到 `/usr/local/libexec/`
   `root:root 0555`。root 侧一律以 `-I -S` 运行它（与 26 个 unit 的 `ExecStart` 同一口径），`rollback`
   同理：`sudo /usr/bin/python3.11 -I -S /usr/local/libexec/rquant-production-deploy.pyz rollback --operation-id <32 hex>`。发布器不加任何 sudoers 条目（U-5），`publish` 走 owner 交互式 sudo。

   **计数补记**：跑 `rquant-runtime-exec.pyz` 的 unit 是 **26** 个（不是早先写的 25）；其中既有
   unit 改动的是 **19** 个（此 19 是「改了 ExecStart 的既有 unit」计数，与下面的第一关启用集合无关）。

   **第一关只启用 16/26 个 protected unit**（S1 §10.6，`acceptance-pra.md` §5）：
   `rquant-lab-claim-finalizer.service`、`rquant-runtime-artifact-catalog@.service`、
   `rquant-runtime-auction-universe@.service`、`rquant-runtime-candidate@.service`、
   `rquant-runtime-daily-orchestrator@.service`、`rquant-runtime-feature@.service`、
   `rquant-runtime-lab-jobs@.service`、`rquant-runtime-paper-broker@.service`、
   `rquant-runtime-paper-constraint@.service`、`rquant-runtime-promotions@.service`、
   `rquant-runtime-runtime-health@.service`、`rquant-runtime-serving@.service`、
   `rquant-runtime-shadow@.service`、`rquant-runtime-signal-router@.service`、
   `rquant-runtime-strategy@.service`、`rquant-runtime-watchlist-quote@.service`。
   推迟 **10** 个，预期状态一律「**未启用**」而不是 failed：credstore 依赖 7 个
   （`rquant-artifact-retention` / `rquant-runtime-auction-match@` / `rquant-runtime-daily-close@` /
   `rquant-runtime-market-minute@` / `rquant-runtime-notifier@` / `rquant-runtime-reference-slow-publisher@` /
   `rquant-runtime-reference-slow-source@`，等路线 A 的 `_seal_runtime_credentials`）、
   硬依赖旧链路 `data/runtime/current` 3 个（`rquant-page-control` / `rquant-runtime-recovery@` /
   `rquant-runtime-recovery-rehearsal@`）。**不承诺 26 全绿，不承诺页面有数据**（serving 的六个
   owner 里 `notifier` 与 `reference_slow_publisher` 都在推迟组）。`rquant-runtime-strategy@`
   在启用名单内，但按 U-12 裁决它在无 legacy `data/runtime/current` 时硬失败（PA-1 D-3，协调者已裁定：
   **failed 是设计**，不是事故；路线 A 产出 `current` 之后它仍受 #187 阻塞）。
   **「failed 是设计」这句到路线 A 为止**：2026-09-07 装机时 `current` 已经在了，`strategy_live`
   仍然三个全失败，报的是 `completion signer profile contains invalid manifests`（#218 A，代码缺陷）。
   #218 修好之后，`strategy_live` 在路线 A 下按前置第 28 条的顺序起两轮就进服务循环，不再有
   「预期 failed」这一档。

   **第一关放行判据（定稿，与 runbook 附录 S 一致）**：15 个 kind-backed / oneshot unit `active`
   （degraded：心跳带 `runtime_root_unavailable`，WARNING 日志可见）+ `rquant-runtime-strategy@` failed
   （~~设计~~，#218 A 之后作废，见上一段）+ 10 个「未启用」。**首次启动前必须由运维按各 unit 的 `ReadWritePaths=` 预建目录**（见 issue #192
   与 runbook v2 的 C-1 步骤）：这 16 个 unit 的 `ReadWritePaths=` 现在每条都带 `-` 前缀（#192 的一半已修），
   路径缺失不再让 systemd 在挂载命名空间搭建阶段以 `226/NAMESPACE` 失败。**但预建目录这一步不能省**：
   这些 unit 带 `ProtectSystem=strict` + `ProtectHome=read-only`，被 `-` 忽略掉的路径不会变成可写挂载点，
   在命名空间里仍然是只读的，`runtime_service_control.py` 的 `mkdir(mode=0o700, parents=True)` 跑到那里会
   拿到 `EROFS`。前缀的真实收益是把「systemd 层 `226/NAMESPACE`、stderr 一片空白」换成 wrapper / role 层
   读得到的失败，排障时能分清是目录没建还是权威链没装。所以**「`data/runtime` 由首个 role 自建」在首次
   安装场景仍然不成立**（稳态重启才成立）。预建时**绝不能顺带创建 `data/runtime/current`**，否则 15 个
   kind-backed role 会从 degraded 变成硬失败（PA-1 M-R1 的事故形态）。`ReadWritePaths=` 的 `-` 前缀与
   25/26 个 unit 缺 `[Install]` 段一并记在 #192（前缀已补，`[Install]` 待 owner 决定）。`stage --bootstrap-from-checkout` 全程不创建这些目录（A17）。
   策略表给 22 个 kind-backed role 加 `--authority-runtime` 使 `profile_id` 改变（TCB-2），首次发布没有
   prior，不需要同步换代任何前代产物。

   **首次发布没有 prior，回滚只有一条路**：停掉已启用的 unit，`sudo rm -f
   /var/lib/rquant/runtime-authority/current.json`（U-4）。第二代起 `rquant-production-deploy.pyz
   rollback --operation-id <新 32 hex>` 做单级回滚。旧 generation 目录内容寻址，永不删除。

   **已知限制**：`publish_runtime_authority` 用**已安装**的 profile 校验每一条 record，所以
   「已有 record 时换 profile」不是现有原语支持的转换——发布器在动任何 root 路径之前就拒绝
   （`not a supported transition`）。任何会改 `profile_id` 的改动（策略表、环境白名单、实例标签）
   在第二代起都需要先补这条转换（另开 issue）。

**回滚**：本分支没有产生任何生产变更，因此没有回滚基线。PR 未合并前直接关闭 PR 即可；
已合并但未部署时，生产仍停在上一次部署的 commit，无需任何动作。

---

## 2026-08-04 · v0.28.3 · 爆量历史搜索与触发日趋势标记

**状态**：PR #151 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag
`v0.28.3` 精确指向 `e4e303b0a4c05d2a4deefbee502718053672fe6f`。受控发布器从
`a637bb62b9efe8c2b9c915466ec086e6f0ba912a` 快进发布，结果为 `deployed`；无 schema、
systemd、nginx 或密钥变更。

**部署内容**：爆量记录支持跨交易日按标的搜索全部触发记录；从当天记录可打开当日附近的
趋势图，并在图上标记每一次触发时点。发布前修复 2026-08-04 监控池实际 15 只标的的
production `minute_bar` 缺口，并刷新只读副本。

**发布与验收**：修复后 preflight 为 `ok=5 warn=0 fail=0 skip=0`；生产健康端点返回
HTTP 200。真实旧历史样本如缺少 `minute_bar`，趋势图会如实显示“暂不可用”；生产真实历史
趋势尚未完成全量数据验收，不将该部分表述为已全绿。

**回滚基线**：`a637bb62b9efe8c2b9c915466ec086e6f0ba912a`。如需执行受控代码回滚，使用
`bash scripts/deploy-production.sh --target a637bb62b9efe8c2b9c915466ec086e6f0ba912a`；
不得盲目拉取 main 或直接覆盖生产数据。

---

## 2026-08-04 · v0.27.2 · 爆量近五日推送次数收尾记录

**状态**：PR #143 的 CI run `30512484263`（Python 3.11/3.12）双绿后 squash merge；
merge SHA 为 `1ec9bac2fa86c9fab3625be980923e8831f7804b`，annotated tag `v0.27.2`
精确指向该 SHA。**本版本未单独部署**：受控发布自动化执行前，生产已经快进至
`v0.28.1`，其 HEAD `a637bb62b9efe8c2b9c915466ec086e6f0ba912a` 包含该 merge 的功能；
此处仅记录后继版本中的生产收尾验收，不将其表述为 v0.27.2 部署成功。

**生产只读验收**：生产 HEAD `a637bb6` 包含目标 SHA，tracked worktree clean；
`rquant-surge-watch` 当日正常退出。由于截至 8 月 4 日的近五交易日没有足够跨日样本，
按时间倒序回推 2 个交易日至窗口 `2026-07-27..2026-07-31`：`300673.SZ` 在 7 月
29 日和 30 日出现，近五日计数为 2，但不进入 7 月 31 日的 `pushed_today`；7 月 31 日
最新真实记录 `300063.SZ` 经离线正文渲染包含“近5日推送次数：1”。验收只调用纯历史
加载与渲染路径，`rquant.notify` 未加载，未发送 Push。

**变更边界**：无 schema、systemd、生产数据、nginx 或密钥变更。

**回滚**：只可对 PR #143 创建 revert PR，合并后打新的 SemVer tag 并向前发布；禁止
直接回退生产 tag 或将本收尾记录当作已完成的独立部署。

---

## 2026-07-30 · v0.28.1 · 全景页冷启动兜底与爆量历史回看

**状态**：PR #148 经 CI 全绿后 squash merge；annotated tag `v0.28.1` 指向
`a637bb62b9efe8c2b9c915466ec086e6f0ba912a`。19:49 通过
`scripts/deploy-production.sh --target v0.28.1` 从 v0.28.0 快进发布，部署器返回
`deployed`，五个长驻服务重启后 active。无 schema、systemd、nginx 或密钥变更。

**背景（当日事故排查）**：v0.28.0 收盘后部署重启暴露既有冷启动缺口——poller
内存快照被清、收盘后 surge feed 停更超 120s、东财/新浪对云端 IP 间歇性拒绝，
页面卡在等首拉全空（连爆量记录 tab 都被 rerun 等待挡住）。本版修复：所有活路由
失败且 slot 为空时，从自家 `panorama_live` drop（优先，含原始 as_of）或陈旧 surge
feed 恢复最后一份快照，`age_seconds` 按数据真实时间回算，⚠️ 陈旧标注如实触发。

**新增**：爆量记录 tab 日期选择器（默认今天，可回看云端留存的历史 events，当前约
15 天）。

**部署插曲**：当晚 19:00-19:30 三次部署被 preflight 正确拦下并自动回滚（只读副本
陈旧超 12 分钟阈值——副本同步 timer 每日 17:30 后停到次日 09:00，叠加 461 日历史
回补批任务持写锁）。确认副本同步脚本与活跃写者本就并行安全（盘中 monitor 持锁时
每 5 分钟照常同步）后，手动跑 `scripts/sync-readonly-replica.sh` 刷新副本，preflight
通过后发布成功。回补批任务全程未受影响。

**验证**：Playwright 带网关 cookie 实测——页面出数（快照路由新浪、数据 0 秒前）、
爆量记录 tab 日期选择器渲染正常（当日 54 条记录）、console 0 errors。冷启动兜底
路径本次未触发（新浪路由恰好可用），由 26 条 poller 单测覆盖，待下次全路由失败时
实战验证。runtime_config 动态口径页脚将于次日 09:25 surge-watch 首启后生效。

**回滚**：`bash scripts/deploy-production.sh --target v0.28.0`（自动回滚基线
`3b9656056452e12393fbb4f86e4cb23c793a725b`）。

---

## 2026-07-30 · v0.28.0 · 全景页爆量图表与脉搏异动

**状态**：PR #142 经 Python 3.11/3.12 CI 全绿后 squash merge（期间与 #143/#144 合并
解冲突，仅版本号一处，保留 0.28.0）；annotated tag `v0.28.0` 精确指向
`3b9656056452e12393fbb4f86e4cb23c793a725b`。15:12 在交易保护窗口外通过
`scripts/deploy-production.sh --target v0.28.0` 从 `v0.27.1`
（`f7c3105e3043d873185674460a6a4358a2599956`，含 #138-#144 的 main 快进）发布，
部署器返回 `deployed`；无 schema、systemd、nginx 或密钥变更。

**内容**：① 爆量记录 tab 行选择联动个股图表，分时/5日图橙色虚线标注每日首次爆量
确认时刻；② 脉搏历史由 surge-watch 每分钟落 `surge_live/pulse-*.jsonl`（新模块
`pulse_watch.py`），📈 浮层改四张分面小图；③ 四类脉搏异动（涨停潮/炸板潮/跌停潮/
涨跌占比突变，10 分钟滑窗 + 30 分钟冷却）触发页面提示条 + PushDeer `pulse_alert`
场景（仅 PushDeer）；④ surge-watch 启动落 `runtime_config.json`，爆量记录页脚动态
显示检测口径；⑤ 分时/5日量柱按分钟涨跌 tick-rule 近似红绿上色。

**配置变更**：部署后云端 `.env` 追加 `RQUANT_SURGE_BOARDS=all`（爆量检测范围
从创业+科创扩至全市场，次日 09:25 surge-watch 启动生效）。排查结论：东百集团
600693.SH 等主板肉眼爆量此前不进台账的根因即检测范围默认仅 gem/star。

**验证**：合并树全量单测 2424 passed（8 条为 backup/replica 本地环境性 pre-existing
失败）；Playwright e2e 10 项 checklist 全过（含点行出图见标记与量柱色）；部署后
五个长驻服务重启均 active，28080 网关健康 200、未登录 302 跳转正常，服务日志无异常。

**回滚**：纯代码 + 单行 .env 变更，无数据写入。回滚命令
`bash scripts/deploy-production.sh --target v0.27.1`（自动回滚基线
`f7c3105e3043d873185674460a6a4358a2599956`）；如需同时撤销检测范围全开，删除云端
`.env` 中 `RQUANT_SURGE_BOARDS=all` 一行即可（次日生效）。

---

## 2026-07-27 · v0.27.1 · 完整历史迁云与爆量事件分钟线解耦

**状态**：PR #137 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag
`v0.27.1` 精确指向 `f7c3105e3043d873185674460a6a4358a2599956`。22:24 在交易保护
窗口外通过 `scripts/deploy-production.sh --target v0.27.1` 从 `v0.27.0`
（`5bb641ab23efa9595100070ff77282e18c14d170`）快进发布，部署器返回 `deployed`；没有
schema、systemd、nginx 或密钥变更。

**历史迁移与新口径**：先把 Mac 主库制作成不可变 zstd 归档并在云端校验源文件 SHA-256，
再按月提交补入 `daily_bar` 4,750,817 行（2020-08-24 至 2024-08-30）和
`minute_bar` 19,399,449 行（2025-03-28 至 2026-07-27）。随后按 12 份爆量事件文件补齐
156 个缺口 code-day、37,596 根分钟线；最终 161/161 个 `confirmed` 或 `unbuyable`
事件 code-day 都有一条日线和规范的 241 根分钟线。`research-ingest` 自本版本起把当日
爆量事件代码并入盘前 Pool 1/2 分钟采集集合，后续留存不再依赖 Mac 本地 monitor。

**验证与备份**：完整本地归档与云端主库再次反连接，日线和分钟线剩余缺口均为 0；云端
主库与只读副本分别通过 161/161 事件覆盖验收。最终主库 7,553,429,504 字节、52 张表，
只读副本已刷新；压缩备份 2,314,254,589 字节，`verified=true`、源延迟 0，`gzip -t`
通过，SHA-256 为 `ffc2aa3def63a65b9e801866b600aa8c0c7f7d2709aae27c0039e1d87e23efda`。
生产包版本为 `0.27.1`，tracked 工作区干净，发布审计无错误；五个活跃长驻服务重启后
均为 active，脉搏、午间报告、研究增量、副本同步和备份 timer 均为 enabled/active。

**本地清理**：云端归档、主库、副本和最终备份全部验收后，删除 Mac 的
5,336,477,696 字节主库及 2,055,209,122 字节上传暂存压缩包；本地 `data/` 由约 5GB
降至 76MB。相关本地 monitor、旧 daily、云同步、脉搏和午间报告 LaunchAgent 保持
disabled，避免重新生成生产数据或重复推送。

**回滚**：本次含生产数据增补，禁止回滚或覆盖云端数据库。若新的事件并集读取导致
`research-ingest` fail closed，先保留当日事件 JSONL 和研究发布证据，再创建 revert PR、
打更高 SemVer tag 并通过受控发布器向前发布；代码发布器自动回滚基线为
`5bb641ab23efa9595100070ff77282e18c14d170`，不得手工 reset 生产仓库。

---

## 2026-07-27 · v0.26.9 · 爆量与脉搏 Push 移动端格式优化

**状态**：爆量格式 PR #133（CI run `30232003418`）与脉搏格式 PR #134（CI run
`30233029001`）均经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag `v0.26.9`
精确指向 `c7b38f0d6177ac35fc87de1c2c58ec51e5629241`，其父提交是包含爆量格式的
`182f0b590313368d306f07a1f2bec18750150370`。15:10-15:11 在交易保护窗口外使用
`scripts/deploy-production.sh --target v0.26.9` 从 `v0.26.7`
（`9bb5235a8a2fd1d4d874a2c71858e99acb58f9fe`）快进发布，部署器返回 `deployed`；
没有 schema、systemd、生产数据或密钥变更。

**变更内容**：爆量 Push 改为每个标的一组，题材、涨幅/涨停空间、累计比/累计额和分钟方向
逐行展示；30 分钟脉搏改为“市场温度 / 新晋涨停 / 题材热度 / 放量异动”四个 Markdown
分节，股票与题材逐项展示。判定口径、去重、数量上限、通知路由和调度时间均未改变。

**验证与生产验收**：

- 本地实际 launchd 运行 worktree 用提交 `089cbe1` 仅覆盖脉搏渲染器与对应测试；47 项
  midday/notify 聚焦测试、Ruff 和差异检查通过。根 `.venv` 已确认导入该 worktree，
  `morning-pulse` 与 `midday-report` 均保持已加载、收盘后不运行、上次退出码 0。
- 生产 HEAD、tag 与包版本分别为上述精确 SHA、`v0.26.9`、`0.26.9`，工作树干净；纯
  `render_pulse` 样例输出四个分节。发布器重启的 canvas、dashboard、nl-screen、
  panorama-auth 和 panorama 五个服务均为 `active`。
- monitor 与 surge-watch 收盘后保持 `inactive`，两个 timer 均等待下一交易日 09:25；
  未盘后补跑监控或发送测试 Push。

**回滚**：本版本只有通知文本渲染变化。若移动端展示异常，创建针对 PR #133/#134 的
revert PR，合并后打更高 SemVer tag，并通过受控部署器向前发布；禁止生产机直接回退旧 tag。

---

## 2026-07-24 · v0.26.7 · Growth 固定回放停牌证据绑定修复

**状态**：PR #131 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag
`v0.26.7` 精确指向 `9bb5235a8a2fd1d4d874a2c71858e99acb58f9fe`。07:38 在交易保护
窗口前完成 dry-run 和正式发布，生产 tag、HEAD 与目标 SHA 精确匹配；发布器从
`v0.26.6` 的 `92c9308cafdf9a24271239d7490d6437471ba01a` 快进部署，没有 schema、systemd
或生产数据迁移。

**修复内容**：Growth Stage 1 执行依赖由 `stage1-v1` 升级为 `stage1-v2`。snapshot
builder 在生产源库中使用完整历史日线、分钟线、停牌事件与覆盖版本，先物化小型
`stock_suspend_session_evidence`，再把它绑定进不可变 execution snapshot。正式回放不再
访问未声明的 `stock_suspend_event`，也不会因按回测日期裁掉历史冲突而把未知停牌误判为
整日停牌；无法还原旧 `as_of` 覆盖版本时明确 fail closed。

**验证与生产验收**：

- 本地聚焦测试 66 项、全量测试 2,319 项通过；ruff、锁文件和差异检查通过。独立语义
  审查重放历史冲突、旧版本 fail-closed、空工件与 binding 身份反例后结论为通过。
- 发布前 dry-run 只包含 9 个 Growth 快照契约、实现、测试和版本文件，目标 SHA 精确；
  正式发布状态为 `deployed`。发布后两次 preflight 均为
  `ok=5 warn=0 fail=0 skip=0`，28 个 unit 全部 verify。
- 发布后备份 07:38:56 开始、07:42:06 成功结束，`Result=success`、
  `ExecMainStatus=0`；随后主动刷新只读副本成功，第二次 preflight 显示副本年龄 0 分钟、
  主副本工件延迟 0 分钟。
- `daily_bar` 最新为 2026-07-23、1,667,446 行；`minute_bar` 最新为
  2026-07-23 14:59、47,549,142 行。daily、monitor、surge-watch、replica 与 backup
  timers 均为 `enabled/active`，monitor 与 surge-watch 下一次触发为当日 09:25。

**Growth Stage 1 后续**：使用独立临时状态库完成了 `v0.26.7` 只读 planner，未写生产
manifest。范围为 2026-04-01 至 2026-07-09，资格记录 22,879 条；baseline 覆盖率
99.9829%，entry/exit 覆盖率 99.9146%；剩余 132 个任务、预计 136,406 行。planner
实耗 1,989.97 秒，若开盘前再向生产状态库重算并继续 snapshot、审计和固定回放，存在跨越
09:10 硬截止的风险，因此正式写入延至交易保护窗口后执行，不将临时 manifest
`2c9bd7b023316c11f40cf8768e2de9e9d9f53d81abc3764ae47a24ac1b9ae58e` 冒充生产证据。

**回滚**：本版本没有 schema 或业务数据写入。若发现 Growth snapshot 语义异常，创建
revert PR、合并后打更高 SemVer tag 并向前发布；禁止生产机直接回退旧 tag 或修改已发布
binding。紧急情况下只停止新的 Stage 1 研究任务，不停止 monitor、daily 和数据采集链路。

---

## 2026-07-23 · v0.26.4 · 成长板 Stage 1 规划内存止损

**补录说明**：本条补录 PR #128 已核验的历史发布与生产审计事实，不是新的部署或重复执行部署。

**状态**：PR #127 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag
`v0.26.4` 精确指向 `b68c37619a90a049b2170866a3e5e86f710857d7`。04:12 在交易保护
窗口外通过受控发布器从 `v0.26.3` 部署，未修改 schema 或生产业务数据。

**修复内容**：

- 成长板开盘结构分类按目标日稳定分批，资格解析与覆盖核对复用同一份结构事实，不再重复
  执行全范围分类。
- `backfill-plan` 的独立 DuckDB 连接默认限制为 2048 MB、2 线程，并使用命令级临时
  spill 目录；异常退出后自动清理。
- 前一交易日 MA5/10/20/60 完整时，候选解析只读取当日收盘；仅为缺少任一均线的代码
  回退读取 60 个交易日日线。混合样本测试锁定 fallback 不扫描完整均线代码。

**验证**：

- 本地全量 2,306 项测试全部通过；受限沙箱内先通过 2,299 项，另外 7 项端口绑定与
  `ps` 权限测试在放宽对应本机权限后通过。Ruff 与差异检查通过。
- GitHub Actions `test (3.11)`、`test (3.12)` 分别用时 6 分 44 秒、6 分 34 秒并通过。
- 修复前成长板 planner 单进程约 5.9 GiB 后被内核 OOM。生产只读隔离基准运行
  43 分 32 秒、峰值 1,901,112 KiB，未新增 OOM；随后按用户指令以 SIGTERM 终止，
  未输出最终 planner JSON，也未写入生产 manifest。该结果只证明内存止损有效，不代表
  Stage 1 或完整耗时验收通过。
- 部署后版本为 `v0.26.4` 且 HEAD 精确匹配 tag；preflight `ok=5 warn=0 fail=0 skip=0`，
  28 个 systemd unit 验证通过。主库与只读副本代际差为 0 分钟；日线最新
  `2026-07-22`、分钟最新 `2026-07-22 14:59`。Dashboard、NL Screen、Panorama 正常
  running；monitor/surge-watch 在盘前保持 inactive，六个关键 timers 全部 active。
- 按“不要再启动远端长任务”的明确要求，本次未手工启动备份或 planner；backup 与
  replica-sync timer 的下一次计划触发均为 2026-07-23 09:00。

**剩余门禁**：需要在另行允许的资源窗口完成新快速路径下的 growth `backfill-plan`，并以
新 manifest 依次通过 repair、snapshot、data audit 和 formal replay，取得
`comparable` 结果。完成前不得宣称成长板 Stage 1 已验收。

**回滚**：本版本无 schema 和业务数据变更。部署失败由受控发布器自动回滚；成功发布后的
回退必须对 `b68c376` 创建 revert PR、合并为新的 main 提交、打新 SemVer tag 后向前发布，
禁止在生产机使用 `git reset` 或直接覆盖 DuckDB。

---

## 2026-07-21 · v0.25.4 · 爆量累计器跨日冻结修复

**状态**：PR #119 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag
`v0.25.4` 精确指向 `0909fa3135c6f6ce42c9ced05040e1c47f6cc730`。16:26-16:27 在
交易保护窗口外完成 dry-run 和正式发布，生产 tag、HEAD 与包版本均精确匹配。

**事故与修复**：09:25 启动时，`rt_min` 在当日首根形成前返回上一交易日 15:00 末根；
原累计器只比较时分，导致今日全部分钟被判为时间回退。2026-07-21 生产服务虽然完整运行
09:25-15:02，247 次全市场请求全部成功，但创业/科创 2,010 只股票的 241 点累计额全部
整日零变化；确认层仅在 09:31-09:34 拉取 6 只候选，之后没有新候选。新累计器绑定交易日，
非当日分钟不再写入累计或分钟锚点；同分钟去重、乱序保护与同日重启 seed 保持不变。

**验证**：

- 回归测试先在旧实现得到预期红灯，再由修复转绿；`test_surge_watch.py` 87 项通过，
  surge/CLI 聚焦测试 90 项通过，ruff、锁文件和差异检查通过。
- 241 分钟合成会话产生 241 个不同累计值，首分钟 100、收盘 24,100；云端生产包以
  “昨日 15:00 → 今日 09:30 → 09:31”重放得到 `[0, 100, 160]`，退出码 0。
- 生产 preflight 为 `ok=5 warn=0 fail=0 skip=0`；monitor 与 surge-watch 保持收盘后的
  `inactive/dead`，未盘后补跑。两个 timer 均为 `enabled/active/waiting`，下一次触发为
  2026-07-22 09:25；daily、replica 与 backup timers 正常。

**影响与回滚**：本版本没有 schema 或业务数据迁移。2026-07-21 的 4 条爆量事件来自冻结
粗筛，不能用于评价正常策略效果，保留原记录作事故证据，不改写历史。若明日累计序列仍不
增长，应先停止 `rquant-surge-watch.timer/service` 并保留 snapshot、series、events 和
journal；代码回退必须创建 revert PR、打更高 SemVer tag 后向前发布，禁止生产机直接
`git reset`。

---

## 2026-07-21 · v0.25.3 · 爆量方向与内外盘确认修复

**状态**：PR #117 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag
`v0.25.3` 精确指向 `f85b8d8cddb83b0fc65e48a31eb50f693f635049`。15:19 在交易保护
窗口外完成 dry-run 和正式发布，生产 tag、HEAD 与包版本分别为 `v0.25.3`、上述精确 SHA
和 `0.25.3`。

**修复内容**：

- surge-watch 在发送前重新确认当前涨跌方向，要求精确分钟覆盖决策时点、当前一分钟收益为正，
  并以逐分钟 tick-rule 近似确认外盘主动买量大于内盘主动卖量；当前分钟数据缺失时延迟判断，
  不用后续分钟补看当时信号。
- 科创/创业放量历史回放修正了内外盘门槛和评分方向：由错误的 `内盘/外盘 > 1` 改为
  `内盘/外盘 < 1`，同时保留旧 CLI 参数作为兼容别名。既有相关回测结论已在分析文档标记为
  失效，等待用修正口径重跑，未把本次方向修复解释为收益已被证明。
- 严格按信号分钟重放 2026-07-21 上午 4 条实际 Push：300901.SZ、301007.SZ 因当分钟
  下跌或内盘占优被拒绝；300889.SZ、300203.SZ 在各自决策分钟仍满足方向条件。没有使用
  信号后的分钟判断信号当时是否成立。

**基线对齐与发布**：生产应用原停在 `v0.25.2`，PR #115 的 backup unit 已于 7 月 20 日
单独安装而 Git 基线未推进。发布器因此按设计拒绝跨越受保护文件。现场复核云端 unit、
`v0.25.3` 仓库文件的 SHA-256 均为
`9a8bb5c92a479bccb076d992d8e2d478b2aff9a6f7c37595c8d35d6cae764003`，
`systemd-analyze verify` 退出码 0、`TimeoutStartUSec=10min`。随后在部署锁内只快进到
PR #116 的精确 SHA `752bf66eae08a2faaac7c1823f7d766348b0c9fb`；该步只对齐已验收
unit、测试与文档，不改变运行时代码，并写入 `baseline_adopted` 审计。标准发布器再从该
基线发布 `v0.25.3`，变更清单不含受保护路径。

**生产验收**：

- preflight 为 `ok=5 warn=0 fail=0 skip=0`，28 个 unit 全部 verify；dashboard、panorama
  等发布前活跃服务完成白名单重启，monitor 与 surge-watch 正常保持收盘后的
  `inactive/dead`，没有盘后强制补跑。
- `rquant-surge-watch.timer` 与 `rquant-monitor.timer` 均为 `enabled/active/waiting`，下一次
  触发为 2026-07-22 09:25；daily、research-ingest、replica、backup 等共 11 个 timer
  均已恢复调度。
- 主库与只读副本摘要完全一致：`daily_bar=1,650,869`、
  `stock_status_daily=1,061,544`、`adj_factor=2,469,013`、`screen_result=856`、
  `minute_bar=46,993,701`，分钟最新为 2026-07-21 14:59，schema migration 为 v10。
- 发布后备份成功原子更新 `backup/latest.duckdb.gz`，`gzip -t` 退出码 0；主动副本同步
  `Result=success`、`ExecMainStatus=0`。验收期间短暂停止 backup timer 以消除连续触发
  竞态，未终止运行中的备份，检查后已恢复为 `enabled/active`。

**回滚**：本版本没有 schema 或业务数据迁移。发现方向过滤异常时必须创建 revert PR，
合并为新的 main 提交并打更高 SemVer tag 后向前发布；受控发布器禁止直接退回旧 tag，
也不得在生产机 `git reset`。紧急止住爆量 Push 时可在保留 monitor 的前提下先停止
`rquant-surge-watch.timer` 与当前 service，保留日志和事件作审计，再发布向前修复。

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
