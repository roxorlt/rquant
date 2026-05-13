# 2026-05-13 事后复盘：5/1 daily-report 翻车 + 5/6 monitor 自愈失败

> 起因：今日（2026-05-13）回查 TODO 中挂着的「5/1 第一份 daily-report」「5/6 节后首日 monitor 跨午休」两个验证项时，发现 **两次都翻车**，且暴露出一个潜伏已久的更严重问题——`backup intraday` 自 v0.7.0 起从未真正运行过。

## TL;DR

| 编号 | 现象 | 根因 | 影响 | 状态 |
|---|---|---|---|---|
| Bug A | 5/1 15:30 daily-report `IOException: Could not set lock on file ... rquant.duckdb`，进程 fatal exit | daily-report 用默认写模式打开 DuckDB，撞上 nl-screen v0.12.1 hotfix 之前还在用写模式持锁的 PID 2597296 | 5/1 当日**没收到日报推送** | 待修（Bug A） |
| Bug B | 5/6 09:30-09:48 watchdog 死循环：每 2min 发"monitor 不在跑" alert + 试图 `systemctl start rquant-monitor.service` 但被 polkit 拒（`Interactive authentication required`） | watchdog.sh 调 `systemctl` **没加 `sudo` 前缀**，走 polkit 直接被拒——但 `/etc/sudoers` 早已 `lighthouse ALL=(ALL) NOPASSWD: ALL`，只需加 `sudo` 即可自愈 | 5/6 上午盘前 40 分钟 monitor 完全没跑（手机被告警刷屏），需用户人工 ssh 上去 reset-failed + start | 待修（Bug B） |
| Bug C | 盘中 5min intraday backup **自 v0.7.0（2026-04 月）起从未真正跑过**；v0.11.3 在 git 改对了 OnCalendar 语法，但**没部署到云端** | git 里 `deploy/systemd/rquant-backup.timer` 是修复后的 `9..15:0/5`，云端 `/etc/systemd/system/rquant-backup.timer` 仍是被 systemd 静默拒收的旧语法 `09:30..15:05/5`；之前缺少「修 unit → cloud 部署 → 验证 trigger 真的跑了」的闭环 | 本地热备 `latest.duckdb.gz` 每天只更新一次（17:30 日终），盘中数据落后最多 6 小时；本地 sync log 显示「sync OK: 215M」是**假阳性**（rsync 拉的内容跟昨天一致，sync 工具不报警） | 待修（Bug C） |

## 时间线

### 5/1（劳动节，节假日）
- 上午 ~04:54：dashboard `streamlit[2521436]` 收到一次 `Invalid HTTP request`（无关）
- ~13:49：dashboard websocket close 1001（无关）
- **15:30:35**：systemd timer 触发 `rquant-daily-report.service`，CLI 在 `DuckDBStore()` 初始化时立刻抛 `IOException`，进程 exit。PID **2597296** 此时持有 db 写锁
- 17:00:33：随后的 `rquant-daily` 流水线服务也撞同样的锁、同样 fatal
- 17:00:34：在错误处理路径里，`notify.api:_log_notification` 试图把这次"通知失败"事件写 `notification_log` 表，**又一次撞锁**——但只是 ERROR 不再 fatal

→ 没造成对外可见的故障升级，但 **5/1 没有日报、没有 daily pipeline**，且 TODO 项一直挂着等"5/1 干净跑结果"。

### 5/6（节后首个交易日）
- **09:30:13**：开盘 2 分钟后 watchdog 第一次触发，检测到 `rquant-monitor.service` 不在跑 → 发 PushDeer alert + 调 `systemctl start rquant-monitor.service` → **被 polkit 拒**（`Interactive authentication required`）
- 09:32, 09:34, 09:38, 09:42, 09:44, 09:48：每 2 分钟一次循环，连发 8 条 alert + 8 次失败启动
- **~09:50** 用户 ssh 上服务器手动恢复：reset-failed → start monitor，并把 nl-screen 改成 `read_only=True` 重启（这就是 v0.12.1 hotfix 内容）
- 09:50 之后 monitor 正常运行，watchdog 不再告警

→ 当日 monitor 上午盘前 ~40 分钟数据缺失，9:50 之后恢复正常。

### 5/13（今日，回查）
- 14:24 起回查 TODO 项的两个验证点
- 通过 journalctl 发现 5/1 + 5/6 双翻车证据
- 旁敲侧击发现 `~/rquant/backup/latest.duckdb.gz` mtime 卡在 5/12 17:30 → 进一步排查 → 云端 unit ≠ git unit → Bug C 浮出水面

## 根因分析（5 Whys）

### Bug A
1. **为什么 5/1 daily-report 翻车？**——DuckDB 写锁冲突
2. **为什么会冲突？**——daily-report 进程要写 `notification_log`，需要写模式打开；同时 nl-screen 旧版（hotfix 前）也是写模式
3. **为什么 nl-screen 用写模式？**——v0.12.0 上线时未约束 read-only 模式
4. **为什么 daily-report 必须写 `notification_log`？**——`notify.api._log_notification` 设计为副作用：每次推送结果落盘成审计记录
5. **为什么用 DuckDB 而不是更轻的存储？**——为了"业务表 + 元数据一站式"，但元数据流量低 + 跟业务表无 join 关系，本不该共享一个文件锁

→ 真正的根因是 **`notification_log` 不该放在与业务表同一个 DuckDB 文件里**。

### Bug B
1. **为什么 watchdog 死循环？**——`systemctl start` 失败
2. **为什么失败？**——`Interactive authentication required`（polkit 拒绝）
3. **为什么 polkit 拒绝？**——watchdog 以 `lighthouse` 身份直接调 systemctl，未走 sudo
4. **为什么没走 sudo？**——脚本写错了
5. **为什么写错了没在本地/staging 暴露？**——本地 macOS 没 systemd；staging 不存在；watchdog 这种"自愈失败"路径只在真正出问题时才走，从未演练

→ 真正的根因是 **缺少 chaos drill：从来没演练过 monitor 挂了 watchdog 能否拉起来**。

### Bug C
1. **为什么 intraday backup 从没真跑过？**——云端 timer 用的是被 systemd 静默拒收的语法
2. **为什么是被拒收的语法？**——v0.7.0 部署时这语法就错了，但当时没有验证机制（`systemd-analyze calendar`）
3. **为什么 v0.11.3 修了 git 但没修生产？**——deploy 流程没把 `deploy/systemd/*.{service,timer}` 自动同步到 `/etc/systemd/system/`，且没做 trigger 验证
4. **为什么没人发现？**——本地 sync 脚本每 5min 报「sync OK: 215M」，假阳性掩盖了真相（rsync 看到文件 size 一样就当 ok）
5. **为什么 sync 报假阳性？**——监控的是「sync 行为是否完成」而不是「源文件 mtime 是否在变化」

→ 真正的根因是 **缺少 trigger / mtime 端到端的验证链路**。修代码不等于修产线。

## 修复

### Bug A — `fix/daily-report-via-snapshot`

走方案 ③：**daily-report 不再打开活的 DuckDB**，改为从 `backup/latest.duckdb.gz` 解压成临时只读副本读。代码上：

- 新增 `DuckDBStoreFromSnapshot(snapshot_gz_path)` 包装器：解压到 `/tmp/rquant-snapshot-{pid}.duckdb` → `DuckDBStore(read_only=True)` 打开 → close 时 unlink
- `health.generate_and_send_daily_report` 改用 snapshot store
- 推送动作完成后**不再写 `notification_log`**（这张表当前没人在读；如果后续要审计，单独走 SQLite 文件，PR 另开）

⚠️ **强依赖 Bug C 先修好**，否则 snapshot 是昨天 17:30 的旧快照，daily-report 报的是错的数。

### Bug B — `fix/watchdog-self-heal-sudo`

`scripts/monitor-watchdog.sh`：
- `systemctl start rquant-monitor.service` → `sudo systemctl start rquant-monitor.service`
- `systemctl reset-failed` → `sudo systemctl reset-failed`
- 重启失败再发"升级告警"（区分一般 alert 和 systemctl 失败的告警）

不动 `/etc/sudoers`（lighthouse 已 NOPASSWD ALL）。

### Bug C — `fix/backup-timer-deploy-and-verify`

两步：

1. **立即热修**：ssh 上服务器
   ```bash
   sudo cp ~/rquant/deploy/systemd/rquant-backup.timer /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl restart rquant-backup.timer
   systemctl list-timers rquant-backup.timer  # 验证下次 trigger 在 5min 内
   ```
2. **结构性修复**：`scripts/deploy.sh` 加 systemd unit 自动 diff + sync + reload；并加 post-deploy 验证（list-timers 检查下次 trigger 是 5min 内不是 24h 后）

### Task #8 — 本地 sync 假阳性

`scripts/sync-from-cloud.sh`：
- 拉完 `latest.duckdb.gz` 后检查 mtime 跟上次比是否变化
- 不变化且在 sync window 内 → 报 `WARNING: source not changing, intraday may be down`
- 这一条 WARN 推 PushDeer 给 admin

## 教训 / 预防措施

| 教训 | 落地 |
|---|---|
| 「git 改了」≠「生产改了」 | `scripts/deploy.sh` 必须 diff + sync systemd unit；deploy 后必须验证 timer 真的在 5min 内 trigger |
| systemd OnCalendar 语法不能本地验，必须云端 `systemd-analyze` | CLAUDE.md 已写过（line 87-91），但 v0.7.0 部署时这条还没诞生。**遵守即可** |
| watchdog 的自愈路径必须演练 | 加 chaos drill：每月人工 kill 一次 monitor，看 watchdog 5min 内能否拉起 |
| 本地 sync 监控的是"sync 是否完成"，不是"上游是否变化" | 改 sync 脚本，监控 mtime 变化而不是 sync 是否报 OK |
| `notification_log` 写 DuckDB 共享文件锁 | 元数据 / 业务表分开存：业务用 DuckDB（单写多读），元数据用 SQLite 或 jsonl（多写） |
| **TODO 里挂"待观察"项必须有截止日期 + 主动回查机制** | 任何 "待观察" 项加 `verify_at: YYYY-MM-DD`，到期没回查则触发 PushDeer 提醒（未来加） |

## 关联

- 触发讨论 commit / 分支：`docs/may13-incident-todo-update`（worktree: `worktree-may13-incident-todo`）
- 修复 PR（计划）：
  - `fix/backup-timer-deploy-and-verify`（Bug C，最紧急，今日就该热修云端）
  - `fix/daily-report-via-snapshot`（Bug A，依赖 C）
  - `fix/watchdog-self-heal-sudo`（Bug B）
  - `chore/sync-stale-detection`（Task #8）
- 历史相关：v0.12.1 hotfix（同根问题首次暴露——nl-screen 抢锁）、v0.11.3（试图修 backup intraday 但没部署）
