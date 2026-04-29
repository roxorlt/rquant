# 本地热备同步（macOS）

本地 macOS 每小时从云端 `82.156.0.68` rsync 拉 DuckDB + Parquet 做热备，云端挂时可降级到本地继续跑。

## 文件

| 文件 | 作用 |
|------|------|
| `scripts/sync-from-cloud.sh` | rsync 拉脚本（业务时段跳过 + 失败 PushDeer 告警） |
| `deploy/com.roxor.rquant-sync.plist` | macOS launchd 配置（每小时触发 + 启动时跑一次） |

## 业务时段（脚本内部跳过，避免拉到 DuckDB 写入中）

- **09:30 – 15:00**：盘中 monitor 写入 `monitor_event` / `pool2_watch`
- **17:00 – 17:10**：每日流水线写入 `daily_bar` / `screen_result`

其他时段（晚上 / 夜间 / 周末）正常 rsync。

## 安装

```bash
# 1. 复制 plist 到 LaunchAgents
cp /Users/roxor/brain/30-projects/rQuant/deploy/com.roxor.rquant-sync.plist \
   ~/Library/LaunchAgents/

# 2. 加载（RunAtLoad=true，会立即跑一次）
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.roxor.rquant-sync.plist

# 3. 验证
launchctl list | grep rquant-sync
tail /Users/roxor/brain/30-projects/rQuant/logs/sync-from-cloud.log
```

## 验证 + 调试

```bash
# 手动跑一次（测试用）
/Users/roxor/brain/30-projects/rQuant/scripts/sync-from-cloud.sh

# 看最近日志
tail -50 /Users/roxor/brain/30-projects/rQuant/logs/sync-from-cloud.log

# 看 launchd 触发记录
log stream --predicate 'process == "com.roxor.rquant-sync"' --info
```

## 失败告警

rsync 重试 3 次（间隔 60s）仍失败时，脚本会通过 `.env` 中的 `PUSHDEER_KEYS` 推一条 PushDeer 告警：标题 `❌ rQuant 数据同步失败`，body 含最近 20 行日志。

## 卸载

```bash
launchctl bootout gui/$(id -u)/com.roxor.rquant-sync
rm ~/Library/LaunchAgents/com.roxor.rquant-sync.plist
```

## 降级路径（云端挂时）

云端不可用 → 立即在本地：

```bash
# 1. 停 sync（避免本地数据被云端 rsync 覆盖）
launchctl bootout gui/$(id -u)/com.roxor.rquant-sync

# 2. 启用本地 launchd（macOS 那套）作为生产
cp deploy/com.roxor.rquant.plist ~/Library/LaunchAgents/
cp deploy/com.roxor.rquant-monitor.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.roxor.rquant.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.roxor.rquant-monitor.plist
```

最差丢失最近 1 小时（上次 sync 之后到云端挂掉之间）的 monitor_event。Pool 2 / screen_result 状态都还在最近 sync 的快照里。
