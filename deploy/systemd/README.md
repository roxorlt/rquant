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

## 安装步骤

服务器跑：

```bash
# 1. 复制 unit 到系统目录
sudo cp ~/rquant/deploy/systemd/*.service /etc/systemd/system/
sudo cp ~/rquant/deploy/systemd/*.timer /etc/systemd/system/

# 2. 让 systemd 重读配置
sudo systemctl daemon-reload

# 3. 启用并启动 timers（service 由 timer 触发，不需要单独 enable）
sudo systemctl enable --now rquant-daily.timer
sudo systemctl enable --now rquant-monitor.timer
sudo systemctl enable --now rquant-morning-pulse.timer
sudo systemctl enable --now rquant-midday-report.timer

# 4. 验证
systemctl list-timers --no-pager | grep rquant
```

研究日增量必须按
[独立上线手册](../../docs/deploy/research-daily-ingest-rollout.md)先完成手工运行和候选验收，
不得随基础 unit 批量启用。

## 验证 + 测试

```bash
# 看 timer 状态
systemctl status rquant-daily.timer
systemctl status rquant-monitor.timer
systemctl status rquant-morning-pulse.timer
systemctl status rquant-midday-report.timer
systemctl status rquant-research-ingest.timer

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
