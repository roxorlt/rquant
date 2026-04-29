# systemd 部署（Linux 服务器）

腾讯云 / VPS / 任何 systemd Linux 主机用。macOS 用 `../com.roxor.rquant*.plist` launchd 那套。

## 4 个 unit 文件

| 文件 | 作用 | 触发时间 |
|------|------|---------|
| `rquant-daily.service` | 跑 `rquant run-daily`（ingest + screen + Pool 2） | 由 timer 触发 |
| `rquant-daily.timer` | 工作日 17:00 触发 daily.service | Mon-Fri 17:00 |
| `rquant-monitor.service` | 跑 `rquant monitor`（盘中实时） | 由 timer 触发，自然退出在 15:00 后 |
| `rquant-monitor.timer` | 工作日 09:25 触发 monitor.service | Mon-Fri 09:25 |

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

# 4. 验证
systemctl list-timers --no-pager | grep rquant
```

## 验证 + 测试

```bash
# 看 timer 状态
systemctl status rquant-daily.timer
systemctl status rquant-monitor.timer

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

A 股节假日 systemd timer 不知道，会照常 09:25 / 17:00 触发。但应用层在非交易日内部退出：

- `monitor` 启动后 `is_trading_day(today)` 检查（akshare 交易日历），非交易日立即 return 0
- `run-daily` ingest 在非交易日 Tushare 返回 0 行，pipeline 跳过

所以节假日 timer 触发也不会出问题。

## 关闭/卸载

```bash
sudo systemctl disable --now rquant-daily.timer rquant-monitor.timer
sudo rm /etc/systemd/system/rquant-{daily,monitor}.{service,timer}
sudo systemctl daemon-reload
```
