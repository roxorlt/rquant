# v0.17.3 通知熔断基础设施发布

## 目的与拆分

`deploy/systemd/` 属于受保护基础设施，`deploy-production.sh` 会主动拒绝包含这类差异的
tag。因此发布分两段：先把 monitor 的 systemd 重启限流作为独立 PR 合并并人工安装；服务器
HEAD 快进到该基础设施提交后，再发布不含 privileged diff 的 v0.17.3 代码 tag。

本变更仅增加：

- `StartLimitIntervalSec=1800`
- `StartLimitBurst=3`

它不会改数据库、密钥、timer 时间或行情采集参数。

## 云端预检与安装

仅在非交易保护窗口执行：

```bash
set -Eeuo pipefail
cd /home/lighthouse/rquant

BACKUP=/etc/systemd/system/rquant-monitor.service.pre-v0173
restore_unit() {
  rc=$?
  trap - ERR
  set +e
  if [[ -f "${BACKUP}" ]]; then
    sudo install -m 0644 "${BACKUP}" /etc/systemd/system/rquant-monitor.service
    sudo systemctl daemon-reload
    sudo systemd-analyze verify /etc/systemd/system/rquant-monitor.service || true
  fi
  exit "${rc}"
}
trap restore_unit ERR

.venv/bin/rquant preflight
git fetch origin --tags
git merge --ff-only <INFRA_MERGE_SHA>

sudo systemd-analyze verify "${PWD}/deploy/systemd/rquant-monitor.service"
sudo cp -a /etc/systemd/system/rquant-monitor.service "${BACKUP}"
sudo install -m 0644 "${PWD}/deploy/systemd/rquant-monitor.service" \
  /etc/systemd/system/rquant-monitor.service
sudo systemctl daemon-reload
sudo systemd-analyze verify /etc/systemd/system/rquant-monitor.service

systemctl show rquant-monitor.service \
  --property=StartLimitIntervalUSec,StartLimitBurst,Restart,RestartUSec
.venv/bin/rquant preflight
trap - ERR
```

预期：verify 返回 0；限流窗口为 30 分钟、burst 为 3；preflight 全绿。盘后 monitor 应为
inactive/dead，次日仍由原 timer 在 09:25 拉起。

## 本地 LaunchAgent

本地 monitor 继续采集研究分钟，但不得再发 monitor/系统异常 Push：

```bash
cp deploy/com.roxor.rquant-monitor.plist \
  /Users/roxor/Library/LaunchAgents/com.roxor.rquant-monitor.plist
launchctl bootout gui/$(id -u)/com.roxor.rquant-monitor 2>/dev/null || true
launchctl bootstrap gui/$(id -u) \
  /Users/roxor/Library/LaunchAgents/com.roxor.rquant-monitor.plist
launchctl print gui/$(id -u)/com.roxor.rquant-monitor | grep NOTIFY_ENABLED
```

预期输出包含 `NOTIFY_ENABLED => false`。晨间脉搏和午间战报暂时保留，它们是独立报告，
不是 monitor 的重复告警；迁云阶段 D 再统一收口。

## 回滚

```bash
set -Eeuo pipefail
sudo install -m 0644 \
  /etc/systemd/system/rquant-monitor.service.pre-v0173 \
  /etc/systemd/system/rquant-monitor.service
sudo systemctl daemon-reload
sudo systemd-analyze verify /etc/systemd/system/rquant-monitor.service
```

代码发布若失败，由 `deploy-production.sh` 自动回滚到前一 SHA；基础设施回滚不依赖代码回滚。
