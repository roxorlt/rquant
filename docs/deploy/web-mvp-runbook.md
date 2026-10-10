# Web MVP 上线 Runbook（82.156.0.68）— 仅准备，未执行

范围：PR #324（`mvp/react-readonly`）。新增 `rquant-web`（FastAPI，127.0.0.1:8768）、nginx `/app/`（basic auth），
page-control 新命令 `ack_alert` / `add_watchlist_item`，Serving 新可选投影 `alert_ack` / `manual_watchlist`，
盯盘（`rquant-monitor`）开盘时读取 `manual_watchlist`。

## 0. 前置
- 非交易时段（15:30 之后或周末）操作；`rquant-monitor` 未运行。
- 记录回滚点：`cd /home/lighthouse/rquant && git rev-parse HEAD > /tmp/rquant-prev-commit`
- 备份 Serving 当前代指针与 page-control 状态目录：
  `tar czf /tmp/rquant-pre-web-$(date +%F).tgz data/runtime/serving/CURRENT* data/runtime/control`
  （以实际 Serving 指针文件名为准。）
- 按 `docs/production-release.md` 的流程准备 bundle（runtime-exec pyz、schema release snapshot），不要绕开 preflight。

## 1. 代码与依赖
```bash
cd /home/lighthouse/rquant
git fetch origin && git checkout <合并后的 main 提交>
uv sync --frozen            # 新增 fastapi / uvicorn / httpx
cd web && pnpm install --frozen-lockfile && pnpm build && cd ..   # 产物 web/dist（不入库）
```
Node 22 需要在服务器上可用（仅构建用；也可在本地/CI 构建后 rsync `web/dist/`）。

## 2. Schema 升级（Serving 投影）
- 变更：`PAGE_PROJECTION_CONTRACTS` 新增 `alert_ack`、`manual_watchlist`（均为 optional、只增不改），
  Serving 物理表指纹变为 `0f752f67…ac79e47`。
- 步骤：重新生成并签发 schema release snapshot（与 runtime bundle 同一提交），跑 deployment preflight，
  通过后再切换。旧代 Serving 不含这两张表：web 读为“未确认 / 无自选”，盯盘读为空清单，均不报错。
- 无 DuckDB 业务库 DDL 变更。

## 3. page-control + notifier 协同升级（必须同一批次）
新 page-control 写 `<page-control 状态目录>/alert_acks/acks.jsonl`、`watchlist/items.jsonl`；
signal 投影发布器（notifier 角色内）从同一目录读取并发布到 Serving。只升其一会导致：
旧 page-control 拒绝新命令（web 返回 502），或新 page-control 写了但无人发布。
```bash
sudo systemctl stop rquant-page-control.service 'rquant-runtime-notifier@*.service'
# 安装新 runtime-exec pyz + 新 instance 凭据（按 production-release.md）
sudo systemctl daemon-reload
sudo systemctl start rquant-page-control.service
sudo systemctl start rquant-runtime-notifier@<instance>.service
journalctl -u rquant-page-control -u 'rquant-runtime-notifier@*' -n 100 --no-pager
```
确认：page-control 状态目录在其 `ReadWritePaths` 内（`data/runtime/control`）；notifier 对该目录有读权限。

## 4. rquant-web systemd unit
`/etc/systemd/system/rquant-web.service`：
```ini
[Unit]
Description=rQuant Web MVP API (read-only + page-control forwarding)
After=network-online.target rquant-page-control.service
Wants=network-online.target

[Service]
Type=simple
User=lighthouse
Group=lighthouse
WorkingDirectory=/home/lighthouse/rquant
Environment=APP_ENV=prod RQUANT_DISABLE_DOTENV=1
ExecStart=/home/lighthouse/rquant/.venv/bin/python -m rquant.web serve --host 127.0.0.1 --port 8768
Restart=on-failure
RestartSec=5s
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
InaccessiblePaths=/home/lighthouse/rquant/.env -/home/lighthouse/rquant/data/runtime/current/secrets -/home/lighthouse/rquant/data/runtime/current/credentials
UMask=0077

[Install]
WantedBy=multi-user.target
```
只读 Serving、只经 127.0.0.1:8767 转发写命令，因此无需 ReadWritePaths。
```bash
sudo systemd-analyze verify /etc/systemd/system/rquant-web.service
sudo systemctl daemon-reload && sudo systemctl enable --now rquant-web
curl -fsS http://127.0.0.1:8768/api/v1/meta
```

## 5. nginx `/app/` + basic auth
```bash
sudo htpasswd -c /etc/nginx/rquant-app.htpasswd owner   # 需 apache2-utils
```
加入现有 server 块：
```nginx
location /app/api/ {
    auth_basic "rQuant"; auth_basic_user_file /etc/nginx/rquant-app.htpasswd;
    proxy_pass http://127.0.0.1:8768/api/;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-User $remote_user;
}
location /app/ {
    auth_basic "rQuant"; auth_basic_user_file /etc/nginx/rquant-app.htpasswd;
    alias /home/lighthouse/rquant/web/dist/;
    try_files $uri $uri/ /app/index.html;
}
```
`sudo nginx -t && sudo systemctl reload nginx`。保留现有 市场全景 Streamlit 的 location 不动。

## 6. 验收
1. 浏览器打开 `https://<域名>/app/`，无凭据 401，有凭据进入总览；8 个页面可打开。
2. 盯盘页点“确认”一条预警 → 下一次 Serving 发布后显示“已确认 · owner”。
3. 加一只自选 → `watchlist/items.jsonl` 新增一行 → Serving `manual_watchlist` 出现该代码。
4. 下一个交易日 9:25 盯盘日志出现 `Watchlist: … manual=N`。没有首板涨停实体的自选会被跳过并告警（已知限制）。

## 7. 回滚
```bash
sudo systemctl disable --now rquant-web
# nginx：删掉两个 /app/ location，nginx -t && reload
sudo systemctl stop rquant-page-control 'rquant-runtime-notifier@*'
git checkout $(cat /tmp/rquant-prev-commit) && uv sync --frozen
# 重新安装旧 runtime-exec pyz / schema release snapshot（旧 bundle），daemon-reload，再启动两服务
```
- JSONL 日志可保留（旧代码不读取）；旧 Serving 代仍可读。
- 若新代 Serving 已发布：旧读取方忽略多出的可选表；如 preflight 拒绝，按备份恢复 Serving 指针。
- 盯盘回滚后恢复为仅 Pool1/Pool2。
