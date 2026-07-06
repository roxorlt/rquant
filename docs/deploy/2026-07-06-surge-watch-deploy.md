# surge-watch 部署清单（pair 模式）

**日期**：2026-07-06　**分支**：feat/surge-watch → main（合 PR 后部署）
**目标**：云端 `82.156.0.68` 上线每分钟爆量推送 + P2（Mac panorama 读云端 feed）。

> Hybrid 分工：Claude 出命令，用户 ssh 上服务器粘贴执行并把输出贴回。**Claude 不
> 直接 ssh 操作生产**。所有路径用绝对路径。

---

## 0. 前置

- PR 已合 main，本地 `git checkout main && git pull`。
- 标定产物 `src/rquant/data/intraday_progress_curve.json` 已随代码进 git（云端 `git pull`
  自动带过去，surge-watch 启动即加载；缺失会线性兜底但精度差）。
- 确认云端 `.env` 有 `TUSHARE_TOKEN_MAIN`（stk_mins 确认层用，已有）。

---

## 1. 部署代码 + systemd unit（用户在云端跑）

```bash
cd /home/lighthouse/rquant
bash scripts/deploy.sh
```

`deploy.sh` 会：git pull → cp 新 unit（`rquant-surge-watch.service` + `.timer`）到
`/etc/systemd/system` → daemon-reload → `enable --now rquant-surge-watch.timer` →
post-check 验证 timer NEXT trigger 在 1 年内。

**核对输出**：`[5/6]` 步应打印
`rquant-surge-watch.timer: 下次 trigger 2026-07-0X 09:25:00（...s 后）`。若为 `n/a`
说明 OnCalendar 被拒收，见第 2 步单独验证。

---

## 2. 单独验证 timer OnCalendar 语法（用户在云端跑）

```bash
systemd-analyze calendar 'Mon..Fri *-*-* 09:25:00' --iterations 5
```

**期望**：5 个 Iteration 均为工作日 09:25:00，相邻间隔为「下一交易日」（跨周末跳到周一）。
这是固定时刻语法（非分钟步进），历史上安全。

---

## 3. 手动冒烟（盘后，用户在云端跑）

盘后用 `--force-session` 忽略时段守卫，`--dry-run` 不推送，`--max-ticks` 限 3 轮：

```bash
cd /home/lighthouse/rquant
.venv/bin/rquant surge-watch --dry-run --force-session --max-ticks 3
```

**期望**：日志有 `surge-watch 启动 ... boards=('gem', 'star')`，每轮拉创业/科创快照
（route=em_direct），无异常，3 轮后退出。若打印了 `[DRY-RUN] 爆量 ...` 报文说明
盘后残留数据也能触发（正常）。产物落 `data/surge_live/`（parquet + events jsonl）。

---

## 4. nginx feed location（P2，用户在云端跑）

`deploy/nginx/rquant-backup.conf` 已含 `/feed/` location（8081 站点，复用
`.rquant-backup.htpasswd`）。同步到宝塔 vhost 目录并 reload：

```bash
sudo cp /home/lighthouse/rquant/deploy/nginx/rquant-backup.conf \
    /www/server/panel/vhost/nginx/rquant-backup.conf
sudo nginx -t && sudo nginx -s reload
```

location 块内容（供核对）：

```nginx
location /feed/ {
    alias /home/lighthouse/rquant/data/surge_live/;
    autoindex off;
    auth_basic "rQuant Feed";
    auth_basic_user_file /www/server/nginx/conf/.rquant-backup.htpasswd;
    if ($request_filename !~* \.parquet$) { return 403; }
    add_header Cache-Control "no-store" always;
}
```

**验证 feed 可访问**（盘中 surge-watch 跑起来后才有 `snapshot_full.parquet`）：

```bash
# 200 + Last-Modified 头（Mac 侧据此判新鲜度）
curl -sI -u '<backup用户>:<密码>' \
    http://82.156.0.68:8081/feed/snapshot_full.parquet | head -5
```

---

## 5. .env 新增项

### 5a. 云端 `.env`（可选，扩板块时才加）

```dotenv
# surge-watch 检测板块（缺省创业+科创）；可 all / main / gem,star
# RQUANT_SURGE_BOARDS=gem,star
# surge_watch 通知开关（缺省 True，只推 PushDeer admin，不推 PushPlus）
# NOTIFY_SURGE_WATCH=true
```

### 5b. Mac 本地 `.env`（P2 生效，让 panorama poller 读云端 feed）

```dotenv
# 云端 surge 全市场快照 feed（配置后 panorama poller 优先读云端，本机不自拉；
# 未配则行为与现状完全一致，零风险）
RQUANT_CLOUD_FEED_URL=http://82.156.0.68:8081/feed/snapshot_full.parquet
RQUANT_CLOUD_FEED_USER=<backup用户>
RQUANT_CLOUD_FEED_PASS=<密码>
```

> Mac 侧改完 `.env` 后重启 panorama（dashboard/canvas）进程生效。feed 陈旧
> （Last-Modified >120s）或 HTTP 失败会自动回落本机现有三级路由。

---

## 6. 次日盘中终验（E4，用户侧）

- 09:25 timer 自启，`systemctl status rquant-surge-watch.service` 应 active(running)。
- 开盘后首批爆量推送到刘彤手机（PushDeer），9:33 前静默、9:33 起才推。
- 观察每分钟批次量级（回测预期日均 5.2 个、开盘高峰 3-5 只/分钟）。
  - **超预期** → 调 `SurgeConfig`（k_rough / k_confirm 提高）后重部署。
- 15:02 服务自然退出（exit 0，systemd 不重启）；`data/surge_live/2026-XX-XX-series.parquet`
  落研究数据。

---

## 回滚

```bash
sudo systemctl stop rquant-surge-watch.timer rquant-surge-watch.service
sudo systemctl disable rquant-surge-watch.timer
# 代码回滚：git checkout <上个 tag> && bash scripts/deploy.sh
```

nginx feed location 回滚：从 vhost conf 删 `/feed/` 块，`nginx -s reload`。
Mac `.env` 删 `RQUANT_CLOUD_FEED_URL` 即回到本机自拉（零风险）。
