# 全景页上云部署清单（pair 模式）

**日期**：2026-07-06　**分支**：feat/panorama-cloud → main（合 PR 后部署）
**目标**：全景页从「Mac 拉数 + SSH 反向隧道」改为**云端直跑**——手机/朋友任意网络
直访 `82.156.0.68:28080`（basic auth），云端 streamlit 读同机 surge feed + 自拉兜底，
零隧道、零 Mac 依赖。

> Hybrid 分工：Claude 出命令，用户 ssh 上服务器粘贴执行并把输出贴回。**Claude 不
> 直接 ssh 操作生产**。所有路径用绝对路径。服务器 IP 统一 `82.156.0.68`。

拓扑变化：

```
旧：手机 → 云 nginx:28080 → 隧道 18506 → Mac:8506（Mac 拉数，被办公网风控）
新：手机/朋友（任意网络） → 云 nginx:28080（basic auth） → 云 127.0.0.1:8506
     （rquant-panorama.service，poller 读同机 surge feed + 自拉兜底）—— 零隧道
```

**Mac 侧完全不动**：本地 8506 全景页、midday launchd、隧道 launchd 照常运行（隧道保留
备用，本次不删）。两套独立运行同一份代码。

---

## 0. 前置

- PR 已合 main，用户在云端 `cd /home/lighthouse/rquant && git checkout main`。
- 本次上线依赖三块地基（今日已就绪）：surge-watch 全市场落盘、`dc_board` 在云、
  em 从云端 IP 干净可达。
- 云端 `.env` 有 `TUSHARE_TOKEN_MAIN`（kpl 题材成分 backfill 用，已有）。

---

## 1. 部署代码 + systemd unit（用户在云端跑）

```bash
cd /home/lighthouse/rquant
bash scripts/deploy.sh
```

`deploy.sh` 会：git pull → cp 新 unit 到 `/etc/systemd/system` → daemon-reload →
`enable --now rquant-kpl-snapshot.timer`（新 timer）→ post-check 验证 timer NEXT
trigger 在 1 年内。

> ⚠️ `deploy.sh` 只**自动启用新 timer**、只 restart 白名单内已在跑的 service
> （monitor/dashboard/nl-screen）。`rquant-panorama.service`（新增、常驻）不在白名单，
> 首次需**手动 enable**（下一步）；`rquant-kpl-snapshot.service` 是 oneshot，由 timer
> 驱动，无需常驻。

**核对输出**：`[5/6]` 步应打印
`rquant-kpl-snapshot.timer: 下次 trigger 2026-07-0X 16:35:00（...s 后）`。若为 `n/a`
说明 OnCalendar 被拒收，见第 3 步单独验证。

---

## 2. 确认云端 streamlit / altair 依赖（用户在云端跑）

全景页 `market_panorama.py` 用 streamlit + altair 绘图。streamlit 是核心依赖，
altair 随 streamlit 传递安装。先同步依赖再确认可导入：

```bash
cd /home/lighthouse/rquant
uv sync                                    # 对齐 pyproject 依赖（幂等）
.venv/bin/python -c "import altair, streamlit; print('altair', altair.__version__, '| streamlit', streamlit.__version__)"
```

**期望**：打印 `altair 6.x | streamlit 1.57.x`，无 ImportError。

---

## 3. 单独验证 kpl timer OnCalendar 语法（用户在云端跑）

```bash
systemd-analyze calendar 'Mon..Fri *-*-* 16:35:00' --iterations 5
```

**期望**：5 个 Iteration 均为工作日 16:35:00，相邻间隔为「下一交易日」（跨周末跳周一）。
固定时刻语法（非分钟步进），历史上安全。

---

## 4. 启动云端全景页 service（用户在云端跑）

```bash
sudo systemctl enable --now rquant-panorama.service
sleep 3
systemctl status rquant-panorama.service --no-pager | head -15
# 本机自查：8506 应有 streamlit 应答（127.0.0.1，未过 nginx auth 前直连）
curl -sI http://127.0.0.1:8506/ | head -3
```

**期望**：`active (running)`；`curl` 返回 `200`（streamlit 首页）。

unit 关键点（供核对）：
- `--server.port 8506 --server.address 127.0.0.1`：只绑回环，只给 nginx，不直接对外；
- `EnvironmentFile=.env` 之后两行 `Environment=` **覆盖生效**（systemd 按出现顺序处理，
  同名变量后者胜）：
  - `RQUANT_CLOUD_FEED_URL=/home/lighthouse/rquant/data/surge_live/snapshot_full.parquet`
    —— poller 第 0 路由读 surge-watch 同机落盘的全市场 parquet（本地文件分支，mtime
    判新鲜 ≤120s，命中零自拉；文件缺失/陈旧自动回落自拉三级路由）；
  - `RQUANT_PANORAMA_SOCKS=`（置空）—— 云端无本地 SOCKS 出口，禁用该级免徒劳重试。

> 首次部署时 `snapshot_full.parquet` 可能尚不存在（surge-watch 次日盘中才落盘），
> poller 会自动回落自拉（em 直连，云端 IP 干净），全景页照常可用；次日盘中起改由
> 共享 feed 供数（状态行 route 显示「云端feed」）。

---

## 5. nginx 28080 反代改本地（用户在云端跑）

`deploy/nginx/rquant-panorama-cloud.conf`（28080 server block，`proxy_pass
127.0.0.1:8506`，WebSocket 头齐全，auth 用手工维护的 `.htpasswd-panorama`）覆盖
服务器上旧的隧道反代版 `panorama.conf`：

```bash
sudo cp /home/lighthouse/rquant/deploy/nginx/rquant-panorama-cloud.conf \
    /www/server/panel/vhost/nginx/panorama.conf
sudo nginx -t && sudo nginx -s reload
```

**验证 28080**（用户在云端或本地跑）：

```bash
# 无凭据 → 401（basic auth 生效）
curl -sI http://82.156.0.68:28080/ | head -3
# 带凭据 → 200（streamlit 首页经 nginx 透传）
curl -sI -u '<panorama用户>:<密码>' http://82.156.0.68:28080/ | head -3
```

**期望**：无凭据 `401 Unauthorized`；带凭据 `200 OK`。

---

## 6. kpl 题材成分首跑（部署当天手动，别等 16:35，用户在云端跑）

默认体系「开盘啦题材」需要云端主库有 `kpl_concept_member` 快照才不为空。timer 每工作日
16:35 自动刷新，但部署当天手动跑一次先把数据填上：

```bash
cd /home/lighthouse/rquant
.venv/bin/rquant data-backfill --dataset kpl_concept --today
```

**期望**：日志打印回补 summary（成功、无 failed_dates）。写云端主库
`kpl_concept_member`（snapshot 模式整表替换）。

**让全景页读到**：kpl 写主库后需经只读副本才对全景页可见。手动触发一次 replica-sync：

```bash
sudo systemctl start rquant-replica-sync.service
```

> 日常路径：16:35 写主库 → **17:30** 那次 replica-sync 带进副本（replica-sync 时刻表
> 在 15:10 与 17:30 之间无触发，故 16:35 的写入当晚 17:30 后才可见，非 5min 内）。
> 部署当天用上面的手动 `start` 立即刷副本，不必等 17:30。

**写者串行确认**（16:35 槽位无并发写主库者，已核对）：
- `rquant-monitor` 15:02 自然退出（盘中写者已停）；
- `rquant-daily` 17:00 起（16:35 时尚未启动）；
- `rquant-backup`（`backup-snapshot.sh` 是 `cp`+`gzip`）与 `rquant-replica-sync`
  （`cp` 主库+WAL）**只读拷贝、不写主库**——不与 kpl 抢写锁；
- kpl timer 用 `Persistent=false`：错过不补跑（题材变化慢、次日自愈），避免补跑撞进
  daily 17:00 写库窗口。

---

## 7. 次日盘中终验（E5，用户侧）

- 09:25 surge-watch timer 自启，盘中每分钟落 `data/surge_live/snapshot_full.parquet`。
- 全景页 `82.156.0.68:28080`（带凭据）打开，脉搏/合表/下钻/个股图正常。
- 状态行数据每分钟更新、age<90s，**路由显示「云端feed」**（poller 命中同机 surge
  快照，本机零自拉）。若显示「东财直连」说明 feed 陈旧/缺失走了自拉兜底（仍可用）。
- 默认体系「开盘啦题材」合表有数据（非空）。
- 换一个不在办公网的网络（手机流量/朋友）能正常访问（不再依赖 Mac 在线）。

---

## 回滚

```bash
# 1. nginx 换回旧隧道反代 conf（若保留了备份），或临时停全景页
sudo systemctl stop rquant-panorama.service
sudo systemctl disable rquant-panorama.service
# 2. nginx 恢复：把 panorama.conf 换回旧隧道版（proxy_pass 127.0.0.1:18506）后 reload
#    （旧版随 deploy/tunnel/ 留档；若无备份，手工改 proxy_pass 回 18506）
sudo nginx -t && sudo nginx -s reload
# 3. kpl timer 回滚（可选）
sudo systemctl disable --now rquant-kpl-snapshot.timer
# 4. 代码回滚：git checkout <上个 tag> && bash scripts/deploy.sh
```

回滚后手机访问回到「隧道 → Mac 8506」老路径（需 Mac 在线 + 隧道 launchd 常驻）。
Mac 侧全程无需任何改动。
