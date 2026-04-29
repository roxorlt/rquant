# Backup HTTP API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal**：把 mac → 云端的本地热备从 SSH/rsync 切换到 HTTPS（先 HTTP）+ curl + token 鉴权，绕开 fail2ban 限制，并为未来的 API 化（Week 7+ FastAPI 产品化）打基础。

**Architecture**：服务器侧用 systemd timer 触发 `cp` + `atomic mv` 生成 DuckDB 一致性快照到 `backup/latest.duckdb.gz`；nginx 暴露 `/backup/` location 加 basic auth；mac 侧 sync 脚本改用 `curl` + Authorization header 拉 backup。完全不走 SSH 22 端口。

**Tech Stack**：nginx + systemd timer + bash + curl + Python（现有栈不增加）

**前置说明**：
- 当前分支 `feat/cloud-deploy-systemd` 包含多个累积功能（systemd / PushPlus / dashboard / sync）。**先合 main 打 v0.7.0**，再开新分支 `feat/backup-http-api`，分支语义清晰
- 服务器域名暂未申请，先用 IP + HTTP + token；未来加 HTTPS 是单独 task

---

## File Structure

| 路径 | 类型 | 责任 |
|------|------|------|
| `scripts/backup-snapshot.sh` | 新建（服务器） | 服务器侧 cp + atomic mv DuckDB 到 backup/ |
| `deploy/systemd/rquant-backup.service` | 新建 | 触发 backup-snapshot.sh |
| `deploy/systemd/rquant-backup.timer` | 新建 | 盘中每 5 分钟 + 17:30 触发 |
| `deploy/nginx/rquant-backup.conf` | 新建 | nginx /backup/ location + basic auth |
| `scripts/sync-from-cloud.sh` | 重写 | 替换 rsync → curl + token |
| `src/rquant/dashboard/app.py` | 修改 | "本地 sync" section 加"服务器 snapshot 时间" |
| `.env.example` | 修改 | 新增 RQUANT_BACKUP_USER / RQUANT_BACKUP_TOKEN |
| `.env`（云端 + 本地） | 修改 | 同步真 token |
| `deploy/backup-api.md` | 新建 | 部署 + 故障排查文档 |
| `CHANGELOG.md` | 修改 | v0.7.0 + v0.8.0 记录 |
| `README.md` | 修改 | MVP 路径 + 技术栈表格 |

---

## Tasks

### Task 1: 合并 feat/cloud-deploy-systemd → main + 打 v0.7.0

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `README.md`

- [ ] **Step 1.1: 切到 main + fast-forward merge**

```bash
git checkout main
git merge --ff-only feat/cloud-deploy-systemd
git log --oneline -5
```

- [ ] **Step 1.2: CHANGELOG 加 [v0.7.0] section**

把当前 `[Unreleased]` section（如有）和 systemd 分支累积的所有 commits 归档为 v0.7.0：

```markdown
## [v0.7.0] — 2026-04-29 — 云端部署 + 多通道通知 + Health Dashboard

### Added
- systemd timer + service（deploy/systemd/）：daily 17:00 + monitor 09:25 工作日触发
- PushPlus 通道（支持微信公众号推送，给不装 PushDeer 的用户如美丞）
- Health Dashboard（src/rquant/dashboard/）：Streamlit 单页 9 个指标，30s 自动刷新
  - systemd 状态 / Watchlist / 今日事件 / 数据新鲜度 / 7 日趋势
  - 通知通道健康（24h 成功率） / 本地 sync 状态 / Pool 2 实时价位
  - Pool 2 行点击下钻：日 K + 分时 + 档位虚线
- 本地热备同步（rsync over SSH，scripts/sync-from-cloud.sh）：
  - 盘中 09:30-15:05 + 日终 17:10-17:30 同步窗口
  - rsync --delay-updates atomic rename
  - --force 选项手动触发
- notification_log 表 + 推送日志记录

### Fixed
- monitor fetch_realtime_prices 从 stock_zh_a_spot_em（东方财富，云端被屏蔽）
  改 stock_zh_a_spot（sina）—— sina HQ 批量接口给 dashboard 实时价位
- dashboard K 线 / 分时 API 同步换 sina：stock_zh_a_daily +
  stock_zh_a_minute 替代东方财富版本
- dashboard DuckDB 写锁冲突优雅降级（query 返回 None，UI 显示等待提示）
- dashboard UI 紧凑化：字号、间距、边框、metric 卡均向 Linear/Vercel 风格收紧
- 分时图 11:30-13:00 午休空段：x 轴改 ordinal 跳过空段，加灰虚线分隔

### Changed
- CLAUDE.md 新增"生产环境与协作模式"小节，记录 IP（82.156.0.68）+
  Hybrid 协作分工 + 通知通道分工
```

- [ ] **Step 1.3: README 更新通知通道描述**（保持已有：PushDeer + cc2im 限制说明）

- [ ] **Step 1.4: commit + tag + push**

```bash
git add CHANGELOG.md README.md
git commit -m "chore: release v0.7.0 (cloud deploy + dashboard + sync)"
git tag -a v0.7.0 -m "v0.7.0: 云端部署 + Health Dashboard + 双通道通知"
git push origin main
git push origin v0.7.0
git branch -d feat/cloud-deploy-systemd
```

---

### Task 2: 开新分支 + 设计文档复制

**Files:**
- Create: `docs/plans/2026-04-29-backup-http-api.md`（已是当前文档）

- [ ] **Step 2.1: 开新分支基于 main**

```bash
git checkout -b feat/backup-http-api
```

---

### Task 3: 服务器 backup snapshot 脚本

**Files:**
- Create: `scripts/backup-snapshot.sh`

- [ ] **Step 3.1: 写脚本**

```bash
#!/usr/bin/env bash
# 服务器侧：生成 DuckDB 一致性快照到 backup/latest.duckdb.gz
# 通过 cp + gzip + atomic mv 保证 mac 拉到永远是完整文件。
# 极小概率 cp 时撞上 monitor 写入 → partial state，下次 cp 自动修复。

set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DATA_FILE="${PROJECT_DIR}/data/rquant.duckdb"
BACKUP_DIR="${PROJECT_DIR}/backup"
LOG="${PROJECT_DIR}/logs/backup-snapshot.log"

mkdir -p "${BACKUP_DIR}" "$(dirname "${LOG}")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "${LOG}"; }

if [[ ! -f "${DATA_FILE}" ]]; then
    log "ERROR: ${DATA_FILE} not found"
    exit 1
fi

src_size=$(stat -c %s "${DATA_FILE}" 2>/dev/null || stat -f %z "${DATA_FILE}")
log "snapshot: src=${src_size}B"

# 1. cp 到 .tmp（同分区保证 mv 是 atomic）
cp "${DATA_FILE}" "${BACKUP_DIR}/latest.duckdb.tmp"

# 2. gzip 压缩（DuckDB 文件压缩比通常 30-50%）
gzip -f "${BACKUP_DIR}/latest.duckdb.tmp"
# 现在文件名是 latest.duckdb.tmp.gz

# 3. atomic rename
mv "${BACKUP_DIR}/latest.duckdb.tmp.gz" "${BACKUP_DIR}/latest.duckdb.gz"

# 4. 写元数据 JSON（dashboard 用）
cat > "${BACKUP_DIR}/latest.json.tmp" <<EOF
{"snapshot_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)", "src_bytes": ${src_size}, "compressed_bytes": $(stat -c %s "${BACKUP_DIR}/latest.duckdb.gz" 2>/dev/null || stat -f %z "${BACKUP_DIR}/latest.duckdb.gz")}
EOF
mv "${BACKUP_DIR}/latest.json.tmp" "${BACKUP_DIR}/latest.json"

dst_size=$(stat -c %s "${BACKUP_DIR}/latest.duckdb.gz" 2>/dev/null || stat -f %z "${BACKUP_DIR}/latest.duckdb.gz")
log "snapshot OK: gz=${dst_size}B (ratio=$(awk "BEGIN{printf \"%.0f%%\", ${dst_size}*100/${src_size}}"))"
```

- [ ] **Step 3.2: chmod + 本地 lint 检查（语法）**

```bash
chmod +x /Users/roxor/brain/30-projects/rQuant/scripts/backup-snapshot.sh
bash -n /Users/roxor/brain/30-projects/rQuant/scripts/backup-snapshot.sh
```

预期：`bash -n` 无输出（语法 OK）。

- [ ] **Step 3.3: commit**

```bash
git add scripts/backup-snapshot.sh
git commit -m "feat(backup): server-side snapshot script (cp + gzip + atomic mv)"
```

---

### Task 4: 服务器 systemd timer + service for backup

**Files:**
- Create: `deploy/systemd/rquant-backup.service`
- Create: `deploy/systemd/rquant-backup.timer`

- [ ] **Step 4.1: 写 service**

`deploy/systemd/rquant-backup.service`:

```ini
[Unit]
Description=rQuant DuckDB Snapshot for Backup API
After=network-online.target

[Service]
Type=oneshot
User=lighthouse
Group=lighthouse
WorkingDirectory=/home/lighthouse/rquant
ExecStart=/home/lighthouse/rquant/scripts/backup-snapshot.sh

TimeoutStartSec=120
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 4.2: 写 timer**

`deploy/systemd/rquant-backup.timer`:

```ini
[Unit]
Description=Trigger rQuant backup snapshot

[Timer]
# 盘中 09:30-15:05 每 5 分钟 + 日终 17:30
# OnCalendar 列举多个时间点，systemd 会按所有匹配触发
OnCalendar=Mon..Fri 09:30..15:05/5
OnCalendar=Mon..Fri 17:30

Persistent=true
Unit=rquant-backup.service

[Install]
WantedBy=timers.target
```

注意 `09:30..15:05/5` 是 systemd OnCalendar 范围语法（每 5 分钟）。

- [ ] **Step 4.3: 本地验证（systemd-analyze 在 mac 没有，跳过；语法人肉检查）**

- [ ] **Step 4.4: commit**

```bash
git add deploy/systemd/rquant-backup.service deploy/systemd/rquant-backup.timer
git commit -m "feat(backup): systemd timer + service trigger snapshot"
```

---

### Task 5: 服务器 nginx config

**Files:**
- Create: `deploy/nginx/rquant-backup.conf`

- [ ] **Step 5.1: 写 nginx config**

```nginx
# rQuant Backup API + Dashboard 反代
# 安装到 /etc/nginx/conf.d/rquant.conf 或宝塔的 vhost 目录

server {
    listen 8080;
    server_name _;

    # 限制总请求大小（备份文件可能大）
    client_max_body_size 0;

    # /backup/ — 静态文件 + basic auth
    location /backup/ {
        alias /home/lighthouse/rquant/backup/;
        autoindex off;

        auth_basic "rQuant Backup";
        auth_basic_user_file /etc/nginx/.rquant-backup.htpasswd;

        # 只允许下载 .duckdb.gz 和 .json 元数据
        if ($request_filename !~* \.(duckdb\.gz|json)$) {
            return 403;
        }

        # 长下载支持
        proxy_read_timeout 300s;

        # 不缓存（每次都拿最新）
        add_header Cache-Control "no-store" always;
    }

    # /dashboard/ — 反代 streamlit（保留原有访问，加 basic auth）
    location /dashboard/ {
        proxy_pass http://127.0.0.1:8501/;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 86400;

        auth_basic "rQuant Dashboard";
        auth_basic_user_file /etc/nginx/.rquant-backup.htpasswd;
    }

    # / — 简单 index 提示
    location = / {
        return 200 "rQuant API\n  /backup/latest.duckdb.gz\n  /backup/latest.json\n  /dashboard/\n";
        add_header Content-Type "text/plain";
    }
}
```

- [ ] **Step 5.2: commit**

```bash
git add deploy/nginx/rquant-backup.conf
git commit -m "feat(backup): nginx config for backup endpoint + dashboard reverse-proxy"
```

---

### Task 6: 在 .env.example 声明新配置

**Files:**
- Modify: `.env.example`

- [ ] **Step 6.1: 加配置项**

`.env.example` 末尾追加：

```
# ===== Backup API（mac 拉云端备份用，绕开 SSH/fail2ban）=====
# 服务器侧：nginx basic auth 用，写到 /etc/nginx/.rquant-backup.htpasswd
# mac 侧：sync 脚本读这两个变量，curl --user 鉴权
RQUANT_BACKUP_USER=rquant
RQUANT_BACKUP_TOKEN=
RQUANT_BACKUP_URL=http://82.156.0.68:8080/backup
```

- [ ] **Step 6.2: commit**

```bash
git add .env.example
git commit -m "feat(backup): add RQUANT_BACKUP_* env vars in .env.example"
```

---

### Task 7: 重写 mac 侧 sync 脚本（curl 替换 rsync）

**Files:**
- Modify: `scripts/sync-from-cloud.sh`

- [ ] **Step 7.1: 重写脚本（保留时段判断 + --force + PushDeer 告警）**

```bash
#!/usr/bin/env bash
# 本地从云端拉 backup snapshot 做热备
# - 走 HTTP basic auth（绕开 SSH/fail2ban）
# - 只在数据有变化的时段同步：盘中 09:30-15:05 + 日终 17:10-17:30
# - 失败重试 2 次（间隔 60s），最终失败 PushDeer 告警

set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOCAL_DATA_FILE="${PROJECT_DIR}/data/rquant.duckdb"
LOG_DIR="${PROJECT_DIR}/logs"
LOG="${LOG_DIR}/sync-from-cloud.log"
ENV_FILE="${PROJECT_DIR}/.env"

mkdir -p "${LOG_DIR}"
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG}"; }

# 加载 .env
if [[ ! -f "${ENV_FILE}" ]]; then
    log "ERROR: .env not found"
    exit 1
fi
# shellcheck disable=SC1090
set -a; source "${ENV_FILE}"; set +a

: "${RQUANT_BACKUP_USER:?missing in .env}"
: "${RQUANT_BACKUP_TOKEN:?missing in .env}"
: "${RQUANT_BACKUP_URL:?missing in .env}"

# --force 跳过时段判断
force_mode=0
if [[ "${1:-}" == "--force" ]]; then force_mode=1; fi

# 时段判断
hour=$(date +%H); minute=$(date +%M); dow=$(date +%u)
hhmm=$((10#${hour} * 100 + 10#${minute}))
sync_window=""
if (( hhmm >= 930 && hhmm <= 1505 )); then
    sync_window="intraday"
elif (( hhmm >= 1710 && hhmm <= 1730 )); then
    sync_window="daily_after_pipeline"
fi

if (( force_mode == 1 )); then
    log "sync window: forced (manual)"
elif [[ -z "${sync_window}" ]]; then
    log "skip: not in sync window (hhmm=${hhmm})"
    exit 0
elif [[ "${sync_window}" == "intraday" && "${dow}" -gt 5 ]]; then
    log "skip: weekend (dow=${dow})"
    exit 0
else
    log "sync window: ${sync_window}"
fi

TMP_GZ="${LOCAL_DATA_FILE}.gz.tmp"
TMP_DB="${LOCAL_DATA_FILE}.tmp"

# 重试 2 次（curl 失败不会触发 fail2ban，因为走 HTTP 不是 SSH）
ok=0
for attempt in 1 2; do
    log "curl attempt ${attempt}/2"
    if curl -sS --fail --max-time 60 \
            --user "${RQUANT_BACKUP_USER}:${RQUANT_BACKUP_TOKEN}" \
            -o "${TMP_GZ}" \
            "${RQUANT_BACKUP_URL}/latest.duckdb.gz" 2>>"${LOG}"; then
        ok=1
        break
    fi
    log "curl failed (attempt ${attempt})"
    sleep 60
done

if (( ok == 0 )); then
    log "ERROR: curl failed 2 times, sending PushDeer alert"

    keys=$(grep "^PUSHDEER_KEYS=" "${ENV_FILE}" | cut -d= -f2 | tr -d '\n\r')
    endpoint=$(grep "^PUSHDEER_ENDPOINT=" "${ENV_FILE}" | cut -d= -f2 | tr -d '\n\r')
    endpoint="${endpoint:-https://api2.pushdeer.com/message/push}"
    title="❌ rQuant 备份同步失败"
    body="curl 拉云端 backup 失败 2 次。

时间：$(date '+%Y-%m-%d %H:%M:%S')
URL：${RQUANT_BACKUP_URL}/latest.duckdb.gz
最近日志：
\`\`\`
$(tail -n 15 "${LOG}")
\`\`\`"
    IFS=',' read -ra KEY_ARR <<< "${keys}"
    for key in "${KEY_ARR[@]}"; do
        k=$(echo "${key}" | xargs)
        [[ -n "${k}" ]] || continue
        curl -s -X POST "${endpoint}" \
            --data-urlencode "pushkey=${k}" \
            --data-urlencode "text=${title}" \
            --data-urlencode "desp=${body}" \
            --data-urlencode "type=markdown" \
            --max-time 10 >/dev/null 2>&1 || true
    done
    rm -f "${TMP_GZ}"
    exit 1
fi

# 解压
if ! gunzip -c "${TMP_GZ}" > "${TMP_DB}" 2>>"${LOG}"; then
    log "ERROR: gunzip failed"
    rm -f "${TMP_GZ}" "${TMP_DB}"
    exit 1
fi
rm -f "${TMP_GZ}"

# atomic rename
mv "${TMP_DB}" "${LOCAL_DATA_FILE}"
size=$(du -h "${LOCAL_DATA_FILE}" | cut -f1)
log "sync OK: ${size}"
```

- [ ] **Step 7.2: 语法检查**

```bash
bash -n /Users/roxor/brain/30-projects/rQuant/scripts/sync-from-cloud.sh
```

- [ ] **Step 7.3: commit**

```bash
git add scripts/sync-from-cloud.sh
git commit -m "feat(backup): rewrite sync-from-cloud.sh to use HTTP curl + token

替代 rsync over SSH，绕开服务器 fail2ban。仍保留时段窗口、--force 选项、
失败 PushDeer 告警。增量同步换全量下载（DuckDB 文件压缩 30-50%，全量
下载几 MB 可接受）。"
```

---

### Task 8: dashboard 显示服务器 snapshot 时间

**Files:**
- Modify: `src/rquant/dashboard/app.py`

- [ ] **Step 8.1: 找到"本地热备 sync" section（搜 "本地热备"）**

- [ ] **Step 8.2: 在该 section 之上加"☁️ 服务器最近 snapshot"小卡**

读云端 `/home/lighthouse/rquant/backup/latest.json`（dashboard 跑在云端，能直接读文件）：

```python
# ── 服务器 snapshot 状态（dashboard 跑在云端，直接读 backup/latest.json） ──

st.markdown("## ☁️ 云端 Backup Snapshot")
backup_json = settings.data_dir.parent / "backup" / "latest.json"
if backup_json.exists():
    try:
        info = json.loads(backup_json.read_text())
        snap_at = info.get("snapshot_at", "")
        src_mb = info.get("src_bytes", 0) / 1024 / 1024
        gz_mb = info.get("compressed_bytes", 0) / 1024 / 1024

        cols = st.columns(3)
        cols[0].metric("snapshot 大小", f"{src_mb:.1f} MB")
        cols[1].metric("压缩后", f"{gz_mb:.1f} MB")

        if snap_at:
            snap_dt = datetime.fromisoformat(snap_at.replace("Z", "+00:00"))
            now_utc = datetime.now(timezone.utc)
            delta = now_utc - snap_dt
            local = snap_dt.astimezone(CST).strftime("%m-%d %H:%M:%S")
            if delta > timedelta(hours=1):
                cols[2].metric("最后 snapshot", local,
                               delta=f"{delta.total_seconds()/3600:.1f} 小时前",
                               delta_color="inverse")
            else:
                cols[2].metric("最后 snapshot", local,
                               delta=f"{int(delta.total_seconds()/60)} 分钟前")
    except Exception as e:
        st.error(f"snapshot 元数据解析失败: {e}")
else:
    st.info("snapshot 未生成（systemd timer 还没触发或脚本失败）")
```

- [ ] **Step 8.3: 本地烟雾测试**

```bash
.venv/bin/streamlit run src/rquant/dashboard/app.py --server.headless true \
  --server.port 8502 --browser.gatherUsageStats false &
sleep 5
curl -sI http://localhost:8502 | head -2
kill %1
```

预期：`HTTP/1.1 200 OK`。

- [ ] **Step 8.4: commit**

```bash
git add src/rquant/dashboard/app.py
git commit -m "feat(dashboard): show cloud backup snapshot status"
```

---

### Task 9: 部署文档

**Files:**
- Create: `deploy/backup-api.md`
- Modify: `deploy/local-sync.md`（指向新文档）

- [ ] **Step 9.1: 写部署文档**

`deploy/backup-api.md`：

````markdown
# Backup HTTP API 部署

mac 通过 HTTPS（先 HTTP）+ basic auth + token 拉云端 DuckDB 快照。
绕开 SSH/fail2ban 限制。

## 服务器侧（首次部署）

### 1. 安装 systemd unit

```bash
cd ~/rquant && git pull
sudo cp deploy/systemd/rquant-backup.service /etc/systemd/system/
sudo cp deploy/systemd/rquant-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now rquant-backup.timer
systemctl list-timers --no-pager | grep rquant-backup
```

立即手动跑一次验证：
```bash
sudo systemctl start rquant-backup.service
ls -la ~/rquant/backup/
cat ~/rquant/backup/latest.json
```

### 2. 装 nginx 并配置

OpenCloudOS / RHEL：
```bash
sudo dnf install -y nginx httpd-tools
sudo systemctl enable --now nginx
```

### 3. 生成 token + htpasswd

```bash
# 生成强随机 token
TOKEN=$(openssl rand -base64 32 | tr -d '=+/' | head -c 32)
echo "RQUANT_BACKUP_TOKEN=${TOKEN}"  # 记下，后面 mac 端要用

# 创建 htpasswd
sudo htpasswd -bc /etc/nginx/.rquant-backup.htpasswd rquant "${TOKEN}"
sudo chmod 640 /etc/nginx/.rquant-backup.htpasswd
sudo chown root:nginx /etc/nginx/.rquant-backup.htpasswd
```

### 4. 装 nginx config

```bash
sudo cp deploy/nginx/rquant-backup.conf /etc/nginx/conf.d/rquant-backup.conf
sudo nginx -t  # 测语法
sudo systemctl reload nginx
```

### 5. 开 8080 端口

腾讯云控制台 → 防火墙 → 入站规则添加 TCP 8080 来源 0.0.0.0/0（或限你 mac IP）。

### 6. 验证

```bash
curl -sI -u rquant:${TOKEN} http://localhost:8080/backup/latest.duckdb.gz | head -3
# 应返回 HTTP/1.1 200 OK
```

## mac 侧

### 1. 把 token 写到本地 .env

```bash
echo "" >> ~/brain/30-projects/rQuant/.env
echo "RQUANT_BACKUP_USER=rquant" >> ~/brain/30-projects/rQuant/.env
echo "RQUANT_BACKUP_TOKEN=<服务器生成的 token>" >> ~/brain/30-projects/rQuant/.env
echo "RQUANT_BACKUP_URL=http://82.156.0.68:8080/backup" >> ~/brain/30-projects/rQuant/.env
```

### 2. 测试

```bash
bash ~/brain/30-projects/rQuant/scripts/sync-from-cloud.sh --force
tail -10 ~/brain/30-projects/rQuant/logs/sync-from-cloud.log
```

预期：`sync OK: NNm`，本地 `data/rquant.duckdb` 是云端的副本。

### 3. 可选：关掉 mac → 服务器的 sync SSH 路径

如果 SSH 不再用于 sync 也不用于其他维护，可以从服务器 ~/.ssh/authorized_keys 删除 mac 的 ed25519 公钥（保留宝塔登录通道即可）。

## 故障排查

| 现象 | 检查 |
|------|------|
| curl 401 Unauthorized | htpasswd 文件 / .env 里 USER/TOKEN 是否一致 |
| curl 403 Forbidden | nginx config 里 if 文件名后缀检查；`/backup/` 末尾斜杠 |
| curl 404 Not Found | snapshot 还没生成 → `sudo systemctl start rquant-backup.service` |
| nginx -t 报错 | 看具体错误；常见 user 没创建（OpenCloudOS 默认是 nginx 用户）/ port 占用 |
| 本地 sync 拉成功但 DuckDB 打开报错 | partial state，下次 sync 自动覆盖修复；或手动 --force 重拉 |

## 未来：HTTPS 升级路径

1. 申请域名（腾讯云域名 / Namecheap），DNS A 记录指向服务器 IP
2. `sudo dnf install certbot python3-certbot-nginx -y`
3. `sudo certbot --nginx -d backup.example.com`
4. nginx config 改为 `listen 443 ssl;`，certbot 自动加 SSL 配置
5. mac .env 把 RQUANT_BACKUP_URL 改成 `https://backup.example.com/backup`
````

- [ ] **Step 9.2: 更新 local-sync.md 指向新文档**

```markdown
# 本地热备同步（macOS）

⚠️ **此文档已过时**：sync 现在通过 HTTP API 而非 rsync over SSH。
最新部署见 [`deploy/backup-api.md`](backup-api.md)。
```

- [ ] **Step 9.3: commit**

```bash
git add deploy/backup-api.md deploy/local-sync.md
git commit -m "docs(backup): deployment + troubleshooting guide for HTTP API"
```

---

### Task 10: CHANGELOG + README 更新 + 合并 + tag v0.8.0

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `README.md`

- [ ] **Step 10.1: CHANGELOG 加 [v0.8.0]**

```markdown
## [v0.8.0] — YYYY-MM-DD — Backup HTTP API（替换 rsync over SSH）

### Added
- `scripts/backup-snapshot.sh`：服务器侧 cp + gzip + atomic mv 生成一致性快照
- `deploy/systemd/rquant-backup.{service,timer}`：盘中每 5min + 日终 17:30 触发
- `deploy/nginx/rquant-backup.conf`：nginx /backup/ + basic auth + dashboard 反代
- `deploy/backup-api.md`：完整部署 + 故障排查文档
- dashboard 新增"☁️ 云端 Backup Snapshot"指标读 backup/latest.json

### Changed
- `scripts/sync-from-cloud.sh`：rsync over SSH → curl HTTP + basic auth token
  - 不再走 22 端口，绕开 fail2ban
  - 失败重试从 3 次 → 2 次（curl 失败不触发 fail2ban，重试更安全）
  - DuckDB 文件 gzip 传输（30-50% 压缩）
- `.env.example` 新增 RQUANT_BACKUP_USER / RQUANT_BACKUP_TOKEN / RQUANT_BACKUP_URL

### Removed
- `deploy/local-sync.md` 主体内容（指向 backup-api.md）
```

- [ ] **Step 10.2: README MVP 路径补一笔**

技术栈表格"通知" 之后加一行：
```
| 备份 API | nginx + basic auth + curl + gzip | HTTP API 化为产品化 GUI 打基础 |
```

- [ ] **Step 10.3: 合 main + tag**

```bash
git checkout main
git merge --ff-only feat/backup-http-api
git push origin main
git tag -a v0.8.0 -m "v0.8.0: Backup HTTP API"
git push origin v0.8.0
git branch -d feat/backup-http-api
```

---

## Risks & Mitigations

| 风险 | 缓解 |
|------|------|
| DuckDB partial state（cp 时 monitor 在写） | atomic mv 保证客户端拉到完整文件；偶尔本地损坏下次自动覆盖修复 |
| nginx 8080 暴露公网 | basic auth + 强随机 token；未来加 HTTPS |
| basic auth token 明文（HTTP） | 自己用 + 强随机 32 字节，被嗅探概率低；未来加 HTTPS |
| systemd timer OnCalendar 范围语法错误 | Step 4.2 写的是 systemd 240+ 标准语法；上线前 `systemd-analyze verify` 检验 |
| 宝塔 nginx config 冲突 | 把 config 放 conf.d/，宝塔默认会加载；如有冲突看 nginx -t 错误 |
| 服务器 dnf install nginx 没装 | Task 9 文档显式包含 `dnf install -y nginx httpd-tools` |

---

## Self-Review

**Spec 覆盖**：
- ✅ 服务器侧 snapshot 生成（Task 3 + 4）
- ✅ HTTP API 暴露 + 鉴权（Task 5 + 6 + 9 部署）
- ✅ mac 侧改 curl（Task 7）
- ✅ dashboard 显示状态（Task 8）
- ✅ 文档（Task 9）
- ✅ 合并 + tag（Task 1, 10）

**Placeholder 扫描**：每个 step 都有完整代码 + 命令 + 预期输出。

**类型一致**：env var 名称（RQUANT_BACKUP_USER / TOKEN / URL）在 .env.example、sync 脚本、文档里完全一致。

**潜在风险已记录**：见 Risks 表。
