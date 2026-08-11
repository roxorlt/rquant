# NL 选股 Streamlit 应用部署

> 与监控看板独立的第二个 Streamlit 应用，端口 8502，nginx 反代到 `/nl/` 路径。
> 首次部署随 v0.12.0 上线，2026-04-30。

## 架构

```
公网 8081
   ├── /dashboard/  → 127.0.0.1:8501  (rquant-dashboard.service, 监控看板)
   └── /nl/         → 127.0.0.1:8502  (rquant-nl-screen.service, NL 选股)  ← 新增
```

两个应用进程互相独立：
- 监控看板 30 秒 meta refresh，编辑型操作不友好
- NL 选股无 refresh，编辑安心

## 一次性部署清单（在 82.156.0.68 上）

> 走「Hybrid 协作」：Claude 写命令，用户在宝塔 Web 终端粘贴执行。

### 1. 拉取最新代码

```bash
cd /home/lighthouse/rquant
sudo -u lighthouse git pull origin main
sudo -u lighthouse git tag -l v0.12.0  # 应能看到
```

### 2. 同步依赖

```bash
cd /home/lighthouse/rquant
sudo -u lighthouse /home/lighthouse/.local/bin/uv sync
# 应看到 + openai==2.33.0
```

### 3. 加 DEEPSEEK_API_KEY 到 .env

```bash
# 编辑 .env，追加：
sudo -u lighthouse tee -a /home/lighthouse/rquant/.env <<'EOF'

# ===== LLM (Week 7 NL 选股, v0.12.0) =====
DEEPSEEK_API_KEY=<SET_VIA_ENVIRONMENT>
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-v4-flash
EOF

# 检查
sudo -u lighthouse grep DEEPSEEK /home/lighthouse/rquant/.env
```

### 4. 安装 systemd unit

```bash
sudo cp /home/lighthouse/rquant/deploy/systemd/rquant-nl-screen.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable rquant-nl-screen.service
sudo systemctl start rquant-nl-screen.service

# 检查
sudo systemctl status rquant-nl-screen.service --no-pager
```

应该看到 `active (running)`。

### 5. 更新 nginx 反代

```bash
# nginx vhost 配置（宝塔自动加载 /www/server/panel/vhost/nginx/*.conf）
sudo cp /home/lighthouse/rquant/deploy/nginx/rquant-backup.conf /www/server/panel/vhost/nginx/

# 测试 + reload
sudo nginx -t
sudo systemctl reload nginx
```

### 6. 验证

```bash
# 内网直连 NL 应用
curl -sf http://127.0.0.1:8502/_stcore/health && echo " ✓ nl-screen up"

# 外网走 nginx + auth（带 -u 用 backup htpasswd 凭据）
curl -sf -u rquant:<htpasswd-password> http://82.156.0.68:8081/nl/_stcore/health && echo " ✓ /nl/ proxy ok"
```

浏览器：http://82.156.0.68:8081/nl/ 输入 backup htpasswd 用户名密码即可访问。

### 7. journalctl 看日志（如有问题）

```bash
sudo journalctl -u rquant-nl-screen.service -n 50 --no-pager
```

常见问题：
- `ModuleNotFoundError: openai` → 步骤 2 没跑或失败，重跑 `uv sync`
- `DEEPSEEK_API_KEY required` → 步骤 3 没生效，检查 `.env` 是否有 key
- nginx 502 → systemd 没起来，看 `journalctl`

## 后续运维

### 重启
```bash
sudo systemctl restart rquant-nl-screen.service
```

### 看日志
```bash
sudo journalctl -u rquant-nl-screen.service -f
```

### NL 查询审计日志
LLM 请求记录写在 `/home/lighthouse/rquant/logs/nl_queries.jsonl`，每行一条 JSON：

```bash
sudo -u lighthouse tail -f /home/lighthouse/rquant/logs/nl_queries.jsonl | jq .
```

字段：`ts / query / plan / tokens_in / tokens_out / latency_ms / model / error`。

## 未来：单独开放 NL 给协作者

当前 `/nl/` 与 `/dashboard/` 共用 `.rquant-backup.htpasswd`，本人和协作者用同一组凭据。
要分离时：

1. 用 `htpasswd -c /www/server/nginx/conf/.rquant-nl.htpasswd <newuser>` 生成新文件
2. 改 `deploy/nginx/rquant-backup.conf` 中 `/nl/` 块的 `auth_basic_user_file`
3. `nginx -t && systemctl reload nginx`

**不要**把 NL 暴露成无 auth 公开服务——LLM 调用 = 真金白银的 token 消耗，
任何人能滥用。
