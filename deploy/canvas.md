# rQuant 画布 Streamlit 部署

> Week 7.5 C 阶段：Pool 节点图 + 规则 CRUD + builtin fork + NL 改 pool（DeepSeek）。
> 独立 Streamlit 应用，端口 8504，nginx 反代 `/canvas/`。

## 架构

```
公网 8081
   ├── /backup/    → 静态文件（latest.duckdb.gz）
   ├── /upload/    → WebDAV PUT
   ├── /dashboard/ → 127.0.0.1:8501  (rquant-dashboard.service, 监控看板)
   ├── /nl/        → 127.0.0.1:8502  (rquant-nl-screen.service, NL 选股 Stage Cards)
   └── /canvas/    → 127.0.0.1:8504  (rquant-canvas.service, 画布) ← 新增
```

三个 Streamlit 进程互相独立，重启不互相影响。

## 一次性部署清单（在 82.156.0.68 上跑）

### 1. 拉代码 + 安装依赖

```bash
cd /home/lighthouse/rquant
sudo -u lighthouse git pull origin main
sudo -u lighthouse /home/lighthouse/.local/bin/uv sync
# 应看到 + streamlit-flow-component==1.6.1
```

### 2. 安装 systemd unit

```bash
sudo cp /home/lighthouse/rquant/deploy/systemd/rquant-canvas.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable rquant-canvas.service
sudo systemctl start rquant-canvas.service

# 检查
sudo systemctl status rquant-canvas.service --no-pager
```

### 3. 更新 nginx 反代

```bash
sudo cp /home/lighthouse/rquant/deploy/nginx/rquant-backup.conf /www/server/panel/vhost/nginx/
sudo nginx -t
sudo systemctl reload nginx
```

### 4. 验证

```bash
# 内网直连
curl -sf http://127.0.0.1:8504/_stcore/health && echo " ✓ canvas up"

# 外网走 nginx + auth
curl -sf -u rquant:<htpasswd-pwd> http://82.156.0.68:8081/canvas/_stcore/health && echo " ✓ /canvas/ proxy ok"
```

浏览器：`http://82.156.0.68:8081/canvas/`（输 backup htpasswd 凭据）

## 功能（v0.13.0 起）

| 模块 | 描述 |
|---|---|
| 节点画布 | streamlit-flow 渲染 PRESET_SCREENS 为节点 + depends_on edge，节点可拖 |
| Sidebar 工具 | trade_date / 重置布局 / 清诊断缓存 / pool 列表 / **+ 新建空 user pool** |
| Pool 详情 | 描述 / 依赖 / 规则数 + per-rule diagnostic 漏斗 + 命中标的预览 |
| Builtin pool | 只读 + **🍴 Fork as user/<name>**（复制成 user pool 即可编辑） |
| User pool CRUD | 规则行 inline 编辑参数 / 📋 复制 / ✕ 删除 / ➕ 加规则（popover） |
| NL 改 pool | text_input → 📤 解析（DeepSeek）→ diff 预览 → ✓ 应用到 pending |
| 持久化 | 💾 保存写 `user_presets/<base>.json` / 🗑 删除整个 user pool |

## DEEPSEEK_API_KEY

复用 `.env` 中 v0.12.0 已有的 `DEEPSEEK_API_KEY`（NL 选股共享）。如果 .env 没配，
NL 改 pool 功能会显示 warning 但其他功能仍可用。

## 后续运维

```bash
sudo systemctl restart rquant-canvas.service  # 重启
sudo journalctl -u rquant-canvas.service -f   # 看日志
sudo journalctl -u rquant-canvas.service -n 50 --no-pager | grep -i error
```

## 故障排查

- **502 Bad Gateway**：systemd 没起来，`journalctl -u rquant-canvas.service -n 50`
- **ModuleNotFoundError: streamlit_flow** → 步骤 1 没跑或失败，重跑 `uv sync`
- **iframe blank / 节点不显示** → 浏览器 F12 看 console，可能 streamlit-flow 版本不兼容
- **DuckDB lock** → 画布是 `read_only=True` 不应撞锁；如撞看 dashboard / nl-screen 是否被改坏了
