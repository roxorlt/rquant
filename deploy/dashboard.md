# rQuant Health Dashboard

Streamlit 单页面，9 个核心指标，30 秒自动刷新。云端部署对外提供 web 访问。

## 9 个指标

1. **systemd 服务状态**：rquant-monitor / rquant-daily 的 active state + 上次/下次触发
2. **monitor.service 健康**：是否在 running，已运行时长
3. **今日触发事件**：monitor_event 表实时列表
4. **当前 Watchlist**：Pool 2 active + 今日 Pool 1 命中
5. **数据新鲜度**：最新 daily_bar / screen_result 日期 + 总行数
6. **最近 7 日 Pool 1 命中数**：折线趋势
7. **通知通道健康**：notification_log 24h 成功率 + 最近 20 条
8. **本地 sync 状态**：本地 rsync 拉云端的最后时间 + 数据大小（marker 上报）
9. **Pool 2 实时价位**：每只 active 标的现价 vs 各档位距离

## 部署

### 1. 把 dashboard.service 复制到系统目录

```bash
cd ~/rquant
git pull
sudo cp deploy/systemd/rquant-dashboard.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now rquant-dashboard.service
systemctl status rquant-dashboard.service --no-pager | head -10
```

启动后默认监听 `0.0.0.0:8501`。验证：

```bash
curl -sI http://localhost:8501 | head -3
# 应返回 HTTP/1.1 200 OK
```

### 2. 防火墙（腾讯云轻量服务器）

腾讯云控制台 → 轻量服务器 → 防火墙 → 入站规则添加：
- 协议：TCP
- 端口：8501
- 来源：`0.0.0.0/0`（公开访问）或自己的家庭 IP（限制访问）

### 3. nginx 反向代理 + Basic Auth（推荐，避免裸露 8501）

宝塔面板 → 网站 → 添加站点（用域名或 IP+8080 端口）→ 配置反向代理 → 目标 `http://127.0.0.1:8501` →
配置 Basic Auth 用户密码。

或手动 nginx config：

```nginx
server {
    listen 8080;
    server_name _;

    auth_basic "rQuant Dashboard";
    auth_basic_user_file /etc/nginx/.htpasswd;

    location / {
        proxy_pass http://127.0.0.1:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 86400;
    }
}
```

生成 `.htpasswd`：

```bash
sudo dnf install httpd-tools -y  # OpenCloudOS
sudo htpasswd -c /etc/nginx/.htpasswd <username>
```

### 4. 访问

- 直连（不安全）：http://82.156.0.68:8501
- nginx 反代：http://82.156.0.68:8080  → 输入 basic auth 用户密码
- 移动端友好（Streamlit 自带响应式）

## 本地开发预览

```bash
cd ~/brain/30-projects/rQuant
.venv/bin/streamlit run src/rquant/dashboard/app.py
# 默认 http://localhost:8501
```

## 卸载

```bash
sudo systemctl disable --now rquant-dashboard.service
sudo rm /etc/systemd/system/rquant-dashboard.service
sudo systemctl daemon-reload
```

## 故障排查

| 现象 | 检查 |
|------|------|
| `systemctl status` 反复 restart | 看 `journalctl -u rquant-dashboard.service -n 50` —— 通常是 .env 里 DUCKDB_PATH 错或 streamlit 包未装 |
| 浏览器打不开 | `curl localhost:8501` 服务端能访问吗？防火墙是否开 8501？ |
| systemd 状态查询返回空 | dashboard 跑在容器/没权限读 systemd？检查 User=lighthouse |
| notification_log 表不存在 | 推送一次（rquant notify-test）后 `_log_notification` 会自动建表 |
| Pool 2 实时价位拉不到 | sina 源被云端封？看 dashboard 上方红色 error；本地手动 `python -c "import akshare as ak; print(ak.stock_zh_a_spot().shape)"` |
