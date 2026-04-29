# Backup HTTP API 部署

mac 通过 HTTP（未来 HTTPS）+ basic auth 拉云端 DuckDB 快照。绕开 SSH/fail2ban 限制。

## 架构

```
云端服务器：
  systemd-timer (每 5 分钟)
    → backup-snapshot.sh
    → cp data/rquant.duckdb → backup/.tmp → gzip → atomic mv → backup/latest.duckdb.gz
    → 写 backup/latest.json 元数据

  nginx :8081
    /backup/ → static serve backup/ + basic auth
    /dashboard/ → reverse proxy 8501 + basic auth

mac 笔记本：
  launchd (每 5 分钟)
    → sync-from-cloud.sh
    → curl --user … /backup/latest.duckdb.gz → gunzip → atomic mv → data/rquant.duckdb
```

## 服务器侧首次部署

### 1. git pull + 安装 systemd unit

```bash
cd ~/rquant && git pull
sudo cp deploy/systemd/rquant-backup.service /etc/systemd/system/
sudo cp deploy/systemd/rquant-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now rquant-backup.timer
systemctl list-timers --no-pager | grep rquant-backup
```

立即手动跑一次验证 snapshot 生成：

```bash
sudo systemctl start rquant-backup.service
ls -la ~/rquant/backup/
cat ~/rquant/backup/latest.json
```

预期：`backup/latest.duckdb.gz` 和 `backup/latest.json` 都存在。

### 2. 装 nginx + htpasswd 工具

OpenCloudOS / RHEL 9：

```bash
sudo dnf install -y nginx httpd-tools
sudo systemctl enable --now nginx
```

### 3. 生成 token + .htpasswd

```bash
# 生成强随机 token（32 字节 base64）
TOKEN=$(openssl rand -base64 32 | tr -d '=+/' | head -c 32)
echo "TOKEN: ${TOKEN}"   # 记下，配 mac 端 .env 要用

# 创建 htpasswd（用户名 rquant，密码就是 TOKEN）
sudo htpasswd -bc /etc/nginx/.rquant-backup.htpasswd rquant "${TOKEN}"
sudo chmod 640 /etc/nginx/.rquant-backup.htpasswd
sudo chown root:nginx /etc/nginx/.rquant-backup.htpasswd
```

### 4. 装 nginx config

```bash
sudo cp deploy/nginx/rquant-backup.conf /etc/nginx/conf.d/rquant-backup.conf
sudo nginx -t   # 测语法
sudo systemctl reload nginx
```

### 5. 开 8081 端口（云防火墙）

腾讯云轻量服务器控制台 → 防火墙 → 入站规则 → 添加：
- 协议：TCP
- 端口：8081
- 来源：`0.0.0.0/0`（公开访问）或限制为你 mac 公网 IP

### 6. 服务器侧验证

```bash
# 本机回环测试
curl -sI -u "rquant:${TOKEN}" http://localhost:8081/backup/latest.duckdb.gz | head -3
# 期望：HTTP/1.1 200 OK

# 元数据
curl -s -u "rquant:${TOKEN}" http://localhost:8081/backup/latest.json | python3 -m json.tool
# 期望：{"snapshot_at": "...", "src_bytes": ..., "compressed_bytes": ...}
```

## mac 侧

### 1. 把 token 写到本地 .env

```bash
cd /Users/roxor/brain/30-projects/rQuant
echo "" >> .env
echo "# Backup HTTP API" >> .env
echo "RQUANT_BACKUP_USER=rquant" >> .env
echo "RQUANT_BACKUP_TOKEN=<服务器 TOKEN>" >> .env
echo "RQUANT_BACKUP_URL=http://82.156.0.68:8081/backup" >> .env
```

### 2. 测试拉一次

```bash
bash scripts/sync-from-cloud.sh --force
tail -10 logs/sync-from-cloud.log
```

预期：
- 日志最后一行 `sync OK: NNm`
- `data/rquant.duckdb` 大小跟服务器原文件一致

### 3. （可选）关掉 SSH 同步路径

如果 SSH 不再用于 sync 也不用于其他维护，可从服务器 `~/.ssh/authorized_keys` 删除 mac 的 ed25519 公钥（保留宝塔登录通道即可）。

## 故障排查

| 现象 | 检查 |
|------|------|
| `curl 401 Unauthorized` | htpasswd 文件存在；mac .env USER/TOKEN 跟服务器一致 |
| `curl 403 Forbidden` | 文件名后缀检查（必须 `.duckdb.gz` 或 `.json`）；URL 末尾斜杠 |
| `curl 404 Not Found` | snapshot 还没生成 → `sudo systemctl start rquant-backup.service` 手动触发 |
| `nginx -t` 报 user 错 | OpenCloudOS 默认 nginx user 是 `nginx`，跟 htpasswd `chown` 一致 |
| `nginx -t` 报 port 占用 | `sudo ss -tlnp \| grep :8080` 看谁在占；改 nginx config listen 别的端口 |
| 本地 sync 拉成功但 DuckDB 打开报错 | partial state，下次 sync 自动覆盖修复；或 `--force` 重拉 |
| timer 不触发 | `systemctl list-timers \| grep rquant-backup` 看 NEXT 时间；`journalctl -u rquant-backup.service -n 30` 看错误 |

## 未来：HTTPS 升级

1. 申请域名（腾讯云 DNS / Namecheap），DNS A 记录指向服务器 IP
2. `sudo dnf install certbot python3-certbot-nginx -y`
3. `sudo certbot --nginx -d backup.example.com` 自动配 SSL + 续期
4. nginx config 改 `listen 443 ssl;`
5. mac `.env` 把 `RQUANT_BACKUP_URL` 改成 `https://backup.example.com/backup`

## 卸载

服务器：
```bash
sudo systemctl disable --now rquant-backup.timer
sudo rm /etc/systemd/system/rquant-backup.{service,timer}
sudo rm /etc/nginx/conf.d/rquant-backup.conf
sudo rm /etc/nginx/.rquant-backup.htpasswd
sudo systemctl daemon-reload
sudo systemctl reload nginx
```

mac 侧 sync launchd 仍可用——只要 .env 配新 token。
