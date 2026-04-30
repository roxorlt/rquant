# Upload HTTP API 部署

mac 通过 HTTP + basic auth 推文件到云端，与 [backup-api.md](backup-api.md) 的下载通道
对称。绕开 SSH/fail2ban 限制。

## 用途

本地 import 的"配置数据"推到云端，比如：

- `risk_blacklist.parquet`（v0.9.0 风险黑名单）
- 未来其他从本地脚本生成的、需要服务器侧消费的数据文件

不适合：日常业务数据写入（那应该在云端 systemd 跑），大文件 / 高频上传。

## 架构

```
mac 笔记本：
  scripts/push-to-cloud.sh <file>
    → curl -T → PUT
    → 文件落到 /home/lighthouse/rquant/data/uploads/<file>

云端服务器：
  nginx :8081
    /upload/ → dav_methods PUT + basic auth (单独 .rquant-upload.htpasswd)

服务器手动消费（宝塔 Web 终端，root）:
  sudo mv uploads/<file> data/<file>
  python -c "..."  # 或 rquant 子命令把文件吃进 DuckDB
```

## 服务器侧首次部署（一次性）

> 因为 SSH 被 fail2ban 卡，所有命令在**宝塔面板 Web 终端**里跑。

### 1. git pull 拉最新代码（含新版 nginx config）

```bash
sudo su - lighthouse
cd /home/lighthouse/rquant
git fetch --tags && git pull origin main
```

### 2. 准备 uploads 目录

```bash
sudo mkdir -p /home/lighthouse/rquant/data/uploads
sudo chown www:www /home/lighthouse/rquant/data/uploads
sudo chmod 775 /home/lighthouse/rquant/data/uploads
```

`www` 是宝塔默认 nginx 用户。让 nginx 可写，lighthouse 通过 `sudo` 读移。

### 3. 生成单独的 upload token（与 backup token 分开）

```bash
TOKEN=$(openssl rand -base64 32 | tr -d '=+/' | head -c 32)
echo "RQUANT_UPLOAD_TOKEN=${TOKEN}"   # 记下！mac 端 .env 用

sudo htpasswd -bc /www/server/nginx/conf/.rquant-upload.htpasswd rquant-upload "${TOKEN}"
sudo chmod 640 /www/server/nginx/conf/.rquant-upload.htpasswd
sudo chown root:www /www/server/nginx/conf/.rquant-upload.htpasswd
```

### 4. 应用 nginx config

新版 `deploy/nginx/rquant-backup.conf` 已经多了 `/upload/` location。复制覆盖：

```bash
sudo cp /home/lighthouse/rquant/deploy/nginx/rquant-backup.conf \
        /www/server/panel/vhost/nginx/rquant-backup.conf
sudo nginx -t                 # 测语法
sudo systemctl reload nginx   # 不会断现有连接
```

如果 `nginx -t` 报 `unknown directive "dav_methods"`：宝塔默认 nginx 没编进 dav 模块。
检查 `nginx -V 2>&1 | grep -- '--with-http_dav_module'`，没有的话需要在宝塔面板
软件商店重装带 dav 的 nginx（"编译安装"勾上 dav）或者切到 OpenResty。

### 5. 服务器本机回环测试

```bash
echo "hello" > /tmp/test.parquet   # 占个名字，扩展名通过白名单
curl -sS -u "rquant-upload:${TOKEN}" -T /tmp/test.parquet \
     -w "%{http_code}\n" \
     http://localhost:8081/upload/test.parquet
# 期望：HTTP 201 Created
ls -la /home/lighthouse/rquant/data/uploads/test.parquet
sudo rm /home/lighthouse/rquant/data/uploads/test.parquet /tmp/test.parquet
```

## mac 侧

### 1. 写 token 到本地 .env

```bash
cd /Users/roxor/brain/30-projects/rQuant
cat >> .env <<EOF

# Upload HTTP API（v0.10.0）
RQUANT_UPLOAD_USER=rquant-upload
RQUANT_UPLOAD_TOKEN=<服务器步骤 3 的 TOKEN>
RQUANT_UPLOAD_URL=http://82.156.0.68:8081/upload
EOF
```

### 2. 推一个文件试试

```bash
echo "ping" > /tmp/foo.parquet
bash scripts/push-to-cloud.sh /tmp/foo.parquet
```

期望：`OK (HTTP 201)`。

## 服务器侧消费 uploaded 文件

文件落到 `data/uploads/`，owner 是 `www`，lighthouse 用 `sudo` 操作：

```bash
# 例：把 risk_blacklist.parquet 移到 data/ 然后 import
sudo mv /home/lighthouse/rquant/data/uploads/risk_blacklist.parquet \
        /home/lighthouse/rquant/data/risk_blacklist.parquet
sudo chown lighthouse:lighthouse /home/lighthouse/rquant/data/risk_blacklist.parquet

cd /home/lighthouse/rquant
.venv/bin/python -c "
from rquant.storage.duckdb import DuckDBStore
s = DuckDBStore()
s._conn.execute(\"DELETE FROM risk_blacklist WHERE list_label = '430黑名单'\")
s._conn.execute(\"INSERT INTO risk_blacklist SELECT * FROM read_parquet('data/risk_blacklist.parquet')\")
n = s._conn.execute(\"SELECT COUNT(*) FROM risk_blacklist WHERE list_label = '430黑名单'\").fetchone()[0]
print(f'imported {n} rows')
s.close()
"
```

## 故障排查

| 现象 | 检查 |
|------|------|
| `curl 401 Unauthorized` | `.rquant-upload.htpasswd` 存在，mac .env USER/TOKEN 与服务器一致 |
| `curl 403 Forbidden` | 文件名后缀（白名单 .parquet/.csv/.pdf/.json）；URL 路径包含文件名 |
| `curl 405 Not Allowed` | nginx 没 reload / dav_methods 没生效 / nginx 没编 dav 模块 |
| `curl 413 Entity Too Large` | 文件 > 100MB → 改 `client_max_body_size` |
| `curl 500 Internal Server Error` | nginx 写不了 uploads 目录 → 检查 owner / 权限 / `client_body_temp_path` |
| `nginx -t` 报 `unknown directive "dav_methods"` | 见步骤 4 备注：装带 dav 的 nginx |
| 文件传上去但 lighthouse 移不走 | 文件 owner=www，用 `sudo mv` |

## 安全考虑

- **白名单后缀**：nginx 强制 `.parquet/.csv/.pdf/.json`，挡掉脚本上传
- **单独 token**：upload 写权限不复用 backup 读 token，泄露其一不影响另一边
- **目录隔离**：写入 `data/uploads/` 不直接落 `data/`，需要服务器侧确认后再移动
- **不允许 DELETE**：`dav_methods PUT` 只声明 PUT，不允许 `dav_methods PUT DELETE`
- **未来 HTTPS**：参考 backup-api.md 的 certbot 流程，nginx config 是同一个 server block

## 卸载

```bash
# 编辑 nginx config 删掉 /upload/ location，或整个回滚到 v0.8.0 版本
sudo nginx -t && sudo systemctl reload nginx
sudo rm /www/server/nginx/conf/.rquant-upload.htpasswd
sudo rm -rf /home/lighthouse/rquant/data/uploads/
```
