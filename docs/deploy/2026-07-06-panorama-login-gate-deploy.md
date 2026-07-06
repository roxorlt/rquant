# 全景页微信友好登录网关部署清单（pair 模式）

**日期**：2026-07-06　**分支**：feat/panorama-login-gate → main（合 PR 后部署）
**目标**：全景页对外入口从 **HTTP basic auth** 改为 **cookie 登录页 + 签名令牌**——
微信内置浏览器不支持 basic auth（不弹框、直接 401），改成网页登录页后，朋友在微信里点
链接即可登录（表单输账号密码 → Set-Cookie 30 天 → 免登录复访）。

> Hybrid 分工：Claude 出命令，用户 ssh 上服务器粘贴执行并把输出贴回。**Claude 不直接
> ssh 操作生产**。所有路径用绝对路径。服务器 IP 统一 `82.156.0.68`。

拓扑变化：

```
旧：手机/微信 → 云 nginx:28080（basic auth，微信不弹框直接 401 → 进不去）→ 127.0.0.1:8506
新：手机/微信 → 云 nginx:28080
     ├ 带有效 cookie → auth_request 校验 200 → 127.0.0.1:8506（全景页 streamlit）
     └ 无 cookie → auth_request 401 → 跳 /login → 127.0.0.1:8507（登录服务）→ 输密码
          → Set-Cookie 签名令牌 → 302 回 /
```

新增常驻组件 **rquant-panorama-auth.service**（标准库 http.server，监听 127.0.0.1:8507），
与 rquant-panorama.service（8506 全景页）同期常驻。**Mac 侧完全不动。**

安全边界（明确）：
- 28080 仍是 **http 明文**，登录密码 over http 会明文传输——当前朋友看盘场景可接受，
  后续可上 TLS。签名令牌（hmac）防的是 **cookie 伪造**，不是链路嗅探。
- 令牌 = `base64url(user|exp).hmac_sha256(user|exp, SECRET)`，服务端无会话存储、自验证；
  改 `SECRET` 即令牌全体失效（应急踢人）。cookie 带 HttpOnly + SameSite=Lax。

---

## 0. 前置

- PR 已合 main，用户在云端 `cd /home/lighthouse/rquant && git checkout main && git pull`。
- 全景页 8506（rquant-panorama.service）已在跑（`systemctl status rquant-panorama.service`
  为 active）。本次只在其前面加一层登录网关，不动 8506。

---

## 1. 生成签名令牌密钥（SECRET）写 .env（用户在云端跑）

```bash
cd /home/lighthouse/rquant
# 生成一次，之后不要随意改（改了全体 cookie 失效、朋友需重新登录）
SECRET=$(openssl rand -hex 32)
echo "RQUANT_PANORAMA_COOKIE_SECRET=$SECRET" >> .env
grep RQUANT_PANORAMA_COOKIE_SECRET .env      # 确认已写入（一行、64 位 hex）
```

> ⚠️ SECRET 为空时登录服务**拒绝启动**（`serve_auth` 直接 SystemExit，不使用空密钥静默
> 降级）。故这一步必须先做，否则第 3 步 service 起不来（journal 会打印拒启原因）。
> 用户库路径默认 `data/panorama-users.txt`（0600 权限，CLI 自动创建）；如需自定义可另配
> `RQUANT_PANORAMA_USERS_PATH=` 到 .env。

---

## 2. 创建首个登录用户（用户在云端跑）

```bash
cd /home/lighthouse/rquant
.venv/bin/rquant panorama-user-add 刘彤        # 交互式：提示输密码两次确认，不回显
# 再给朋友建号（示例）
.venv/bin/rquant panorama-user-add friend
.venv/bin/rquant panorama-user-list           # 列出用户名（不含哈希）
```

**期望**：打印 `✅ 已添加 <name>（用户库 /home/lighthouse/rquant/data/panorama-users.txt）`。
用户名只允许字母/数字/点/下划线/短横（1-64 字符）。密码用 pbkdf2_sha256（200000 迭代）+
随机 salt 存储，用户库文件权限 0600。

移除用户：`.venv/bin/rquant panorama-user-remove <name>`（不存在为 no-op）。

---

## 3. 安装 + 启动登录服务 systemd unit（用户在云端跑）

```bash
cd /home/lighthouse/rquant
sudo cp deploy/systemd/rquant-panorama-auth.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now rquant-panorama-auth.service
sleep 2
systemctl status rquant-panorama-auth.service --no-pager | head -12
# 本机自查：8507 登录服务应答（未过 nginx 前直连）
curl -sI http://127.0.0.1:8507/login | head -3          # 期望 200
curl -s -o /dev/null -w "verify 无 cookie: HTTP %{http_code}\n" http://127.0.0.1:8507/verify  # 期望 401
```

**期望**：service `active (running)`；`/login` 返回 `200`；`/verify` 无 cookie 返回 `401`。
若 service 反复重启，`journalctl -u rquant-panorama-auth.service -n 20 --no-pager` 看是否
打印「RQUANT_PANORAMA_COOKIE_SECRET 未配置」——回到第 1 步。

---

## 4. 检查 nginx 是否含 auth_request 模块（用户在云端跑）

```bash
nginx -V 2>&1 | grep -o with-http_auth_request_module
```

**期望**：打印 `with-http_auth_request_module`（宝塔编译版通常自带）。

- **有输出** → 用主方案（第 5 步）。
- **无输出** → auth_request 不可用，改用降级方案：见
  `deploy/nginx/rquant-panorama-cloud.conf` 文件底部注释的 `map $cookie_rq_panorama`
  静态令牌版（安全性弱于主方案，仅应急；需另配登录服务用静态令牌，非默认路径）。
  本清单以下步骤按主方案写。

---

## 5. 替换 nginx conf 为 auth_request 版并 reload（用户在云端跑）

```bash
# 先备份现网（basic auth 版），便于回滚
sudo cp /www/server/panel/vhost/nginx/panorama.conf \
    /www/server/panel/vhost/nginx/panorama.conf.basicauth.bak

sudo cp /home/lighthouse/rquant/deploy/nginx/rquant-panorama-cloud.conf \
    /www/server/panel/vhost/nginx/panorama.conf
sudo nginx -t && sudo nginx -s reload
```

**期望**：`nginx -t` 通过（`syntax is ok` / `test is successful`），reload 无报错。
若 `nginx -t` 报 `unknown directive "auth_request"` → 回第 4 步走降级方案。

---

## 6. 验证 28080 登录流（用户在云端或本地跑）

```bash
# 1) 无 cookie 直访 → 302 跳 /login
curl -sI http://82.156.0.68:28080/ | head -3

# 2) GET /login → 200 登录表单
curl -sI http://82.156.0.68:28080/login | head -3

# 3) POST 正确凭据 → 302 + Set-Cookie（拿到令牌）
curl -s -D - -o /dev/null -X POST \
    --data 'username=<你的用户名>&password=<你的密码>' \
    http://82.156.0.68:28080/login | grep -iE '^HTTP|^Location:|^Set-Cookie:'

# 4) 带该 cookie 访问 / → 200（全景页经 nginx 透传）
TOKEN=<上一步 Set-Cookie 里 rq_panorama= 后到分号前的值>
curl -sI -H "Cookie: rq_panorama=$TOKEN" http://82.156.0.68:28080/ | head -3
```

**期望**：① 无 cookie `302`（Location: /login）；② `/login` `200`；③ POST 正确凭据
`302` + `Set-Cookie: rq_panorama=...; Max-Age=2592000; HttpOnly; Path=/; SameSite=Lax`；
④ 带 cookie `200`。

---

## 7. 微信实测（E3，用户侧手机）

1. 手机微信里打开 `http://82.156.0.68:28080/`（发给自己或直接输）。
2. 自动跳登录页（移动端友好表单，大输入框大按钮）。
3. 输账号密码 → 提交 → 进全景页（脉搏/合表/下钻正常，WebSocket 生效不白屏）。
4. **关掉重新打开** → 免登录直接进（cookie 30 天有效，验证 cookie 生效）。
5. 换一个朋友的微信、非办公网（手机流量）重复一遍。

---

## 8. basic auth 退场确认（E4）

- 新 `panorama.conf` 已**不含 `auth_basic` / `auth_basic_user_file`**（cookie 登录页取代）。
- 旧 `.htpasswd-panorama`（`/www/server/nginx/conf/.htpasswd-panorama`）**保留不删**，
  回滚时复用。第 5 步已把 basic auth 版 conf 备份为 `panorama.conf.basicauth.bak`。

---

## 回滚

```bash
# 1) nginx 换回 basic auth 版（第 5 步的备份）
sudo cp /www/server/panel/vhost/nginx/panorama.conf.basicauth.bak \
    /www/server/panel/vhost/nginx/panorama.conf
sudo nginx -t && sudo nginx -s reload
# 2) 停登录服务（可选，basic auth 版不依赖它）
sudo systemctl disable --now rquant-panorama-auth.service
```

回滚后对外恢复 basic auth（`.htpasswd-panorama` 未删，账号照旧可用）——但微信内置浏览器
仍会因不支持 basic auth 而进不去（这正是本次改造要解决的问题，回滚只用于登录服务 /
auth_request 出故障时临时恢复非微信访问）。全景页 8506 与 Mac 侧全程不受影响。
