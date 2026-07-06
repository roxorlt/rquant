# 全景页微信友好登录网关部署清单（pair 模式，map 固定令牌方案）

**日期**：2026-07-06　**分支**：fix/panorama-login-map → main（合 PR 后部署）
**目标**：全景页对外入口从 **HTTP basic auth** 改为 **cookie 登录页 + 网关令牌**——
微信内置浏览器不支持 basic auth（不弹框、直接 401），改成网页登录页后，朋友在微信里点
链接即可登录（表单输账号密码 → Set-Cookie 30 天 → 免登录复访）。

> ⚠️ **为什么用 map 方案**：云端 nginx 编译时**没有** ngx_http_auth_request_module
> （`nginx -V 2>&1 | grep auth_request` 无输出），auth_request 指令不可用。故改用 nginx
> `map` 静态比对 cookie：登录服务下发**一个固定网关令牌**（所有已登录用户共用同一 cookie
> 值），nginx map 认这个字面值放行。auth_request 签名令牌版留档（见文件末尾「附录」），
> 将来换成支持 auth_request 的 nginx 可切回。

> Hybrid 分工：Claude 出命令，用户 ssh 上服务器粘贴执行并把输出贴回。**Claude 不直接
> ssh 操作生产**。所有路径用绝对路径。服务器 IP 统一 `82.156.0.68`。

拓扑变化：

```
旧：手机/微信 → 云 nginx:28080（basic auth，微信不弹框直接 401 → 进不去）→ 127.0.0.1:8506
新：手机/微信 → 云 nginx:28080
     ├ cookie 值 == 网关令牌 → nginx map 命中 → 127.0.0.1:8506（全景页 streamlit）
     └ cookie 缺失/不匹配 → map default 0 → 302 跳 /login → 127.0.0.1:8507（登录服务）→ 输密码
          → Set-Cookie 固定网关令牌 → 302 回 /
```

新增常驻组件 **rquant-panorama-auth.service**（标准库 http.server，监听 127.0.0.1:8507），
与 rquant-panorama.service（8506 全景页）同期常驻。**Mac 侧完全不动。**

安全边界与权衡（明确）：
- 28080 仍是 **http 明文**，登录密码 over http 会明文传输——当前朋友看盘场景可接受，
  后续可上 TLS。
- 网关令牌 = `hmac_sha256("panorama-gate", SECRET)[:32]`（由 `RQUANT_PANORAMA_COOKIE_SECRET`
  确定性派生，重启稳定；也可显式配 `RQUANT_PANORAMA_GATE_TOKEN` 覆盖）。nginx 只比对
  cookie 是否等于这个字面值，**不验 hmac 签名**。cookie 带 HttpOnly + SameSite=Lax。
- **权衡**：所有已登录用户共用同一 cookie 令牌，nginx 无法区分是谁 → **无法单独踢某个
  用户**，踢人 = 轮换令牌（改 `RQUANT_PANORAMA_GATE_TOKEN` 或 `..._COOKIE_SECRET` + 重生成
  map 文件 + reload），令**全体**重登。但 per-user 密码仍各自独立，登录审计（谁登录成功）
  在 `journalctl -u rquant-panorama-auth.service` 里仍可分辨。

---

## 0. 前置

- PR 已合 main，用户在云端 `cd /home/lighthouse/rquant && git checkout main && git pull`。
- 全景页 8506（rquant-panorama.service）已在跑（`systemctl status rquant-panorama.service`
  为 active）。本次只在其前面加一层登录网关，不动 8506。

---

## 1. 生成密钥（SECRET）写 .env（用户在云端跑）

```bash
cd /home/lighthouse/rquant
# 生成一次，之后不要随意改（改了网关令牌随之变化、全体 cookie 失效、朋友需重新登录）
SECRET=$(openssl rand -hex 32)
echo "RQUANT_PANORAMA_COOKIE_SECRET=$SECRET" >> .env
grep RQUANT_PANORAMA_COOKIE_SECRET .env      # 确认已写入（一行、64 位 hex）
```

> ⚠️ SECRET 为空时登录服务**拒绝启动**（`serve_auth` 网关令牌为空即 SystemExit，不使用
> 空令牌静默降级）。故这一步必须先做，否则第 3 步 service 起不来（journal 会打印拒启原因）。
> map 方案下 nginx 侧的**网关令牌**由 SECRET 确定性派生（`hmac_sha256("panorama-gate",
> SECRET)[:32]`），第 5 步用 `rquant panorama-gate-token` 打印它写进 map 文件。若想让网关
> 令牌与 SECRET 解耦（例如只轮换令牌不动签名 SECRET），另配 `RQUANT_PANORAMA_GATE_TOKEN=`
> 到 .env（显式值优先于派生）。
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

## 4. 确认 nginx 无 auth_request 模块 + 生成 map 令牌文件（用户在云端跑）

```bash
cd /home/lighthouse/rquant
# 4.1 确认云端 nginx 不含 auth_request（无输出 = 走本 map 方案，符合预期）
nginx -V 2>&1 | grep -o with-http_auth_request_module || echo "无 auth_request 模块 → 用 map 方案"

# 4.2 打印当前生效的网关令牌（由 SECRET 派生或读显式 GATE_TOKEN）
.venv/bin/rquant panorama-gate-token          # 期望打印一行 32 位 hex，非空

# 4.3 把令牌写进 nginx map 文件（一行 `"<GATE_TOKEN>" 1;`，令牌不 checkin，仅落此文件）
echo "\"$(.venv/bin/rquant panorama-gate-token)\" 1;" \
    | sudo tee /www/server/nginx/conf/rq-panorama-gate.map
cat /www/server/nginx/conf/rq-panorama-gate.map   # 确认形如 "33bd...b164" 1;
```

**期望**：4.1 无输出（确认缺 auth_request，正合本 map 方案）；4.2 打印非空 32 位 hex；
4.3 map 文件内容为 `"<32位hex>" 1;`（**令牌两侧有双引号、结尾有分号**，缺一 nginx -t 报错）。
若 4.2 报「均未配置」→ 回第 1 步写 SECRET。

---

## 5. 安装 map 版 nginx conf 并 reload（用户在云端跑）

```bash
# 先备份现网（basic auth 版），便于回滚
sudo cp /www/server/panel/vhost/nginx/panorama.conf \
    /www/server/panel/vhost/nginx/panorama.conf.basicauth.bak

sudo cp /home/lighthouse/rquant/deploy/nginx/rquant-panorama-cloud.conf \
    /www/server/panel/vhost/nginx/panorama.conf
sudo nginx -t && sudo nginx -s reload
```

**期望**：`nginx -t` 通过（`syntax is ok` / `test is successful`），reload 无报错。
- 若报 `open() ".../rq-panorama-gate.map" failed` → 回第 4.3 步生成 map 文件。
- 若报 `"map" directive is not allowed here` → 说明本 conf 未被 include 进 http 块，
  检查宝塔 include 路径（map 必须在 http context，conf 顶部的 `map{}` 依赖 include 位置）。

---

## 6. 验证 28080 登录流（用户在云端或本地跑）

```bash
# 1) 无 cookie 直访 → 302 跳 /login
curl -sI http://82.156.0.68:28080/ | head -3

# 2) GET /login → 200 登录表单
curl -sI http://82.156.0.68:28080/login | head -3

# 3) POST 正确凭据 → 302 + Set-Cookie（固定网关令牌，等于 panorama-gate-token 输出）
curl -s -D - -o /dev/null -X POST \
    --data 'username=<你的用户名>&password=<你的密码>' \
    http://82.156.0.68:28080/login | grep -iE '^HTTP|^Location:|^Set-Cookie:'

# 4) 带该 cookie 访问 / → 200（map 命中，全景页经 nginx 透传）
TOKEN=<上一步 Set-Cookie 里 rq_panorama= 后到分号前的值>
curl -sI -H "Cookie: rq_panorama=$TOKEN" http://82.156.0.68:28080/ | head -3

# 5) 健康探针（不校验 cookie）
curl -s http://82.156.0.68:28080/_gate_health   # 期望 ok
```

**期望**：① 无 cookie `302`（Location: /login）；② `/login` `200`；③ POST 正确凭据
`302` + `Set-Cookie: rq_panorama=<32位hex>; Max-Age=2592000; HttpOnly; Path=/; SameSite=Lax`
（该 hex **等于** `rquant panorama-gate-token` 的输出、也等于 map 文件里的令牌）；
④ 带 cookie `200`；⑤ `/_gate_health` 返回 `ok`。

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

## 9. 轮换网关令牌 / 踢人（用户在云端跑）

map 方案下所有已登录用户共用同一 cookie 令牌，**无法单独踢某个用户**；踢人 = 轮换令牌令
全体重登（per-user 密码不变，重新输密码即可再进）。

```bash
cd /home/lighthouse/rquant
# 方式 A：只轮换网关令牌（不动签名 SECRET）——显式配一个新值，优先于派生
NEW=$(openssl rand -hex 16)
# 若 .env 已有 RQUANT_PANORAMA_GATE_TOKEN 就替换，否则追加
grep -q '^RQUANT_PANORAMA_GATE_TOKEN=' .env \
    && sed -i "s|^RQUANT_PANORAMA_GATE_TOKEN=.*|RQUANT_PANORAMA_GATE_TOKEN=$NEW|" .env \
    || echo "RQUANT_PANORAMA_GATE_TOKEN=$NEW" >> .env
# 方式 B：轮换 SECRET（连带签名令牌一起换，见第 1 步）

# 重启登录服务读新 .env → 重生成 map 文件 → reload
sudo systemctl restart rquant-panorama-auth.service
echo "\"$(.venv/bin/rquant panorama-gate-token)\" 1;" \
    | sudo tee /www/server/nginx/conf/rq-panorama-gate.map
sudo nginx -t && sudo nginx -s reload
```

**期望**：旧 cookie 全部失效（老用户下次访问被 302 跳登录页），新登录下发新令牌。

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
仍会因不支持 basic auth 而进不去（这正是本次改造要解决的问题，回滚只用于登录服务 / map
网关出故障时临时恢复非微信访问）。全景页 8506 与 Mac 侧全程不受影响。

---

## 附录：auth_request 签名令牌方案（留档，将来可切回）

当前云端 nginx 缺 `ngx_http_auth_request_module`，故用 map 固定令牌方案。若将来换成含该
模块的 nginx（`nginx -V 2>&1 | grep -o with-http_auth_request_module` 有输出），可切回
**每用户 hmac 签名令牌**方案，好处是能单独踢人（per-user 令牌互不相同）、cookie 值验签
而非静态比对。

切回要点（`deploy/nginx/rquant-panorama-cloud.conf` 文件末尾保留了该版 server{} 注释）：

1. nginx conf：注释掉 map 版的 `map{}` + `server{}`，启用文件末尾「留档」的 auth_request
   版 `server{}`（含 `location = /_panorama_auth { proxy_pass .../verify; }` 与
   `auth_request /_panorama_auth;`），无需再维护 `rq-panorama-gate.map`。
2. 登录服务：`_handle_login_post` 改回下发 `sign_token(user, exp, SECRET)`（该函数已在
   `panorama_auth.py` 保留），`/verify` 改回 `verify_token`（同样已保留）。
3. `nginx -t` 若报 `unknown directive "auth_request"` → 说明该 nginx 仍缺模块，退回 map 版。
