# 全景页微信友好登录网关(cookie 登录页替代 basic auth)

**日期**:2026-07-06　**分支**:feat/panorama-login-gate
**执行**:Opus 4.8 subagent(coding)/ Fable 5(规划·测试设计·验收)
**背景**:微信内置浏览器不支持 HTTP basic auth(不弹框、直接 401)。改成网页登录页
+ 签名 cookie(微信浏览器原生支持 cookie 与表单 POST),朋友微信里点链接即可登录。

## 架构(零第三方依赖,全标准库)

```
手机(微信/任意) → 云 nginx:28080
  ├ 已登录(带有效 cookie) → auth_request 校验 200 → 反代 127.0.0.1:8506(全景页)
  └ 未登录 → auth_request 401 → 跳 /login → 登录服务(127.0.0.1:8507)返回登录表单
       ↓ 用户输账号密码 POST /login
     登录服务校验通过 → Set-Cookie 签名令牌(30 天)→ 302 回 /
```

- **登录服务** `src/rquant/panorama_auth.py`:标准库 `http.server` 单文件服务,
  监听 127.0.0.1:8507,三个端点:
  - `GET /login`:返回移动端友好的登录 HTML 表单(内联 CSS,POST 到 /login);
  - `POST /login`:读 user+password,查用户库校验(pbkdf2),通过则
    `Set-Cookie: rq_panorama=<签名令牌>; Max-Age=2592000; HttpOnly; Path=/; SameSite=Lax`
    + 302 到 `/`;失败回登录页带错误提示;
  - `GET /verify`:nginx auth_request 内部子请求调用,读 Cookie 头里的令牌,
    验签 + 查过期,有效返 200、否则 401(不返回 body)。
- **签名令牌**(无会话存储,自验证):`base64url(user|exp).hmac_sha256(user|exp, SECRET)`,
  SECRET 来自 env `RQUANT_PANORAMA_COOKIE_SECRET`(部署时 `openssl rand -hex 32` 生成一次
  写 .env);验签 = 重算 hmac 比对(`hmac.compare_digest` 防时序)+ exp > now。
  改 SECRET 即全体失效(应急踢人)。
- **用户库** `data/panorama-users.txt`:每行 `user:pbkdf2_sha256$iter$salt_hex$hash_hex`,
  CLI 管理。pbkdf2 用 `hashlib.pbkdf2_hmac`(标准库),iter=200000。

## CLI(用户自助管理账号,傻瓜化)

- `rquant panorama-auth-serve`:启动登录服务(systemd 拉起);
- `rquant panorama-user-add <name>`:交互式(`getpass` 提示输密码,不回显)→
  追加/覆盖该用户 pbkdf2 行到用户库;打印"✅ 已添加/更新 {name}";
- `rquant panorama-user-remove <name>`:删行;
- `rquant panorama-user-list`:列用户名(不含哈希)。

## nginx(rquant-panorama-cloud.conf 改造)

```nginx
server {
    listen 28080;
    server_name _;

    # auth_request 内部校验端点(把 Cookie 透传给登录服务 /verify)
    location = /_panorama_auth {
        internal;
        proxy_pass http://127.0.0.1:8507/verify;
        proxy_pass_request_body off;
        proxy_set_header Content-Length "";
        proxy_set_header Cookie $http_cookie;
    }

    # 登录页(未登录跳这里;本身不校验)
    location /login {
        proxy_pass http://127.0.0.1:8507/login;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    # 全景页本体:先过 auth_request,401 跳登录
    location / {
        auth_request /_panorama_auth;
        error_page 401 = @go_login;

        proxy_pass http://127.0.0.1:8506;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 86400;
    }
    location @go_login { return 302 /login; }
}
```

**auth_request 可用性**:标准 `ngx_http_auth_request_module`,宝塔 nginx 编译版通常含
(部署第一步 `nginx -V 2>&1 | grep -o with-http_auth_request_module` 确认)。**若缺失**
→ 降级方案:`map $cookie_rq_panorama` 静态令牌校验(单一共享密码、令牌值写死在 map,
失登录服务只 set-cookie 不 verify)——部署文档写清两条路,agent 主实现 auth_request 版
+ 附降级 conf 片段。

> Streamlit websocket(`/_stcore/stream`)走 `location /`,auth_request 子请求是普通
> HTTP、能正常校验;已登录 cookie 随 websocket 握手带上,不受影响。

## systemd

`deploy/systemd/rquant-panorama-auth.service`:Type=simple,ExecStart 跑
`rquant panorama-auth-serve`,EnvironmentFile=.env(读 SECRET),Restart=always。
与 rquant-panorama.service 同期常驻。

## 交付物

1. `src/rquant/panorama_auth.py`(登录服务 + 签名/验签 + 用户库读写,纯标准库);
2. CLI 四子命令(cli.py);config 新增 `panorama_cookie_secret` / users 文件路径字段;
3. `deploy/systemd/rquant-panorama-auth.service`;
4. `deploy/nginx/rquant-panorama-cloud.conf`(auth_request 改造 + 降级片段注释);
5. `docs/deploy/2026-07-06-panorama-login-gate-deploy.md`:pair 部署(生成 SECRET、
   建用户、起 auth service、nginx reload、auth_request 可用性检查、微信实测、
   basic auth 保留/移除说明、回滚);
6. 测试 + CHANGELOG。

## 测试用例(全离线,标准库)

- U1 签名令牌:sign→verify 往返成功;篡改 user/exp/mac 任一 → 验签失败;过期 exp → 拒;
- U2 pbkdf2:add→verify 正确密码通过、错密码拒;同密码两次 add salt 不同(哈希不同)、
  均可验;
- U3 用户库 CRUD:add/list/remove 幂等,remove 不存在的 no-op,覆盖同名更新;
- U4 HTTP 端点(用 http.client 打本地起的服务或直接调 handler 逻辑):
  GET /login 返回含 form 的 HTML 200;POST 正确凭据 → 302 + Set-Cookie 含令牌;
  POST 错凭据 → 200 回登录页含错误文案、无 Set-Cookie;GET /verify 带有效 cookie
  → 200、带无效/无 cookie → 401;
- U5 CLI 解析:四子命令 argparse;user-add 走 getpass mock 写库;
- U6 安全:cookie 含 HttpOnly + SameSite;compare_digest 用于验签(代码审查点);
  SECRET 缺失 → 服务启动即报错退出(不静默用空密钥)。

## E 组(Fable 5 验收)

- E1 本地起 auth 服务 8507 + 造 SECRET + 加测试用户 → curl 全流程:
  无 cookie GET /verify=401;POST /login 正确凭据拿 Set-Cookie;带该 cookie
  GET /verify=200;篡改 cookie=401;
- E2 CLI user-add/list/remove 实跑(tmp 用户库);
- E3 云端部署(pair):nginx auth_request 检查 → reload → 手机微信打开 28080
  → 自动跳登录页 → 输账号密码 → 进全景页 → 关掉重开免登录(cookie 生效);
- E4 basic auth 退场:确认 rquant-panorama-cloud.conf 不再有 auth_basic(登录页
  取代),旧 .htpasswd-panorama 保留备用不删。

## 验收标准

U 全绿 + 全量 pytest(底线 977)+ ruff;E1/E2 本地通过;部署文档完整;
无新第三方依赖(hmac/hashlib/http.server/base64/getpass 全标准库);
SECRET 缺失强制报错(不降级空密钥);CHANGELOG。

## 明确不做

- 不做注册/找回密码(用户库 CLI 手动管);
- 不做 HTTPS(28080 仍 http;登录 over http 明文——文档注明风险,后续可上 TLS,
  当前朋友看盘场景可接受,SECRET 防的是 cookie 伪造非嗅探);
- 不改全景页本体、不动 Mac 侧。
