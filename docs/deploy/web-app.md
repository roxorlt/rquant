# 网页 `/app/`：目录、权限、发布与回滚

云服务器 82.156.0.68（lighthouse 用户）上，新网页（React 前端 + 只读网页 API）在「第二关」之前不走受控部署器，
用自己的目录和发布脚本 `scripts/web-release.sh`。第一次安装的逐条命令、每条的预期输出和回滚见 `DEPLOY.md`
最上面的「待安装 · 网页 `/app/`」一条；本文说明结构和以后每次发布怎么做。

## 目录

```
/home/lighthouse/rquant-web/          0700，nginx 用户 www 只有 ACL 给的「穿过」权限（--x）
├── repo.git/                         裸仓库，从生产检出 /home/lighthouse/rquant 的 origin 拉 main 和 tag
├── releases/<tag>/                   每个 tag 一个 git worktree，自带 .venv（uv sync --frozen --python 3.11 --no-dev）
│   └── .rquant-web-release           发布脚本写的完成标记（tag + 提交号）；没有它的目录下次会被重建
├── current  -> releases/<tag>        rquant-web.service 的 WorkingDirectory 和 ExecStart
├── app      -> releases/<tag>/web/dist   nginx /app/ 的静态根
├── previous -> releases/<tag>        --rollback 切回的版本
└── releases.jsonl                    每次发布 / 回滚一行（时间、tag、提交号、结果、权限方式 acl/chmod）
```

- 只保留最近 3 个版本（`current` 和 `previous` 永远保留）。
- 生产检出 `/home/lighthouse/rquant`、路线 A 各 unit、`/preview/`、8506 都不受影响。

## 权限

- 脚本以 `umask 0077` 创建一切，只有 lighthouse 能读。
- nginx（宝塔，用户 `www`）只拿到两样：`/home/lighthouse`、`rquant-web`、`releases`、`releases/<tag>`、`releases/<tag>/web`
  这几级目录的 `u:www:--x`（只能穿过，不能列目录），以及 `releases/<tag>/web/dist` 下所有文件的 `u:www:rX`。
  `.venv`、`src`、`.git` 对 www 不可见。
- 主机不支持 ACL（`setfacl` 报错）时，脚本退回 `chmod o+x` 这几级目录、`chmod -R o+rX web/dist`，并在 `releases.jsonl` 记 `access: chmod`。
- 脚本在设完 ACL 后不再 `chmod`（`chmod` 会改写 ACL 的 mask，让 `u:www` 失效），并用 `getfacl` 核对 `index.html` 的实际权限。

## 网页 API 单元

`deploy/systemd/rquant-web.service`：`rquant-serving.slice`，`MemoryHigh=384M`、`MemoryMax=640M`，`Restart=on-failure`，
只监听 `127.0.0.1:8768`，`ProtectSystem=strict` + `ProtectHome=read-only`，serving 根只读，`.env`、密钥目录、主库、只读副本、
备份目录都不可见，不读 `.env`（`RQUANT_DISABLE_DOTENV=1`），只允许回环网络。代码来自 `current` 链接。

lighthouse 能且只能执行 `sudo -n /usr/bin/systemctl restart rquant-web.service`（`deploy/sudoers/rquant-web`，单独的 drop-in，
不改 `rquant-production-deploy`）。重启这个服务不受交易时段限制：它是新的只读服务，不是路线 A 或旧系统的常驻 unit。

## nginx

`deploy/nginx/rquant-backup.conf` 里 `/preview/` 之后的四个 location（`= /app`、`/app/api/`、`/app/assets/`、`/app/`），
与其他页面共用 htpasswd。`/app/api/` 转发 `Host $host:$server_port`（写接口的同源检查要比较 Origin 的主机和端口）
和 `X-Rquant-User $remote_user`（覆盖浏览器自带的同名头）。发布与回滚只切 `app` 链接，不改 nginx。

## 以后每次发布

```bash
# 云服务器 82.156.0.68，lighthouse 用户
TAG=v0.34.1            # 已合入 main 的精确 tag
bash /home/lighthouse/rquant-web/current/scripts/web-release.sh --target "$TAG" --dry-run
bash /home/lighthouse/rquant-web/current/scripts/web-release.sh --target "$TAG"
# 最后一行：web-release: published v0.34.1 (<提交号前 12 位>); previous: v0.34.0; nginx access: acl
```

脚本的顺序：拉取 → 确认 tag 在 main 上 → 建 worktree → `uv sync` → `rquant web-serve --self-check` → 给 nginx 权限 →
切 `current` → 重启 API → 等 `/api/v1/meta` 应答 → 切 `app`。API 没起来就把 `current` 切回上一版并再重启一次，`app` 不动，
退出码 1。同一个 tag 再跑一次什么都不改（「already current」）。

## 回滚

```bash
bash /home/lighthouse/rquant-web/current/scripts/web-release.sh --rollback --dry-run
bash /home/lighthouse/rquant-web/current/scripts/web-release.sh --rollback
bash /home/lighthouse/rquant-web/current/scripts/web-release.sh --status
```

整体撤掉 `/app/`（包括第一次安装后没有上一版可回的情况）见 `DEPLOY.md` 那一条的「回滚」。

## 「第二关」之后

API 跟生产检出一起由 `deploy-production.sh` 发布：`current` 改指 `/home/lighthouse/rquant`，`app` 改指
`/home/lighthouse/rquant/web/dist`（再加一条默认 ACL `setfacl -R -d -m u:www:rX /home/lighthouse/rquant/web/dist`），nginx 不用改。
