# Web MVP 上线 Runbook（82.156.0.68）

依据 2026-10-10 只读勘察结果编写。PR #324（`mvp/react-readonly`）。分两期上线：**一期只上网页**，**二期打通写路径和盯盘**。

## 勘察结论（实际情况，和旧版 runbook 不一致的地方）
| 项 | 实际 |
|---|---|
| nginx | 宝塔版本 `/www/server/nginx`，站点配置在 `/www/server/panel/vhost/nginx/*.conf`，没有 `/etc/nginx`。另有 openresty 和 docker 里的 nginx，跟本项目无关，不要动 |
| `/app/` | **已经存在**：`rquant-backup.conf`（端口 **8081**），分 `/app/api/`→127.0.0.1:8768、`/app/assets/`、`/app/` 三段，静态目录 `/home/lighthouse/rquant-web/app/`，basic auth 和 backup、dashboard 共用同一个文件 `/www/server/nginx/conf/.rquant-backup.htpasswd` |
| 8768 | 现在被 Codex 的临时进程占用：nohup 启动，不归 systemd 管，代码在 `/home/lighthouse/rquant-web-interim/0516efb…`，**读的是 replay 的 Serving**（`~/replay/runs/20260925T173059-13cb88/...`），不是生产数据 |
| 市场全景 | `panorama.conf` 监听 28080，用 cookie map 做登录门（登录服务 `rquant-panorama-auth`:8507，Streamlit:8506）。**不动** |
| runtime | 正式 runtime 服务都是 `rquant-runtime-<role>@svc-<hash>`，统一用 `/usr/local/libexec/rquant-runtime-exec.pyz` 拉起，每个 instance 有自己的 `LoadCredentialEncrypted`。当前代 `data/runtime/current → generations/e458c47…`，代码版本 producer_commit `df621ef`，由 `rquant-production-deploy.pyz` 发布 |
| page-control | `rquant-page-control.service` 文件已装好，但是 **static 状态，从没启动过**（inactive）。也就是说现在生产上没有写入通道 |
| 盯盘 / Streamlit | `rquant-monitor`（由 timer 触发）和所有 Streamlit 都跑在 `~/rquant` 这份 checkout 上，**版本停在 e4e303b（2026-08-04）**，`.venv` 是 Python 3.14 |
| 工具 | 有 python3.11、node **18**（版本太低，前端构建不了）、htpasswd；没有 uv、pnpm。lighthouse 有 sudo 免密 ALL 权限 |

## 一期实际上线状态（2026-10-10 20:10）
- `rquant-web.service` 读 `EnvironmentFile=/etc/rquant-web.env`：`RQUANT_SERVING_ROOT`（**唯一的数据源开关**）+ 可选 `RQUANT_WEB_NOTICE`（顶栏提示）。当前指向 replay `~/replay/runs/20260925T173059-13cb88/host/data/runtime/serving`，提示“回放数据 2026-09-25（非实时）”。
- 切生产：改 `RQUANT_SERVING_ROOT=/home/lighthouse/rquant/data/runtime/serving`、删 NOTICE 行、`sudo systemctl restart rquant-web`。
- 备份：`/home/lighthouse/backup-web-202610101942`；旧静态 `~/rquant-web/app.old`；Codex 临时 API 启动命令在备份的 `interim-launcher.cmd`。

## 生产 Serving 为何从未发布（只读诊断）
链路：`rquant-runtime-reference-slow-publisher@svc-62c9…` → 产出 `live/reference-slow/serving-authority` → `rquant-runtime-serving@svc-63af…` 汇总 signals/paper_accounts/runtime_health/reference_slow_authority → `data/runtime/serving`。
- serving 发布者：`last_success_at=null`，连续失败 27553 次：`reference_slow_authority reader failed: current authority is unavailable`（该目录不存在）。
- reference-slow 发布者：连续失败 65967 次：`reference slow publisher started after 09:25`。它是常驻服务，**只接受 09:25 前启动**；本实例自 2026-09-25 11:29 起一直运行、从未重启，所以每天都拒绝工作。reference-slow source 本身健康。
- 其它三个来源（signals、paper、runtime_health）都有 current.json，正常。

## 恢复生产发布（需单独批准；交易日 09:25 前执行）
1. 交易日 08:45 前备份：`tar czf ~/backup-refslow-$(date +%F).tgz -C ~/rquant/data/runtime control/reference-slow-publishers control/serving-publishers live/reference-slow`
2. 08:50：`sudo systemctl restart rquant-runtime-reference-slow-publisher@svc-62c9061740150340b1f1e3a8a54323e26794caf9616d34047546383cdc027abd`
3. 看心跳：`control/reference-slow-publishers/svc-62c9…/heartbeats/*.json` 的 `last_success_at` 非空、`generation_published` 出现；`ls live/reference-slow/serving-authority/current.json`
4. 看 serving 心跳 `control/serving-publishers/svc-63af…/heartbeats/*.json`：`last_success_at` 非空；`ls data/runtime/serving/generations` 出现代、有 current 指针
5. 切 web 数据源（见上），`curl 127.0.0.1:8768/api/v1/meta` 返回 ready 且 generated_at 为当天
6. 长期：该服务每日需在 09:25 前重启——应由 production profile 加启动 timer（走 `rquant-production-deploy.pyz` 发布），不要手工加 unit
- 风险：只重启一个已失败的常驻实例，不改代码/凭据/unit；若 09:25 前没起来则当天仍失败（无副作用）。首次发布 Serving 可能暴露下一环问题（schema 合同、配额），需逐个看心跳。
- 回滚：web 改回 replay 路径并重启；reference-slow 状态目录可用第 1 步备份还原（先停该实例再解包再启动）。

## 一期前置条件（2026-10-10 实测拦截）
生产 Serving（`~/rquant/data/runtime/serving/generations`）**从未发布过任何代**（自 9/5 起为空，无 current 指针），`rquant-web` 指向它只会返回“current pointer is missing”。一期必须先让 runtime 的 serving 发布者产出生产代，或经批准临时指向 replay Serving（现状临时进程即如此）。pip 安装需用腾讯镜像 `-i https://mirrors.cloud.tencent.com/pypi/simple/`（pypi.org 实测极慢）。

## 一期：只上网页（安全，不碰 runtime、`~/rquant` 和市场全景）
在 box 上：
1. `cd /workspace/rQuant && git rev-parse --short HEAD`，记下来作为 `$REL`
2. `cd web && pnpm install --frozen-lockfile && pnpm build`（box 上用 Node 22）
3. `git archive HEAD | gzip > /tmp/rquant-$REL.tgz && tar czf /tmp/app-$REL.tgz -C web/dist .`
4. `scp -i ~/.ssh/rquant_deploy /tmp/rquant-$REL.tgz /tmp/app-$REL.tgz lighthouse@82.156.0.68:/tmp/`

在服务器上（`B=/home/lighthouse/backup-web-$(date +%Y%m%d%H%M)`）：
5. 备份：`mkdir -p $B && sudo cp -a /www/server/panel/vhost/nginx/rquant-backup.conf /www/server/nginx/conf/.rquant-backup.htpasswd $B/ && cp -a /home/lighthouse/rquant-web/app $B/app && cat ~/rquant/var/web-interim/api.pid > $B/interim.pid && ps -o args= -p $(cat $B/interim.pid) > $B/interim.cmd`
6. 解包代码：`D=/home/lighthouse/rquant-web-rel/$REL && mkdir -p $D && tar xzf /tmp/rquant-$REL.tgz -C $D`
7. 建 venv：`python3.11 -m venv $D/.venv && $D/.venv/bin/pip install -q $D`（没有 uv，用 pip）
8. 单独建一个 htpasswd，不碰共用的那个：`sudo htpasswd -cbB /www/server/nginx/conf/.rquant-app.htpasswd roxor "$PW"`（`$PW` 在 box 上生成，存到 `/home/box/secrets/rquant-web-basic-auth.txt`，权限 600）
9. 写 `/etc/systemd/system/rquant-web.service`（内容见下），然后 `sudo systemd-analyze verify /etc/systemd/system/rquant-web.service`
10. 停掉临时进程：`kill $(cat $B/interim.pid)`，确认 `ss -ltn | grep 8768` 已经没有输出
11. 起服务：`sudo systemctl daemon-reload && sudo systemctl enable --now rquant-web && curl -fsS 127.0.0.1:8768/api/v1/meta`
12. 换静态文件：`mkdir -p /home/lighthouse/rquant-web/app.new && tar xzf /tmp/app-$REL.tgz -C /home/lighthouse/rquant-web/app.new && mv /home/lighthouse/rquant-web/app /home/lighthouse/rquant-web/app.old && mv /home/lighthouse/rquant-web/app.new /home/lighthouse/rquant-web/app`
13. 改 nginx：只把 `rquant-backup.conf` 里 `/app` 三段的 `auth_basic_user_file` 换成 `.rquant-app.htpasswd`，`sudo sed -i '/location \/app/,/^    }/ s#\.rquant-backup\.htpasswd#.rquant-app.htpasswd#' …`
14. 检查并重载：`sudo /www/server/nginx/sbin/nginx -t -c /www/server/nginx/conf/nginx.conf && sudo /www/server/nginx/sbin/nginx -s reload`
15. 验收：`http://82.156.0.68:8081/app/` 不带凭据返回 401，用 roxor 登录后返回 200，`/app/api/v1/meta` 返回 ready；`curl -s 127.0.0.1:28080/_gate_health` 返回 ok；所有 `rquant-runtime-*` 都还是 active

`rquant-web.service`：
```ini
[Unit]
Description=rQuant Web MVP API
After=network-online.target
[Service]
Type=simple
User=lighthouse
Group=lighthouse
WorkingDirectory=/home/lighthouse/rquant-web-rel/current
Environment=APP_ENV=prod RQUANT_DISABLE_DOTENV=1 RQUANT_SERVING_ROOT=/home/lighthouse/rquant/data/runtime/serving
ExecStart=/home/lighthouse/rquant-web-rel/current/.venv/bin/python -m rquant.web serve --host 127.0.0.1 --port 8768
Restart=on-failure
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
UMask=0077
[Install]
WantedBy=multi-user.target
```
（发布时执行 `ln -sfn $D /home/lighthouse/rquant-web-rel/current`。venv 不能整个搬走，要在 `$D` 里建，并通过 `current` 这个软链接引用。）

一期期间，「确认预警」和「加自选」会返回 502（page-control 没启动），只读页面正常可用。

**一期回滚**：`sudo systemctl disable --now rquant-web`；`sudo cp $B/rquant-backup.conf /www/server/panel/vhost/nginx/` 后执行 `nginx -t && nginx -s reload`；`rm -rf ~/rquant-web/app && mv ~/rquant-web/app.old ~/rquant-web/app`；如果还要恢复临时 API，按 `$B/interim.cmd` 重新执行。

## 二期：写路径和盯盘（需要单独批准，风险高）
1. page-control 和 Serving 的发布方都属于 runtime 代，必须由 `rquant-production-deploy.pyz` 带着 producer_commit 发一个新代（新的 manifests、schema-contracts 和 instance 凭据）。不能手动替换 pyz，也不能手动改 unit。发布前按 `docs/production-release.md` 跑一遍 preflight
2. 新代必须包含 `alert_ack` 和 `manual_watchlist` 两个投影（Serving 物理表指纹 `0f752f67…`），另外要把 page-control 加进 profile 并第一次启动它（`ReadWritePaths` 已经包含 `data/runtime/control`）
3. 回滚：`data/runtime/current` 软链接指回原来那一代 `e458c47…`，然后重启各个 `rquant-runtime-*@` 实例
4. 盯盘读自选要求 `~/rquant` 这份 checkout 升级（从 e4e303b 升到包含本 PR 的 main），而市场全景、dashboard 等 Streamlit 也跑在同一份 checkout 上，**会被一起升级**。建议另建 `~/rquant-monitor-rel/<commit>`，把 `rquant-monitor.service` 的 ExecStart 和 WorkingDirectory 指过去，不碰 Streamlit。回滚就是把 unit 改回去
