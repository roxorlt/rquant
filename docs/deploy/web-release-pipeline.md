# Web 发布流水线（rquant-web）

## 产物
`.github/workflows/web-release.yml`：main 上 **Web MVP 检查通过后**（`workflow_run`），构建前端并打包
`rquant-web-<sha8>.tar.gz`（`git archive` 的代码 + `uv.lock` + 构建好的 `web/dist` + `RELEASE.json`），
连同 `.sha256` 发布为 GitHub prerelease，tag `web-<sha8>`。也可手动 `workflow_dispatch` 指定 commit。
仓库是 public，服务器直接 `curl` 下载，**服务器上不放任何 GitHub 凭据**。

## 服务器布局（82.156.0.68，用户 lighthouse）
| 路径 | 说明 |
|---|---|
| `~/rquant-web-rel/<sha8>/` | 一个发布：代码 + `.venv`（`uv sync --frozen --no-dev`，python3.11） |
| `~/rquant-web-rel/current` | 指向当前发布的软链接；`rquant-web.service` 从这里启动 |
| `~/rquant-web-rel/.previous` | 上一个健康发布，`--rollback` 用 |
| `~/rquant-web/app` | 软链接 → `current/web/dist`（nginx `/app/` 静态根），随 `current` 一起切换。首次运行会把旧的目录改名为 `app.pre-pipeline-<时间>` |
| `~/.local/bin/uv` | uv（已安装） |
| `~/bin/rquant-web-deploy` | 部署脚本（来自 `deploy/web/rquant-web-deploy.sh`） |
| `/etc/rquant-web.env` | 配置，**部署脚本不碰** |

## 部署
```bash
ssh -i ~/.ssh/rquant_deploy lighthouse@82.156.0.68 '~/bin/rquant-web-deploy latest'      # 最新
ssh ... '~/bin/rquant-web-deploy 1a2b3c4d'                                              # 指定
ssh ... '~/bin/rquant-web-deploy --rollback'                                            # 回上一个
```
流程：下载并校验 sha256 → 解包到 `~/rquant-web-rel/<sha8>` → `uv sync --frozen` → import 自检
（前三步失败不影响线上）→ 原子切 `current` → `sudo systemctl restart rquant-web` → 30 秒内
`/api/v1/meta` 返回 200 且服务 active、静态 index.html 存在 → 否则打印 journal、自动切回原
`current` 并重启。同一时间只允许一个部署（flock）。

更新脚本本身：`scp deploy/web/rquant-web-deploy.sh ...:~/bin/rquant-web-deploy`。
旧发布不会自动删除（每个约 700MB 含 venv），需要时手工清理，不要删 `current` / `.previous` 指向的目录。

## 注意
- 不改 `/etc/rquant-web.env`（周一 08:50 例行会改 `RQUANT_SERVING_ROOT` 并重启服务）。
- 不碰 runtime、page-control runtime、市场全景（28080）和其它 nginx 站点。
- 手工方式（`docs/deploy/web-mvp-runbook.md` 一期）仍可用，但手工把 `~/rquant-web/app` 换回目录会让静态文件不再跟随 `current`。
