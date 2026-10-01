# Web `current_user` 路由清单

对应 [私有身份边界](2026-09-29-web-private-identity-boundary.md)。以下为当前代码中的全部 70 个 `current_user` 依赖路由；`/api/v1` 前缀已包含。公开类即使收到裸用户头，也只得到 `viewer=None`，不能据此取得身份。私有类统一使用已验证的代理身份；已有 CSRF、角色和业务条件仍单独检查。

## 公开读取（20）

| 方法 | 路径 |
|---|---|
| GET | `/api/v1/meta` |
| GET | `/api/v1/overview` |
| GET | `/api/v1/pools` |
| GET | `/api/v1/health` |
| GET | `/api/v1/panorama/pulse` |
| GET | `/api/v1/panorama/boards` |
| GET | `/api/v1/panorama/boards/{board_code}/members` |
| GET | `/api/v1/panorama/stocks/{ts_code}/intraday` |
| GET | `/api/v1/panorama/stocks/{ts_code}/daily` |
| GET | `/api/v1/panorama/surge` |
| GET | `/api/v1/panorama/surge/search` |
| GET | `/api/v1/screen/tdx/preview/source` |
| GET | `/api/v1/screen/blocks` |
| GET | `/api/v1/stocks/search` |
| GET | `/api/v1/stocks/{ts_code}/summary` |
| GET | `/api/v1/data/catalog` |
| GET | `/api/v1/data/catalog/{dataset}` |
| GET | `/api/v1/data/health` |
| GET | `/api/v1/data/issues` |
| GET | `/api/v1/data/fundamentals/summary` |

## 私有读取（29）

| 方法 | 路径 |
|---|---|
| GET | `/api/v1/pools/editor` |
| GET | `/api/v1/paper/accounts` |
| GET | `/api/v1/monitor/channels` |
| GET | `/api/v1/monitor/timeline` |
| GET | `/api/v1/watchlist` |
| GET | `/api/v1/watchlist/{ts_code}` |
| GET | `/api/v1/tasks/jobs` |
| GET | `/api/v1/tasks/jobs/control-capabilities` |
| GET | `/api/v1/tasks/jobs/{job_id}/events` |
| GET | `/api/v1/tasks/overview` |
| GET | `/api/v1/tasks/services/log-capabilities` |
| GET | `/api/v1/tasks/services/{unit}/logs` |
| GET | `/api/v1/backtests` |
| GET | `/api/v1/strategies` |
| GET | `/api/v1/factors/definitions` |
| GET | `/api/v1/factors/capabilities` |
| GET | `/api/v1/factors/results` |
| GET | `/api/v1/factors/results/{job_id}` |
| GET | `/api/v1/factors/run-availability` |
| GET | `/api/v1/backtests/{run_id}` |
| GET | `/api/v1/screen/tdx/market/jobs` |
| GET | `/api/v1/screen/tdx/market/jobs/{task_id}` |
| GET | `/api/v1/screen/tdx/market/jobs/{task_id}/matches` |
| GET | `/api/v1/pools/formula` |
| GET | `/api/v1/pools/formula/{base_name}/members` |
| GET | `/api/v1/data/report` |
| GET | `/api/v1/data/audit-report/calendar` |
| GET | `/api/v1/data/backfill-plans` |
| GET | `/api/v1/data/backfill-plans/{plan_hash}` |

名单按认证 owner 过滤。服务日志能力在未登录时只返回空列表，日志正文仍拒绝；普通任务、池子、监控、研究和审计页面则直接返回 401。公开的 `meta` 在没有有效证明时不显示 viewer。

## 受保护操作（21）

| 方法 | 路径 |
|---|---|
| POST | `/api/v1/screen/tdx/parse` |
| POST | `/api/v1/screen/tdx/preview` |
| POST | `/api/v1/screen/run` |
| POST | `/api/v1/pools/editor/nl-preview` |
| POST | `/api/v1/pools/editor/commands` |
| POST | `/api/v1/monitor/ack` |
| POST | `/api/v1/watchlist/commands` |
| POST | `/api/v1/screen/nl-preview` |
| POST | `/api/v1/screen/tdx/market/commands` |
| POST | `/api/v1/pools/formula/commands` |
| POST | `/api/v1/data/audit-report/commands` |
| POST | `/api/v1/data/backfill-plans/commands` |
| POST | `/api/v1/tasks/jobs/commands` |
| POST | `/api/v1/factors/definitions/save` |
| POST | `/api/v1/factors/definitions/save/resume` |
| POST | `/api/v1/factors/definitions/save/retry` |
| POST | `/api/v1/factors/definitions/{factor_id}/archive` |
| POST | `/api/v1/factors/definitions/{factor_id}/archive/resume` |
| POST | `/api/v1/factors/runs` |
| POST | `/api/v1/factors/runs/resume` |
| POST | `/api/v1/factors/runs/retry` |

公式检查、单股预览、运行选股及自然语言预览不持久写入，但会消耗计算或付费模型资源，所以按受保护操作处理。其余操作由现有 CSRF 与命令准入继续约束；代理证明不替代这些条件。

因子定义保存/归档使用独立编辑名单，运行检验使用独立运行名单；两端均再次核验权限。运行恢复携带完整原请求，仅核对原任务，不重新编译到新来源或新版本。未开放的账号仍可读取已发布因子结果。

## 本片与生产边界

本片只读取 Web 进程专用的合成或运行时 `0400` 凭据副本，且只在显式私有入站配置下验真。Web 副本的父目录检查为 root 或 Web UID 拥有且不可由组/其他用户写入。生产的 nginx 原件及各级父目录必须由 root 控制，专用代理身份读取；当前共享 `www` 配置不满足冻结前提。本片没有修改 nginx、systemd、sudoers 或任何生产凭据，也没有启用生产身份能力。真实 nginx 双头覆写、专用 UID/GID 与凭据交付须经独立 Linux/发布门禁。
