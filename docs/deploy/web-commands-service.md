# rquant-web-commands（网页写入与闭环读取通道）

## 架构定位
- Web 应用彻底解耦历史 `rquant.page_control` 守护进程与 Route A Serving 投影表依赖：
  - `src/rquant/web/commands_service.py` 内置独立 Pydantic 命令校验模型、`fcntl.flock` 进程锁与 `command_id` 幂等 JSONL 追加器，零依赖 `rquant.page_control`。
  - **写入路径**：
    1. 若配置了 `RQUANT_PAGE_CONTROL_URL`（如线上 `http://127.0.0.1:8769/v1/commands` 的 `rquant-web-commands.service`），`rquant-web` 经 HTTP 转发写入；
    2. 若未配置外部转发且提供本地命令目录（`RQUANT_WEB_COMMANDS_DIR` 或默认 `~/rquant-web-commands`），`rquant-web` 直接调用 `commands_service.handle()` 幂等落盘。
  - **读取闭环**：`rquant-web`（`GET /pools`、`GET /watchlist`、`GET /alert-rules`、`GET /operations`）、快照候选池构建（`rquant.research.snapshots.build_universe`）与监控规则读取（`rquant.alert_rules.load_alert_rules`）均**直接投影消费** `~/rquant-web-commands/` 下的 JSONL 日志，无需等待 Route A Serving 代际发布即可实时回显。

## 支持命令与存储布局

默认命令目录：`/home/lighthouse/rquant-web-commands/`（可通过 `RQUANT_WEB_COMMANDS_DIR` 覆盖）：

| 命令 (`kind`) | 落盘文件 | 读取消费端 |
|---|---|---|
| `add_watchlist_item` / `remove_watchlist_item` | `watchlist/items.jsonl` | `GET /api/v1/watchlist`、`GET /api/v1/pools`（「自选股」池）、`snapshots.build_universe`、`GET /api/v1/operations` |
| `save_alert_rule` | `alert_rules/rules.jsonl` | `GET /api/v1/alert-rules`、`rquant.alert_rules.load_alert_rules`、`GET /api/v1/operations` |
| `save_canvas` | `canvas_saves/saves.jsonl` | `GET /api/v1/pools`（支持中文/空格池名如 `选股 10-20`、筛选后精确 `codes` 列表及 `mode="existing"` 并入已有池）、`snapshots.build_universe`、`GET /api/v1/operations` |

此外，选股方案与跨端界面偏好由独立 SQLite Web Store（`RQUANT_WEB_STORE_PATH`，默认 `~/rquant-web-store/web_store.sqlite3`）持久化承载：
- `GET / PUT / DELETE /api/v1/screener-schemes`
- `GET / PUT /api/v1/ui-preferences`

## 实时只读数据源（`ResearchSource`）切换

`rquant-web` 支持通过环境变量 `RQUANT_WEB_SOURCE` 选择只读底座：
- `RQUANT_WEB_SOURCE=research`：启用 `ResearchSource`，每次连接使用内存库只读挂载 `RQUANT_RESEARCH_ROOT/market.duckdb`（`ATTACH ... READ_ONLY`）及可选生产只读副本 `DUCKDB_READONLY_PATH`（`rquant_ro.duckdb`，**严禁且代码硬拦截直连主库 `rquant.duckdb`**），动态投影全部标准视图并提供锁死外部访问的只读 SQL 查询台，彻底摆脱冻结的 Route A Serving 快照依赖。
- `RQUANT_WEB_SOURCE=serving`：沿用历史 `ServingSource`（读取 `RQUANT_SERVING_ROOT/current.json`）。
- 未显式设置时：若 `RQUANT_SERVING_ROOT` 未配置且默认 `current.json` 不存在，自动回退至 `ResearchSource`。

## 运维与自检
```bash
sudo systemctl status rquant-web-commands
curl -s http://127.0.0.1:8769/healthz
ls -la ~/rquant-web-commands/{watchlist,alert_rules,canvas_saves}
```

如需在生产 `/etc/rquant-web.env` 切换至实时 `ResearchSource`（须在非交易时段或获明确授权后执行）：
```bash
# /etc/rquant-web.env 追加：
# RQUANT_WEB_SOURCE=research
# RQUANT_WEB_COMMANDS_DIR=/home/lighthouse/rquant-web-commands
# DUCKDB_READONLY_PATH=/home/lighthouse/rquant/data/rquant_ro.duckdb
sudo systemctl restart rquant-web
curl -fsS http://127.0.0.1:8768/api/v1/meta
```
