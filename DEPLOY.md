# Deploy Log

> 每次部署到 82.156.0.68 时追加一条。日期 + tag + 备注 + 回滚命令。
> 最新在最上面。

---

## 2026-07-15 · v0.15.0 · 阶段 1 PR-B PIT 质量守卫

**状态**：PR #78 已 squash merge 为
`22618768aaf6bf507eaeb7ed4c8c42813b19fe4b`。权威交易日历于 7 月 14 日完成初始化，
代码于 7 月 15 日 08:37 部署；云端与本地核验完成后恢复盘中调度。

**部署内容**：

- 历史证券名称/ST 状态、上市/重新上市边界改为 nullable、PIT 且 fail closed。
- 日线/分钟语义审计、分钟时间戳语义归一、复权价格可见性守卫与质量问题落库。
- 涨停池修复采用权威交易日历、稳定 plan id、CAS 重算和调用方事务所有权保护。
- 竞价与科创/创业策略回放必须使用匹配的历史状态，禁止回退当前证券快照。

**生产初始化与验证**：

- 首次部署在 preflight smoke 阶段因 `trade_calendar` 缺少 `2026-07-13` anchor 自动回滚到
  `v0.14.0`；根因是目标版本的 fail-closed 筛选先于部署后的日历 bootstrap 执行。
- 使用同一 `v0.15.0` 临时 worktree 执行幂等 bootstrap，Tushare 返回并验证
  `2020-01-01..2026-12-31` 共 2,557 个自然日、1,697 个交易日；随后正式发布成功。
- 发布后云端 preflight 为 `ok=5 warn=0 fail=0 skip=0`；主库和只读副本日历范围、行数、
  交易日计数完全一致。
- 09:05 重新生成 252M HTTP 生产快照，本地 `cloud_backup.duckdb`、主库和只读副本均完成
  合并并核验为 2,557/1,697；本地 preflight 为 `ok=3 warn=0 fail=0 skip=2`。
- 同时恢复多个 timer 时，monitor 首次启动与一次性备份任务争夺 DuckDB 写锁；backup 与
  replica 成功结束后 monitor 按 `RestartSec=30s` 自动恢复，`NRestarts=1`、10 只 watchlist
  进入盘前阶段。后续恢复顺序应先跑完一次性写任务，再启动 monitor/surge-watch。

**回滚基线**：`bb6141982d65f5ed78ed59c24c6c694d11cbd0c1`（`v0.14.0`）。
代码和依赖仍使用受控发布器自动回滚；已写入的交易日历和新增 schema 向后兼容，代码回滚时
可保留，不得在服务运行期间整文件覆盖 DuckDB。

---

## 2026-07-14 · v0.14.0 · 阶段 1 PR-A 数据可信底座

**状态**：PR #76 已 squash merge 为
`bb6141982d65f5ed78ed59c24c6c694d11cbd0c1`，01:01-01:05 部署并完成生产迁移。

**部署内容**：

- 新增 checksum 固定、事务执行的 DuckDB migration v1-v3；项目版本更新为 `0.14.0`。
- 新增数据集快照、覆盖率、质量问题、PIT 数据契约与权威交易日历基础能力。
- 研究同步改为跨表原子事务；只读副本发布、回滚和提交结果按真实状态报告。
- 此版本不修改策略触发、买卖规则或历史业务数据，不执行历史清理。

**生产迁移**：

- 迁移前在无写锁窗口生成 `backup/latest.duckdb.gz`：主库 264,515,584 bytes，
  压缩快照 110,463,165 bytes，完成时间 `2026-07-14 01:03:31 +08:00`。
- 创建 `schema_migration`、`dataset_snapshot`、`dataset_coverage`、
  `data_quality_issue`、`trade_calendar`；账本精确记录 v1-v3 及固定 checksum。
- 迁移后原表行数保持：`daily_bar=1,617,757`、`screen_result=848`、
  `monitor_event=2,337`；`trade_calendar` 初始为 0 行，留给后续权威日历回补。
- 原子刷新 `rquant_ro.duckdb`，主库与只读副本均为 264,515,584 bytes。

**验证**：

- 本地最终 HEAD：`1276 passed in 31.40s`；GitHub Actions Python 3.11/3.12 均通过。
- 发布器状态为 `deployed`，仅重启部署前 active 的 canvas、dashboard、nl-screen、
  panorama-auth 和 panorama；monitor、surge-watch 保持 inactive。
- 部署后 preflight：`ok=5 warn=0 fail=0 skip=0`，smoke screen 命中 8。
- 8501/8502/8504/8506 四个 Streamlit 健康端点均返回 `ok`。

**回滚基线**：`e3b48c0b358c4fd98748f4a57bb142c900294b4c`。代码/依赖使用受控发布器
自动回滚；新增空表为向后兼容 schema，代码回滚时可保留。如需文件级恢复，使用本次迁移前
`backup/latest.duckdb.gz`，不得在服务运行时直接覆盖主库。

---

## 2026-07-13 · v0.13.2 · 受控自动发布

**状态**：已于 15:42-15:45 部署到腾讯云，commit
`e3b48c0b358c4fd98748f4a57bb142c900294b4c`。

**部署内容**：

- 精确 tag/SHA、main 归属与快进校验；tracked 脏文件和并发部署拒绝。
- diff 自动计算服务重启；工作日 09:15-15:10 有重启需求时自动延期。
- 仅允许 7 个 rQuant 长驻服务走 `sudo -n systemctl restart`；基础设施变更自动拒绝。
- 依赖/preflight/服务健康失败自动回滚；审计写入 `logs/production-deploy.jsonl`。
- 包含尚未上云的 `v0.13.1` preflight 只读副本热修复。

**首次引导**：

1. SSH 22 端口恢复后，使用已授权密钥登录 `lighthouse@82.156.0.68`。
2. 从 `77f6ebf` 精确快进到 `v0.13.2`，执行 `uv sync --frozen`。
3. `visudo` 校验通过后，安装
   `/etc/sudoers.d/rquant-production-deploy`（root:root，0440）并复验授权。
4. 以后的日常发布全部调用
   `scripts/deploy-production.sh --target <exact-ref>`。

**验证**：

- 环境包版本为 `0.13.2`，tracked 工作区干净，仅保留 untracked `backup/`。
- 服务重启前后两次 preflight 均为 `ok=5 warn=0 fail=0 skip=0`。
- 只重启发布前 active 的 canvas、dashboard、nl-screen、panorama-auth 和 panorama；
  已按日程退出的 monitor 和 surge-watch 保持 inactive。
- Dashboard `127.0.0.1:8501/_stcore/health` 返回 `ok`。
- 受控入口复核返回 `already_current`，JSONL 审计时间为
  `2026-07-13T15:45:10+08:00`。

**回滚基线**：`77f6ebfb7782521e5c58ffc2e9226e20af9ac96c`；使用受控发布器的自动
回滚链路，不手工改生产数据。

---

## 2026-07-13 · v0.13.1 · preflight 只读副本热修复

**状态**：PR #73 已合并为 `347d57d`，annotated tag `v0.13.1` 已推送；修复已随
`v0.13.2` 于 2026-07-13 一并上云。

**候选内容**：

- `preflight` 的数据新鲜度与 smoke 筛选优先读取只读副本，避免盘中撞 monitor 主库写锁。
- `lsof` 只有 `mem` 等未分类 FD 时改报“无法判断”，不再误报 monitor 未运行。
- 修正 Stage 0 新增 CI 的上下文作用域，使 Python 3.11/3.12 矩阵能实际创建 job。
- 不改数据库 schema、systemd unit、策略逻辑或生产数据。

**部署后验证**：

1. `rquant.__version__` 输出 `0.13.1`。
2. monitor 运行期间执行 `.venv/bin/rquant preflight`，不再出现 DuckDB conflicting lock。
3. `duckdb_lock_detail` 可以预警未分类 FD，但不应因此令 preflight 失败。

**回滚命令**：`git checkout 77f6ebf && /home/lighthouse/.local/bin/uv sync --frozen`

---

## 2026-07-13 · v0.13.0 · 研究可信度阶段 0

**状态**：已部署到腾讯云，commit `77f6ebf`。

**部署内容**：

- Strategy Lab 研究记录增加四级可信度 manifest；旧记录自动降级为探索性。
- 页面增加 N 字、科创/创业、集合竞价三项当前可信度警示。
- 新增研究基线、中文总路线图和 GitHub Actions CI。
- 版本元数据和 README 对齐到当前活跃项目状态。

**验证**：

- `uv sync --frozen` 完成，包版本由 `0.1.0` 更新到 `0.13.0`。
- `git rev-parse --short HEAD` 为 `77f6ebf`，`rquant.__version__` 为 `0.13.0`。
- 26 个 systemd unit 验证通过，9 个生产 unit 状态正常，monitor 盘中 active/running。
- preflight 的数据新鲜度与 smoke 检查因直连主库撞 monitor 写锁而失败；部署本身正常，
  该问题由上方 `v0.13.1` 热修复处理。

**回滚命令**：`git checkout 20eadf9 && /home/lighthouse/.local/bin/uv sync --frozen`

---

## 2026-05-06 · v0.12.1 · hotfix：nl-screen 只读 DuckDB

**背景**：节后首日 09:30 开盘 monitor 启动失败，38 次 crash-loop + 持续 OnFailure
PushDeer 告警。根因：`rquant-nl-screen.service`（自 4/30 部署起常驻）持有
`/home/lighthouse/rquant/data/rquant.duckdb` 的写锁（PID 2597296），monitor 拿
不到锁就退出。

**部署内容**：

- `DuckDBStore.__init__` 新增 `read_only: bool=False` 参数；read_only=True 时跳过
  `_init_schema()` 的 DDL
- `dashboard/nl_screen.py` 改用 `DuckDBStore(settings.duckdb_path, read_only=True)`
  打开 DB（NL 选股是纯查询场景，与 `dashboard/app.py` 同模式）
- 不动 systemd unit、nginx、依赖

**入口**：无变化（NL screen 仍在 8502 / `/nl/`，monitor 仍 systemd 调度）

**验证**：
- 本地 smoke：read-only 开 DB 不锁、能读、写被拒 ✓
- 云端 hotfix 流程：先停 nl-screen 释放锁 → reset-failed monitor → 起 monitor（active running）→ pull fix 分支 → restart nl-screen ✓
- monitor PID 716834 自 09:49:58 起稳定，nl-screen restart 后 monitor PID 不变 ✓
- ⏳ 待验：浏览器跑 NL query 时 monitor 不被踢（read-only + 写锁理论上不冲突，等真请求覆盖）

**回滚命令**：

```bash
# 回到 v0.12.0
cd /home/lighthouse/rquant
git checkout v0.12.0
sudo systemctl restart rquant-nl-screen.service
# ⚠️ 回滚后会重现 nl-screen 占写锁问题，monitor 与 nl-screen 不能同时跑。
#   要么停 monitor 让 nl-screen 用，要么停 nl-screen 让 monitor 用。
```

**预防**：未来任何想跟 monitor 共存的 DB 消费者（dashboard / nl-screen / 新增 Streamlit / 临时 CLI 查询）必须用 `read_only=True` 打开 DuckDB。

---

## 2026-04-30 · v0.12.0 · Week 7 NL 选股

**部署内容**：

- 新 systemd 服务 `rquant-nl-screen.service`（端口 8502，独立 Streamlit）
- nginx 增加 `/nl/` 反代 → 8502，与 `/dashboard/` 共用 `.rquant-backup.htpasswd`
- `.env` 加入 `DEEPSEEK_API_KEY` / `DEEPSEEK_BASE_URL` / `DEEPSEEK_MODEL`
- `openai>=1.0` 依赖（实际安装 2.33.0）

**入口**：
- 浏览器 http://82.156.0.68:8081/nl/（用 `.rquant-backup.htpasswd` 凭据）
- 监控看板继续走 8501 / `/dashboard/`，无影响

**验证**：
- 内网 `127.0.0.1:8502/_stcore/health` ✓
- 外网 nginx /nl/ 反代 + auth ✓
- 浏览器 NL 流程跑通（解析 + Stage Cards + 运行 + 结果）

**已知遗留**：
- 前端 UX 与最终目标差距较大，下一迭代优化（Week 7.5 真画布会处理一部分）
- `/nl/` 与 `/dashboard/` 共用 htpasswd，未来想给协作者单独开放 NL 时
  按 `deploy/nl-screen.md` "未来：单独开放 NL" 一节切独立 htpasswd

**回滚命令**：

```bash
# 1. 停 systemd 服务
sudo systemctl stop rquant-nl-screen.service
sudo systemctl disable rquant-nl-screen.service
sudo rm /etc/systemd/system/rquant-nl-screen.service
sudo systemctl daemon-reload

# 2. nginx 摘掉 /nl/ location（手编辑 /www/server/panel/vhost/nginx/rquant-backup.conf 删 location /nl/ 块）
sudo nginx -t && sudo systemctl reload nginx

# 3. 代码回滚（main 上拉前一个 tag）
cd /home/lighthouse/rquant
sudo -u lighthouse git checkout v0.11.3
# 或回到 v0.12.0 上一个 commit：
# sudo -u lighthouse git reset --hard v0.11.3
sudo -u lighthouse /home/lighthouse/.local/bin/uv sync

# 4. .env 删除 DEEPSEEK_* 三行（手工编辑或 sed）
sudo -u lighthouse sed -i '/^DEEPSEEK_/d' /home/lighthouse/rquant/.env
sudo -u lighthouse sed -i '/^# ===== LLM (Week 7/d' /home/lighthouse/rquant/.env
```

监控看板（`rquant-dashboard.service`）不受回滚影响，继续运行。
