# Deploy Log

> 每次部署到 82.156.0.68 时追加一条。日期 + tag + 备注 + 回滚命令。
> 最新在最上面。

---

## 2026-07-13 · v0.13.0 candidate · 研究可信度阶段 0（尚未部署）

**状态**：仅本地功能分支，未修改腾讯云生产代码、systemd 或数据。

**候选内容**：

- Strategy Lab 研究记录增加四级可信度 manifest；旧记录自动降级为探索性。
- 页面增加 N 字、科创/创业、集合竞价三项当前可信度警示。
- 新增研究基线、中文总路线图和 GitHub Actions CI。
- 版本元数据和 README 对齐到当前活跃项目状态。

**部署前必须做**：

1. 合并功能分支并确认 GitHub Actions 全绿。
2. 本地启动 Strategy Lab，核验旧历史记录和 Markdown 导出。
3. 本次没有 systemd 改动，无需更新 unit；部署仍使用 `scripts/deploy.sh` 的指定 commit。
4. 实际部署后把本节“尚未部署”改为部署 commit、验证结果和回滚命令。

**当前回滚**：无需回滚；生产尚未包含本候选。

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
