# rQuant 项目主交接文档（Living Handoff）

> **强制会话规范（所有 AI Agent / 协作者必读）**
> 1. **新 Session 启动必读**：在本项目开启任何新会话时，在修改代码、运行命令或给出方案前，**必须首先完整阅读本文件（`HANDOFF.md`）**，对齐当前真实架构、云地拓扑、红线约束与最新迭代上下文。
> 2. **全部迭代强制记录**：本项目的每一次迭代（`feat` / `fix` / `refactor` / `deploy` / `docs`，含主仓服务、Web 前端、L2 组件及本地衍生工具如 `notch_app/`），在完成交付时**必须将迭代记录追加到本文档末尾「六、迭代记录（Iteration Log）」章节**；若架构、目录、端口或运行状态发生变化，须同步更新本文档正文对应章节。
> 3. **严禁写入敏感信息**：本文档入 Git 仓库，**禁止包含任何真实 Token、密码、API Key、Cookie 或未公开私网凭据**（仅允许记录环境变量名、Keychain 服务名与配置文件路径）。

---

## 一、项目定位与当前真实边界

**rQuant** 是个人自用的 A 股量化选股、实时盯盘、告警推送与可信策略研究平台（当前主线基线标签：`v0.37.0`（L2 常驻组件仍为 v0.35.5 / 8de81101，archive 修复尚未部署））。

### 1.1 核心边界
- **只做**：盘后日线/因子/状态更新 → 条件筛选与候选池生成 → 盘中实时监控（分钟/快照/竞价/量能异动/受控 L2）→ 多通道告警通知（PushDeer / PushPlus）→ Point-in-Time（PIT）可信历史回放、策略评估与自动模拟盘。
- **绝不做**：不接券商实盘交易接口、不自动下单、不做高频交易策略；离线自动参数优化（Strategy Lab）结果绝不自动发布到 live。

### 1.2 历史文档认知对齐（防踩旧文档坑）
仓库早期文档（如 `_index.md`、`TODO.md` 及 `AGENTS.md` / `CLAUDE.md` 中部分 MVP 初期表格）写于 2026-04～05 月，阅读时需以以下**当前真实状态**为准：
1. **前端 UI 已演进为精简 11 页 React SPA**：主力界面已迁至 `src/rquant/web/`（FastAPI）+ `web/`（React 19 + TypeScript + Vite，线上 `:8081/app/`，已收敛为 11 个核心导航页并移除多人 RBAC/操作审计等伪需求）；旧 Streamlit 页面仅保留市场全景 `src/rquant/dashboard/market_panorama.py`（`:8506` / `:28080`）及停用引导桩。
2. **旧版 Codex Route-A / `page_control` / `source_broker` / `lab` 守护进程体系已在 `v0.36.0` 下线清理**：删除 217 个不可达 Python 模块、35 个遗留脚本与 372 个旧套件测试（净减约 67 万行代码与测试），保留实际运行的 `daily` / `monitor` / `research` / `l2` / `web` / `wuhu` 与 Stage 1 `formal-smoke-replay` 链路。
3. **L2 原始行情已作为独立受控组件上线**：`src/rquant/l2/` 采集并缓存单连接节流原始批次，提供本地回环 HTTP/WebSocket 消费与盘后 Cloudflare R2 校验归档（生产常驻 **20 股 / 60 流** 档位，非高频下单用途）。
4. **新增 MacBook 本地五虎刘海插件**：`notch_app/`（macOS 14+ 原生 SwiftUI + `NSPanel` 顶层刘海挂件 `RQuantNotch.app`）配合云端只读聚合接口 `notch_app/server/wuhu_live.py`（`:8775`，经 nginx `:8081/api/v1/wuhu/` 反代）。

---

## 二、仓库架构与模块地图（Codebase Map）

```text
rquant/
├── HANDOFF.md               # 本项目持续交接主文档（新 Session 首读 + 全量迭代记录）
├── AGENTS.md / CLAUDE.md    # 项目级 AI 协作与工程约束规则
├── CHANGELOG.md / DEPLOY.md # 版本变更日志 / 腾讯云生产部署台账
├── src/rquant/              # Python 核心业务包（CLI 入口 rquant.cli:main）
│   ├── adapter/ & data/     # Tushare Pro 主源 + AKShare/Ashare/sina/dc 兜底采集与 data_catalog 审计
│   ├── storage/             # DuckDB 存储、Schema 迁移与只读副本连接封装（open_readonly_store）
│   ├── indicator/ & state/  # 前复权行情、技术指标（ta/MyTT）与 daily_state 涨跌停/连板派生列
│   ├── screen/ & llm/       # 通达信/MyTT 风格积木规则、Pool1/Pool2 管线与 DeepSeek 白盒自然语言编译
│   ├── factor/ & backtest/  # 自研因子评估、TDX 公式引擎、单策略/组合回测（portfolio/ & perf/）
│   ├── macro/               # 宏观估值/利差/宽度/国家队与机构持仓指标刷新
│   ├── notify/              # PushDeer + PushPlus 独立通道、熔断门禁与事故去重通知
│   ├── research/            # 可信研究平台：PIT 契约、L1 3s 快照采集、R2 归档、竞价池与归因、量能异动、研报因子挖掘
│   ├── l2/                  # 受控 L2 原始行情采集、.head 批次缓存、HTTP/WS 接口、冷存与 R2 归档
│   ├── risk/                # 负向因子日度 PIT 过滤（risk_flags_daily）与交易所异动监控/预测
│   ├── vp/                  # 三重锁相 Volume Profile 子包（engine/ 只读底座 + sandbox/ 7 文件研究沙盒）
│   ├── dashboard/           # 市场全景 Streamlit 页面（market_panorama.py）与旧入口兼容桩
│   └── web/                 # FastAPI 服务端（ResearchSource 实时只读/Serving 兼容 + 3s 板块流 + 命令与偏好闭环）
├── web/                     # React 19 + TypeScript + Vite 前端（精简 11 页导航 + 个股 F9 工作台）
├── notch_app/               # MacBook 五虎盯盘刘海插件（Sources/RQuantNotch/*.swift + server/wuhu_live.py）
├── deploy/                  # systemd units & slices、launchd plists、nginx 配置、web 发布脚本、sudoers
├── scripts/                 # L2 受控组件发布器、云地同步、日度入库、Stage 1 验收与核心质量门禁
├── docs/                    # 架构设计（architecture/）、运维手册（ops/README.md）、L2 规范、研究报告与阶段计划
└── tests/                   # 单元/集成/Web API 测试（pytest）与前端 E2E（web/e2e Playwright）
```

### 关键子系统说明

1. **可信策略研究与 PIT 门禁（Stage 1 & Research Lake）**
   - 严守时间点可见性（Point-in-Time fail-closed）：权威 SSE 交易日历、Tushare 逐日停复牌事实、历史名称/ST 状态、集合竞价最早 `09:26` 可见、`09:30` 分钟回退最早 `09:31` 可见、第 `t` 分钟信号最早 `t+1` 成交、T+1（B 日不 S）。
   - 研究数据湖独立于生产主库：`research.duckdb` + 交易日分区 Parquet，支持 `research-export`、`research-ingest`、`research-repair-auction`、`research-repair-minute`、`dataset-snapshot` 与 `formal-smoke-replay`。
   - 策略成熟度分四级：`exploratory` → `comparable` → `paper_candidate` → `monitor_approved`（当前 N 字、科创/创业放量、集合竞价均处 `exploratory`，待完整资格 manifest 覆盖达标后晋级）。

2. **宿主机资源隔离（systemd slice）**
   - 通过 systemd cgroup v2 层级隔离资源：`rquant.slice` 下设 `rquant-live.slice`、`rquant-serving.slice`、`rquant-research.slice`、`rquant-maintenance.slice`，防止重型研究/因子任务挤占盘中采集与盯盘内存。
   - 早期实验性的 Route-A 25 角色运行时、`source_broker_v2`、`signal_bus`、`page_control` 与 `lab` 守护进程代码已在 `v0.36.0` 完整清理下线。

3. **全市场 3 秒快照、竞价池与盘中异动（`rquant.research`）**
   - `rquant-snapshot-collector`（工作日 `09:10` 启动）：每 ~3.25s 采集全 A 快照写 `snapshots/YYYY-MM-DD/quotes/part-*.parquet`（30s 正式分片）及 `stream/d-*.parquet`（~3s 增量流，供 Web 板块资金流与刘海插件低延迟读取），内嵌量能异动扫描器 `rquant.research.surge`；盘后 `16:50` 由 `rquant-snapshot-archive` 打包校验归档至 Cloudflare R2（`rquant-snapshots`）。
   - 竞价选股链路：`08:40` 候选池（`rquant-auction-pool-candidates`）→ `09:26:30` 竞价定池（`rquant-auction-pool-pick`，产出 `pools/auction/YYYY-MM-DD.json`）→ `09:27` 概念/新闻/DeepSeek 归因与推送（`rquant-auction-pool-attrib`）。
   - 研报因子挖掘：`rquant-report-factors`（周二至周五 `06:30` 重算）+ `rquant-report-tracking`（周六 `06:30` 增量追踪新金工研报）。

4. **受控 L2 原始行情组件（`src/rquant/l2/`）**
   - 采集器维护唯一上游连接（或受控多连接候选参数，默认关闭），将成交、买委托、卖委托批次原子写入本地缓存并发布 `.head`；本地回环 API 提供股票列表管理、HTTP 游标补取与最多 8 路单向 WebSocket 推送。
   - 生产常驻 20 股核心（`v0.34.0`–`v0.35.5`），已配置 `Restart=on-failure`、`OnFailure=rquant-alert@%n.service` 与交易时段每 2 分钟的 `rquant-l2-collector-watchdog.timer`。严禁与生产共享同一上游设备会话跑并行测试（`run_80_window.sh` 有硬门禁）。

5. **Web 应用（`src/rquant/web/` + `web/`，11 个核心导航页）**
   - 支持双只读底座切换：`ResearchSource`（`RQUANT_WEB_SOURCE=research`，以内存 DuckDB 只读挂载 `market.duckdb` 与可选只读副本 `rquant_ro.duckdb`，代码硬拦截直连主库 `rquant.duckdb`，动态投影标准视图）与兼容旧 `ServingSource`（`RQUANT_SERVING_ROOT`）。
   - 侧栏收敛为 **11 个核心页面**：`总览` (`/overview`)、`选股器` (`/screener`)、`池子` (`/pools`)、`因子库` (`/factor`，含自研因子与研报挖掘双 Tab)、`盯盘与告警` (`/monitor`)、`市场全景` (`/panorama`)、`板块资金流` (`/sectorflow`)、`模拟盘` (`/paper`)、`回测` (`/backtest`，含单策略回测 / 组合回测 / 策略版本三 Tab)、`宏观` (`/macro`)、`系统状态` (`/health`，统一整合健康度、systemd 服务/定时器日志抽屉与数据集覆盖率审计）。旧路由（`/experiments`、`/strategies`、`/reportmining`、`/tasks`、`/data`、`/query`）均保留兼容重定向。
   - 单用户无锁闭环与形态 A 选股工作台：`rquant.web.commands_service`（线上 `127.0.0.1:8769` 或进程内直写 `~/rquant-web-commands`）提供幂等 JSONL 落盘与实时读取投影，闭环「加/删自选、保存/并入池子（含筛选后精确股票列表与空格池名）、告警规则」；`/screener` 已升级为形态 A 级联选股工作台（`screener_engine.py` 只读内存 DuckDB 挂载，支持全市场 A 股 / 今日候选池 / 预设 / 自定义与竞价池 / 自选股及并集、交集、排除集合运算，26 个内置规则积木、实时可执行 TDX 公式、全量财务与已验证因子分位过滤、白盒自然语言编译、「存为我的积木」`screener_custom_block` 复用，以及可点击预览任意中间层的逐层漏斗诊断，底层 `upstream + blocks + ranks` 原生预留形态 B 画布兼容）；选股方案、自定义积木与跨端 UI 偏好由独立 SQLite Web Store（`screener_schemes.py`）持久化；已彻底移除多人 RBAC 只读锁、告警 `ack/unack`、`/ops` 操作审计页、网页 SQL 查询与 Brinson/PBO 过度包装指标。
   - 个股页提供日 K（含 VP / 筹码分布）、分时（240 分钟连续交易轴 + VWAP/BVC 订单流估算）、5 日分时、TPO 市场轮廓、资金流、财务指标及一键加/移自选。

6. **MacBook 五虎盯盘刘海插件（`notch_app/`，当前 `v1.0.5`）**
   - **服务端** `notch_app/server/wuhu_live.py`（systemd `rquant-wuhu.service`，监听 `127.0.0.1:8775`，内存上限 300M，nginx `:8081/api/v1/wuhu/` 反代，提供免鉴权 `/health` 与需 Bearer 鉴权的 `/live`，Token 存于服务器 `/etc/rquant-wuhu.env` `0600`）：全程只读读取 `pools/auction/<date>.json`（前 5 名「五虎」，当日池未产出时回退最近日期文件）、`snapshots/<date>/quotes/part-*.parquet`（约 30s 全市场快照）、`snapshots/<date>/stream/d-*.parquet`（约 3s 增量快照）与 `alerts/<date>.jsonl`（末尾 256KB 最近 20 条，按类型映射偏多/偏空/中性），输出实时涨跌、日内涨幅、240 分钟交易轴分时序列（`spark` + `spark_m`），支持 `?replay=1` 按 8s 步进循环回放异动。
   - **客户端** `notch_app/Sources/RQuantNotch/`（macOS 14+，`LSUIElement` 无 Dock 图标，菜单栏「虎」图标，`SMAppService.mainApp.register()` 注册登录项自启；Token 优先读 macOS Keychain `service=rquant-wuhu, account=token`，备用 `~/.config/rquant-notch/config.json` 须 `0600`）：
     - 周一至周五 `09:14–15:05`（Asia/Shanghai）自动显示并每 3s 轮询，盘外自动隐藏并清零未读数（暂按星期判断，法定节假日工作日白天也会显示上一交易日数据）；
     - 展开/收起统一由 `AppDelegate.updateMouseThrough()` 按鼠标几何热区判定（退场动画 ~0.6s 期间整窗完全穿透点击，仅物理刘海可重展开，防止退场残影误触重开；无 AppKit 状态机模拟见 `notch_app/sim/hover_sim.swift`）；
     - 三态交互：**收起态（collapsed）**热区仅占物理刘海（不挡浏览器标签栏点击），有未读五虎异动时在刘海右侧外耳显示未读数（`99+` 封顶）；**异动弹出态（peek，300×56，持续 3s）**提示最新五虎异动；**悬停展开态（expanded，440pt，固定 5 行高）**左栏展示五虎行情与固定全天宽度分时简图（`09:30–11:30` 为左半 `0–50%`，午休 `11:30–13:00` 精确停在 `50%` 半宽，`13:00–15:00` 为右半 `50–100%`，点击跳转 `/app/#/stock/<code>`），右栏三列紧凑展示最新 5 条五虎异动。
   - **运维排障与待办事项**：接口当前经 `:8081` 走明文 HTTP（`Info.plist` 配 `NSAppTransportSecurity` 例外）；客户端仅通过 `NSLog` 写系统统一日志（`log show --last 2h --predicate 'process == "RQuantNotch"'`）。若遇盘中不显示（如进程跨数周未重启或外接显示器切换），按「`curl .../api/v1/wuhu/health` → Keychain Bearer 调 `/live` → 云端 `systemctl status rquant-wuhu` → 本机 `pgrep/ps` 与统一日志 → 菜单栏『虎』刷新或重开 App」由外向内排查。后续待办：接入交易日历跳过节假日、补本地日志文件、长周期窗口自愈与升级 HTTPS。

---

## 三、生产环境拓扑与铁律红线

### 3.1 主机与路径矩阵

| 节点 | 角色与关键路径 | 说明 |
|---|---|---|
| **腾讯云生产机** `lighthouse@82.156.0.68` | OpenCloudOS 9.2，宝塔 nginx（`/www/server/panel/vhost/nginx/`）<br>• `~/rquant/`：主仓 checkout 与 `data/`（生产 DuckDB 与只读副本）<br>• `~/rquant-research/`：研究根（`market.duckdb`、`intraday.duckdb`、`snapshots/`、`pools/`、`alerts/`、`report_factors/`）<br>• `~/rquant-web-rel/current`：Web 发布软链（`:8768`，静态根 `~/rquant-web/app`）<br>• `~/rquant-wuhu/`：五虎刘海插件只读服务（`:8775`） | **避坑**：服务器 IP 唯一为 `82.156.0.68`，严禁误写历史错误 IP `82.156.4.48`。SSH 显式指定密钥 `~/.ssh/rquant_deploy`（或 Host `rquant`）。 |
| **本地 MacBook** | `/Users/roxor/brain/30-projects/rQuant`（开发主目录）<br>`/Users/roxor/rquant-notch/notch_app`（本地刘海插件构建目录）<br>`/Applications/RQuantNotch.app`（已安装五虎刘海插件） | 负责本地开发、离线研究/重型回补与云端 DuckDB 热备同步；**禁止**在本地运行 `rquant serve` 生产日度调度。 |

### 3.2 线上核心端口映射（`82.156.0.68`）
- `:8081`（nginx `rquant-backup.conf`）：`/app/` & `/app/api/` → `127.0.0.1:8768`（`rquant-web`）；`/api/v1/wuhu/` → `127.0.0.1:8775`（`rquant-wuhu` Bearer 鉴权）；`/dashboard/` → `:8501`；`/preview/` → `:8509`
- `:28080`（nginx `panorama.conf`）：Cookie 登录门 `:8507` → 市场全景 Streamlit `:8506`
- `127.0.0.1:8769`：`rquant-web-commands`（Web 幂等 JSONL 写命令入口，落盘 `~/rquant-web-commands`，旧 `8767` `rquant-page-control` 已解耦淘汰）

### 3.3 DuckDB 并发与云地同步强制铁律（最高优先级）
1. **DuckDB 单文件排他锁**：写者（如盘中常驻 `rquant-monitor`）持锁期间，**任何直连主库的新连接（即使 `read_only=True`）都会立即抛 `IOError: Could not set lock on file`**。
2. **只读消费者必走副本**：Web / Dashboard / 临时查询脚本必须使用 `open_readonly_store()` 或 `open_readonly_connection()` 读取 `rquant_ro.duckdb`（由 `rquant-replica-sync.timer` 原子同步），严禁直连 `rquant.duckdb`。
3. **本地热备严禁整文件覆盖主库**：`scripts/sync-from-cloud.sh` 只能下载到独立 `data/cloud_backup.duckdb`，盘后由 `rquant research-sync` 合并进本地 `rquant.duckdb` 并刷新 `rquant_ro.duckdb`（防止 unlink 盘中正在写入的 inode 导致 WAL 损坏）。

---

## 四、常用开发、验证与发布流程

### 4.1 本地开发与验证命令

```bash
# 1. Python 后端与核心单测（worktree 注意指定 Python 3.11）
uv sync --python 3.11
uv run pytest tests/unit/test_<target>.py -q     # 改哪块测哪块（瘦身后全量 4.7k 用例）
uv run ruff check <改动的文件>                    # 检查本次改动文件
bash scripts/check-core-quality.sh               # 核心质量门禁

# 2. Web 前后端（Node 22 + pnpm）
cd web && pnpm install --frozen-lockfile
pnpm run lint && pnpm run typecheck && pnpm test
cd .. && uv run pytest -q tests/web

# 3. MacBook 刘海插件（macOS 14+ 本地编译安装）
cd notch_app && ./build.sh && ./install.sh
```

> **CI 现状说明**：日常 PR 与 `main` 合并门禁为 `.github/workflows/web-mvp.yml`（L2 + Web/Research API + 核心模块单测 + 前端 lint/typecheck/vitest/build/OpenAPI/Playwright E2E），`main` 通过后自动触发 `web-release.yml` 产出 `web-<sha8>` 发布包；`.github/workflows/ci.yml` 为精简后的手动全量验证入口（`workflow_dispatch`）。

### 4.2 生产发布与高风险变更纪律
1. **L2 受控组件发布**：仅允许通过 `bash scripts/deploy-production.sh --component <l2|l2-archive> ...` 发布已合并且验收通过的精确版本，发布后同步登记 `DEPLOY.md`。
2. **Web 独立发布**：`deploy/web/deploy-from-box.sh <sha8|latest|--rollback>`（内部调用服务器 `~/bin/rquant-web-deploy`，原子切换 `~/rquant-web-rel/current` 并自检 `/api/v1/meta`，失败自动回滚）。
3. **高风险操作必须单独获用户明确授权**：任何涉及 `deploy/systemd/`（含修改前必须在云端跑 `systemd-analyze verify` 与 `systemd-analyze calendar`）、`deploy/nginx/`、sudoers、生产数据库写入/修复、L2 容量档位切换或凭据轮换的操作，绝不可借日常代码发布绕过。

---

## 五、当前推进重点（Active Focus）

1. **L2 持续稳定性、段式写盘上限验证与归档闭环**：观察 20 股常驻核心 + `rquant-l2-collector-watchdog`（含帧内行情 lag 告警）表现；推进 40/80 股段式写盘窗口验证与 R2 日终归档组件独立验收部署。
2. **生产 `ResearchSource` 切换与池子编排画布演进（形态 B）**：Wave 1（`v0.35.9` 读写闭环）、Wave 2（`v0.36.0` ~67 万行旧版 Codex 代码清理 + Web 18→11 页收敛）与 Wave 3（`v0.37.0` `/screener` 形态 A 级联选股工作台）均已合入主线；下一步在盘后窗口将生产 `/etc/rquant-web.env` 切至 `RQUANT_WEB_SOURCE=research`，并在形态 A 日常使用稳定后按需推进多分支 DAG 池子编排画布（形态 B，优先适配 MacBook 触控板双指平移与 pinch 缩放手势）。
3. **Stage 1 三策略正式验收晋级**：推进 N 字、科创/创业放量、集合竞价三策略真实资格 manifest 回补与覆盖率达标（baseline ≥95%，eligibility/B/S ≥99%），通过 `formal-smoke-replay` 后从 `exploratory` 晋级。
4. **终端盯盘体验迭代**：Web 桌面/手机端细节打磨与 MacBook `notch_app` 五虎盘中盯盘体验优化。

---

## 六、迭代记录（Iteration Log）

> **维护规则**：每次项目迭代均在此处**按时间倒序置顶追加**（最新记录在最上方）。每条记录须包含：日期、关联模块/版本、变更摘要、关键文件与验证/部署说明（脱敏）。
>
> **记录模板**：
> ```markdown
> ### YYYY-MM-DD · <简短标题>（<版本或分支>）
> - **背景/问题**：...
> - **主要变更**：...
> - **涉及文件**：...
> - **验证与部署状态**：...
> ```

### 2026-11-13 · Wave 3 Web 选股器工作台（形态 A：级联上游范围 + 全量积木/实时 TDX/因子分位 + 白盒自然语言与自定义积木 + 交互式漏斗）（`feat/wave3-screener-workbench` → `v0.37.0`）
- **背景/问题**：
  1. 旧版 Web `/screener` 仅能在已落盘的 `screen_result` 候选池行上做少量数值列的客户端内存过滤（仅涨跌幅、ROE 等极少数列可调），既无法以全市场 A 股、自定义池或竞价池为输入范围，也无法直接调用平台沉淀的 26 个通达信/MyTT 风格内置规则积木、实时可执行通达信选股公式与已验证因子分位。
  2. 自然语言一句话生成此前仅输出只读表达式预览，无法白盒落入当前条件漏斗逐条微调参数或沉淀为个人可复用的「我的积木」；同时漏斗条仅静态展示数字，无法点击查看中间层剩余标的定位错杀。
- **主要变更**：
  1. **级联上游输入范围与多池集合运算（`ScreenerUpstream`）**：支持在 `今日候选池`（`presets`）、`全市场 A 股`（`all_market`）、单个系统预设（`preset`）、已保存池子/竞价选股池（`pool`）与 `自选股`（`watchlist`）之间一键切换，并支持叠加 `+ 并集`（`include_pools`）、`∩ 交集`（`intersect_pools`）与 `− 排除`（`exclude_pools`）级联集合修饰，单页完成「在 A 池基础上排除黑名单池再筛首板」等真实工作流。
  2. **只读内存 DuckDB 实时选股执行引擎（`src/rquant/web/screener_engine.py`）**：使用 `:memory:` 连接只读 `ATTACH` 研究库 `market.duckdb` 与可选只读副本 `rquant_ro.duckdb`（代码级硬拦截直连主库 `DUCKDB_PATH` `rquant.duckdb`，严守单写者排他锁铁律；日期查询采用周末安全的 `max(trade_date) <= target`），服务端实时执行四大类条件积木：
     - **26 个内置规则积木（`type="rule"`）**：完整接入 `rquant.screen.rules` / `rquant.llm.registry`（基础过滤、涨跌停与连板、均线趋势、量价异动、通用表达式比较），自动提取参数 Schema 供前端行内编辑周期、回溯日、板块多选等参数；
     - **实时可执行通达信公式积木（`type="tdx"`）**：经 `rquant.factor.tdx.translate` 校验翻译并在日线面板上通过 `rquant.factor.expr.evaluate` 实时求值；
     - **全量数值/财务与已验证因子积木（`type="field"`）**：覆盖行情、估值、资金流、6 项核心财务指标（`roe`、`grossprofit_margin`、`netprofit_margin`、`netprofit_yoy`、`or_yoy`、`debt_to_assets`）及自研/研报已验证因子横截面分位（`f:<name>`，自动附带 `FactorTracking` 近期 IC 提示），支持 `>=`、`<=`、`between`、`top_pct`（前 N%）与 `bottom_pct`（后 N%）；
     - **个人自定义积木（`type="custom"` / `★ 我的积木`）**：SQLite Web Store（`screener_schemes.py`）新增 `screener_custom_block` 表（并对旧版 4 列 `screener_scheme` 表平滑自动迁移 `upstream_json` / `blocks_json`），支持将单条规则/公式或当前多条件组合一键「★ 存为我的积木」并在积木库搜索复用或删除。
  3. **白盒自然语言编译与交互式逐层漏斗诊断**：新增 `POST /api/v1/screener/from-text`（`compile_nl_to_blocks`，支持 DeepSeek 积木白盒编排与无 Key 本地规则/TDX 双重兜底，硬拒绝未注册积木幻觉），一句话直接拆解追加为可勾选、可改参、可排序的白盒积木；漏斗条（`FunnelStrip`）返回 `step 0..N` 每层剩余数、淘汰数与淘汰占比，支持点击任意中间层（`preview_step`）即时预览该层剩余股票并一键返回最终结果；交互控件全面适配 MacBook 触控板（`pointerdown` 外点关闭、`overscroll-behavior: contain`、无滚轮劫持）。
- **涉及文件**：`src/rquant/web/{screener_engine,screener_schemes,models,routes,__main__}.py`、`web/src/api/{endpoints,useServingQuery}.ts`、`web/src/api/{openapi.json,schema.d.ts}`、`web/src/copy/publicText.ts`、`web/src/pages/screener/{BlockWorkbench,Builder,Conditions,FieldPicker,Schemes,index}.tsx`、`web/src/styles/screener.css`、`tests/web/{test_screener_workbench,test_ui_preferences}.py`、`web/src/pages/screener/workbench.test.tsx`、`web/e2e/smoke.spec.ts`、`CHANGELOG.md`、`HANDOFF.md`。
- **验证与部署状态**：严守并行开发边界（零触碰 `src/rquant/l2/`、`scripts/*l2*`、`scripts/deploy-production.sh` 与 `deploy/`）；`bash scripts/check-core-quality.sh`、`ruff check`、`pytest tests/web`（含全市场/集合运算、26 规则/TDX/因子分位、自定义积木 CRUD、白盒 NL 防幻觉、旧库自动迁移与主库排他锁隔离单测）、前端 `biome ci`、`tsc`、`vitest`（含新增 `workbench.test.tsx`）、`vite build`、OpenAPI 契约一致性与 Playwright E2E（12 用例全绿）全部通过。合并主线打标 `v0.37.0` 产出 `web-573b25bc`，并已于 **2026-11-13 21:33 CST** 原子部署上线至 `82.156.0.68`（`9b15a034` → `573b25bc`，`/api/v1/meta` `state=ready`）。

### 2026-11-13 · Wave 2 旧版 Codex 后端模块大瘦身（净减 ~67 万行）与 Web 18→11 页收敛及伪需求清理（`refactor/wave2-codex-slimming` → `v0.36.0`）
- **背景/问题**：
  1. 历史 Codex 迭代遗留了庞大的实验性 Route-A 25 角色运行时（`runtime_*.py`）、数据源配额网关（`source_broker*.py`、`source_quota*.py`）、信号总线与 R07 差分门禁（`signal_*.py`）、旧签名画布守护进程（`page_control*.py`）及离线 `lab_*` 形式化守护进程，造成主仓积压二十余万行不可达业务代码与四十余万行重型测试，严重拖慢研发与维护。
  2. Web 端存在 18 个碎片化侧栏菜单及多项脱离个人单用户自用定位的「企业级/学术化伪需求」：多人 RBAC 只读访客锁（`viewer | admin` 与 `<ReadOnlyBanner />`）、独立「操作记录」审计页（`/ops`）、重复拆分的回测/实验/策略 3 个入口与长期霸屏的 `<FlowSteps />` 教学条、割裂的系统健康/任务调度/数据中心/网页 SQL 查询 4 个入口（且健康页展示已退役 Route-A 微服务表）、回测详情中的公募 FOF 风格 `Brinson 归因` / `PBO/DSR 过拟合概率` / `AI 解读`，以及拆成两个独立菜单的「因子研究」与「研报挖掘」。
- **主要变更**：
  1. **Web 18 → 11 页导航收敛与伪需求彻底移除**：
     - **A1/A2（删审计与多人 RBAC）**：移除 `/ops` 页面及 `GET /api/v1/operations`；移除 `viewer | admin` 角色区分、`RQUANT_WEB_ADMINS`、`<ReadOnlyBanner />` 与 `useReadOnlyReason()`，通过网关鉴权即具备完整操作权限。
     - **A3/B2（3 页合一为「回测」`/backtest` 并精简机构包装指标）**：将单策略回测、组合回测（原 `/experiments`）、策略版本与事件研究（原 `/strategies`）整合为 `/backtest` 页内三 Tab（`单策略回测` / `组合回测` / `策略版本`），移除 `<FlowSteps />`；保留独立 `/macro` 页，删除 `Brinson 归因`、`PBO/DSR 过拟合统计` 及 `POST /api/v1/portfolio-backtests/{run_id}/explain`（保留 `llm.py` 供自然语言转公式使用）。
     - **A4/B1（运维与数据审计合一为「系统状态」`/health`）**：将分层健康度、关键表新鲜度、真实活跃 systemd 服务/定时器（支持点击打开日志抽屉）与数据集 Schema/覆盖率审计（原 `/data`）整合进单一 `/health` 页，剔除已废弃的 `runtime_services` 表；下线网页版 SQL 查询（`/query` 与 `/api/v1/query*`）及 `/api/v1/fundamentals` 孤立接口。
     - **B3（因子与研报合一为「因子库」`/factor`）**：将自研因子与研报挖掘整合为 `/factor` 页内双 Tab（`自研因子` / `研报挖掘`）；为 `/experiments`、`/strategies`、`/reportmining`、`/reportfactors`、`/tasks`、`/data`、`/query` 保留前端兼容重定向，并同步重生成 OpenAPI 契约与更新 Playwright E2E 冒烟用例。
  2. **切断生产入口中的 Route-A 钩子并完成 AST 可达性精确瘦身（净减 ~67 万行）**：
     - 清理 `monitor.py`、`surge_watch.py`、`pre_market_check.py`、`preflight.py` 与 `cli.py`（`8,845` → `4,690` 行）中的 `legacy_shadow_export`、Route-A 权威回执校验与废弃 CLI 子命令，完整保留日度管线、盘中监控、研究湖与回补、`formal-smoke-replay`、`stage1-acceptance`、竞价池、异动监控、通知与 `notch_app` 五虎接口；严守并行边界，**零触碰** `src/rquant/l2/`、`scripts/*l2*`、`scripts/deploy-production.sh` 及 `deploy/systemd/`。
     - 基于全量入口与受保护套件 AST 传递闭包校验，精确删除 **217 个不可达源码文件**（`src/rquant/` 约 26.6 万行）、**35 个废弃脚本**与 **372 个旧测试/辅助文件及旧分片清单**（清理 `tests/conftest.py` 中的 Route-A autouse 夹具，瘦身后全仓 `4,752` 条用例 `pytest --collect-only` 零错误收集）；同步精简 `.github/workflows/web-mvp.yml` 与手动 `.github/workflows/ci.yml`。
- **涉及文件**：`src/rquant/{cli,monitor,pre_market_check,preflight,surge_watch}.py`、`src/rquant/web/{commands_service,models,routes,source,tasks}.py`（并删除 `ai_explain.py`、`brinson.py`、`overfit_stats.py`、`query.py` 及 217 个旧后端模块与 35 个旧脚本）、`web/src/app/{pages,routes,icons}.tsx`、`web/src/pages/{backtest,factor,health,monitor,pools,screener,stock}/`（并删除 `ops`、`experiments`、`strategies`、`tasks`、`data`、`query`、`reportmining` 独立页）、`web/src/api/{endpoints.ts,openapi.json,schema.d.ts}`、`web/e2e/smoke.spec.ts`、`tests/conftest.py`、`tests/web/*`、`tests/unit/test_{cli,monitor,preflight,surge_watch}.py`（并删除 372 个旧测试文件）、`.github/workflows/{web-mvp,ci}.yml`、`CHANGELOG.md`、`HANDOFF.md`。
- **验证与部署状态**：`bash scripts/check-core-quality.sh` 全绿；`ruff check` 零告警；L2 套件、Web/Research 套件、Roadmap 套件与 CLI/Monitor/Preflight/SurgeWatch/Wuhu 单测全部通过；前端 `pnpm run lint`、`pnpm run typecheck`、`vitest`（31 文件 133 用例）、`vite build`、OpenAPI 契约一致性与 Playwright E2E（11 用例）全绿。合并主线并打标 `v0.36.0` 产出 `web-<sha8>` 发布包。

### 2026-11-12 · Web 平台全链路读写闭环、移除告警「确认/撤销」伪需求、解耦旧 `page_control` & 新增实时只读 `ResearchSource`（`feat/web-closed-loop` → `v0.35.9`）
- **背景/问题**：
  1. 线上 Web 原依赖冻结于 `2026-09-25` 的 Route-A `ServingSource` 快照与旧 `rquant.page_control` 模型；`rquant-web-commands` 虽然已将操作写入 JSONL，但读取端未消费，导致「加自选、保存池子（尤其默认带空格名称如 `选股 10-20` 被旧正则拒收，且未保存筛选后精确股票列表）、告警规则、操作记录」写入后均无法回显，选股方案开关也在构建中被隐藏。
  2. 旧版 Codex 引入的告警逐条「确认 / 撤销确认」（`ack` / `unack` 及总览「N 条告警未确认」横幅）属于多人值班 PagerDuty 式伪需求，不符合个人单用户盯盘场景；同时 Route-A 25 角色控制面与 `page_control` 体系过度沉重，阻碍新版 Web + Research + L2 平台敏捷演进。
- **主要变更**：
  1. **移除告警 `ack` / `unack` 伪需求并简化告警体验**：删除后端 `POST /api/v1/alerts/{ack,unack}`、`AlertItem.acked_at/acked_by`、`alert_ack` 表投影与命令日志；移除盯盘页「未确认」筛选、状态列「确认」按钮及总览页「N 条告警未确认」焦虑提醒；顶栏铃铛改为仅统计当日晚于上次浏览 `/monitor` 时间的最新告警。
  2. **解耦 `rquant.page_control` 并打通核心命令读写闭环**（`src/rquant/web/commands_service.py`、`src/rquant/web/routes.py`、`src/rquant/web/models.py`）：内置独立 Pydantic 命令模型、`fcntl.flock` 幂等 JSONL 落盘与读取投影（`read_watchlist_items` / `read_alert_rules` / `read_saved_pools` / `read_operations`）；放开池名空格限制并支持保存筛选后 `codes` 列表与 `mode="existing"` 并入已有池；新增 `GET /api/v1/watchlist` 与 `DELETE /api/v1/watchlist`；未配置外部转发 URL 时自动直写本地命令目录，`--fixture` 演示模式同样在临时目录内完整闭环。
  3. **接入选股方案与跨端界面偏好持久化**：合入 SQLite Web Store（`src/rquant/web/screener_schemes.py`、`GET/PUT /api/v1/ui-preferences`、`deploy/web/web-store.sh`），并在 `.github/workflows/web-release.yml` 正式开启 `VITE_FEATURE_SCREENER_SCHEMES=1`。
  4. **新增实时只读 `ResearchSource` 摆脱冻结 Serving 代**（`src/rquant/web/source.py`、`src/rquant/web/app.py`）：通过 `RQUANT_WEB_SOURCE=research`（或无 `current.json` 时自动回退）使用内存 DuckDB `ATTACH ... (READ_ONLY)` 挂载 `market.duckdb` 与可选只读副本 `rquant_ro.duckdb`（代码级硬拒绝挂载主库 `DUCKDB_PATH` `rquant.duckdb`，严守单写者文件锁铁律），完整投影 18 张标准视图并提供锁死外部访问的沙盒 SQL 查询；总览模拟盘净值缺失时自动回退 `read_sim()`。
  5. **前后端交互与监控宇宙联动**：选股页从 `GET /api/v1/watchlist` 持久回显「✓ 已在自选」并传递筛选 `codes` + `mode` 保存池子；池子页「自选股」支持单行「移出」；个股 F9 页头部新增「加自选 / ✓ 已在自选」切换；修复 `DataTable` 父组件重渲染时 `flexRender` 卸载重挂单元格 DOM 节点的问题；`snapshots.build_universe` 与 `load_alert_rules` 直连新命令投影与 `market.duckdb`。
- **涉及文件**：`src/rquant/web/{commands_service,source,routes,app,models,__main__,screener_schemes}.py`、`src/rquant/research/snapshots.py`、`src/rquant/alert_rules.py`、`web/src/api/{endpoints.ts,openapi.json,schema.d.ts}`、`web/src/app/{Topbar.tsx,status.ts}`、`web/src/pages/{monitor,overview,screener,pools,stock,ops}/index.tsx`、`web/src/table/DataTable.tsx`、`web/src/theme/storage.ts`、`web/src/copy/publicText.ts`、`deploy/web/web-store.sh`、`.github/workflows/web-release.yml`、`docs/deploy/web-commands-service.md`、`tests/web/{test_closed_loop_and_research_source,test_ui_preferences}.py`。
- **验证与部署状态**：`ruff check` 零告警；`pytest tests/web`（含新增闭环与主库排他锁隔离单测）全部通过；前端 `pnpm run lint`、`pnpm run typecheck`、`vitest` 与 `vite build` 全绿。合并主线产出 `web-<sha8>` 后可先零停机升级 Web，盘后可将 `/etc/rquant-web.env` 切换至 `RQUANT_WEB_SOURCE=research`。

### 2026-11-12 · 修复阻塞发版的 CI flaky 测试（L2 feed 种子优先级 + web poolScope 等待）
- **背景/问题**：`test_receive_pump_prioritises_the_seed_over_queued_pushes` 用墙钟阈值（`seed_wait < 0.5s`、固定 4s 时长）与批次顺序断言，在共享 CI 负载下偶发 0.51s 超限、种子回执数不足，连续阻塞 PR #450/#451/#452 的 main push；`web/src/pages/monitor/poolScope.test.tsx` 的 `waitFor` 使用默认 1s 超时，偶发超时失败。
- **主要变更**：
  1. L2 集成用例改为功能性断言（放宽等待阈值到 2s、待处理帧余量、pumps.received 下限），并新增确定性用例 `test_frame_pump_serves_response_frames_before_queued_pushes`：用脚本化 reader 在 pump 前排队 200 个 push 帧 + 1 个响应帧，断言响应帧先被返回、push backlog 仍在队列中——直接验证优先通道，不再依赖墙钟。
  2. web 测试 setup 增加 `configure({ asyncUtilTimeout: 5000 })`，避免 CI 下 `waitFor` 默认 1s 过紧造成假失败。
- **涉及文件**：`tests/unit/test_l2_feed.py`、`web/src/test/setup.ts`、`HANDOFF.md`。
- **验证与部署状态**：`pytest tests/unit/test_l2_feed.py tests/unit/test_l2_cache.py -q` → 299 passed（新增用例连跑 3 次稳定）；改动仅测试与测试配置，不影响运行时与生产部署；CI 验证后合入 main。

### 2026-11-12 · L2 行情时间新鲜度指标（P0 观测）：status.json 新增帧内行情时间戳 lag + watchdog 滞后告警（`v0.35.8`）
- **背景/问题**：现有 `status.json` 只有本地接收时间（`last_market_received_at`/`last_transport_received_at`）与 `cache_stale`，无法区分「帧在到达但帧内事件时间变旧」和「链路正常」；P0 需要观测行情事件时间新鲜度并在盘中告警。
- **主要变更**：
  1. `FeedStatus` 新增两个候选字段：`last_market_timestamp`（最近已提交批次中最新的帧内行情事件时间；取自成交页表字段 `34320` 的 epoch 秒，订单页布局未携带该字段）与 `market_time_lag_ms`（接收时间 − 事件时间，毫秒、下限 0）。解码、缓存布局与既有 status 语义不变；`source_freshness` 仍为 `unknown`（字段为候选值，非交易所认证时间）。
  2. `feed.py` 新增 `market_event_timestamp(table)` 提取函数（多行取最新、忽略展示用字符串与不合理 epoch），提交批次后仅当能解析出事件时间时更新上述字段。
  3. `scripts/l2-collector-watchdog.sh` 新增滞后告警：交易时段内，若传输仍新鲜（`last_transport` 未超 `STALE_SECONDS`）而 `market_time_lag_ms > LAG_ALERT_MS`（默认 30000）且**连续两次** watchdog 检查（间隔 ≤6 分钟）都超限，则通过 `rquant alert` 推送（独立 dedup key + 15 分钟冷却）；恢复正常后 resolve 并清除首见状态文件，避免开盘 seed 旧数据造成瞬时假报。
- **涉及文件**：`src/rquant/l2/models.py`、`src/rquant/l2/feed.py`、`scripts/l2-collector-watchdog.sh`、`tests/unit/test_l2_feed.py`、`docs/l2-interface.md`、`CHANGELOG.md`、`HANDOFF.md`。
- **验证与部署状态**：`pytest tests/unit/test_l2_feed.py tests/unit/test_l2_cache.py -q` → 全部通过（新增 2 个用例：事件时间提取、状态字段序列化）；watchdog 内嵌判定逻辑用合成 status 手工验证（lag 45s：阈值 30s → LAG、阈值 60s → OK）。已随 PR 合入 main 并发布 **`v0.35.8`**（release `web-<sha8>`）。**部署**：watchdog 脚本可立即单独更新到生产（字段缺失时按 `lag=unknown` 处理）；status.json 新字段需随下一次 L2 组件部署（需新一轮 acceptance-v3 绑定新源码树）后才会出现。

### 2026-11-12 · L2 归档可靠性修复（R2 重试/失败诊断/续跑）与 v0.35.7 发布（`fix/l2-archive-reliability` → `v0.35.6`）
- **背景/问题**：2026-11-10 的日终 R2 归档在运行 4 小时+ 后失败，仅返回 `archive_failed` 且无 traceback，远端 catalog 未推进。本机 R2 大对象实测：PUT/GET 均可用、9/9 往返无损坏，但带宽仅 ~0.7–1.3 MB/s、GET 延迟不稳（3MB 最高 24.8s、64MB 最高 70s）；判断为长串行请求中某次超时/失败而客户端无重试、异常被模块吞掉。前一次运行另被 systemd `TimeoutStartSec=2h` 超时杀掉。
- **主要变更**：
  1. R2 客户端统一重试层（`snapshot_archive.py`）：仅对超时/连接错误/`URLError`/`http.client`/5xx 做指数退避重试（默认 5 次，1/2/4/8s），4xx 立即失败；单请求超时默认 600s，可用 `R2_TIMEOUT_S` 调整。PUT 每次尝试回卷请求体并重新签名；GET 整段下载在重试内完成，响应体中断会删除半成品。条件写遇 412 按“核对远端字节”语义处理，幂等性未变。
  2. 失败可诊断（`archive.py`）：`main` 捕获处生成脱敏错误摘要（类型/消息/cause、stage、`first/last_sequence`、chunk、day），写入 `state_root/archive-error.json` 并打印 stderr；失败 receipt 新增 `error`/`error_artifact`，仍 exit 1；`init`/`run` 成功后清除旧错误文件；artifact 不会写入源 cache。
  3. 幂等续跑：`_new_chunks` 上传前按内容寻址 key 做 HEAD 校验，size+sha256 一致则跳过重复上传（仍做一次完整类型化验证读取）；不一致走原 `immutable=True` 发布，绝不覆盖。catalog 语义未变（仍只在末尾 CAS 提交）。
  4. 分片诊断覆盖：`MAX_SHARD`/`MAX_LINE` 默认不变，支持 `RQUANT_ARCHIVE_SHARD_BYTES`/`RQUANT_ARCHIVE_LINE_BYTES` 覆盖（仅诊断用）。
- **涉及文件**：`src/rquant/research/snapshot_archive.py`、`src/rquant/l2/archive.py`、`tests/unit/test_snapshot_archive.py`、`tests/unit/test_l2_archive.py`、`CHANGELOG.md`、`HANDOFF.md`。
- **验证与部署状态**：`pytest tests/unit/test_l2_archive.py tests/unit/test_snapshot_archive.py -q` → **80 passed**（原 63 + 新增 17）；相关 L2 套件（retention/cold_store/cache_floor/archive_deploy/cache）→ 271 passed；未跑 ruff（环境无该二进制）。已随 PR 合并主线并以 annotated tag **`v0.35.7`** 发布（v0.35.6 已被当日 HANDOFF 文档发布会话占用） GitHub release asset `web-<sha8>`；**尚未部署**到生产（archive 组件部署需 archive 验收 + `deploy-production.sh --component l2-archive`，另行排期）。生产侧当前兜底：archive timer 停用，夜间 retention（`--no-archive-required`，每天 21:00）负责压缩清盘；历史数据（2026-11-09~12）已在冷存与本地备份，回填 R2 需单独方案。
- **运维状态同步**：L2 采集器目前使用 daily start timer（工作日 09:30:10）+ daily stop timer（15:05）；夜间 retention timer 21:00 已启用并改为 archive 修复前的无归档兜底模式。

### 2026-11-12 · 建立项目主交接文档机制、补齐刘海插件 `v1.0.2–v1.0.4` 入库 & 修复盘中分时简图比例尺（`RQuantNotch v1.0.5`）
- **背景/问题**：
  1. 项目历经多阶段演进（MVP → 云地分家 → Stage 1 PIT 研究湖 → Route A 工作负载隔离 → React Web MVP → 受控 L2 → 五虎刘海插件），缺少统一的跨会话主交接文档，部分早期文档与现状存在偏差。
  2. MacBook 五虎刘海插件此前 `v1.0.2–v1.0.4` 源码（几何热区防退场误触重开、收起态右侧未读数外耳、固定 5 行展开布局、盘后未读清零及 `hover_sim.swift`）仅存于本地机器未入 GitHub 主仓。
  3. 插件盘中分时走势简图（`Sparkline`）原按当前已采集点数 `i / (pts.count - 1)` 直接拉伸铺满整个画布宽度，导致早盘 9–10 点仅一两小时的走势被拉宽成全天宽度，午休查看时也铺满全宽而非停在一半位置。
- **主要变更**：
  1. **建立全项目主交接文档与规则**：新增根目录 `HANDOFF.md` 梳理全仓架构、云地拓扑、红线、五虎刘海插件完整上下游与排障手册（脱敏吸收本地插件交接文档要点）；在项目级规则 `AGENTS.md` 与 `CLAUDE.md` 顶部写入强制规范——新 Session 必先读 `HANDOFF.md`，未来全部迭代记录必须追加至 `HANDOFF.md`。
  2. **补齐 `notch_app` `v1.0.2–v1.0.4` 源码入库**：同步 `App.swift`、`Store.swift`、`Views.swift`、`build.sh`、`README.md` 与 `sim/hover_sim.swift` 入库，并在 `.gitignore` 中忽略本地备份目录 `notch_app/rollback/`。
  3. **修复刘海插件分时简图固定全天比例尺（升级至 `v1.0.5`，build `6`）**：
     - 客户端（`Model.swift` / `Views.swift`）：基于 `v1.0.4` 新增 `SessionAxis` 240 分钟 A 股连续交易轴映射（`09:30` → `0%`，`11:30` 及 `11:30–13:00` 午休 → **精确 `50%` 一半宽度**，`13:00–15:00` → `50%–100%`，收盘后及历史回退日 → `100%`，兼容 `--replay` 回放时钟与上游断流保护）。昨收虚线基准保持全天固定总宽度（`30pt`），分时折线按打开/当前交易进度占用对应比例宽度，并支持早盘封板标的的水平延续绘制；仅重编本地 Mac 客户端即可对线上现有接口立即生效。
     - 服务端（`notch_app/server/wuhu_live.py`）：向后兼容新增 `session_min()` 与字段 `spark_m`（每个分时点对应的精确交易分钟槽位 `0..240`），后续盘后同步服务端可进一步消除盘中缺分像素偏移；补充单元测试 `tests/unit/test_wuhu_live.py`。
- **涉及文件**：`HANDOFF.md`、`AGENTS.md`、`CLAUDE.md`、`CHANGELOG.md`、`.gitignore`、`notch_app/Sources/RQuantNotch/{App,Store,Model,Views}.swift`、`notch_app/sim/hover_sim.swift`、`notch_app/build.sh`、`notch_app/README.md`、`notch_app/server/wuhu_live.py`、`tests/unit/test_wuhu_live.py`。
- **验证与部署状态**：`python3 -m unittest tests/unit/test_wuhu_live.py -v` 3 项用例全绿；Mac 本机已执行 `./build.sh && ./install.sh` 编译安装验证通过（`v1.0.5`，build `6`）；云端 `rquant-wuhu` 无需强制重启即可兼容运行（盘后 15:10 后可按需同步 `wuhu_live.py`）。

### 2026-04 ～ 2026-11 · 历史阶段演进基线摘要（详见 `CHANGELOG.md` 与 `DEPLOY.md`）
- **2026-10 ～ 2026-11（`v0.34.0` – `v0.35.5` & `web-*`）**：
  - 受控 L2 原始行情核心上线（20 股 60 流 HTTP/WS 共享缓存、R2 归档与校验恢复、page-0 控制帧容错与单流种子超时隔离、种子流控/冷存可选参数、断线自动重启与每 2 分钟 watchdog 告警）。
  - Web React MVP（`src/rquant/web/` + `web/`）持续迭代：桌面与手机自适应壳、日 K / 分时 / 5 日触控十字线拖拽、TPO 市场轮廓、板块资金流真 3 秒 delta 叠加、两市成交额卡片、告警范围与文案规范化。
  - 新增负向风险因子日度 PIT 过滤与异动监控（`src/rquant/risk/`）、研报因子自动挖掘与周度跟踪（`report_factors` / `report_tracking`）、三重锁相 VP 引擎与沙盒（`src/rquant/vp/`），以及 MacBook 五虎刘海插件（`notch_app/`，`31c70dd` 修复收起态鼠标穿透防遮挡浏览器标签栏）。
- **2026-08 ～ 2026-09（`v0.31.x` – `v0.33.22`）**：
  - 工作负载解耦（Workload Isolation / Route A）落地：systemd slice 层级限额、25 个受保护 runtime role 与 root-owned 执行器、跨代 `schema-rollout`、`notifier` 影子档（`shadow`）与单日沙箱回放工具 `scripts/route_a_day_replay.py`。
- **2026-07（`v0.17.0` – `v0.25.2`）**：
  - 云地职责分家（`2026-07-02`：云端管生产调度与唯一告警权威，本地热备走 `cloud_backup.duckdb` + `research-sync`）；上线盘中全景 `market_panorama`、晨间脉搏、午间战报与量能异动 `surge-watch`。
  - Stage 1 可信策略研究底座：20 个数据集 PIT 契约、权威日历与停复牌快照、独立 `research.duckdb` + Parquet 研究湖、不可变 `manifest` / `snapshot` / `binding` 与 `formal-smoke-replay`。
- **2026-04 ～ 2026-06（`v0.0.1` – `v0.16.x`）**：
  - 完成 Week 1–7 MVP 闭环：Tushare + DuckDB 日线与复权因子接入、技术指标与 `daily_state` 派生层、通达信风格积木筛选与 Pool1/Pool2、APScheduler/systemd 调度、盘中监控、PushDeer/PushPlus 通知、NL 选股与腾讯云受控部署体系。
