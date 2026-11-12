# rQuant 项目主交接文档（Living Handoff）

> **强制会话规范（所有 AI Agent / 协作者必读）**
> 1. **新 Session 启动必读**：在本项目开启任何新会话时，在修改代码、运行命令或给出方案前，**必须首先完整阅读本文件（`HANDOFF.md`）**，对齐当前真实架构、云地拓扑、红线约束与最新迭代上下文。
> 2. **全部迭代强制记录**：本项目的每一次迭代（`feat` / `fix` / `refactor` / `deploy` / `docs`，含主仓服务、Web 前端、L2 组件及本地衍生工具如 `notch_app/`），在完成交付时**必须将迭代记录追加到本文档末尾「六、迭代记录（Iteration Log）」章节**；若架构、目录、端口或运行状态发生变化，须同步更新本文档正文对应章节。
> 3. **严禁写入敏感信息**：本文档入 Git 仓库，**禁止包含任何真实 Token、密码、API Key、Cookie 或未公开私网凭据**（仅允许记录环境变量名、Keychain 服务名与配置文件路径）。

---

## 一、项目定位与当前真实边界

**rQuant** 是个人自用的 A 股量化选股、实时盯盘、告警推送与可信策略研究平台（当前主线基线标签：`v0.35.7` / `web-d518e931`（L2 常驻组件仍为 v0.35.5 / 8de81101，archive 修复尚未部署））。

### 1.1 核心边界
- **只做**：盘后日线/因子/状态更新 → 条件筛选与候选池生成 → 盘中实时监控（分钟/快照/竞价/量能异动/受控 L2）→ 多通道告警通知（PushDeer / PushPlus）→ Point-in-Time（PIT）可信历史回放、策略评估与自动模拟盘。
- **绝不做**：不接券商实盘交易接口、不自动下单、不做高频交易策略；离线自动参数优化（Strategy Lab）结果绝不自动发布到 live。

### 1.2 历史文档认知对齐（防踩旧文档坑）
仓库早期文档（如 `_index.md`、`TODO.md` 及 `AGENTS.md` / `CLAUDE.md` 中部分 MVP 初期表格）写于 2026-04～05 月，阅读时需以以下**当前真实状态**为准：
1. **前端 UI 已演进为 React SPA**：主力界面已迁至 `src/rquant/web/`（FastAPI）+ `web/`（React 19 + TypeScript + Vite，线上 `:8081/app/`）；旧 Streamlit 页面仅保留市场全景 `src/rquant/dashboard/market_panorama.py`（`:8506` / `:28080`）及预览实例（`:8509`）。
2. **L2 原始行情已作为独立受控组件上线**：`src/rquant/l2/` 采集并缓存单连接节流原始批次，提供本地回环 HTTP/WebSocket 消费与盘后 Cloudflare R2 校验归档（生产常驻 **20 股 / 60 流** 档位，非高频下单用途）。
3. **新增 MacBook 本地五虎刘海插件**：`notch_app/`（macOS 14+ 原生 SwiftUI + `NSPanel` 顶层刘海挂件 `RQuantNotch.app`）配合云端只读聚合接口 `notch_app/server/wuhu_live.py`（`:8775`，经 nginx `:8081/api/v1/wuhu/` 反代）。

---

## 二、仓库架构与模块地图（Codebase Map）

```text
rquant/
├── HANDOFF.md               # 本项目持续交接主文档（新 Session 首读 + 全量迭代记录）
├── AGENTS.md / CLAUDE.md    # 项目级 AI 协作与工程约束规则
├── CHANGELOG.md / DEPLOY.md # 版本变更日志 / 腾讯云生产部署台账
├── src/rquant/              # Python 核心业务包（CLI 入口 rquant.cli:main，40+ 子命令）
│   ├── adapter/ & data/     # Tushare Pro 主源 + AKShare/Ashare/sina/dc 兜底采集
│   ├── storage/             # DuckDB 存储、Schema 迁移与只读副本连接封装（open_readonly_store）
│   ├── indicator/ & state/  # 前复权行情、技术指标（ta/MyTT）与 daily_state 涨跌停/连板派生列
│   ├── screen/              # 通达信/MyTT 风格积木规则与 Pool1/Pool2 选股管线
│   ├── notify/              # PushDeer + PushPlus 独立通道、熔断门禁与事故去重通知
│   ├── research/            # 可信研究平台：PIT 契约、L1 3s 快照采集、R2 归档、竞价池与归因、量能异动、研报因子挖掘
│   ├── runtime_*.py         # 工作负载解耦（Route A）：25 个角色化实例运行时、权威代际发布、Schema Rollout、沙箱与恢复
│   ├── source_broker*.py    # 数据源网关 v2 与 source_quota_* 配额租约治理
│   ├── signal_*.py          # 信号总线、路由 spool 与信号族差分校验（R07）
│   ├── l2/                  # 受控 L2 原始行情采集、.head 批次缓存、HTTP/WS 接口、冷存与 R2 归档
│   ├── risk/                # 负向因子日度 PIT 过滤（risk_flags_daily）与交易所异动监控/预测
│   ├── vp/                  # 三重锁相 Volume Profile 子包（engine/ 只读底座 + sandbox/ 7 文件研究沙盒）
│   ├── dashboard/           # 遗留/专用 Streamlit 页面（market_panorama.py 等）
│   └── web/                 # FastAPI 服务端（只读读 Serving 代 + 3s 板块流合成 + page_control 写转发）
├── web/                     # React 19 + TypeScript + Vite 前端（总览/选股/池子/回测/模拟盘/告警/全景/健康/个股 F9）
├── notch_app/               # MacBook 五虎盯盘刘海插件（Sources/RQuantNotch/*.swift + server/wuhu_live.py）
├── deploy/                  # systemd units & slices、launchd plists、nginx 配置、web 发布脚本、sudoers
├── scripts/                 # 受控生产部署器、云地同步、单日沙箱回放（route_a_day_replay.py）、质量门禁等
├── docs/                    # 架构设计（architecture/）、运维手册（ops/README.md）、L2 规范、研究报告与阶段计划
└── tests/                   # 单元/集成/Web API 测试（pytest）与前端 E2E（web/e2e Playwright）
```

### 关键子系统说明

1. **可信策略研究与 PIT 门禁（Stage 1 & Research Lake）**
   - 严守时间点可见性（Point-in-Time fail-closed）：权威 SSE 交易日历、Tushare 逐日停复牌事实、历史名称/ST 状态、集合竞价最早 `09:26` 可见、`09:30` 分钟回退最早 `09:31` 可见、第 `t` 分钟信号最早 `t+1` 成交、T+1（B 日不 S）。
   - 研究数据湖独立于生产主库：`research.duckdb` + 交易日分区 Parquet，支持 `research-export`、`research-ingest`、`research-repair-auction`、`research-repair-minute`、`dataset-snapshot` 与 `formal-smoke-replay`。
   - 策略成熟度分四级：`exploratory` → `comparable` → `paper_candidate` → `monitor_approved`（当前 N 字、科创/创业放量、集合竞价均处 `exploratory`，待完整资格 manifest 覆盖达标后晋级）。

2. **工作负载解耦与角色化运行时（Workload Isolation / Route A）**
   - 通过 systemd cgroup v2 层级隔离资源：`rquant.slice` 下设 `rquant-live.slice`（含子级 `rquant-live-runtime.slice` 承载运行时 role）、`rquant-serving.slice`、`rquant-research.slice`、`rquant-maintenance.slice`，并由 root-owned `/usr/local/libexec/rquant-workload-arbiter` 保证 `research` 与 `maintenance` 跨平面互斥。
   - 25 个受保护 `rquant-runtime-*@.service` 统一经固定 `/usr/local/libexec/rquant-runtime-exec.pyz` 拉起，只认 `/var/lib/rquant/runtime-authority/current.json` 签发代际。
   - `notifier` 支持三档投递模式：`paused`（急停）/ `shadow`（走完整 live 入库与 `signals` 权威发布，但不向外发包，回执带 `shadow:` 前缀）/ `live`（真实推送）。

3. **全市场 3 秒快照、竞价池与盘中异动（`rquant.research`）**
   - `rquant-snapshot-collector`（工作日 `09:10` 启动）：每 ~3.25s 采集全 A 快照写 `snapshots/YYYY-MM-DD/quotes/part-*.parquet`（30s 正式分片）及 `stream/d-*.parquet`（~3s 增量流，供 Web 板块资金流与刘海插件低延迟读取），内嵌量能异动扫描器 `rquant.research.surge`；盘后 `16:20` 由 `rquant-snapshot-archive` 打包校验归档至 Cloudflare R2（`rquant-snapshots`）。
   - 竞价选股链路：`08:40` 候选池（`rquant-auction-pool-candidates`）→ `09:26:30` 竞价定池（`rquant-auction-pool-pick`，产出 `pools/auction/YYYY-MM-DD.json`）→ `09:27` 概念/新闻/DeepSeek 归因与推送（`rquant-auction-pool-attrib`）。
   - 研报因子挖掘：`rquant-report-factors`（周二至周五 `06:30` 重算）+ `rquant-report-tracking`（周六 `06:30` 增量追踪新金工研报）。

4. **受控 L2 原始行情组件（`src/rquant/l2/`）**
   - 采集器维护唯一上游连接（或受控多连接候选参数，默认关闭），将成交、买委托、卖委托批次原子写入本地缓存并发布 `.head`；本地回环 API 提供股票列表管理、HTTP 游标补取与最多 8 路单向 WebSocket 推送。
   - 生产常驻 20 股核心（`v0.34.0`–`v0.35.5`），已配置 `Restart=on-failure`、`OnFailure=rquant-alert@%n.service` 与交易时段每 2 分钟的 `rquant-l2-collector-watchdog.timer`。严禁与生产共享同一上游设备会话跑并行测试（`run_80_window.sh` 有硬门禁）。

5. **Web 应用（`src/rquant/web/` + `web/`）**
   - 纯只读进程（读 `RQUANT_SERVING_ROOT` 指向的 Serving 代及 `RQUANT_RESEARCH_ROOT` 研究快照/库），仅有的 3 个写动作（保存池子、确认告警、加自选）全部经 `rquant.web.page_control` 转发至 `127.0.0.1:8767/v1/commands`。
   - 个股页提供日 K（含 VP / 筹码分布）、分时（240 分钟连续交易轴 + VWAP/BVC 订单流估算）、5 日分时、TPO 市场轮廓、资金流与财务指标。

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
| **腾讯云生产机** `lighthouse@82.156.0.68` | OpenCloudOS 9.2，宝塔 nginx（`/www/server/panel/vhost/nginx/`）<br>• `~/rquant/`：主仓 checkout 与 `data/`（生产 DuckDB、Runtime 代际）<br>• `~/rquant-research/`：研究根（`market.duckdb`、`intraday.duckdb`、`snapshots/`、`pools/`、`alerts/`、`report_factors/`）<br>• `~/rquant-web-rel/current`：Web 发布软链（`:8768`，静态根 `~/rquant-web/app`）<br>• `~/rquant-wuhu/`：五虎刘海插件只读服务（`:8775`）<br>• `/var/lib/rquant/`：runtime-authority、receipt signer、workload-isolation 状态 | **避坑**：服务器 IP 唯一为 `82.156.0.68`，严禁误写历史错误 IP `82.156.4.48`。SSH 显式指定密钥 `~/.ssh/rquant_deploy`（或 Host `rquant`）。 |
| **本地 MacBook** | `/Users/roxor/brain/30-projects/rQuant`（开发主目录）<br>`/Users/roxor/rquant-notch/notch_app`（本地刘海插件构建目录）<br>`/Applications/RQuantNotch.app`（已安装五虎刘海插件） | 负责本地开发、离线研究/重型回补与云端 DuckDB 热备同步；**禁止**在本地运行 `rquant serve` 生产日度调度。 |

### 3.2 线上核心端口映射（`82.156.0.68`）
- `:8081`（nginx `rquant-backup.conf`）：`/app/` & `/app/api/` → `127.0.0.1:8768`（`rquant-web`）；`/api/v1/wuhu/` → `127.0.0.1:8775`（`rquant-wuhu` Bearer 鉴权）；`/dashboard/` → `:8501`；`/preview/` → `:8509`
- `:28080`（nginx `panorama.conf`）：Cookie 登录门 `:8507` → 市场全景 Streamlit `:8506`
- `127.0.0.1:8767`：`rquant-page-control`（Web 写命令入口）

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
uv run pytest tests/unit/test_<target>.py -q     # 改哪块测哪块（全量 13k+ 用例耗时约 60min）
uv run ruff check <改动的文件>                    # 仅检查本次改动文件（避免历史存量告警干扰）
bash scripts/check-core-quality.sh               # 核心质量门禁

# 2. Web 前后端（Node 22 + pnpm）
cd web && pnpm install --frozen-lockfile
pnpm run lint && pnpm run typecheck && pnpm test
cd .. && uv run pytest -q tests/web

# 3. MacBook 刘海插件（macOS 14+ 本地编译安装）
cd notch_app && ./build.sh && ./install.sh
```

> **CI 现状说明**：GitHub Actions 中历史重型全量套件 `.github/workflows/ci.yml` 已改为仅 `workflow_dispatch` 手动触发；日常 PR 与 `main` 合并门禁为 `.github/workflows/web-mvp.yml`（L2 + Web API + 前端检查），`main` 通过后自动触发 `web-release.yml` 产出 `web-<sha8>` 发布包。

### 4.2 生产发布与高风险变更纪律
1. **主仓受控自动发布**：仅允许通过 `bash scripts/deploy-production.sh --target <exact-tag-or-full-sha>` 发布已合并且 CI 全绿的精确版本（工作日 `09:15–15:10` 自动拒绝重启类发布），发布后同步登记 `DEPLOY.md`。
2. **Web 独立发布**：`deploy/web/deploy-from-box.sh <sha8|latest|--rollback>`（内部调用服务器 `~/bin/rquant-web-deploy`，原子切换 `~/rquant-web-rel/current` 并自检 `/api/v1/meta`，失败自动回滚）。
3. **高风险操作必须单独获用户明确授权**：任何涉及 `deploy/systemd/`（含修改前必须在云端跑 `systemd-analyze verify` 与 `systemd-analyze calendar`）、`deploy/nginx/`、sudoers、生产数据库写入/修复、L2 容量档位切换或凭据轮换的操作，绝不可借日常代码发布绕过。

---

## 五、当前推进重点（Active Focus）

1. **L2 持续稳定性与归档闭环**：观察 20 股常驻核心 + `rquant-l2-collector-watchdog` 自愈表现；独立完成 R2 日终归档目标验证与 timer 启用；评估写盘吞吐后再决定是否推进 80 股档位。
2. **Route A Serving 与 Web 写路径二期**：推进生产 Serving 代日常稳定发布与 `rquant-page-control` 启用，闭环 Web 端「保存池子 / 确认预警 / 加自选」写链路。
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
