# Changelog

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 格式，版本号遵循 [SemVer](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Added

- **Week 7.5 C-Canvas-1 — 多画布切换 + Canvas CRUD**：
  - 新增 `src/rquant/dashboard/canvas_files.py`：Canvas 持久化模块
    - `data/canvases/<name>.json` schema：`{name, description, pool_refs, created_at, updated_at, source}`
    - 虚拟默认画布 `__default__`（动态生成含全部 PRESET_SCREENS，不存盘，不可删）
    - `list_canvases() / load_canvas(name) / save_canvas(...) / delete_canvas(...)`
    - `add_pool_to_canvas(canvas, pool) / remove_pool_from_canvas / filter_pool_refs`
  - 改 `src/rquant/dashboard/nl_canvas.py`：
    - sidebar 加 Canvas 切换 selectbox（默认画布总在最前 + 文件系统 canvases 按名排序）
    - 当前画布详情：📦 pool 数 / description / 复制 / 删除（默认不可删）
    - "➕ 新建空 canvas" expander（base name + description → 创建空 canvas）
    - 复制 canvas：保留 pool_refs 写新文件 + 切到新 canvas
    - 删 canvas：unlink 文件 + active fallback 到默认
    - `_build_initial_state(pool_refs)`：根据当前 canvas 的 pool_refs 过滤 PRESET_SCREENS，
      切换 canvas 时画布只显示该 canvas 包含的 pool
    - 「新建 user pool」「fork builtin」自动 `add_pool_to_canvas` 加到 active canvas
      （默认 canvas 跳过 — 自动 include 所有 pool）


- **Week 7.5 部署 — `rquant-canvas.service` (端口 8504) + nginx /canvas/ 反代**：
  - 新增 `deploy/systemd/rquant-canvas.service`：streamlit run nl_canvas.py 端口
    8504 / EnvironmentFile=/home/lighthouse/rquant/.env（复用 DEEPSEEK_API_KEY）
  - 改 `deploy/nginx/rquant-backup.conf`：加 `location /canvas/` 反代到 8504，
    复用 `.rquant-backup.htpasswd`
  - 新增 `deploy/canvas.md`：详细部署清单 + 验证命令 + 故障排查
  - 入口：`http://82.156.0.68:8081/canvas/`（基础认证同 dashboard / nl）

- **Week 7.5 C.2 — NL 改 user pool（DeepSeek + diff 预览）**：
  - 新增 `prompts.build_edit_system_prompt(current_rule_calls)`：编辑场景 system
    prompt 注入当前规则状态，让 LLM 在此基础上输出**完整新规则列表**（不是 patch）
  - `DeepSeekClient.nl_to_screen_plan(query, today, *, current_rule_calls=None)`：
    新增 optional 参数；传 current_rule_calls 时走编辑 prompt，None 时走原 few-shot
    新建 prompt
  - 新增 `src/rquant/dashboard/canvas_nl_edit.py`：
    - `nl_edit_pool(query, current, today)` → list[RuleCall]
    - `diff_rule_calls(old, new)` → (added, removed, unchanged)，args 通过
      args_model 归一化避免类型差异误判
  - user pool 详情底部加 NL 输入区：输指令 → 「📤 解析」→ diff 预览（➕ 绿色
    新增 / ✕ 红色划线删除）→ 「✓ 应用提议到 pending」
  - 用 `st.button(on_click=callback)` 模式而不是 `if st.button():` —— 后者在
    input 改值紧接 button click 的 streamlit widget 嵌套场景下偶尔返回 False


- **Week 7.5 C.3 — 画布 pool CRUD（新建 / 删除 user pool）**：
  - sidebar 加 expander「➕ 新建空 user pool」：输 base name + description → 创建
    空 user pool → runtime merge + 切 active；新 pool 立即出现在画布
  - user pool CRUD 底部加「🗑 删除此 user pool」按钮（二次确认）：物理删除
    `user_presets/<base>.json` + 从 `PRESET_SCREENS` 移除 + 清 active + 重建画布
  - 新增 `canvas_persistence.delete_user_pool(base_name) -> bool`
  - 不在本 PR 范围（推后续）：edge 拖拽创建 / 右键删 edge / lookback_days 编辑

- **Week 7.5 C.4 — builtin pool fork-to-user**：
  - `ScreenPreset` 加 `rule_calls: list[RuleCall]` 字段；presets.py 给两个 builtin
    pool（n-shape-pool1 / pool2）手写维护 rule_calls 元数据（跟 rules 闭包列表
    一一对应）。`load_user_presets` 也填 rule_calls 给 user pool
  - 新增 `canvas_persistence.fork_builtin_to_user(builtin, target=None)`：把 builtin
    的 rule_calls + description + include_columns 写到 `user_presets/<base>.json`
  - canvas 右侧 builtin pool 详情加「🍴 Fork as user/<name>」按钮：fork 后 runtime
    merge PRESET_SCREENS + 切 `active_pool_id` 到新 user pool + 重建画布 state，
    无需重启 streamlit 即可编辑
  - dirty 检测改用 `_normalize_rule_call`（通过 args_model 校验+dump），规避 widget
    写回 int→float 类型差异导致的虚假 dirty banner

### Fixed

- **Week 7.5 C.1.2 — 画布 selected_id 在 popover / 拖动后被重置 bug**
  （`src/rquant/dashboard/nl_canvas.py`）：用户反馈两个交互 bug：
  (1) 在「➕ 加规则」popover 内点 selectbox / 加入按钮，rerun 时 streamlit-flow
  返回 `state.selected_id = None`，右侧详情立刻退回「点击左侧节点」空状态；
  (2) 拖动节点完成时 react-flow 也把 selected_id 清成 None，详情同样消失。
  修：把当前选中 pool 单独存到 `st.session_state.active_pool_id`，只有在
  streamlit-flow 返回的新 selected_id 是合法 pool 名（命中 `PRESET_SCREENS`）时
  才更新；None / 未知值不覆盖。右侧详情读 `active_pool_id` 而不是
  `canvas_state.selected_id`。
  playwright e2e 自测 6 个 case 全过（click 节点 / click 空白 / 拖动 / popover
  加规则 / 改 args / 保存写盘）。

### Changed

- **Week 7.5 C.1.1 — 画布 UI 精致化**（`src/rquant/dashboard/nl_canvas.py`）：用户反馈
  3 个痛点的修复——
  - 页面标题改 "rQuant 画布"（去掉"选股"）；CSS 注入压缩 `.block-container` 顶部 padding +
    把 streamlit header 缩到 2.2rem，画布顶到顶部
  - 左侧 `st.sidebar` 工具栏：trade_date 显示 / 「🔄 重置画布布局」/「🗑 清诊断缓存」/ Pool
    列表（🟦 user/⚪ builtin 区分）
  - 画布高度 560 → 720，**节点 `draggable=True`**（用户可拖位置）
  - 右侧详情列重做：
    - 删 `st.expander` 包裹，规则改紧凑行（`N · name · args inline | 📋 复制 | ✕ 删除`）
    - 参数 widget 改了 → 自动进 pending（取消原"应用参数"按钮）
    - 加规则改 `st.popover` 弹出（小窗选规则 + 加入）
    - diagnostic / 命中表折叠到 `st.expander`（默认展开漏斗、折叠命中表）
    - dirty banner 单行：改动概述 + ↩ 撤销 + 💾 保存（按钮宽度均分）
    - 顶部一行 💡 引导提示：「改参数自动进 pending，改完点保存写盘」

### Added

- **Week 7.5 C.1 — NL 画布 user pool 规则 CRUD**（`src/rquant/dashboard/nl_canvas.py`）：
  user/ 前缀的 pool 现在可以在画布上**编辑规则**：
  - 新增 `src/rquant/dashboard/canvas_persistence.py`：`load_user_pool_rule_calls()` /
    `save_user_pool()`，读写 `user_presets/<base>.json`（schema 兼容 v0.12.0 nl_screen 落库格式）
  - 新增 `src/rquant/dashboard/canvas_rule_editor.py`：基于 RuleSpec.args_model（Pydantic）
    反射生成 inline 编辑 widget（int → number_input / float → number_input / bool → checkbox /
    str → text_input）；`rule_spec_options()` 列所有 26 条积木供 `+ 加规则` 下拉
  - 右侧面板新增 CRUD 块（仅 user/）：每条规则 expander（应用参数 / × 删除）+
    `+ 加规则（模板）` 下拉 + `加到 pending` + Pending banner（撤销 / 保存）
  - builtin pool（n-shape-pool1 / pool2）显示 info 提示「不可编辑」（rules 是闭包，参数
    无法反查；fork-to-user 留到后续）
- **Week 7.5 B — NL 画布接入 per-rule diagnostic + 命中标的预览**（`src/rquant/dashboard/nl_canvas.py`）：
  在 A spike 基础上加入完整 read-only 体验：
  - 新增 `src/rquant/dashboard/canvas_diagnostic.py`：`diagnose_preset()` 一次 load_universe + 内存
    incremental apply 规则（性能从 N 倍 SQL → 1 次 SQL + N 次 pandas mask）；递归处理
    depends_on（父 preset 跑出 ts_codes 作为子 preset 的 ts_whitelist）
  - DuckDB `read_only=True` 跟 monitor / nl-screen 共存（不抢写锁）
  - `st.cache_data(ttl=300)` 缓存单 pool diagnostic 结果，避免重复点击重算
  - 右侧面板：pool 元信息 / **diagnostic 漏斗表格**（规则名 / 保留数 / % of 初始）/ 命中标的表
  - `latest_trade_date(store)` 自动选 daily_bar 最新交易日，无需用户输入
- **`screen/rules._bool_state_rule` 增加 `__rquant_name__` 属性**：内部工厂闭包的 `__qualname__`
  失意义（变成 `_bool_state_rule.<locals>._rule`），canvas diagnostic 显示规则名时退化为
  "_bool_state_rule"。挂 friendly 名（如 `is_first_limit_up(1)` / `not_is_limit_up(0)`），
  canvas 显示规则漏斗时识别度大幅提升。
- **Week 7.5 A spike — `src/rquant/dashboard/nl_canvas.py`**（独立 Streamlit 应用，端口 8503）：
  用 `streamlit-flow-component` 渲染 `PRESET_SCREENS` 中所有 pool 为节点 + `depends_on`
  关系为 edge。点节点 → 右侧面板显示 pool 描述 / 依赖 / 规则数 / 规则名列表 / include_columns。
  spike 决策门已通过：playwright smoke 验证 0 console errors / 渲染 OK / click→state sync OK，
  下一步走 B 阶段（接 per-rule diagnostic 漏斗 + 命中标的预览）。
  依赖：`streamlit-flow-component>=1.6.1`。
- **`scripts/sync-from-cloud.sh` 检测源 stale（5/13 复盘 Task #8）**：每次 sync 完
  额外拉 `latest.json` 取 `snapshot_at`，跟本地 `data/.last-sync-snapshot-at` 比较。
  intraday 时段下 snapshot_at 持续不变（源没在 5min 步进）→ 推 PushDeer
  `[RQ][WARN] backup intraday 卡住`，含云端排查命令。
  防刷屏：相同 stale 状态 30 分钟内只推 1 条。
  防 v0.11.3 翻车再发：本地 sync log 不再用 "sync OK: 215M" 假阳性掩盖源没在跑的真相。

### Fixed

- **`scripts/deploy.sh` 已 enabled 的 timer daemon-reload 后没 restart，新 OnCalendar
  从不生效**（v0.11.3 翻车根因，5/13 复盘）：v0.11.3 在 git 改对了
  `rquant-backup.timer` 的 OnCalendar，但部署到云端后**timer 已经在 active 状态**，
  daemon-reload 只重新读 unit 文件，没让 timer 用新调度——结果 backup intraday
  自 v0.7.0 起从未真跑过（云端 timer 一直用旧的被 systemd 静默拒收的 OnCalendar）。
  改 step [3] 逻辑：所有改动的 timer（不只是新增的）都执行 restart。
- **`scripts/deploy.sh` 缺 post-deploy 验证，OnCalendar 拒收 silent fail**：新加
  step [5/6]，用 `systemctl show -p NextElapseUSecRealtime --value` 检查每个改动
  timer 的下次 trigger。`n/a` 或 > 24h → exit 2，让 caller 知道翻车。
- **`scripts/monitor-watchdog.sh` 调 systemctl 没加 sudo，自愈被 polkit 拒**
  （5/6 翻车实锤，5/13 复盘）：5/6 09:30-09:48 monitor 没起来，watchdog 每 2min
  检测到 → 发 alert → `systemctl start rquant-monitor.service` → polkit 返回
  `Interactive authentication required`。`lighthouse` 用户**早已** `NOPASSWD ALL`，
  脚本忘加 sudo → 死循环 40 分钟无法自愈。改：`systemctl start` 加 `sudo`；
  start 失败时再发 `[RQ][CRITICAL]` 升级告警（区分 sudoers / polkit / unit 错）。
  新增 watchdog 日志 tag `restart-failed`。
- **`rquant daily-report` 用写模式开 DuckDB，撞别人持的写锁 fatal exit**
  （5/1 翻车实锤，5/13 复盘 Bug A）：5/1 15:30 节假日 daily-report 触发时，
  `DuckDBStore()` 用默认写模式打开活 DB，撞上 nl-screen 旧版（v0.12.1 hotfix
  前还是写模式，PID 2597296）持的锁，立刻 `IOException: Could not set lock`
  fatal exit。当日**没收到日报推送**。修：`health.generate_and_send_daily_report`
  改用 `DuckDBStore(read_only=True)`（count_today_business_data 全是 SELECT，
  本来就不需要写）。套路同 v0.12.1 nl-screen hotfix。
- **`preflight unit_files` 误报 15/15 失败**：`systemd-analyze verify` 会顺带把
  系统其他 unit（如腾讯云 `tat_agent.service` 的 `PIDFile= references a path
  below legacy directory /var/run/` warning）的 stderr 也吐出来，原代码把「stderr
  非空」一律算 fail。修：只看 exit code + 只把 stderr 中**包含本 unit 名**的行
  当真错。
- **`scripts/deploy.sh` services 状态多余 `unknown` 行**：bash `state=$(cmd ||
  echo unknown)` 在 `cmd` 退码非 0 但有 stdout（systemctl is-active 对 inactive
  退码 3 + 输出 "inactive"）时，`||` 会把 echo 也拼进 state，导致 "inactive\nunknown"
  两行。改用 `state=$(cmd) && true; [[ -z $state ]] && state=unknown`。

### Added

- **`rquant preflight`**：手动触发的全家服务深度体检（5/6 incident 复盘 P2 #5）。
  跟 `pre-market-check`（被动定时）的差别：preflight 是**主动深度** dry-run，
  典型场景：节后第一天开盘前 / 大 PR merge 后 deploy 完 / 怀疑系统状态时随手跑。

  5 项检查：
  - `unit_files`：对 `deploy/systemd/*.{service,timer}` 跑 `systemd-analyze verify`
  - `systemd_state`：8 个 unit 的 ActiveState/SubState/NRestarts/start 时间戳详情
  - `duckdb_lock_detail`：lsof 列每个持有者的 PID + COMMAND + FD 模式（u/r/w）
  - `data_freshness`：daily_bar / screen_result / monitor_event 最新 trade_date + 行数
  - `smoke_screen`：跑一次 `n-shape-pool1` preset 端到端，确认 screen 流水线还活着

  CLI: `rquant preflight [--notify]`（默认只 stdout markdown，--notify 推 PushDeer 摘要）。
  非 timer，纯手动；本地 mac 跑 systemd 项自动 skip。


- **`scripts/deploy.sh`**：云端一键部署脚本。功能：
  - `git pull` 并展示变更文件
  - `deploy/systemd/` 改动 → 自动 cp 到 `/etc/systemd/system/` + `daemon-reload`
  - 新增的 `*.timer` 自动 `enable --now`
  - 按 Python 路径 → service 关联表（在脚本里用 `svc_pattern()` case），
    只 restart **运行中** 且**实际受影响** 的 service（不无脑全部重启）
  - 改动的 `*.service` 文件本身也触发对应 service restart
  - 输出部署后 timer + service 状态汇总

  支持 `--dry-run`（只打印不执行）和 main-only 安全栏（非 main 分支拒绝跑）。
  替代以前手动跑 5-7 条命令的部署流程，sudo 密码只输一次。

- **`rquant pre-market-check`** + systemd timer：每个交易日 09:00（开盘前 30min）主动
  跑 5 项体检，PushDeer 推一条「✅ 通过」/「⚠️ N 项要修」摘要。检查项：
  - DuckDB 文件锁状态（多写锁持有者 = 5/6 incident 重现，直接 fail）
  - 数据分区剩余空间（< 5GB warn）
  - Tushare 积分余额（< 500 warn，附到期时间）
  - 8 个 systemd unit 的 `is-active` 状态
  - 近 24h 各 unit 的 ERROR 日志条数（≥ 10 warn）

  失败项触发 `OnFailure=rquant-alert@%n.service` 兜底；warn 不触发 OnFailure（PushDeer
  已经说明）。把 5/6 那种「9:30 monitor 启动失败才发现」的事故前移到 9:00 主动发现。

  新增文件：
  - `src/rquant/pre_market_check.py`
  - `deploy/systemd/rquant-pre-market-check.service` + `.timer`

### Fixed

- **nl-screen 独占 DuckDB 写锁导致 monitor crash-loop**（2026-05-06 节后首日真实回归）：
  `dashboard/nl_screen.py` 用默认 `DuckDBStore()` 打开 DB（写模式），与 `rquant-monitor`
  抢锁，monitor 启动即 `IO Error: Could not set lock on file ...rquant.duckdb`，38 次
  crash-loop + 持续 OnFailure 告警。修复：`DuckDBStore.__init__` 新增 `read_only: bool=False`
  参数（read_only=True 时跳过 `_init_schema()`），`nl_screen.py` 改为 `read_only=True`
  开 DB（NL 选股是纯查询场景，与 `dashboard/app.py` 一致）。
  部署后 monitor 与 nl-screen 可共存。

---

## [v0.12.0] — 2026-04-30 — Week 7：自然语言选股（NL → 积木）

新建 `src/rquant/dashboard/nl_screen.py` 作为**独立 Streamlit 应用**（与监控看板
完全隔离，独立 URL / 端口 / auth）：用户用一句中文描述筛选意图，DeepSeek-V4-Flash
解析为结构化 ScreenPlan（按 stage 分层），可视化卡片预览/编辑后跑 screen()
出表格，可一键保存为 user preset 接入 daily pipeline。

### Added

- `src/rquant/llm/`：完整 LLM 集成模块
  - `schemas.py`：ScreenPlan / Stage / RuleCall Pydantic 模型 + trade_date YYYY-MM-DD 验证
  - `registry.py`：26 条积木 RuleSpec 注册表（Pydantic args model + examples + category）
  - `schema_export.py`：to_openai_tools() 生成 OpenAI Tool Calls schema + build_rule_catalog_md() 系统提示用规则目录
  - `dispatch.py`：ScreenPlan → list[Rule] → screen()，含 per-rule 累加命中诊断
  - `prompts.py`：system prompt + 4 条 few-shot examples（DeepSeek thinking 模式带 reasoning_content）
  - `client.py`：DeepSeekClient（OpenAI SDK + DeepSeek base_url），retry 3次指数回退 + jsonl 日志 + 澄清场景识别
- `data/user_presets/*.json`：NL 输入保存的 preset，启动时合并到 PRESET_SCREENS（user/ 前缀）
- `src/rquant/dashboard/nl_screen.py`：**独立 Streamlit 应用**（不是 multi-page tab）。
  本机 dev `streamlit run src/rquant/dashboard/nl_screen.py --server.port 8502`，
  与 8501 监控看板互不干扰；NL 页面**无 meta refresh**，编辑时不会被打断。
- `deploy/systemd/rquant-nl-screen.service` + nginx `/nl/` 反代 + `deploy/nl-screen.md`：云端部署 8502 端口的 NL 应用
- `scripts/llm_smoke.py`：手动 smoke test（5 个真实 query，已验证）
- `.env.example`：DEEPSEEK_API_KEY / DEEPSEEK_BASE_URL / DEEPSEEK_MODEL
- 40+ 单元测试覆盖 schema / registry / completeness / schema_export / dispatch / client mock / prompts / user_presets

### Changed

- `pyproject.toml`：+ openai>=1.0（实际安装 2.33.0）
- `src/rquant/config.py`：+ deepseek_api_key / deepseek_base_url / deepseek_model 字段 + deepseek_enabled property
- `src/rquant/presets.py`：+ load_user_presets() loader，启动自动 merge `data/user_presets/*.json`
- `src/rquant/dashboard/app.py`：保持原监控看板内容 + 路径不变（systemd 服务零迁移）

### Verified

- 396 tests passing（baseline 340 + 56 新增）
- 真实 DeepSeek API smoke test 5 个 query：4 个产出合理 plan，1 个模糊 query 走澄清路径
- 监控看板 (port 8502) + NL 页面 (port 8503) 各自 healthcheck `/_stcore/health` 通过
- user preset roundtrip 验证：写 JSON → 重启读 PRESET_SCREENS → key 含 `user/` 前缀

### Why

`screen/rules.py` 已有 26 个积木但每次跑新组合必须翻函数列表写 Python。Week 7
让用户用中文描述意图直接触达积木组合。Stage Cards 形态为 Week 7.5 真画布
（streamlit-flow / react-flow）预留数据结构（每 stage 直接映射成节点）。

**两 app 拆分而非 multi-page**：监控看板 30s 自动刷新（meta refresh）会打断 NL
页面交互编辑；且未来部署上 nginx 反代时希望两页面分别配 auth（监控仅自己访问，
NL 可选择性对外开放），独立 Streamlit 应用比 `st.navigation` 多页更天然。

---

## [v0.11.3] — 2026-04-30 — Backup intraday timer 同根 bug 修复

### Fixed
- **`deploy/systemd/rquant-backup.timer`：v0.7.0 引入的 `09:30..15:05/5` 同样
  被 systemd 拒收**（v0.11.1/v0.11.2 watchdog 同根 bug）。意味着自 v0.7.0
  起 **intraday backup 从未跑过**，本地 `sync-from-cloud` 09:30-15:05 时段
  拉到的 `.gz` 都是昨天 17:30 那份。
- 改用 v0.11.2 验证过的 `9..15:0/5` 语法，09:00-15:55 每 5min 一次。

### Why 现在才发现
今天 sync log 显示 09:30-15:05 时段 `latest.duckdb.gz` 一直 208M 没变化
——如果 intraday backup 在跑，每 5min 应有 KB 级变化。结合 watchdog
踩同坑，确认 backup intraday 一直没 work。日终 17:30 那条 OnCalendar 用
的是 `Mon..Fri 17:30` 简单语法，一直正常。

### 影响范围
- mac 端"热备"实际是日级别，不是 5min 级——但用户场景里没出过事，因为
  daily 流水线 17:00-17:10 完成、17:30 backup snapshot 之前都不更新业务
  数据，意外的分钟级回滚没发生过
- 修复后 mac 本地 sync 在交易时段能拉到接近实时（5min 滞后）的 cloud 状态

### Deploy
```bash
ssh lighthouse@82.156.0.68
cd /home/lighthouse/rquant && git pull origin main

# 部署前先验证语法（CLAUDE.md 新规：systemd 改动必须 cloud 端 systemd-analyze）
systemd-analyze calendar 'Mon..Fri *-*-* 9..15:0/5' --iterations 5
# 期望：Iteration#2 = 09:05:00（5 分钟步进）

sudo cp deploy/systemd/rquant-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart rquant-backup.timer

# 验证：从 bad-setting 恢复（如有），NEXT 应是下个 5min 边界
systemctl status rquant-backup.timer --no-pager | head -5
systemctl list-timers rquant-backup.timer --no-pager
```

5/1 节假日 Mon..Fri 不触发，下次 work 是 5/6 周二节后开盘。

---

## [v0.11.2] — 2026-04-30 — Watchdog OnCalendar syntax fix v2

### Fixed
- **`deploy/systemd/rquant-monitor-watchdog.timer`：v0.11.1 的 `*-*-* 09..14:*/2`
  **也**被 systemd 拒绝**（`Failed to parse calendar specification: Invalid
  argument`）。timer 进入 `bad-setting` 状态，`Trigger: n/a`，完全不排队。
  cloud `systemd-analyze calendar` 实测 4 个候选语法：

  | 候选 | 结果 |
  |---|---|
  | `*-*-* 09..14:*/2` | ❌ Invalid（v0.11.1 推的） |
  | `*-*-* 9..14:0/2` | ✅ Iteration#2 = 09:02:00（2 分钟步进） |
  | `9:30/2` | ✅ 单小时 9:30/9:32/9:34 |
  | `9:0/2` | ✅ 单小时 9:00/9:02/9:04 |

  采用候选 2：`OnCalendar=Mon..Fri *-*-* 9..14:0/2`。
  关键差异：minute 字段不接受 `*/N`（通配步进），但接受 `0/N`（显式 start=0 step=N）。

### Why v0.11.1 没在 mac 测出
mac 没装 systemd，本地测不了 OnCalendar 语法。v0.11.1 推的语法看起来"更通用"，
但实测 systemd 还有更严的解析规则。这次 cloud 直接 `systemd-analyze` 4 个
候选并行验证，找到能 work 的最简形式。

### Deploy
```bash
ssh lighthouse@82.156.0.68
cd /home/lighthouse/rquant && git pull origin main
sudo cp deploy/systemd/rquant-monitor-watchdog.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart rquant-monitor-watchdog.timer
# 验证：从 bad-setting 恢复，NEXT 显示 Fri 09:00:xx
systemctl status rquant-monitor-watchdog.timer --no-pager | head -5
systemctl list-timers rquant-monitor-watchdog.timer --no-pager
```

---

## [v0.11.1] — 2026-04-30 — Watchdog OnCalendar 语法 hotfix

### Fixed
- **`deploy/systemd/rquant-monitor-watchdog.timer`：v0.10.2 双段 OnCalendar
  syntax 实际不工作**。部署后 `systemd-analyze calendar` 揭示：
  - `Mon..Fri 09:30..11:30/2` → `Failed to parse: Invalid argument`（systemd
    不接受跨小时的分钟范围）→ **早盘段整段静默丢弃**
  - `Mon..Fri 13:00..15:00/2` 解析成功，但 `/2` 是 **2 秒**步进不是 2 分钟
    → 下午段每 2s 触发一次（13:00-15:00 共 3600 次）
  改为单行 `Mon..Fri *-*-* 09..14:*/2`（hour 范围 + 分钟 */2），watchdog 脚本
  自检 09:30-15:00 交易窗口外 log `out-of-window` 静默退。

### Added
- `scripts/monitor-watchdog.sh` 加 NOW_HM 自检：09:30 前 / 15:00 后跑就 log
  `out-of-window` 静默退（不调 systemctl is-active，不告警）
- `health.py` 报告区分 in-window / out-of-window，避免冗余触发污染统计

### Why
v0.10.2 deploy 时 `systemctl list-timers` NEXT 显示 `Fri 13:00:08`（不是
预期的 `Fri 09:30`），用 `systemd-analyze calendar` 验证才发现两个 syntax bug
同时存在。如果不修，节后周二 5/6 上午盘 watchdog 完全缺席。

### Tests
- `TestBuildDailyReport.test_holiday_only_out_of_window`：out-of-window
  统计独立计数，不污染 in-window 总数
- 既有用例补 `out-of-window` 字段验证
- 总数 340 → 341

### Deploy
```bash
ssh lighthouse@82.156.0.68
cd /home/lighthouse/rquant && git pull origin main
sudo cp deploy/systemd/rquant-monitor-watchdog.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart rquant-monitor-watchdog.timer
# 验证：next 应是 Fri 09:30:xx 而不是 Fri 13:00:xx
systemctl list-timers rquant-monitor-watchdog.timer --no-pager
# 看 systemd 实际解析（应见 09:00, 09:02, 09:04 ... 间隔 2min）
systemd-analyze calendar 'Mon..Fri *-*-* 09..14:*/2' --iterations 6
```

### 顺手发现的 v0.7.0 旧 bug（不在本 PR 修）
`rquant-backup.timer` 的 `OnCalendar=Mon..Fri 09:30..15:05/5` 也是无效语法
——意味着 **intraday backup 自 v0.7.0 起从未跑过**，cloud `latest.duckdb.gz`
一直是日终 17:30 那一份。证据：今天 timer LAST="-"。下个 PR 修。

---

## [v0.11.0] — 2026-04-30 — 每日健康摘要（cloud 端 15:30 PushDeer 自报）

### Added
- **`rquant daily-report [--dry-run]`** CLI：扫今日 systemd 状态 + watchdog
  日志 + DuckDB 业务数据 → 拼成 PushDeer 摘要推到刘哥手机
  - 交易日：monitor 应跑足 5h+ 跨午休、watchdog 应 60 次 active、price_level
    事件计数、daily pipeline 状态
  - 非交易日：monitor 应秒退、watchdog 应全 skip-clean-exit、提示业务不更新
  - **`--dry-run`**：mac 本地 smoke 测试不推送（防呆）
- **`src/rquant/health.py`**：`get_service_snapshot()` / `read_watchdog_log()`
  / `count_today_business_data()` / `build_daily_report()` 模块化数据采集 + 报文构造
- **`scripts/monitor-watchdog.sh`** 加结构化日志：每次调用 append 一行
  `<ISO ts> <tag>` 到 `logs/watchdog-YYYY-MM-DD.log`
  - tag ∈ {`active`, `skip-clean-exit`, `alert-restart`}
- **`deploy/systemd/rquant-daily-report.{service,timer}`**：每日 15:30 自动跑
  - 不依赖 journalctl 权限（systemctl show + 自记日志）
  - 自身 OnFailure 也走 alert relay

### Why
之前一次性 schedule 验证 watchdog 节假日修复 + 跨午休回归不可行（远程 agent
没 SSH / 没 brain 写入权 / 没 PushDeer 凭据）。把验证做成 cloud 端 systemd
timer 反而**一举三得**：
1. 5/1 节假日验证（明天即生效）
2. 5/6 节后跨午休回归验证
3. **长期日常 health monitoring**——以后任何 monitor / watchdog / daily 异常
   都会被发现，不用人盯 dashboard

### Tests
- `TestParseSystemdTs` / `TestGetServiceSnapshot` / `TestReadWatchdogLog`
  / `TestBuildDailyReport`：覆盖时间戳解析、systemd 失败兜底、watchdog log
  统计、节假日干净跑 / 节假日告警轰炸 / 交易日跨午休正常 / 跨午休 bug 复发
  / watchdog 0 触发 / daily 失败 / monitor 未触发等 8 种场景
- 总数 321 → 340（+19）

### Deploy
```bash
ssh lighthouse@82.156.0.68
cd /home/lighthouse/rquant && git pull origin main

# 装 daily-report systemd unit
sudo cp deploy/systemd/rquant-daily-report.service /etc/systemd/system/
sudo cp deploy/systemd/rquant-daily-report.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now rquant-daily-report.timer

# 立即跑一次验证（dry-run 不推；不带 --dry-run 推到手机）
.venv/bin/rquant daily-report --dry-run
.venv/bin/rquant daily-report   # 手机收到第一份日报

# 看 timer 排上了没
systemctl list-timers rquant-* --all --no-pager
```

明天 5/1 15:30 自动收到第一份"非交易日干净跑"日报。

---

## [v0.10.3] — 2026-04-30 — Blacklist parquet round-trip CLI

### Added
- **`rquant blacklist export-parquet [--output PATH] [--label LABEL]`**：mac
  端从 DuckDB 导出黑名单为 parquet（v0.9.0 时这步靠手撸 SQL）
- **`rquant blacklist load-parquet <path> [--label LABEL]`**：云端把 parquet
  落库到 DuckDB `risk_blacklist` 表（替换今天 v0.9.0 部署时被遗忘的
  Python one-liner）
  - 不传 `--label`：全表覆盖（适合 mac 完全镜像到云端）
  - 传 `--label X`：只覆盖该 label 的行（多 label 共存场景）
- **`deploy/upload-api.md`** 加完整 round-trip SOP（mac 1-2-3-4 → 云端 5-6-7-8）

### Why
v0.9.0 部署黑名单时 push parquet 上云成功，但**云端 import 进 DuckDB 这步
被忘了**——`rquant blacklist import` 只接 PDF，没有 parquet 入口。结果今天
（2026-04-30）发现黑名单过滤实际**没在跑**（pipeline 拿到空 dict）。这次
补齐 round-trip CLI，下一次刷新（明年 4-30 续期）一行命令搞定。

### Tests
- `TestParquetRoundtrip`：4 用例覆盖 export → load 全流程、`--label` 选择性
  覆盖、全表覆盖、文件不存在
- 总数 317 → 321（+4），全绿

---

## [v0.10.2] — 2026-04-30 — Watchdog 节假日告警轰炸 hotfix

### Fixed
- **`scripts/monitor-watchdog.sh`：节假日 60 次/天告警轰炸 bug**。v0.10.1
  watchdog timer 每 2min 巡检 monitor，但漏想了 A 股节假日场景：
  - 09:25 monitor.timer 触发 → monitor `is_trading_day(today)` False 立刻退 0
  - 09:30 watchdog 见 inactive → 推 PushDeer + systemctl start → monitor 又退
  - 09:32 watchdog 又触发 → 又推 → 60 次/天告警
  改为先看 `systemctl show ExecMainExitTimestamp/ExecMainStatus`：今天已
  exit 0 过 → 静默不告警不重启（覆盖节假日 + 收盘后两个场景）

### Why
明日 Fri 2026-05-01 是 A 股劳动节假期（5/1-5/5 休市）。如果不修，刘哥手机
凌晨开始就被 PushDeer 打爆。systemd OnCalendar 不支持中国节假日规则，所以
watchdog 必须从 systemd state 推断"是否今日已正常完成"。

### Deploy（仅一个文件 + reload，秒级）
```bash
ssh lighthouse@82.156.0.68
cd /home/lighthouse/rquant && git pull origin main
# scripts/ 在仓库内，不需要 cp 到 /etc，直接 git pull 就生效
# 验证
bash -n scripts/monitor-watchdog.sh && echo "syntax OK"
# 可选：手动触发一次看效果（monitor 已 inactive 状态）
sudo systemctl start rquant-monitor-watchdog.service
sudo journalctl -u rquant-monitor-watchdog.service -n 20 --no-pager
```

---

## [v0.10.1] — 2026-04-30 — Monitor 跨午休 bug 修复 + 盘中守护 / OnFailure 告警

### Fixed
- **`src/rquant/monitor.py`：盘中监控在上午收盘时静默退出的 bug**。原 `while
  _is_trading_hours()` 在 11:30 后跳出主循环 → systemd 标记 inactive(dead) →
  下午 13:00 既不重启也不补触发，**每个交易日下午盘 13:00-15:00 实际无监控**。
  改为五阶段状态机（pre / morning / lunch / afternoon / closed）：
  - `_market_phase(now)`：根据时间分阶段（替代二值 `_is_trading_hours`）
  - 主循环 `while True`：lunch 阶段调 `_wait_for_afternoon_open`（每次 ≤60s
    sleep，便于响应中断）、closed 才 break
  - lunch 阶段**不调 akshare**（限频保护 + 静态价无意义）
- **现象**：`Apr 30 11:31:03 rquant-monitor.service: Deactivated successfully`
  （code=0），下午盘看板上 monitor badge 显示红色 inactive(dead)。

### Added
- **`rquant alert --subject ... [--body ...]` CLI**：极简告警入口，独立于
  scene 体系，给 systemd OnFailure / watchdog 等运维场景用，直接走 PushDeer
  + PushPlus
- **盘中 watchdog**：`deploy/systemd/rquant-monitor-watchdog.{service,timer}`
  + `scripts/monitor-watchdog.sh`。每 2 分钟（仅 09:30-11:30 + 13:00-15:00）
  检查 `rquant-monitor.service` 是否 active，不活则推 PushDeer 告警 + 自愈
  `systemctl start`。盘外不触发，无打扰
- **OnFailure 钩子**：`deploy/systemd/rquant-alert@.service` 模板。
  `rquant-monitor.service` / `rquant-daily.service` 加 `OnFailure=
  rquant-alert@%n.service`，service 失败的瞬间推 PushDeer，无需等用户主动
  看 dashboard
- **Dashboard 三态 badge**：`monitor` 红的判定改为「**仅交易时段且非 active
  才红**」，盘前 / 午休 / 收盘后非 active 显示灰色 idle（避免假阳性）。
  badge `_badge(label, status, sub)` 接 `"ok"|"bad"|"idle"` 三态字符串

### Tests
- `TestMarketPhase`：14 个时间边界用例（08:00 / 09:24 / 09:29 / 09:30 / 11:29
  / 11:30 / 12:59 / 13:00 / 14:59 / 15:00 等）
- `TestWaitForAfternoonOpen`：3 个用例验证 60s 上限 + 边界
- `TestRunMonitor.test_crosses_lunch_break_without_exiting`：关键回归测——
  morning → lunch → afternoon → closed 期间进程**不退出**，lunch 阶段不
  fetch akshare
- 总数 297 → 317（+20 用例），全绿

### Deploy（cloud：82.156.0.68）
```bash
cd /home/lighthouse/rquant
git pull origin main
sudo cp deploy/systemd/rquant-{monitor,daily,monitor-watchdog,alert@}.{service,timer} /etc/systemd/system/ 2>/dev/null
sudo cp deploy/systemd/rquant-monitor.service /etc/systemd/system/
sudo cp deploy/systemd/rquant-daily.service /etc/systemd/system/
sudo cp deploy/systemd/rquant-monitor-watchdog.service /etc/systemd/system/
sudo cp deploy/systemd/rquant-monitor-watchdog.timer /etc/systemd/system/
sudo cp deploy/systemd/rquant-alert@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now rquant-monitor-watchdog.timer
sudo systemctl restart rquant-dashboard
# 验证
.venv/bin/rquant alert --subject "[Test] v0.10.1 deployed" --body "OnFailure + watchdog 已上线"
systemctl list-timers rquant-* --all --no-pager
```

---

## [v0.10.0] — 2026-04-30 — Upload HTTP API（与 Backup download 对称）

mac → 云端的"配置数据"推送通道：nginx WebDAV PUT + 单独 basic auth。
绕开 fail2ban，建立稳定的本地→云推送路径，下次 v0.9 黑名单续期、其他
"本地生成 / 服务器消费"的小数据 push 都走这条。

### Added
- `deploy/nginx/rquant-backup.conf`：`/upload/` location with `dav_methods PUT`
  + 文件后缀白名单（.parquet / .csv / .pdf / .json）+ 单独 htpasswd
  `/www/server/nginx/conf/.rquant-upload.htpasswd`（与 backup token 隔离）
- `scripts/push-to-cloud.sh`：客户端 wrapper，curl -T 上传，自动从 `.env`
  读 user/token/url，参数 `<local-file> [<remote-name>]`
- `deploy/upload-api.md`：部署 + 客户端用法 + 故障排查（含 dav_module 缺失
  的 fallback、文件 owner=www 后续 sudo mv 处理）
- `.env.example` 加 `RQUANT_UPLOAD_USER` / `RQUANT_UPLOAD_TOKEN` /
  `RQUANT_UPLOAD_URL`

### Why
v0.9.0 部署黑名单时 fail2ban 卡住 SSH/SCP，不得不走宝塔 web。这次顺手建
通道，下一次（明年 4-30 续期 / 其他类似的 push）直接 `bash
scripts/push-to-cloud.sh data/risk_blacklist.parquet` 一行搞定。

---

## [v0.9.0] — 2026-04-30 — 风险控制黑名单（"430 黑名单"）

### Added

PDF 风险名单解析 + DuckDB 落库 + 跨流水线分场景过滤/标签。1 年有效期，过期后
dashboard 推提醒不静默失效。

- `src/rquant/risk/blacklist.py`：PDF 解析（pypdf）→ 代码标准化（补前导 0 +
  自动加 SH/SZ/BJ 后缀）→ 多类别合并 → DuckDB upsert（`replace=True` 覆盖同
  list_label 旧数据）→ 过滤 / 标签 API（`filter_blacklist` 硬剔除、
  `annotate_blacklist` 软标签）
- `risk_blacklist` 表：`(list_label, ts_code)` 主键，`sub_categories[]` 多类别合并
- Pipeline：`run_daily_pipeline` 在每个 preset 落库前过滤命中黑名单的 ts_code
  （新推荐**剔除**），不影响已存在 pool2_watch 持仓
- Monitor：`build_watchlist` 给已持仓 WatchItem 打 `blacklist_label` 标签
  （**保留+标签**），档位触发推送 subject 加 `[430黑名单]` 前缀，body 加 ⚠️ 类别行
- Pool 2 退出汇总 + 每日选股汇总：命中行加 `[430黑名单]` 标签
- Dashboard：Pool 2 active / Pool 2 实时价位表加 `黑名单` 列；新增 Section 9
  "🛑 风险黑名单状态"，显示标签 / 标的数 / 导入日 / 失效日 / 剩余天数 +
  即将到期黄色警告 + 已过期红色警告（提示用 `rquant blacklist import` 刷新）
- CLI `rquant blacklist {import,list,check,remove}`：从 PDF 导入 / 列出 /
  查询单只 / 删除整个 list_label
- pypdf 依赖加入 pyproject.toml

### Verified
- 本地 prod DB 导入 147 只 → Pool 2 active 中 1 只命中（`000952.SZ 广济药业` 年报审计风险）
- 22 个 blacklist 单测 + 2 个 pipeline 集成测全部通过（总 297 / 297）

---

## [v0.8.0] — 2026-04-29 — Backup HTTP API（替换 rsync over SSH）

云端备份从 SSH/rsync 切换到 HTTP basic auth + curl，绕开 fail2ban 阻断
mac 端的 sync。同时为未来 API 化（FastAPI 产品化）打基础——nginx
反代 + token 鉴权这套架构能直接扩展。

### Added
- `scripts/backup-snapshot.sh`：服务器侧 cp + gzip + atomic mv 生成一致性快照
- `deploy/systemd/rquant-backup.{service,timer}`：盘中每 5min + 日终 17:30 触发
- `deploy/nginx/rquant-backup.conf`：nginx `/backup/` static + basic auth +
  `/dashboard/` reverse-proxy + auth
- `deploy/backup-api.md`：完整部署 + 故障排查文档（含 HTTPS 升级路径）
- dashboard 新增"☁️ 云端 Backup Snapshot"指标读 `backup/latest.json`：
  显示 snapshot 大小 / 压缩比 / 最后同步时间

### Changed
- `scripts/sync-from-cloud.sh`：rsync over SSH → curl HTTP + basic auth
  - 不再走 22 端口，绕开 fail2ban
  - 失败重试从 3 次 → 2 次（HTTP 失败不触发 fail2ban，重试更安全）
  - 传输 gzip 压缩文件，30-50% 体积减半
- `.env.example` 新增 `RQUANT_BACKUP_USER` / `RQUANT_BACKUP_TOKEN` /
  `RQUANT_BACKUP_URL`
- `deploy/local-sync.md` 改为指向 backup-api.md 的 stub

---

## [v0.7.0] — 2026-04-29 — 云端部署 + 多通道通知 + Health Dashboard

把 rQuant 从本地 macOS 单点搬到腾讯云轻量服务器（82.156.0.68）systemd 调度，
解决本地笔记本休眠 APScheduler 死亡问题。增加 PushPlus 通道（不装 PushDeer
的协作者）、Streamlit Health Dashboard、本地热备 rsync 同步。

### Added
- systemd timer + service（`deploy/systemd/`）：daily 17:00 + monitor 09:25
  工作日触发，腾讯云 OpenCloudOS 9 验证通过
- PushPlus 通道（`notify/client.py:PushPlusClient`）：微信公众号推送，给
  不装 PushDeer 的用户（如美丞）；与 PushDeer 双通道独立失败
- Health Dashboard（`src/rquant/dashboard/app.py`）：Streamlit 单页 9 个指标
  - systemd 服务状态 / Watchlist / 今日触发事件 / 数据新鲜度 / 7 日趋势
  - 通知通道 24h 成功率 / 本地 sync 状态 / Pool 2 实时价位 vs 档位
  - Pool 2 行点击下钻：日 K candlestick + 分时（午休 11:30-13:00 跳过空段）
  - 30s 自动刷新 + Linear/Vercel 风格紧凑 UI
- 本地热备 rsync（`scripts/sync-from-cloud.sh` + `deploy/com.roxor.rquant-sync.plist`）：
  - 盘中 09:30-15:05 + 日终 17:10-17:30 同步窗口
  - rsync `--delay-updates` 原子 rename 保证 mac 端读到完整文件
  - `--force` 选项手动触发；失败 PushDeer 告警
- `notification_log` 表 + `notify/api.py` 写入推送日志（dashboard 读取展示）
- `_to_sina_symbol` helper：ts_code → sina 代码格式（sh/sz/bj 前缀）
- CLAUDE.md 新增"生产环境与协作模式"小节：服务器 IP / Hybrid 协作分工 /
  通知通道分工
- `deploy/dashboard.md` + `deploy/local-sync.md` + `deploy/systemd/README.md`：
  部署 + nginx basic auth + 故障排查文档

### Fixed
- monitor `fetch_realtime_prices` 从 `stock_zh_a_spot_em`（东方财富，云端
  腾讯云 IP 段被屏蔽）改 `stock_zh_a_spot`（sina HQ 接口），云端可用
- dashboard K 线 / 分时 API 同步换 sina：`stock_zh_a_daily` +
  `stock_zh_a_minute` 替代东方财富版本
- dashboard DuckDB 写锁冲突优雅降级：query 返回 None 时 UI 显示等待提示，
  不再裸抛错误堆栈（daily 流水线 ingest 期间 dashboard 自动降级）
- dashboard UI 大幅紧凑化：字号 16→13px、metric 卡值 1.5→1.15rem、H2
  改 Vercel 小号大写、container border 1px 浅灰圆角、健康 badge 扁平化
- 分时图 11:30-13:00 午休空段：x 轴改 ordinal 跳过空段，加灰虚线分隔
- dashboard Pool 2 实时价位：sina HQ 批量接口替代 ak.stock_zh_a_spot 全市场
  拉取（300ms vs 3s），数字列严格 `%.2f` 格式
- sync 窗口策略修正：原"每小时跑 + 业务时段跳过"改为"仅业务时段相关跑 +
  其他时间不跑"，避免错过 monitor_event 实时备份
- systemd timer NEXT 字段微秒时间戳转 UTC+8 + delta 显示"X 小时后"

### Changed
- `WatchItem` 加 `name` / `entry_date` 字段，`build_watchlist` 末尾批量
  从 `stock_basic` join 填股票名（dashboard 用）
- `_send_pushdeer` / `_send_pushplus` 写 `notification_log` 表记录每条
  推送的 target/success/error_msg
- `pyproject.toml` 新增 streamlit 依赖

---

## [v0.6.0] — 2026-04-29 — Week 6: PushDeer 告警通知

替换原计划的 cc2im（受限于微信 token 限制）为 PushDeer。完全替代 monitor.py 的 osascript 弹窗，云端零迁移成本。

### Added
- `notify` 独立模块（`src/rquant/notify/`）：
  - `client.py` — PushDeerClient，多 key 并发推，timeout/异常都捕获不抛
  - `messages.py` — 5 类场景消息构造（price_level / pool2_exit / daily_summary / error / heartbeat）
  - `api.py` — `notify(scene, **kwargs)` 统一入口 + 总开关 + 各场景独立开关
- `rquant notify-test` CLI 命令：直接推 PushDeer 测试消息验证通道
- 5 类推送场景接入：
  - **A 档位触发**：实时单条（替换 osascript 弹窗），价格阶梯从高到低展示（bodyTop / 40 / 30 / 20 / bodyBtm + 强弱止）
  - **B Pool 2 退出汇总**：收盘后批量一条（无事件不推），breakdown 自动踢，expired 保留待用户决策
  - **C 每日筛选汇总**：17:00 流水线完成后一条，含 Pool 1 命中名单 + Pool 2 持仓状态 + 耗时
  - **D 系统异常**：cli/pipeline/monitor 入口 try/except 捕获后实时推（含 stack trace 前 15 行）
  - **E Monitor 启停心跳**：09:30 启动 + 15:00 结束各一条
- `WatchItem` 新增 `name` 和 `entry_date` 字段，`build_watchlist` 末尾批量从 `stock_basic` join 填股票名
- `tests/conftest.py` autouse fixture：默认禁用真实 PushDeer 推送，避免测试副作用刷手机
- 配置项 `.env`/`.env.example`：`PUSHDEER_KEYS` / `PUSHDEER_ENDPOINT` / `NOTIFY_*` 开关

### Changed
- `monitor.check_exits()` 改为自动化：breakdown 直接 `update_pool2_exit`，expired 保留 active 加入待决策列表，末尾推汇总，返回 `auto_kicked_count` 用于心跳统计
- `pipeline.run_daily_pipeline()` 末尾计算耗时并触发 daily_summary 推送

### Removed
- `monitor.alert_price_level()`（osascript 弹窗，被 PushDeer 替代）
- `monitor.alert_exit_confirm()`（osascript 退出确认弹窗，PushDeer 单向推无法承载交互决策）
- `subprocess` 导入（不再使用）

### Fixed
- 测试套件：删除 osascript 相关测试用例（TestAlertPriceLevel / TestAlertExitConfirm），新增 32 个 notify 模块测试 + check_exits 重写后的 3 个测试

---

## [v0.5.1] — 2026-04-28 — Hotfixes: 调度可靠性 + monitor 自动拉起

### Fixed
- `rquant serve` APScheduler 可靠性：`misfire_grace_time` 从 1s → 3600s → 7200s（覆盖周末/长 sleep 后的 misfire），并加 stdlib logging bridge 输出 APS 内部错误
- monitor 自动每日拉起：`com.roxor.rquant-monitor.plist` 加 `StartCalendarInterval` 09:29，`run_monitor` 加 `_wait_for_market_open()` 在 09:30 前 10 分钟内 sleep 到开盘

---

## [v0.5.0] — 2026-04-21 — Week 5a: 盘中实时监控 + Pool 2 持久池

### Added
- `rquant monitor` 命令：盘中实时监控 Pool 1 + Pool 2 标的价格
  - akshare 实时行情轮询（5 秒间隔），检测 5 个档位（40%/30%/20%/强止/弱止）
  - macOS 原生弹窗提醒（osascript display alert），非阻塞
  - 当日最低价补漏机制，防止闪跌遗漏
  - 交易日历检查（含中国节假日），非交易日自动跳过
- `pool2_watch` 表：Pool 2 持久池，从每日快照升级为有进出机制的持久池子
  - 入池：pipeline 跑完 Pool 2 筛选后自动同步
  - 退出：收盘后检查跌破止损/超期（3 天），所有退出弹窗确认（踢出/保留）
- `monitor_event` 表：盘中事件日志，记录每次档位触发详情
- `rquant pool2 list / remove` 命令：查看和管理持久池
- `deploy/com.roxor.rquant-monitor.plist`：盘中监控 launchd 自启配置
- `rquant ingest --date` 命令：按 trade_date 模式拉全市场 stock_basic + daily_bar + daily_basic + derive_state，约 30 秒完成
- `rquant run-daily` 现在自动先 ingest 再 pipeline（`--no-ingest` 跳过）
- `rquant serve` 的 cron 改为 ingest → pipeline 串联，数据未就绪时自动重试 3 次（间隔 15 分钟）
- `deploy/com.roxor.rquant.plist`：macOS launchd 开机自启配置

### Changed
- `pipeline.py`：`run_daily_pipeline()` 尾部新增 pool2_watch 同步逻辑
- Pool 1 下影线阈值从 1.5 放宽至 0.5（下影/实体比），命中从 5 只提升至 12 只
- Pool 1 前涨停窗口从 90 交易日放宽至 120 交易日
- Pool 2 `offset_days` 从 1 改为 2，合并 T-1 + T-2 两天的父预设白名单
- Pool 2 下影线阈值同步从 1.5 放宽至 0.5
- `run_daily_pipeline()` 依赖链改为范围回溯：`offset_days=N` 表示合并 T-1 到 T-N 的父预设结果

---

## [v0.4.0] — 2026-04-20 — Week 4b: 调度 + 流水线 + N 形态预设

CLI 入口、APScheduler 调度、screen_result 落库、N 形态 Pool 1 + Pool 2 预设注册表、流水线依赖链编排。

### Added
- CLI：`rquant serve`（APScheduler cron，Mon-Fri 17:00）和 `rquant run-daily --date --preset` 子命令
- `screen_result` 表：筛选命中结果落库（trade_date + preset_name + ts_code，extra JSON 列存附加字段）
- `ScreenPreset` 数据类 + `PRESET_SCREENS` 注册表：Python 代码即策略声明，支持 depends_on 依赖链
- N 形态预设：Pool 1（11 条规则，全市场）+ Pool 2（3 条规则，依赖 Pool 1 T-1 结果子集）
- `run_daily_pipeline()`：按依赖拓扑排序遍历预设，子预设自动从父预设结果取 whitelist
- `screen()` 新增 `ts_code_whitelist` 参数，支持在指定子集中筛选

### Changed
- `pyproject.toml`：新增 `apscheduler>=3.10` 依赖 + `[project.scripts]` 入口

---

## [v0.3.1] — 2026-04-20 — Week 4a: daily_basic + N 形态积木

为 N 形态策略补全数据层和规则积木。新增 `daily_basic` 表接入流通市值/换手率/量比，宽表暴露 `BODY_UPPER[n]`/`BODY_LOWER[n]`/`CIRC_MV[n]`，6 个新积木 + AggregateRequest 长窗口聚合机制。

### Added
- `daily_basic` 表（turnover_rate / volume_ratio / total_mv / circ_mv）
  - `DuckDBStore.upsert_daily_basic()` / `count_daily_basic()`
  - `TushareAdapter.daily_basic(ts_codes, trade_date)` — 单日查询
  - `ingest_daily.py` 追加按日逐天拉取 daily_basic
- 宽表扩展：
  - `STATE_COLS_MAP` 新增 body_upper / body_lower → `BODY_UPPER[n]` / `BODY_LOWER[n]`
  - 新增 `BASIC_COLS_MAP`（circ_mv / total_mv / turnover_rate）→ `CIRC_MV[n]` / `TOTAL_MV[n]` / `TURNOVER_RATE[n]`
- AggregateRequest 机制：规则声明长窗口聚合需求（max / any / sum / count_nonzero），load_universe 动态生成 DuckDB SQL，支持 exclude_offset
- 6 个新积木：
  - `not_yiziban(offset)` — 某日非一字板
  - `circ_mv_lt(threshold_yi, offset)` — 流通市值 < N 亿
  - `has_lower_shadow(min_ratio, min_amplitude, offset)` — 下影线达标
  - `no_consec_ups_in_window(threshold, window)` — 近 N 日无 M 连板
  - `no_limit_down_in_window(window)` — 近 N 日无跌停
  - `has_prior_limit_up(window, exclude_offset)` — 近 N 日（排除某日）有涨停
- 测试：新增 ~50 个单测（storage 4 + loader 11 + rules 30+ + core 4），累计 162 个

---

## [v0.3.0] — 2026-04-16 — Week 3b: 筛选规则引擎

Week 3b 在 daily_state + daily_indicator 基础上做多条件组合筛选。原子条件"积木"函数库，命名对齐通达信/MyTT 风格（`CLOSE[0]` / `MA20[0]` / `IS_LIMIT_UP[1]`），为 Week 8 通达信代码支持铺路。

### Added
- `rquant.screen` package：
  - `load_universe(trade_date, lookback)`：从 DuckDB 加载全市场宽表（每行 1 只股票，字段 `CLOSE[n]` / `MA20[n]` / `IS_LIMIT_UP[n]` 等）
  - 积木函数库：属性（not_st / not_bj / board_in）、涨跌停（limit_up / first_limit_up / yiziban / consecutive_ups_gte / limit_down / not_limit_up）、比较（gt / lt / gte / lte / between）、指标（cross_above / cross_below / above_ma / rsi_oversold / rsi_overbought）、成交量（volume_ratio_gte）
  - `screen(trade_date, rules)`：AND 组合 + 自动 lookback 推断 + 结果 DataFrame 返回
- `scripts/smoke_screen.py`：跑用户原始场景的冒烟脚本

### Verified
- 用户原始场景「非 ST + 非北交所 + 昨首板 + 今未涨停 + 今高>昨收」在集成测试 + 真实近期数据（2026-04-15 前后）上跑通
- 单测：新增约 30 条（属性 6 + 涨跌停 7 + 比较 6 + 指标 5 + 成交量 1 + screen 5 + loader 5），全量 105 绿，整体累积 ~165 个

---

## [v0.2.1] — 2026-04-16 — Week 3a: 派生字段层（daily_state）

为 Week 3b 筛选规则引擎铺底：把「涨停/跌停/首板/一字板/连板/实体上下沿/板块/ST」这些 SQL 难表达的概念先算好落库，规则引擎只做 SELECT 过滤。

### Added
- `rquant.state.derive` 模块：基于日线原始价（非前复权，涨停判断必须用真 `pre_close`）推导 15 列派生字段
  - `_classify_board(ts_code)`：688/689 → star，300/301 → gem，.BJ → bj，else main
  - `_detect_st(name)`：忽略空格，识别 `ST` / `*ST` / `SST` 前缀
  - `_limit_pct(is_st, board_type)`：ST 5% / 主板 10% / 创业板科创板 20% / 北交所 30%
  - `derive_state(df_daily, ts_code, name)`：一次算完 `is_limit_up` / `is_first_limit_up` / `is_yiziban` / `consecutive_limit_ups` / `body_upper` / `body_lower`
  - 涨停识别带 1 分价格容差（`close >= limit_up_price - 0.01`）
- `schema.DAILY_STATE_DDL`：`daily_state` 表（15 列，PK = ts_code + trade_date）
- `DuckDBStore.upsert_state` / `count_state` / `get_state`
- 依赖：`mytt==2.9.3`（通达信/同花顺风格公式库，用其 `BARSLASTCOUNT` 算连板数）
- `ingest_daily.py` 扩展：拉完 daily 后自动算派生字段落 `daily_state`；顺带拉一次全量 `stock_basic`（~5500 行）用于 ST 判断
- `status.py` 扩展：展示每只股票的涨停/跌停/首板/一字板/最大连板统计，以及最新一日的涨跌停价和实体区间
- 测试：新增 42 个单测（state 模块 37 个 + storage 5 个），累计 65 个全部通过

### Verified
- 赛力斯 601127.SH（华为概念股）2024-09-30 / 10-23 / 11-04 / 11-05 四次涨停识别正确，11-04+11-05 连板 2 正确，首板标记正确
- 涨停价公式：`pre_close × (1 + limit_pct)` 对主板 10% / 创业板 20% 实测与东方财富一致
- 宁德时代 2024-10-08 +18.70% 因不足 20% 限制 → `is_limit_up=False` 正确

---

## [v0.2.0] — 2026-04-16 — Week 2: 复权因子 + 技术指标

### Added
- 复权因子层
  - `schema.ADJ_FACTOR_DDL`：`adj_factor` 表
  - `TushareAdapter.adj_factor(ts_codes, start, end)`
  - `DuckDBStore.upsert_adj_factor` / `get_daily_qfq(ts_code, start, end)` / `count_adj_factor`
  - `get_daily_qfq` 以该股票最新 adj_factor 为参照计算前复权价，同时返回原始价和因子值便于核验
- 技术指标层（基于前复权价，避免分红除权造成指标跳变）
  - `rquant.indicator.compute_indicators(df)`：MA5/10/20/60 + RSI6/14 + MACD(12,26,9) + KDJ(9,3,3)
  - KDJ 用 A 股常用口径（α=1/3 指数平滑）手写实现
  - `schema.DAILY_INDICATOR_DDL`：`daily_indicator` 表（13 列指标）
  - `DuckDBStore.upsert_indicators` / `count_indicators`
- `ingest_daily.py` 扩展：拉完 daily+factor 后自动基于全量 qfq 重算指标并入库
- `status.py` 扩展：展示首日/最新日的原始价 vs 前复权价对比、最新技术指标（含多空/金死叉判断）
- 依赖：`ta==0.11.0`（放弃 pandas-ta 因其锁死旧版 numba 与 pandas 3 冲突）
- 测试：新增 15 个单测（adj_factor/qfq 5 个 + 指标 8 个 + indicator 落库 2 个），累计 23 个全部通过

### Infrastructure
- Week 1 代码推送到 GitHub private：<https://github.com/roxorlt/rquant>

### Verified
- 茅台 2025-12-18 前复权收盘 1407.04（除权日 2025-12-19 前一天）对上东方财富
- 茅台 2026-04-15 MA5 = 1454.43 对上东方财富日 K 均线

---

## [v0.1.0] — 2026-04-16 — Week 1: 数据接入 + DuckDB 存储

### Added
- 项目 scaffold：uv 包管理 + Python 3.12 + pyproject.toml + ruff/pytest 配置
- 配置层：`rquant.config.Settings`（Pydantic Settings 读 `.env`，校验 token 长度、自动创建目录）
- 日志层：`rquant.logging.setup_logging`（loguru stderr + 按日轮转到 `logs/`，保留 30 天）
- 数据模型：`rquant.models.DailyBar`（Pydantic，frozen）
- Tushare Adapter：`rquant.adapter.TushareAdapter`
  - `daily(ts_codes, start, end)` 拉日线 OHLCV，主 token 失败自动切备用
  - `stock_basic(list_status)` 拉股票基础信息
- DuckDB 存储：`rquant.storage.DuckDBStore`
  - 建表 DDL 集中在 `schema.py`（`daily_bar` + `stock_basic`）
  - `upsert_daily` / `upsert_stock_basic` 幂等写入
  - context manager 支持
- CLI：`scripts/ingest_daily.py` 一次性拉历史日线入库
- 测试：8 个单测（config 4 + DuckDB 4），`tests/README.md` 规范说明

### Infrastructure
- 项目初始化：README + CLAUDE.md + docs/（data-sources-matrix、references）
- CHANGELOG.md（Keep a Changelog 格式）+ .gitignore（Python + data/ + .env）
- .env.example 模板，`.env` 忽略提交
- git init + `v0.0.1` scaffold tag

---

## 版本计划（MVP 路径对应）

| 版本 | 里程碑 | 对应周 |
|------|-------|--------|
| v0.1.0 | 数据接入 + DuckDB 存储跑通 | Week 1 |
| v0.2.0 | 指标计算模块 | Week 2 |
| v0.3.0 | 筛选规则引擎 | Week 3 |
| v0.4.0 | APScheduler 调度 | Week 4 |
| v0.5.0 | 盘中 Ashare 轮询监控 | Week 5 |
| v0.6.0 | cc2im 告警通知 | Week 6 |
| v0.7.0 | Streamlit 最小 UI（MVP 完整） | Week 7 |
| v1.0.0 | 第一次真正日常可用 | MVP 稳定后 |

---

<!--
模板用法：
- 每次合 main 前，把 [Unreleased] 下的条目整理好
- 打 tag 时，把 [Unreleased] 换成 [v0.X.0] - YYYY-MM-DD
- 下方重建一个空的 [Unreleased]
- 分类用：Added / Changed / Deprecated / Removed / Fixed / Security
-->
