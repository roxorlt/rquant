# Changelog

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 格式，版本号遵循 [SemVer](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Fixed

- **退市整理期误报历史状态 P0**：Tushare `namechange.change_reason=退市整理期` 现在归类为
  已知但主动排除的上市状态边界，继续 fail closed，不伪装为普通非 ST，也不参与任何策略或
  涨跌停派生；`security-status-backfill --missing-only` 不再反复下载这些已确认排除的股票日。
  Stage 1 审计升级为 `stage1-v3`：真正缺失、未知和冲突仍保持 P0，主动安全排除单列为可追溯
  的 P2 证据。项目版本从 `0.17.1` 更新到 `0.17.2`。

- **历史 ST 状态回补误报 P0**：Tushare `namechange.change_reason=其他` 视为有效名称区间，
  不再把新股首日误判为未知边界；`stock_st` 正例按 `ts_code + trade_date` 确认 ST，展示名
  被截断或带 `XD` 前缀时不再与历史名称制造伪冲突。完整且未触及 1000 行上限的逐日 ST
  清单允许用“未出现”确认非 ST，同时严格拒绝错日/混日响应。名称仍保持 unknown，不使用
  当前 `stock_basic` 回填历史；派生状态、筛选、市场情绪和策略候选只依赖 PIT 可见的 ST
  事实，缺名称时仅用代码作展示标签。migration v9 解耦历史名称与 ST 事实，现有状态表无损
  重建；v0.17.0 首次部署 runbook 的空修复计划、交互 shell `set -e` 和不可复现问题一并修正。

- **DuckDB 备份可能携带未合并 WAL 或掩盖 shell 失败**：定时备份改从已验证只读副本复制
  主文件与 WAL，在私有临时代际 checkpoint 后做只读和 gzip 双重校验再原子发布；部署可显式
  对静默主库生成精确恢复点，主库仍被写者锁定时 fail closed 并保留上一份成功快照。副本
  明显落后主库/WAL 时等待同轮同步，超时不发布“时间新、数据旧”的备份；主库模式在同一个
  DuckDB 写锁内完成 checkpoint 与复制，消除新 writer 插入其间的竞态。只读副本也先在
  私有代际回放并 checkpoint WAL，再以单文件原子替换，避免 DB/WAL 两步发布窗口。

- **竞价与题材因子 PIT 时点泄漏**：同花顺竞价候选决策时刻统一为 09:27；Tushare
  竞价源最早 09:26 可用，09:30 分钟 fallback 最早 09:31 可用。候选、板块竞价强度和
  次日竞价离场统一通过受控可见性查询；信号日收盘后才形成的题材成分快照不再参与同日盘中
  判断。竞价候选的涨跌停规则改由当时可见的 ST 状态、板块、上市日期和交易日历推导，信号日
  收盘后生成的 `daily_state` 只作结果标签；历史板块基准可使用届时已可见的早期分钟 fallback，
  同日 09:30 fallback 仍在 09:31 前隐藏。数据集快照重复执行时复用已完成记录，不再因新的
  完成时间戳产生冲突。

- **preflight 盘中只读检查撞 DuckDB 主库写锁**：`data_freshness` 与 `smoke_screen`
  改用 `open_readonly_store()` 优先读取 `rquant_ro.duckdb`，并在退出时关闭连接；`lsof`
  仅返回 `mem` 等未分类 FD 时改报预警，不再误判为“monitor 当前未跑”。
- **GitHub Actions 无法创建 CI job**：workflow 顶层环境变量不再引用尚不可用的
  `runner.temp`，改用该位置允许的 `github.workspace` 作为测试数据目录。

### Added

- **阶段 1 真实数据审计与正式研究门（PR-D）**：preflight 改为按数据契约、权威交易日和
  当前可见分区检查生产/研究水位，区分必需空表、可选空表、午休/开盘宽限及只读副本滞后；
  当天盘后数据在下一交易日前不会被误判为可见。新增持久化 `data-audit` 运行凭证和
  `stage1-v2` 规则集，历史状态缺失、休市日涨停池污染或覆盖率未达 B/S 99%、基准 95% 时，
  Strategy Lab 只能运行探索模式，worker 会在执行前再次校验。接入 Tushare `suspend_d`
  权威停复牌事件与逐日 coverage，日终增量、历史回补、云端/本地同步均保持快照原子性；
  零成交量不再冒充停牌。新增 `suspension-backfill`、`security-status-backfill --dry-run` 和
  可审计的真实数据验收报告。验收发现历史状态 379,658 个股票日缺失及涨停池 400 行休市日
  污染，正式研究保持阻断，未启动大规模分钟下载。
  审查进一步确认现有快照仅保存覆盖元数据，formal gate 会以 `snapshot_execution_unbound` 继续
  阻断，不允许底层数据代际未冻结的结果晋级 `comparable`；数据审计改为直读主库且只能解决
  同一日期区间的问题，灾备恢复把停复牌事件与 coverage 作为跨表原子包处理。

- **阶段 1 可恢复分钟回补（PR-C）**：为 N 字、集合竞价和科创/创业放量建立不可变策略
  回补规格与 PIT 资格全集；按权威 SSE 交易日展开 90 日基准、B 日和 10 日 S 窗口，相邻窗口
  合并下载但保留各阶段独立分母。覆盖率严格要求 Tushare 1 分钟完整 241 根交易时段，先扣除
  已完成会话，再按 8000 行上限切块，并给出请求数、行数、磁盘、限频耗时和置信度 ETA。
  独立 SQLite 以 WAL、短事务、租约、原子 claim、重试上限和结构化失败保存 manifest/task/
  eligibility 状态；执行器支持 API 退避、截断递归拆分、崩溃后覆盖复核、允许缺失分类和单任务
  失败隔离。最终尝试崩溃后会做一次不请求 API 的恢复核验，大清单按 ordinal 游标单向认领，
  慢请求返回后必须续租成功才允许写 DuckDB，丢失租约的数据会被丢弃。生产执行在 API 请求前
  释放 DuckDB、仅用短连接检查/写入，并在盘中保护窗口前主动停领新任务。竞价资格分母使用
  `量比 > 0 + 含 ST` 的参数宽口径母集，Lab 收紧/放宽 0.15–5 和 ST 过滤均不会越出 manifest。
  新增 `backfill-plan`、`backfill-run`、`backfill-status`、
  `dataset-snapshot` 四条 CLI；空资格分母不得伪装成 100%，快照 `as-of` 必须覆盖最后所需窗口，
  水位也截断在该时点，并仅在 B/S 覆盖至少 99%、历史基准至少 95% 后固化元数据，不执行整表
  刷新。

- **阶段 1 历史质量与 PIT 门禁（PR-B）**：新增逐日证券名称/ST 事实表与 nullable unknown
  语义，筛选、市场状态和三类策略回放统一读取决策时点可见的历史状态；新增可重复的数据质量
  审计、权威交易日历 bootstrap、涨停池双重交易日守卫和带 CAS 的审计式修复计划。日线/分钟
  一致性审计按来源配置 240/241 根及时间戳语义，跨源比较先归一到逻辑分钟起点，再区分缺日线、
  缺分钟、缺时段和跨源冲突。新股前五日、旧制度上市首日及重新上市首日等无涨跌幅限制窗口
  明确标为不可研究；历史竞价回放缺少当时可见的名称/ST 状态时直接排除，涨停价按历史制度与
  上市窗口推导，不再读取信号日盘后状态或用当前静态名称补齐。
  涨停池采集加入外层事务时不再擅自提交或回滚调用方事务。
  前复权查询只能用决策日及以前的因子作锚，缺因子明确不可用；价量分布统一采用复权价格和
  可比股数，同时保留原始成交额。新增 `available_at` 纯函数与受控查询入口，竞价、分钟和盘后
  数据按来源及决策时点 fail closed，供下一批资格全集解析器复用。
- **阶段 1 数据可信底座（PR-A）**：新增可校验 checksum、逐版本事务执行的 DuckDB
  `schema_migration`；新增研究数据快照、覆盖率和质量问题元数据，并把三张关联表作为原子包
  同步，避免云端/本地状态倒退或部分提交。首批 20 个策略数据集获得显式主键、价格口径、
  历史起点、新鲜度和 Point-in-Time 可见性契约；集合竞价按真实来源区分 Tushare 09:26
  与 09:30 分钟 fallback 09:31 的最早可用时间。新增权威 `trade_calendar` migration v3、
  Tushare 全自然日日历接入、缺口检测和前/后/最近交易日 typed API；已知休市与未知缺数不再
  混为一谈。此批次只建设数据契约和存储能力，尚未改动策略触发与回放查询。
- **受控自动发布**：新增 `scripts/deploy-production.sh` / `rquant.ops.production_deploy`，
  仅部署 `origin/main` 中的精确 SemVer tag 或完整 SHA；包含 tracked 清洁检查、快进校验、
  diff 风险分类、交易时段重启保护、部署互斥锁、最小 sudo 服务白名单、双 preflight、
  JSONL 审计和失败自动回滚。Codex 可在 CI 全绿后代管 PR merge、annotated tag 与日常
  腾讯云发布；systemd/nginx/生产数据操作继续单独授权。
- **研究可信度阶段 0 护栏**：新增 `research_manifest` 四级状态和证据校验；Strategy Lab
  新记录自动保存代码 commit，旧 JSON 记录保持可读但自动降级为“探索性”；Markdown 导出
  增加资格全集、数据区间、覆盖率、数据快照、执行/成本模型和缺失证据。脏工作树 commit
  标记 `-dirty` 并禁止晋级；模拟候选至少需要 100 笔严格样本外成交，监控通过还需至少
  20 个前瞻交易日和 30 笔前瞻成交。
- **研究可信度基线与中文总路线图**：固化 2026-07-13 数据覆盖、N 字短回放、模拟盘空表和
  已知数据问题；后续按数据契约、PIT 特征、统一 StrategySpec、账户模拟、严格样本外和
  前瞻模拟顺序执行。
- **最小 CI**：GitHub Actions 在 Python 3.11/3.12 + 锁定依赖下运行核心 ruff、DuckDB
  schema + 固定分钟 replay smoke 和全量 pytest，覆盖云端 3.11.6 与本地 3.12 两种环境。

### Changed

- 项目版本从 `0.17.0` 更新到 `0.17.1`；修复生产 Stage 1 审计发现的 218 条历史状态
  unknown/冲突误判，并保持重新上市、无效字段、空响应和截断响应继续 fail closed。

- 项目版本从 `0.16.0` 更新到 `0.17.0`；阶段 1 PR-D 完成契约新鲜度、停复牌事实、真实数据
  审计和正式研究阶段门。代码能力已就绪，阶段 1 仍需完成生产数据修复与覆盖率验收。

- 项目版本从 `0.15.0` 更新到 `0.16.0`；阶段 1 PR-C 将历史分钟数据准备从零散脚本升级为
  可复现资格分母、精确覆盖率、可恢复任务状态和显式研究快照阶段门。

- 项目版本从 `0.14.0` 更新到 `0.15.0`；阶段 1 PR-B 将历史证券状态、交易日池子捕获、
  日线/分钟质量、复权价格及数据可见性从隐含约定升级为可审计的 Point-in-Time 契约。
- 项目版本从 `0.13.2` 更新到 `0.14.0`；阶段 1 PR-A 引入版本化 DuckDB migration、
  数据集快照/覆盖率/质量问题元数据、Point-in-Time 数据契约和权威交易日历。同步与
  只读副本发布改为失败即阻断，并明确报告回滚、提交和副本发布的不确定状态。
- 项目版本从 `0.13.1` 更新到 `0.13.2`；旧 `scripts/deploy.sh` 明确降为人工基础设施发布，
  不再用于无人值守代码部署。
- 项目版本从长期失真的 `0.1.0` 对齐到当前 `0.13.0`；README 从 4 月 planning 草案更新为
  云端生产/本地研究分工、实际入口、DuckDB 并发约束和当前策略可信度。

- **surge 推送更早 + 临近涨停也推(2026-07-08,据 25 日回测校准)**:
  - **9:31 起就推**(原 9:33):`skip_first_minutes` 1→0(9:31 gi=1 即可确认,9:30 首格仍恒不确认)、
    `silent_until_hhmm` 09:33→09:31。盘中盯盘要尽早,代价是 9:31 分母仅 2 分钟累计、rel 略抖,
    靠 rel∈[2.5,8]+粗筛兜住;报文口径行由硬编码"(9:31)"改为按 skip 动态显示"X:XX起判"。
  - **临近涨停/封板的确认票也推**(原吞掉只落 events):`_evaluate` 里 unbuyable 不再只进
    `_pending_events`,改进 `_pending_push` 照推,报文按距涨停加 icon——`🔔临近涨停`(0<room≤门)/
    `🔒已封板`(room≤0),并加图例行"临近涨停/已封板 N 只:买入难度大,自行判断"。理由:2026-07-07
    回测证明最强的爆量往往就是这批秒板票,吞掉等于漏掉最强信号。`max_room_to_limit_pct` 语义从
    "不推"改为"标记仍推";`_pending_events` 保留恒空作扩展位。

### Changed

- **surge/全景页全市场快照数据源：爬东财/新浪 → tushare `rt_min`（根治云端 IP 反爬）**：
  2026-07-07 盘中云端 IP 被东财（RemoteDisconnected）+ 新浪（HTML 反爬页）双双拉黑，
  surge 零快照饿死、一早无推送。token 认证的 tushare `rt_min` 一次拉全部 A 股（~5600 只）
  从本机秒回、不吃 IP 反爬，是根治。落地（仅 `src/rquant/surge_watch.py`，不碰全景
  poller/adapter）：
  - **全市场快照改 rt_min + 累加器**：`fetch_full_market_snapshot(baseline, tracker)` 调
    `rt_min(全 A 股代码, 1min)` 一次（代码全集/名称/昨收在启动时从只读副本 `stock_basic`
    /`daily_bar` 预载，rt_min 不带名称/昨收），归一化成快照列（price=close、volume=vol、
    amount=当分钟量）。**单位核对**（2026-07-07 本机实测）：rt_min `amount`=**元**（与
    stk_mins 同族，300499 全日 stk_mins 合计 2.446936e9 元 = daily_bar 2.446936e6 千元
    ×1000，逐位吻合）、`vol`=**股**（806400×39.88≈32.2M≈amount），与旧东财快照列契约
    （volume=股/amount=元）一致，**无需换算**；唯 daily_bar 是千元（avg20 已 ×1000）。
  - **`CumulativeTracker`（纯内存、可单测）**：per ts_code 记 `{last_minute, cum_amount,
    cum_volume}`，仅当 rt_min 的 trade_time 分钟 **严格大于** 上次记录才累加该分钟
    amount/vol（防同分钟重复计、防分钟回退），输出快照 amount/volume=当日累计（语义对齐
    旧东财累计口径），price/涨停价用最新值 + `add_limit_prices`。
  - **重启续算 seed**：run 启动读上一份 `snapshot_full.parquet`（须为当日、含 trade_time）
    seed 累加器续到最近一 tick（重启只丢 tick 间隙，确认层 rt_min_daily 恒精确兜底）；
    非当日不 seed（从零）。
  - **确认层今日累计改 `rt_min_daily` 精确 cumsum**：新候选拉 `rt_min_daily(候选, 1min)`
    当日全序列 cumsum 得精确今日累计（一天一调、缓存），前 N 日同刻基线仍用 stk_mins；
    rt_min_daily 空/失败 → 退累加器近似 + warning（不阻塞）。
  - **删 surge 侧东财/新浪/socks 路由**：移除 `_fetch_em_clist` / `_snapshot_routes` /
    `_DEFAULT_SOCKS_PROXY` 及东财 clist/新浪 spot 依赖（全景页 poller 的东财三路 **保留**，
    Mac 本机全景仍用，仅 surge 换源）；全景页经共享 feed（snapshot_full.parquet 现来自
    tushare）间接受益，poller 代码零改动。
  - 盘中零 DB 写不变（累加器纯内存、快照 parquet 原子写）；不新增依赖（tushare 已在）；
    测试增补累加器/快照组装/seed/rt_min_daily 确认/rt_min 失败 miss 共 22 例（全离线 mock），
    既有 mock `_fetch_em_clist` 用例迁到 mock 注入的 rt_min，simulate 三戏路输出不变。

### Added

- **surge 爆量记录加"推送价"(入场价)**:SurgeConfirmed 新增 price 字段,确认时记当时价,
  写入 events jsonl;全景页「爆量记录」tab 加"推送价"列。这是"标记标的→次日 S 点→
  最终收益"闭环的入场价基准,此前台账缺此列无法算收益。

- **全景页微信友好登录网关（cookie 登录页替代 basic auth）**：微信内置浏览器不支持
  HTTP basic auth（不弹框、直接 401，朋友进不去），改成网页登录页 + 签名 cookie——微信
  HTTP basic auth（不弹框、直接 401，朋友进不去），改成网页登录页 + 签名 cookie——微信
  原生支持 cookie 与表单 POST，点链接即可登录。新增 `src/rquant/panorama_auth.py`（**纯
  标准库**，零第三方依赖：`http.server` / `hmac` / `hashlib.pbkdf2_hmac` / `base64` /
  `secrets` / `http.cookies`）：监听 127.0.0.1:8507 三端点——`GET /login` 返回移动端友好
  表单、`POST /login` 校验 pbkdf2（200000 迭代 + 随机 salt）通过则
  `Set-Cookie: rq_panorama=<签名令牌>; Max-Age=2592000; HttpOnly; Path=/; SameSite=Lax`
  + 302 回 `/`、`GET /verify` 供 nginx auth_request 子请求验签+验过期（200/401）。签名令牌
  `base64url(user|exp).hmac_sha256(user|exp, SECRET)` 无会话存储自验证，验签用
  `hmac.compare_digest` 防时序，`SECRET`（`RQUANT_PANORAMA_COOKIE_SECRET`）缺失则服务
  **拒绝启动**（SystemExit，不用空密钥静默降级），改 SECRET 即全体失效（应急踢人）。用户库
  `data/panorama-users.txt`（行 `user:pbkdf2_sha256$iter$salt$hash`，0600 权限，原子写）。
  落地：① CLI 四子命令 `panorama-auth-serve`（--host/--port，默认 127.0.0.1:8507）/
  `panorama-user-add <name>`（getpass 输密码两次确认）/ `panorama-user-remove` /
  `panorama-user-list`；② config 新增 `panorama_cookie_secret` +
  `panorama_users_path`（validation_alias 对齐 RQUANT_* env）；③
  `deploy/systemd/rquant-panorama-auth.service`（Type=simple/Restart=always，
  EnvironmentFile=.env 读 SECRET）；④ `deploy/nginx/rquant-panorama-cloud.conf` 改造为
  auth_request 版（`/_panorama_auth` 内部端点透传 Cookie 给 8507 `/verify` + `/login`
  反代 + `location /` 的 `auth_request` + `@go_login` 302，去掉 `auth_basic`；文件底部附
  无 auth_request 模块时的 `map $cookie_rq_panorama` 静态令牌降级片段注释）；⑤ 部署清单
  `docs/deploy/2026-07-06-panorama-login-gate-deploy.md`（生成 SECRET、建用户、起 auth
  service、`nginx -V` 检查 auth_request、reload、微信实测、旧 `.htpasswd-panorama` 保留、
  回滚换回 basic auth 版）。旧 basic auth（`.htpasswd-panorama`）保留备用不删。
  - **登录网关改 map 固定令牌方案（云端 nginx 无 auth_request 模块）**：云端 nginx 编译时
    缺 `ngx_http_auth_request_module`（`nginx -V | grep auth_request` 无输出），
    auth_request 指令跑不了，改用 nginx `map` 静态比对 cookie。登录成功后 `Set-Cookie` 下发
    **固定网关令牌**（所有已登录用户共用同一 cookie 值，nginx map 认这个字面值放行、
    `default 0` 拦截），不再是 per-user 签名令牌。网关令牌显式 `RQUANT_PANORAMA_GATE_TOKEN`
    优先，否则由 `RQUANT_PANORAMA_COOKIE_SECRET` 确定性派生
    （`hmac_sha256("panorama-gate", secret)[:32]`，重启稳定）。`GET /verify` 改为
    `compare_digest(cookie, GATE_TOKEN)`（保留供将来 auth_request 环境用）；`sign_token` /
    `verify_token` 保留（未来切回）。新增 CLI `panorama-gate-token`（打印生效令牌供部署写进
    map 文件）+ config `panorama_gate_token` 字段 + `panorama_gate_token_resolved` property。
    nginx conf 改为 map 版（`include rq-panorama-gate.map` + `if ($rq_panorama_ok = 0)`
    302，auth_request 版留档在文件末尾注释）。**权衡**：无法单独踢某用户（踢人 = 轮换令牌
    令全体重登），但 per-user 密码仍独立、登录审计可分辨谁登录。
- **全景页上云（云端直跑，零隧道）**：全景页从「Mac 拉数 + SSH 反向隧道」改为云端
  常驻——手机/朋友任意网络直访 `82.156.0.68:28080`（basic auth）→ 云 127.0.0.1:8506
  的 streamlit，poller 读同机 surge feed + 自拉兜底，零隧道、零 Mac 依赖（Mac 本地
  8506 与隧道照常保留，两套跑同一代码）。落地：① `deploy/systemd/rquant-panorama.service`
  （Type=simple/Restart=always，`--server.address 127.0.0.1`，unit 内 `Environment=` 覆盖
  `RQUANT_CLOUD_FEED_URL` 指向本地 `snapshot_full.parquet` + 置空 `RQUANT_PANORAMA_SOCKS`）；
  ② `deploy/nginx/rquant-panorama-cloud.conf`（28080 反代 8506，WebSocket 头齐全，沿用
  `.htpasswd-panorama`）；③ `deploy/systemd/rquant-kpl-snapshot.{service,timer}`（工作日
  16:35 跑 `rquant data-backfill --dataset kpl_concept --today` 把开盘啦题材成分写云端主库，
  写者串行槽位：monitor 15:02 止 / daily 17:00 起 / backup·replica-sync 只读拷贝不写主库，
  `Persistent=false` 避补跑撞 daily 写窗）；④ 部署清单
  `docs/deploy/2026-07-06-panorama-cloud-deploy.md`。`deploy/tunnel/README.md` 标注已被取代。
- **surge-watch 全市场单次取数重构（D1）**：盘中每分钟改为一次拉**全市场**快照
  （复用 panorama `_EM_SPOT_FS` 五段：沪深主板+创业+科创+北交所），检测层按
  `config.boards` 按 ts_code 前缀过滤回创业/科创（`_detection_domain` 复用
  `_classify_board`，ST 仍在 `_rough_candidates` 排除，检测行为与旧「只拉创业/科创」
  一致）；`snapshot_full.parquet` 落盘从每 5 分钟改为**每分钟与主循环同拍**（删掉独立的
  full 拉取代码路径与 `full_snapshot_fetcher` 形参），请求量反降（原创业科创 + 独立全市场
  两拉 → 一次全市场共用）。`RQUANT_SURGE_BOARDS` 保留为检测范围覆盖（default_factory）。
- **panorama poller 云端 feed 本地文件分支（D1）+ 分时段节奏（D2）**：`_default_cloud_feed`
  按 `RQUANT_CLOUD_FEED_URL` 值路由——`/` 开头或 `file://` 前缀走**本地文件**读
  （mtime 判新鲜 ≤120s，云端同机形态；缺失/陈旧回落自拉），HTTP(S) 分支保留（Mac P2，
  Last-Modified 判新鲜，零回归）；`SourcePoller` 新增 `off_hours_interval=600`——交易时段
  （工作日 09:00–15:10）用 60s、盘外/周末 600s（`is_off_hours` 纯函数 now 注入可测），
  云端 24/7 常驻的取数卫生。

### Changed

- **全景页移动端响应式屏效优化**(用户):PC(>768px)布局逐像素不变,窄屏(≤768px)
  单独优化——st.columns 两栏在手机竖排堆叠(左总表在上/右下钻+图表在下)、脉搏
  5 metric 换行 3 个/行、体系与周期 segmented 按钮换行、表格高度收紧(总表 560→360/
  下钻 250→240)、页边距字号缩小。纯 CSS @media 媒体查询(用 :has() 精准区分标题/
  脉搏/主两栏三类横排块,不误伤脉搏行),零 Python 布局改动。Playwright 双视口实测:
  PC 逐像素一致、移动 390px 无横向溢出且联动/图表可操作。

- **surge-watch 确认层 v2→v3:纯累计口径**(2026-07-06 全天真实分钟回测,用户 pinned):
  确认判定从「rel_cum_3d≥3.0 + VWAP门 + 单分钟增量门」改为单条累计比值门——
  今日 9:30 至今累计额 ÷ 前 4 日同刻累计中位 ∈ [2.5, 8.0],9:32 起确认。
  回测证明纯累计完胜 v2:v2 的曲线/VWAP/增量门把信号系统性拖到爆量展开后、
  买在阶段高点(全组合负 EV),纯累计买在爆量刚起(86% 在 10 点前、+0.53%/胜率51%);
  示例 300499 从 v2 的 -2.30% 翻 +2.83%(推送 10:44→9:41)。默认:cum_lookback_days=4、
  k_cum=2.5、ratio_cap=8(封 11-20× 出货毒尾)、skip_first_minutes=1(9:31 base 噪声)、
  k_rough 1.5→1.2(候选早入确认池);VWAP/增量门保留字段默认关。CLI 加
  --k-cum/--ratio-cap/--skip-first-minutes/--require-vwap。仍「观察提示,非买入信号」。

- **surge-watch 请求量与落盘节奏**：单进程每分钟只对 em 发一次全市场分页请求（兼作检测
  输入与共享 feed），替代原「创业科创检测拉 + 每 5min 全市场拉」的双拉路径。

- **每分钟爆量推送（surge-watch）+ 取数迁云端**：新模块
  `src/rquant/surge_watch.py` 与 CLI `rquant surge-watch [--dry-run] [--simulate DIR]
  [--force-session] [--max-ticks N]`（云端 systemd timer `Mon..Fri 09:25` 拉起、单进程
  每分钟循环、15:02 自然退出）。盘中拉创业板+科创板全量快照（em clist 直连，复用
  `panorama_data` 加固 Session：每次全新 + `trust_env=False` + 桌面 UA + Connection:close，
  fs 换 `m:0+t:80` 创业 / `m:1+t:23` 科创），两层判定：**粗筛**（零外部调用）当日累计额
  ≥ `K_rough(1.5) × 20日均额 × 进度曲线(t)` 且 pct_chg>0、非 ST、有 20 日基线；**确认层**
  （近 3 天口径，用户 pinned）对新候选拉 tushare `stk_mins` 近 3 交易日 1min bars 构 3 日
  同刻累计额中位，`rel_cum_3d = cum(t)/median_3d(t) ≥ K_confirm(2.0)` 且现价 ≥ 当日均价
  （快照 amount/volume 近似 VWAP）。tushare 限频队列 2/分、当日缓存不重拉、失败延后重试；
  每票每日仅推一次、9:33 前静默收集、单条 ≤8 只超出折叠；聚合一条 PushDeer（新 scene
  `surge_watch`，**只 admin**，PushPlus 跳过）+ append `data/surge_live/events-*.jsonl`，
  收盘落当日累计额序列 parquet。**绝不写 DuckDB**：仅启动预载只读副本 20 日均额 + kpl 题材，
  盘中零 DB 访问；时钟/源/推送/sleep 全可注入（单测不真 sleep 不碰网络）。守卫：非交易日即退、
  午休 sleep、快照连续 5 分钟 miss 推一条降级告警（error scene）并退避 60/120/300。
  口径 v1: rough1.5×20d·curve / confirm2.0×3d同刻 / VWAP门（产品初始值，跑几天按量级调）。
- **盘中累计成交额进度曲线标定**：`scripts/calibrate-intraday-curve.py` 从本地只读副本
  `minute_bar`（1min，取当日恰好 241 根的干净股·日样本）在 DuckDB 侧两级中位聚合（股内
  跨日中位 → 跨股中位）产出 `src/rquant/data/intraday_progress_curve.json`（241 点、单调
  不减、首≈0.006 尾=1；本次 1716 只样本）。随包分发（importlib.resources 定位，`.gitignore`
  加例外让 `src/rquant/data/*.json` 进 git）；surge-watch 启动加载，缺失 → 线性兜底 + warning。
- **panorama poller 云端 feed 第 0 路由（P2）**：`SourcePoller` 快照新增最高优先级路由 ——
  env `RQUANT_CLOUD_FEED_URL` 配置时 HTTP GET 云端 surge 全市场 `snapshot_full.parquet`
  （basic auth 走 `RQUANT_CLOUD_FEED_USER/PASS`），Last-Modified ≤120s 新鲜则本机不再自拉；
  **env 未配 → 行为与现状完全一致（零风险）**，陈旧/失败自动回落现有三级路由。配套
  `deploy/nginx/rquant-backup.conf` 加 `/feed/` location（8081，静态 parquet + basic auth）。
  surge-watch 每 5 分钟额外拉一次全市场快照落 `snapshot_full.parquet` 供 Mac 消费。
  部署清单 `docs/deploy/2026-07-06-surge-watch-deploy.md`（deploy.sh + systemd-analyze
  验证 + nginx location + .env 新增项 + 验证 curl）。
- **盘中 30 分钟脉搏 + 午间战报（midday_briefing）**：新模块
  `src/rquant/midday_briefing.py` 与两条 CLI —— `rquant morning-pulse
  [--slot HH:MM] [--force] [--dry-run]`（launchd Mon..Fri 10:00/10:30/11:00/
  11:30 各推一份 30 分钟脉搏，短、以增量为主）和 `rquant midday-report
  [--date] [--force] [--dry-run]`（12:00 推午间战报，五节全量）。全程**只读**：
  实时数据复用 `panorama_data` 三级路由（`fetch_market_snapshot` /
  `add_limit_prices` / `fetch_sector_fund_flow` / `build_board_overview`），
  T-1 数据只走 `open_readonly_store()`（连板现算的昨日涨停榜、候选池的 20 日
  均额、持仓体检的活跃仓），**绝不写 DuckDB 主库**；自产数据落 parquet
  （`data/midday/YYYY-MM-DD/`，pyarrow 引擎，无引擎降级 pickle）+ markdown
  （`data/reports/midday/YYYY-MM-DD.md`，逐节幂等 upsert）+ `meta.json`（各槽位
  route/fetched_at/pushed 去重）。槽位守卫：自动归槽容差 ±5min、迟到 >10min
  跳过；`--force` 手动补跑绕过去重，`--dry-run` 全流程跑但不推送（parquet 照落）。
  脉搏 Δ 来自与上一槽位 parquet 对比（首槽只报绝对值）；午间战报五节 =
  情绪温度（各槽位涨停走势 + 昨日终值）/ 连板梯队（昨 N 板 + 今涨停现算）/
  最强题材 Top5（kpl 口径 + 上午四槽位涨停演变）/ 下午候选观察池（创业·科创
  半日量能预筛，daily_bar 千元 ×1000 对齐快照元）/ 持仓午间体检（空仓整节省略）。
  notify 新增 `morning_pulse` / `midday_report` 两 scene（报文预渲染直通，只推
  admin）。调度：`deploy/launchd/com.roxor.rquant-{morning-pulse,midday-report}.plist`
  （跑主 checkout venv，Weekday 1-5 显式排周末，节假日兜底靠 CLI is_trading_day）
  + `scripts/install-midday-launchd.sh`（幂等 bootout/bootstrap）。
- **全机单一取数者：panorama poller 共享 drop（panorama_live）**：7/6 下午
  办公网 IP 因全机多进程全天高频访问被东财+sina 双风控（直连/SOCKS 同时 RST、
  sina 456），midday CLI 独立再拉三路只会雪上加霜。`SourcePoller` 每轮成功
  槽位后原子落盘（tmp+rename）`data/panorama_live/`：`snapshot.parquet` +
  `flow_{行业|概念}资金流.parquet` + `live_meta.json`（各源 as_of_iso/route/
  written_at；落盘失败只 log 不影响轮询，进程重启合并磁盘旧 meta 不丢他源
  记录）。midday 快照/资金流获取改三级优先：① 读共享 drop（快照 as_of ≤300s
  新鲜才用，报文与 meta.json 的 route 标注「共享:{原route}」）→ ② 自拉三级
  路由（总失败 sleep 60s 重试一次，仍在槽位容差内）→ ③ 降级短讯。drop 目录
  进 .gitignore。
- **T 日板块集合竞价强度因子 + 日度题材成分表（board_auction_strength）**：
  新增日度题材成分表 `kpl_concept_member_daily`（PK `(trade_date, board_code,
  con_code)`，与快照表 `kpl_concept_member` 只留「当前成分」不同，本表逐日存
  每天的打点，回测才能按 ≤T 最近打点还原信号日成分），配套 `data-backfill
  --dataset kpl_concept_daily` 逐日回补（复用逐日分页，normalize 不折叠）。
  新模块 `board_auction_strength(store, ts_code, signal_date, *,
  membership_lookback_days=30, hist_days=20)`：候选票所在题材（≤T 最近打点还原
  成分）在信号日集合竞价的整体强度——`board_gap_up_ratio`（题材内竞价价>昨收的
  高开占比）、`board_auction_amount_ratio`（题材当日竞价总额 / 过去 hist_days
  竞价总额中位）、`board_member_count`；一票多题材取资金比最强的题材；无题材
  归属返回 None。昨收取 daily_bar 的 T-1、竞价价/额取 auction_bar 的 09:25
  快照，全程 ≤signal_date（无未来函数）。接入 `growth_board_surge`：
  `GrowthBoardSurgeConfig` 新增 `require_board_favor` / `min_board_gap_up_ratio`
  / `min_board_auction_amount_ratio` 候选级闸门（早于分钟循环，缺归属/缺竞价
  历史保守拦截），`GROWTH_SURGE_V1_FACTORS` 加 `board_gap_up_ratio` /
  `board_auction_amount_ratio` 两个板块闸门因子（键名测试锁死），CLI
  `growth-board-surge-replay --require-board-favor --min-board-gap-up-ratio
  --min-board-auction-amount-ratio`（默认关=现状）。表进 `research_sync`
  MERGE 语义（本地回补权威，云端没有，整表替换会抹历史）。
- **科创/创业放量追击：用户三条件 + factor_confirm 评分确认
  （growth_board_surge）**：`GrowthBoardSurgeConfig` 新增开关组——
  ①量比：沿用 `min_cum_amount_ratio` 成交额口径宽门，另输出经典量比观察值
  `classic_volume_ratio`（当日每分钟均量 / T-1 可知 5 日每分钟均量）；
  ②内盘>外盘（`require_inner_outer`/`min_inner_outer_ratio`）：真实盘中内外盘
  无历史数据，用分钟 tick-rule 近似（close 对比前一分钟 close，升=外盘/降=内盘/
  平=均分，首分钟对比自身 open），按用户口径 inner/outer > 1 判多；
  ③大单净量（`require_large_net_vol`/`min_large_net_vol`）：T 日盘中不可知，
  用 T-1 `moneyflow_daily.large_net_vol` 防未来函数（与用户口径「今日」有一天
  滞后）；④`enable_factor_confirm`/`factor_score_threshold`：宽门不动，入场再过
  `growth_surge_b_v1` 加权评分（满分 100，复用 `score_feature_terms`；经典量比
  与市场温度只观察不计分）。新 factor_set `GROWTH_SURGE_V1`（signal_provenance，
  键名测试锁死），命中矩阵与观察值随交易行输出。CLI：`growth-board-surge-replay
  --require-inner-outer --require-large-net-vol --factor-confirm
  --factor-score-threshold`（默认全关=现状）。
- **竞价跳空分钟 B 确认加评分对照实验（死刑复核）**：
  `AuctionGapMinuteReplayConfig.factor_score_threshold`（默认 None=现状），
  入场分钟用既有 `auction_gap_v1` 键名因子按 `AUCTION_GAP_B_V1_SCORE_TERMS`
  打分过阈值（不引入新因子）；输出列 `auction_factor_score`。CLI：
  `auction-gap-minute-replay --factor-score-threshold`。归因终审已判死该策略线，
  此实验仅验证评分层是否翻案。
- **Strategy Lab factor_confirm 可见**：入场模式增加「多因子确认」，sidebar
  新增「多因子确认阈值」数字输入，经 `run_entry_mode_comparison` 透传，收益
  对比与历史记录带阈值参数。

- **N 字分钟级 B 点多因子确认（`entry_mode=factor_confirm`，设计文档
  2026-07-03-nshape-factor-confirm-design）**：minute_replay 新增确认层入场
  模式——宽门沿用 first_break 同门（强承接 + 破 T 高），B 决策改由
  `n_shape_b_v1` 加权评分过阈值决定（满分 100，只装已验证方向弹药：竞价
  强度/跳空、T 日官方封板质量、T-1 250 日低位百分位与均线多头、VWAP 位置；
  已证伪的相对放量与市场温度只观察不计分）。静态因子每候选预取一次
  （`_prefetch_nshape_static_factors`），缺数据按 0 贡献降级；入场快照
  signal_features 带完整因子命中矩阵（`signal_provenance` 新增
  `N_SHAPE_MINUTE_STRATEGY` / `N_SHAPE_V1` / `N_SHAPE_V1_FACTORS`，键名
  三端锁死）。`topn_selection.score_feature_terms` 新增单项打分薄公共入口
  （复用 `_score_term`，不复制 transform 公式）。CLI：`minute-replay
  --entry-mode factor_confirm --factor-score-threshold`（默认 35，训练段网格中位档）；
  `run_entry_mode_comparison` 支持透传阈值。回测结论见
  docs/analysis/2026-07-03-nshape-factor-confirm.md。

- **封板质量驱动的条件持有期（seal_hold，归因报告决策项 B）**：
  `AuctionGapMinuteReplayConfig` 新增 `seal_hold_*` 配置组——B 日收盘封住
  （`b_close_at_limit_up`）且封板质量达标（官方 `limit_list_daily.open_times`
  ≤ 阈值，可选封单金额/流通市值占比下限；官方缺行回退分钟推算 `b_open_times`）
  的仓位持有上限从 `max_hold_days` 放宽到 `seal_hold_max_days`，竞价弱退/VWAP
  破位/止损/移动止盈照旧生效；其余仓位维持 T+1。输出列加 `hold_policy`
  （t1/seal_hold）。多日持有窗口分钟数据缺失时新增日线降级退出
  （`_run_daily_tail_exit`，逐日近似 gap/止损/移动止盈，输出列
  `exit_daily_fallback` 标记）。CLI：`auction-gap-minute-replay --seal-hold-days
  --seal-hold-max-open-times`（默认关闭=现状）。

- **盘中市场全景页 P0**（`market_panorama.py`，端口 8506，仅本地）：涨停/跌停/
  炸板实时计数 + 分钟 sparkline、东财板块资金流排行（行业/概念、额/率双排序）、
  板块成交额排行（东财 1022 板块成分聚合，行业粗分兜底）、板块下钻成分股表
  （含池内标记）；外部源故障灰态降级，全程只读副本零写主库。
- **模拟盘信号溯源 P0**：paper_position 加 strategy_name / signal_factors JSON /
  run_mode / run_id 列（幂等迁移）；FactorSpec 统一因子键名（auction_gap_v1，
  与全景页共用）；`persist_position_with_provenance` 统一写入口（快照+仓位同
  事务）；replay `--persist-positions --run-id` 可落库带溯源的模拟仓（默认不落），
  复盘查询 `query_paper_positions` + 按因子命中四象限聚合。

- **统一「按日数据集回补」层（`rquant data-backfill`）**：新增
  `rquant/dataset_backfill.py` 注册表（`DatasetSpec` + `DATASETS`），一条管线
  接入 19 个 Tushare 接口：板块日行情（ths_daily/dc_daily）、板块列表与成分
  快照（ths_index/ths_member/dc_index/dc_member）、资金流 7 口径（moneyflow
  全 20 字段 + dc/ths 个股 + ths 行业/概念 + dc 板块 + dc 大盘）、龙虎榜
  （top_list/top_inst）、开盘啦榜单（kpl_list）、市场交易统计（daily_info）、
  游资名录（hm_list）、主要指数日线（index_daily → 现有 index_daily_bar）。
  by_date 模式复用 trade_cal 日历 + 单日故障隔离 + 幂等 upsert，snapshot 模式
  一次拉全量整表替换（空快照拒绝替换）；限频退避统一走 adapter 泛化后的
  `_call_with_backoff`（原 `_call_by_date_with_backoff`），dc_member/ths_member
  经 offset 分页拉全量（实测单页 8000/6000 封顶）。全部 17 张新表 +
  moneyflow_daily 迁移列注册进 research_sync `MERGE_TABLES`，日终合并不会
  抹掉本地回补历史。CLI：`data-backfill --dataset <name|all> --start-date
  --end-date [--dry-run]`，日终增量 `--dataset <name> --today`。

- **竞价跳空特征归因（roadmap #10）**：`scripts/analyze_auction_gap_attribution.py`
  + `docs/analysis/2026-07-02-auction-gap-feature-attribution.md`。1028 笔全区间
  归因结论：盈亏由收盘封板决定、每个入场位置期望均为负、最优过滤费后打平，
  原始竞价跳空 + T+1 判死；附三个下一步决策项（lookback 回补 / 持有期改造 /
  转回 N 字主线）。

- **本地热备与研究库分家（`rquant research-sync`）**：云端快照改落
  `data/cloud_backup.duckdb` 纯备份工件，不再整文件替换本地主库；生产表
  （daily_bar/screen_result 等 9 张）从备份整表替换，研究表（minute_bar/
  auction_bar/模拟盘等 9 张）按主键 merge，本地研究数据永不被热备冲掉。
  支持 `--restore-from` 从旧副本恢复研究表；合并后原子刷新本地只读副本。
- **盘中实时源紧急回退开关**：`INTRADAY_QUOTE_SOURCE=akshare` 可在不改代码
  的情况下把 monitor 实时源从 Tushare rt_min 切回 akshare（权限到期/故障
  止损用），默认 `tushare`。
- **Strategy Lab 后台任务**：新增 `rquant lab-run --spec` 后台执行入口与
  `strategy_lab_worker` 状态文件机制，自动优化可提交为后台任务、可取消，
  关掉浏览器不再丢结果；「历史记录」页签可查看/管理后台任务。

### Changed

- **surge-watch 确认层口径 v1→v2 收紧**(2026-07-06 全天真实分钟重放 + 24 组合门槛
  扫描依据,首个交易日上线前):k_confirm 2.0→3.0(4.0 存在逆选择——极端爆量多为
  出货盘,勿再抬)、新增同分钟增量门(当分钟增量 ≥3×3日同分钟中位,None-fail)、
  新增可买性守卫(距涨停 ≤1% 或已封板不推送,events 标 unbuyable 且占每日名额)、
  报文标注「观察提示,非买入信号」。重放对照:81 推/胜率 32.1%/均值 −1.15% →
  31 推/35.5%/−0.86。CLI 加 --k-confirm/--k-delta/--max-room。扫描四发现:kc4.0
  逆选择、增量门方向对但确认时点近恒真、涨幅窗连坐赢家(不加)、当日下午信号
  优于上午(不上仅上午模式)。产物与扫描明细见 jobs surge_replay/。

- **盘中市场全景 v2：屏效 + 性能 + 合表联动 + 个股图表**（8506，用户五点需求）：
  ①屏效——单屏两栏布局（左 52% 板块总表 / 右 48% 下钻+图表），隐藏 Streamlit
  顶栏、CSS 压缩边距、消灭全部 tab 与 divider，脉搏 sparkline 收进 popover，
  1440×900 全区块首屏可见；②性能——聚合结果按快照时间戳缓存（cached_overview /
  cached_constituents，交互零重算，实测体系切换→全区更新 105ms）、排序改
  st.dataframe 列头点击（纯客户端零 rerun）、快照 TTL 30s→60s 对齐 fragment、
  **全市场快照三级路由**（东财 spot 直连→SOCKS 云端出口→sina 兜底；sina
  逐页爬全市场实测单次 >90s，是「切换等太久」的真实大头，东财路由盘中秒级）；
  ③合表——资金流/成交额/涨停数合并为一张 build_board_overview 总表（东财 BK 码
  +".DC" 与 dc_board 精确 join，ths 兜底按名 join；开盘啦体系无资金流口径自动隐列），
  体系三选一 segmented_control；④联动——总表行选择→板块下钻（强度分默认序）；
  ⑤级联——下钻行选择→个股图表（分时/5日/日K，altair 蜡烛红涨绿跌+MA5/10/20+量柱，
  分时东财 trends2 直连→SOCKS→sina 三级路由，日K 只读副本+盘中快照拼当日 bar，
  量纲股→手 ÷100 对齐）。新增 RQUANT_PANORAMA_FAKE=1 确定性 fixture 模式支撑
  离线 e2e；panorama_data 新增 build_board_overview / fetch_intraday_trend /
  load_daily_kline，_normalize_em_flow 保留 f12 板块码。单测 853→860，
  playwright e2e 8 条（冷启动/合表/联动/级联/性能/空态/屏效/真实模式）全过。
  见 docs/plans/2026-07-06-panorama-v2.md。
- **创业/科创放量策略退出结构：持仓 1→3 日 + 单票止损 −4%→−5%**：
  `GrowthBoardSurgeConfig.max_hold_days` 默认 1→3，`paper.stop_loss_pct` 0.04→0.05
  （take_profit 8% / trailing 3% 不动），CLI `--max-hold-days` 默认 1→3。2026-07-04
  实验（训练 ≤2025-12-31 / 验证 2026-01+，入场不变同 546/748/1294 笔）：T+1 硬出把
  赢家砍早了，放到 3 日让 2-3 日延续涨幅接住，均收益训练 2.96→4.61 / 验证 1.09→1.42
  （同向为正=真增益）；止损 4%→5% 对 worst 值中性（尾部由隔夜 gap_stop 主导）。
  同一实验验证板块窗口 3→2 更差（验证段 1.01 vs 1.78），维持 board_hist_days=3；
  板块门本身仍默认关（更彩票化，可选过滤器）。见
  `docs/analysis/2026-07-05-growth-exit-structure-hold3-stop5.md`。
- **板块集合竞价额历史窗口 20 日 → 3 日（board_hist_days 解耦）**：原先板块竞价额
  相对历史的比较窗口误复用核心爆量因子的 `lookback_days`（20），改板块窗口会连带
  改爆量同刻中位口径。解耦为独立字段 `GrowthBoardSurgeConfig.board_hist_days`
  （默认 3），CLI 加 `--board-hist-days`（默认 3）。2026-07-04 实验（训练 ≤2025-12-31
  / 验证 2026-01+）：3 日窗口在三档阈值上全面 ≥ 20 日，且把 g50a10 档从 v3 的
  「训练段负贡献=噪音」升级为训练/验证同向的弱真信号（板块资金青睐是短周期情绪，
  20 日中位摊平了当下强弱）。板块闸门本身仍默认关（对裸 9:30 基线增益薄、砍样本狠，
  只作可选选择性/降尾部过滤器）。见 `docs/analysis/2026-07-04-growth-board-window-3d.md`。
- **Strategy Lab 交互重构一期**：收益对比/交易明细/退出原因结果进
  `st.session_state`（切页签不再丢）；收益对比结果同样落盘到历史记录；
  sidebar 与自动优化/科创放量参数区包进 `st.form`（改参数不再整页刷新）；
  自动优化默认值改为小跑组合（持有期 1/2/3 + 基础评分画像）。

### Fixed

- **分时图盘中被拉伸成整天(2026-07-07)**:#54 分时图 x 轴改 bar 序号后,单日刻度
  按"数据条数等分"放 9:30/10:30/11:30/14:00/15:00,满仓 240 根没问题,但盘中只有
  ~100 根时把半天数据拉伸铺满整宽、五刻度等分,看着像 9:30-15:00 实际只到当前分钟。
  修:单日 x 轴刻度钉死在全天真实位置(0/60/120/180/239),x 轴定域 [0,239]——盘中
  数据不足整天时线只填左边一截、停在当前时刻、右边留空。多日不受影响。

- **surge-watch 云端快照饿死事故(2026-07-07 盘中)——加新浪兜底**:surge 全市场
  快照 fetcher 只有东财直连+SOCKS 两路、无 sina 兜底,东财对云端每分钟全市场拉取
  反爬(RemoteDisconnected)后两路全灭 → 零快照 → 从不检测/推送(实盘一整早无推送)。
  且默认还去连 Mac 专属 SOCKS(127.0.0.1:1086,云端不存在)刷屏 refused。修复:
  fetch_full_market_snapshot 加新浪兜底(东财全掐时降级,慢但有数据),对齐全景页
  三路韧性;socks 默认关(仅显式配 RQUANT_PANORAMA_SOCKS 才试)。

- **全景页个股分时/5日图午休与隔夜空档**：x 轴从真实时间轴（dt:T）改为 bar 序号
  （行情软件标准做法），11:30-13:00 午休与 5 日图的隔夜空档消除、线连续；刻度标签
  映射回时间（09:30/10:30/11:30|13:00/14:00/15:00，5 日按日首根标日期），tooltip
  保留真实时刻；fake 分时 fixture 时间戳同步改为真实交易时段分布（否则空档不可见、
  修复无法视觉验证）。

- **全景页盘中不可用事故（2026-07-06 开盘）——渲染与取数解耦**：东财风控在开盘
  高峰钉住长驻进程（233 次 RST，新进程同刻三路全通），sina 兜底又被每分钟 70 页
  重复爬取激怒到超时，三路全灭 → 空快照；同步取数架构把页面交互全部阻塞在取数链
  上（最长 90s+）。修复：新增 `panorama_poller.SourcePoller` 后台单飞拉取线程
  （快照+两路资金流），UI 只读内存 slot **永不同步拉外部源**，交互恒秒开；
  last-known-good 语义（源挂保留旧数据+新鲜度标注，age>180s ⚠️+错误摘要，
  不再出现空白「暂不可用」）；每源独立熔断（连败 3 冷却 180s，sina 级连败 2
  冷却 300s，不再反复撞风控）；em 全部请求改每次全新 Session + 浏览器 UA +
  Connection: close + trust_env=False（快照直连级弃 akshare 统一自实现分页）。
  单测 860→873（poller 13 条），fake e2e 回归全过。


- **watchdog 尾盘监控真空（审计 PR3-F）**：`rquant-monitor-watchdog.timer` 原
  `OnCalendar=Mon..Fri *-*-* 9..14:0/2` 最后一次触发是 **14:58**，但脚本窗口到 15:00。
  若 monitor 在 14:58–15:00（含 14:57-15:00 收盘集合竞价，A 股最关键时段）死掉，
  下次 watchdog 要等次日 09:00，尾盘完全无守护。扩到 `9..15:0/2` 让 15:00 也触发一次
  收尾检查（脚本 `NOW>1500` gate 让 15:00 动作、15:02+ 静默退）。
  ⚠️ systemd unit 改动，merge 前须云端 `systemd-analyze calendar 'Mon..Fri *-*-* 9..15:0/2' --iterations 5` 确认步进 2min。
- **7/2 本地主库损坏事故根治**：`sync-from-cloud.sh` 整文件替换主库导致
  盘中 monitor 写入丢进幽灵 inode、残留 WAL 代际错配打不开库。下载与合并
  分离后杜绝；`research-sync` 内置陈旧 WAL 抢救（挪 `.corrupt-<ts>.bak`
  后重试）与副本撕裂防护（主库有活跃 WAL 时拒绝刷新副本）。
- 卸载本地僵尸 LaunchAgent `com.roxor.rquant`（`serve --hour 17` 与云端
  daily 重复跑、重复推送，2026-05-20 起残留）。
- **对抗式审查修复（23 项确认缺陷，55-agent workflow 双反驳验证）**，要点：
  - `sync-from-cloud.sh`：日终合并加 `.last-research-sync-date` 记账 +
    睡过 17:10-17:30 窗口后任意 tick 追赶补跑；mkdir 原子锁防并发截断；
    `--force` 显式下载+合并；合并失败告警 30min cooldown
  - `research-sync`：ATTACH 路径单引号转义（含 `'` 的备份卷路径不再炸）；
    restore 改 `INSERT OR IGNORE`（旧副本不再覆盖本地已更新行）；
    有表失败时跳过副本刷新（不发布跨表不一致快照）；顶层异常转报告
    避免 CLI 与脚本双重告警
  - monitor：`rt_min` 加 `RT_MIN_POLL_SECONDS`（默认 15s）节流，不再每
    5s 打分钟级 API；闭市时段启动直接退出，不再整晚占 DuckDB 写锁
  - 存储：`query_minute_bars` 跨 source 去重（stk_mins 优先于盘中 rt），
    研究口径不再把同一分钟双计
  - 云端 `rquant-daily.service` 加 `--skip-minute-backfill`：分钟历史只在
    本地按需回补，不放大云端主库与三条 5 分钟拷贝链路
  - Strategy Lab worker：状态文件原子写；cancel 校验进程组防 pid 复用误杀；
    cancel/done TOCTOU 不再丢 `saved_run_id`；僵尸进程不再永久显示运行中；
    后台任务防重复提交
- `_detect_st` 容忍 NaN 名字（长区间回补遇退市票 join 不到 stock_basic
  时不再 AttributeError 崩掉整个回补）。

- **Tushare 全量接口审计与接入目录**：新增官方文档抓取脚本和结构化接口目录，
  用 `TUSHARE_COOKIE` 抓取 268 个文档入口、合并 MCP 元数据，并将 141 个
  A 股可调用接口转成 `tushare_interface_catalog`，按已接入、盘中/竞价、
  策略特征、环境过滤、参考观察分层；Strategy Lab 新增“数据接口”页签，可按
  阶段、状态、权限、能力标签筛选后续接入候选。
  - 同步抓取购买页当前积分、积分商品、独立权限和套餐报价，写入
    `tushare_account_score` / `tushare_purchase_goods` / `tushare_activity_packages`
    独立表；接口目录按 API/doc_id 关联独立权限报价，并显示当前积分缺口和补积分
    估算成本。
  - 新增接口历史覆盖审计字段：`history_coverage_type` / `history_start` /
    `history_coverage_note`，用于判断新权限开通后是否能立即回补历史数据；已确认
    `stk_auction` 历史从 `2025-01` 开始，`rt_min_daily` 属于仅当日开盘以来。
  - 新增盘中实时分钟与集合竞价迭代路线图：
    `docs/plans/2026-06-26-intraday-rt-auction-roadmap.md`。
- **集合竞价数据链路**：接入 Tushare Pro `stk_auction` 当日集合竞价成交接口，
  新增 `auction_bar` 表和 `rquant auction-backfill` 命令，按本地 `daily_bar`
  交易日历回补 2025-01 以来历史集合竞价数据。
  - `auction_bar` 按 `(ts_code, trade_date, auction_type, source)` 幂等落库，
    先支持 `open_realtime`，后续可扩展开盘/收盘盘后集合竞价接口
  - `stk_auction` 与历史分钟一样不自动切备用 token，避免备用 token 未开独立权限时
    掩盖真实权限错误
  - 已完成 `2025-01-01` 至 `2026-06-26` 本地回补，实际非空数据范围为
    `2025-01-16` 至 `2026-06-25`，共 `1,901,255` 行、`345` 个交易日
- **集合竞价跳空高开策略回测**：新增 `rquant auction-gap-replay`，可将同花顺
  “跳空高开 + 竞价量/近5日均量 + 今日未涨停 + 非 ST”动态分组规则转成可回放策略。
  - 支持昨收跳空与昨高跳空两种 gap 口径，支持严格 ST 过滤和近似同花顺小写 `st`
    过滤口径
  - 按 Tushare 日线成交量单位自动换算“竞价量 / 近 5 日均量”，并用次一交易日开盘价
    作为符合 A 股 T+1 的无未来函数基准离场
  - 新增 `rquant auction-gap-minute-replay`：集合竞价只生成候选，B 日等待开盘后
    1 分钟因子确认并用下一分钟开盘价成交；S 日结合 B 日封板强度、次日集合竞价强弱、
    次日早盘 VWAP 破位和移动止盈止损
  - 新增 `rquant auction-gap-minute-backfill`：先按集合竞价规则生成候选，再只回补
    候选从信号日到退出窗口的分钟线，避免为了一条策略盲目补全市场全历史分钟线
  - 已完成 `2026-04-16` 至 `2026-06-24` 集合竞价候选分钟窗口短区间回补：
    `249` 个候选、`249` 次请求、`0` 失败，写入 `121,223` 行分钟线；首轮
    分钟 B/S replay 为 `134` 笔交易、平均收益 `-1.51%`、胜率 `27.61%`
  - 分钟因子层新增日内加速度和开盘段标记：保留同分钟历史基准、累计成交额进度基准，
    并增加 5/10 分钟成交额加速度；9:30-9:32 暂作为可调 opening segment，不参与普通滚动加速度
  - Strategy Lab 新增“集合竞价跳空”页签，对比“竞价直接 B/次日开盘 S”和
    “竞价候选/分钟 B/S”的候选数、触发率、收益、胜率和弱竞价退出占比
  - 新增中文分析文档：
    `docs/plans/2026-06-26-auction-gap-strategy.md`
- **盘中上攻信号监控**：基于 6/25 Pool1 / Pool2 次日涨停归因结果，先不新增高风险数据源，
  改为吃满现有 AKShare/Sina 实时源已提供的 `今开` / `最高` / `昨收` / `涨跌幅` /
  `成交量` / `成交额` 字段。
  - 新增 `RealtimeQuote` Pydantic 行情模型和 `fetch_realtime_quotes()`，保留
    `fetch_realtime_prices()` 兼容旧调用
  - `build_watchlist()` 补齐 T 日高点、T 日收盘价、T+1 涨停价，供盘中上攻判断使用
  - `monitor.check_attack_signals()` 新增开盘强、强承接、突破 T 高、临近涨停 4 类信号，
    替代原 40/30/20/强止/弱止盘中回踩提醒
  - 通知文案新增上攻信号标签（开盘强 / 强承接 / 突破T高 / 临近涨停）
  - 新增 forward premium 目标函数复盘文档：不再只评估次日涨停，改看入池后 / 信号后
    1、3、5、10 日最大溢价
- **准实盘模拟盘止损基础**：新增 `rquant.paper` 纯逻辑模块和实施计划文档，
  将 B 入场定义为候选池标的第一次触发 `attack_*` 信号，并在入场瞬间冻结
  `entry_price` / `entry_signal` / `stop_loss_price` / `stop_loss_basis`。
  - 初始止损线采用“结构止损优先、百分比兜底、入场价下方缓冲上限”的候选参数框架
  - 支持 A 股 T+1 出口门禁：B 入场当天即使触发止损/止盈也不模拟卖出
  - 支持普通止损、跳空止损、移动利润保护候选机制
  - 每笔模拟仓冻结 `candidate_id`，后续用于历史分时 replay / walk-forward 对比，
    不把 3% / 5% / 2.5% 这类临时值当作策略结论
  - 同一只票同一交易日不重复开模拟仓
- **历史分钟数据地基**：接入 Tushare Pro `stk_mins` 历史分钟接口，并新增分钟线 /
  特征快照 / 模拟盘持仓事件相关表结构。
  - 新增 `minute_bar` 表，按 `(ts_code, trade_time, freq, source)` 幂等落库
  - 新增 `intraday_feature_snapshot` 表，预留 Pool1 入池后 90 日分钟价量结构特征
  - 新增 `paper_position` / `paper_position_event` 表，支持后续模拟盘分批止盈、
    移动保护、复盘事件流
  - 新增 `rquant minute-backfill` 命令，可在 Pool1 出结果后回补标的前 N 个交易日
    历史分钟；已用 `600000.SH` 2026-06-24 1min 数据端到端 smoke 写入 241 行
- **历史分钟 replay 初版**：新增 `rquant.minute_replay` 和 CLI 固定路径：
  `minute-replay-backfill` 先回补 T 日命中标的从 B 日到退出窗口的 1min 数据，
  `minute-replay` 再按“B 日累计低点不破 T 收盘、累计高点突破 T 高点”触发模拟买入。
  - replay 复用 `rquant.paper` 的 A 股 T+1、结构/百分比止损和移动利润保护逻辑
  - 分钟内退出采用保守顺序：先按上一分钟已有止损/止盈线判断，再用本分钟高点更新
    下一分钟移动止盈线，避免同一分钟 high/low 顺序的未来函数
  - 新增除权/复权价格基准处理：盘中信号用实时 `昨收` 动态缩放 T 日参考价；模拟仓跨
    价格断点时同步缩放成本、止损、止盈、移动止盈线，并保留 `entry_price_raw`
    原始成交价用于复盘
  - 新增 3 种入场模式对比：`first_break`（首次突破）、`break_retest`（突破后回踩确认）、
    `late_confirm`（10:30 后确认），用于同一收益口径下做科学对照
  - Streamlit dashboard 新增“分钟策略实验室”：支持日期区间、持有天数、入场模式多选，
    展示触发率、均值/中位收益、胜率、跳空止损率、退出原因和交易明细
  - 新增独立 Streamlit 页面 `src/rquant/dashboard/strategy_lab.py`（建议端口 8504），
    将分钟 replay、90 日价量分布覆盖率检查、参数组合收益对比从健康看板中拆出，
    避免与 30 秒健康监控刷新互相干扰
  - 策略实验室新增“自动优化”页签，并新增 `rquant.strategy_optimizer`：
    自动枚举入场模式 / 风控版本 / 持有期，按训练区间与验证区间生成策略排行榜，
    避免人工逐个勾选组合
  - 自动优化器新增“触发后按特征分取 topN”对比：先不改变交易触发，只比较
    全量触发与每日 top1 / top2 / top3 / top5 等特征排序样本的训练/验证收益差异，
    并在策略实验室中展示 topN 排行和入选样本
  - topN 特征分升级为可配置评分画像：新增基础版、分时放量/建仓代理/高低位/市场环境
    消融版，以及轻量权重偏置版；自动优化器可同时比较多个评分画像，并将画像名写入
    candidate_id
  - 新增 topN walk-forward 验证：按时间顺序生成 expanding-window 折，只用过去日期训练、
    后续日期验证，并在策略实验室展示出样本排行和入选样本
  - Strategy Lab 自动优化页签新增开跑前工作量与耗时估算，按区间候选、持有期、
    入场模式、风控版本、topN、评分画像和 walk-forward 折数展示 replay 次数、
    候选扫描量、topN 组合数和预计耗时；新增中文说明书
    `docs/strategy-lab-auto-optimization-guide.md`
  - Strategy Lab 新增研究历史记录：自动优化与集合竞价跳空每次运行后都会保存
    Markdown + JSON 到 `data/strategy_lab_runs/`，页面新增“历史记录”页签，可回看、
    下载 Markdown，并把结果文件路径暴露给后续 agent 分析
  - 新增 Pool1+Pool2 合并 replay 口径 `n-shape-combined`：同日同代码重复时 Pool2
    优先，用于把 Pool1 新候选与 Pool2 持续观察标的一起做策略研究
  - 新增 replay trade cache、风控参数搜索和特征权重搜索模块：可复用已跑出的分钟
    replay 样本，比较 stop loss / take profit / trailing stop 组合，以及不同特征组
    权重乘数下的 topN 排序效果
  - replay cache 下沉到入场事件层：新增 `ReplayEntrySnapshot` / `EntryReplayCache`，
    将分钟信号识别、执行价、风控计划、退出分钟窗口和特征快照缓存起来；风控参数搜索
    可先加载一次入场快照，再对多组 stop loss / take profit / trailing stop 做退出重放
  - 新增 `volume_profile` 特征层，用历史分钟线近似计算 90 日价量分布所需的
    VWAP、POC、70% value area、上/下方成交额占比，为后续买卖价、止盈止损动态化预留结构依据
  - 90 日价量分布已接入 replay 风控：可按 POC 收复情况过滤入场，用下方筹码
    支撑生成结构止损，用上方筹码压力或固定收益兜底生成止盈，并在 dashboard 中与
    baseline 同口径对比
  - 日终 daily pipeline 筛选完成后可自动回补当日 Pool1 的 90 日分钟上下文；
    `rquant run-daily` 默认开启，可用 `--skip-minute-backfill` 跳过
  - 已完成 2026-04-16 至 2026-06-24 Pool1 历史样本 90 日分钟上下文回补：
    `minute_bar` 共 8,961,585 行、394 只股票、135 个交易日；479 个 Pool1
    候选样本的 90 日价量分布覆盖率为 100%

### Changed

- **分钟 replay 成交口径更保守**：盘中强承接/突破信号在当前 1 分钟 K 收完后确认，
  买入改为下一分钟开盘价成交，避免同一根分钟 K 内“看到 high/low/close 后仍按
  当前 close 买入”的乐观假设；`PaperTradeConfig` 新增 `entry_slippage_pct` 预留滑点。
- **90 日价量分布复权归一**：`calculate_volume_profile()` 在 `adj_factor` 可用时，
  将历史分钟估算成交价缩放到参考日价格基准后再计算 POC / value area；
  `ingest_daily()` 日终同步写入当日 `adj_factor`。

### Removed

- **盘中回踩档位提醒**：移除 `monitor.check_levels()` 和 40 / 30 / 20 / 强止 / 弱止
  盘中事件触发逻辑。强止 / 弱止价位仍作为 Pool2 退出风控参考保留，不再用于盘中通知。

### Fixed

- **Tushare 历史分钟批量回补鲁棒性**：`stk_mins` 不再自动切换到备用 token
  （备用 token 未必开通分钟权限，实测会退化成 1次/min 限频）；`minute-replay-backfill`
  单只请求失败时记录 `failed_requests` 并继续后续标的，避免一只限频/异常打断整批回补。
- **daily / monitor 全面故障隔离（审计后系统性加固）**：连续多起"外部依赖临时故障
  搞崩整条定时任务"事故（接口下线 / 数据延迟 / 网络超时）后，一次全面 review 给核心
  定时任务加故障隔离，原则是"单点异常不拖垮整条流程"：
  - `pipeline.py`：daily 流水线 preset 循环每个 preset 独立 try/except（失败标
    `summary=-1` + 推 error 通知，继续其他 preset）；`_sync_pool2_watch` /
    `check_exits` / `_push_daily_summary` 各自独立 try/except——保证某 preset 或
    某步失败不连带打掉 `check_exits` 兜底（Pool 2 退出检查）
  - `monitor.py`：盘中主循环单票 try/except（某只票存库 / notify / 日期计算异常不
    终止整个盯盘进程）；`check_exits` 单票 try/except（某只票退出处理失败不中断整批）；
    `fetch_realtime_prices` 加 akshare 列存在性校验（改列名时返回空而非 KeyError 崩）
  - `cli.py::_ingest_with_retry` 重试范围从"仅 `RequestException`"扩到"所有 ingest
    异常"——覆盖 tushare 服务端业务错误（限频 / 接口下线，客户端抛裸 `Exception`），
    短间隔重试，耗尽仍失败则 `raise` 不吞
  - `loader.py`：补列从"仅 IND/BASIC 的 `[0]`"扩到"IND/BASIC/STATE 各 offset + 标量
    `is_st`/`is_bj`/`board_type` 默认值"——daily_state 整表缺失也不让 `not_st` /
    `board_in` / Pool 2 的 `BODY_UPPER[1]` 引用崩
  - 新增 7 个故障隔离单测，全量 427 passed

- **ingest 网络超时搞崩 daily pipeline**（6/4 真实事故）：6/4 17:00 拉 tushare
  `stock_basic` 时 30s 读超时（`requests.exceptions.ReadTimeout`），
  `rquant-daily.service` exit 1。根因：`_ingest_with_retry` 的重试只覆盖
  `bar_count == 0`（数据未就绪），不捕获异常 → 网络抖动直接冒泡崩溃。
  - `cli.py::_ingest_with_retry` 改为捕获所有 ingest 异常（见上条），短间隔
    （`_NETWORK_RETRY_INTERVAL = 60s`）重试，与"数据未就绪"15min 长间隔区分
  - 恢复方式：网络恢复后 `rquant run-daily --date <date>` 重拉
- **告警链路可靠性加固（审计 PR2）**：全面 review 发现告警/兜底链路多处会静默失效。
  - **告警黑洞兜底（E）**：`alert-on-failure.sh` 原用 `exec rquant alert`，PushDeer/
    PushPlus 全推送失败时告警**静默消失**（所有业务 service 的 OnFailure 都汇到这里）。
    改为捕获失败 → 落盘 `logs/alert-failures.jsonl`；`daily-report` 扫描当日记录并入
    日报正文（`health.py::_read_recent_alert_failures`），保证故障"至少服务器有记录 +
    日报能看见"。
  - **守护/备份缺 OnFailure（J）**：`rquant-monitor-watchdog.service`（盘中 monitor
    自愈的唯一通道）和 `rquant-backup.service` 都没配 OnFailure，自身崩了无人知。
    各加 `OnFailure=rquant-alert@%n.service`（原 watchdog 注释"不应触发 watchdog 链"
    是误解，alert 是独立 oneshot）。
  - **token 提醒推送失败静默（M2）**：`remind-tushare-token-renewal.sh` 推送失败仍
    exit 0（最后一句是 print），一次性 timer 触发后不再来 → 续费提醒永久丢。改为
    全失败 `sys.exit(1)` 让 OnFailure 接管落盘兜底。
  - **daily-report 直连主库（I）**：`health.py` 改用 `open_readonly_store()` 优先读
    副本，避开 monitor 延后退出 / backup 持锁时的 `IOError` fatal exit（遵循
    CLAUDE.md DuckDB 并发约定）。

- **daily_basic 数据源临时缺失搞崩整条 pipeline**（5/29 真实事故）：5/29 daily_bar
  拉到了但 tushare daily_basic 接口延迟返回空，ingest 静默跳过，screen 阶段
  `circ_mv_lt` 引用 `CIRC_MV[0]` 列（已消失）→ `KeyError: 'CIRC_MV[0]'`，
  `rquant-daily.service` exit 1。
  - `screen/loader.py`：merge 后补全 daily_basic / daily_indicator 标准列的 `[0]`
    为 NaN（float），让依赖列的规则拿 NaN 判定为 False（该股不入选），一个数据源
    延迟不再搞崩全流程
  - `ingest.py`：daily_basic 返回空时记 WARNING（不再静默），提示 `rquant run-daily
    <date>` 重拉
  - 恢复方式：tushare 数据就绪后 `rquant run-daily <date>` 重拉即补全

### Changed

- **daily pipeline 末尾加 Pool 2 退出兜底检查**（5/25 发现，根因追溯到 PR #26）：
  原 `check_exits`（aged_out + breakdown 踢出）只在 `monitor.run_monitor` 收盘后
  调用，但 monitor 在盘中被 deploy / watchdog 等 restart 时 SIGTERM 中断会**跳过
  整个 check_exits**。5/19→5/25 期间 PR #27 #28 #29 #30 #31 多次部署 restart
  monitor，导致 aged_out 规则名义上启用但实际未执行，pool2 active 堆到 53 只。
  - 在 `run_daily_pipeline` 末尾（`_sync_pool2_watch` 之后、`_push_daily_summary`
    之前）lazy import `monitor.check_exits` 并调用一次
  - daily 17:00 是 systemd oneshot timer，必跑完整流程，作为可靠兜底
  - 重复执行无副作用：已 exited 的不在 active 池，幂等
  - monitor 收盘后那次保留（双保险）

### Added

- **Tushare token 到期 systemd 兜底提醒**（5/25 事故 follow-up）：因 `pro.user` 接口
  被 tushare 下线，没法再自动监控积分到期日，改用 systemd 一次性 timer 兜底。
  - `scripts/remind-tushare-token-renewal.sh`：调 PushDeerClient 推一条「token
    34 天后到期，去 web 端续费 / 换 token」的提醒
  - `deploy/systemd/rquant-tushare-token-reminder.{service,timer}`：
    `OnCalendar=2027-03-13 12:00:00`（周六中午，距 4/16 到期 34 天），`Persistent=true`
    保证服务器停机错过时开机补跑

### Changed

- **deploy.sh post-deploy timer 验证阈值 24h → 365d**：支持长期一次性提醒 timer
  （token 续费 / 节假日特殊提醒 等）。原 24h 阈值的初衷是检测 OnCalendar 被
  systemd 静默拒收，但拒收的真正信号是 `NEXT=n/a`（已单独检测），不是 NEXT 远期。
  放宽到 1 年仍保护原核心场景。

### Fixed

- **pre-market-check tushare 积分检查降级为 warn 不再 fail**（5/25 真实事故）：
  tushare 服务端 5/22 → 5/25 周末把内部 `user` 接口禁用，返回"请指定正确的接口名"。
  没有公开替代品（tushare 没有官方查积分 API），但 token 整体可用（trade_cal /
  daily 等业务接口仍正常）。原代码把 tushare exception 当 fail → 体检 exit 1 →
  OnFailure 每天 9:00 推一条 alert 噪音。改为 warn：检查仍可见但不阻塞，真的
  token 失效会在 daily pipeline 跑 ingest 时立即暴露并 fail。

### Changed

- **notification_log 从 DuckDB 表迁移到 JSONL 文件**（5/22 真实事故）：手动跑 push
  （命令行 / inline python / `rquant notify-test`）在盘中（monitor 持写锁）写
  `notification_log` 表撞 `IOError: Could not set lock on file ...`，3 条 channel log
  全丢，dashboard 通知历史缺失。
  - 新增 `src/rquant/notify/log.py`：append-only JSONL 写文件，OS 层 O_APPEND 短写
    原子，多进程并发无锁；提供 `append() / read_recent() / read_since()` API
  - `notify/api.py::_log_notification` 不再写 DuckDB，改调 `notify.log.append()`
  - `dashboard/app.py` 「通知通道」section 两处 `query_duckdb FROM notification_log`
    改读 JSONL（pandas groupby + 排序），移除「等流水线完成」分支（JSONL 无锁，永远能读）
  - `storage/schema.py` 移除 `NOTIFICATION_LOG_DDL` 和 `ALL_DDL` 引用；云端旧表保留
    备查不强制 drop（DuckDBStore._init_schema 不再 CREATE TABLE，旧表数据无影响）
  - `sent_at` 改用 microseconds 精度，确保同秒内多条 append 的 sort 顺序稳定

### Fixed

- **rquant-replica-sync.service OnFailure 误放 [Service] 段**：systemd 把
  `[Service]` 段的 `OnFailure=` 静默 ignore（journalctl 提示 `Unknown key name
  'OnFailure' in section 'Service'`），导致 sync 真崩溃时 alert 链路不触发。
  移到 `[Unit]` 段（OnFailure 本来就属于 Unit 级别）。
- **scripts/deploy.sh post-deploy timer 验证段 unbound variable**：systemd 252
  （OpenCloudOS 9 / RHEL 9）上 `systemctl show -p NextElapseUSecRealtime --value`
  返回**人类可读时间戳**字符串（"Wed 2026-05-20 11:50:00 CST"），不是微秒
  数字。原代码 `next_s=$((next_us / 1000000))` 把 "Wed" 当变量名 → `set -u`
  报 unbound variable，部署中断。改用 `date -d "${next_raw}" +%s` 解析。

### Added

- **DuckDB 只读副本 + dashboard / canvas / nl-screen 切读副本**（5/20 真实事故）：
  monitor 盘中 9:25–15:00 持写锁期间，dashboard / canvas / nl-screen 任何直连主库的
  `read_only=True` 连接都撞 `IOError: Could not set lock on file ...` —— CLAUDE.md 原
  「单写多读 → read_only 可共存」表述是误读，DuckDB 实际行为是「写者持锁期间，任何新
  open 都失败」。
  - `scripts/sync-readonly-replica.sh`：cp 主库 + WAL → 用 DuckDB read_only 验证 tmp
    可打开 → atomic mv 替换 `rquant_ro.duckdb`；验证失败 (exit 2) 保留旧副本，下个周期重试
  - `deploy/systemd/rquant-replica-sync.{service,timer}`：工作日 9:00–15:00 每 5min +
    15:10 + 17:30 + 周末早晚 backup；`SuccessExitStatus=2` 让验证失败不触发 alert
  - `config.py` 增 `duckdb_readonly_path: Path | None`，留空则从 `duckdb_path` 派生
    （同目录 `_ro.duckdb` 后缀）；新增 `duckdb_readonly_path_resolved` property
  - `storage/duckdb.py` 增 `open_readonly_store()` / `open_readonly_connection()` 两个
    helper：优先副本，副本不存在或损坏 → 降级主库 `read_only=True`（主库也撞锁时 raise
    IOException 给 caller 渲染友好提示）
  - `dashboard/nl_canvas.py`、`dashboard/nl_screen.py`、`dashboard/app.py` 三处直连主库的
    read_only 调用全部切到 helper
  - `CLAUDE.md` DuckDB 并发约束章节重写，修正认知偏差并给出新的正确写法

- **Pool 2 aged-out 自动踢出阈值**：入池超过 `POOL2_MAX_AGE_DAYS`（默认 6 个交易日）
  在收盘后由 `monitor.check_exits` 自动 `update_pool2_exit(reason='aged_out')`。
  与已有 `breakdown`（跌破止损）并列为第二条硬退出路径，原 `days >= 3` 的
  `expired_held` 早期提醒保留。
  - `config.py` 增 `pool2_max_age_days: int = 6`，`.env.example` 同步加 `POOL2_MAX_AGE_DAYS`
  - `pool2_exit` 通知 body 新增「## 自动踢出（超期）」分组，与「跌破止损」并列
  - `auto_kicked` dict 增 `kind` 字段（`"breakdown"` | `"aged_out"`），缺省视为 `breakdown`

### Changed

- **systemd OnFailure 告警从单行 → markdown 包含排查/恢复命令**：5/14 真实事故复盘
  发现 daily 17:00 失败时 alert 链路其实**成功推了 3/3** PushDeer，但 subject 太工程化
  「[D] rquant-daily.service 失败（systemd OnFailure）」用户没注意。
  改：
  - 新增 `scripts/alert-on-failure.sh`：构造 markdown body 含 unit/host/time 表格 +
    立即排查命令 + 通用恢复命令 + DuckDB 锁排查命令
  - `deploy/systemd/rquant-alert@.service` ExecStart 改为调 wrapper script
  - subject 改为「🚨 [RQ] %i 失败 — 立即排查」，emoji 让手机一眼看出严重性

### Fixed

- **canvas DuckDBStore 永久持锁导致 daily 17:00 拿 write lock 失败**（5/14 真实事故）：
  `nl_canvas.py` 用 `@st.cache_resource` 缓存 `DuckDBStore` 实例 → 用户首次访问
  canvas 触发 cache 建立 → conn 永久持锁不释放 → daily 17:00 拿不到 exclusive
  write lock fatal exit（`Conflicting lock is held in ... PID xxx`）。
  改用 lazy 模式：`_cached_diagnose` 和 `_read_latest_trade_date` 内部用
  `with DuckDBStore(read_only=True) as store:` 自己开关 conn，函数返回后 conn
  自动 close。`@st.cache_data(ttl=300)` 仍缓存 diagnostic 结果，重复 click 不重
  连。dashboard / nl-screen 跟 daily 之前就是这种共存模式，没问题。

### Added

- **Week 7.5 C-Canvas-2 — Canvas 内 add/remove pool（pool 池 ↔ canvas 解耦）**：
  - `canvas_files.canvas_membership_of(pool_name) -> list[str]`：返回引用该 pool 的
    canvas 名列表（含默认 canvas，因其自动 include 全部）
  - `canvas_files.set_canvas_pool_refs(canvas, pool_refs)`：直接 override（默认 canvas
    不可改）
  - sidebar 当前 canvas 详情下加「⚙ 管理 pool 成员」expander（仅非默认 canvas 显示）：
    `st.multiselect` 列出全部 pool，default = 当前 pool_refs，「✓ 应用」覆盖写入
  - 右侧 pool 详情顶部加「📋 在 N 个 canvas 中：默认 · canvasA · canvasB」caption


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
