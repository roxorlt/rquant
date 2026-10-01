# 历史股票名单采集 Implementation Plan

> 执行：Codex 原生 implementer；按仓库 AGENTS.md 执行与一次最终独立审查。

**Goal:** 将实际历史股票名单及上市资料采集、归一化为已有成员归档可消费的逐日事实，使全市场和创业板/科创板检验使用可核验的真实来源。

**Architecture:** 采集器只读已选 Tushare API，按响应记录实际接收时间并保存来源事实；纯归一化函数交叉核对上市区间、证券市场和历史名称，输出现有 DailySecurityBatch。数据不足的日期保留明确拒绝原因，不以行情代码合集、当前名称或数量猜测补齐。现有成员归档、运行计划、worker 和 Serving 接线复用。

**Tech Stack:** Python 3.11+、Pydantic、现有 TushareAdapter、现有因子成员归档；不改网页 API 与 React 产物。

---

## 范围与验收边界

- **普通任务**：新增受限的离线采集及纯事实归一化，只写指定私有来源目录；不改权限、任务账本、主库、生产契约或服务部署。
- 产品：Codex 桌面；根任务 `/root` 为 orchestrator，原生子代理为 implementer；工作树 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-history-source`，分支 `cdx/20261001-factor-history-source`，基准 `b11cc5e0c70796bc329e4bffaf178d5750a61ee6`，新建时干净。本计划是根任务唯一已知初始改动。
- 子代理只在此树及其自有临时目录开发/测试，默认离线，禁用 dotenv，不访问凭据、网络、生产环境，也不继续委派。真实来源调用与捕获由根任务通过已有应用配置进行，密钥不打印、不归档。
- 写集：新增 `src/rquant/factor/security_collect.py` 及必要的同域文件、聚焦测试、本计划；现有 `src/rquant/adapter/tushare.py` 只新增所需原始请求方法，不改旧方法语义。如需超出此写集，先向根任务说明具体证据。
- 排除：CSI 历史成分、市值/行业中性化、每日跟踪、用户界面改动、生产发布/写库/切流/停服。此片通过不表示 M3 或整体 goal 完成。

## 实际证据与来源规则

官方文档：历史逐日列表 [bak_basic](https://tushare.pro/document/2?doc_id=262)（2016 起、7,000 上限）；上市资料 [stock_basic](https://tushare.pro/document/2?doc_id=25)（6,000 上限、L/D/P/G/UN、交易所与市场、上市/退市日期）。只用官方语义，受测范围内到上限即拒绝可能截断。

根任务 2026-10-01 实际采集的来源档案：
- `/private/tmp/rquant-factor-history-probe-oy028cqm/`：09-30 bak_basic，5,577 行、无重复代码/空单元格，实际观察时间 `2026-10-01T06:10:54.871319+00:00`。
- `/private/tmp/rquant-factor-history-reference-vm2b4a1a/`：stock_basic 全交易所 L=5,572、D=339、P=0；09-29/09-28 bak_basic 各 5,578 行。读取它们不需要也不允许读取凭据。
- 09-30 bak_basic 相对已经上市的当前资料漏 `301139.SZ`，并包含六个上市日期 `0` 的未上市代码。09-29/09-28 返回的即将上市记录 `list_date=0` 与后续明确上市日期不同。D 分区有 `T600018.SH`（2006 退市、无市场）；不能把这种范围外资料当作当前有效证券，也不能无说明地跳过范围内未知资料。
- 需要实际核对 G/UN 分区再决定如何处理未上市记录；未获得参考事实的代码、未知市场或无法解释的日期冲突不得转成完整事实。
- 来源明确为 `historical_retrospective`，`observed_at` 为所有使用响应的最大实际接收时间；不能设为历史交易日收盘，不能宣称 PIT。
- 历史名称用于当日 ST 判断，复用已有 normalize_name 规则；当前 stock_basic 名称不用于历史 ST。上市区间取明确上市/退市日期，退市日排除；上市前记录不进入名单，未知上市日不能默认为已上市。市场用提供方明确 market/exchange，未知保持未知并按原池合同拒绝。
- 用 L/D/P/G/UN 和交易所分区明确捕获覆盖，不因单张表行数不大就宣称完整。跨分区重复/日期矛盾、响应缺列/错日/触顶/异常均拒绝该日期。合法空分区与失败响应区分；实际空交易日列表不能生成完整有效市场。
- 已上市证券缺历史名称时保留 `is_st=None` 和明确诊断，不能删除这只股票或用当前名称补 ST。是否拒绝由已有股票池实际需要的事实决定：`all` 剔除 ST，候选 ST 未知必须拒绝；`gem` 只依赖上市、交易所和板块，不增加未经原型要求的 ST 筛选。未确定枚举覆盖/上市事实仍不能发布；未知板块依旧由原池合同裁决。

## 实施步骤

### 1. 原始取数与捕获

文件：`src/rquant/adapter/tushare.py`、新增 `src/rquant/factor/security_collect.py`、相应聚焦测试。

先写并实际运行失败用例：请求字段/交易所/状态/date 明确；坏/缺列响应与触顶不能成功；未配置的 import 不初始化 Settings 或网络。新增薄方法走现有 backoff/transport observer，不切换未经验证的备用权限。

采集请求显式限制日期、目标私有目录及调用预算；响应采用 Pydantic 类型化，保留实际请求、字段/原始值、接收时刻和实算摘要，来源原件不混入密钥。按日处理，最多保留一日及一次参考资料，不积累整个区间的 DataFrame。保存中断不得伪装完整目录；不覆盖已存在的来源档案。提供可直接运行的显式 live 采集和离线捕获回放入口，Web import 不带入配置/联网客户端。

### 2. 交叉核对与逐日事实

以范围内日历/日期和真实参考资料核定预期上市集合。先写红测，覆盖上市前/上市首日/退市日、主板/创业板/科创板/北交所、历史 ST 名称、非 ST、真实 `0` 未上市记录、历史资料缺失/重复/冲突、范围外旧退市异常代码。

可归一化日期输出已有 `DailySecurityBatch`，完整 manifest 与 facts 一致，source SHA 绑定实际捕获内容与所用参考响应。逐字段保留未知，按上一节的实际池依赖验收；不能通过默认 False、按行情代码取名单或忽略缺股票让检验通过。错误使用短中文原因，详细来源诊断留在命令回执中。

### 3. 接入已有归档及实际回放

测试将采集输出写成 `FactorMemberDayInput` 并交给 `publish_factor_member_archive` / 现有 `FactorMemberStream` 消费，验证全市场与创业板+科创板选择、归档摘要/实际观察时间、拒绝日不出现可用 manifest。指数选项无完整逐日成分时拒绝，不 forward fill 月度权重。

根任务用新入口读取真实捕获，再在许可的 API 上对最新可用日期执行一次实际采集/归档；09-30 真实遗漏使 `all` 必须拒绝，`gem` 仅在其上市/板块必需事实齐全时可准入。对 `all` 倒序寻找最近满足条件日期并记录回退天数。存在可用日期则形成实际成员档案和选择结果；后续原数据/worker 验证只复用已有正式合同，不另建计算器。

### 4. 验收与冻结

implementer 报告准确命令、Python 环境、红测→绿测、直接依赖回归、实际文件写入边界和所有自有进程处置。Ruff/format/diff 自检；不因阶段变化运行全仓测试或重编前端。

最终候选记录准确 commit/diff 边界；一名独立 reviewer 一次性审查此 diff/直接依赖/验收标准。普通任务至多一轮原作者集中修复与同审查者定向复核，范围外问题进 backlog。根任务合入干净本地集成、更新来源和进度文档，并在新增 pytest 用例时按既有清单收集/合同门禁更新库存；不复跑全 18,817 项。

## 实施与验收附录（2026-10-01）

### 已核定的股票池依赖

根任务结合实际 `select_factor_universe` 合同修正了前文「缺历史名称整日拒绝」的范围：
完整 `stock_basic` 分区可以证明已上市集合，历史名称缺失时保留 `is_st=None`，由既有池合同决定可用性。
`all` 对有效非北交所候选要求 ST 已知；`gem` 仅要求上市、交易所和非北交所板块，不筛 ST。
未知板块同样按既有合同处理，不改 `universe.py`。未知证券、覆盖缺口、上市或日期冲突仍拒绝全日。

### 交付入口与来源

- `src/rquant/factor/security_collect.py` 提供 `live`、`import-probes`、`replay`、`archive` 四个可运行入口。
  示例：`python -m rquant.factor.security_collect replay --capture-root <绝对来源目录> --selection all`。
- `live` 需要显式日期、新建绝对私有目录和实际调用预算；默认覆盖三个交易所的 L/D/P/G/UN，共十五次参考请求。
  每次 transport dispatch（含重试）计入预算；可显式用 `--all-exchanges` 请求五个全交易所分区。
  无网络/配置副作用的普通 import 与显式 live 入口分离，live 使用主 token，不切备用。
- 来源原件保留 fields/items 原始值、实际请求、实际接收时刻、真实 UTF-8 原件 SHA256。
  完成回执只记录捕获操作完成，不证明市场覆盖；归一化核对覆盖后才产生可归档事实。
  中断保留既有原件与单独中断回执，未完成目录不能回放为完成；已有目标目录不会被覆盖。
- `import-probes` 核对元数据摘要、行数、实际参数及接收时间；忽略旧探针的 `listing_facts` 诊断（含 NaN），
  只使用标准 null 的原始响应，额外过滤参数不得被丢弃后冒充全市场请求。
- `archive` 复用 `FactorMemberDayInput`、现有 publisher 和 reader；所有请求日期先核对，再发布 manifest。
  无完整逐日成分的指数池明确拒绝，未使用任何月度 forward fill。

### 实际离线行为证据

读取根任务的三个原件目录，未联网、未读取 `.env`、未访问主库或生产环境。
额外 G/UN 来源：`/private/tmp/rquant-factor-history-unlisted-k6pmm3ys/`，G=0、UN=8。
其资料明确解释六个未上市代码，真实最大接收时刻为 `2026-10-01T06:21:08.089999+00:00`。

实现者自有证据目录：`/private/tmp/rquant-security-implementation-didrm_qf/`。

| 日期 | 已上市事实数 | all | gem |
|---|---:|---|---|
| 2026-09-30 | 5572 | 拒绝，301139.SZ 历史 ST 未知 | 可用，保留 is_st=None |
| 2026-09-29 | 5571 | 可用 | 可用 |
| 2026-09-28 | 5569 | 可用 | 可用 |

从最新 09-30 倒序寻找全市场可用日期，回退一天到 09-29。
真实 CLI 成员归档与现有 reader 已验证：09-29 all 选中 5024；09-30 gem 选中 2027。
二者均自然读取至尾部完成、真实 observed_at 保持上述最大接收时刻。
09-30 all 的 archive 返回 exit 2，目标 archive 目录未创建，没有可用 manifest。
回放与归档回执保存在证据目录中的 `cli-replay-*.json`、`cli-archive-*.json`；原件、成员档案保留供根任务复核。

### 聚焦验证及边界

- Python：`/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration/.venv/bin/python`，3.13.12；
  `PYTHONPATH` 指向本工作树 `src`，`RQUANT_DISABLE_DOTENV=1`，配置完全临时离线，token 是明确合成的测试字符串。
  变更源码的 Python 3.11 语法解析通过；本机未声称已执行 3.11/3.12 runtime 或 CI。
- TDD：新增入口前 27 项实际失败；池合同修正时缺 selection 参数的两项实际失败；
  探针过滤参数与 live 配置异常输出均观察到聚焦红测后修复，最终新增库存为 28 项（没有额外追加矩阵）。
- 聚焦命令：`python -m pytest tests/unit/test_factor_security_collect.py tests/unit/test_factor_universe.py tests/unit/test_factor_member_archive.py tests/unit/test_factor_member_stream.py tests/unit/test_historical_security_status.py tests/unit/test_formula_market_universe.py tests/unit/test_source_quota_transport.py -q --tb=short`。
  287 passed，0 skipped/0 deselected，10.96s；其中新 28、直接旧依赖 259，不重复计数。
  日志 `focused.log`、JUnit `focused.xml`。最终只变更 live 错误回执后，新 28 项再次通过（3.15s），
  日志 `new-final.log`、JUnit `new-final.xml`；旧回归不受此异常文案分支影响，复用有效结果。
- Ruff check、format check、`git diff --check` 通过；没有全仓测试、前端编译或清单生成。
- 实现者所有 pytest/CLI 子进程均已退出，所有测试临时目录由 pytest 清理；上述自有证据目录明确保留。
  真实 live 调用、正式原数据/worker 验证和一次最终独立审查由根任务继续完成，本片不宣称整体目标已达成。
