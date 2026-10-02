# 因子行业与市值中性化：计算、可信任务和原型选项

## 目标、身份和分级

原型及新前端计划 v2 §4.3 C3.1/C3.2 已由 owner 授权：表达式中的 `industry_neutralize` / `size_neutralize` 可消费真实上下文；运行参数为「无 / 行业 / 行业 + 市值」。本片把已验收的市值、申万一级区间接到原流式检验、持久任务和 React 参数，不把来源存在等同于功能可用。

集成基线为干净 `cdx/20260929-factor-source-integration` 的 `40ab6a0ae72ebc4c7aaeb0240086168cbfd8a1ad`。市值和行业源码及真实 32 日来源对照已独立验收，原无中性化两池 32 日 worker 证据仍有效；不重新实现来源、归档、账本或权限系统。root 是本产品原生 orchestrator；实际可用原生 implementer 负责实现，最终一名原生独立 reviewer 集中覆盖整个候选及本 SPEC。

**分级：高风险。** 本片扩展持久任务和原请求的参数、来源摘要及恢复绑定，明确涉及已冻结的任务输入合同。沿用 `2026-10-01-factor-run-entry.md` 的权限、CSRF、Unix peer、actor、registry/ledger、原命令和幂等提交不变量；本文件仅冻结新增上下文边界。最终一次集中审查同时核对 SPEC 与实现，禁止额外逐子任务或两阶段审查；最多三轮原作者定向修复/原审查者复核。

## 单一计算口径

1. 股票池仍由实际日成员决定；计算日的行情、行业和总市值都读取既有日历认定的**前一交易日**。行业的严格区间投影沿原 reader，端点待确认/冲突/缺失不猜测。市值沿原 reader 的万元原值，回归使用正值的自然对数；单位常数不影响含截距的残差。
2. 表达式中的两个原算子保持原数学语义。完整横截面按各节点自己的计算日和实际成员计算，时序窗口保留节点已有结果；不能用今天的池子重算过去节点，也不能把当前行业、市值前填至历史。原 `daily_v1` 没有上下文时仍明确拒绝相关表达式；仅实际绑定必要来源的请求可用上下文算子。
3. 运行参数在表达式值计算后处理：`none` 保留原值；`industry` 按申万一级去均值，每组至少两个有效样本；`industry_size` 对行业虚拟变量和 log 总市值做**联合 OLS 残差**。采用组内中心化 y 与 log 市值、全截面共同斜率再求残差的等价计算，使结果同时正交于行业和规模。不能串接「行业去均值后的 y 对原 log 市值回归」。
4. 联合回归只用因子、行业和市值都有效的样本；先剔除不足两人的组。有效 n 必须大于独立行业数 + 1，且组内 log 市值有非零可辨方差；否则给出既有 `insufficient_samples` / `zero_variance` / `precision_limit` 等有限缺失原因，不悄悄退化成无中性化。缺上下文为 `missing_context`，原因子缺失继续保留。数值溢出沿既有有限值规则处理。
5. 同一纯计算实现供稠密算子与逐日流式路径复用；API、worker、页面不另算残差。只保留一日上下文及既有 AST 有界历史，单日最多 7,000 码、单查询最多 500 码，最大计算日和缓存预算不扩张。上下文附加输入也必须计入实际工作集预算。

## 新增资产、信任边界和不变量

- **资产**：原行情准备包/副本代/范围、已验收市值与行业模型及其内容寻址 artifact、显式运行配置和文件身份、原公开参数、被冻结的 spec/hash、实际计算与显示结果。
- **配置边界**：只有可信配置可指定经摘要验证的上下文来源引用；沿现有私有根、规范文件名、no-replace 保存、strict JSON 和文件身份复验。浏览器不能提供路径、artifact、来源、SQL、股票名单或 actor。配置与 Web 不读取 `.env`，不自动采集或回退主库。
- **配对**：行业、市值均绑定同一 `FactorPreparedStreamSource` 的 SHA、snapshot/binding、scope/content hash 和代码 commit；市值还须同 RO 代，行业实收时刻不晚于行情 asof。必要来源、字段、日期、查询范围或任一绑定缺失/不符则拒绝；不能把独立行业 API 原件说成同 RO 事务或历史 PIT。
- **可见性**：继续明确 `historical_retrospective` 及 09:25 的回顾性适配假设。原实收时刻保存在来源，日值和上下文使用前一交易日；不要伪造“当时已采集”的说明。产物/回执必须可核对上下文 SHA、边界和参数，旧 `single_snapshot_transaction` 仅指原行情来源，不能扩写为所有输入的事实。
- **任务和恢复**：`neutralization` 及表达式所需上下文都进入冻结 request/spec/输入链/产物摘要。换定义、配置、来源后，只沿同 actor 的完整原请求恢复同 job/spec；不能改成新 neutralization、重新编译到新来源或绕过原 ledger 身份。原权限名单、peer、CSRF 和 owned command gate 不扩张。
- **兼容**：已有 v1/v2 无上下文任务、配置、请求、回执和 full/display artifact 的规范字节与摘要保持。新增来源字段缺省时按明确规则省略；锁定 Pydantic 2.13.1 已实际预检 `exclude_if` 支持，但必须用旧规范 payload/hash 行为证据验证，不能只凭代码认为兼容。新增上下文 payload 要有明确有类型合同，旧 decoder 不得静默忽略或降级。
- **完成 authority**：只有全部实际计算日、原行情和成员尾部、上下文读取、公式和统计自然完成且摘要一致才成功；途中异常、停止、来源改变或回执不符不能发布 succeeded。reader/private copy/FD、日批次、iterator、journal 和 heartbeat 必须实际关闭；原子发布沿原权威，不新造成功标记。
- **默认开关**：正式配置仍默认关闭。可用性由必要来源是否实际可核验决定；缺来源的模式仍禁用并给短中文原因，不能仅硬编码把两选项放开。合法来源中的个别股票缺值由覆盖率/有效样本处理，不凭空补全。

### 聚焦失败模型与阻断范围

| 新增失败路径 | 必须保持的结果 |
| --- | --- |
| 浏览器伪造上下文/不支持 mode 或来源缺失 | 不建立或提交任务，返回既有有限拒绝/未知状态 |
| 替换 scope/source SHA、副本代、实收时刻、artifact/文件身份 | 来源准入或定向执行拒绝，没有成功产物 |
| 同命令不同 mode / actor，换配置或提交后回执丢失 | 完整原请求/身份精确绑定；恢复原 job，不重编译 |
| 当日未收盘值、周末日期映射、行业切换/端点、缺市值 | 只用实际前一交易日事实，缺值如实记录 |
| 行业与规模强相关、组不足、退化矩阵或数值极端 | 独立 OLS 对照/正交成立，或明确有限缺失，不虚假中性化 |
| 上下文异常发生在中间日/末尾，私有 reader 退出 | 无成功回执；所有自有资源释放 |
| 旧规范任务/配置/产物重新解析或执行 | 旧字节和摘要一致，原 none 路径及结果保持 |
| React 改参/刷新/切账号/换代/失联恢复 | 保存并显示原 mode 与原任务，提交不误报为完成 |

P0/P1 阻断；P2 仅可复现违反上述不变量或本片验收时阻断。审查限 diff、直接依赖/测试和本模型；其他 C3.2 诊断/功能进入既有后续计划。不重新审计已验收来源或全仓。

## 连贯实施与委派写集

1. **后台实现**：纯联合残差、来源感知 capability/逐日 context、raw adapter/runner、可信保存/配置/计划/worker、原公开 neutralization 参数及可用性。复用现有路径，必要的新上下文模块限定在 `src/rquant/factor/`。直接受影响的 ledger/PageControl/private/Web 只为新参数及摘要接线；禁止改鉴权设计、数据库 schema、旧 lease/recovery 或生产基础设施。新 CLI 仅给显式可信来源/配置装配，不扩大生产权限。相关 Python 聚焦红绿、规范旧字节/hash 证据和实际小样本 worker 一并交付。
2. **原型前端**：后台合同冻结后，原生 frontend implementer 接 OpenAPI→TS 单一类型来源、可用选项/Tip、确认与原请求恢复文案、有效样本显示。桌面与 390px、键盘、加载/错误/空态；正文不出现来源编号、服务代号或内部阶段。后台实现者不写 `web/` 或生成合同；frontend 写集与后台分离，最终组装一个候选后审查一次。
3. **最终候选**：集中独立审查以上连贯 diff、直接依赖、实际证据和本 SPEC；修复沿原作者/原 reviewer。root 完成正常精确测试清单增量及两必要门禁、进度/changelog、本地合入；不为了审查重复仍有效旧测试或全 18,985 清单执行。

## 可验证验收

- 手算不同两/三行业、规模与行业相关的日截面，对照独立 dummy-matrix OLS（测试可用 NumPy `lstsq`）及行业均值/规模正交；顺序变化结果不变。覆盖组不足、n≤groups+1、缺/非正/极端规模、退化规模和有限值精度，断言真实语义，不镜像生产步骤。
- 表达式 `industry_neutralize` / `size_neutralize` 与已有稠密语义对照；动态出入池、嵌套时序和节点原历史成员保留。原 none 字节、hash 和已验收结果证据保持；只对失效依赖跑必要旧回归。
- 实际合成来源包及成员文件 → 定义 → 原 PageControl/Unix/plan/ledger → worker → full/display → Serving/Web/React；分别完成 industry 与 industry_size，核对原 mode、job/spec、定义、ctx来源及实际数值/覆盖率。没有上下文时拒绝；提交后的来源变更和 effect 回执前失败恢复原任务的等价失败证据保持原 authority。
- 对新增实际计算，root 在最终候选上复用已取得原件，沿只读 RO 做 32 日真实 mode 对照和 worker 行为证明；新发行准备使用同一行情包配对两个上下文，不请求额外行业数据。记实际有效数、缺失、资源和清理，不能把历史回顾说成 PIT、最大负载或正式生产上线。
- 前端/API合同改变时按仓库实际门禁一次执行：OpenAPI/schema、直接 Web gate、`web check/build/verify:dist` 和浏览器 e2e。保留配置跳过及环境缺口，不计为通过；修复只重跑失效范围。最终清单正常 collection、精确 added nodeids、批准 skips 原字节不变、两 manifest gate。单个测试阶段结果不要重复累加。

## 排除和停止条件

CSI 完整历史成分、18:40 因子跟踪、C3.2 尚缺的其他统计、正式配置安装、完整最大资源/生产体验属于继续 goal 的后续范围。本片不改既有采集器、不请求实际供应方、不读凭据或真实数据给子代理、不改生产数据库或 `deploy/`、不 push/main/tag/部署/切流/停 Streamlit，不调用第三方模型。子代理只能在指定干净 cdx 树离线开发，不继续委派；写入前确认真实原生身份、角色、父任务、分支、基准和 dirty 来源。

后台准备好可冻结合同后交 root，停止扩展新功能；最终完成聚焦验证和所需门禁后提交干净候选并停写，交唯一独立 reviewer。越界、新增高风险前提或阻断修复上限到达时冻结证据并报告，不自动架构复盘或另起审查轮次。整体 goal 始终以原型全部功能和差距表整体验收收敛，本片不宣称 M3 完成。

## 后台实现证据（2026-10-02）

实际产品为 Codex desktop；原生子任务地址 `/root/factor_security_collection_impl`，父任务 `/root`，承担已委派 implementer 职责，无继续委派或第三方执行。工作树为 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-neutralization`，分支 `cdx/20260929-factor-neutralization`，启动基准 `b5c657cf656dcd936c87a0350fa9067ef365be9c`；启动状态和本片修改来源已核对，未混入未知改动。身份、最终文件清单、源码/测试 SHA 和每个实际命令的结果集中保存在自有证据根 `/private/tmp/rquant-neutralization-implementation-gptxxerj`。

### 实现和稳定接口

- `time_series.neutralize_factor_cells` 为唯一纯计算核；原行业/规模 DSL 调用原语义，联合模式采用组内中心化的共同斜率。共同样本、组大小、秩/方差和缩放丢失精度显式拒绝；原缺失原因保留。
- `bind_factor_neutralization_context(prepared, *, industry=None, market_cap=None)` 返回有类型完整配对包；`open_factor_neutralization_context(context, *, lake_root)` 返回私有 lease，`query(trade_date, panel_date, stock_codes, assumed_visible_at)` 按 500 码拆分并保留原 raw fact/status。输入日摘要通过原模型的显式非有限数值字符串编码保留合法坏值，坏值仅退出共同样本。行业来源仍明确独立 API 回顾边界，日可见时间仍是回顾适配假设。
- 可信 `save_factor_neutralization_context(root, context)` 返回内容寻址 `FactorRunFileReference`；配置新增可选 `neutralization_context`。原 `compile_factor_run_plan` 和 `run_configured_factor_worker` 签名不变。冻结 spec 中 `adapter_request.context` 保留完整包，`formula.sources.context`、完成回执和 display 保留紧凑配对摘要；source 的代码 commit 和执行代码 revision 各自记录，不假设二者相同。
- 显式离线 CLI 为 `python -m rquant.factor.run_entry seal-context --root … --prepared-source … --industry-source … --market-cap-source … --lake-root … [--reference <原配置引用JSON>]`；输出有类型 `context_reference/configuration_reference`，引用原配置时保留其开关、权限名单和权威标识，只装配该上下文。
- public parameters 为默认 `none` 的 `none/industry/industry_size`；availability 新增可选 `neutralizations`，每项为 `neutralization/label/available/reason`。浏览器不能声明来源、路径、actor 或 capability。public 稳定合同先行提交为 `84cc63a269565b20fe5af5783ca1b75996a1afd5`、`c67754822b58abf6cd1110a642b8e289118e5645`。
- 实际发现 `draft.py` 固定 daily gate 会阻断 DSL 保存；经 root 明确确认，该直接依赖沿可信 backend/private capability 接线。原保存和运行名单独立，原 owned command、head、actor、registry/ledger 身份与恢复权威不变。无来源返回有限拒绝，不建立任务；原 effect 丢失后换配置仍恢复原 spec/job/mode。
- journal 沿原 `full.journal.days[].artifact.filename` 保存 `FactorDailyStreamBatch.factor_values`。Serving 和 Web 从冻结产物导出 `research.neutralization/neutralization_label/context_basis_label/context_note`，覆盖率沿原 counts；通用 `missing_context` 不伪称分开统计行业和市值。

### 实际验证与去重

环境为只读借用的 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration/.venv/bin/python`：Python 3.13.12、Pydantic 2.13.1、DuckDB 1.5.2、pytest 9.0.3。每个命令使用清洁隔离 env、`RQUANT_DISABLE_DOTENV=1`、本树 `PYTHONPATH=src:.`、禁写 pyc、dummy token 和自有离线目录；无网络/供应方 HTTP、真实来源、凭据、生产访问或安装。

必要实际红测包含：初始数学/public 缺功能；新来源模块缺失（2 failed/1.37s）；修正夹具后的旧公式基线（5 failed/1.10s）；可信配置/任务入口（7 failed/1.62s）；DSL trusted draft（两个实际失败，另一个首次为合成目录 gid 问题）；合法非有限 cap 摘要（1 failed/1.27s）；request/completion 配对（2 failed/2.45s）；缩放丢失非零因子（1 failed/1.02s）。所有原 log/XML 保留，早期无效 catalog/hash/window/目录夹具失败未冒充产品拒绝证据；修正后的基线公式红测为实际离线重放，未伪称其早于最初候选代码。

| 有效实际结果 | 实际范围与时长 |
| --- | --- |
| 数学初绿 | 5 新 + 8 原稠密节点，13 passed/1.18s；新增精度节点随后通过（`green-precision-worker.xml` 中 6 个数学节点通过，命令中 Web 夹具两节点失败如实保留） |
| public / DTO | 5 passed/1.13s；默认 none/public 原字节与 hash 有固定断言 |
| formula/context | 最终配对命令中 8 个公式、3 个来源和 1 个完成绑定节点通过；同命令仅 CLI 输入文件权限夹具失败，随后修正为私有文件并实际通过 |
| 任务、恢复和生命周期 | 9 passed/6.59s（7 新任务 + 2 来源）；真实中途异常、末尾同字节替换无成功产物，配置改变/回执丢失保留原任务 |
| private DSL 保存→运行 | 3 passed/3.46s；原保存/运行名单独立 |
| worker→Serving→Web/独立 OLS | 2 passed/5.47s；两个模式读取实际合成 journal 日值，与独立 NumPy dummy-matrix OLS 比较，API 返回原模式及中文标签 |
| 最终日对象释放/绑定 | 3 passed/5.74s；前一日对象在下一日 observations 前释放，结束后全部 weakref 为空 |
| 最终格式后直接 CLI/private DSL | 2 passed/4.36s，子进程回收、Unix 两服务线程 join/socket 清理 |
| 原流式直接回归 | 43 passed/2.21s：none 算子黄金、动态成员、尾部、默认 context 拒绝、SSE/收益成熟日和原摘要链 |
| 原可信入口直接回归 | 15 passed/8.29s：配置/CLI/取消、原 v1 固定摘要、独立 journal 重放和权限/恢复 |

`new-nodeids.txt` 为实际 collection 的 **34 个精确新节点**；`old-passed-nodeids.txt` 为 **66 个去重旧节点**（8 原稠密 + 43 流式 + 15 入口，含一处旧 node 的必要新 mode 拒绝断言调整）。100 个不同节点有实际 passing JUnit；不把分命令重复通过、failed 命令中的失败、skip/deselect 或 collection 算为新增通过。最终 formatter/import 排序和等价测试格式改动复用有效行为证据，不再次运行全套；`test-command-results.json` 明确记录每份失败和通过及每个新节点的有效 JUnit 来源。

启动前保存的七份旧 none payload（spec/formula request/config/completion/display/full/public run request）在新模型逐字节重放、原 SHA 均一致；旧 full 重投影的 display/completion 也一致（`legacy-replay.json`）。原 v1 spec 固定 SHA `3cc03cd3b8940db3431e5e1e4d53725602642d812e63766d4b25bcebc3dc233e` 的旧回归通过。Ruff/check/format/diff 均通过，27 个修改 Python 文件以 `ast.parse(feature_version=(3,11))` 实际解析；未声称使用 Python 3.11 runtime 执行。

### 资源、范围与交接

全部自有 exec/pytest/CLI 命令已退出；私有 reader、执行副本、日对象、journal 和 Unix 服务线程由实际断言确认释放。早期两个 gid 夹具失败所留空 `fneu-*` 目录按创建时间与对应 JUnit、dev/inode/owner 明确归属后移除。清洁 env 的默认 /private/tmp 父目录被原 proxy-proof 私有父目录检查正确拒绝；成功 Web 证据使用本树自有临时私有 proof 目录，finally 删除，未改鉴权或共享权限。仅自有合成旧规范原件与 log/XML 在证据根留存；无借用 runtime 改动。

本片改动严格为必要后台/直接可信与 Web 投影、Python 测试和此附录；未改已验收采集器、ledger/schema、前端、生成合同、测试清单、依赖、生产或部署。后台候选交 root 后停写：真实两池 32 日、Frontend 组装、统一生成合同/必要门禁和唯一集中独立审查仍由 root 完成；这里不宣称整个 M3、PIT、最大负载或生产上线。

## 根任务最终验收（2026-10-02）

本地合入 `f3065f668f83bdc5342acbc8aea6f263a34ec28c`，最终受审候选 `b1eee49bb2a247d8cb7015106599e92203fed461`，合入时树内容完全一致。原生独立审查一次集中覆盖本片冻结模型、diff、直接依赖、测试和验收，accept、无finding、零修复轮。报告 `/private/tmp/rquant-neutralization-final-review-no1g4vu7/review.md`，SHA256 `aed161a5752769e94e00b31776cce3aef7f2e2c26c3a144febde9804993ff9b9`。此前FHA额外补修授权没有用于本片。

后台34新 + 66旧共100个不同节点有效；前端576、Web API987通过，浏览器119收集/115通过/4原配置跳过。最终6项桌面/390px确认截图重叠，不累加；root实际查看确认及恢复截图。TypeScript、Biome、构建和已提交dist校验通过，首屏323.1KB低于550KB门限。缺私有TMPDIR的API失败、磁盘不足中断、未稳定动画截图、root首次清单缺跳过文件的错误均保留，未计为通过。有效命令、日志、XML及截图见 `/private/tmp/rquant-factor-neutralization-final-20261002-ufRVRHOk`；后台与前端聚焦证据分别见 `/private/tmp/rquant-neutralization-implementation-gptxxerj` 与 `/private/tmp/rquant-factor-neutralization-react-20261002-SpNDUN5e`。本地实际Python3.13.12，3.11为语法证据；真实诊断用云端Python3.14.4，没有新增3.11/3.12 runtime或CI结果。

root在实际任务前重新只读捕获原始收盘价/市值，5.487s，旧代原件保留于 `previous-reference-20261002T051137861079Z`。新JSONL SHA256 `96887a24d92d893ff532f4c2718a299d2fd6f4203143b399a85e7e2bee3f4c5e`；独立NumPy行业哑变量矩阵 + 对数市值OLS没有调用产品中性化核。两组均为08-14—09-29的32计算日/32评价日、代码并集5,571，按前一实际SSE交易日取上下文，journal日期、代码、空值原因和数值逐项比较：

| 实际任务 | worker秒 | 比较因子值 | 有效值 | 缺失分类 | 最大绝对误差 |
|---|---:|---:|---:|---|---:|
| 全市场 / 行业 | 250.173 | 160,462 | 160,251 | 205缺观测、6缺上下文 | 4.547e-13 |
| 创业科创 / 行业 + 市值 | 189.319 | 64,685 | 64,526 | 89缺观测、64样本不足、6缺上下文 | 4.775e-12 |

两份配置、原任务、completion及32日展示覆盖均核验成功，共225,147比较、224,777有效、370明确缺失。总诊断521.080s、峰值RSS481.359MiB包含准备、导入和独立验证，不代表单个worker峰值或最大区间负载。原件/市值在准备配对时核对同代；运行后live RO确实自动刷新，成功依据是封存原件及worker完整绑定，不声称live RO全程未变。行业API区间原件沿自己的实际采集边界，语义保持historical_retrospective，未证明PIT或全部来源同一RO事务。本次供应方数据HTTP为0，没有写生产库或发布正式结果。

固定清单19,019/55，SHA256 `855197adec2aa611fea6b358f83ac5d071905c4ff65b2cc18e36a258bededc0e`；精确增34、无旧删除/重复，批准跳过原字节和SHA `1367a714636bb473ff37edd8af1928d460d2b95f84f3c2658b6a4004cbb1b813`不变。正常collection成功，两个必要门禁2 passed/8.591s，没有执行全部19,019节点。原none七类规范payload/摘要及旧full重投影逐字节兼容。

root完整证据在 `/private/tmp/rquant-factor-neutralization-root-2wcyf7nd`，含冻结67文件SHA、接受记录、原始引用、独立黄金、`worker-response.json`、`actual-worker-evidence.json`、清单和门禁。worker回复SHA256 `6f2ba085d3d1483cbbaa2d6f9e8a40278004f7411e3bdee7a53605dd0b2e8626`。自有命令均完成；源/湖句柄0、心跳线程/私有副本为空，自有远端 `/tmp/rquant-factor-neutralization-proof-20261002-b096036a6e46` 已删除。测试服务器退出；临时node_modules指针精确移除，原依赖目录保留；未知来源目录及工作树未清理。

收尾文档属于小改动：仅登记已有事实和后续范围，不改变接口、数据或行为，由root自检，不叠加审查或重跑有效测试。主工作树原HEAD/三份已知tracked修改保持；未push/main/tag/部署/切流或停Streamlit。M3仍部分；18:40跟踪、CSI完整历史、其他尚缺C3.2统计、正式配置/数据代和完整资源/生产体验继续实施。
