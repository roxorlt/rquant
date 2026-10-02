# 因子全市场技术指标历史来源

## 范围与角色

按CC原型/frontend-plan-v2 §4.3及已接受后续顺序，补齐12项技术指标的全市场研究来源、价格口径和递归初始化。前片库存16字段及持续跟踪已本地验收，候选9500818f、文档b69ebdd4；不重复实现。只读预检发现日线7,338,351行/5,789码，库存指标仅24,421行/280码。日线始于2020-08-24，复权因子始于2024-09-02；可用来源历史不等于上市完整历史或PIT。

普通任务：在既有研究来源包/字段目录/检验适配路径增加显式、有界、只读的技术推导模式；不改鉴权、原账本、幂等、租约、恢复、并发提交、生产写入或已冻结生产合同。若实际必须触及这些边界，停止相关写入并向root报告证据，再决定范围。一次最终独立集中审查，最多一次原作者定向修复/原reviewer复核，不叠加子任务审查或全仓审计。

实际Codex desktop原生 `/root` 为orchestrator，父任务是已授权React整体验收goal。复用唯一实现树 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-tracking-react`，分支 `cdx/20261003-factor-technical-source`，基线 `b69ebdd4750f0f0e6c8fae675fa29cf08d78ca63`。前片作者/reviewer/真实worker均已停止且树clean；无新增worktree，原分支保留。仅root可委派。子任务仅访问指定树、自有private tmp及已知只读依赖；禁止网络、.env、凭据、真实资料、生产、第三方模型、继续委派或自行新建树。

## 冻结口径

1. 复用 `indicator.technical.compute_indicators`，当前领域实现与依赖ta0.11.0保持。MA5/10/20/60需完整窗口；RSI6/14为首观察涨跌0、alpha=1/n、adjust=False、min_periods=n、无跌幅时100。MACD EMA12/26从首价格播种、adjust=False、完整窗口可见；signal9从首个有效DIF播种，hist=DIF−DEA、不乘2。KDJ窗口9/min_periods1，K/D初50、alpha1/3，价差0沿旧K/D，J=3K−2D不裁剪。任何后续算法变更须有新版本，不能悄然解释旧结果。
2. 每码按升序读取截至输出末日的全部实际可配对历史，不以检验start_date重新播种，不使用末日/未来factor归一化历史。历史high/low/close乘各观察日正有限adj_factor，价格类MA/DIF/DEA/hist在每个输出日除该日factor；RSI/KDJ为点值。派生中非有限不伪装为有效；原high/low/close须正有限且满足low<=close<=high。复用领域计算，不另造指标框架。
3. 初始化从源历史第一条有效OHLC+factor配对开始，记录原始历史起点、每码首有效日与不足/失败原因，不能声称上市全历史。前导未配对观察如实计入初始化边界；播种后任何实际存在的必要观察无效或缺factor会断开该码的可靠后缀，不能跳过、填0、静默重新播种。停牌等无日线观察不凭空补K线。窗口不足、无可初始化历史和中段断裂须有typed来源/覆盖证据，网页可理解其原因。
4. 增加显式typed推导版本/模式，12技术值声明history-derived、价格基准与初始化政策；4项daily_basic仍为供应方库存原值/原单位。既有stored_not_recomputed包、旧六字段/spec/config默认序列化及旧prefix黄金保持。优先沿现有daily_feature_source接口做有版本的增量，不复制并行reader/worker/恢复框架；新模式不能冒充库存原件。实际派生算法/来源输入/起点与输出工件均须绑定，使用同代真实行情准备范围及同一个固定只读事务，封存可核验的输入/输出来源。
5. 来源准备以<=500码批次、单码有界历史帧完成，不全市场宽DataFrame；沿7000代码/4096输出日期的现有范围，历史输入最多16,000,000行、单码最多50,000观察，输出格数显式预检。超限在发布前明确拒绝。表缺失/重复业务键/源代变化拒绝；输出只含原范围日期/代码，行数/有限/缺失与封存数据相符。原来源只读、派生输出仅私有lake，无任何生产回补/UPDATE/INSERT。
6. 显式离线CLI可准备并绑定新模式到原配置；默认库存行为及旧引用字节不变。当前可信能力目录由实际已核验模式提供字段说明，浏览器不能提供路径或自称口径。定义运行、journal重放、结果来源与覆盖、历史结果和已接通跟踪复用实际来源；新派生依赖的prefix须包含实际逻辑值/缺因和必要算法/初始化政策，排除来源代/路径/receipt哈希。未消费派生字段不污染旧prefix；历史变动仍沿原冲突暂停，不改原提交或恢复。
7. 页面沿原型布局和短中文规范，公式仍用原字段名；描述与结果Tip按各自实际来源展示价格与初始化口径，不能把派生结果说成库存、把旧库存说成已核验。窗口不足/历史断裂保留未知和断点。键盘/390px/刷新/换代后历史结果来源仍独立，内部版本号/路径/技术状态不进正文。没有可信来源时不开放本模式。

## 写集与顺序

backend implementer先在本树：新增技术历史推导模块；daily_feature_source.py、capability.py、run_configuration.py、run_entry.py、run_plan.py、run_backend.py、formula_stream.py、stream_adapter.py、stream_job_artifact.py、tracking_backend.py、tracking_runner.py及直接相关typed依赖按实际必要修改；已有技术生产/backfill模块只读复用，不改。直接新增/受影响Python测试与本计划实施附录可写；不写web生成物、测试清单、依赖、生产或真实资料。超出上述直接依赖先报告实际原因，不自扩范围。

backend冻结后frontend同树顺序接实际可信目录/来源与覆盖、测试、规范OpenAPI/TS/dist。root最后精确收集节点及两必要门禁，冻结唯一候选，交唯一独立reviewer；ACCEPT后root进行独立真实只读验收，再本地合入并同步进度文档。所有实现者记录实际原生身份/父root/分支/基线/导入来源/冻结字节；停止时交接clean提交和资源处置。

## 可验证验收

- 独立手算/递归参考核对所有12字段，长历史、初始值、窗口边界、平盘、除权变化、每日期价格归一、未裁J及DIF−DEA；禁止仅调用相同compute_indicators生成“预期”。首有效日固定，短检验切片等于长检验相同后缀；追加新日不改变旧日，前导缺因/中段坏值/无观察/停牌有实际状态及边界证据。
- 原库存v1封存/默认CLI/config/source/spec的字节黄金、旧六字段prefix与直接旧检验/跟踪回归仍有效；新模式原配置→worker→journal独立重放→展示产物及实际能力可运行。混合派生/库存表达式和纯旧字段，开始/取消/原恢复均按原流程；同逻辑换代不暂停，已消费历史值/状态或政策变化暂停。
- 固定同代/单事务、来源全部scope代码（含无有效历史）与缺因、重复/坏数据/超限/换代拒绝、500码查询/单码帧和resource关闭有聚焦行为证明。合成fixture只写自有private tmp，真实预检和原件不提供给子任务。
- frontend正常类型/check/API/build/size/dist及桌面/390px浏览器相关门禁一次；复用未失效旧结果。最终清单只精确收集并跑两必要门禁，不执行全清单；独立审查一次覆盖当前diff、直接依赖和以上验收，不做无边界attack sweep。
- 唯一ACCEPT后root固定RO，独立原始OHLC/adj参考核对实际全scope派生来源及至少同原真实成员的3日表达式worker，验证区间/追加一致、真实覆盖/缺因、耗时/RSS/句柄/线程与自有临时资料清理。不声称PIT、上市全历史、最大负载或生产上线。

## 后续

完成本片后继续已有选股日线特征、分钟/竞价/温度/VP、CSI完整历史、正式配置/数据代/18:40及生产UI验收。M3仍部分，整体goal active；生产发布、切流、停Streamlit与基础设施/生产数据写入按已有单独授权规则。

## Backend 实施证据（2026-10-03）

- 实际执行者：Codex desktop 原生 `/root/factor_security_collection_impl`，implementer，父 `/root`；本树 `cdx/20261003-factor-technical-source` 从 clean `70996889b17fe6e22766360e2336f4c7d9df19e4` 开始，全部新增改动自有。原37行 SHA256 仍为 `e55ad01b6fe6a606de74034fcb1eba207d0388383648a67c8c83e6fd35a98a75`。
- 新 `technical_history_source.py` 及原 daily-feature 接口的 v2 分支封存原始输入、12项派生输出和4项库存基本事实；保存引用为 `factor-daily-feature-source-v2`，显式 CLI 为 `seal-technical-history`。v1/stored/default/未消费新字段均保留原路径。直接接线实际修改 capability、formula_stream、run_backend、run_configuration、run_entry、stream_job_artifact、tracking_runner；原计算核、stream_adapter/transaction/ledger/lease/recovery/auth 无需修改。
- 同一个固定 RO 事务内每<=500码批次复制必要输入到私有 TEMP 表，单码帧调用原核，NaN仅在全输出结束后按实际需要的列各修复至多一次；全部代码保留播种/断裂元数据。私有reader用封存输入核验初始化回执；journal重放/原件身份witness同时包含输入工件。实际派生prefix只含逻辑值/状态/缺因、政策与panel当时已可知的播种/断裂，未来首次有效及未来断点不改变旧日；原 `_reserve`/`_commit` 完全未动。
- 证据根 `/private/tmp/rquant-technical-history-implementation-7vlnxwto`：`identity.json`、逐命令 `.json/.log/.xml` 保留 argv/UTC/wall/exit 和所有失败原件。`source-red.xml` 实际2 failed；`source-audit-cap-red.xml` 实际2 failed/3 passed；`pipeline-fixture-corrected-red.xml` 实际1 failed/1 passed；`seed-binding-red.xml` 实际1 failed。夹具权限/序列化与格式检查的失败亦保留，未称通过。
- 有效去重23新节点=`final-focused-green.xml` 22 passed/25.61s（wall26.064690s）+`derived-nonfinite.xml` 1 passed/1.41s（wall1.718518s）；后者直接核验批次NaN修复相关极值分支。15旧节点=`direct-legacy-regression.xml` 15 passed/8.47s（wall8.923178s），包含原库存来源/默认spec及draft/CLI/旧六字段prefix固定黄金。`new-nodeids.txt`/`old-nodeids.txt` 无重叠，无删除或改名；skip=0。随后产品变动仅导入排序与等价字符串换行，不重复累计原节点。
- 借用 Python3.13.12/Pydantic2.13.1/DuckDB1.5.2/ta0.11.0/pytest9.0.3，仅本树PYTHONPATH、禁dotenv/bytecode、dummy配置及自有TMPDIR；无网络/真实资料/生产。`ruff-accepted.json`/`format-accepted.json` 实际exit0，11文件Python3.11 AST与源码SHA在 `syntax-and-file-sha.json`；git diff --check实际exit0。
- `public-api.json` 是真实合成配置/产物导出的capability/display形状；`synthetic-source.json` 是完整合成typed v2来源。`resources.json`/`handoff-capture.json` 实际FD前后相等、只有MainThread、无私有reader/prepare目录；所有自有命令session已结束，无daemon/UDS。合成basetemp、工件和日志仅保留在该0700私有证据根供最终复核。
- Backend 候选冻结后停写。前端接线/规范API生成/组合门禁、唯一独立审查、真实RO及worker验收由root接续；本片不宣称真实全市场覆盖、PIT、上市全历史、最大负载或上线。


## React 前端实施与合成验证（2026-10-03）

- 前端起点 `c5619a64388d6bdf87c3ff6a4dc373a092689d42`；原后端作者的 schema 修正 `25a5ac1819e144e1ed3e15a0833200df6c13c923` 后规范生成 OpenAPI/TS。前端源码与 dist 快照 `6bc8e9704d917b9af4cc681fb18ca22c0d5b069c`；生产 UI 仅改 `FactorDailyFeatures.tsx`，逐字段来源、初始化事实及覆盖缺因来自该历史结果自身。
- 新增 9 个 Web 行为节点，首轮 8 failed / 1 passed 到最终 9 passed；首两次 check 在 TS 阶段退出，原件保留。唯一有效完整 check：60 文件 / 661 passed，exit 0；API 正常 `tests/unit/test_web_*.py` 门禁：993 passed，exit 0。前端新增 Python 节点 0；后端节点收据由 root 单独接收。
- 首次完整浏览器：122 passed / 3 failed / 4 原配置 skipped，exit 1。精确修正新两节点的中文 Tip 定位及关闭等待后 2 passed；旧数据审计 390px 原节点不改代码的一次复现 1 passed。唯一有效通过共 125，4 skipped 不计通过；原失败 XML/log/trace 与 409 请求/响应时间序保留。
- 1440×900 / 390×844 的键盘、触摸说明、历史来源与当前能力独立及换代恢复均有合成浏览器证据。仅为截图等待 Tip opacity=1 后补两节点 2 passed，未重复累计；最终 4 张 PNG 已实际查看。
- Node 22.22.2 / pnpm 10.33.0 / Python 3.13.12 / Pydantic 2.13.1 / DuckDB 1.5.2。build、size、正常快照提交后的 verify:dist 均 exit 0；首屏 gzip 323.2 KB / 上限 550 KB，按需合计 437.7 KB。
- 完整 argv、UTC、wall、exit、XML、节点、生成合同与截图 SHA、资源收据：`/private/tmp/rquant-technical-source-react-2jfu0_dg`。本片仅合成验证，真实来源验收与唯一联合审查由 root 接续。自有端口 18825 / 14225 已随服务器退出；API basetemp 与 browser-serving 保留给最终审查，句柄检查无匹配，不修改保护或删除未知资料。

## 真实运行发现的局部性能问题（2026-10-03）

受审 clean 候选 `f62f762b683b43797e204ba4cc0906825b1ab4bc` 的首轮真实验收在 1801.112s 以 exit 124 超时，不能计为通过。独立原审查报告已接受代码；实际账本只读观察为一个 succeeded job、一个已提交跟踪日。中断栈位于根任务再次核验该首日结果时，daily-feature lease 退出所调用的 `verify_materialized_table_artifact -> _logical_content_hash`；同一技术历史输入在原件、私有副本和退出阶段重复逐值扫描。原日志/响应/归档/原审查及精确候选保留在 `/private/tmp/rquant-factor-tracking-root-yegakn31/technical-source-proof/attempt-1-timeout`。远端 bootstrap 回执确认自有临时目录已删除，生产只读边界不变。

稳定 finding `FTH-PERF-01`：仅对新增技术历史输入的重复完整验证做一次原作者局部修复及原审查者定向复核，仍为普通任务；不修改通用 research_lake/research_snapshot 哈希、v1 库存行为、公共 schema、账本/提交/恢复/鉴权或生产路径。实现前用有界合成材料测量重复逻辑扫描，确认耗时来源。

验收：首次完整验证仍覆盖原物化工件的全部元数据、逻辑摘要与初始化证据；任何验证结果复用必须绑定完整工件元数据，并在每次读取/复制/退出核对实际文件字节的加密摘要及适用的文件/root 身份，不能仅靠路径、mtime、大小或宽泛缓存。修改原件/副本或改变声明须拒绝；缓存有明确容量且不保留 reader、连接或文件句柄；旧 v1 路径保持原行为。聚焦红绿与直接旧回归后，原 reviewer 只复核本 finding 与修复回归；前端及其他未失效证据复用。真实首1日＋续2日/整3日、独立参考、重复/取消及资源验收仍必须完整走完，未完成前不合入。超出上述局部边界先停写并报告，不自动重设计。

## FTH-PERF-01 局部修复证据（2026-10-03）

- 原 implementer `/root/factor_security_collection_impl`，父 `/root`；本树 `cdx/20261003-factor-technical-source` 从 `f62f762b683b43797e204ba4cc0906825b1ab4bc` 开始，仅 root 上述附录 dirty。只改 technical_history_source、daily_feature_source、新直接测试及本附录；原37行 SHA256 保持 `e55ad01b6fe6a606de74034fcb1eba207d0388383648a67c8c83e6fd35a98a75`。
- 私有合成证据 `/private/tmp/rquant-technical-history-perf-tlbhbd1m`。同一200,000行/500码/400观察、同一文件 SHA 的六次原件/复制件验证，修前完整逻辑扫描6次、1.906158s（逻辑1.845759s），修后首次完整扫描1次、0.324324s；冷调用0.323444s，热调用0.000140–0.000276s。仅是有界合成测量，不代表真实7M行或最大负载验收。
- 新技术输入分支用完整 artifact 元数据摘要与规范 asof 绑定最多32项成功逻辑验证；不保存路径、reader、连接或文件句柄。每次仍实读 SHA，并核对命名文件/已打开文件、适用 root 和时刻；活动 reader 原件/复制件尾部另核对原 dev/inode/完整文件身份。首次沿原 generic 全验证，每个 reader 的初始化 SQL 校验保留。v1、通用 hash、公开模型及原事务/恢复不改。
- `perf-fixture-corrected-red.xml` 实际1 failed/1.50s，准确复现6次完整扫描；前两份夹具错误失败原件亦保留。有效去重6新节点见 `perf-initial-green.xml` 6 passed/3.23s，修改分支补验 `warm-mutation-green.xml` 2 passed/1.56s 属于同两节点，不累计。直接旧6节点 `direct-regression.xml` 6 passed/4.90s，包含原初始化、库存损坏、完整 worker/replay、输入缺失/尾部及旧 spec/prefix 黄金；无skip或删除改名。新元数据/时刻错配、恢复mtime的原件/副本修改、同字节换inode、容量及FD关闭有实际断言。
- Python3.13.12/Pydantic2.13.1/DuckDB1.5.2/pytest9.0.3；仅本树导入、dummy配置、禁dotenv/bytecode和自有TMPDIR。逐命令 JSON/log/XML 记录实际argv/UTC/wall/exit，所有失败静态结果保留；最终 Ruff/format 通过，3.11语法/源码SHA/资源与精确nodeids随私有 handoff 保存。测试后变化仅等价排版及合并 with 上下文，不重复累计节点。
- 实现冻结后停写；原 reviewer 仅复核 FTH-PERF-01 和直接回归，root 继续原真实3 jobs/1800s上限及独立参考/资源验收。未访问真实材料或声称超时已在真实来源解决，未重跑前端/API/浏览器及全清单。
