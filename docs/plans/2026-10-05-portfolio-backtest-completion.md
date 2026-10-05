# 日线组合回测：完整使用路径

日期：2026-10-05。状态：SPEC 待独立核对，尚未编码。

## 1. 目标与范围

用户在回测页选择池子、区间、资金、基准、成本、权重和调仓周期。提交后，由现有 Lab 执行任务。页面读取封存结果，显示净值、回撤、绩效和全部明细，并下载同一结果的 HTML 与 ZIP。

本片闭合 C6.1、C6.2、C7.1～C7.3，以及已实现的 C9.1、C9.2 的配置和结果使用路径。C6.3 的旧分钟引擎迁移另按原计划逐笔对照。过拟合检查只显示有依据的结果；M8 的全量实验比较另接同一产物。M11 不在范围内。

沿用原型回测页与 v2 §4.6、§4.7。原型中的示例数据和可关闭 A 股约束的开关不能进入正式日线执行。所有不具备真实来源的选项显示具体不可用原因。

实施基准：`8fe2627c03471d5c33868f0fadc8fb283793877d`。工作树 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-tracking-react`，分支 `cdx/20261005-portfolio-backtest-platform`。原生代理 `/root/research_query_impl` 为 implementer，父任务 `/root`。开始时 tracked 与非 ignored 文件均 clean；工作树复用记录保留在本片 verification 目录。

风险为高风险：本片新增受信任务准入和执行／结果／重试绑定。审查只覆盖本片身份、前视、私有账本和重复执行边界，以及直接回归。

## 2. 已核对的复用边界

原计划：`docs/plans/2026-09-28-portfolio-backtest-execution.md`。父任务的 NEXT-1 清单列出 19 个入口；本树对应文件的 SHA256 全部一致。精确来源和哈希见 `data/verification/portfolio-backtest-completion-20261005/spec-preparation/source-lock.json`。

| 现有能力 | 本片用法 |
|---|---|
| `run_portfolio_backtest`、`PaperBrokerStore` | 调用同一撮合、成本与逐日账本；每次执行使用 runner 的临时私有账本。 |
| 候选回执、日历、价格、参考事实适配器 | 从一次冻结来源装配完整 `BacktestRequest`，逐项核对可见时刻和来源摘要。 |
| `portfolio.weights`、`portfolio.drawdown`、`perf`、`overfit` | 使用已有算法及有效参考；本片补装配、调用和封存。 |
| 基准与 HTML 报告 | 使用已有六种指数目录、同区间基准和 `render_backtest_html`，HTML 上限仍为 4 MiB。 |
| Lab v3、原命令回执、实验登记、worker、finalizer | 继续原准入、租约、恢复、封存和登记链。 |
| 当前回测 API／React | 保留旧分钟回放合同，新增日线组合结果和运行配置；旧逐笔收益不作为组合净值。 |

v3 `ResearchRunSpec` 已能携带有类型参数、成本、审计快照、定义及实验身份。采用已有 `strategy_replay`，新增受控 `portfolio-backtest@1` adapter；不需要新增 JobType、任务账本或 spec v4。只执行一个跨全区间的顺序 shard，防止拆开资金和 T+1 状态。复用 `DateBucketShardInput` 表示区间，不按日期独立并行。

两处源码边界需明确接线：内置 adapter 目前只有四类；materialized-only 来源目前只允许精确 `factor_eval` 合同。父任务串行注册新的精确日线来源合同和受信定义，不能借旧分钟策略身份执行。旧任务的参数、哈希、序列化和封存字节保持。

## 3. 资产与信任边界

| 资产／边界 | 规则 |
|---|---|
| 浏览器 → Web → PageControl | 浏览器只提交配置与原请求身份。Web 用现有可信 ingress 身份、`lab_control_users` 和 CSRF 判定权限，并派生 interaction key。代码 SHA、资源窗、路径、快照、定义和正式实验计划由受信服务解析。 |
| 来源生产者 → 冻结输入 → worker | 生产者一次冻结并验证来源。请求、源模式、源哈希、日历、参考及开盘证据都进入同一有类型输入；worker 只消费精确审计绑定。执行期间不读变动中的主库或 Serving。 |
| worker → 私有账本 | 只允许受信研究根下的 runner 临时目录；线上模拟账户和生产库不在写集。执行失败或取消须由现有 worker 回收进程及自有临时目录。 |
| worker → 封存 → API／下载 | 原 spec／plan／claim／代码／快照身份与结果一一绑定。只有原 finalizer 接受的 sealed 产物可读。读取按原产物索引、FD 身份与哈希核验，浏览器不能指定文件路径。 |

不新增用户免登录分享、生产写入、任意 Python／SQL、网络取数、任务守护进程或第二实验登记链。生产安装、资源配置和对应旧服务停用由父任务另验收。

## 4. 输入、资源与失败规则

1. 输入使用新的小 `PortfolioBacktestConfig` 和冻结输入模型，浏览器完整请求上限 32 KiB。配置包括池子版本／排名来源、起止日期、初始资金、六选一基准、权重、单票／行业上限、现金保留、最小目标金额、调仓周期和 v3 成本。金额以精确十进制保存；拒绝 NaN、负值和超界输入。正式定义／实验计划缺失时准入失败，不能降级为无登记任务。
2. 日期沿用 Lab 的最多 `5 * 366` 日跨度；候选代码最多 500，装配的日期／代码价格对最多 20,000，沿用现有来源适配器上限。单任务一个 shard，工作量按实际日期／代码对计入原资源准入；超资源类预算或截止时间先拒绝。资源租约、运行窗和失败回收保持原机制，不能因本次取消行政时长限制而取消产品限额。
3. 采用受信的精确日线来源合同。新增 materialized 输入行须携带完整请求及来源清单摘要，并由原 DatasetSnapshotBinding 验证 schema、内容、行数和审计身份。代码、配置、成本、日历与输入 generation 必须一致；替换文件或换代即失败。
4. 前一交易日候选和决策价格须早于次日 09:25。当日开盘价只用于撮合，收盘价只用于估值。排名分／行业来源缺失时，对依赖它的配置返回不可用；不能以零分或当前行业补历史。
5. 保留原接受的回溯假设：历史日线按“前一日收盘价可在次日决策前使用”重放。源清单记录 `retrospective` 及假设，不把合成决策时间称为实际历史 `observed_at`。历史真实回执和参考发布证据分别核验。
6. 开盘可交易条件须有对应时点的受信证据。仅有日线 OHLC、价格在涨跌停范围内或日常停牌记录，不能证明开盘可成交。缺证据时记录“交易条件未核验”，保留原 runner 的跳过／拒单，不伪称一次有成交的真实来源验收。
7. 缺持仓收盘价时，保留原 `incomplete` 结果及已完成前缀；不补零、不延用旧价、不发布完整绩效或成功报告。选定基准缺起点前一交易日、区间内日期、非正价格或同代绑定时，明确显示“基准数据不足”，不伪造超额指标。
8. A 股 T+1、停牌、方向性封板、100 股整数倍和 v3 费用强制保留。日线按当日开盘价模拟，不保证实盘成交；分钟撮合和未实现的复权／公司行动处理只展示真实能力。组合回撤规则如参与决策，必须只读前一日已知净值；当日最终净值不能反过来改变当日订单。

## 5. 不可变结果与报告

采用新的领域 payload `portfolio-bundle/v1`，保持 Lab manifest v1 的固定文件合同。封存一个 `portfolio_bundle` Parquet 表，携带有类型的请求、来源清单、`BacktestResult`、`BenchmarkSeries`、完整绩效、HTML 字节及其 SHA256。精确 Decimal 以字符串往返，日期与缺值按模型编码；拒绝截断或不可逆类型转换。模型禁止额外字段，来源只保存有类型的身份摘要；不接收代码、路径、表达式或任意文件，不使用 pickle／eval 等可执行载荷。

另封存六张可分页的结果表：`portfolio_nav`、`portfolio_trades`、`portfolio_holdings`、`portfolio_daily`、`portfolio_monthly`、`portfolio_log`。共七张表，低于现有八表上限。日志来自实际决策、跳过和成交事实，不拼原型示例日志。逐日持仓可按日期查看，交易含拒单及费用，收益按完整账户与 FIFO 往返事实统计。

完整 bundle 的 UTF-8 JSON 上限 16 MiB，HTML 4 MiB；JSON 含已封存 HTML。所有表同时受原 wire／Parquet／finalizer 预算约束，超限任务明确失败，不发布部分表冒充完整产物。API 单次分页最多 50 行、完整 JSON 响应最多 16 MiB。图表点数受区间上限约束。

HTML 在 worker 中调用已有 renderer 生成并随结果封存，下载只返回原字节，离线可直接打开，无外部资源。新组合回测专用 ZIP 导出适配调用原受信 Lab 导出，核验原 ZIP 的 receipt／FD 身份／SHA256 后，向新 request 私有临时 ZIP 加入固定顶层 `report.html`，内容逐字节等于 bundle 内的封存 HTML。原 ZIP 的条目字节保留；新 ZIP 固定条目次序、时间和压缩配置，最长 32 MiB，并记录 job／complete-result-hash／HTML-SHA／ZIP-SHA／长度绑定。发布前后核验原产物与原 ZIP 未变化；失败清理只删除本请求自有临时文件。

新适配使用原 PageControl journal 的小命令 `export_portfolio_backtest_zip`，浏览器只传 job 和预期结果身份，不能传路径或文件清单。原 `export_lab_artifact_zip`、Lab manifest、封存文件和旧 ZIP 字节保持。下载 HTML 与 ZIP 均来自同一封存代；Web 不临时重算结果，也不另造封存或导出登记链。

绩效调用已有全部适用函数：净值／回撤、收益／年化／波动、夏普／索提诺／卡玛、月度／滚动统计、基准相对指标、换手、往返交易／行业／时长、分布与连胜连亏。数学上未定义、样本不足或需要未提供证据的项保留明确缺值。过拟合指标只消费同一日收益和受信试验信息；缺正式信息时显示“尚未评估”，不补造试验次数或解封记录。

## 6. API 与页面

| 接口／操作 | 精确行为 |
|---|---|
| `GET /api/v1/backtests/portfolio/capabilities` | 返回可用池子／版本、实际来源覆盖、基准、规则和权限；缺来源与可信零结果分开。 |
| `POST /api/v1/backtests/portfolio` | 小型配置请求，由受信组合提交适配构造原 `submit_lab_command` 创建 v3 任务；重复请求先读取原 journal 和已冻结输入，来源更新不能改写原任务。同 ID 换内容冲突，未知结果只恢复原请求。 |
| `GET /api/v1/backtests/portfolio` | 读取原任务与同代产物索引，区分等待、执行、失败、完成、未封存和不完整。 |
| `GET /api/v1/backtests/portfolio/{job}/summary\|nav\|trades\|holdings\|daily\|monthly\|log` | 只读精确 job／artifact／generation；换代返回冲突并重新取首批，不混结果。 |
| `GET /api/v1/backtests/portfolio/{job}/report` | 返回已核验的封存 HTML，下载模式与内容类型固定。 |
| ZIP | 受控专用导出命令取得绑定回执，再只读下载新 ZIP；顶层 `report.html` 可直接打开。未知回执恢复原请求。 |
| 暂停／恢复／取消／重试 | 使用现有受控命令与原请求恢复。页面得到提交回执后等待实际任务／sealed 状态，不提前称完成。 |

新增静态路由在原 `/{run_id}` 前注册；旧分钟接口和客户端合同保持。API 的公共错误短、可行动，不泄露目录、SQL、服务编号或哈希。所有 API 模型由 Pydantic 生成 OpenAPI 和 TS。

React 沿用原型：配置抽屉、运行状态、顶部 KPI、策略／基准净值与回撤联动、3 月／1 年／全部，以及交易／持仓／每日／月度／日志五个标签。使用现有 `@/ui`、`EChart`、`DataTable`、`useServingQuery`、统一 token 与红涨绿跌格式。说明放 hover／focus／tap tip。

运行时保留上一份已成功结果并标清当前任务状态。切换任务、改配置或加载新来源时，旧响应只能完成原请求恢复，不能覆盖当前选择或触发新任务。抽屉焦点、Esc、标签方向键、空态、错误重试、390px 横向表格和移动端 tip 均有实际验收。

## 7. 必须先执行的失败用例

对应实现前先保存红测或等价可执行状态矩阵；已有原算法的红绿和参考不重复制造。

| ID／等级 | 失败输入或中断 | 验收结果 |
|---|---|---|
| PB-01／P1 | 无可信身份、未授权账号、跨站写；浏览器带路径／代码／owner／任意 spec | 不创建任务；恢复身份仍绑定原可信账号和请求。 |
| PB-02／P1 | 同请求重发、同 ID 换配置、发布后回执丢失 | 一份原任务／实验尝试；换内容冲突；原回执可恢复。 |
| PB-03／P1 | 换 snapshot／请求／成本／代码／plan 或同路径替换源 | 准入或执行失败，无伪成功产物。 |
| PB-04／P1 | 使用次日排名／当日收盘／迟到参考作为当日决策输入 | 装配失败；保留明确回溯假设和真实历史时刻区别。 |
| PB-05／P1 | 缺开盘条件、停牌、涨停买入、跌停卖出、T+1、不足一手 | 沿用原明确跳过／拒单；现金、费用和数量对账。 |
| PB-06／P1 | 缺持仓估值、缺／重复基准、缺排名或行业事实 | 明确不完整／不可用；完整绩效、基准超额和完整报告不被伪造。 |
| PB-07／P1 | 超日期／500 代码／20,000 对／JSON／表／资源预算，已过截止 | 有界拒绝或任务失败；无部分表封存，无泄漏进程和 scratch。 |
| PB-08／P1 | worker 在成交后、提交结果前、封存前后退出；取消后重试 | 原租约和恢复链生效；重试使用新私有账本及原输入，线上账本不变，无重复登记。 |
| PB-09／P1 | 读未封存／错 job／错 generation／错 hash，文件替换或 symlink | 拒绝读取和下载，不混另一任务结果。 |
| PB-10／P1 | HTML 与 bundle 不同结果，ZIP 或下载被截断，导出时替换原 ZIP，宽行超 HTTP／ZIP 预算 | 身份／长度／摘要失败；只清理自有临时文件。单独 HTML 与 ZIP 的 `report.html` 字节相同且离线可打开；旧 ZIP 原字节不变。 |
| PB-11／P1 | 提交 A 未回执时切 B，随后 A 成功；B 分页期间索引换代 | A 原恢复完成，B 仍当前；B 的页面、下载与分页不混 A 或新代。 |
| PB-12／P1 | 精确金额／缺值／零交易／未平仓／短样本／不完整结果 | 模型往返、表及图表一致；未定义值为“—”，不伪造盈亏、净值或评估。 |

本片没有另行发现必须增加范围的 P0 路径。发生生产账本写入、未授权执行或跨输入成功封存时按 P0 阻断。

## 8. 实现写集与顺序

当前准备阶段仅写本 SPEC 和本片 verification；尚不提交 Git、安装依赖或改产品。

SPEC 接受后，业务写集：`src/rquant/backtest/**`、新的 `src/rquant/portfolio_backtest_*.py`／`src/rquant/web/portfolio_backtest_*.py`、`src/rquant/web/models/backtests.py`、`src/rquant/web/routes/backtests.py`、`web/src/api/backtests.ts`、`web/src/pages/backtest/**`、直接 `tests/unit/test_backtest_*.py`／`test_portfolio_backtest.py`／`test_web_backtests.py`、`web/e2e/backtest*.spec.ts`，以及本片资料。

父任务串行写集：原清单的 `page_control.py`、`lab_job_center.py`、必要 `lab_jobs.py`／`runtime_builder*.py`／`cli.py`／`web/app.py`，以及实读确认的 `strategy_job_adapters.py`、`strategy_dependencies.py`、`research_snapshot.py`、`strategy_evaluators.py` 中受信 Definition／execution registry 注册入口。最小补丁如下：

- `strategy_job_adapters.py` 注册领域模块导出的 `portfolio-backtest@1`；`lab_job_center.py` 的闭合输入加入组合配置与原预算预检。
- `strategy_dependencies.py`／`research_snapshot.py` 只为 `portfolio_backtest` 的精确日线 contract 加 materialized-only 分支；拒绝任意策略、任意表和不一致 schema。受信定义登记只加该 replay 身份。
- `page_control.py` 注册小型组合提交／ZIP 请求，使用原 journal／回执；`lab_page_control.py` 只接专用领域 adapter。Web 与运行时入口只提供受信构造参数。
- 新任务的模型、装配、执行、读取和 ZIP 适配独立放 `portfolio_backtest_*.py`，root 串行完成上面共用注册。旧任务的 run-spec JSON、spec／plan 哈希、manifest 清单和 ZIP 字节用固定兼容例核对。`research_run_spec.py`、`lab_artifacts.py` 与原导出实现预计无需改；若实现证明需要改变边界，先冻结具体最小 diff。

OpenAPI、`schema.d.ts` 和 `web/dist/**` 由父任务串行生成。禁止改生产部署、竞价／因子业务、Notebook、旧分钟执行器及无关共用框架。

顺序：① 聚焦红测和模型／装配；② 日线 adapter、原 Lab 接线与实际私有 worker；③ 完整产物／只读 API／报告；④ React 全路径；⑤ 一份候选集中独立审查与定向复核。实际阻断才补修；不按阶段重复全套审查或测试。

## 9. 来源、环境与验收证据

父任务只读预检历史池子回执、日历、开盘证据、参考和指数。按日期倒序选择最近可证明区间，记录起点、回推天数、文件身份与哈希。源码可复用不代表真实数据已具备；当前准备阶段未验证实际历史来源或正式安装。

优先使用已有 Python／Node 缓存，隔离 import、禁止 dotenv 和真实通知，测试只在私有目录使用合成输入。编码前核对 UDS、真实子进程、文件权限、剩余磁盘及浏览器能力；正式环境的资源窗、受信身份和来源绑定另由父任务实际验证。跳过或受环境限制的项记缺口，不称通过。

复用现有三股十日、A 股规则、绩效独立参考和原 Lab 身份／恢复／封存的有效结果。新增验证覆盖 PB-01～12、实际 worker→sealed→API→网页→下载，以及每个标签、1440px／390px、键盘和延迟响应。只对受影响模块运行相关回归、生成类型、构建和 dist 检查；全量仅由新增共享风险或仓库门禁决定。

完成条件：网页可配置并提交实际任务；同一完整逐日账本的现金＋市值＝净值、费用和数量可对账；基准／绩效／图表／明细／HTML／ZIP 同源；失败、重试与缺来源保持准确；实际子进程与自有目录已收尾；精确候选 SHA、真实命令／退出／时长、红绿证据与剩余安装缺口齐备。仅有本 SPEC 不构成产品完成。
