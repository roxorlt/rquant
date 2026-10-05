# 实验平台补全

## 1. 目标与范围

完成 v2 §4.8 的 C8.1–C8.4：正式登记、网格和随机搜索、参数热力图、过拟合检查、两条实验对比、备注和一次样本外解封。页面以原可点击原型为准；不复制其示意净值或统计数字。

本片是高风险任务。新增风险限于正式族的完整登记、用户隔离、提交恢复、样本外读取和结果绑定。原撮合、绩效公式、定义登记及 Lab 队列继续复用。没有生产迁移、权限安装、真实通知或 Streamlit 切流授权。

来源、直接入口和实际 SHA 见 `data/verification/experiment-platform-20261005/spec-proposal/source-lock.json`。ROOT 基准由父任务提供：`0e7bb989f20d3778c38b82a0aff1971e3de52713`。C6 最新候选由父任务提供；本片只读其领域接口，不把待复核状态写成已通过。

本次只补原审查的 EXP-SPEC-01/02。新资料在 `spec-proposal/repair-r01/`，直接来源以该目录的 `source-lock.json` 为准；父任务提供的当前 HEAD 为 `66fe67ae2f577b42831ecf35ab474d4c86c25c8f`。原方案、冻结和审查保留，26 项验收仍未执行。

## 2. 复用的实际路径

| 已有入口 | 本片用法 |
|---|---|
| `ExperimentRegistry` 的 family、formal plan、attempt、submission intent | 保存原不可变正文；完整参数族先登记，再发布原提交意图。 |
| `build_research_job_submission`、`LabCommandSubmissionFacade` | 每个实际配置进入原 v3 任务。任务、定义、成本、来源和参数必须精确相符。 |
| `ExperimentJobLifecycleSynchronizer` | 计算完成仍记为 `EXECUTED`。没有完整统计证据时不伪造 `SUCCEEDED`。 |
| C6 `freeze_portfolio_config`、`publish_portfolio_input`、`PreparedPortfolioRequest.submission` | 生成真实受限输入，沿原 `portfolio_backtest@1` 执行。 |
| C6 `PortfolioResultReader.read`、`PortfolioBundle` | 读取完整封存结果和参数。摘要或截断 preview 不能代替净值原件。 |
| `overfit.py`、`overfit_pbo.py`、原 BH 实现 | 只接真实输入和结果，不重写公式，不把年度夏普直接当单期夏普。 |
| 原 PageControl、Serving、实验分页 API | 复用原请求恢复及同代读取。旧列表 DTO、分页窗口、旧产物和 ZIP 字节保持。 |

第一条可运行路径使用 C6 的组合配置。C5 模板接口完成后，接精确定义 ID、版本、登记回执及有类型运行输入；不按 ID 前缀或附属名称获得执行权。C5 尚未交付的接口只记依赖，不阻断其实施。已有分钟优化器只保留原受信执行口径；没有完整封存曲线的旧任务显示不可用，不能套成新组合结果。

## 3. 正式输入与容量

新增严格模型 `ExperimentFamilyRequest`。浏览器只给显示名、可选策略版本、现有来源 key/version、受限配置、三个日期区间、搜索方式、参数取值、选择指标和随机种子。actor、文件路径、代码身份、登记回执和函数绑定由服务端取得。

| 输入 | 固定边界 |
|---|---|
| 训练、验证、样本外 | 三个非空、连续交易日范围，严格先后且不重叠；由同一份 SSE 日历核对。开始搜索前固定。 |
| 组合搜索参数 | 第一版仅 `weight_rule.max_positions`、`max_stock_weight`、`cash_reserve`、`min_target_amount`，以及 `every_n` 模式的 `rebalance_rule.every_n_days`。其他配置固定。 |
| 数值 | 沿原完整配置校验；最多 500 持仓，N 为 1–252，比例沿原模型，金额精确到分。拒绝布尔整数、NaN、Infinity、额外字段。 |
| 网格 | 1–5 个可搜索字段，各 1–16 个唯一有序取值；实际组合 1–64 个。每个完整组合均校验，任一非法则整个新请求拒绝。 |
| 随机 | 同一离散空间最多 4,096 个合法组合；无放回抽取 1–64 个；种子 0–2³²−1。算法版本和抽取顺序一并固定。 |
| 来源与产物 | 沿 C6 每项 500 码、20,000 个 instrument-day、五年日历跨度、16 MiB bundle、32 MiB ZIP；本片全部准备输入合计不超过 512 MiB。超限拒绝，不截短窗口。 |
| 统计矩阵 | 2–64 候选、最多 4,096 期、262,144 个值；原 CSCV 切片数只能为 4/6/8/10。 |
| 页面与备注 | 单次列表最多 50 条；旧最近 500 条窗口继续标明。备注最多 1,024 字、4 KiB；响应最多 8 MiB，超过时分页，不截断权威数据。 |

随机方法固定为 Python 标准库 `random.Random(seed).sample`，输入是按字段名及合法 Decimal/整数取值排序的完整组合数组。保存实现版本和实际抽取数组；恢复直接用已保存数组，不靠重新随机复现。潜在空间大小、已计划配置数、已登记尝试数、失败数和取消数分别保存。重复参数先拒绝，不增加试验次数。

所有候选使用同一执行器、成本、资金、基准、交易频率及原始来源版本。来源完整范围或实际数据缺失时拒绝正式提交；允许说明实际历史回顾假设，不能冒称 PIT。预热只用计算日之前的资料，不能把预热收益算进评价范围。

## 4. 完整登记、提交与恢复

新增 `register_experiment_family` 命令，走原受信 PageControl；配置默认关闭。已核验身份和 CSRF 必须先通过。相同 owner/command ID 先查原日志，再恢复原正文；换正文拒绝。不得先按当前目录重新生成一份搜索。

1. 原日志保存请求。原 registry 同库的附属 `experiment_request` 表固定 actor、原请求摘要、服务器时刻、政策代、完整参数数组和稳定 child job ID。状态先为 `preparing`。
2. 受信准备器按参数顺序冻结每份训练/验证输入，保存原发布回执及完整输入摘要。失败保留准备记录；没有完整族时不发布任何 job。
3. 新 `ExperimentRegistry.register_family_submission` 在一个 `BEGIN IMMEDIATE` 内校验并提交原 family、全部 formal plans、全部 attempts、原 submission intents，以及附属请求的 `ready` 状态和精确 family→owner 私有事实。复用原校验和 insert 逻辑，不嵌套调用各自提交的旧事务方法。新私有事实与原 attempts 同事务出现；不能先写会进入旧共享列表的无所属记录。
4. COMMIT 后原 `recover_pending_experiment_submissions` 发布原意图。每个 stable child ID 只对应第一次固定的 spec 和 envelope。新正式子项须先持久取得下面的发布准入，再调用原 `_publish`；准备中或取消先成立的子项不能发布。
5. 计算终态和封存事实沿原 lifecycle 写回。页面另存精确 result binding；它不能修改原 attempt 状态或给自己统计权限。

准备器复用 C6 的冻结和发布函数，新增纯计划构建入口，允许受信调用方提供已核验的完整族 manifest。原 `register_portfolio_plan` 的单项默认行为保持。准备回执用私有路径及原内容/来源核验恢复；不能仅凭同名文件当成成功。输入发布后、回执保存前中断可能留下未引用资料；记录并清理自有孤儿，不创建第二份 job。

### 发布与取消的持久顺序（EXP-SPEC-01）

顺序以原 registry 附属事务提交为准，不以时间字符串、最后一次重读或发布回执为准。附属 `experiment_child_admission` 固定 owner/family、stable job/request ID、spec 和原 envelope 摘要、递增操作序号、发布准入序号及取消序号。发布准入一经提交便不可撤销；取消后来提交时记录待取消，不把它改为从未发布。

| 先后 | 必须保持的事实 |
|---|---|
| 取消先于发布准入 | 同一事务固定取消顺序，并用原失败校验/写入逻辑终止尚未准入的原 attempt。原 intent 保留且附属事实标为禁止发布；后续 stale 发布者取得准入失败，不调用 `_publish`。 |
| 发布准入先于取消 | 保留原 spec、attempt 和 intent；取消写为 `pending`，绑定同一 child。即使 job 还查不到或回执丢失，也不能直接标取消、删除 intent 或推断从未发布。 |
| 原作业已成功或永久失败 | 先按原 lifecycle 同步实际终态和结果，再结束待取消事实。已完成仍为 `EXECUTED`，已有合法 outcome 保持；不能改为取消。 |
| 原作业确认取消 | 沿原 lifecycle 写 `CANCELLED`，待取消事实绑定真实 job/command 终态后才确认。 |

`LabCommandSubmissionFacade.submit_create`、`recover_pending_experiment_submissions` 的新正式分支都调用同一 `admit_child_publication` 事务入口。它核完整 ready 族、原 intent、原 child 身份及取消顺序，再提交准入。取消族调用同库入口，和准入在 SQLite 写事务中排序；不跨库假装原子。取消先提交时不存在可用准入；发布先提交时后来的取消不会令已准入身份失效。

原 `validate_prepared_experiment_submission` 在 `LabJobLedger.apply_command` 的既有正式准入 callback 内，除原 spec/登记校验，还必须读取新私有子项的实际准入事实：无准入、身份不符或取消先成立时拒绝新建 job；发布已先准入、取消仍 pending 时允许原同一 job 创建，随后继续原取消。既有 `lab_jobs.py` callback 位置复用，不改旧普通任务规则或创建第二个执行器。成功准入至实际建 job 之间发生取消，可能先运行或完成；页面要诚实显示该顺序。

恢复先核原 spool/command 回执和真实 job，不仅看 intent 的 prepared/published 字段。结果未知时，保留原准入及待取消事实，按原 envelope 恢复/重放；不新建 child。原 job 可见后，固定其实际 version 和稳定取消请求，再提交原 `submit_cancel`；取消回执未知先 lookup 同一请求。仅在已证实 stale 且原请求未生效时，才以新的实际 version 记录后继取消请求，旧请求留存。`job_not_found`、未保存发布回执或仅收到取消提交回执都不是取消完成证据。

准入后中断不得遗漏 child：恢复原发布能让 job 落地，再取消或读取其真实终态；重复 envelope 仍只产生原一个 job。取消先成立的子项从可恢复发布集合中排除，后续有效 intent 仍继续。计算已完成的子项不删结果；未知或 pending 取消显示「正在核对取消结果」，必要 Tip 说明「已准入任务可能先完成」。基础设施重试复用原 job 和 intent，不增加参数尝试数；永久失败或取消的 attempt 不复活。新研究保留前次关联及累积搜索事实。

## 5. 样本外封存区间与一次解封

这里的封存是研究隔离，不是文件保存期限。新增服务端 `ExperimentHoldoutPolicy`：N 为 0–36 个月，默认 0，policy ID/version、实际覆盖范围和管理员身份均有记录。`set_experiment_holdout_policy` 只接受已核验、服务端明确配置的管理员 owner，并按期望 version 做 CAS；浏览器不能给自己管理员身份。N 月从请求服务器上海日期减去日历月，月底按实际月长收敛，再取该日及以前已收盘的 SSE 交易日。所有正式受保护任务的结束日必须不晚于这个 cutoff；N=0 也不能运行未来或未收盘日期。

政策只对明确正式族及服务端登记的受保护来源身份启用。旧普通回测默认行为保持，不扩到因子或任意旧任务。页面称「实验样本外封存」，并说明当前覆盖正式实验；未完成旧入口的显式接线前，不宣称全平台所有回测均受此限制。

搜索只准备训练和验证的具名切片。受保护来源可能已经含有样本外日期；其基库只供受信来源服务读取，搜索任务拿到的 private snapshot 不含样本外行或可绕过的原路径。Serving 和 Web 不读取基库。正式提交、原恢复、阶段 source resolver 和完整结果 reader 都核对精确族、phase、来源许可和日期。阶段来源入口接收服务端许可及明确日期，在基库查询或具名读取阶段就取正确范围；不得先交给研究 worker 全包再裁剪。若来源只支持包含封存值的整包输入，无法形成受限切片时，该来源暂不能用于正式搜索。

约束保护本平台正式路径。它不证明用户此前从未查看数据，也不能阻止同一系统身份直接访问自有原件或在其他工具研究；因此不宣称盲测独立性或 PIT。旧公开回测结果不能通过改名转成未查看的样本外证据。

解封流程固定如下：

1. 搜索终态后，用户选一份实际完整结果。服务端固定所选参数、来源、代码、成本、原族、选择规则和样本外区间，显示一次确认。
2. 新 `unseal_experiment_outer_test` 通过原日志。附属 quota 表按 owner 和实际样本外日期记录；与已准入区间有交叠的新解封拒绝。改族名、参数、来源版本或命令 ID 不重置次数。
3. 在准备样本外行之前，单事务核对政策、所选结果和 quota，并记录不可撤销的 `admitted` grant。稳定 outer job ID、完整所选配置及原搜索总数在同事务固定。此时计为一次解封，不冒称 job 已执行。
4. 受信准备器按 grant 取样本外切片。为它另建关联搜索族的原 formal family/plan/attempt/intent，仍用原 v3 执行链；三个区间沿原搜索协议，实际 run 参数只覆盖样本外。outer family 的单项计数不能代替原搜索总数。
5. 原请求恢复可续完同一 grant、source 和 job。准入后准备失败、取消或永久失败也消耗次数；只能恢复该原作业，不能换参数重新解封。再次读取原封存结果不算解封。

原搜索参数数组、全部终态和真实统计来源随 outer evidence 保留。新的统计产物关联原搜索 N，不把关联 outer job 算成新独立试验，也不把 N 改成 1。quota、policy、grant 和原请求记录使用原 registry 的专用附属表，不创建第二个实验登记器或通知/任务队列。

政策变化前已成功准入的原请求按固定政策代恢复；新请求使用当前政策。读取只能返回已封存且授权的 phase。解封 grant 没有产物时显示「已解封，结果未完成」，不能显示样本外已通过。

## 6. 结果、热力图与邻域

新增严格 `ExperimentResultBinding`：owner、原 family/plan/attempt/job、精确定义回执、实际 phase/日期、完整参数、source 和 cost 摘要、spec/manifest/result hash、完整曲线与全部指标来源。只能由原封存 reader 生成。请求摘要或 PageControl 成功回执本身不是计算结果。

训练和验证复用同一条连续回放，按固定日期分别取实际净收益，并复用原 `perf` 计算范围指标；验证承接训练末账户，清楚标明。样本外另起原配置资金的独立回放，只用解封前可用预热资料；页面不能把三段拼成一条连续账户净值。分段交易数沿原实际 fill/round-trip 日期，不能把跨段未闭合持仓伪成已闭合交易。

每个实际参数组合都占参数×指标表的一行；失败、取消、缺数据也有行和原因，不删除以美化结果。指标包括实际净收益、年化、波动、夏普、索提诺、卡玛、回撤及持续期、交易数、换手、胜率、盈亏比和可用的基准/超额指标。缺指标为 null，不能填 0。

热力图任选两个已搜索数值参数，固定其余参数、同一 phase 和指标。相同格只允许一个完整实际配置；不能取其他维度的最好结果来填格。随机搜索未抽到的格标「未运行」。当前参数来自所选实际配置，不另写原型默认值。

邻域是两个有序轴的相邻索引，排除中心，最多八格。显示实际格数和可用最低值；有失败或缺格时标「邻域未完整运行」。边缘只有三格或五格，按实际数量写。仅给邻域数值，不据此自动声称稳定或非孤立尖峰。键盘和触摸都能打开与同一完整参数绑定的详情。

## 7. 有证据的过拟合检查

新增 `ExperimentOverfitEvidence`，保存原族、完整尝试账本摘要、日期/频率/成本、收益向量摘要、输入证据、算法版本、可用结果或稳定 unavailable reason。由服务端输入构建器生成并封存，不能从前端列表的年度夏普倒推完整收益。正文用 canonical JSON，在原 registry 的专用 `experiment_evidence` 附属表按内容摘要追加；真实输入始终引用原 Lab 封存结果。它不是新 Lab result，也不能追加或改写旧 job 的 manifest/ZIP。原 artifact 被替换或失效时，派生证据不可用。

| 检查 | 输入和拒绝条件 |
|---|---|
| PSR | 实际单期净收益的夏普、偏度、Pearson 峰度及独立观测证据；至少 30 期。无独立性证据、零方差或非法矩时不可用。 |
| MinTRL | 同一份 PSR 输入、预登记置信度，默认 95%。低于目标夏普时显示「当前收益水平无法达到目标」，沿原 `unreachable`，不造有限天数。 |
| DSR | 完整搜索族的实际单期夏普及可信独立试验数证据。没有独立性或完整族不能计算；回测总条数不自动等于独立试验数。 |
| CSCV/PBO | 全部候选在相同训练/验证日期的有限净收益矩阵。两半至少各 30 期且满足原整除、容量条件；缺候选、错日期、并列夏普或零方差按原拒绝原因显示不可用。内层 OOS 不是外层解封。 |
| BH | 复用原 BH，分母为完整已登记搜索 N，含失败和取消；只校正有真实合格 p 值的条目。没有 p 值不补造。 |

单期输入构建统一使用已封存的实际净收益。第一版沿 C6 的日频和零无风险利率：夏普用均值/样本标准差（ddof=1），偏度和 Pearson 峰度用总体中心矩及总体方差标准化；保存估计约定及原模块输入。PSR 原假设满足时，预登记单侧检验的 p 值为 `1−PSR`；其他检验不得沿用该值。原 BH 只增加受限纯函数包装调用已有实现，不改算法或旧 outcome。

独立性不能从摘要、相关系数或用户勾选推出。输入模型只接受受信 evidence ID 和正文摘要，验证实际观测/完整族范围、独立试验数和适用假设；本片不创建有效试验数估计器。证据解析器由服务端安装，默认返回无证据，不开放 Web 上传或自填独立次数。当前没有这类真实证据时，PSR/DSR/MinTRL 显示「缺少独立性证据」，PBO 仍按自身完整矩阵合同判断。合法明确的独立测试输入须能走通原公式；合成证据只能作为测试，不能显示为真实研究通过。

原 lifecycle 的 `EXECUTED` 不转换成有统计 outcome 的 `SUCCEEDED`。新统计结果是附属证据，不能用点估计补造收益置信区间、既有 `ExperimentOutcome` 或晋级结论。原已具合法完整 outcome 的实验继续使用原 `adjust_hypothesis_family`、`evaluate_promotion`。`PromotionDecision.approved` 只表示计算 gate；C5.3 的证据评估和两步人工批准仍须完成，本片不写用户批准或启动监控。

## 8. 两条实验对比与备注

新增 `GET /api/v1/experiments/compare?a=&b=`，必须恰好两个不同、同 owner、已封存的结果。返回两份完整参数、曲线、全部绩效和逐字段参数差异。不能用列表摘要补曲线，也不能使用其他版本的最近结果。

比较口径包括实际日期、frequency、phase、资金、基准、执行器/代码、成本以及 `FrozenPortfolioInput.sources` 的 market/reference/opening/ranking/industry hash；参数配置差异单列。真实口径相同才给 B−A 指标差值；否则两份结果仍可并排查看，并显示「口径不同，暂不计算差值」。空指标保持空，回撤符号沿原定义。每条曲线保留原实际日期、断点和 normalized NAV，不补行情、不插值拼日期。

新增 `set_experiment_note`：owner、实验身份、期望 note version、正文，经原日志和同库 CAS 写入。备注只能改用户说明，不能改定义、试验次数、结果或解封状态。同请求重放返回原版本；并发编辑冲突保留本地草稿。

### 本人列表与旧共享列表（EXP-SPEC-02）

选定兼容路径：**从旧共享投影排除所有新增私有族，新本人列表走 `GET /api/v1/experiments/mine`。** 旧 `GET /experiments` DTO、游标类型及最近 500 条合同保持；它不显示本片新增私有族，不按浏览器 owner 参数补过滤。

原 registry 只读器新增 `read_legacy_shared_serving_snapshot`，在同一只读事务内按精确 family→owner 私有事实排除新 search/outer 族，再排序和 LIMIT。`PromotionsSourceReader.__call__` 的旧 `experiment_attempt`/window 接线调用此入口；窗口数量、truncated、oldest、事件时刻及游标可达行均来自该排除后的同一集合。不得先取全局最近 500 条再删私有行，否则旧记录会被挤走。关联的新私有晋级记录也不进入旧共享 promotions；旧共享记录的原数值和顺序保持。

旧 promotions 的仅事件模式也用 `read_legacy_shared_promotion_decisions` 排除这些族。新写能力关闭后，已存在私有事实仍继续排除；不能因关闭新按钮而重新公开。私有版本未知、应有附属事实缺失时发布失败，不回退全局读取。只有确认为旧 schema、没有本片私有记录的旧环境沿旧路径继续。

本人列表用严格新 DTO，从附属同代投影按已核验 `current_user` 取记录；不接受 owner 选择参数。每个 owner 的最近 500 条先按本人集合取 501 判断 truncated，再保存本人 window 和正文。新私有投影最多 32,000 行、8 MiB；超限整个新私有视图明确不可用，不丢 owner 或伪造完整窗口。首批、数量、oldest、下一页和结果引用只涉及本人集合。新签名 cursor 固定 route kind、owner、generation、page size 和末行；其他 owner 的 cursor 拒绝，不返回对应行。

启用新 writer 前，共享 publisher 必须已接受私有 schema 和排除接口；缺新 schema、未知版本或私有事实不完整时拒绝新写入及相关发布。不能把新记录默认为旧共享。未启用本片且只有旧数据时继续原只读路径。Web 旧路由不改查询和 DTO；其输入表已是明确旧共享集合，新增直接测试同时读取该旧出口。

新详情、family、heatmap、statistics、compare 用独立严格 DTO；S2 新附属版本发布真实 result binding、备注、policy 和 grant。新摘要从同一有界 Serving 代读取；完整结果沿原只读私有 reader，前后核对 owner、同代引用及结果 hash。409 时撤下两份统计和曲线，不混代。

## 9. 用户路径与文案

- 「新建实验」依次选择策略/组合来源、三个区间、搜索参数和固定成本。确认页显示实际计划数量及「样本外未解封」。提交后进原任务中心，不另建运行页。
- 本人列表显示策略、版本、完整参数摘要、阶段、状态和主要绩效；旧共享记录入口保留。取消未知或尚未确认时不显示「已取消」。计算封存且统计不足时用「结果已生成」和具体统计原因，不能显示研究验证成功。
- 「对比所选」最多选择两条；第三条暂不可选，保留原选择。正文显示短结论，hash、source revision 和内部计算说明只放 Tip。
- 备注有明确保存按钮和成功/冲突状态。未知回执显示「正在核对结果」，只恢复原命令；迟回执不能覆盖新草稿。
- 样本外确认显示区间、实际所选参数及「解封后不能更换参数重试」。失败显示「已解封，结果未完成」，链接原任务。
- 热力图有可聚焦表格等价视图，方向键选择、Enter 查看参数，不能只靠颜色。Tip 可 hover/focus、Enter 和 390px tap；Escape 关闭后返回原入口。
- 1440px 展示并排曲线和指标；390px 按块排列，表格在自身容器滚动，不让整页横向溢出。数字沿统一百分比/千分位/时间格式。
- 换用户、来源失效、409 或权限失效时撤下旧选择、详情和写入能力。未完成、取消、不可用和统计证据不足分别显示原因，不将空表或零收益称为健康。

## 10. 最小写集与角色

实现者 owns 新 `experiment_platform.py`（严格模型、参数枚举和来源绑定）、`experiment_platform_commands.py`（原命令效果和恢复）、`experiment_platform_evidence.py`（真实产物和统计输入）、`experiment_platform_projection.py`、专用 Web DTO/路由/服务、实验页及直接测试。可收拢紧邻文件；不按步骤创建框架或新 worker。

root 串行共享接线仅限：

| 共享文件 | 必要变更 |
|---|---|
| `experiment_registry.py` | 显式 private schema、附属表、完整族单事务、发布/取消排序、旧共享排除只读入口、原 BH 纯包装；旧正文模型保持。 |
| `lab_job_center.py` | 新正式分支的 submit/recover 持久准入；既有正式准入 callback 核 actual grant；原 pending 取消恢复。`lab_jobs.py` 只读复用 callback，不新增队列。 |
| `promotions_serving_authority.py` | 旧 experiment_attempt/window 及 promotions 接线切到精确旧共享集合；排除新私有族后再算有界窗口。 |
| C6 `portfolio_backtest_source.py` | 拆出纯计划构建及正式阶段切片入口，受保护来源需 exact grant；原单项默认路径保持。 |
| `page_control.py`、`page_control_service.py` | 注入专用 backend 和受信命令，不给裸 actor、路径或任意 payload 执行权。 |
| `lab_jobs_serving_authority.py`、`serving_read_models.py`、`serving_page_projection_source.py`、`runtime_builder_serving.py` | 新实验附属投影和严格同代引用；旧窗口和原策略/任务投影保持。 |
| Web app/settings/readers 与 API 生成文件 | 默认关闭的能力、受信 owner、CSRF、新 mine route/owner cursor 和生成类型；旧 `web/routes/experiments.py` 查询/DTO 保持，直接测试其首批和分页。 |

不改 `definition_registry.py` 的发布协议、原 `overfit`/PBO 数学、原 broker/perf、旧 Lab 产物格式或旧 ZIP 构造。C5 adapter 完成时 root 另列 exact typed shared diff；本计划不冻结未实现源码。附属 schema 仅在私有测试库及显式入口升级，production SQLite 迁移另需授权。

## 11. 不变量与直接验收

完整可执行失败矩阵以 `spec-proposal/repair-r01/failure-matrix.json` 为准；每项固定触发、观测和失败条件。原矩阵仍保留。以下 ID 稳定使用：

| ID | 必须证实的边界 |
|---|---|
| EXP-01–04 | 身份/CSRF、参数/容量、日期/成本/来源、精确定义绑定；失败无新任务。 |
| EXP-05–08 | 完整族单事务、持久发布/取消两排序、原请求优先、未知发布待核取消、取消/失败全账本。 |
| EXP-09–13 | N 月 cutoff、仅正式受保护来源、零样本外提前读取、单次并发准入、准入后失败恢复。 |
| EXP-14–18 | 全封存结果绑定、合法原统计/不可用原因、完整族计数和 BH、MinTRL unreachable。 |
| EXP-19–22 | 参数表完整、热力图固定其他维度和邻域、两份真实结果及不同口径、本人与旧共享首批/分页隔离。 |
| EXP-23–26 | 备注 CAS/恢复、全部用户动作、键盘/390px、旧合同及收尾。 |

## 12. 实施顺序和完成判定

1. 冻结本 SPEC 与对应失败用例。一次独立 SPEC 核对，只看本片失败模型。
2. 实现完整族事务、原命令恢复、阶段来源和解封准入；先跑 EXP-01–13 的定向红绿证据。
3. 复用 C6 executor 走网格及随机、真实封存绑定、原统计和比较；补 EXP-14–23。旧公式的有效论文/纯噪声测试复用，不重审数学。
4. 接 React 和同代投影，实际完成新建、取消、备注、查看热图、对比、一次解封和未知回执恢复；验证 1440/390、键盘和换用户。
5. root 安排用户自有只读材料的真实小范围研究：至少两份实际封存结果、完整尝试记录、成本/来源、参数差异及曲线逐值核对。来源不足或统计假设不足须写明确限制；合成场景不替代真实研究。
6. 冻结最终候选，做一次集中独立终审。原作者修范围内 finding，原 reviewer 定向复核；有效证据继续复用。清理自有临时输入和进程，保留失败原件。

本文件只完成方案。尚未实现 C8 产品，不宣称统计真实通过、用户晋级、全市场容量或生产运行。实现验收后再更新差距表和来源状态；本片的新按钮不得把未完成依赖隐藏为成功。
