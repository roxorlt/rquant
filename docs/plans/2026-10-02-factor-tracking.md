# 因子持续跟踪：真实开关、每日增量与失效提醒

## 目标、身份与分级

依据 owner 已授权的完整原型/差距表目标及新前端计划 v2 §4.3 C3.3：页顶加入/取消跟踪，同页展示昨日 IC、近20日均值/IR、昨日/近一周/累计多空收益，失效进入总览“需要关注”，每工作日18:40在研究面增量计算。v2 的18:40优先于原型toast中的18:10示例。

Codex desktop 原生 `/root` 是 orchestrator，父任务为当前 goal；实现、相关测试和定向修复由原生 implementer，最终候选由一名独立原生 reviewer集中审查。规划基线 `bd7232eda489e14f63afa876cfce769b3b375a0c`，集成树 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration`，分支 `cdx/20260929-factor-source-integration`。原生后台/前端实现者已各完成只读准备，未修改代码或重复测试。编码树以本计划提交后的精确commit隔离创建，写前记录真实身份、父任务、分支、worktree、基准及dirty来源。

本片为高风险任务：持久跟踪集合、原命令故障恢复及逐日贡献/游标的幂等提交涉及状态一致性；范围严格为新增跟踪路径和直接共享契约，不扩成全仓审计。根任务实际预检 Python3.13.12、APScheduler3.11.2、NumPy2.4.4、Pydantic2.13.1、DuckDB1.5.2、pytest9.0.3，私有 Unix socket 可绑定并已清理，证据 `/private/tmp/rquant-factor-tracking-root-yegakn31/root-preflight.json`。3.11/3.12运行时与新CI尚无证据。

## 单一产品与计算口径

- `set_factor_tracked` 的用户意图只有因子ID和布尔值；完整原请求另含必要的command、head、操作者及Serving代保护，网页不提交路径、SQL、来源承诺或任意策略。加入前简短中文确认，取消与加入均经原命令恢复。跟踪独立于左栏未提交的单次检验参数。
- 后台固定策略 v1：全市场（剔除北交所、ST）、每日、5组、RankIC、无运行后中性化；定义表达式自身的行业/市值算子仍要求真实上下文。策略说明放Tip。固定所加入的定义版本；当前head更新/归档时暂停，重新加入捕获新head并开新段，不把旧版本累计拼入新版本。
- 初次用已核验配置的实际可评价历史建立段，记录实际累计起日；没有20日时保留不足覆盖。后续只计算新成熟评价日和必要时序预热，复用原 plan→ledger→worker、逐日统计与已验证产物，不二次实现表达式、IC、分组或收益数学。
- 昨日指标采用最新已成熟的实际SSE交易日，带实际日期/更新时间，过旧或等待来源时如实显示。近20日窗口是最后20个成熟交易日期，不挑选20个非空值绕过缺日；IR沿现有均值/样本标准差，不额外年化。IC已在原计算中按定义方向签名，不再乘一次方向。
- 昨日多空为方向排序后的两端组期间收益差；近一周取最近5个成熟交易日期，两端组各自依原顺序复利后相减；累计沿实际跟踪段起日折叠。复用原partial/gap规则，缺失不补零，不能直接拼接不同窗口的累计数值。日期不足、样本不足、零方差或数值不可用都有明确覆盖与缺因。
- “失效”仅在最近20个成熟日期的IC全部有效、每天横截面complete且方向对齐平均IC≤0时成立；不足20日或partial不猜测失效。短原因和实际日期来自后台，同代总览跳至确切因子的跟踪面板。此项是研究诊断，不能表述为交易建议或实盘收益。

## 资产、信任边界与必须保持的不变量

1. **跟踪权威**：独立研究状态库保存集合、段版本、原命令/回执及游标；固定库实例/文件身份，不迁移生产主库。可信服务核对registry/head和显式跟踪allowlist；Web只能经既有风格的私有准入→PageControl写入，默认关闭，双端身份/权限和CSRF均生效。旧保存、归档、运行路径及默认权限不变。
2. **原操作恢复**：入队前持久化完整原请求；相同ID+actor+payload仅恢复原效果，变更actor/head/bool/代或库实例拒绝，不重新解释为新操作。已受理原命令可在Serving/配置变化后续查，查不到原请求不称成功，也不自动丢弃原意图。
3. **定义与并发**：跟踪段绑定registry、确切定义摘要/版本及策略；更新、取消、归档或重加后，旧运行不能推进当前段或覆盖新状态。原成功贡献/游标原子且幂等提交；重复日期不累加，尾部错误、中断、租约丢失、坏产物不得提交部分成功。只使用现有受信运行恢复机制，不建立通用调度框架。
4. **来源延伸**：每次任务固定新的完整sealed configuration/source/member/必要context配对及完整spec。追加前核验既有日期的因果输入前缀（原始值、时序预热、前日/收益端点、有效成员、所需上下文与影响可用性的字段），来源代身份另存；不能直接比较含全代身份的旧completion哈希。前缀变化或无法证明一致时暂停并提示重新加入重建，不能静默沿用旧贡献后标成功。
5. **完成权威**：仅接受原worker已完整验证的journal/display/completion、日期网格和绑定；提取精确逐日贡献后按同一口径汇总。读取/核验失败时关闭FD、reader、心跳与自有临时文件；旧成功值可保留历史但不能标本次已更新。累计和滚动统计只能由领域模块负责，Web/React不再推算。
6. **有界读取与发布**：沿现有7,000代码、1,024计算日、按日500码和时序缓存约束；超过容量明确拒绝。跟踪集合/Serving小表有显式上限、全组校验和同代绑定。旧Serving缺表为不可用，可信空集合为未跟踪，坏/混代/超限不得回退伪空或零。只读网页不连接主库、不读研究权威库或凭据。
7. **调度与时点**：本地显式CLI/研究调度使用Asia/Shanghai工作日18:40、单实例与coalesce；实际SSE闭市或数据未成熟是等待，不提交错误日期。只有明确配置的可信来源提供者/精确引用能延伸；不读.env、不隐式联网补资料。正常重复tick、进程重启及延迟回补不能重复累计；过去数据修订走前缀变化处理。

## 冻结失败模型与验收

| 失败路径 | 必须出现的行为/证据 |
|---|---|
| 未授权、CSRF缺失、外来UDS身份、伪库/路径/SQL | 拒绝且没有跟踪写入；正常受信加入/取消可通过 |
| 原ID内容/操作者变化，提交后断线、Serving换代或回执未发布 | 原请求续查/重试或明确冲突；不得创建新效果/假成功 |
| 定义更新/归档/取消/重加与旧worker交错 | 当前段不被旧运行推进，版本累计不混合 |
| worker中断、尾部/完成文件I/O、坏journal/源/成员/context | 贡献/游标没有部分提交，旧历史保留且本次失败可辨，资源释放 |
| 新来源只延伸或修订旧因果输入 | 纯延伸只计算新日期；修订暂停/新段重建，不能拼旧累计 |
| 20日/5日缺口、partial、零方差、方向相反 | 覆盖与null诚实，原数学对照一致；仅完整20日满足条件才进失效关注 |
| 18:40重复tick、休市、未成熟收益、重启恢复 | 日期及去重正确，等待不报异常，不新增供应方请求 |
| UI失联、换账号、旧回执晚到、同代投影不匹配 | 完整原操作保留；只能同代、原命令/段匹配后更新按钮和指标 |

高风险对应红到绿或等价失败用例必须实际执行；聚焦从跟踪状态/增量数学、原命令及直接旧依赖开始，不跑全仓。增量与同一来源整段重算逐日IC、组期间贡献、20日/5日/累计汇总及缺因一致，并证明未重算已完成评价日（必要预热除外）。至少含初次/追加、取消与重复、时序因子、动态成员、表达式上下文、历史修订、缺日和反方向样例；独立黄金不调用新增跟踪汇总核。

前端使用唯一Pydantic→OpenAPI→TS来源，中文文案、Tip、键盘、390px、真实空态/错误/恢复和确切因子深链接有实际行为与截图；不新增共享UI框架或伪演示数据。改网页API和前端时执行仓库必要API/Web/build/dist/浏览器门禁一次，复用仍有效覆盖，skip不称通过。清单只增加实际新nodeids，原批准skip字节不变，仅执行必要清单合同门禁。

root在唯一集中终审accept后，复用现有真实原件及只读副本做初次→增量→同源整段对照、记录时间/内存与资源；真实材料不交子代理。真实来源能力不足时记录具体缺口，不能用合成证据替代。正式配置/长期资源/生产安装属于后续验收，不能把这一本地片段称为M3全部完成。

## 写集、收敛和停止条件

后台实现者负责新增 `src/rquant/factor/tracking*.py`、直接PageControl/准入/Serving/Web有类型合同与入口及对应Python测试，必要源前缀读取只允许最小相关适配改动；本计划仅追加自己的实施证据。不得改已验收采集器、依赖锁、生产数据库或部署文件。后台合同冻结后，前端实现者从精确commit接续，仅写页面内跟踪模块、因子入口/API、必要操作槽/CSS和测试、生成OpenAPI/TS及dist。root负责清单、集成和进度记录，不混入来源不明改动。

一次独立终审集中覆盖SPEC、冻结diff、直接依赖/测试及上述失败模型。稳定ID为 `FT-FINAL-*`；原实现者修复、原审查者定向复核，最多3轮，P2仅可复现违反本片不变量/验收才阻断。范围外问题进入backlog；不额外叠加每子任务/两阶段审查、重复全量或自动架构重设计。

排除CSI完整历史、其他C3.2统计、分钟/竞价新来源、正式配置/数据代/最大负载、生产infra/定时器安装、推送、切流和停Streamlit；它们仍在整体goal后续范围，不重复讨论方向。子代理只在指定干净cdx树离线实现，不继续委派，不访问网络、.env、真实凭据/数据或生产，不调用第三方模型或绕过平台控制。未完成/有阻断/权限或写集越界时冻结证据并交root，不能宣称功能已迁移。

## 实施证据：接口冻结（后台仍在实施）

- 实际产品 Codex desktop，原生子任务 `/root/factor_security_collection_impl`，角色 implementer，父 `/root`；指定树 `cdx-factor-tracking`、分支 `cdx/20261002-factor-tracking`，写前实际 HEAD `e38aee99af619ef4b85774bcfa495bf047545b62` 且 clean。身份、环境及命令证据根 `/private/tmp/rquant-tracking-implementation-NKpV3Fag`，未访问外部网络、真实资料、凭据或 `.env`，未委派。
- 冻结用户合同为 `FactorTrackingRequest/OperationResult/Receipt/Panel/Summary`：用户仅选择 factor_id/tracked，原命令、UTC时间、head、Serving代及可选跟踪代提供恢复与并发保护；来源/路径/actor/策略不可由客户端指定。GET `/api/v1/factors/{factor_id}/tracking`，POST `/api/v1/factors/tracking/commands`、`/resume`、`/retry`。开关独立默认关闭，精确原命令恢复先于新的 Serving/配置预检。
- 新14个唯一聚焦节点有效绿证据：领域7、可信 PageControl3、Web3、真实合成 UDS1；缺实现红测为 `domain-missing-red`、`control-red`、`web-contract-feature-red`。各 log/JSON/JUnit 原件在上述证据根。Web私有证明夹具曾被 `/private/tmp` 可写父目录真实拒绝，改为本树内自有0700临时目录及0400合成证明，未改变平台/产品权限校验，测试后自动移除。UDS listener 已 shutdown/join/server_close，socket 与自有目录删除均有断言。
- 本次接口提交只供前端并行接续；增量计划/前缀/贡献提交、完整 Serving 生产接线及 CLI 尚未完成，不据此宣称 C3.3 完成。最终候选仍由 root 统一门禁和一次集中审查。

## 实施证据：后台最终候选

- 接口基线为 `6be8cd72031b3f4c77a16f9ccc5361e6f54f5fed`；最终公开 DTO 字段保持该形状，`basis_label` 如实说明独立 API 回顾归属和累计实际起日。最终写集共23文件：15个直接后台源码、7个测试/夹具及本计划，精确清单和 SHA 在证据根 `file-hashes.json`；角色、父任务、branch/base 和初始 clean 来源沿 `identity.json`。全部资料为本任务自有合成资料。
- `FactorTrackingRunner(root, reference, tracking_identity, clock=UTCnow)` 的 `run_history(factor_id, target_end=None)` 返回 `FactorTrackingRunOutcome`；`due_tick()` 返回逐因子 outcome。可信状态库的 `days(factor_id)` 读取该段精确逐日贡献。`tracking_runs` 保存规范 payload/digest，公开 `FactorTrackingRun` 字段为 run_id、segment_id、factor_id、root、configuration_reference、original_cursor、plan、prefix_spec、prefix、status、committed_at；原 job 按完整冻结 plan 的原 command/spec 在被捕获的 ledger 恢复，outcome 提供 job_id，不凭当前配置重编译。全部字段/默认/方法与行号另存 `public-api.json`。
- 增量复用原 compile→ledger→worker→完整产物核验。因果前缀包含实际特征/缺值、预热、成员、必要上下文及收益端点，完整来源代另存冻结 spec；封存来源仅延伸时保留旧贡献且只评价新日期，修订旧输入暂停。来源 reader 和 configuration 的自然尾部先结束，之后在同一私有 SQLite 事务内复核输入/原 ledger 完成/当前定义及段游标，再原子提交日贡献、游标和原 run 状态；中断回滚，重试恢复原成功 job。
- root 已明确批准共享入口最小补充：`job_ledger.claim(job_id=None)` 与 `job_worker.run_one_factor_job(job_id=None)`；授权证据为 `/private/tmp/rquant-factor-tracking-root-yegakn31/shared-worker-entry-addendum.json`。默认 None 沿旧队列语义，显式 ID 不存在/不可领取/取消时不消费其他 job；对应新增红→绿及旧默认 claim 节点均有实际证据。
- 独立合成日收益/排序/组均值黄金与同源整段原 worker 对照：初次18评价日、追加11评价日、整段29日，动态历史成员和时序预热均一致，追加 spec 不含已完成评价日；定义 DSL 的行业上下文仍生效，运行后 mode 固定 none。真实私有 UDS→PageControl→targeted worker→完整产物→同代 Serving/Web/总览深链接可运行，更新/归档投影暂停、取消重加旧任务栅栏、原命令失联恢复、并发预留、事务中断及来源/完成自然尾部失败均有聚焦证据。
- 精确新增30个唯一 nodeid 在 `new-nodeids.txt`，直接旧4个在 `old-nodeids.txt`；`test-evidence.json` 按 testcase 去重，`command-results.json` 保留每次真实 argv、UTC时刻、退出码、时长、JUnit 和失败/skip，不把部分失败命令称为全绿。缺实现/显式 ID/typed 状态/自然尾部等实际红测原件保留。最终 `final-affected-regression` 为15 passed、pytest 16.95s、实耗17.652453084s、exit0：当前 runner 10、新 Serving/typed 2、旧 configuration/worker/none canonical 3；旧默认 claim 以及未失效接口/入口证据直接复用，不重跑全仓。
- 清洁离线环境禁 dotenv，显式本树 PYTHONPATH；只读借用指定 integration Python3.13.12，Pydantic2.13.1/APScheduler3.11.2/DuckDB1.5.2/pytest9.0.3。22个实际改动 Python 文件 Ruff check、format check 和 diff check exit0，3.11 grammar 实际接受；没有3.11/3.12运行测试或新 CI 证据。
- `resources.json` 明确保留约305MiB自有合成夹具/日志供唯一终审；所有自有 tool/子进程命令已结束，两条 UDS 的 shutdown/join/server_close/unlink、并发 executor 退出有有效断言，本树临时 Web 私有证明目录已删除，证据根未留 socket。尝试 `/bin/ps` 被 sandbox 拒绝，`final-evidence` exit1 保留，不作 OS 整体零进程声明；按 root 指示不追加枚举/权限尝试，`final-evidence-corrected` exit0 保存现有资源收据。未启动常驻调度器或生产服务。
- 显式 `python -m rquant.factor.tracking_entry` 提供 initialize/run-history/due/schedule/snapshot/serve；可信新 sealed reference 显式更换，pending 原 run 仍使用原 reference。历史 run 不伪造当前时钟；18:40 due 需实际当日 SSE/成熟就绪，过旧 calendar 进入真实 waiting。当前段固定代码域，代码域变化或全因果前缀超过原1024计算日预算时暂停/重新加入，不混接或隐式扩容；7000代码、单日500查询预算保持。
- 本候选只完成后台停止点；真实32日验收、前端组合 API/Web/build/dist/浏览器/清单门禁及唯一集中终审由 root 后续执行，尚未据合成证据宣称真实长期来源/正式18:40服务或整个 M3 完成。未改采集器、旧计算数学、依赖、生成合同/前端、测试清单或生产/部署。

## 实施证据：FT-FINAL 第1轮定向修复候选

- 原作者身份/角色/父任务不变；写前实际核对本树分支及 clean HEAD `a6562da5cb2ae1c16b0290e3b57aefc323c3c371`。证据根 `/private/tmp/rquant-tracking-repair-1-c7tEmVgk` 的 `identity.json` 保留原集中报告 SHA `aa3836b30a91f25a66e044a15a9618e1e410e02407d1864f472e35ac5e1b3a4a`、两原反例/JSON 的来源与摘要；没有改动原55行冻结范围。以下为同一第1/3轮修复，待原审查者仅复核01/02/03。
- FT-FINAL-01：在原子 reserve/commit 核对当前 paused；旧 `_message` 返回并保留已有暂停/原因。确定性交错中 A 完成旧配置 prepare 后，B 实际读到已修订前缀并持久化暂停；预留和提交两个边界均拒绝 A 继续推进原段。
- FT-FINAL-02：统一可信贡献读取按同段、同定义/registry 的 committed `tracking_runs.plan.spec.adapter_request.evaluation_days` 和原游标链核对准确日期网格；Store/Serving 读取及推进前后均核验，缺失非首尾日期拒绝，不用旧日期补20/5日，不更改IC/分组/收益数学。新增反例在完整worker核验后、贡献事务前实际删除中间行，调用不推进游标，后续读取/投影均拒绝。
- FT-FINAL-03：原 registry reader 的自然退出位于 tracking 事务仍可回滚的区域；同一事务临时以 `mode=ro` ATTACH 读取原 registry，保留 SQLite 读锁直至 tracking COMMIT/ROLLBACK，未改两库 schema、registry 源码或公共DTO。真实私有 registry rename＋同字节新 inode 替换触发原自然身份检查，贡献/游标回滚；恢复原 inode 后续原 job 成功，提交前另一真实 SQLite 写者的 COMMIT 被读锁拒绝。
- 四个新增 testcase 在 `focused-red.xml` 实际4 failed/10.30s；`focused-green-corrected.xml` 实际4 passed/9.82s、实耗10.3274874169s、exit0。首个 green/格式命令因本地嵌套缩进编辑错误失败，原日志/XML保留；只修正该编辑错误，未放宽反例/校验。十个精确直接旧节点（增量黄金、暂停、原任务恢复、取消重加、来源/完成/自然配置尾部、中断、并发预留、私有至Serving链）在 `direct-regression.xml` 实际10 passed/16.56s、实耗17.1142381660s、exit0；没有重跑BE/API/浏览器全套。
- 指定只读 Python3.13.12、清洁禁dotenv dummy环境；最终源码 Ruff/format/diff 与3.11 grammar证据及五文件 SHA 另存本轮证据。实际 `python -m rquant.cli web-openapi` exit0，完整原公开 schema 摘要仍为 `bd5ec989b1da2131097928ccf69e09d9ab0c5b32b6cc85d7805fa1aa4ea331ff`。本轮新增nodeids和直接旧去重、各真实命令结果由证据原件列明，root负责组合清单。
- 自有同步命令已结束；原私有UDS链shutdown/join/unlink以及各测试执行副本归零断言有效，真实SQLite竞争写者rollback/close，原registry reader/附属读锁随本事务退出释放。自有合成夹具与失败日志保留供定向复核，不枚举OS整体进程、不清理他人目录。未访问网络、.env、凭据、真实数据或生产，未委派/安装/合并/推送/部署；真实32日验收仍由root在阻断关闭后执行。
