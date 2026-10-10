# C3.2 补齐：MAD 处理、行业 IC 与因子自相关

## 目标和身份

用户已授权按原型、新前端计划 v2 §4.3 和全部差距持续实施。本片只补单因子检验缺少的 MAD 参数、申万一级行业 IC 和相邻评价期因子自相关。已有九项 IC 统计、衰减、组收益和换手继续使用。独立于固定 v1 每日跟踪策略。

当前 Codex desktop 原生 root 为 orchestrator，父任务为已授权的整体goal。跟踪已ACCEPT、真实21+11与同RO32日整算验收并合入；本片规划基线为干净集成分支 cdx/20260929-factor-source-integration 的 `5f0667d7b0c50f8dc60520ca09a825a7672a70f2`。重用已停止且来源明确的后台树 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-tracking`，新分支 cdx/20261002-factor-diagnostics；后端最终候选后前端重用 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-tracking-react`，新分支 cdx/20261002-factor-diagnostics-react。旧分支commit保留，root仅从本计划精确提交切换干净树，写前再记录实际产品/角色/父任务/branch/base及dirty来源。原生implementer的有界只读提案已返回并停止，无文件修改和测试；尚未开始本片编码。本片按高风险管理，依据是扩展已冻结的 request/spec/journal/full/display 内容摘要与完成合同。一次最终集中独立审查，最多三轮原作者定向修复、原审查者复核。

## 固定产品与数学口径

1. 新公开参数为 mad_multiple: float | None = None（有限正数，None 不处理）以及 extended_statistics: bool = False。新网页运行请求固定开启扩展统计；不增加让用户选择内部诊断开关的流程。旧请求的默认字段必须按 exclude_if 省略，规范字节和 SHA 不变。参数与完整原请求恢复绑定，不能从当前表单推断历史结果。
2. 唯一处理顺序：DSL 表达式值 → 可选 MAD 截尾 → 已有运行后中性化 → 原统计和新统计。MAD 仅由该计算日实际入池且有效的因子值确定阈值；复用原 cs_winsorize 的 median ± multiple × 1.4826 × MAD，包括零 MAD、并列和非有限语义。被排除证券不能影响截尾阈值；方向只沿现有规则签名一次。
3. 开启扩展时，即使 neutralization=none，也选择可信配置已包含的行业来源。按原适配器的真实前一 SSE 交易日读取申万一级标签，与同次处理后因子和收益按代码配对。未配置行业时主检验可继续，行业 IC 明确未生成；局部缺标签、冲突和区间端点待确认保持缺因。损坏或错配的已配置来源、自然尾部失败必须拒绝，不能静默降级成功；无自动联网或今日分类回填。
4. 行业 IC 使用本次 RankIC/NormalIC 方法和原相关核；沿原日程汇总每行业 IC 与有效期数/样本覆盖，最多原 SW2021 一级目录允许的类别数。少于两对或零方差保留空值与原因。整池原 IC 不受分行业统计影响，不能先行业均值后伪装为整池 IC。
5. 自相关为相邻 evaluation_days 中共同有效成员的处理后因子 Rank 相关；平均秩处理并列，严格保留相邻日期。第一期、无交集、样本不足、零方差和数值不可用为空值；不能跳过中间无效日期，也不依赖收益是否有值。调仓周期沿评价序列，页面称“相邻评价期”，不改成每日滚动。
6. 扩展诊断由领域有类型结果保存行业可用性、标签/样本覆盖/IC 摘要与 ≤1024 个自相关点。API 和网页只展示，不重新计算。处理参数、方法、实际日期、原 source/member/context 和 definition/job/spec 的绑定进入 journal、full/display 与后续 Serving。

## 冻结资产、失败路径和不变量

| 边界/失败路径 | 必须保持的行为 |
| --- | --- |
| MAD/扩展开关被改、同命令不同参数、失联后当前表单变化 | 冻结完整原请求/spec；只恢复原操作，不重编译到新参数或来源 |
| 旧请求、spec、配置、v1/v2 full/display 没有新字段 | 原规范字节/SHA 和验证仍成立；没有伪造旧诊断或强制迁移 |
| 行业来源换日、错配 snapshot/binding/SHA、端点待确认或损坏 | 真正缺分类保留未知；错配/损坏拒绝，不用今天分类或静默跳过校验 |
| 入池变化、极端因子、缺收益、并列/零 MAD 或退化相关 | 独立黄金与真实覆盖一致；不补零，不用被排除成员求阈值，不重复方向 |
| 相邻评价日期无共同有效成员或中间日期全空 | 日期网格完整，空值断点明确；上一期缓存仍被当前期取代 |
| journal 新字段漏写/错写、尾部/重放失败 | 原件重放重算新统计及绑定，未完整消费不得发布成功或部分显示 |
| 请求或产物超容量、额外来源读取 | 保持7000码/1024日/单查询500码、现有AST预算和4MiB display上限；只缓存当前行业批与上一评价期因子向量 |
| 新网页改参/换代/刷新/失联及旧结果 | 原请求恢复和精确结果确认继续成立；历史结果说明本次实际处理与统计可用性 |

原权限、peer/UDS、CSRF、actor、registry/ledger、lease、原子产物和命令权威沿已验收路径，不新增权限系统或存储框架。P0/P1 阻断；P2 仅可复现违反本片验收或不变量时阻断。范围限新 diff、直接依赖和上述模型，稳定 finding ID 为 FEX-FINAL-*。

## 写集与执行顺序

1. 后台原生 implementer：src/rquant/factor/ 下 request/plan/formula/adapter/daily statistics/runner/journal-artifact/result-serving 的最小直接修改及新领域模块；必要 typed Web result/projection，相关 Python 测试和计划证据。不改配置 schema、数据库 schema、鉴权/lease/recovery、采集器、依赖锁、测试清单、web/ 或 deploy/。先冻公共参数及诊断 DTO；后端最终候选完成后前端从精确commit接续编码；实质改变上述数学或失败模型时先交 root 更新范围。
2. 前端原生 implementer：从冻结后台精确 commit 接 OpenAPI→TS 单一类型源；原左栏增加“离群值处理”及选中后的 MAD 倍数（初值3，说明放 Tip），原结果面增加行业 IC 和因子自相关。确认与原请求恢复、历史结果参数、中文缺因、桌面/390px/键盘和加载/空态一起接通；复用原图表组件，不引入新框架。生成合同和 dist 归前端。
3. root：精确新 nodeid 的清单增量及两必要清单门禁、完整候选冻结、一次集中独立审查、真实材料只读对照、进度/changelog 和本地集成。没有每子任务审查或第二次 SPEC 审查；未失效旧证据复用。

## 可验证验收

- 实际红→绿：独立 MAD 手算/排序黄金（异常值、并列、零 MAD、缺值、入池变化），复用原算子并确认全部统计使用同一处理后向量。
- 相邻日期独立平均秩相关：动态交集、全空中期、缺收益但因子有效、首期和退化；不重复扫描来源且缓存有界。
- 已配置行业 + none，缺行业 + none，局部未知、真实前日标签切换、source/context错配；行业摘要/有效样本与独立分组相关一致。
- 实际合成原件→trusted request/plan/ledger/worker→journal/full/display重放→同代Serving/Web；损坏、未完成/自然尾部失败没有成功发布。
- 旧 request/spec/config/full/display 原规范字节与 SHA；关闭新字段保持旧分支。固定 v1 tracking 无 MAD/扩展，直接依赖回归按实际 diff 执行，不重跑已通过且未失效证据。
- 前端合同改动触发仓库必需 OpenAPI、Web/API、build/size/dist 和浏览器门禁一次；修复仅重跑失效范围。配置 skip 不算通过，不执行全19054清单；root只正常收集新增节点并运行必要两门禁。
- 最终 accept 后 root 使用已经取得的真实原件与只读副本做新增数学/覆盖对照，不向子代理提供真实资料。不重新采集行业，不声明历史 PIT、最大负载或生产安装。

## 排除和停止

CSI 完整历史、分钟/竞价新来源、正式配置/数据代、最大负载与生产安装仍在整体 goal 后续范围。本片不更改固定跟踪策略、生产数据库、调度/基础设施，不 push/tag/部署/切流或停 Streamlit。子代理仅在指定干净树离线工作、不继续委派、不读网络/.env/凭据/真实资料/生产，不使用第三方工具或绕过平台控制。

完成相关证据后冻结干净候选并停止写入，交唯一独立 reviewer。范围外问题记后续；到修复上限冻结证据，不自动架构复盘。整体 M3 在正式来源与上线验收齐备前保持部分。

## 实施证据：后台候选

- 实际产品 Codex desktop，原生子任务 `/root/factor_security_collection_impl`、implementer、父 `/root`；写前核对 `cdx/20261002-factor-diagnostics`、clean HEAD `6ac9f6407df432c1426a7a894dfa3ff5aac80e76`，本树实际导入、冻结53行 SHA 和借用运行时在 `/private/tmp/rquant-diagnostics-implementation-1aMv2kPF/identity.json`。未写另一工作树、访问网络/.env/真实资料/凭据/生产或继续委派。
- 公开 `FactorRunParameters` 新增 mad_multiple（None关闭/有限正数）和 extended_statistics（False旧行为），省略默认；原命令保存完整参数。处理严格为表达式→原 MAD 核→原中性化。原 AST 2,000,000 槽预算计入 MAD 工作向量；固定 tracking 不开启两新参数。配置/schema/ledger/lease/recovery、原计算数学、采集器及生产/前端生成物不改。
- 扩展开启时从原可信配置选择已配对行业来源，即使运行后none；未配置时主统计成功、行业诊断明确未生成。行业事实仍按真实前一 SSE 日期/全计算代码读取，每查询最多500；journal包含评价日原行业批，绑定原context SHA/日期/代码域与来源完成回执。只在评价日缓存当前行业批，消费后释放；自相关只保留上一评价期有效因子向量，不因缺收益删除因子或跳空期。自然尾部、错配、损坏和重放失败不发布成功。
- 领域 `FactorExtendedStatistics` 保存本次 IC 方法、行业状态/短中文原因、最多31个行业IC摘要（复用原相关/摘要核）、最多1024个原日期覆盖日和相邻评价期平均秩自相关点。v2 display/Serving/Web可信投影来自原产物及spec，公开形状和源码行号在 `public-api.json`；没有浏览器source/path/actor输入、新存储框架或新schema版本。
- 聚焦实际 `domain-red.xml` 为10 failed/6 passed、1.29s；`worker-red.xml` 为4 failed/2 passed、8.08s，其中两个夹具失败（原周期仅一期、错误外层journal绑定）保留并修正，没有把失败命令称绿。`domain-green` 16 passed/1.33s、`domain-final` 20 passed/1.28s、`worker-green` 6 passed/8.79s；最终 `final-new.xml` 26 passed/8.78s、实耗9.4947793330s，`last-affected.xml` 3 passed/5.70s、实耗6.4290992080s，后者含1新恢复节点及2原新增节点强化缓存释放。去重为27新节点；`direct-regression.xml` 15旧节点 passed/11.19s、实耗11.8245120420s，含旧canonical、独立原统计、原模式、原命令恢复、journal拒绝及固定tracking。精确nodeids/XML/每条argv、UTC、exit/time与失败日志由本证据根保留，不重跑全BE/API/Web/browser或清单。
- 实现前用原None合成worker捕获request/spec/config及v1/v2 full/display共7份黄金。候选全部重载规范字节/SHA完全不变，两个display原投影也相等；`legacy-golden-sha.json`、`legacy-verification.json`保留准确摘要。捕获脚本函数名与v1直接JSON解码误用的原failed日志保留，修正为既有v1 loader。源尾部用自有行业文件同字节换inode实际失败，重算摘要/自相关和坏panel字节的原件校验拒绝；没有从这些合成证据声称真实资料已验收。
- 只读借用Python3.13.12（Pydantic2.13.1/pytest9.0.3），清洁隔离dummy环境、显式本树PYTHONPATH、禁dotenv。12个改动Python文件Ruff/format/diff及3.11语法检查的真实收据、13文件SHA在本证据根；仅格式/import排序后的测试证据复用，其他受影响用例实际复跑。不宣称3.11/3.12运行或最大负载证据。
- 所有自有命令已结束，没有常驻服务/调度器或tool session。自有Web合成证明目录自动移除有断言；执行副本、行业reader临时目录归零，当前行业对象weakref释放、单次消费与成功/失败缓存close均有断言。自有合成夹具/产物/失败日志保留给唯一组合终审；不枚举或清理其他进程/目录。后台干净commit后停写，前端/root负责组合门禁、唯一独立审查和accept后的真实只读对照。

## 实施证据：前端与组合门禁

- Codex desktop 原生子任务 `/root/factor_save_react_final_review`，实际 frontend implementer、父 `/root`；复用授权树 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-tracking-react`，分支 `cdx/20261002-factor-diagnostics-react`，写前 clean 基线 `072902a49c35def40ff026610aaa598149a866ce`。未写后台/Python tests/manifest/lock/共享UI/权限或访问网络/.env/真实资料/生产，不继续委派。私有0700证据根 `/private/tmp/rquant-factor-diagnostics-react-gr0tgnew` 保存实际 argv、UTC、wall、exit、JUnit/nodeids、SHA和失败日志。
- 原左栏增加离群值处理，启用MAD默认3倍并校验正有限数；新Web请求固定extended_statistics=True，关闭MAD省略参数。确认、持久化和失联恢复保留完整原请求，旧省略字段请求原样恢复。结果从自身可信DTO读取MAD/IC方法、行业摘要/覆盖和相邻期自相关，不套当前可编辑表单、不重算统计，保留31行业、空点断线和旧结果诚实空状态。
- 首轮19项红测18 failed/1旧兼容 passed；图表/小倍数精度3 selected failed/19 deselected，均保留exit1。最终22新增节点绿、8个直接受影响文件138 passed；首次类型exit2及修正后exit0保留。两旧拒绝fixture字节不变，新增独立fixture原样采用后台真实合成factory→UDS→Web且extended_statistics=True捕获，3个既有拒绝恢复节点绿，来源/SHA在fixture-adoption.json。
- 完整当前Pydantic CLI→OpenAPI→TS exit0；OpenAPI SHA `05fa95e688e9af55c1216c46eecc6cc6f991d2e99ad6a3e86d782994d29768a8`，TS SHA `e3bce1dcca3d8108c2941b39a447658da8654fd46d712bfd3e13b8d213414617`。实际Node22.22.2/pnpm10.33.0/Python3.13.12/Pydantic2.13.1/pytest9.0.3，当前src的显式PYTHONPATH、禁dotenv和私有dummy环境；唯一完整check57文件625 passed/31.831s，55个Web API文件993 passed/270.907s、0skip。前端新增Python/API节点0，未执行全Python套件，不宣称3.11/3.12运行证据。
- 完整e2e首轮实际exit1/209.538s：118 passed、1新desktop节点因退场Tip的全局严格定位失败、4原screenshots skip；原XML/trace保留。只修为当前真实文案定位，保留键盘/触摸/内容断言。真实390像素行业刻度碰叠经root确认只改该行业轴hideOverlap，保留原精度/Tip/表格和共享图表；受影响图表2文件7 passed、type/精确Biome绿。只补2个新增desktop/phone节点，2 passed/11.165s；去重有效119 passed+4skip，不称首轮命令全绿。四张1440/390确认/结果原图实际目视，绝对路径/尺寸/SHA在screenshots.json；手机实际touch、正文术语和页面溢出断言绿。此为合成UI边界证据，不是实际worker或生产验收。
- build/size首轮exit0；局部轴源码变化后的必要build4.852s、size和最终verify:dist1.157s均exit0，生成dist与已提交快照一致。首屏gzip323.2KB<550KB、因子按需23.1KB；未重复625/API/完整浏览器。源码候选 `bfc494e33b9e9307c27e6966c01d3776796bf844`，最终附录提交SHA在handoff.json；root在组合候选安排唯一集中独立审查。
- 自有命令全部结束，18884/14284实际可重新bind，记录的命令PID无活动；自有0400 proxy-proof已精确unlink，不读内容/改权限，root的26依赖入口保留。独占原生basetemp `/private/var/folders/d8/tklkkr6x3gl8y8drmfrnmztm0000gp/T/rqd-api-kchxyn5v` 首次清理遇合成发布generation只读目录EACCES，失败记录保留；剩895文件/3,564,559,120字节按root明确授权作为合成门禁夹具保留，不改变保护或清理未知目录。resources.json记录归属和关闭状态，其他合成Serving/日志/截图保留且无自有常驻服务。

## 根任务本地验收（2026-10-02）

源码候选 `e8888488c13694b5a7cf640cfcbbf2abda2fd201` 已快进合入 `cdx/20260929-factor-source-integration`。唯一集中独立终审 ACCEPT，FEX 阻断项0、产品修复轮0；原审查报告 `/private/tmp/rquant-c32-final-review-78gx64au/review.md`，SHA256 `c165be36c38119544b344226012f448b88f9c01a077a309b263dbf1ee5b69b13`。原计划首53行保持冻结；本段只记录证据，不改数学或运行合同。

后台27新增/15直接旧、前端625、Web API993、浏览器119唯一有效通过/4既定跳过及构建、类型、大小、dist证据有效；初始浏览器exit1与局部补验原件保留。清单19,054→19,081只增加本片27项，55批准跳过字节不变；两必要门禁2通过/8.571s，没有执行全清单或再次运行未失效套件。实际本地Python3.13.12、云端3.14.4，不声称3.11/3.12 runtime或新CI通过。

root使用已取得的成员/行业原件及同一约10.0GiB只读副本，在自有远端tmp执行全市场、行业中性化、MAD3、RankIC与扩展开启的32日任务（08-14—09-29，09-30收益尾部；5571计算代码）。160,462个因子值及缺因逐项核对：160,251有效、205缺观察、6缺行业上下文；160,220有效配对，18,169原始有效因子被截尾。31行业的248项摘要数值、覆盖/缺因/有效期计数及32相邻期自相关点一致，主RankIC和五组收益也通过独立对照；full/display诊断完全相等。因子值最大误差7.105e-15。

独立参考只读取保留的原始值、成员与行业区间，不导入产品计算模块。行业哑变量OLS使用有理数精确行业均值，按已声明浮点口径将均值舍入一次后求残差；160,251值与独立NumPy矩阵OLS交叉核对，最大误差1.421e-13。原矩阵参考完整保留，未放宽因子2e-8/统计2e-10门槛；旧p值核未单独重算，不能将248项称九项全独立对照。

真实过程共3次同范围任务；前两次SSH为0、对照wrapper为1，不算验收通过。第一次root脚本误要求可用行业的说明为空，遗漏6个未知标签应有的说明；第二次矩阵OLS约1e-14残差误差改变08-17两个并列排名，RankIC相差约5e-8。仅修root参考/判断，产品候选未改、没有追加审查。最终对照exit0：worker260.940s、含准备/校验304.071s、SSH307.034s，峰值435.051MiB。原失败命令、两份输入包及旧黄金在 `/private/tmp/rquant-factor-tracking-root-yegakn31/c32-real-proof/failed-attempt-1`、`failed-attempt-2`、`independent-matrix-reference` 保留；最终原件和完整接受记录见同root的 `c32-local-acceptance.json`。

最终源/湖FD为0、心跳和私有执行scratch为空，三次远端自有tmp均清理；无新增供应方HTTP或生产写入。审查后已精确清理895文件/3,564,559,120字节的自有合成API基目录，日志/XML/截图保留。后台临时树 `cdx-factor-tracking` 已在集成祖先覆盖、审查/实测通过且停写后移除；仅有两份已核对缓存，没有未知WIP/锁/嵌套仓库，分支引用保留。继续复用 `cdx-factor-tracking-react` 作为下一片唯一实现树；集成树保留作目标分支及已知运行时，未清理其他会话工作树。

本片为本地验收，入口默认关闭；CSI完整历史、每日指标与已有选股特征/分钟/竞价等来源、正式配置/数据代、完整资源及生产安装继续完成。下一片先接同RO已存的12项每日指标和4项daily_basic事实，再续已有选股特征，不以原始价DSL近似替换复权指标。保持回溯研究，不声称PIT、最大负载或M3完整上线；没有push/tag/生产部署、切流或停Streamlit。
