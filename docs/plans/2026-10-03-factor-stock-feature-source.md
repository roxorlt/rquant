# 因子日线选股特征来源实施计划

**执行者：Codex 原生 implementer；根任务负责范围、委派和验收，最终候选一次独立审查。**

**目标：** CC v2 §4.3 已有选股特征中的日线部分接入因子定义、可信检验、结果和持续跟踪；随后接分钟、竞价、温度和 VP。整体 M3 仍部分。

**架构：** 从配对的只读副本封存足够的日线和复权历史，独立选股特征 producer 复用 `stock_features.py` 的单一计算核，沿现有有类型 daily-feature 读取、formula、任务及跟踪路径消费。允许为共享复用抽出等价的纯历史帧入口；旧 store 包装和计算口径保持。网页不计算特征。

**技术：** 现有 Python / Pydantic / DuckDB / Parquet / SQLite、React / TypeScript / Vite，不增加依赖。

## 范围、身份与验收边界

- 普通任务：新增事后研究来源和用户可见字段，未涉及生产数据写入、权限或并发/恢复规则变更。原技术来源已在 `1b381b005acaf92157b0a63a2582eb60fc7d4a29` 完成收尾；本片不是其额外修复轮。
- 所属当前 root goal；唯一实现树 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-tracking-react`，分支 `cdx/20261003-factor-stock-source`，干净基准为上述 commit。集成树保留供验收后快进；无新 worktree。
- 只修改日线特征单一核、独立来源 producer、直接来源/合同/能力/消费接线、必要 API 生成合同及因子页。不得修改 ledger、lease、tracking commit/recovery、generic artifact 校验、权限、生产部署或旧 Streamlit。
- 现有原六字段默认 JSON/摘要及 v1/v2 来源/任务黄金保持；新增能力只由真实配对的选股来源开启。来源缺失、错配或损坏拒绝新字段，不回退库存或重新连生产主库。能力目录不承诺实际覆盖或 PIT。
- root 明确追加：`stream_adapter.py` 字段宽度和 `tracking_backend.py` 准入目录是直接依赖，允许修改。v3 独立选股来源可通过 `FactorStockFeaturePrepareRequest.base_daily_source` 组合已经验证的 v1/v2 日线来源；严格绑定同一 prepared/scope/generation，复用封存原值，以实际 fields 开启目录。支持技术/基本16项与选股23项的同一次混合表达式；窗口诊断与递归初始化分别保留，不重算或虚构旧16项。

## 1. 冻结现有 23 字段语义，先红后绿

文件：`src/rquant/stock_features.py`、`src/rquant/price_adjustment.py`（只读参照）、新 `tests/unit/test_factor_stock_feature_source.py`；相关旧 `tests/unit/test_stock_features.py` / `test_price_adjustment.py`。

- 90/120/250 观察窗口各五字段：`price_window_days_{N}d`、`price_position_{N}d_pct`、`price_rank_{N}d_pct`、`distance_to_high_{N}d_pct`、`distance_to_low_{N}d_pct`，共15项。窗口含参考日；按每个窗口实际观察日期和参考日完整正复权因子调整；短窗口沿用原核实际观察数，不凭空要求满 N 日。平价位置50，排名包含并列 `<=`，价格/距离百分数不二次乘100。
- 20日吸筹六项：`accum_window_days_20d`、`accum_obv_change_20d_pct`、`accum_ad_flow_20d_pct`、`accum_up_down_amount_ratio_20d`、`accum_heavy_no_drop_days_20d`、`accum_close_position_avg_20d_pct`。排除参考日；OBV 要完整窗口/参考日价格基准；其余项保留原核的价格尺度不变口径、原 vol/amount/pct_chg 缺值处理及零分母/空窗口结果。
- `ma_alignment`：满60观察、5>10>20>60严格排列，0/1；`price_percentile_250d`：满250观察、包含并列、0–1比例。保持四位小数、原有每窗口诊断、缺因与无日线语义，不套用技术指标的递归初始化/断裂策略。
- 红测应精确证明新来源/目录或消费能力缺失，随后复用原核得到23项数值与字段级来源/缺因；验证复权跨界、缺参考因子/窗口因子、短窗口、平价/并列、参考日排除、未来尾部无影响及尺度不变项仍可用。不得通过改旧算法来匹配新测试。

## 2. 封存来源并接原路径

创建 `src/rquant/factor/stock_feature_source.py`；按必要修改 `daily_feature_source.py`、`capability.py`、`formula_stream.py`、`run_configuration.py`、`run_entry.py`、`run_backend.py`、`stream_job_artifact.py`、`tracking_runner.py` 及显式 CLI。确需新增直接调用文件先报告，不开展通用框架重构。

- 单一固定 RO 事务、原副本代/范围配对；只保留每个参考日所需真实历史（最多250观察及参考因子），无未来输入。独立 typed receipt 包含来源、具体窗口策略、原件完整性及字段覆盖/缺因；新 schema/字段以追加方式保留旧缺省序列化。
- 沿现有500码查询、有界单证券帧、输入/输出容量门槛；不能物化全市场全期矩阵或每字段重复读取。先测合理的小规模合成准备/热查询与资源关闭，保留实际耗时/峰值记录；首次全校验与每次原件身份/字节完整性沿现有严格约束，不能重现重复全历史逻辑扫描的问题。
- 同一来源消费23字段的公式、时序预热、产物、结果来源/覆盖和 replay；在保存定义的目录与可信运行配置中启用。新增字段跟踪准入、因果前缀使用实际消费值/状态/诊断和窗口策略；不把未来尾部、整份包摘要、generation/commit 混入历史日的逻辑前缀。原增量/整算、修订暂停、重复与取消规则复用。
- 每命令记录 argv/UTC/exit/wall/log/XML、实际不同 nodeids 与源码 SHA；聚焦新源/消费和直接旧回归，旧有效结果可复用。不得运行全仓测试或另开逐步审查。

## 3. 顺序接 React 与必要门禁

后台冻结并停写后，同树顺序接 `web/src/pages/factors/` 的真实中文字段目录、搜索/插入、单位与窗口说明、来源/覆盖和缺因；复用 Tip 的悬停/聚焦/手机点按。有技术来源与选股来源的结果分别显示自身真实口径，缺包不显示可用，正文无内部 ID/英文实现词。Pydantic → OpenAPI → TypeScript 生成，不手写接口类型；前端不重复计算。

前端执行仓库必要 check/build/size/verify-dist、直接 Web API 合同与桌面/390px交互（含键盘、恢复/换代和各来源缺态）。复用无影响的旧证据，保留所有失败、skip 和实际截图。最终更新精确清单及两项必要门禁，不冒称全清单执行。

## 4. 一次终审与真实收尾

合并候选冻结 commit/diff/SHA 后，独立 reviewer 一次集中覆盖上述语义、直接消费和 UI；普通任务最多一次原作者定向修复与原 reviewer 复核，范围外进入 backlog。

根任务以最新日期倒序选择实际固定RO和原成员档案，独立标准库参考读取所需原始日线/复权（不得调用产品核构造预期），逐值核验23字段、缺因和窗口诊断，再验证含新字段表达式的首1日＋续2日与整3日贡献/汇总/前缀精确相等、重复与取消。保留实际资源与清理证据；真实结果就绪才本地快进集成并更新 CHANGELOG/进度/实施顺序。正式最大负载、生产配置/18:40、切流与停服沿独立授权边界继续；没有全 M3 完成声明。

## 根任务最终验收（2026-10-03）

候选 `7a6333dc3567c8040fa3971bbe2c7ac0ceb0c6c5` 已本地合入。23项日线选股特征及与原16项混合的检验、结果和持续跟踪已接通。5,571码×6日×23字段共768,798格数值、缺因和窗口诊断与独立原始参考一致；首1日＋续2日与整3日贡献、汇总和完整逻辑前缀精确相等，14,766有效/301明确缺失，最大表达式误差1.78e-15。重复不追加，取消保留已有历史。原60分钟命令超时，使用保存的来源和原任务公开恢复接口完成续验；这项功能验收不代表冷启动耗时或最大负载通过。分钟/竞价/温度/VP、CSI完整历史、正式配置与数据代、18:40安装及完整资源/生产验收继续实施；M3保持部分。

- 原计算核、复权口径、观察窗口与参考日排除规则保持；选股来源可单独使用或与原16项组合，网页只展示后台事实。单一独立终审与原审查者定向复核ACCEPT；普通一轮补修及用户另准的一轮限定诊断/补修均已使用，未追加产品修改或审查轮次。
- FSS-REAL-01修复仅压缩未发布v3表示，完整重建每码来源证据；旧v1/v2规范字节、原2MiB规格门槛、算法和来源校验保持。7,000码/234范围日/233公式日/39字段加完整中性化上下文的离线规格1,883,593B，余213,559B；它是容量形状验证，不是最大计算负载。
- FSS-REAL-02限定补修将完整解码移出短读事务，保留当前完整行/command、规范字节/摘要/类型/身份、token/version、可信时钟、原租约/续租到期及prepared-CAS和产物检查。原30s租约、5s心跳、deadline、journal和恢复机制保持。合成时序证明可复现的阻塞与越界路径，未宣称先前实际失败只有这一种归因；见[限定补修](2026-10-03-factor-stream-lease-boundary-repair.md)。
- 真实验收按最新日期倒序取样：09-22/23/24/28/29/30六开市日；评价09-24/28/29三日。第3代重新捕获的业务记录与第2代精确相同，31份独立原件冻结，标准库参考未调用产品数学核。原RO刷新、参考求和顺序修正、容量拒绝、失租和脚本前置失败证据均保留，未重写为通过。
- 本次原命令3605.350s/exit255在最后整算阶段达到3600s上限；此前768,798格、首日、增量及重复均通过。已封存来源及两个提交完整保留，原整算任务以公开接口回收过期租约，attempts从1到2、原deadline未改，原reservation只提交一次；续验821.185s/exit0，累计两个命令4426.535s。续验重新核对三段产物和完整字段网格，并通过重复、取消和资源归零。
- 冷准备实测1886.946s，其中选股来源1506.860s；原超时进程的完整峰值未取得。续验峰值1636.324MiB只覆盖封存来源复用与原任务恢复，不能补成冷启动峰值。60分钟预算未通过、正式最大负载及完整资源验收仍待完成；后续应据此处理生产运行体验，不能反复重跑同一大任务冒充优化。
- 原来源133个有效节点、本片租约154个有效节点有29个重叠；清单容量3项、必要门禁2项通过。租约新增27节点使清单超过5MiB总上限616B，原失败保留；有限总上限改为6MiB，其余上限、结构/摘要和批准跳过项保持。清单19,227/55仅收集，精确增81（首次25＋容量29＋租约27），无删除/重复、批准skip字节不变，未执行全清单。
- Web677（44最新直接验证、633有效复用）/API999、浏览器127（本片26及未影响101）有效通过，4原skip未计通过；构建及dist证据有效，首屏323.2KB。后台本地Python3.13.12，云端Python3.14.4；没有实际Python3.11CI或历史PIT声明。
- 独立原件、精确命令/UTC/exit、所有失败、续验结果、实际资源及审查绑定见 `/private/tmp/rquant-factor-tracking-root-yegakn31/stock-feature-source-proof-generation3-lease-repair-execution/accepted-real-proof.json`，原60分钟失败记录见 `/private/tmp/rquant-factor-tracking-root-yegakn31/stock-feature-source-proof-generation3-lease-repair-execution/worker-command.json`。远端自有临时目录已清理，读句柄/心跳/执行scratch归零。
- 当前任务实现树和集成树按未完成goal保留；下一片接分钟来源。尚未main/PR/tag/上线、配置18:40、切流或停Streamlit；正式发布与生产基础设施仍按既定授权边界推进。
