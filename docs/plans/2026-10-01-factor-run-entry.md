# 因子研究：可信输入、运行检验与原请求恢复

## 目标与基线

按原型左栏的检验参数与因子列表、右栏的统计与图表，把「运行检验」接到已经验收的 v2 持久任务：用户选择定义、股票池、区间和 1/5/10/20 日调仓周期，确认后提交，刷新或失联后可继续核对同一次操作，下一代发布后查看真实结果。分组和 IC 方法沿用现有结果的 3/5/10 组及 RankIC/NormalIC 单一计算口径。每日跟踪及缺失的行业/市值上下文另行接通，不把本片称作 M3 完成。

基准为干净集成分支 `cdx/20260929-factor-source-integration`，代码及文档 HEAD `c133d2863aa5c5a07ca694095ef54a213edec9df`。上轮 v2 任务、产物和只读结果已实际合入，属于进展；本轮不重写计算器、账本或产物格式。

**分级：高风险。** 本片将已认证用户操作跨私有进程边界送入 PageControl 与因子任务账本，涉及权限、重放恢复和持久身份；必须先冻结本 SPEC，随后实现，最终候选进行一次整体独立审查。

## 输入与领域边界

1. 浏览器请求只包含稳定命令 UUID/时刻、所见数据代、因子 ID/精确 head、股票池、起止日期、调仓周期、分组及 IC 展示方法、中性化 `none`。不接受定义正文、计算代码/SQL、来源摘要、路径、成员名单、任意 deadline 或 actor。服务端根据代理证明确定 actor。
2. 可信写端有显式、默认关闭的运行配置：因子定义仓库及外部捕获的完整身份；已初始化任务账本及外部保存的 `FactorLedgerIdentity`；已准备来源、湖根、成员根及四池中实际存在的归档引用；实际代码 revision 和有界任务时限。配置读写不经过 Web。配置缺失、路径/身份不符或来源不足时，返回短中文原因，不自动创建权威库、扫描主库、回退旧固定名单或连接外部数据服务。
3. 把 `FactorPreparedStreamSource` 的实际输出作为只读、有摘要及文件身份检查的来源包，提供现有 metadata 协议所需的原 snapshot/binding。必须有实际保存/装配入口及单次 worker 运行入口，不能只注入测试中的手写 metadata。来源包中的 `calendar_open_days` 用于构造有界计划，但提交前仍经既有 v2 admission 核对原绑定及实际冻结日历，不能仅相信来源包的声明。只在配置的 research lake 打开冻结事实，Web 进程不导入配置、存储或打开 DuckDB。
4. 工厂从实际成员 manifest 及被冻结日历产生所选区间：按用户区间的第一个有效交易日锚定，每隔所选调仓周期取一个评价日；纳入定义实际历史窗口、每个评价日的前一交易日特征和实际收益窗口。沿用已验收 adapter 的 09:25 决策及复权开收价格口径；区间、warmup、收益尾部或成员日期缺失时明确拒绝，不静默缩短参数或改变周期。计算日单调且最多 1,024，完整范围最多 7,000 股，旧容量不变。需要子区间归档时，仅从已核验归档的实际日文件生成新的规范 manifest，不接受任意 Iterable/外部名单作为准入事实，不把全期行情矩阵载入内存。
5. cutoff 来自冻结来源，成员 observed_at 必须不晚于它。`historical_retrospective` 可使用后来采集的实际历史事实，但不声称当年已采集/PIT、外部完整覆盖或可交易。没有实际逐日成员归档的池子不可用；日/月最新指数名单不能补历史。行业和行业+市值中性化显示不可用及 tooltip 原因，服务端同样拒绝；不得伪造行业诊断。
6. 因子定义从原仓库核对精确 head、内容摘要及未归档状态；冻结后允许原定义版本继续检验，后续保存/归档不悄悄替换它，也不解释为取消已提交任务。参数与所选版本在 PageControl 原命令中持久保存。分组/IC 方法只选择已计算诊断，不重复数值计算。

## 冻结失败模型

- **资产与信任边界**：代理证明的登录身份、独立精确 `factor_run_users` 名单、同站 JSON/CSRF、所见 Serving 因子版本/仓库实例、受控 Unix peer UID、写端捕获的原仓库/账本/来源身份、冻结 v2 spec、PageControl original owned request/claim/effect、ledger 命令锚及 job/spec、后续 Serving 结果。Web 只有类型化私有准入客户端；PageControl 是网页写操作的唯一入口，现有 ledger 是研究任务的唯一权威。
- **失败路径**：伪造头或 actor、非运行用户、公开 TCP/parser/outbox 直投内部运行命令、无 CSRF/跨站/超大请求；Serving A 与写端 B/旧 head/归档；同 ID 异 actor/参数；重试时重新编译到新来源/定义/deadline；原任务已提交但 effect 回执未记录就崩溃；换空库、坏 schema 或 ledger 身份迁移后恢复误建或重复任务；来源/成员被换、缺 warmup/收益尾部、超容量；同次操作的旧账号缓存、保存/归档与运行交错；未决或错误回执被当作计算完成；命令提交顺带 drain 其他待办。
- **不变量**：新提交和可推进的续查均需已认证 actor 与两个进程中相同的独立运行名单；私有 listener 校验固定 Web UID，client 校验 service UID/目录/socket。所有配置默认关闭，缺任一必需身份不开放写入口。新请求在同一个借用 Serving 代核对 head/实例；写端再次核对原仓库身份与 head，只从可信配置构造 v2 spec。owned command 持久绑定完整原请求哈希、actor、registry/ledger 身份、冻结 spec；公开路径拒绝 ownerless/owned run。先做 actor+全原请求的精确 lookup，再考虑首次编译；resume/retry 不依赖旧数据代，不重新基准到新源。只定向执行该命令，使用原账本身份及同 command/spec 做幂等 submit/只读 lookup，崩溃后恢复同 job，不创建或迁移 ledger。已提交只代表入队，只有后续核验到相同 job/spec/定义的 Serving 状态和结果才能显示检验完成。
- **阻断范围**：本片授权、来源绑定、原请求恢复、任务身份、实际参数口径和界面误报；P0/P1 阻断，P2 仅违反本段或验收时阻断。后续仅修原 finding、修复回归和本冻结范围的新证据。最多三轮定向修复/复核，不进行全仓审计或自动架构重设计。
- **排除**：外部历史采集器、行业/市值数据、每天 18:40 跟踪、Lab 调度器重建、取消/重试新任务策略、生产库写入/修复、systemd/nginx/UID 安装、生产开关、发布、切流或停 Streamlit。保留来源/权限缺口并沿持续 goal 后续完成，不把临时合成入口说成线上可用。

## 实施顺序（一次最终候选审查）

1. **可信计划工厂与配置**：不可变来源包的实际保存/读取、运行配置装配及参数编译；复用原 source/admission/member archive/adapter/spec 模块。构造后的 spec 单独可执行，worker 消费同 metadata/lake/member/artifact 根。提供只使用显式配置的本地命令，运行一次 worker，不隐式读取 `.env` 或主库。
2. **PageControl 与私有准入**：新增有类型的 run command/owned variant，复用 outbox effect/目标 claim 与恢复模式；独立后台接口只负责身份、spec 构造、ledger submit/lookup，不计算因子。给 ledger 增加必要的有界只读 command lookup，不改 v1/v2/schema/lease/完成 authority。私有准入可复用已验证因子 Unix transport 的 framing/UID 机制，运行权限独立于定义编辑权限；不可新增通用任意命令或公开写接口。
3. **Web 与合同**：新增因子运行参数可用性、新提交及原请求 resume/retry；代理身份、请求尺寸、CSRF、固定 Serving preflight、合法回执及 job 绑定。OpenAPI→TypeScript 单一类型来源；无权限仍能看既有结果，不能复用前一个账号的写权限。配置/路径/身份只作为后台事实，公开响应为有限状态、日期/选项和简短中文原因。
4. **React 原型交互**：左栏检验参数（四池、日期、周期、分组、IC 方法、中性化）及因子列表，右栏统计/图表；「运行检验」连接同一表单，较重确认框列出实际对象/参数，提交前保存原请求。按钮双击/并发回调只创建一次命令；失联、刷新、切账号、换代及参数修改不丢旧操作。明确“等待检验/检验中/已提交，等待更新/暂未确认/检验失败”，提供原请求刷新/重试；下一代相同结果自动选中已有 `FactorResults`，历史结果仍可看。桌面/390px、键盘、Tip、loading/empty/error、正文内部词门禁均遵守 `web/AGENTS.md`。
5. **直接门禁债务**：已有 Web API gate 的五项失败已在干净基线复现，且本片直接扩展相同因子路由/导入面与认证 route inventory。仅修三因子路由的 DuckDB/存储导入边界、app 的间接存储加载及对应已认证路由清单，不弱化/删除隔离或认证断言，不扩到无关审计。

## 可执行验收及证据

- 小型实际冻结 raw、来源包与成员文件（四池，动态/空池）→ 保存定义 → 真实 PageControl run → ledger queued → 原 worker v2 succeeded → 原投影/下一代 Serving → Web/React 图表。核对所选参数、定义/head、job/spec，数值计算仍单一来源；没有“提交=完成”。没有配置、成员或中性化事实时入口明确不可用。
- 红测覆盖无认证/非运行用户/peer/CSRF/跨站/公开绕过、旧 head/错仓库、同 ID 异 actor/参数、上限/不支持 neutral、丢 warmup/收益尾部/成员、换源/坏配置、入队后换 registry/ledger、ledger commit 后 effect 前崩溃、恢复时坏/空库、来源发布更新后恢复仍是原 spec、原命令定向执行、不重复提交。先红后绿或等价失败用例，测资源关闭而非镜像实现。
- 必要直接回归是原因子 ledger/PageControl/准入/source/member/Serving/result 与 Web settings/security/route inventory。先小样本聚焦；全市场 7,000 的原计算证据复用，不因本片重复 100 秒数值跑。来源计划须实际验证有界规模及清理，不把合成值称作外部来源证明。
- 仓库真实前端/API门禁：`pnpm -C web check/build/verify:dist`、`pnpm -C web e2e`、`pytest tests/unit/test_web_*.py -q`、OpenAPI snapshot；每个最终候选通常一次，定向修复仅重跑实际失效证据。记录 3.12 core/3.13 完整 Web 环境差异及截图 skip；无 3.11/CI/生产证据不得声称通过。
- 最终集成统一更新新增测试固定清单、两项 manifest 合同门禁、CHANGELOG 与进度；不运行无新增理由的 18,790 全量测试。自有进程/socket/线程/执行副本明确清理或处置。最后只在已批准本地特性分支合入，不自行 push/main/tag/发布。

## 委派与停止

根代理冻结本文件并请原生独立 reviewer 一次 SPEC 审查；必要修改只定向处理。合适的原生 implementer 在干净 cdx 工作树内完成以上连贯写集，不读真实凭据、生产或网络、不继续委派。最终完整候选交同一 reviewer 一次整体审查，原 implementer 定向修复，原 reviewer 复核。普通工具/环境错误可直接诊断；缺来源只拒绝对应运行，不伪造数据或扩大生产权限。
