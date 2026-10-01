# 流式因子检验的持久任务与结果发布

**目标：** 将已验收的实际成员文件及冻结 raw v2 输入接入原持久任务账本，完成一次流式检验、独立统计复算、租约内成功提交和轻量 Serving 读取，为原型「运行检验」提供完整后端能力。

**架构：** 显式 v2 jobSpec 与 v2 full/display 使用独立合同；原 ledger/worker 按明确版本分派，继续维护唯一命令、实例身份、租约与 CAS。执行时按日封存实际统计批，心跳运行期间由同一账本实例独立复算并保留私有准备证据，最终短提交窗口只做文件身份、绑定和租约核对。Serving 读取已成功任务的确定性 display，不在轮询时重算。

**技术：** Python 3.11+、Pydantic、SQLite 原账本、既有按日公式/统计/衰减算子、规范 JSON 与私有文件原语；不新增数据库、任务框架或通用产物框架。

## 分级、基线与执行身份

- 高风险任务：改动持久任务成功准入、并发租约/CAS 和数值产物权威；风险来自这些明确边界，不由文件数或代码量决定。
- 冻结代码基线 `755e0770203e1773ffe7ea123b599757b185f43a`，集成分支 `cdx/20260929-factor-source-integration`，实际核对 clean。从本计划最终提交创建 `cdx/20261001-factor-stream-jobs` 与 `.worktrees/cdx-factor-stream-jobs`，实现前核对 branch、HEAD、基准与 clean。
- 当前产品 native Codex，父任务 `/root`；既有 native implementer 负责编码、测试和修复。既有 reviewer 先作本高风险 SPEC 审查，最终候选集中独立审查一次；后续只复核原 finding、修复引入的回归与冻结范围内新证据，最多三轮定向修复，不追加逐文件或逐子任务审查。
- 子任务仅访问指定工作树与自有临时目录，离线，不访问网络、`.env`、真实凭据或生产；不得继续委派或调用第三方模型/CLI。主 checkout 和已验收候选保持冻结。

## 写集与责任

| 文件 | 责任 |
|---|---|
| 新增 `src/rquant/factor/stream_job_spec.py` | 严格 v2 规格及规范摘要，复用已验收 adapter request 与实际成员归档引用 |
| 新增 `src/rquant/factor/stream_job_artifact.py` | 按日统计输入 journal、其小 manifest、compact full/display 的发布与加载；实际统计复算及确定性投影 |
| 新增 `src/rquant/factor/stream_job_runner.py` | 实际文件驱动执行、完成链绑定、发布/复读与 v2 completion |
| `src/rquant/factor/stream_runner.py`、`member_stream.py` | 最小可选统计批观察桥；原 public 调用在无观察器时保持结果不变 |
| `src/rquant/factor/job_ledger.py`、`job_worker.py` | 明确版本分派；同一实例准备验证和最终租约/CAS；不迁移 SQLite schema |
| `src/rquant/factor/result_serving.py` | 显式 v1/v2 display 解析与成功任务绑定，保持 Serving 容量与代际验证 |
| `src/rquant/web/models/factor_results.py`、`web/routes/factor_results.py` | 真实四池标签和轻量流式分组诊断类型，保持 v1 DTO 原形 |
| 新增 `tests/unit/test_factor_stream_job_spec.py`、`test_factor_stream_job_artifact.py`、`test_factor_stream_job_runner.py`、`test_factor_stream_job_authority.py` | 新合同、真实文件链路、独立复算和并发失败证据 |
| 现有 job/worker/Serving/Web 直接相关测试 | 必要兼容与回归；不复制原实现作测试预期 |

如测试组织需要调整，只在上述直接相关测试内记录实际节点。清单、CHANGELOG 和进度文档由 root 在受审代码合入后更新。此增量不修改 React、生产配置或数据；不增加行业/市值上下文能力。

## 合同与数值口径

1. v2 spec 显式 `schema_version=2`，保存 `FactorStreamAdapterRequest`、实际成员 archive 小引用、定义内容 SHA、代码 commit 和 deadline。已冻结 raw v2 binding 中的 snapshot/source/scope/hash 为来源身份；目录由受信运行时注入。不得存任意 RO 路径、SQL、Python、Iterable 或全期间证券矩阵。
2. spec JSON 显式按 1/2 分派。原 completion 没有版本字段，仍走原解析；新 completion 显式版本 2。未知、混合或缺少 v2 判别的结构拒绝，不能通过宽松 union 回落。原 v1 规范字节、旧 display-unavailable 回读及 500/366/100,000 等现有边界不变。
3. 保留四池选择、未知 ST/板块/缺完整指数成员拒绝、原型 1/5/10/20 日调仓评价日与 1–10 评价期衰减。`as_of` 保持全局研究截止、`historical_retrospective` 保持回溯性质；不把真实观察时刻改写成过去采集。
4. journal 每个评价日仅封存一次**实际已被 daily 和 decay 消费的 `FactorDailyStreamBatch`**：完整 selected codes、universe 请求/输入身份、因子值/可见时刻/缺因、收益/成熟时刻/缺因、窗口及来源绑定。观察桥在现有两种统计消费同一批后调用，不能二次查 raw、重算公式或保存 AST 缓存。
5. journal 完成 manifest 只能在原成员、raw、公式、统计及衰减自然完整消费后发布。日期序列精确匹配评价日，不接受少日、多日或重日；不可变日文件可能在失败后保留，但不能生成成功完成引用。
6. v2 compact full 绑定 spec、成员完成、实际 raw/formula/statistics/decay 完成链和 journal 引用。它保存轻量标量/统计结果，不能以自身 SHA 自洽冒充横截面复算。旧 v1 full 的原数值重算保持不变。
7. 独立验证逐日读取 journal，使用**实际成员 archive reader**核对完整计算日程、每个评价日实际 `select_factor_universe` 结果、成员历史链和输入摘要；不能只核对声明 SHA、selected 数量或标签。调用现有 daily/decay 算子复算并逐项比较完整输出（IC、汇总、3/5/10 分组、累计、多空、换手及十期衰减）。
8. 该验证证明统计/衰减相对于实际 journal 的计算与实际成员选择绑定；不声称独立重算原始公式，也不证明外部提供方完整性或当时实际采集。此边界在结果内部合同与验收文档明确，不在页面堆实现说明。
9. display 从已验证 full 唯一确定性投影，发布后复读验证。四池按真实选择显示；流式分组只提供现有轻量诊断，不伪造旧 dense holdings。v1 DTO 输出保持原样，v2 使用明确类型判别。产物内容不包含 lease token、随机执行 ID 或验证墙钟，完成时间属于交付回执。

## 文件与资源边界

- 新日 journal 每文件最多 16 MiB、journal manifest 2 MiB、v2 full 16 MiB、v2 display 4 MiB；解析前检查字节界限，spec 2 MiB 和 completion 64 KiB 沿原账本上限。原产物前缀/上限及 Serving raw 4 MiB、owner 7 MiB、4 display/50 jobs 不扩大。
- 计算范围沿原最多 7,000 股票、1,024 计算日；journal 仅评价日。内存保留当前批、前期分组状态、最多十期衰减缓存和有界文件身份/引用，不保留整段因子/收益点矩阵。
- 复用既有 owned 0700 root、600 单链接普通文件、NOFOLLOW、具名/FD 身份、规范字节、atomic no-clobber 与自有 temporary 清理原语。v2 独立文件前缀，类型加载器不能误收 v1。
- 捕获 root 及各依赖文件身份至少包括 dev/ino/size/mtime/ctime/mode/uid/nlink；准备后同字节换 inode、目录换代或可见变更均拒绝。异常/取消清 FD、reader、执行 session、自有临时文件和准备记录；不得清他人文件。

## 冻结失败模型

### 资产与信任边界

资产：SQLite 持久状态及命令恢复身份、真实 lease 所有权、不可变研究输入/结果、数值口径、Serving 代和私有文件资源。trusted：受信运行时目录/实例身份、原 ledger 时钟及 CAS、领域算子。需验证：调用方 spec/completion、规范文件内容、文件名到 inode 的绑定、工作者输出与 heartbeat 的并发进展。进程内恶意代码任意篡改所有私有内存、OS root 或系统时钟控制不在本任务范围；普通调用方构造对象、文件替换和错误结果在范围内。

### 必须保持的不变量

- **SJ-01 版本和绑定：** 成功任务必须具有同版 spec/completion/full/display/journal，定义、代码、来源、scope、窗口、成员、日程及摘要精确对应；v1 字节/回读/恢复行为保持兼容。
- **SJ-02 数值准入：** 改值后重算所有外层摘要也不能将错误 IC/分组/换手/衰减提交成功；真实 journal 复算与成员文件选择验证为准。
- **SJ-03 完成完整性：** 取消、尾部缺失/损坏、观察桥错误或未完整消费均不得得到成功完成链。
- **SJ-04 唯一权威：** 最终提交使用同一已钉住 ledger 实例、实际当前时钟、最新 lease token/version、deadline 和原 CAS；旧 lease、丢失/过期 lease 或 ledger 换代均不能成功。heartbeat 正常推进 version 不使同一活 lease 的准备证据无效。
- **SJ-05 准备证据不可冒充：** 长验证在 heartbeat 仍运行时，由实际同一 ledger 对象产生并私有持有。最终接口接受对象身份受核对的 opaque handle；调用方构造、复制、跨 ledger 或旧 claim 的 handle/“verified SHA”不能代替真实验证。绑定 job、spec、token、实例及验证过的文件身份，不钉住会合法递增的初始 version。
- **SJ-06 文件稳定性：** 准备完成到 CAS 期间，full/display/journal/member 依赖的 root/文件身份再次核验；必须在取得原 writer 事务后、写 succeeded 前复核全部身份和 handle 绑定，防止等待写锁期间换代。变化无成功；写事务内不复放整次 raw/AST 或逐日数值计算。
- **SJ-07 有界资源：** 每日逐批处理，无全期间点矩阵。准备记录有界（同一实例最多 200 个、一 job/claim 只保留一个），完成/失败/取消/失去 claim 后清理；重启不复用内存 handle，沿原 reclaim 重做验证。线程、FD、临时资源和无关文件正确处置。

### 失败路径与提交顺序

1. worker 使用原 claim 并启动 heartbeat，按 spec 版本执行 v1/v2；v2 运行和发布期 deadline 实查。
2. v2 调用同一 ledger 的准备阶段：先查真实 live claim，再在**无 SQLite 写事务、heartbeat 仍运行**期间读取产物、成员及 journal、独立数值复算，保存私有验证绑定/身份。任何失败返回原失败分类，不写 succeeded。
3. worker 停 heartbeat 并 join，取它实际维护的最新 version；线程未结束或报告 lost 时无终态权威。长验证期间 lease 可以合法续期；丢失后最终 CAS 必拒绝。
4. 短完成阶段可在事务外预检 handle 和绑定；**必须取得原 writer 的写事务后**重新核对 handle、全部已验证具名文件/root 身份及任务绑定。等待 `BEGIN IMMEDIATE` 期间的替换也须被拒绝。身份检查完成后使用最新 trusted ledger clock 再核 live lease/token/version/deadline，然后原 CAS 记录 succeeded。该窗口不重新读取并解析全部 journal、不重算 raw/AST/统计。
5. 原 default lease 30s；已有 7,000×16 raw/公式执行实测约 103s。不得把长复算放到停心跳后的 complete，或通过放宽租约到任意时长隐藏此问题。journal 复算实际耗时尚未测量，本轮要留下至少小样本真实时间与窗口行为证据，不将小样本时间写成全市场 SLA。

排除：生产数据库/文件修复或迁移、权限/会话系统改造、Provider 历史采集/PIT 真实性、行业/市值中性化、React 运行按钮、每日跟踪、生产部署/切流/停 Streamlit、M11 下单。这些仍在完整目标后续依赖中，不以本片结论宣称原型完成。

## 集中实施与可执行验收

1. 在冻结 SPEC 经既有 reviewer 审查后，从计划提交创建 clean 工作树；预检 Python、sqlite/private FS、线程、既有测试依赖，用实际临时文件和假凭据离线红测。
2. 实现显式 spec/产物、日 journal 观察桥和实际文件 runner；将模型、执行与持久任务接线作为一个候选完成，不先交只含模型的子片。
3. 实现原 ledger/worker 版本分派、实例私有准备证据和短 CAS；随后接 Serving/API。让真实合成 archive + frozen raw → submit → claim → worker → 独立复算 → CAS → full/display → Serving/Web 的完整链路成功，四池有准确标签和空/部分样本行为。
4. **P0/P1 及等价失败行为证据：**
   - 实际修改并重哈希 IC、分组/累计/换手、衰减输出，准备验证拒绝，持久状态无 succeeded。
   - 改 journal universe 并重哈希，用实际成员 reader 验证拒绝；错日期/来源/scope/定义/code/manifest/混版同样拒绝。
   - 实际尾部文件缺失/损坏、取消或观察器异常没有完成回执；FD/session/执行副本自有临时资源清理。
   - 通过可控制 trusted ledger clock 与真实线程证明准备期间 heartbeat 至少推进一次；超过初始 lease 但保持续期可成功，使用最新 version；停心跳后只有有界身份/绑定检查，无统计复算。
   - 真实 lease 过期或被另一 claim 获得、ledger inode 换代、伪造/复制/跨实例/旧 claim handle、验证后文件或目录换代均无成功；同字节 inode 替换至少一个真实行为用例。另用真实 SQLite 写锁，在事务外预检后、等待取得 writer 时替换同字节文件 inode，取得事务后必须拒绝，持久状态无 succeeded。
   - v1 原规范字节固定对照、legacy 无 display 回读、既有提交成功/失败/命令重放仍成立；Serving 一次读取不调用数值重算，原容量/代际拒绝不变。
5. 多日小样本验证逐批弱引用释放、十期缓存/有界元数据和所有 owned 线程/FD/临时文件处置；测试字节界限在解析前生效。旧 7,000×16 runner 规模证据仅在未变边界内复用，journal/准备成本只按本轮实际测量陈述，不宣称最大区间或真实负载完成。
6. 改动区 Ruff/format/diff-check；冻结 clean candidate、完整 diff 和原始命令/日志/负面证据，由既有 reviewer 一次终审覆盖当前 diff、直接依赖、全部验收及上述冻结失败模型。最多三轮定向修复，范围外进入 backlog，不扩张全仓。
7. root 仅在无阻断 finding 后本地合入，核对受审文件逐字节；正常生成清单并核对只新增实际节点，无旧删除/重复、55 approved skips 不变，执行仓库两项必要清单门禁。同步 CHANGELOG 和平台进度，不因阶段名追加全量测试。

实际产品验证解释器 `/Users/roxor/brain/30-projects/rQuant/.venv/bin/python`（3.12.13），显式 `PYTHONPATH=<新 worktree>/src`、`RQUANT_DISABLE_DOTENV=1`、假 Tushare token、空通知 key、`NOTIFY_ENABLED=false`、独立 tmp DuckDB/parquet。实现者记录准确命令、实际环境、exit、红绿和资源日志；deselect/skip/环境受阻不能算通过。清单门禁按集成现有 Python 3.13.12 环境执行，不充当 3.11 或生产运行证据。

## 完成定义

只有上述实际完整后端链路、负面行为、兼容、必要门禁及独立终审成立才可接受本片。它是原型检验后端依赖，不是网页可运行或线上替代验收。受信真实成员/范围/中性化上下文、认证提交及 React 运行/恢复、跟踪和其他模块继续实施；完整 goal 保持 active。

## SPEC 审查（2026-10-01）

原 native reviewer 对计划提交 `9eaab78d2f123417a4125396125c350dc2e96aa6` 一次集中核对发现 `SJ-SPEC-01 P1`：事务外身份检查后，`BEGIN IMMEDIATE` 等待期间可能换代。最小修订已将最终全部身份/handle 复核明确置于取得 writer 后，随后最新时钟检查租约及原 CAS，并加入真实写锁等待窗口换 inode 的失败验收。此阶段是 SPEC/直接代码时序审查，尚无产品测试证据；不增加数据库 schema 或扩大写集。
