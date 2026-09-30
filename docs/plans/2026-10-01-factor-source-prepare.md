# 正式只读来源准备

## 范围、风险与基线

- 父目标仍是完整 CC 原型、v2 计划与差距表；本片把现有 v2 raw 来源变成可实际准备、可交给检验执行器的输入，保留四种股票池、任务、网页检验、跟踪和生产替代的最终范围。
- 普通任务：在未发布的 factor v2 研究边界增加只读来源准备及独立离线元数据写入；不改生产数据、鉴权、并发账本、共享存储生命周期或冻结生产契约。仅观察输入文件与调用方明确提供的独立研究 metadata/lake，不创建生产仓库。
- 代码基线 `20dc8bc76efba8ee87516dc998037edc9514e128`，本计划提交后作为干净实现树 base。主目录的其他工具改动不接管。
- 已有 runner/decay 聚焦验收与独立终审有效；源码/测试仅对下述写集和直接受影响旧 builder 回归，不默认复跑全仓或旧 7,000 股规模。

## 输入和公共结果

新增 `prepare_factor_stream_source(request, *, metadata_store, lake_root, now)`。严格不可变 request 接受受信配置中的只读副本路径、预期主库路径字符串、计算 scope 与代码 revision；这些内部路径和可信参数不是网页可填写字段。请求的 scope 仍是计算代码超集，不代表已验证的全市场或指数成员。

返回严格不可变的 `FactorPreparedStreamSource`：实际准备收据、snapshot/binding、既有 admission request、scope hash及规范内容摘要，能直接传入既有执行器。准备收据记录读取方式、RO/sidecar 身份及 sidecar SHA、观察时刻、请求范围和实际三表的计数/日期/代码分布、NULL及结构缺行观测。原始缺数保留；行数、来源标签和文件代不能证明外部归档完整性或 PIT。

## 代与只读边界

- 只接受规范路径和普通非 symlink 的副本及 `.generation.json`，拒绝 RO WAL；绝不回落或 SQL 连接主库。
- 严格解析现有 `ReplicaGenerationMetadata`；主库名称匹配受信配置字符串，`source_before == source_after`，主库与副本记录的 inode 身份不同，sidecar 的 replica watermark 与实际 RO/FD 相同。
- 同时绑定 sidecar 字节摘要及 RO/sidecar 文件身份；打开前后、读取完成和准备成功前复核，代变化拒绝。本片不 stat 当前主库，不借 `validate_replica_generation` 访问它。
- `connect_pinned_readonly` 只负责 FD/in-place 的只读连接；代验证由准备层完成。09:25 沿既有历史回溯假设，真实首次接收时刻和成员事实另行核验。

## 同次观察、物化与就绪

1. 在同一个自有源事务内核验三表 schema/业务键、请求范围内实际日期/代码分布、SSE 完整自然日覆盖及范围内前交易日链；首行范围外 anchor 不伪造，须明确其验证依据或未验证边界。
2. 同事务物化并校验现有四项 artifact（范围、日线、复权、SSE 日历），观测计数/边界与导出证据相符。范围 watermarks 从实际已验证的日历/导出得出，不直接把请求范围当已覆盖承诺；结构缺行数不称作全市场缺失。
3. COMMIT 并确认源代未变后，依据实际收据/内容摘要生成 snapshot 身份，begin/finalize 实际 raw snapshot ready；再构造、发布、登记/finalize binding，最后使用既有 admission 复核。
4. `DuckDBStore.begin_dataset_snapshot_binding` 要求 snapshot 已 ready，必须保留此公共合同。后段失败允许 raw ready + missing/building binding，但整体不可准入、不得返回 Prepared 成功；不回退 ready，不增 failed 状态。
5. 所有路径关闭自有连接、FD及未完成事务，清理自有临时文件；不删除共享内容寻址文件或他人文件。失败与取消不发布完整准备成功。

## 允许写集

- 新 `src/rquant/factor/source_prepare.py`
- 新 `tests/unit/test_factor_source_prepare.py`
- `src/rquant/factor/stream_snapshot.py`：仅最小提取事务内物化/校验与绑定构造/发布私有助手。原公开 builder 的 ready 前置、独占 BEGIN/COMMIT、四 artifact、版本和上限保持。
- `tests/unit/test_factor_stream_snapshot.py`：直接相关旧入口/提取边界的聚焦证明。

存储、readside/replica generation、通用 research_snapshot、ledger/spec、Web、生产配置及成员采集不在写集。确有必要外扩时先报位置和具体证据；不得建立新的通用来源框架。

## 可验证验收

- 实际只读 DuckDB fixture + 匹配代收据，水位、counts/bounds/NULL与导出一致；一次源事务内观察和物化，stock scope和来源输入摘要有真实绑定，未填写承诺来绕过准备。
- 实际准备产物直接通过现有 v2 admission 并接入小型 runner/decay；旧 public builder 相关回归有效。合法原始缺数明确观测，不能静默补零或声明成员完整。
- 不完整/不一致日历、错误 schema、代替换/WAL/sidecar不匹配、物化或 binding 发布失败有聚焦拒绝证据；失败不得形成可准入成功，按上述生命周期允许 raw ready但binding缺失。取消和完成均验证资源清理。
- 只补实际桥接/完成/绑定风险用例，不逐字段生成拒绝矩阵。复用未改变边界的旧 7,000 股规模证据，不称作本片真实资源证明。
- 真实红到绿或等价行为证据，记录 Python/命令/exit/passed/skip/deselect和自有进程；Ruff/format/diff后冻结候选。最终一次集中独立终审，普通任务最多一次原实现者定向修复/原审查者复核。
- root 处理生成测试清单、两项必要清单门禁和进度文档；未运行全量、CI、Python 3.11或真实负载不能写通过。

## 已知现场前提

云端 RO 10,761,547,776 bytes 与匹配 generation sidecar 已经只读观察，最近 09-30 有 1 个 ST 未知/冲突，尚非完整可信成员归档。Mac 当前仅余 11,345,704 KiB，不在这里下载完整 10GB副本；真实负载采用后续云端受控诊断或独立区间提取，不能用小 fixture 替代该验收。本片只完成来源准备，不宣称正式任务/网页/每日跟踪/Streamlit替代完成。
