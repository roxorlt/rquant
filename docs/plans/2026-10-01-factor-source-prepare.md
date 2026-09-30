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
- 同时绑定 sidecar 字节摘要及 RO/sidecar 文件身份；打开前后、读取完成及 COMMIT 后、开始元数据发布前复核，冻结前代变化拒绝。这次末尾复核是来源冻结点；之后正常刷代不撤销已核验的历史 artifact，最终准入核对冻结内容。本片不 stat 当前主库，不借 `validate_replica_generation` 访问它。
- `connect_pinned_readonly` 只负责 FD/in-place 的只读连接；代验证由准备层完成。09:25 沿既有历史回溯假设，真实首次接收时刻和成员事实另行核验。

## 同次观察、物化与就绪

1. 在同一个自有源事务内核验三表 schema/业务键、请求范围内实际日期/代码分布、SSE 完整自然日覆盖及范围内前交易日链；首行范围外 anchor 不伪造，须明确其验证依据或未验证边界。
2. 同事务物化并校验现有四项 artifact（范围、日线、复权、SSE 日历），观测计数/边界与导出证据相符。范围 watermarks 从实际已验证的日历/导出得出，不直接把请求范围当已覆盖承诺；结构缺行数不称作全市场缺失。
3. COMMIT 并完成最后源代复核后，依据实际收据/内容摘要生成 snapshot 身份，begin/finalize 实际 raw snapshot ready；再构造、发布、登记/finalize binding，最后使用既有 admission 复核。元数据发布阶段不继续要求 live 副本保持同代；回执仍绑定实际冻结的原代，不声明是当前最新数据。
4. `DuckDBStore.begin_dataset_snapshot_binding` 要求 snapshot 已 ready，必须保留此公共合同。后段失败允许 raw ready + missing/building binding，但整体不可准入、不得返回 Prepared 成功；不回退 ready，不增 failed 状态。
5. 所有路径关闭自有连接、FD及未完成事务，清理自有临时文件；不删除共享内容寻址文件或他人文件。失败与取消不返回完整 Prepared 成功；源内容已冻结且 binding 实际 ready 后的取消或后段异常，可以留下有效的双 ready 历史产物。

## 允许写集

- 新 `src/rquant/factor/source_prepare.py`
- 新 `tests/unit/test_factor_source_prepare.py`
- `src/rquant/factor/stream_snapshot.py`：仅最小提取事务内物化/校验与绑定构造/发布私有助手。原公开 builder 的 ready 前置、独占 BEGIN/COMMIT、四 artifact、版本和上限保持。
- `tests/unit/test_factor_stream_snapshot.py`：直接相关旧入口/提取边界的聚焦证明。

存储、readside/replica generation、通用 research_snapshot、ledger/spec、Web、生产配置及成员采集不在写集。确有必要外扩时先报位置和具体证据；不得建立新的通用来源框架。

## 可验证验收

- 实际只读 DuckDB fixture + 匹配代收据，水位、counts/bounds/NULL与导出一致；一次源事务内观察和物化，stock scope和来源输入摘要有真实绑定，未填写承诺来绕过准备。
- 实际准备产物直接通过现有 v2 admission 并接入小型 runner/decay；旧 public builder 相关回归有效。合法原始缺数明确观测，不能静默补零或声明成员完整。
- 不完整/不一致日历、错误 schema、冻结前代替换/WAL/sidecar不匹配、物化或 binding 发布失败有聚焦拒绝证据；未完成物化或 binding 不得形成完整准入，按上述生命周期允许 raw ready但binding缺失。冻结后刷代仍保留原代回执；发布后取消不返回 Prepared，但有效历史 binding 可保留。取消和完成均验证资源清理。
- 只补实际桥接/完成/绑定风险用例，不逐字段生成拒绝矩阵。复用未改变边界的旧 7,000 股规模证据，不称作本片真实资源证明。
- 真实红到绿或等价行为证据，记录 Python/命令/exit/passed/skip/deselect和自有进程；Ruff/format/diff后冻结候选。最终一次集中独立终审，普通任务最多一次原实现者定向修复/原审查者复核。
- root 处理生成测试清单、两项必要清单门禁和进度文档；未运行全量、CI、Python 3.11或真实负载不能写通过。

## 已知现场前提

云端 RO 10,761,547,776 bytes 与匹配 generation sidecar 已经只读观察，最近 09-30 有 1 个 ST 未知/冲突，尚非完整可信成员归档。Mac 当前仅余 11,345,704 KiB，不在这里下载完整 10GB副本；真实负载采用后续云端受控诊断或独立区间提取，不能用小 fixture 替代该验收。本片只完成来源准备，不宣称正式任务/网页/每日跟踪/Streamlit替代完成。

## 本地验收（2026-10-01）

- 候选 `3b8caa21970594b8f144621ade7cd2fb02136006`，原实现 base `395b8e96538a3b160ef67ab8a07b67ee8853f2a5`；首候选 `6f87c8a82fb465aa12645bf912981faa67421f2f`。本地合入 `f2521bacd7f5f2aaf9bad59a64296b85d2d2d7d1`，四文件与受审候选字节精确一致。
- Python 3.12.13：26 个不同用例有有效通过证据。首批集中 23 passed、1 测试构造失败、43 deselected；修正构造单项通过，冻结后换代单项红到绿；终审修复新增类型拒绝与既有正向接线 2 passed，后者不重复计数。不能写成一次 26 项全绿。43 个旧 deselected（含旧规模）及全仓没有执行。
- 一次集中独立终审发现 `FSPREP-FINAL-01`：INTEGER 日历开关可准备但严格执行器无法消费。原实现者仅增加 BOOLEAN schema 门禁及一项测试，实际 1 failed → 2 passed；原审查者唯一一次定向复核关闭 finding、accept，无剩余阻断。最终 Ruff/format/diff 均 exit 0；全部自有测试和审查进程已结束。
- 实现及命令证据：`/private/tmp/rquant-factor-source-prepare-evidence-20261001.md`。未改变旧公开 builder、存储生命周期或版本/容量；源冻结点沿本计划修订口径。

## 真实副本准备测量

root 用已终审的精确候选，在云端独立 `/tmp` 目录运行来源准备与再次准入，生产输入只有 RO；元数据/湖/代码都在自有临时目录，结束已删除，连接及源 FD 为零、执行副本及准备 scratch 为空。没有部署或生产数据写入。

| 项目 | 实际结果 |
|---|---|
| 运行时 | Python 3.14.4 / DuckDB 1.5.2 / Pydantic 2.13.1 |
| 物理输入 | 10,761,547,776 bytes，只读 descriptor |
| 范围 | 2026-09-08—09-30，23 自然日 / 16 SSE 交易日 |
| 计算代码 | 5,570 个实际日线代码并集；不是可信股票池或全市场证明 |
| 实际导出 | 日线 88,849 / 复权 89,008 / SSE 日历 23 行，四 artifact |
| 结构缺行 | 相对计算代码×开盘日：日线 271、复权 112；不能称作市场缺失 |
| NULL 与闭市日行 | 受查必需字段 NULL 0、原始行情闭市日行 0 |
| 耗时 | 准备 5.469s，再次准入 1.398s，含选范围总计 7.241s |
| 进程峰值 RSS | 292,179,968 B（约 278.6 MiB） |

报告、完整准备收据、代码归档摘要和可复现脚本在 `/private/tmp/rquant-real-source-proof-grffhuvz/`；原始响应确认远端目录已删除。此项是实际 10GB 文件上的有限区间准备/准入测量，未测整段最大容量、完整公式/统计/衰减执行、恢复、成员归档或 PIT，也未测 Python 3.11。

完整准备 DTO 的额外本机跨主机解析没有通过：受信请求在本机重新核验云端绝对路径，macOS 的 `/home` 解析不同。该项不记为通过；云端构造与准入以及原同机严格 roundtrip 证据有效。后续任务用冻结身份、scope 和摘要接线，来源路径收据留在准备主机。

## 清单门禁与后续

- 集成后的固定清单为 18,719 项 / 55 批准跳过，SHA256 `82d4f21952d7ca794c74d0c035e45fd8c6e42ced86139cfec6a79c1c033add1f`。只新增本片 18 个节点（17 来源准备、1 旧入口前置），无删除/重复；批准跳过原件不变。
- 初次候选预生成因共享 editable 环境仍指向尚未合入新模块的集成树而收集失败，日志保留，不是产品红测或全绿。接受合入后在正常集成环境生成成功；两项必要清单门禁在 Python 3.13.12 实际 2 passed（9.25s），无 skip/deselect。没有运行全部 18,719 项或新 CI。
- 清单证据 `/private/tmp/rquant-factor-source-prepare-manifest-z63l82ok/`；门禁日志 `/private/tmp/rquant-factor-source-prepare-manifest-gates-20261001.log`。
- 下一依赖仍是可信成员/时点归档、持久任务及结果封存与 Serving，再接原型运行按钮和每日跟踪。本片不是网页或生产替代验收，整体目标保持未完成。
