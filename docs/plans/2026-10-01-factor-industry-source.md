# 申万行业历史归属来源 Implementation Plan

> **For Codex:** 本片由原生 implementer 实现，最终候选由一名独立 reviewer 集中审查；普通任务最多一次定向修复和原 reviewer 复核。用户已授权按原型持续开发，无需重新确认。此前 FHA 异常清理的额外补修许可已经消耗，不适用于本片。

**Goal:** 保存真实申万行业目录及历史归属原件，提供绑定研究范围的按日行业读取，为原型中的行业及行业＋市值联合中性化准备真实输入。

**Architecture:** 行业来源单独负责分类版本、原始有效区间、采集时刻、摘要及缺失/边界状态；行情包提供计算范围和配对身份。后续适配器负责前一交易日映射，联合回归负责消除行业与市值影响，不把两个单独去暴露操作串起来冒充联合回归。本片不改公式、worker、鉴权、账本、公开 API 或网页合同。

**Tech Stack:** Python 3.11+、Pydantic、DuckDB、内容寻址 Parquet；复用当前集成树 Python 3.13.12，离线实现不安装新依赖。

## 等级、身份、写集及停止条件

普通任务：增加研究原件与只读领域接口，不触及生产写入、权限、并发状态或已冻结合同。实际产品 Codex desktop；父任务 `/root` 负责范围、真实来源验收和本地集成，原生 implementer 编码，独立 reviewer 最终审查。

工作树 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-industry-source`，分支 `cdx/20261001-factor-industry-source`，干净基准 `b933517668671c1e3f2be69bd95fb6819cd872c9`。初始唯一未提交文件是 root 写的本计划。实现仅可写：

- `src/rquant/factor/industry_source.py`
- `tests/unit/test_factor_industry_source.py`
- 本计划的实现/证据附录。

这是用户自有仓库内的授权可靠性开发。子任务离线，只访问指定工作树及自有 `/private/tmp` 证据；不得读 `.env`/真实凭据、访问网络或生产、修改主工作树或未知改动、继续委派或使用第三方代理。需要共享写集或改变本冻结范围时先报告具体依赖。冻结干净候选、证据和摘要后停写；root 负责清单、门禁、实际采集、进度和集成。全局与项目 AGENTS 的收敛规则优先于技能模板，不叠加逐阶段审查或全仓测试。

## 已核实事实与方案

- 官方 [index_classify](https://tushare.pro/document/2?doc_id=181) 支持 `level=L1, src=SW2021`；[index_member_all](https://tushare.pro/document/2?doc_id=335) 支持明确 `l1_code` 和 `is_new=Y/N`，返回原始 `in_date/out_date`，单请求上限 2000 行。成员接口没有分类版本参数，也没有文档化分页或日内生效时间；以实际 SW2021 目录中的 L1 代码逐项查询并记录这个限制。
- root 已通过现有配额观察器实际执行 3 次标准 Tushare SDK 请求：目录 31 行，电子行业当前 530 行、历史 174 行。原件、实际参数、时刻和 transport receipts 在 `/private/tmp/rquant-factor-industry-root-527cxk6q`。这些是可重用真实回执，不等于完整行业覆盖，也不是历史 PIT。子任务不读取该真实根，由 root 构造导入对象。
- 采用完整目录＋每个 L1 的 Y/N 原件，按实际目录动态生成有限日程。当前 31 个 L1 共 63 份响应；可导入上述 3 份并只发剩余 60 次请求。最多 64 次实际 transport dispatch（包含导入回执原调用），不得重取已存在原件、静默换 token、隐藏重试或假称导入产生新 HTTP。每份成员响应达到 2000 行即可能截断，拒绝完成；没有未经核实的分页。
- 保存原始字段/值、请求、实际采集时刻和摘要；只投影 L1 行业上下文。股票简称不用于 ST、上市状态或股票池；这些继续由已有证券来源负责。历史记录不得用最新分类前填，不声称完整覆盖所有年代或当年的分类法。
- 原型的“行业＋市值”将做联合回归；本片仅完成其输入。M3 继续部分，真实按钮和参数须在后续引擎及服务合同实现后开放。

## 验收标准

1. **类型化采集与完成约束。** 严格请求限定上述两个 API、字段、SW2021/L1 或明确 L1＋Y/N。复用 `RawSecurityTable` 和现有私有目录/原件工具，提供可信 callback 注入及已保存响应导入入口；普通模块导入不得初始化 SDK、设置或网络。响应绑定请求、时间、原件 SHA 和实际 transport receipts；完成资料必须恰好包含目录与每个目录 L1 的 Y/N，拒绝重复/错配、超限、截断或缺项。先核对导入再发剩余请求；回执区分本次新采集和既有导入，不改写导入的原字节/原时刻。root callback 用现有持久配额观察器在每次实际 dispatch 前记账，禁止自动重试；子任务以明确模拟回执验证有限日程和计数。
2. **可核验原件与失败语义。** 新建自有私有目录，不覆盖既有资料；保存逐响应原件及最后发布的完成 manifest。源文件/manifest 摘要和字段均可重验。失败/取消不留下可消费成功 manifest；复用现有按 inode 身份清理与 no-replace 发布工具，不自行添加无必要的持久状态机。部分原件可保留诊断，临时文件与句柄必须关闭。至少覆盖完成发布后的失败清理及原件被改动这一直接风险，不扩展成普通任务 attack sweep。
3. **研究配对与有效区间。** 准备请求引用真实 `FactorPreparedStreamSource` 和已完成采集；绑定其 SHA、snapshot/binding、scope/scope hash、code commit 及行业原件 manifest。行业来源边界标为 `captured_api_responses`，实际原件采集不晚于 scope.as_of_time；不能标成同一 RO 事务或历史首见/PIT。原始成员日期解析严格，保留 in/out/is_new/L1 和原响应摘要；无效 schema、错行业/状态、非法日期拒绝。对该股票所有有效区间投影，不按研究开始日过滤 `in_date` 而丢掉多年以前已纳入的有效记录；内容寻址 Parquet 可复用 `materialize_table_dependency`，区间表使用无事件日期的依赖及明确 interval 主键，不能把 `in_date` 当 daily 日期。
4. **边界与歧义。** 官方仅说明纳入/剔除日期，没有确认当天端点的包含关系。`in_date < panel_day < out_date`（out 为空则无右界）才视为非边界候选；当天触及相关 in/out 则返回 `boundary_unverified`。多个候选为同一 L1 可合并为这一 L1；多个不同 L1 返回 `ambiguous`；没有候选为 `missing`。返回 `valid/missing/ambiguous/boundary_unverified` 的类型化事实及计数，仅 valid 给行业代码/名称；不制造未知行业类别或取最新一行。目录名称用于版本内展示，原历史名称仍保留，不能仅因旧名称变化而丢记录。
5. **按日有界 reader。** 只读已校验私有 Parquet 副本；一日最多 500 代码，日期/代码严格在绑定范围，上限复用 7000 码/4096 自然日。输出每个请求代码，不删除缺失者，不回到主库/可变 RO/API，不构造全期逐日对象。来源摘要、原件 schema/行数、范围和公开状态可复核；正常/异常退出后连接、私有副本和句柄清理，关闭后不能继续读。复用生成/物化/验证工具，不新建来源框架。
6. **聚焦行为证据。** 实际红测到绿测：完整采集＋导入复用、真实形状解析与原件核验、两日行业变更、研究前已纳入记录、缺失/同 L1 重叠/不同 L1 歧义/端点、超限/截断/非法字段、配对或时刻错误、损坏及生命周期。选择必要旧成功节点证明旧三表行情与市值合同保持，复用仍有效结果。记录精确新增 nodeids、命令/环境/结果，Ruff/format/diff 和 3.11 语法证据；语法不称 runtime，不自动全仓执行。
7. **真实来源验收。** 最终候选接受后 root 重用 3 份真实预检，完成 31 个 L1 的 63 份原件；记录总实际请求及新请求数、覆盖/异常、来源摘要和资源关闭。与新冻结时刻的行情范围配对，在已验收 32 日/5571 代码的前一开盘日面板上完整比较原始区间投影与 reader 的值/状态/计数。保持回溯语义，不重跑旧无中性化 worker，不将原件验收称联合计算、最大负载或生产上线通过。

## 实现步骤与交付

1. 在新测试先实际建立针对上述事实的失败证据；用显式 `PYTHONPATH=<本树>/src` 和集成树 `.venv/bin/python`，清空用户环境并禁用 dotenv。实施严格模型、有限采集/导入、校验完成 manifest。接口确定后尽早把实际类名/函数签名发给 root，便于准备正常 SDK callback。
2. 实施独立行业准备包和按日 reader，复用现有内容寻址工具；不改旧 source schema 或冻结的公式/API/job spec。聚焦回归实际运行，遇到明确失败才扩大相关验证。
3. 更新本计划证据附录；仅提交授权三文件，冻结干净 candidate、源码/测试 SHA、实际新增 nodeids、命令日志和遗留处置后停止。root 安排一次独立最终审查，最多一次定向修复/原 reviewer 复核，然后做真实验收、清单精确增量及两必要门禁。

root 证据根 `/private/tmp/rquant-factor-industry-root-527cxk6q` 已保存当前 18956/55 清单基线及主工作树三项已知修改摘要。后续更新不覆盖主工作树 `AGENTS.md`、`CLAUDE.md`、`scripts/sync-from-cloud.sh`。

## 实现与候选证据（2026-10-01）

实际产品为 Codex desktop，原生子任务 `/root/factor_security_collection_impl`，implementer，父 `/root`。开写前实际核对上述 worktree、分支、基准 `b933517668671c1e3f2be69bd95fb6819cd872c9`，唯一初始 dirty 是 root 的本计划。写入仅新增行业模块、对应测试及本附录；未访问网络、`.env`、真实凭据、生产或实际行业来源根，未改共享模块、依赖、清单或主树，未继续委派。

### 公开接口与行为

- `IndustrySourceRequest(api_name, level/src, l1_code/is_new, fields)` 严格限定 SW2021/L1 目录与逐 L1 的 Y/N 成员请求；完整字段常量为 `INDUSTRY_DIRECTORY_FIELDS` / `INDUSTRY_MEMBER_FIELDS`。
- `make_industry_capture(request, frame, *, requested_at, observed_at, transport_receipt)` 返回 `CapturedIndustryResponse`，保留 `.response: RawSecurityTable`、`.response_sha256` 和 `.transport_receipt: SourceTransportUsageReceipt`。不初始化 Settings、SDK 或网络。
- `collect_industry_sources(IndustryCaptureRequest(root, import_paths=(), max_calls=64), *, fetch)`；`fetch(request)` 返回类型化 capture，仅对缺少日程调用。导入为既有 capture 的 canonical JSON，要求现有私有工具的目录 700/文件 600；有导入时必须包含配套目录以在任何新 dispatch 前核验。保留原字节、原时刻及原 transport receipts；63 响应、64 实际 dispatch 上限含导入。`load_industry_collection(root)` 重验完整日程、原件、摘要、字段、时间及 transport，manifest 明示 imported/new/total 计数。
- `FactorIndustryPrepareRequest(prepared_source, collection_root)` 与 `prepare_factor_industry_source(request, *, lake_root, now)` 绑定旧包 SHA、snapshot/binding/scope、code commit 和实际采集 manifest。来源为 `captured_api_responses` / `historical_retrospective` / `SW2021`，所有采集不得晚于 scope.as_of_time。
- 内容寻址 `industry_interval` 不设事件日期，主键为 `(ts_code, response_sha256, source_row)`，仅按 scope 代码筛选，完整保留其原有效区间、原 L1 名、Y/N、响应摘要和行号；没有以 `in_date` 限制研究起点。只保留当前响应的最多 1999 行、有界目录/引用和代码集合，不建立全期逐日对象。
- `open_factor_industry_source(source, *, lake_root)` 返回私有副本 lease；`lease.query(FactorIndustryQuery(source_sha256, trade_date, stock_codes))` 一日最多 500 码，输出每个请求码及 valid/missing/ambiguous/boundary_unverified 和计数。端点优先未验证；区间内部同 L1 合并、不同 L1 歧义。只有 valid 给代码及版本目录展示名，原历史名称仍在原件和 Parquet。reader 不访问原 RO、主库或 API。

采集最后发布 manifest，随后重验其字节及原件；发布后的 OSError/取消或原件变化拒绝时按自有根内 dev/inode 清理完成文件并记中断。复用 no-replace 发布和既有私有 FD 工具，不引入来源框架。准备失败不返回来源收据，完成的内容寻址原件可保留诊断。

### 实际命令、红绿和去重

证据根 `/private/tmp/rquant-industry-implementation-hyufgvsh`。Python 绝对路径 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration/.venv/bin/python`，实际 3.13.12；DuckDB 1.5.2、pytest 9.0.3，既有 Ruff 0.15.10。各命令在指定树运行，公共前缀为：

```sh
env -i PATH=/usr/bin:/bin RQUANT_DISABLE_DOTENV=1 TUSHARE_TOKEN_MAIN=00000000000000000000000000000000 DATA_DIR=/private/tmp/rquant-industry-implementation-hyufgvsh/offline DUCKDB_PATH=/private/tmp/rquant-industry-implementation-hyufgvsh/offline/test.duckdb PARQUET_DIR=/private/tmp/rquant-industry-implementation-hyufgvsh/offline/parquet LOG_DIR=/private/tmp/rquant-industry-implementation-hyufgvsh/offline/log TZ=Asia/Shanghai PYTHONPATH=/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-industry-source/src PYTHONDONTWRITEBYTECODE=1 /Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration/.venv/bin/python
```

token 仅为离线测试占位值。以下是该前缀后的实际参数，输出重定向到对应 log，JUnit 及 basetemp 均在证据根：

| 命令参数 | 实际结果 | 日志/JUnit |
| --- | --- | --- |
| `-m pytest tests/unit/test_factor_industry_source.py -q --tb=short --basetemp=/private/tmp/rquant-industry-implementation-hyufgvsh/red-tmp --junitxml=/private/tmp/rquant-industry-implementation-hyufgvsh/red.xml` | exit 1，26 failed / 1.32s；写源码前模块缺失 | `red.log/xml` |
| `-m pytest tests/unit/test_factor_industry_source.py -q --tb=short --basetemp=/private/tmp/rquant-industry-implementation-hyufgvsh/green-initial-tmp --junitxml=/private/tmp/rquant-industry-implementation-hyufgvsh/green-initial.xml` | exit 1，17 failed / 9 passed / 1.64s；严格 JSON before validator 的 tuple 解析问题 | `green-initial.log/xml` |
| `-m pytest tests/unit/test_factor_industry_source.py -q --tb=short --basetemp=/private/tmp/rquant-industry-implementation-hyufgvsh/green-json-tmp --junitxml=/private/tmp/rquant-industry-implementation-hyufgvsh/green-json.xml` | exit 1，2 failed / 24 passed / 3.50s；剩余导入夹具权限未设 600 | `green-json.log/xml` |
| `-m pytest tests/unit/test_factor_industry_source.py::test_ordinary_module_import_does_not_initialize_settings_or_network -q --tb=short --basetemp=/private/tmp/rquant-industry-implementation-hyufgvsh/import-red-tmp --junitxml=/private/tmp/rquant-industry-implementation-hyufgvsh/import-check.xml` | exit 0，1 passed / 1.36s；追加普通导入等价验收 guard，没有声称该节点红测失败 | `import-check.log/xml` |
| `-m pytest --collect-only -q tests/unit/test_factor_source_prepare.py::test_preparation_observes_missing_rows_and_nulls_then_admits tests/unit/test_factor_market_cap_source.py::test_two_days_match_raw_sql_units_binding_and_every_missing_state` | exit 0，2 collected / 1.02s | `direct-old-collection.log` |
| `-m pytest tests/unit/test_factor_industry_source.py::test_complete_31_industries_reuses_three_imports_and_dispatches_only_60 tests/unit/test_factor_industry_source.py::test_all_imports_are_checked_before_any_new_dispatch tests/unit/test_factor_industry_source.py::test_imported_actual_dispatches_count_toward_the_64_call_limit tests/unit/test_factor_source_prepare.py::test_preparation_observes_missing_rows_and_nulls_then_admits tests/unit/test_factor_market_cap_source.py::test_two_days_match_raw_sql_units_binding_and_every_missing_state -q --tb=short --basetemp=/private/tmp/rquant-industry-implementation-hyufgvsh/focused-green-tmp --junitxml=/private/tmp/rquant-industry-implementation-hyufgvsh/focused-green.xml` | exit 0，6 passed / 1.75s，4 新导入相关节点 + 2 直接旧成功节点 | `focused-green.log/xml` |

精确新增 27 nodeids 见证据根 `new-nodeids.txt`，直接旧成功 2 项见 `direct-old-nodeids.txt`。有效去重为上述 24 个新增成功并入定向补足的 2 个此前失败节点，再加普通导入 guard 1 = 新增 27；其中重复执行的两个导入拒绝节点不重复计数。加旧 2 为本次 29 个不同成功节点；没有任何单命令 27 passed 的声明。旧已有效的 61/259 等仅复用历史证据，不计为本次执行。

合成 31 L1 验证导入 3 原件后仅 callback 60 次，总实际模拟 receipts 63；完整导入不发 HTTP。两日合成范围保留 11 个区间，包含 2010 年已纳入、行业变更、原旧名称、同 L1 重叠、不同 L1 歧义、缺行及日期端点；预期投影显式独立断言。必要拒绝覆盖导入预检、2000 截断、非法字段/日期/行业/状态、计数预算、asof/配对/原件/完整性、范围、损坏及生命周期。

最终 `-m ruff check src/rquant/factor/industry_source.py tests/unit/test_factor_industry_source.py` 和 `-m ruff format --check src/rquant/factor/industry_source.py tests/unit/test_factor_industry_source.py` 均 exit 0，见 `ruff-final.log` / `ruff-format-final.log`；此前格式/导入排序和 SQL 字符串等价拼接不改变已有效行为。两文件 `ast.parse(feature_version=(3, 11))` 通过，只是语法证据。完整身份、哈希、结果去重和资源汇总见 `implementation-evidence.json`。

SHA-256：

- source `efc802d65426ae426583d1dc8eb09e6d14ee62648789678b9238a33d1e8b339d`
- test `a8e1fb299088fa6e9331b92c5f97c810664cda82ca243fd9e1a3066ca8f377a7`

### 资源与待 root 验收

所有命令已退出，无后台进程/运行会话。正常与异常 reader 退出实际验证连接关闭、私有副本删除且 lease 再读拒绝；改变原 DB、collection 与 Parquet 后活动私有 reader 仍返回同一事实。证据根检查没有 `.industry-prepare-*`、`.industry-reader-*`、发布临时文件或 `*.tmp-*`；诊断原件/失败夹具仅留自有证据目录。采集和准备的 FD 在 finally 关闭，逐文件读/发布复用既有 FD 工具。

未访问实际三份预检或做真实 63 响应/32 日对照，未执行中性化、下一交易日映射、历史 PIT、最大负载或 3.11/3.12 runtime 验收。root 在本地干净候选后安排唯一最终独立审查，再完成冻结计划第 7 项实际来源对照及本地集成；普通最多一次定向修复，不沿用其他任务例外。

## FIS-FINAL-01：唯一一次定向修复（2026-10-01）

初版干净候选 `8eb267fc56c48f76289e42694396f201172045ad` 已由唯一集中审查 ACCEPT。其后 root 实际 SDK 发现 `index_member_all` 交通行业 Y 含退市原始别名 `T00018.SH`，旧六位计算代码规则误拒绝合法原件。root 报告已保存完整 63 响应/7920 成员行，总实际调用 64（3 旧预检 + 61 新 SDK，含一次未保留 frame 的精确重取），预算耗尽，不再请求。子任务没有访问这些实际资料；该新证据启动本片普通任务第 1 次、也是唯一一次定向修复，未使用此前 FHA 例外。

修复前再次实际确认同一原生 implementer/父 `/root`、指定分支、上述 HEAD 和 clean status。写集仍仅原三文件。仅在原件词法校验使用独立 `_PROVIDER_SYMBOL`：正常六位数字或恰好 `T` + 五位数字，后缀 `.SH/.SZ/.BJ`；不接受任意字符串。原值、名称、日期、时刻、摘要不变，原行业/状态/字段/配额校验不变。共享 StockCode、FactorComputationScope、按日 query 和区间筛选均未改，别名不能加入计算范围、映射成另一股票或前填。

实际环境/清洁离线前缀沿用上节；证据根 `/private/tmp/rquant-industry-implementation-hyufgvsh/fis-final-01`。必要命令为：

| 公共前缀后的实际参数 | 结果 | 证据 |
| --- | --- | --- |
| `-m pytest tests/unit/test_factor_industry_source.py::test_provider_delisted_alias_is_retained_raw_but_excluded_from_computation tests/unit/test_factor_industry_source.py::test_provider_alias_rule_still_refuses_malformed_symbol -q --tb=short --basetemp=/private/tmp/rquant-industry-implementation-hyufgvsh/fis-final-01/red-tmp --junitxml=/private/tmp/rquant-industry-implementation-hyufgvsh/fis-final-01/red.xml` | exit 1，1 failed / 1 passed / 1.23s；合成 T12345.SH 被旧原件规则误拒绝，坏 symbol 拒绝已成立 | `red.log/xml` |
| `-m pytest tests/unit/test_factor_industry_source.py::test_provider_delisted_alias_is_retained_raw_but_excluded_from_computation tests/unit/test_factor_industry_source.py::test_provider_alias_rule_still_refuses_malformed_symbol tests/unit/test_factor_industry_source.py::test_complete_31_industries_reuses_three_imports_and_dispatches_only_60 tests/unit/test_factor_industry_source.py::test_two_days_preserve_old_intervals_changes_overlap_missing_and_unverified_endpoints tests/unit/test_factor_industry_source.py::test_bad_provider_schema_intervals_or_limit_cannot_complete -q --tb=short --basetemp=/private/tmp/rquant-industry-implementation-hyufgvsh/fis-final-01/green-tmp --junitxml=/private/tmp/rquant-industry-implementation-hyufgvsh/fis-final-01/green.xml` | exit 0，10 passed / 1.74s，新增 2 + 直接相关旧 8 | `green.log/xml` |

合成原件保留 `T12345.SH`、中文证券/行业名、原时刻和响应摘要；成员原件为 12 行，合法八码准备范围的 Parquet 仍 11 行，reader 输出八码完整事实/状态计数，别名 Scope/query 均明确拒绝。明显错误 `T1234.SH` 仍拒绝。没有读取真实别名资料、再发 SDK 请求、泛化新矩阵或重跑整个测试文件。

新增精确两节点见修复目录 `new-nodeids.txt`；上级 `new-nodeids.txt` 已更新为本片全部 29 新节点。复用原 27 新节点和 2 个直接旧成功节点的有效证据；本次实际只跑上述 10 节点，不声称重跑 29/31。修复摘要为 `evidence-summary.json`，完整修复 diff 为 `fix.diff`。

`-m ruff check` 和 `-m ruff format --check` 对两文件均 exit 0，见修复目录 `ruff-check.log` / `ruff-format-check.log`；3.11 AST 语法检查通过，实际 runtime 仍 3.13.12。最新 SHA-256：source `ba1ec66c17bb5d2b28a50711863feced9e37df61f65f080d3ec0d069a5caaa87`，test `02f33f4e9b9238bad9fd3d386433987518830131fa045e3075969809e27197a0`。

所有命令已结束，无运行会话/后台进程；自有证据根没有准备、reader 或发布临时文件遗留，合成失败原件仅保留诊断。干净本地修复 commit 后停写，交原 reviewer 只复核本 ID 与直接修复回归；本片普通修复次数已用完，如仍阻断须停写报告。
