# 每日市值来源 Implementation Plan

> **For Codex:** 按本冻结范围由原生 implementer 实现；最终一次独立 reviewer 集中审查，普通任务最多一次定向修复及原审查者复核。用户已授权按原型和 v2 持续开发，无需重新确认执行方式。

**Goal:** 从已验证的同一只读副本代提取有界每日总市值，提供可核验、按日读取的类型接口，为 `size_neutralize` 接线准备真实输入。

**Architecture:** 复用现有 `FactorPreparedStreamSource` 的范围、原副本文件身份与 sidecar 代信息，新增独立的市值来源包，不扩张旧 v1/v2 三表合同。市值模块负责单位、来源及缺值事实；后续公式适配器负责与前一交易日面板配对，网页只展示真实可用能力。

**Tech Stack:** Python 3.11+、Pydantic、DuckDB、内容寻址 Parquet；实际本地验证 Python 3.13.12。

## 任务等级、身份和边界

普通任务：新增回溯研究来源与按日只读接口，不改鉴权、生产数据、已冻结旧合同、账本、API、页面或基础设施。实际产品 Codex desktop；父任务 `/root` 负责范围与验收，原生 implementer 编码，独立 reviewer 最终集中审查。

工作树 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-market-cap`，分支 `cdx/20261001-factor-market-cap`，干净基准 `672be65afe2a54b6d88728b316d0e8f6975c3750`。唯一初始未提交文件是 root 写的本计划。全局 AGENTS 和项目 AGENTS 收敛规则优先于技能模板；不叠加逐子任务审查、全仓 sweep、重复全量验证或第三方代理。

实现者授权仅该工作树及其自有 `/private/tmp` 证据；离线、不得读取 `.env`/凭据、访问网络/生产或继续委派。不修改主工作树或来源不明改动。复用集成树既有 Python 运行时，通过显式 `PYTHONPATH=<本树>/src` 加载本候选源码，无安装或自动全套基线。

## 已确认的来源和时间语义

- 官方 [Tushare daily_basic](https://tushare.pro/document/2?doc_id=32) 的 `total_mv` / `circ_mv` 单位为万元。根任务已实际核验只读副本 `daily_basic(ts_code, trade_date, turnover_rate, volume_ratio, total_mv, circ_mv)`，09-30/29/28 各 5561/5559/5557 行；一只股票与真实 API 原件值一致。此预检不等于完整日期/代码覆盖。
- 当前流式适配器在交易日 09:25 使用前一开盘日面板。新来源保留实际 `trade_date`、总市值原单位和当前实际冻结/读取时刻，保持 `historical_retrospective`；没有历史首见时间字段，不能声称 PIT，不能把当日收盘市值用于当日 09:25，也不能前填或用最新值补过去。真正的下一日配对由后续适配器按已绑定日历完成。
- 复用同一副本代，避免另一套采集和生产表写入。旧来源准备、stream snapshot/read lease、公式、配置、worker、发布/账本全部保持原合同。本片不开放页面选项或宣称中性化检验完成；整体 M3 继续部分。

## 方案选择

采用独立市值包，与已准备行情来源的 `sha256`、snapshot/binding、scope、代身份及 code commit 绑定。直接把第四张表塞入旧包会改变现有精确三表校验；重新调用全期提供方则重复已有副本事实并增加额度，两者均不采用。新代码只承担这一领域输入，优先复用现有 generation、内容寻址物化及验证工具，不设计来源框架。

## 验收标准

1. **有类型的配对来源。** 新请求引用实际 `FactorPreparedStreamSource`；开读前、事务中及结束后核对同一 replica 与 sidecar 代，不能接新副本配旧行情。只打开 RO 副本，主库路径仅为预期身份名、不 resolve/stat/open。显式 Pydantic 收据绑定源包 SHA、范围、代、单位 `CNY_10000`、原始字段 `total_mv`、原件摘要、schema/行数和实际读取时刻，摘要可重验。
2. **有界导出和完整性。** 复用范围上限 7000 代码 / 4096 日；在一个只读事务内按既定范围物化 `daily_basic` 到自有 lake root。验证 `ts_code, trade_date` 主键与必须字段，不默许重复键或转换单位。不保留全期 DataFrame/所有逐日对象；只可留有界日历、代码和小计数摘要。不得覆盖既有源包或数据文件。失败不返回/发布成功来源收据，部分内容寻址原件可明确留诊断，自有临时文件和句柄须处置。
3. **按日读和缺值。** 新公开 context manager/lease 仅查询本包已验证私有 Parquet 副本，日期与代码严格在绑定范围，每次最多 500 代码且一日。跨层输出 Pydantic，原总市值保持万元；缺行、NULL、非正或非有限值明确区分/计数，不填补、不当作正常值，不将缺失市场代码从范围删除。reader 不能回到原主库或可变 RO。关闭/异常后不可继续读，完成后私有拷贝、连接和句柄清理。
4. **关键行为证据。** 新成功用例对原始 SQL 逐日逐股黄金核对单位/值、两日变化、缺行/NULL/无效值；必要拒绝用例覆盖代不匹配/读取期间代改变、错误 schema/重复主键、包或原件损坏/范围超限、reader 生命周期。针对真实风险补聚焦回归，不另建攻击矩阵。必要旧 source_prepare 成功节点证明旧精确三表合同保持。记录新增精确 nodeids 与每条命令实际结果，Ruff/format/diff 和 3.11 语法证据，不把语法检查说成 runtime。
5. **真实来源验收。** 候选接受后 root 用已授权只读 SSH 与实际 32 日范围/代码，并原来源准备包配对；按前一交易日映射核对原 RO 的总市值、覆盖率和缺值，与新 reader 完整消费一致，测量耗时/内存及资源关闭。根任务不重跑已成立的无中性化 worker，不把这次 raw-source 验收当作中性化检验、历史 PIT 或最大负载通过。

## 实现步骤与写集

### Task 1: 绑定市值来源与有界准备

- Create: `src/rquant/factor/market_cap_source.py`
- Test: `tests/unit/test_factor_market_cap_source.py`
- Update: 本计划的实现和证据附录。

先编写必要成功/失败红测并实际运行。使用 `FactorPreparedStreamSource`、`FactorSourceGeneration` 与现有 `_generation` / `_check_generation` / `connect_pinned_readonly`；如果现有工具确实不能满足边界，在新模块实现局部工具，不静默改共享文件。设计严格类型请求/收据、事务导出、范围和 schema 验证。复用 `materialize_table_dependency` / `verify_materialized_table_artifact` 时须实际确认其范围和 source table 参数，不靠名字推断语义。

### Task 2: 私有副本的按日 reader

在同一新模块实现 context manager 与按日有界 query，记录来源配对/单位和状态，原件校验及私有复制后才查询。命令例：`PYTHONPATH=<本树>/src <集成树>/.venv/bin/python -m pytest tests/unit/test_factor_market_cap_source.py --tb=short --junitxml=<自有证据根>/green.xml`；聚焦旧节点从 `test_factor_source_prepare.py` 正常收集后选择真实成功节点，不臆造名称。

### Task 3: 冻结和唯一最终审查

只提交上述三文件、冻结干净 candidate 与源码 SHA。实现者报告确切命令、红绿结果、差异范围、计数和资源处置后立即停写。root 查看实际 diff/证据，安排同一原生独立 reviewer 一次集中审查；后续最多一次局部修复/原 reviewer 定向复核。根任务实际来源验收、固定清单仅新增节点比较、批准跳过字节校验和两必要门禁、进度/changelog/本地集成由 root 负责；不运行 18935 项全套、不发布生产。

根任务证据根 `/private/tmp/rquant-factor-market-cap-root-iqd42zb3` 已保存清单七文件及主工作树三项已知修改 SHA，最终核对保留原件。

## 实现与候选验收附录（2026-10-01）

### 身份、范围和公开入口

实际运行于 Codex desktop 原生子任务 `/root/factor_security_collection_impl`，角色 implementer，父任务 `/root`。开写前核对指定 worktree、分支和基准 `672be65afe2a54b6d88728b316d0e8f6975c3750`，仅本计划为 root 来源的初始 dirty。实现只新增 `src/rquant/factor/market_cap_source.py`、`tests/unit/test_factor_market_cap_source.py` 并追加本附录；没有共享模块、依赖、清单或生产写入。

公开调用：

```python
request = FactorMarketCapPrepareRequest(prepared_source=prepared_source)
source = prepare_factor_market_cap_source(request, lake_root=lake_root)
with open_factor_market_cap_source(source, lake_root=lake_root) as lease:
    batch = lease.query(FactorMarketCapQuery(
        source_sha256=source.sha256,
        trade_date=trade_date,
        stock_codes=stock_codes,
    ))
```

`source` 绑定 `prepared_source_sha256`、snapshot/binding/scope、原副本代、code commit、实际读时刻、Parquet/schema/计数与自身摘要；`source.unit == "CNY_10000"`，`source.value_field == "total_mv"`，`source.source_mode == "historical_retrospective"`。准备请求引用实际旧包，读收据只含明确绑定事实，reader 校验不再访问原 RO 路径。复用旧 generation、内容寻址物化及校验工具，原 v1/v2 三表合同保持。

准备过程只打开配对 RO，在单一只读事务内按范围导出 `daily_basic`，读取前、事务内、事务结束后核对代身份。`ts_code/trade_date/total_mv` 要求现有 VARCHAR/DATE/DOUBLE schema 及准确业务主键。保留原表导出列和实际摘要，按日只返回 `total_mv`，没有单位转换。范围最多 7000 码 / 4096 自然日；SQL 聚合只保留有界代码/日计数，结构缺行按绑定开盘日计数，闭市日实际行另计。

reader 先验证原件并创建独立私有副本；一日最多 500 码。`batch.facts` 保留每个请求码及原值，`status/counts` 分别表示 valid、missing、null、non_positive、non_finite；NaN/Infinity 的 JSON 使用字符串，内存原值仍为浮点非有限值。关闭或异常离开后连接、私有目录清理且不能继续查询。来源包不添加历史 first_visible_at、前填或下一日 09:25 配对。

### 实际命令和分次结果

证据根 `/private/tmp/rquant-market-cap-implementation-pg715dt4`；Python 为 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration/.venv/bin/python`，实际 3.13.12，DuckDB 1.5.2、pytest 9.0.3、Ruff 0.15.10。所有 pytest 从指定树运行，公共命令前缀如下，token 为测试占位值：

```sh
env -i PATH=/usr/bin:/bin RQUANT_DISABLE_DOTENV=1 TUSHARE_TOKEN_MAIN=00000000000000000000000000000000 DATA_DIR=/private/tmp/rquant-market-cap-implementation-pg715dt4/offline DUCKDB_PATH=/private/tmp/rquant-market-cap-implementation-pg715dt4/offline/test.duckdb PARQUET_DIR=/private/tmp/rquant-market-cap-implementation-pg715dt4/offline/parquet LOG_DIR=/private/tmp/rquant-market-cap-implementation-pg715dt4/offline/log TZ=Asia/Shanghai PYTHONPATH=/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-market-cap/src PYTHONDONTWRITEBYTECODE=1 /Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration/.venv/bin/python
```

以下为该前缀后的实际 pytest 参数（日志与 JUnit 在上述证据根）：

| 命令参数 | 实际结果 | 证据 |
| --- | --- | --- |
| `-m pytest tests/unit/test_factor_market_cap_source.py -q --basetemp=/private/tmp/rquant-market-cap-implementation-pg715dt4/red-tmp --junitxml=/private/tmp/rquant-market-cap-implementation-pg715dt4/red.xml` | exit 1，21 failed / 1.24s；缺新增模块的必要红测 | `red.log`, `red.xml` |
| `-m pytest tests/unit/test_factor_market_cap_source.py -q --tb=short --basetemp=/private/tmp/rquant-market-cap-implementation-pg715dt4/green-initial-tmp --junitxml=/private/tmp/rquant-market-cap-implementation-pg715dt4/green-initial.xml` | exit 1，19 passed、2 failed / 5.22s；生命周期断言直接比较 NaN，NaN 不等于自身 | `green-initial.log`, `green-initial.xml` |
| `-m pytest --collect-only -q tests/unit/test_factor_source_prepare.py::test_preparation_observes_missing_rows_and_nulls_then_admits tests/unit/test_factor_source_prepare.py::test_observation_and_exports_share_one_read_transaction_and_never_stat_primary` | exit 0，2 collected / 1.01s | `direct-old-collection.log` |
| `-m pytest tests/unit/test_factor_market_cap_source.py::test_private_reader_never_reopens_ro_and_closes_after_context_or_error tests/unit/test_factor_source_prepare.py::test_preparation_observes_missing_rows_and_nulls_then_admits tests/unit/test_factor_source_prepare.py::test_observation_and_exports_share_one_read_transaction_and_never_stat_primary -q --tb=short --basetemp=/private/tmp/rquant-market-cap-implementation-pg715dt4/focused-green-tmp --junitxml=/private/tmp/rquant-market-cap-implementation-pg715dt4/focused-green.xml` | exit 0，4 passed / 1.22s；改为保持非有限值字符串的 JSON 比较后，新生命周期 2、旧成功 2 | `focused-green.log`, `focused-green.xml` |

新增精确 nodeids 全部 21 项见证据根 `new-nodeids.txt`，直接旧成功 2 项见 `direct-old-nodeids.txt`。有效去重口径为初次实现的 19 个新增成功 + 修正后的生命周期 2 个新增成功 + 旧成功 2 = 本片 23 个不同成功节点；没有声称任何单条命令 21 passed。后续仅只读 source 属性、等价格式/导入排序与字符串拼接排版，行为结果复用；没有重复全套。旧 61/259 等结果仅作为仍有效历史证据，不计为本次执行。

黄金测试使用自有合成 RO：8 码、2 个开盘日、14 行；明确保留 2 缺行、2 NULL、4 非正、3 非有限、5 正常值，逐日逐股与原始 SQL 比较，单位与两日值变化一致。另 501 码范围按 500+1 两次查询消费，不能据此声称最大负载通过。聚焦拒绝及资源用例覆盖冻结验收中的代变化、schema/主键、包/原件损坏、范围/查询上限、失败/取消和 reader 生命周期。

最终 `python -m ruff check src/rquant/factor/market_cap_source.py tests/unit/test_factor_market_cap_source.py` 与 `python -m ruff format --check src/rquant/factor/market_cap_source.py tests/unit/test_factor_market_cap_source.py` 均 exit 0，见 `ruff-final-clean.log` / `ruff-format-final-clean.log`。源码及测试 `ast.parse(feature_version=(3, 11))` 通过，仅为 3.11 语法证据，不是 3.11 runtime。完整身份、哈希、去重与资源汇总为 `implementation-evidence.json`，版本为 `runtime.json`。

最终 SHA-256：

- `src/rquant/factor/market_cap_source.py`: `7c1f136448347f57cb5b1a8472e57dc739abe44527626b7793b21df2a53a6a07`
- `tests/unit/test_factor_market_cap_source.py`: `9c87b555394003eb9d71fec391dd467c5e0e7632b0d643d6140208810bf6c9a7`

### 资源处置与剩余验收

所有执行命令已退出，无后台进程或运行会话。用例实际验证一次 RO BEGIN/COMMIT、失败或取消 ROLLBACK、连接和源 FD 关闭、主库路径未 resolve/stat/open；正常及异常 reader 退出均关闭连接并删除私有副本。证据根没有 `.market-cap-reader-*` / `.market-cap-prepare-*` 或 `*.tmp-*` 遗留。代变化后已完成的内容寻址 Parquet 和损坏用例原件仅保留在自有临时证据根作诊断，没有成功来源返回。

本片没有读取实际 RO/生产资料，没有 32 日真实对照、下一交易日适配、中性化、历史 PIT、3.11/3.12 runtime 或最大负载结果；这些不以合成证据替代。root 在干净候选后安排一次最终独立审查，并负责计划第 5 项真实来源验收和集成。普通任务最多一次定向修复，不沿用此前其他任务的额外许可。

## 根任务最终验收（2026-10-01）

受审候选 `29ce9eee3110b9ed67962291cb247bc913a9888e` 在本地集成树 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration`、分支 `cdx/20260929-factor-source-integration` 合入，merge `e539d7696dbfda2012870dfae5455435f886c2f7`。唯一集中独立审查 accept、范围内无 findings，报告 `/private/tmp/rquant-market-cap-final-review-0tehmTCA/review.md`，SHA256 `90583cf3855bbd2ea75bb2fa0d162698d21b8ddc0d5c0e93ee795f1cc52cb605`。没有修复/额外审查轮次；有效21新增＋2旧结果复用，没有重复文件级验证。源码/新测试保持上述受审字节，root仅维护清单和验收记录。

### 实际只读副本与完整日范围

根任务证据 `/private/tmp/rquant-factor-market-cap-root-iqd42zb3`。`accepted-candidate.json` 保存冻结候选、受审哈希和报告摘要；`actual-scope.json` 保存已验收的两池原成员档案 SHA、08-14—09-29 共32计算日及5571代码范围。原档案 all/gem SHA 分别为 `f50016de6a135f9a5339ce4a399f5ade598ddf76ec6f9470df09452ab22c7c23` / `8a3b5b8439c799ed678de93f8ad87e4b756a57900e13fb8193e7d2e776fbff1f`；仅复用日程与计算范围，没有重新取提供方数据或重跑已成立的无中性化 worker。

`run-cloud-proof.py` 仅打包精确候选的 tracked `src/` 和上述已核对档案摘要的范围，不包含 `.env`/凭据；SSH `lighthouse@82.156.0.68` 调用 `/home/lighthouse/rquant/.venv/bin/python`，代码、元数据及产物仅在自有 `/tmp/rquant-factor-market-cap-proof-20261001-<随机ID>`。标准超时和 finally 清理，没有服务/基础设施或生产数据写入。实际云端 Python 3.14.4，从 `/home/lighthouse/rquant/data/rquant_ro.duckdb` 准备原行情包及新市值包，同副本代/范围/SHA、单位和前后 generation 完全一致。

- 原RO约10.0GiB，实际冻结时刻 `2026-10-01T14:00:30.706958+00:00`；范围08-12—09-30。原三表194187日线、194478复权、50日历行；总市值物化194187行，NULL/非正/非有限及闭市日行均0，全35开盘日结构缺行798。
- 32计算日按实际绑定日历的前一开盘日核对，新reader分500码/一日，完整5571×32＝178272组合逐值/逐状态/逐计数对比原RO。177529有效值完全一致、743缺行如实保留；共385查询，包含一项09-30原API核验样本。首日08-14配08-13：5540有效/31缺；末日09-29配09-28：5557有效/14缺，没有当日收盘值用于09:25或前填。
- 样本000001.SZ/09-30总市值原值 `22452647.3574` 万元，与原RO及先前实际API回执一致。这里核验原日期配对及原值，不产生历史首见时间或PIT声明，也不是中性化公式、最大负载或生产安装通过。
- 行情准备9.967s、市值准备2.814s，总19.683s；峰值RSS324419584 bytes＝309.391MiB。关闭后的reader拒绝查询，私有副本和两类准备scratch空，源FD与自有lake FD均0，RO代未变。远端tmp finally 已移除，所有SSH/采集对照/门禁命令退出，本地诊断原件明确保留。

行情来源SHA `be3f51bb694240324142a2e904a312f51ae616b72c73b1798fd4db1ee3fe8215`，市值来源SHA `a791d90eaecb32f8b832963d83d60fb1b926ad8d0db2af271adfd9e2862e41e3`，市值原件SHA `59d2e65558f7117a0af2cadc9f412640ffd352922ec54ad7e3c1b77c2f087280`。完整response和逐日结果为 `response.json` / `real-summary.json`，response SHA256 `69de353ba213b82107533839d56dc6e18e9f6beb4774c2b54198181829be8208`；命令、归档及远端路径在 `dispatch.json`、实际stdout/stderr中。

### 清单、集成与剩余范围

正常收集18956 cases / 55 skips，SHA256 `999bca1bf6f8a36beef499fad2056097a3f9cd0330b92d10c40f5780d6099ab8`。相对18935旧清单精确仅新增21，无旧删除/重复；批准跳过原字节及SHA `1367a714636bb473ff37edd8af1928d460d2b95f84f3c2658b6a4004cbb1b813` 不变。`comparison.json`保留精确差异；root临时比较脚本曾把nodeid逻辑摘要误按文件字节SHA核验，按仓库 `nodeid_digest` 规则修正后通过，产品候选未改。两必要门禁本地Python3.13.12实际2 passed / 8.60s（`gates.log/xml`），没有执行全部18956項或新CI。

市值事实来源已接通；下一片接中性化流式计算与该来源、申万行业有效区间，再接实际可用参数及React交互。CSI完整历史、18:40跟踪、正式配置/数据代、完整资源与生产体验仍须完成；M3继续部分，整体goal active。本轮没有push、main merge、tag、部署或Streamlit切流，主工作树既有修改保持原件。
