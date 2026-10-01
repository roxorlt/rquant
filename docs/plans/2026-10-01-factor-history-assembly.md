# 因子长历史多批装配

## 目标与身份

将多个已完成的历史采集批次按显式交易日日程装配为已有成员归档，解除单次归档只能使用31日期采集批次的限制。继续复用名称区间补充、逐日股票池、成员reader与v2计算；不提高单批采集预算。

普通任务：新增离线有界来源装配与私有归档，不改鉴权、生产数据、账本或既定下游契约。实际产品Codex桌面；root负责范围与验收，原生implementer编码、最终一名独立reviewer集中审查。工作树 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-history-assembly`，分支 `cdx/20261001-factor-history-assembly`，干净基准 `9f44927a5eb2b2a33c96ae74c44403a1c8dbb4d2`；本计划是root唯一初始改动。

写集：一个同域装配模块（建议 `src/rquant/factor/history_assemble.py`）、对应单元测试及本计划；只有复用现有捕获/归档确实需要时才最小调整 `security_collect.py`。不改name_collect、security_status、universe、member_archive/reader契约、worker、Web/React、部署、依赖或固定清单。root维护后续清单、进度和真实取数。

子任务在指定树与自有/private/tmp离线开发，禁dotenv、网络、凭据、生产访问和继续委派。先确认实际身份、分支、基准与初始改动来源；未知dirty、范围扩大或一轮修复后仍阻断时停止报告。普通任务一次集中终审，至多一次原作者定向修复和原reviewer复核。

## 已有效证据

- 现有collector单批31日期、64实际dispatch（含重试），已有私有完成发布/中断拒绝与逐日源绑定；名称补充已验收，旧入口摘要/行为兼容。
- 既有归档和v2公式最多1,024计算日/7,000代码，按日消费；任务/config/worker/结果与网页运行入口已接通，不重复实现。
- 实际18次来源捕获位于 `/private/tmp/rquant-factor-security-root-j2oc17a3/live-capture`，名称实际单码捕获 `/private/tmp/rquant-factor-name-root-7dyir3f0/names-live`。仅只读，不修改或重记接收时刻。
- root只读SSH预检RO约10GB，最近36开盘日08-11—09-30有表内日线；记录 `/private/tmp/rquant-factor-name-root-7dyir3f0/next-ro-window-preflight.json`，只作为下一实测候选，不以行数证明市场/来源完整。

## 设计与验收

1. **显式、有界来源组合。** Pydantic请求明确完整交易日日程、all/gem选择、截止时间、采集目录和可选单个名称目录。最多34采集批次，最终日程不超过已有1,024日、代码并集不超过7,000；保留每批31日期/64dispatch上限。日程必须升序、唯一，每个请求日恰由一批覆盖；输入批次之间重叠日期、缺日、重复目录或冲突直接拒绝，不取最后一份、不填邻日。不开放CSI或中性化缺事实选项。
2. **同域装配、复用归档。** 来源load/normalize沿既有入口，日结果仍绑定实际当日原件及其批次参考资料。先校验所有请求日期、实际observed<=as_of及代码并集，再按日写已有FactorMemberDayInput并调用原publisher。预验所得日绑定在写入阶段核对；来源变动或尾批失败不能发布成功manifest。复用私有文件/根目录校验，不另建持久化框架或任务调度器。
3. **资源按日释放。** 一次只打开/读取当前采集批次参考及当日响应；不保留全期DataFrame、DailySecurityBatch或所有原始响应。可保留有界日程、日摘要/时刻、批次小回执和代码集合；按日yield后原始/规范化批对象释放，reader沿既有require_completion完整消费。
4. **旧合同和来源语义。** 单批旧replay/archive保持；新独立离线CLI显式重复capture-root、完整date日程及可选name-root，不初始化Settings/客户端。名称区间仅按已有规则补必要事实；源SHA/观察时刻不得重新伪造或改成采集日。historical_retrospective保持，不宣称PIT或提供方全部历史覆盖。
5. **可验证行为。** 最少32日跨两个真实格式批次，与逐日原入口的事实/成员/摘要/时刻黄金对照；只有完整日程才归档成功。缺日/重叠、必要事实缺失/坏原件/中断批、晚于截止、末批失败及预验后源绑定改变均拒绝且无成功manifest。只加必要失败用例，按实际风险做聚焦回归；不追加跨模块/全仓矩阵。验证当前日批对象释放，并用原reader完整读取跨批归档。

## 执行与收敛

先必要红测、局部实现、实跑新增及直接依赖；复用仍有效的名称61/旧259等证据，不重复全量。准确记录新增nodeids、分命令实际结果、Ruff/format/diff和语法/运行时区别。源码与证据足够后立即冻结干净candidate，交一次最终独立审查，不追加可选检查。

root在接受后补实际跨批捕获/回放/归档及大于31计算日的原RO→配置/worker测量；以实际来源准入决定日期及股票池，缺必要事实明确报告，不修生产数据。预计使用已有09-28/29/30与一批更早日期，通过显式日程选择32个计算日并保留实际未来收益尾部。收集固定清单仅比较精确新增节点、同步计数/摘要及两必要门禁，不执行全部18,878项。

本片之后继续CSI有效成分、行业/市值、18:40跟踪和正式来源/资源/生产验收。网页UIUX与中文文案按原型既定规范；本片不改页面，整体goal保持active，M3不提前标完整。

## 实现与验收记录（2026-10-01）

实际身份为Codex桌面原生子代理 `/root/factor_security_collection_impl`、父任务 `/root`。开始前确认分支和HEAD与冻结基准一致，唯一初始dirty为root创建的本计划。最终写集仅本计划、`src/rquant/factor/history_assemble.py`和`tests/unit/test_factor_history_assemble.py`；无需调整旧collector或任何既有下游合同。

`HistoryAssemblyRequest`显式限制all/gem、升序唯一的1—1024请求日、1—34不同完成采集根、含时区截止时间、最多一个名称根和不同的新建输入/归档目录。先核对全部来源日程（任意批间重叠拒绝，请求缺日拒绝），再逐批预验全部请求日、as_of与最多7000代码并集。原单批31日期/64dispatch上限保持。

装配只保留有界小日绑定（规范输入SHA、原sourceSHA、实际observed）、代码并集和批次文件身份/回执摘要；复用已有私有根、读取和身份核对工具。每批沿原逐日迭代器读取，离开当前日/批释放原始及规范化对象。写入时再次核对日绑定和来源，原publisher自然尾部再次复核来源后才可发布manifest。原sourceSHA、observed、historical_retrospective及股票池事实不改写；缺事实、坏或中断来源、预验后变化、写入末批失败均拒绝，部分自有输入可保留诊断但无成功manifest。

离线入口：

```text
python -m rquant.factor.history_assemble archive --capture-root <完成批次一> --capture-root <完成批次二> --date <逐个升序显式日期> --selection all --as-of <含时区截止时间> --name-root <可选名称根> --input-root <新私有输入根> --archive-root <新私有归档根>
```

证据根 `/private/tmp/rquant-history-assembly-implementation-96jjh7po`。实际Python为`/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration/.venv/bin/python` 3.13.12；PYTHONPATH为本树src。测试/CLI使用`env -i`、PATH=/usr/bin:/bin、RQUANT_DISABLE_DOTENV=1、全零合成token和该证据根下offline目录的DATA_DIR/DUCKDB_PATH/PARQUET_DIR/LOG_DIR、TZ=Asia/Shanghai，未初始化真实来源客户端或读凭据。

- `red.log/xml`：最初18项实跑失败，原因是新模块缺失；`green-initial.log/xml`为18 passed，3.11s。随后`publisher-tail-red.log`实跑证实来源在原publisher读最后输入期间改动仍能错误发布（DID NOT RAISE），加入自然尾部复核后`green.log/xml`为19 passed，2.83s。
- 最终命令：`python -m pytest tests/unit/test_factor_history_assemble.py tests/unit/test_factor_member_archive.py::test_import_rechecks_already_read_inputs_and_closes_cancelled_iterator tests/unit/test_factor_member_stream.py::test_reader_releases_daily_models_and_completes_only_after_natural_tail --tb=short --junitxml=<证据根>/focused-regression.xml`。`focused-regression.log/xml`为21 passed，2.96s，无skip/deselect。19项新增精确节点在`new-nodeids.txt`，两个直接旧节点在`direct-old-nodeids.txt`。
- 仍有效的名称/collector61项及旧依赖259项复用。两个本次旧节点已包含在259中，与19个新增不交叉；去重独立有效口径61+259+19=339项，并非本次执行339项；`dedup.txt`由实际JUnit节点集合核对。未改固定清单或执行全仓门禁。
- 32日、31+1两批、all/gem黄金对照均通过：原逐日事实/选择成员/sourceSHA/observed完全一致，上市代码并集含末日IPO，批输入顺序倒置也不改变显式日程。当前批捕获/日对象weakref释放验证通过；原reader自然尾部require_completion处理32日。
- `cli-proof.py/json/log`：两池32日CLI实际执行并原reader完整消费，精确对照保存原逐日输入。all归档SHA `0dcb24f7f81bf6fa6de1bed9473a9ec657692c526848b8e32584f5ecc3d5c61d`，gem `25801ff31504260e7962d323ab489c434234d39f74493b29c6cbe6365a09c35e`。这是明确标记的离线合成提供方与注入时钟数据，未冒称真实live或历史PIT；完整命令argv保存在JSON中。root已有30早日期真实捕获及后续32日RO-worker实测未在此重复。
- 可选名称的跨批CLI按既有完整区间来源补值通过；普通模块导入的子进程证实无Settings/客户端初始化。Ruff check、format --check及git diff --check通过，两源/测试文件3.11语法解析通过；实际runtime仅3.13.12，不宣称3.11/3.12运行验证。

资源处置：全部工具命令会话及subprocess子进程退出，未启动后台服务；自有证据根无发布`*.tmp`。合成采集、CLI输入/归档、离线配置及日志保留终审，测试临时资料遵循pytest默认保留，未全局清理。真实来源目录未写入。冻结干净候选后由root安排一次集中终审、实际32日装配/原RO-worker、清单两项门禁及本地集成决策。
