# CSI官方日样本来源与成员归档

## 目标与边界

给原型中的沪深300/中证1000检验提供可追溯的真实完整成员来源。最终仍须覆盖用户选择的每个历史决策日；本片建立官方日样本捕获、离线校验与已有归档接线，root同步核实历史基线/调整覆盖。只有实际表内日期可准入，缺日期继续不可检验，不用最新名单或月度权重回填历史，也不把本片称为指数历史功能完整。

普通任务：新增同域公开来源适配与私有归档，复用既有成员、统计和worker合同，不改鉴权、生产数据、调度或发布。实际产品Codex桌面；root负责范围、委派和真实取证，原生implementer编码/聚焦验证，最终一名独立reviewer集中审查。工作树 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-index-source`，分支 `cdx/20261001-factor-index-source`，干净基准 `9c6e6ecbe41493cbe73ed932e2ee52af28b7f405`；本计划是root唯一初始dirty。

写集：`src/rquant/factor/index_collect.py`、对应 `tests/unit/test_factor_index_collect.py`、本计划；允许把锁内已存在xlrd提升为直接依赖，仅调整pyproject.toml与uv.lock对应项。无需新Excel/HTTP库，不改旧security/name/history_assemble/universe/member_archive/reader、worker、Web/React、部署、固定清单或其他依赖版本。root负责后续清单/进度/真实网络调用，不在实现树混入其他来源修改。

子任务只在指定树及自有private/tmp离线TDD，可只读下述已下载公开原件；禁网络、dotenv、凭据、生产访问及继续委派。未知dirty、范围扩大或普通唯一修复后仍阻断时停写。一次集中终审，至多一次原作者定向修复/同一reviewer复核；此前长历史任务的额外一次授权不适用于本片。

## 已核实事实与预检

- 官方详情页 [沪深300](https://www.csindex.com.cn/#/indices/family/detail?indexCode=000300)、[中证1000](https://www.csindex.com.cn/#/indices/family/detail?indexCode=000852) 暴露固定样本列表XLS，表头九列、OLE传统Excel。固定URL分别为 `https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile/cons/000300cons.xls`、`https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile/cons/000852cons.xls`。
- 原件位于 `/private/tmp/rquant-csi-official-current-pw5zv0yv`：hs300-cons-with-receipt.xls，67072 bytes，SHA256 `0f9fde0e470ba81bddd0d3e6db05269e1b5077aca778879f3833d04e027fa25d`，实收2026-10-01T09:35:07.839996+00:00；zz1000-cons.xls，213504 bytes，SHA256 `f5fe053ab5925b5838118a44c5e33c41581da2304fd7131c631c8af2cba8acf3`，实收09:19:23.129705+00:00。各`.receipt.json`保留实际HTTP200/请求时刻/响应头/实收时刻；旧HS300没有初次HTTP回执的原件不可重新标记为fresh。
- 两份表内日期均20260930，分别300/1000行；纯原领域预检在已接受的09-30完整证券事实中得到300/300和1000/1000。它不是新collector/历史归档或32日指数验收。
- 09-09临时公告生效取证券退市日，09-30名单仍含待调出证券；不能把公布日当生效日。已有新闻目录和月度权重不足以证明全期有效区间。root继续核实官方历史基线/完整调整覆盖；本片不由公告缺席推断成员不变。
- 集成venv Python3.13.12实际有xlrd2.0.2、pytest和Ruff；uv已可用，xlrd已锁为AKShare依赖。只用该venv，不自动安装整套环境或跑全仓基线。3.11语法与3.11/3.12 runtime证据分开。

## 冻结验收

1. **真实HTTP捕获。** Pydantic请求只接受hs300/zz1000及上述固定官方URL，不接受任意端点。独立显式live入口不读Settings/密钥，标准HTTPS验证、有限超时、每次dispatch计预算，默认无隐式重试；最多两指数、总实际dispatch上限4、每响应最多4MiB。保存原始XLS字节、SHA/字节数、实际requested_at/observed_at、响应状态/最终URL/必要头和完成回执。错误/超预算/中断不得被loader当成完成；复用现有私有目录/原子文件工具，导入无网络或配置初始化。
2. **严格日样本解释。** 用xlrd读取有界原件，只接受一张样本表、完整九列表头和唯一日期/指数代码；每行代码、交易所与预期指数一致，代码以文本六位保存并保留前导0。拒绝重复/缺行/混日期/其他指数/未知交易所/坏文件/过多行；300和1000完整数量是必要条件，还必须逐行验证内容及真实官方请求绑定，不靠数量单独认可。空列表不可冒称完整。原件及回执摘要改动、实收晚于as_of、请求历史日期不同于表内日期时明确拒绝。实际响应时刻不能替换成历史日期。
3. **已有领域合同。** 解释结果是原`DailyIndexConstituentBatch`，sourceSHA绑定实算原件、observed沿实际回执，historical_retrospective/daily_complete_membership保持。归档入口接受显式日期日程与完成指数捕获根、已有完成证券来源及可选名称根；仅同日期实际完整输入才能生成原`FactorMemberDayInput`、原publisher/reader归档。单个CSI原件只覆盖一个日期，多根可组成显式完整日程（沿已有1024日/7000代码上限）；缺/重复/冲突日期或证券事实拒绝，不延伸相邻日期。必要证券事实消费原日迭代器，代码并集实算并核对来源变化与完整消费，失败不能留下成功完成manifest。不要在CLI另算选池或统计。
4. **聚焦行为证据。** 必要红到绿覆盖两指数准确文本代码与全部字段、重复/日期/指数/交换所/行数/原件错误、as_of/原件摘要变化、有限dispatch/网络失败/中断完成拒绝；注入HTTP seam离线实跑。至少一个跨日完整归档用原reader精确成员/来源对照，缺日期拒绝且无成功manifest；普通模块导入无网络/Settings。可用上述公开原件离线验证实际XLS/parser/CLI，但不得把调用注入或旧回执导入称作本次live。只运行新节点和必要直接旧依赖，既有21/61/259等有效结果复用，不追加全量或攻击矩阵。

## 执行与最终验收

implementer确认原生身份、父任务/root、分支、基准和唯一初始dirty；TDD后实际运行聚焦测试、Ruff/format/diff，记录每命令真实节点/运行时与资源。新依赖只在锁内提升，uv离线锁更新不得升级其他包；如环境不支持，停报告，不绕过。冻结干净candidate，再一次集中独立终审；原作者只修阻断finding，原reviewer按ID定向复核。

root接受后执行新live两次实际官方捕获，保存原件与HTTP回执，对照既有原件/逐行解释；最新有共同证券事实的日期按倒序验证，两池原归档/reader接通。只有真实未来收益尾部也具备时才执行实际worker评价，不把0评价日或合成收益称为真实指数检验。root正常收集固定清单、核对精确新增/原跳过不变、同步计数/SHA并运行两必要门禁，维护进度台账和本地集成。

整体goal active，M3仍部分。CSI历史完整覆盖、行业/市值、18:40跟踪、正式配置/数据代、完整资源/生产体验继续完成；本片不发布、切流或停Streamlit。

## Implementer验收记录（2026-10-01）

- 原生Codex桌面子任务 `/root/factor_security_collection_impl`，角色implementer、父任务 `/root`；实际Git分支与HEAD符合上述基准。初始仅root计划，最终写集为本计划、新collector、新测试、pyproject与uv.lock五文件。未委派、未访问网络、dotenv、凭据或生产。
- 新入口为 `python -m rquant.factor.index_collect {live,import-receipts,replay,archive}`。live使用固定官方URL、标准TLS验证、有限超时、无重试/重定向及实际dispatch预算；原件和回执绑定实算SHA与接收时刻，完成发布/中断拒绝沿已有工具。归档按批/日预验、完整消费与发布后复核，再由原publisher/reader消费v1合同；来源失败按自有dev/inode清理完成manifest。
- 按root明确的保守复用边界，证券源经既有 `all` 日迭代器准入，保留全部证券事实，CSI最后由原universe消费membership选池；未放宽ST/board未知、未将all已选成员替代证券事实。未来若需按CSI最少必需字段准入，应另行决定旧normalizer边界。
- 环境：绝对解释器 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration/.venv/bin/python`，Python3.13.12、pytest9.0.3、xlrd2.0.2、Ruff0.15.10。所有执行均用清空环境、`RQUANT_DISABLE_DOTENV=1`、全零测试token、自有离线配置；源码与测试通过Python3.11语法解析，未运行3.11/3.12解释器。
- 必要红测 `-m pytest tests/unit/test_factor_index_collect.py`：35 failed/1.50s（模块尚不存在）；初绿35 passed/4.76s。初绿之后仅Ruff格式化/导入排序，行为未改；按root收尾消息已执行的格式化后命令正常完成，35 passed/4.91s，无skip/deselect。日志/JUnit分别为证据根下 `red`、`green-initial`、`green-formatted`；精确35个新增nodeids保存在 `new-nodeids.txt`。
- 直接旧回归仅两个节点，2 passed/1.16s：`tests/unit/test_factor_member_archive.py::test_import_rechecks_already_read_inputs_and_closes_cancelled_iterator`、`tests/unit/test_factor_member_stream.py::test_reader_releases_daily_models_and_completes_only_after_natural_tail`；完整命令/输出见 `direct-old-regression.log/xml` 和 `direct-old-nodeids.txt`。本片37个不同节点；复用既有61/259/21证据，两个直接旧节点已在259内，累计不同有效节点376不是单次或本片执行数量。未跑全仓或清单生成器。
- 空自有uv缓存下初次 `uv lock --offline --no-config --python <上述解释器> --no-python-downloads` 返回1，缺AKShare跨平台缓存元数据，未联网。root针对这一环境事实授权仅精确补齐根dependencies/metadata.requires-dist的xlrd直接声明；实际 `uv lock --check --offline --no-config --python <上述解释器> --no-python-downloads` 返回0，102 packages/12ms，未复制本机缓存或安装依赖。TOML对比证明两个允许条目之外结构完全相同，pyproject仅新增 `xlrd>=2.0.2` 一行，Python支持仍 `>=3.11`，全部包版本/markers/哈希不变，见 `lock-structure-evidence.json`。
- 官方原件离线CLI执行一批import、两池09-30 replay、两池archive均返回0；09-29 replay返回2且明确缺该日。未执行live，import真实dispatch为0；原件/旧回执字节保持一致，300/1000名单、实际原件SHA和实际接收时刻沿归档日payload保留。原reader自然尾部 `require_completion()` 两池通过，reader沿原合同使用归档汇总SHA，未改该合同。
- 上述CLI归档只用明确合成的1300代码完整证券fixture；证明真实官方XLS/回执到现有归档的接线，不是实际全市场证券/未来收益或多日CSI实测。两日归档/成员黄金对照是单元合成fixture。真实资料仅09-30，历史缺日仍拒绝，32日CSI完整与3.11/3.12运行时均未在本片声称通过。
- CLI证据脚本修正了自身tests路径、reader返回request/汇总SHA及负向fixture资源归类，错误日志保留；已成功的六个CLI子命令未重跑。其五个返回0/一个预期返回2，逐命令耗时未持久化，分别保留两个执行批的聚合2.852409958s/2.185477834s；最终证据核验脚本返回0/1.072962833s，见 `official-cli-evidence.py/json/log`。这些脚本修正未修改产品源码或测试。
- Ruff check与format --check均返回0，`git diff --check`返回0。源码SHA256 `b789ab8e50e1c519353b0aedf0f7f129dd5af70b5e0e98a835560d0a3b4e0065`，测试SHA256 `0938ad53c28964afc033d4c5c73cb01f834b2abe8c4222df8e0a6cd1190409bc`；详细环境/语法/身份见 `source-environment-resource-evidence.json`。
- 证据根 `/private/tmp/rquant-index-implementation-b5xzah9b` 保留，成功资料目录700/文件600，无发布临时文件。十个中断标记仅是两次绿测的预期负向fixture，保留供复核；成功CLI资料无中断。所有自有命令/CLI子进程已退出，无后台服务或未完成工具会话。候选本地提交后停止，root安排唯一一次集中终审及真实live/真实证券归档验收。

## FIC-FINAL-01唯一一次定向修复

- 原候选 `e89479709c40e4c7e6bea2080c8d48c2c97ceabc` 集中终审唯一阻断为FIC-FINAL-01（P2）：publisher返回后首次manifest身份stat位于清理保护外，来源变化且该stat抛错时仍留完成manifest。开始修复前实际分支/HEAD符合候选，status干净；只改本模块、同域测试与本计划。
- 只将首次stat纳入原清理保护。若异常前尚未取得身份，使用同私有目录FD、`O_NOFOLLOW|O_CLOEXEC`打开该完成文件并由fstat取得dev/inode，关闭新增文件FD后沿原 `_cleanup_owned_temporary` 身份匹配清理和目录fsync。原publisher/reader、所有权检查与其他模块保持原合同；旧history_assemble同形stat登记为root后续backlog，未修改或扩展本片验证。
- 证据目录 `/private/tmp/rquant-index-implementation-b5xzah9b/fic-final-01`。reviewer原脚本只读复制到自有 `reviewer-red/repro.py`，SHA256保持 `8b2572f770af9d059303edcfb879623c77ba0683ae4c6f8ff04f40e427a89d14`；精确原反例实跑exit0/1.266589459s，确认1个manifest、原reader完整2日、来源loader拒绝，作为红证据保留，不修改审查者材料。
- 新增唯一节点 `tests/unit/test_factor_index_collect.py::test_initial_post_publication_identity_failure_removes_owned_completion`。实际红命令 `-m pytest tests/unit/test_factor_index_collect.py::test_initial_post_publication_identity_failure_removes_owned_completion -q --basetemp=<修复证据目录>/red-tmp --junitxml=<修复证据目录>/red.xml`，1 failed/0.68s，失败位于残留完成manifest断言。
- 绿命令只运行上述新节点、`test_source_change_during_final_publication_cannot_leave_consumable_completion`与`test_two_exact_days_archive_through_original_reader_and_pool_contract`（均在同测试文件），`-q --basetemp=<修复证据目录>/green-tmp --junitxml=<修复证据目录>/green.xml`，5 passed/2.29s，无skip/deselect。新失败路径完成manifest为0，原reader无法接受完成，来源仍明确拒绝；旧两日两池成功和既有尾部来源/read失败直接回归通过。完整5节点在 `tested-nodeids.txt`。
- Ruff首次仅SIM117提示测试with写法，合并等价上下文后check/format均exit0；测试行为未改，复用刚完成的5节点。3.11语法与diff检查exit0。原35中4个直接归档节点已重验，其余、原旧2、CLI、锁等证据保持有效，不重跑套件。`new-nodeids.txt`已更新全部36不同新增节点，累计有效不同节点377不是本轮运行数量。
- 修后源码SHA256 `dc2151902666f13171f0d0bc20e8bd80d607c38184685991b97e3cbaadb16ab2`，测试SHA256 `d64011c10a9855560d94ce7854d63839f4c5bfa685603c312730eab094d8fe90`，收据为 `repair-evidence.json`。环境仍原Python3.13.12与离线配置。来源变更仅限自有合成fixture；红反例成功manifest保留为证据，绿失败目录只保留日文件/输入且无完成manifest或发布tmp。所有自有进程/工具会话已结束，新增FD由finally关闭。
- 本普通任务唯一修复轮次到此用完；本地冻结新候选后停写，由原reviewer只复核此ID及修复直接回归，仍有阻断则交root决策，不继续第二轮或扩大范围。
