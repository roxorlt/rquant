# C3 日线指标与基本事实接入

## 范围、身份与收敛

用户已授权按可点击原型、新前端计划v2与全部差距持续实施。前一片MAD、行业IC和相邻期自相关已独立审查ACCEPT、真实32日验收并合入；本片只接已存日线事实到因子定义、检验及网页字段选择，不重新实现统计核。

普通任务：新增可选只读事实来源与直接接线，原鉴权、命令幂等、租约、恢复和生产合同沿用。若实际实现需要改变这些边界，先向root说明证据和范围，不自行升级或重设计。一次最终集中独立审查，最多一次原作者定向修复、原审查者复核；不叠加SPEC审查、逐子任务审查或全仓审计。

实际产品Codex desktop，原生root `/root` 为orchestrator、父任务为已授权整体goal。唯一实现树复用 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-tracking-react`，分支 `cdx/20261002-factor-daily-features`，规划基线 `c0edd7b7f79cd3d4272f70506ad7b56a6a8065b6`。旧分支保留，原作者均已停止，切换前tracked/untracked clean；初始唯一新文件为root写的本计划。实现者先记录实际角色、父任务、分支、计划精确提交、dirty来源及本树源码导入，再写代码；不得另建实现或审查树。

## 实际来源与语义

root于2026-10-02经已授权只读SSH核对固定RO副本：12项 `daily_indicator` 与4项 `daily_basic` 字段全部存在。倒序09-30/29/28，指标行数280/278/276，基本事实5561/5559/5557；09-30 MA20有限262、MA60有限231、供应方量比有限5556。原件与命令在root私有 `/private/tmp/rquant-daily-source-preflight-71mv44gy`，实际Python3.14.4/DuckDB1.5.2，1.077s、exit0、事务和连接结束、副本代前后相同。子代理不得读该真实资料。

这是局部覆盖预检，不证明全市场、完整历史或指标生产口径。现有技术核/回补可计算复权指标，但库存表没有每行生产版本、复权基准或历史初始化收据；本片只提供所观察的库存原值，来源明确 `historical_retrospective`、`stored_not_recomputed`，指标价格基准和递归初始化未核验。不得把库存标签改成“已核验复权指标”，不得用任意60/250日重新初始化RSI/MACD/KDJ补缺。

| 来源 | 字段 | 原值单位与口径 |
| --- | --- | --- |
| daily_indicator | ma5、ma10、ma20、ma60 | 已存均线数值；价格基准未核验 |
| daily_indicator | rsi6、rsi14 | 已存RSI点值 |
| daily_indicator | macd、macd_signal、macd_hist | 已存MACD数值；价格基准未核验；hist沿库存差值，不另乘2 |
| daily_indicator | kdj_k、kdj_d、kdj_j | 已存KDJ点值，不把J强制裁到0–100 |
| daily_basic | turnover_rate | 百分数原值，例如0.5387表示0.5387%，不再乘100 |
| daily_basic | volume_ratio | 供应方量比原值，不等同自行构造的vol/均量 |
| daily_basic | total_mv、circ_mv | 万元原值，不从旧市值上下文或其他日期补值 |

基本事实单位按官方[Tushare daily_basic](https://tushare.pro/document/2?doc_id=32)与已有采集/存储合同核对。有限库存值原样保留，不统一拒绝零/负数（MACD、KDJ等允许）；缺行、NULL与非有限值明确区分，不前填、不补零、不从结果删除原范围代码。

## 固定接线合同

1. 同域增加可选封存日线事实包与有类型按日reader，复用generation、私有文件及内容寻址物化工具，不引入来源框架或新服务。绑定原prepared SHA、snapshot/binding、scope、真实代码、code revision、读取截止/完成时刻、副本与sidecar代、表/字段/单位及原件SHA；原行情三表包保持精确合同。同一个只读事务消费本片两表，代变化、schema/主键或配对错误拒绝完成；只开RO，不stat/resolve/open主库。
2. 包与reader只证明实际观察的库存事实和覆盖。按日单次最多500码，只查所需字段；保留7000代码、来源4096日期、计算1024交易日、原AST200万工作槽与4MiB展示上限。不保留全历史DataFrame或所有逐日panel；完成/异常/关闭后句柄与私有副本释放，自然尾部失败不发布成功。
3. 可信配置新增默认省略的可选引用；原默认request/spec/config/journal/full/display规范字节与SHA不变。仅当定义确实依赖这16个字段时，将已核验包身份和实际字段集合冻结进新spec/输入摘要与原件；浏览器不提交路径、包身份或数据口径。配置虽有新包，纯旧六字段定义仍走原绑定和字节。
4. 准入、定义草稿和公式使用同一可信能力目录。新增字段只能来自已核验配置包，不能把旧全局daily_v1目录无条件改成22字段；缺配置保持原六字段与旧目录，单位/说明由后台单一类型源提供。原六字段定义catalog、默认摘要和旧恢复不被重编译。字段依赖、工作槽预算与有界源读取按实际字段集合计算。
5. 新字段与原raw六字段在真实前一SSE交易日配对，09:25回溯假设和完整日历不变；不能把当日收盘指标用于当日盘前，不能跳过缺日或只按已有行尾窗。只要所需字段未知就保留缺因，其他代码和主检验沿既有部分可用规则。原件重放验证新字段值、缺因及来源身份，沿原统计、发布、Serving与Web，不修改鉴权、ledger、lease、完成权威或固定v1跟踪策略。
6. 网页字段选择消费后台真实能力，16项均可按中文名插入定义；使用现有控件、键盘和390px触屏路径，单位/库存口径及覆盖说明放Tip或详情，不在正文堆内部字段/来源编号。保存、检验、历史结果与原请求恢复沿已有流程；不根据当前表单改写旧结果，不在浏览器重新计算指标。

## 顺序与写集

- 原生backend implementer先在唯一树实现：新factor领域模块，`capability/draft/run_configuration/run_backend/run_plan/formula_stream/stream_adapter/stream_runner/stream_job_artifact`及必要typed发布/Web/CLI接线、直接相关Python测试和本计划证据。公开DTO完成后给root准确形状与行为。不得写web生成物、测试清单、依赖锁、schema、指标生产核/采集器、鉴权/账本/租约/恢复或deploy。具体直接依赖超出此列表时先报告理由。
- 后端冻结后，原生frontend implementer顺序在同树接OpenAPI→TS、字段选择与真实合成浏览器证据、必要dist；不与后台并行写入。root负责清单新增精确节点及两门禁、候选冻结、唯一集中独立审查和接受后的真实只读对照、进度与本地合入。
- 子代理仅在指定树及自有private tmp离线开发/合成测试，不访问网络、.env、凭据、真实材料或生产，不继续委派或使用第三方模型。复用已知集成树Python3.13.12与本树已知Node依赖，显式PYTHONPATH加载本候选，禁止盲目安装、启动调度或全清单执行。

## 可验证验收

1. 直接原始SQL/手算黄金核对16字段、日期/代码/单位及每字段缺行/NULL/非有限；覆盖MACD负数/差值、KDJ_J超区间、换手百分数和市值万元、不相互补值。原prepared错代、scope/SHA/表配对、重复主键、损坏原件与自然尾部失败拒绝；500+1有界查询和已关闭reader真实验证。
2. 保存的可信能力、定义catalog和字段依赖一致；混合旧/新表达式、ref/时序预热、真实前日切换与中间缺日有独立逐值期望。只消费依赖字段、槽预算仍生效；没有任意递归指标重算或动态网络调用。
3. 完整合成配置→准入→原worker→journal/full/display→重放→Serving/Web路径包含新字段；来源或panel改动不能完成/被当原件发布。旧六字段与默认规范字节/SHA、原恢复、上下文及固定v1跟踪采用实际黄金与必要直接回归，不重复已验收统计数学。
4. 新字段网页插入/单位Tip、保存并运行、刷新/换代/失联原请求恢复、历史结果及空态按桌面/390px/键盘验证。接口更改只在最终候选运行仓库必需OpenAPI、Web/API、build/size/dist和浏览器门禁一次；失效范围定向补验，旧skip如实保留。root正常收集新节点，不执行全19,081清单。
5. 唯一reviewer ACCEPT后，root从实际固定RO保留独立原值，32日5571码范围逐日核对16字段及缺因/覆盖，与新reader完整消费一致；一个依赖新字段的真实worker与独立表达式验证前日配对、原件和发布。测量本次耗时/RSS与资源清理，不宣称全量覆盖、递归生产核/PIT、最大负载或生产上线。

## 后续与停止

库存指标全市场生产口径/长历史初始化、已有日线选股特征、分钟/竞价/温度/VP、CSI完整历史、正式配置/数据代与18:40服务和生产验收继续属于整体目标，不因本片来源接入而完成。没有push/tag/生产部署、切流或停Streamlit。

相关证据齐备后冻结干净候选并停写；范围外问题记后续。普通任务到唯一修复/复核上限仍有阻断时冻结证据、报告选项，不自动扩大审查或架构复盘。整体goal保持active。

## Backend 实施证据（2026-10-03，追加）

实际 Codex desktop 原生任务 `/root/factor_security_collection_impl`，backend implementer、父 `/root`；本树分支 `cdx/20261002-factor-daily-features`，实际 clean 基线 `6105e17280c244f50bde62aafc73229a85aeef58`，root 计划已提交，无未知 dirty。首56行原 SHA256 不变。只读借用 integration Python3.13.12/Pydantic2.13.1/DuckDB1.5.2/pytest9.0.3，显式本树 `src:root`、禁 dotenv/bytecode、`env -i` dummy 与自有离线目录；无网络/.env/真实资料/生产/第三方工具/继续委派/安装。

证据根 `/private/tmp/rquant-daily-features-implementation-jrKsUJ5h`（0700）：`identity.json`、`public-api.json`、`file-sha256.json`、`command-summary.json`、`valid-node-evidence.json`、`new-nodeids.txt`、`direct-old-nodeids.txt`、`resources.json`。精确20个 Python 写入文件与本计划的路径/字节/SHA 在文件清单，完整实际 argv/UTC/wall/exit、log/JUnit 在命令收据；失败原件均保留。

### 实际实现与必要直接依赖

新 `daily_feature_source.py` 同 pinned RO 事务封存两表16字段及代/范围/原prepared身份；严格 typed 单日 reader，500码分块、只读实际依赖、缺行/NULL/NaN/±Infinity 原因分开，库存原值不补值/重算。可信 capability/draft/config/plan 与前一 SSE adapter、输入摘要及 journal 接通。纯新字段表达式不依赖旧行情缺行，因子值与未来收益缺失保持独立；7000码/4096自然日/1024交易日/原AST预算保留。

root 明确确认的直接边界：`daily_stream.py` 默认省略 typed 单日输入；`stream_job_spec.py` 仅使用新字段时冻结配置原内容寻址 `daily_feature_lake_root`，`stream_job_runner.py` 核对真实执行根；`stream_job_artifact.py` 独立读封存原件核对原值/缺因、实际前日与来源 witness，自然尾部仍沿原完成权威。`result_serving.py`、实际 `web/models/factor_results.py` 与 `web/routes/factor_results.py` 只沿既有投影传 compact 来源/实际字段/覆盖，不泄露私有路径。`run_entry.py` 显式 `seal-daily-features` CLI 真实回执见 `cli-subprocess.json`/`cli-response.json`（exit0、0.573s），返回来源及可选配置引用。

额外 root 授权 `tracking_backend.py::compile` 的最小开始守卫：原入口只核对head/generation，精确红测真实入队而未拒绝；现以本片能力元数据在 enqueue 前拒绝依赖新字段的开始请求，不调用会额外拒绝旧算子的全局检查。新字段有/无当前来源均不能开始；原旧状态仍可取消，旧六字段、industry/size算子的 toggle 准入、原恢复沿旧合同。没有改 tracking runner/prefix/账本/lease/recovery 或统计核。

### 合同与有效验证

公开 run request 不变。可信 config 新 `daily_feature_source` 引用默认省略；仅实际新字段依赖的 spec/日输入/结果发回执。公共 compact `daily_features`、`daily_feature_coverage_days`（≤1024日）与字段单位/库存边界来自同一类型源，私有 source/query/config schema 仅供可信入口。16字段能力新增 `tracking_supported=false` 与 `tracking_unavailable_reason_zh="库存日线字段暂不支持持续跟踪。"`，原六项省略且 catalog 不重绑定。准确 schema/签名/文件行号在 `public-api.json`。

新增22 unique、直接旧16 unique，集合不重叠，局部重验不累加。`source-red` 9failed、`pipeline-red` 3failed；`focused-final-green.xml` 20passed/10.816s，`direct-old-regression.xml` 13passed/10.995s；最后原件日历/能力变动只补 `final-direct-green.xml` 3passed/4.786s。能力元数据及开始守卫各有1failed精确红测。`tracking-admission-green.xml` 实际4passed/1failed（上下文夹具错误，该命令仍failed），只复用成功的新增守卫1和旧PageControl3；上下文夹具 typed catalog 错误原件另保留，精确修正后 `tracking-old-contract-corrected-green.xml` 1passed/2.319s。其余早期夹具/格式失败保持failed；一次未引用方括号的shell glob在pytest前失败，不计覆盖。Ruff check/format check 全20文件passed、`syntax-311.json` 为3.11 grammar及当前runtime compile，不冒充实际3.11运行。没有全仓/API/Web套件或额外审查。

`legacy-canonical-equality.json`：仅将本树基线 `git show` 的16个已改源码文件保存自有私有目录（214,639字节），同一合成封存源分别真实基线/当前 worker，request/spec/config/full/display五份规范字节和SHA、completion全相等，当前原件独立重放通过；旧v1固定SHA节点也passed。独立原SQL/手算核对16字段/单位/状态、J超100/hist差值/负零、前日/ref预热中间缺日；实际 trusted PageControl→worker→journal/full/display→replay→Serving/Web通过。自然尾部变动无成功completion/full/display，重算所有外层摘要的伪改新字段值仍被原件拒绝；单日对象下一日之前释放。

### 资源与未完成边界

自有 command sessions 已退出，无后台 daemon/listener。Web TestClient lifespan关闭，本树 `tests/.daily-features-web-*` proof目录删除，reader异常/成功均close并删除私有副本，当前私有副本/本树proof均0；CLI子进程退出及fixture删除见 `cli-resources.json`。其余自有0700 pytest/黄金封存合成资料明确保留供复核，非运行资源。

不声明真实全市场/32日worker、PIT/复权初始化、最大负载、网页按钮或生产上线。新增字段持续跟踪尚未接通，后续立即补相应prefix/策略；本片诚实禁止开始。frontend待干净后台候选后顺序接线，root负责最终组合门禁/唯一独立审查/接受后真实只读验收；整体goal继续active。

### 字段说明局部收尾

root 在干净 `93f82954fbab7ea2a5267cd758917abc8d01e375` 交FE前授权单一字段目录文案修正：MA/RSI/K/D只说明对应指标；J才说明不裁到0–100，MACD分别为DIF/信号线/柱差值，仅hist说明已存DIF−DEA、不另乘2。基础4项、字段/单位/DTO、原值、旧六字段及其他逻辑不变。说明文字属于固定来源绑定，旧合成新字段包在新目录下明确拒绝，原件原封保留于证据根 `metadata-wording/before-*`，当前CLI真实重新封存回执已更新。

只补实际失效范围 `wording-direct-green.xml`：原source原值/状态、能力目录、完整worker→原件重放→Serving/Web3节点passed（4.792s wall），22新nodeids不增加；16旧及五份旧字节黄金继续有效复用，没有扩大套件/审查。`metadata-wording/binding-and-strings.json` 核对仅中文名/说明变化、基础4项与旧六项不变及旧新字段包拒绝；public-api、SHA/handoff随新候选更新。CLI/reader/proof资源按同一收据关闭删除，Ruff/格式/3.11语法与diff直接自检保留。

## Frontend 实施证据（2026-10-03，追加）

实际 Codex desktop 原生任务 `/root/factor_save_react_final_review`，当前 frontend implementer、父 `/root`；同树同分支 clean 基线 `ed7d35c7ac0d9ac350c44ab9eefa049db30e59a0`。首56行保持原字节。只写直接相关 web、规范生成的 OpenAPI/TS/dist 及本附录；无 backend/Python test、依赖、权限或生产修改。实际 Node22.22.2/pnpm10.33.0、只读借用 Python3.13.12/Pydantic2.13.1，显式本树源码、禁 dotenv/dummy/private 夹具。公开合同与字段说明沿 backend `public-api.json`，SHA256 `0864cf9056534de706458a31a35034bdfecbda97097b35fc50c7f9a3be422fdc`。

字段默认按实际 API 顺序显示六项，“全部字段”与中文搜索覆盖完整目录；独立说明 Tip、真实选区插入/光标、搜索 Enter 不保存。可信同代能力与已保存依赖只限制新加入跟踪，取消和原请求恢复仍可用；保存/单次检验不受新增跟踪限制。历史结果的库存口径与按日原始覆盖只来自自身结果，不用当前能力/表单重写。

证据根 `/private/tmp/rquant-daily-features-react-gjh9cpxp`（0700），实际 argv/UTC/wall/exit/log/XML、版本、节点与 SHA 见 `evidence.md`、`command-summary.json`、`handoff.json`。新增 Web 19 unique、浏览器2 unique、Python 0。聚焦首次19项15failed/4passed，定向最终19passed；直接回归原件91passed/1failed（旧断言持有已卸载按钮），保留原断言意义后精确1passed/26deselected，最终正常 `pnpm check` 58文件644passed。正常 Web/API 55文件993passed，build/size passed，首屏 gzip323.1KB/550KB；产物未提交导致首次 `verify:dist` 比较失败原件保留，精确提交后正常验证 exit0。Git 默认 sandbox 的 index.lock 失败原件明确记录，随后正常 require_escalated 精确提交。

完整浏览器原命令119passed/2failed/4skipped：新增取消夹具立即换代碰到旧代在途查询；首次定向又因响应定位漏 `/app/` 前缀失败，均保留原件与 trace。仅改该合成夹具为 applied 等待同步、旧查询完成后显式发布并刷新，保持原请求和精确代断言；最终桌面1440/390触屏2passed，unique有效121、原配置4skipped不计通过。测试类型/格式直接验证 exit0，生产源码/dist 未再变更，复用仍有效644/API/build/size/dist，不再完整重跑。最终四张编辑/结果截图已实际目视，路径/SHA 见 `screenshots.json`。

所有自有命令退出，18817/14217释放，临时 proxy proof已删除，借用 node_modules保留。普通删除自有 API basetemp `rqdy-api-dmbjppvc` 与 `browser-serving` 遭封存只读夹具 PermissionError；准确路径、创建归属与保留原因见 `resources.json`，未改保护或其他目录。日志/XML/截图/合成原件保留供唯一集中审查。此证据不代表真实材料、32日worker、全市场生产口径或生产上线；接受后真实只读验收仍由 root 完成。

## Root 清单与联合审查冻结（2026-10-03，追加）

前后端均已提交并停止写入，frontend候选 `4d882f4cd1716802899c514694a27d771fd255fd` clean。root逐文件核对54项frontend SHA、28份实际命令日志及11份JUnit原件；失败/定向修复记录保留，unique浏览器有效121与原4跳过不相加重复。四张截图已实际查看，首56行冻结SHA保持不变。后台20项Python源/测试未被frontend修改。

正常隔离收集 exit0（Python3.13.12，8.119s），清单从19,081增加22个精确新节点至19,103；旧节点零移除、零重复，55项既定skip原字节不变。新逻辑摘要 `40deb84aa5071e4d5079d94204b8fb72a95e015d9d7223b64e626838be943eca`。只更新清单及测试说明/精确计数；两项仓库清单门禁实际2passed、exit0、8.381s，没有执行19,103项全量套件。证据根 `/private/tmp/rquant-factor-tracking-root-yegakn31/daily-feature-manifest-proof`，正常collect/比较/install/两门禁的原日志、XML和SHA收据保留。

最终候选包含前后端、必要生成物和本次清单，唯一原生reviewer `/root/factor_security_collection_final_review` 只审精确diff、直接依赖、上述验收与有效原件；普通任务最多一次原作者定向修复、原审查者复核。真实32日逐值及worker验收留给接受后的root。全量生产覆盖、指标初始化核验和持续跟踪仍列后续，不把本片单次检验等同于整体验收。


## Root真实只读及本地验收（2026-10-03，追加）

唯一原生reviewer `/root/factor_security_collection_final_review` 对 `52e1bca5bede7ca03c32dd51108fa20f328b733f` 相对 `6105e17280c244f50bde62aafc73229a85aeef58` 一次集中终审ACCEPT、无DFE阻断、零产品修复轮。报告 `/private/tmp/rquant-daily-feature-final-review-wbbndhrd/review.md`，SHA256 `ac0528cc7e370322b52961d19fa2e20c025294f79f8615c2a4c6e6e9f45ea974`；没有重跑有效套件或读取真实资料。首56行原SHA保持。

root在接受后由精确Git src归档、34份已接受原成员文件及独立参考构造8,985,456字节私有包，实际SSH固定RO只读/自有远端tmp：Python3.14.4/DuckDB1.5.2，原RO代前后相同。32日5571码两表16字段2,852,352格、768次最多500码reader查询逐项核对原SQL原值及缺因；所有有限值位相同，负MACD13,656格、J出范围1,506格保持。真实非有限及负零未出现，合成矩阵补其异常证据。

新库存表达式 `rsi14 / 100 + turnover_rate / 100 + total_mv / circ_mv + ts_mean(volume_ratio,2)` 在09-24/28/29三个最新成熟评价日走真实trusted配置/worker/journal独立sealed replay与Serving投影；515有效值/14,552缺失结果与独立stdlib原始参考一致，最大误差3.552713678800501e-15（事前冻结容差2e-12+1e-12*abs(expected)）。来源和逐日覆盖与发布投影相等、无私有路径。真实32日为reader域，不宣称本次worker计算了32日。

本次价格准备9.107s、库存准备3.765s、原值reader对照38.061s、worker62.437s，含独立重放/投影总154.974s、峰值612.344MiB；SSH含打包/传输159.374s、exit0。读句柄/湖句柄/心跳/私有scratch归零，自有远端tmp删除，无provider HTTP/生产写入/生产发布。原件 `/private/tmp/rquant-factor-tracking-root-yegakn31/daily-feature-proof/worker-response.json` 及dispatch/command/stderr，独立原始参考SHA和实际清理收据保留。root汇总收据首次将空心跳列表误比数字0，失败原件保留，按实际类型定向修正；产品与真实worker未改/未重跑。过期API/browser合成目录已按创建归属/owner/无句柄核对，恢复普通只读目录的owner写权限后标准删除；首次browser创建归属断言选错receipt保留，改核真实e2e job后删除，未动flags/ACL/平台保护。

本地集成树从 `c0edd7b7f79cd3d4272f70506ad7b56a6a8065b6` 快进合入受审候选并核对clean。仅本片进度/顺序/Changelog与验收附录收尾；当前实现树保留用于已明确下一片库存字段跟踪，不增建worktree。原用户主树及其他任务未接管。新增字段跟踪、指标全市场生产口径/初始化、既有选股特征/分钟/竞价/温度/VP、CSI完整历史、正式配置/数据代/18:40及生产验收继续完成；M3部分、整体goal active，没有push/tag/生产部署或停Streamlit。
