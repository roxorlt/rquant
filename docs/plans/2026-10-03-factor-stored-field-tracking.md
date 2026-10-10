# 库存日线字段持续跟踪

## 目标与边界

接通已接受的12项库存技术指标及4项日线基本事实的持续跟踪，使已保存定义可加入/取消、按成熟交易日追加，保持真实缺失和回溯研究口径。前一片候选52e1bca5已唯一终审ACCEPT、32日库存reader及3日worker真实对照通过、文档合入8a3d10a9，本片顺序开始。

普通任务：只扩展既有因果输入的可选事实分支与能力准入，原账本/幂等/租约/恢复/并发提交/生产契约不变。若必须改变这些边界，实现者先停写并向root报告实际证据，重新冻结范围与等级。一次最终独立联合审查，最多一次原作者定向修复/原reviewer复核；不额外叠加子任务审查或全仓审计。

实际Codex desktop原生root `/root` 为orchestrator、父任务为用户已授权整体goal。唯一实现树复用 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-tracking-react`，分支 `cdx/20261003-factor-stored-tracking`，规划基线 `8a3d10a98707449a30b2e14f6dc7968b93044d0e`。前一片作者和reviewer已停止，切换前后tracked/untracked clean、无未知dirty，原分支保留；没有增加worktree。实现者须记录原生实际角色、父/root、此分支/工作树、root计划精确提交、冻结前缀与源码导入证据；仅root可委派。

## 直接实现

1. tracking_runner._input_files仅在spec确实依赖库存字段时增加已经配对的来源原件witness；read_factor_tracking_prefix在同一ExitStack打开已有typed封存reader并传给原adapter。500码/按日/实际依赖字段约束、实际前一SSE日和完整日期序列沿原路径，不重新初始化或补写指标。
2. 没有库存依赖时保持旧v1 semantic tuple及prefix字节精确不变。新依赖在明确分支附加有序字段列名、trade/panel日期、code与有限/缺行/NULL/非有限状态和原值等逻辑事实；库存source SHA、generation、路径、文案和回执摘要是来源证明，不作为跨代逻辑值。reader/源witness仍用于同次执行防漂移。
3. 原_prepare/worker前/worker后/原_commit前prefix与witness核对继续生效，打开的新reader/临时副本在原子追加前关闭。不得修改_reserve/_commit/账本/租约/recovery或成功发布权威。普通缺失保留统计的partial语义；历史已消费事实改变沿原冲突暂停，不追加伪成功。
4. 后台直接验证后才开放新字段tracking能力；实际缺可信库存来源或与冻结定义不配对时开始仍在enqueue前拒绝。取消不依赖新增来源。无需改库存字段单位/名称/源包格式或daily_stored_v1定义目录。旧六字段与industry/size准入保持原行为。
5. React沿可信同代能力开放加入，原命令/取消/原检验恢复、历史结果自身来源仍保留；说明短中文，细节Tip。实现者按实际必要范围调整测试、生成OpenAPI/TS/dist，不引入浏览器路径/口径输入。

## 写集与顺序

backend：tracking_runner.py、tracking_backend.py、capability.py，新增直接跟踪测试及受影响的旧日线字段测试和本计划证据。其他实际直接依赖先报告root原因，不自行扩张。不能写web生成物、测试清单、依赖、生产或数据。backend冻结后frontend同树顺序接页面/测试与必要规范生成物。root最后正常收集精确节点、两门禁、冻结唯一候选、唯一reviewer、真实验收、本地合入与文档。

原生子任务只访问指定树、自有private tmp和已知只读依赖；明确禁网络/.env/凭据/真实材料/生产/第三方模型/继续委派/新worktree。root真实只读资料不向孩子提供。

## 可验证验收

- 正常原toggle入口到configured worker、journal独立重放、跟踪日贡献与Serving状态，新定义首段+后续增量等于同source整段；重复同日不累计，成熟尾部/缺失保留。
- 新16字段库存状态及混合旧/新表达式、时序预热、真实前日、中间缺日有独立期望；旧纯六字段prefix黄金精确一致，虽配置有库存源但未消费时旧prefix不混入它。
- 同逻辑值的新来源代不改prefix；已消费的旧日原值或missing/null/nonfinite状态变化触发原历史冲突暂停，历史条目/游标/贡献不追加。不能用新source SHA变化代替逻辑比较。
- 开始前缺来源拒绝且未enqueue，原取消/原恢复仍可用；处理中新增原件witness变动或源尾部失败不能提交成功，reader/copy关闭，500查询边界真实验证。测试限此新增路径和直接回归，不重新审查旧故障模型。
- desktop/390px/键盘验证加入与取消、能力缩减、原请求保留；最终必需Web/API/build/size/dist/浏览器门禁一次，有效旧结果复用、失效范围定向补验。root精确收集清单，只执行两必要门禁，不执行整清单。
- 唯一ACCEPT后root在固定RO用独立原件做实际新字段跟踪的分段/整段/重复对照，记录真实覆盖、耗时/RSS及资源清理。受测范围以实际原件为准，不把32日reader核验声称为32日跟踪worker或PIT/全市场初始化/最大负载。

## 后续

本片接受后继续库存指标全市场生产口径与递归历史初始化、已有选股日线特征、分钟/竞价/温度/VP、CSI完整历史、正式数据/配置/18:40与生产UI验收。M3仍部分，整体goal active；生产部署/切流/停Streamlit按已有单独授权规则。

## backend 实施与证据（2026-10-03）

- 实际原生 implementer `/root/factor_security_collection_impl`，父 `/root`；本树/分支起点 `9197abd1acec5ced495dac7c30b16e4befdea049` clean，产品 Codex desktop。身份、源码导入和环境回执在 `/private/tmp/rquant-stored-tracking-implementation-hy_tbd2h/identity.json`。首36行SHA仍为 `1bdb4a5b0b88bc49c52fabae0eeb432d43c4a604879d9c0aa9f9ce2e0317b058`。
- 写集仅三项 factor 模块、`test_factor_stored_tracking.py`、直接受影响的 `test_factor_daily_feature_pipeline.py` 及本附录。实际依赖才打开配对库存 reader/双表 witness；prefix 附加有序列名、trade/panel、code、status/value/nonfinite。来源代、路径、文案和receipt SHA不进入逻辑事实。cap 的 `tracking_supported` 支持 bool，可信已载来源16字段为true；缺来源/缺原件在开始前拒绝，取消不打开新reader；旧六字段省略此元数据。
- `initial-red.xml` 两项实际红测：旧开始守卫及 prefix 缺 reader。`initial-green.xml` 为1失败1通过；worker已成功但追加拒绝，原因是 final-prefix 新reader清理私有目录改变 lake mtime，晚于原件 witness 冻结。root批准仅新字段分支先读/关闭/比对 final-prefix，再 verify 原件并 recheck witness/loaded；旧分支顺序不变。`_reserve`/`_commit` AST与基准相同，不修改共享 witness、事务或恢复。
- 最终有效节点按 testcase 去重为14新增（13新文件节点+1改名替代）及9旧直接回归；原 `test_tracking_stored_start_is_refused_before_enqueue_but_existing_state_can_cancel` 移除，替代为 `test_tracking_stored_missing_source_is_refused_before_enqueue_but_existing_state_can_cancel`，原因是已有可信来源现在应支持开始，不能保留旧误导验收名。精确nodeids、removed→replacement、逐命令UTC/argv/exit/wall/XML和SHA见私有 `evidence-summary.json` / `new-nodeids.txt` / `old-nodeids.txt`。
- 有效绿来自 `stored-core-green.xml` 的6个成功case（整命令另1个fixture身份失败，保留原件）、`stored-paths-green.xml` 5 passed/9.52s、`stored-final-paths-green.xml` 3 passed/5.88s 和 `direct-regressions-green.xml` 10 passed/11.44s；重复节点不累加。实际原toggle→首段1日+追加2日→sealed journal手算/独立重放→同源整段daily values/贡献/prefix/Serving摘要一致，重复不追加。四种历史原值/状态修订暂停；同值新代prefix不变；16字段所有状态、SSE休市前日/预热/中间缺行、501码分500+1仅读依赖字段、源自然tail及关闭后verify期间原件变更拒绝且无追加均已验证。
- `legacy-prefix-proof.json` 用基准实际函数读取同一合成来源，对照当前有/无未用库存配置，旧v1两日prefix精确一致；测试固化实际黄金。Ruff/format check和5文件Python3.11 AST语法通过；最终源码/测试SHA、public shape、自然reader/copy与执行会话清理见 `source-resource-proof.json` / `public-api.json`。格式化/导入排序不改变行为，有效旧来源/统计证据复用，未跑全仓/API/FE门禁。
- 环境为只读借用 integration Python3.13.12/Pydantic2.13.1/DuckDB1.5.2/pytest9.0.3，显式本树PYTHONPATH，禁dotenv/bytecode、隔离dummy配置和自有TMPDIR。所有命令已退出，reader scratch/执行会话零残留；自有合成资料保留于0700证据根供最终复核。未接触真实资料/网络/生产；FE组合门禁、唯一联合审查和root真实跟踪对照仍待后续完成，未宣称生产上线或整体goal完成。

## frontend 实施与合成门禁（2026-10-03）

- Codex desktop 原生 frontend implementer `/root/factor_save_react_final_review`，父 `/root`，起点 `ad52729ba5280d38e4239e46b49df3babfde16ce` clean。证据根 `/private/tmp/rquant-stored-tracking-react-pv55stie` 为自有0700目录；身份、当前树导入、生成合同来源及完整实际argv/UTC/exit/XML见 `identity.json`、`fixture-source.json`、`command-summary.json` 和 `evidence.md`。首36行冻结字节保持不变。
- 现有同代能力判断、加入/取消及原请求恢复已满足本片，不修改生产hook/UI。当前可信22字段夹具取自后台 `public-api.json`（SHA256 `7bd229d6049f72a26552dd08affc818e0ec7610772e904384bc049aded655541`），经实际Pydantic Web模型JSON验证生成，16项支持跟踪；原false夹具明确保留为独立不可用场景。CLI重新生成OpenAPI与TS，旧false类型的16项真实编译红测exit2，生成后类型绿；不是手写业务DTO。
- 新增8项组件行为及2项桌面/390px浏览器节点：可信当前head加入、固定策略不绑定左栏未提交参数、锁内能力缩减、缺来源/错代/失败时取消独立、丢回执/刷新/换代保留完整原请求和原head、账号变化禁写、receipt与投影精确确认。新增Python节点为0；后台有效23节点不重复执行。
- 唯一最终门禁实际结果：`pnpm check` 59文件/652 passed（exit0/31.064s），API的55个 `tests/unit/test_web_*.py` 文件993 passed（exit0/251.049s），`pnpm build` exit0/5.155s、首屏gzip323.1KB/550KB，`verify:dist` exit0/1.028s。正常浏览器门禁123 passed、4项原配置skipped（exit0/222.559s）；跳过不计通过。原生产UI和dist字节未变，不重复有效门禁。
- 四张1440×900与390×844确认/原请求恢复截图在私有 `browser-results/factor-stored-tracking-*`，均实际查看；策略Tip支持键盘/手机点按、确认固定5组与左栏10组独立，旧第2版原操作不套新第3版。原编译红、夹具helper严格tuple解析失败和一项格式失败均保留原日志，修正后实际绿，不删除失败原件。
- 实际运行时Node22.22.2/pnpm10.33.0及只读借用Python3.13.12/Pydantic2.13.1/DuckDB1.5.2；显式本树PYTHONPATH、禁dotenv与隔离dummy配置，只连接自有loopback/Unix合成夹具。服务/命令退出与端口归零证明由 `resources.json` 记录；自有API basetemp及浏览器合成资料保留供唯一联合审查，不改保护权限或删除未知目录，root依赖目录保留。root清单门禁、唯一联合审查和ACCEPT后的真实只读跟踪对照尚待完成，本附录不声称真实worker或生产验收。

## root 清单与最终候选冻结（2026-10-03，真实验收前）

后台 ad52729ba、前端45ccdf4e已各自提交clean并停写；root核对后台6文件、前端7文件、20份前端原log及8份新旧原XML。23个后台不同有效节点（14新增含1replacement、9旧）、Web652记录、API993、浏览器123通过/4原skip及build/size/dist有效证据复用；4张1440/390确认与恢复图已实际查看。生产hook/UI及dist字节不变；类型仅tracking_supported扩为boolean/null。

正常精确collection实际exit0/8.585s，19103→19116，精确增14/删1对应已记录replacement、0重复、55批准skip字节不变。全套逻辑SHA154f4889ecbcc2f512456aad9cf42d8544361b542e3a8bacdef81c781c6e868d；只执行两必要清单门禁，实际2 passed/8.484s，不执行19116全套。root将本候选冻结后交唯一原reviewer集中终审（当前0修复轮）；ACCEPT后再按独立原始事实执行首1日/续2日/整3日及重复的实际只读跟踪对照，不将前片32日reader核验声称为32日新字段tracking。

root证据在/private/tmp/rquant-factor-tracking-root-yegakn31/stored-tracking-{backend-freeze,frontend-freeze,root-visual-check}.json及stored-tracking-manifest-proof/；原红测、部分失败及helper失败全部保留。前端自有端口18823/14223已释放，合成资料暂保留给终审。计划前36行冻结SHA保持，后台5个Python字节保持；M3仍部分/未发布，整体goal active。

## 根任务最终验收（2026-10-03）

最终代码候选 `9500818f65d29b09c4ffdbbc215a2d4bed3dcc3b` 已由唯一原生独立 reviewer 集中接受，报告 `/private/tmp/rquant-stored-tracking-final-review-c_gpmmky/review.md`，SHA256 `699ef892d79b1399e0183b03c77c2a8eab78d19e7ceaf2bfaaa8ac32f8c92465`。无阻断 finding，0产品修复轮；审查复用仍有效证据，没有重复套件。root在真实对照成功后将候选快进合入本地研究集成分支；未发布、切流或停用Streamlit。首36行冻结字节保持。

root仅用固定只读副本及既有原始事实，实际一次远端分发、三个worker任务：首段1日、追加2日、同源整段3日。计算范围5,571码，评价日为2026-09-24、09-28、09-29；表达式 `rsi14 / 100 + turnover_rate / 100 + total_mv / circ_mv + ts_mean(volume_ratio,2)`。首日171有效/4,850缺失；续段344有效/9,702缺失；整段515有效/14,552缺失（14,533缺观察、19空值）。与独立原始参考的最大绝对误差3.552713678800501e-15，容差固定2e-12＋1e-12×abs(expected)。分段与整段逐日贡献、汇总和完整逻辑前缀完全一致；重复返回等待、无新任务或追加，原取消成功且保留两个持久历史运行。

准备4.733s，首段91.830s，续段157.482s，整段156.933s；整次worker证据420.585s、SSH墙钟425.458s，峰值629.695MiB。源副本物理身份执行前后相同；源和自有句柄0、worker心跳线程0、私有scratch空，远端自有目录已删除。没有提供方HTTP、生产写入或发布。证明为历史回溯研究；不证明PIT、库存指标全市场生产口径/递归初始化、32日新增字段跟踪或最大真实负载。

有效聚焦证据为后台23个不同节点（14新含1明确replacement、9旧）、Web652记录、API993、浏览器123通过及4原配置skip；跳过不计通过。类型检查、构建、首屏323.1KB/550KB与产物一致性通过，4张桌面/390px确认与恢复图已由root和reviewer实际查看。清单19,103→19,116，精确增14/删1对应replacement、无重复，55批准skip字节未变；仅两必要清单门禁2通过，不执行19,116全套。

原件与验收收据：`/private/tmp/rquant-factor-tracking-root-yegakn31/stored-tracking-accepted-real-proof.json`、`stored-tracking-proof/worker-response.json`及同目录原command/stderr；后台/前端冻结和原XML、清单原件保持。首次root本地收据断言把报告中文句号当ASCII句号，实际exit1且未分发worker；`stored-tracking-proof/acceptance-receipt-first-attempt.json`保留，随后仅修正收据helper，未改产品或增加审查轮次。原红测、部分失败和helper错误均保留。

终审和真实对照完成后，root核对来源、设备/inode和无打开句柄，删除两份过期自有合成目录，实际释放3,624,679,432B；日志、XML、trace和截图保留。清理证据为 `stored-tracking-fixture-cleanup-preflight.json` / `stored-tracking-fixture-cleanup.json`，未改变平台保护；当前任务依赖因下一片有实际用途而保留。

下一片只读预检已确认：正式只读副本日线7,338,351行/5,789码，而库存日线指标仅24,421行/280码；日线始于2020-08-24，复权因子始于2024-09-02，不能宣称上市全历史初始化。后续实现有明确价格口径与初始化边界的全市场技术指标研究来源，继续已有选股特征、分钟/竞价/温度/VP、CSI完整历史、正式配置/数据代/18:40与生产UI验收。M3仍部分，整体goal active；不重复已验收的库存来源与跟踪实现。
