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
