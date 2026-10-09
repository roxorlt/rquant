# React 投研平台：交付清单

更新：2026-10-09 18:46，上海时间。

## 当前进度

| 项目 | 当前值 |
|---|---:|
| 固定交付项 | 25 |
| 本地已验收 | 19 |
| 剩余交付项 | 6 |
| 追加交付项 | 0 |
| 当前工作流 | 最终候选已冻；提交同一候选并启动仓库CI |
| 已迁移生产模块 | 0 / 15 |

19项表示本地功能已验收。正式资料、整体候选、上线和市场观察仍须完成。
各项大小不同，19/25不表示工作量百分比。
原型范围为15个模块，M11实盘交易排除。14个模块已本地验证；M3仍缺正式指数历史来源。

## 剩余6项

| 编号 | 交付项 | 还需完成 | 执行估计 |
|---|---|---|---|
| R10 | 完整指数历史 | 取得可用的历史成员、实际生效日和事件覆盖 | 等来源 |
| R11 | 真实历史验收 | 补齐正式资料和旧完整流水，完成32日验收 | 等资料 |
| R12 | 整体候选验收 | 版本、身份、界面、定向复核及最终构建已验；仓库CI和合法批准后的正面链仍待 | 3–6小时 |
| R13 | 正式安装 | 固定版本和真实来源；获授权后安装并核验回退 | 2–4小时 |
| R14 | React切换与旧服务退役 | 按已验功能切换，获授权后停用旧单元 | 1–2小时 |
| R15 | 模拟盘真实观察 | 正式批准后累计20个实际开市日 | 20个开市日 |

R02、R06已本地验收。新版竞价创建、旧请求恢复和结果兼容均有实际验证。
整体验收和上线仍估6–12小时。资料与授权等待、20个实际开市日另计。
估时不是停止上限，也不保证完成日期。原10月8–10日候选窗口保留，仍有延期风险。

## 本轮已关闭

- 新增34+7叶CI差异由原审查者定向复核通过，FCR-001保持关闭，0新增阻断。完整收集和精确元数据已冻。

- 必要CI前置已局部修复：原路径6项、归档类别17项、固定输入17项回归通过；5叶导入成功收集102个节点。未运行测试正文的收集结果不计测试通过。

- 唯一集中审查中的FCR-001已关闭，原审查者定向复核通过，无新增阻断。
- 最终前端只构建一次；79个产物和423个源/合同输入已核，首屏370.6 KiB低于550 KiB。有效测试继续复用。
- R02、R06：完整参数及三策略最终集成通过。后端56个不同节点、前端24个不同节点有绿证据；未选择的节点按单独证据复用。
- 新竞价使用已确认口径；旧v1请求、封存标识和代码指纹保持。类型、格式和一次构建通过。
- R07：原11种评分、搜索、训练选择和热图已验；详情9.02秒，热图8.69秒。
- R08：原五组结果、真实接口、桌面和手机刷新重开已验；最慢11.87秒。
- R09：原两窗结果、真实接口、桌面和手机刷新重开已验；最慢5.90秒。
- 前端相关19项、类型检查、一次构建和包大小检查通过。
- 原数值、零交易、缺值和负收益保留。浏览器及服务已退出，临时根已删除。
- 不追加这三项的执行或逐项审查。最终同一候选审查统一归R12。

## 仍需答复或补充

| 事项 | 当前证据与下一步 |
|---|---|
| 竞价次日数据口径 | 已确认：分钟候选只用当时已知资料；跳过非交易日；缺后续价格的相关成交或收益保持不可用。旧日线默认与行为保持。参数及结果接线已本地验收。 |
| Linux测试依赖 | 两份隔离环境已构建并核验：Python 3.11.16和3.12.14、固定uv 0.10.11，各23模块加载通过。临时容器及配置已清理，镜像保留供后续验证。 |
| CSI及真实历史资料 | 当前公开来源和现有原文件不足；新资料到位后继续，不伪造时点事实。 |
| 正式安装、切换和停服 | 按项目AGENTS.md取得相应授权；使用精确版本与已准备的回退路径。 |

历史重建及新旧成交口径已确认：保留原件，按分钟结束时刻和上一交易日完成候选重建；缺少当时可见资料则保持不可用。
新旧引擎按各自冻结成交规则比较，未解释差异须为零。20个实际开市日另行完成。
R11的准备、标签、比较和命令入口均已完成；这不替代正式32日验收。

Linux环境证据：[依赖准备验收](../../data/verification/goal-progress-root/linux-dependencies-accepted-20261009-01.json)。环境就绪不等于R12整体验收完成。

## 已验收的19项

| 编号 | 交付项 | 验收证据 |
|---|---|---|
| L01 | 数据中心 | [记录](2026-10-06-data-center-integration-acceptance.md) |
| L02 | 研究环境 | [记录](2026-10-05-research-query-acceptance.md) |
| L04 | 选股与池子 | [记录](2026-10-06-screener-integration-acceptance.md) |
| L09 | 组合风控 | [记录](2026-10-06-paper-portfolio-integration-acceptance.md) |
| L10 | 模拟盘 | [记录](2026-10-06-paper-portfolio-integration-acceptance.md) |
| L12 | 监控告警 | [记录](2026-09-27-react-platform-progress.md) |
| L13 | 调度任务 | [记录](2026-10-06-task-center-integration-acceptance.md) |
| L14 | AI 辅助 | [记录](2026-10-06-ai-assistance-completion.md) |
| L15 | 协作权限 | [记录](2026-10-06-collaboration-completion.md) |
| L16 | 运维健康 | [记录](2026-10-06-health-completion.md) |
| R01 | 真实日末流水与净值 | [记录](../../data/verification/strategy-promotion-20261006/native-domain-implementation-15/r01-daily-chain-result.json) |
| R02 | 三条策略完整验证 | [记录](../../data/verification/goal-progress-root/auction-policy-r02-r06-accepted-20261009-01.json) |
| R03 | 前推试验与区间研究 | [记录](../../data/verification/minute-engine-completion-20261007/r03-scope-closure-01/EV.json) |
| R04 | 策略页面完整接线 | [记录](../../data/verification/minute-engine-completion-20261007/legacy-parameters-implementation-06/native-normal-recovery-78/final-checkpoint.json) |
| R05 | 分钟绩效与完整报告 | [记录](../../data/verification/minute-engine-completion-20261007/frontend-report-root-07/RESULT.json) |
| R06 | 完整旧参数 | [记录](../../data/verification/goal-progress-root/auction-policy-r02-r06-accepted-20261009-01.json) |
| R07 | 11 种评分与参数优化 | [记录](../../data/verification/goal-progress-root/r07-r09-closure-facts-20261009-01/ROOT-ACCEPTANCE.json) |
| R08 | 五组消融对照 | [记录](../../data/verification/minute-engine-completion-20261007/study-pages-completion-resume-20261008-01/real-installed-input-01/prepared-01/attempt-01/HANDOFF.md) |
| R09 | 分钟滚动验证 | [记录](../../data/verification/minute-engine-completion-20261007/study-pages-completion-resume-20261008-01/real-installed-input-01/prepared-01/attempt-01/HANDOFF.md) |

## Target与更新方式

Goal尚未完成，工具已实际返回active。当前无需再点Target「继续」。
参数与三策略最终集成本地验收已结束。两版本Linux三身份各3项、新参数各54项、AI界面18项及最终构建证据继续有效。唯一FCR-001已由原审查者关闭。必要CI输入修复已冻结，23,081节点完整收集、清单验证和原容量门禁通过，3.12精确元数据生成及check通过。原审查者对新增34+7叶CI差异定向复核PASS，无新增阻断。现在提交同一候选并运行仓库CI。完成19项，剩余6项。
正式来源、生产安装、切换和停服仍按上表处理。旧阻断记录保留，不阻止本轮已具备条件的工作。

固定基线25项，追加0项。测试、诊断、修复和代理运行归入原交付项。
每次完成真实交付后更新完成数、剩余数、下一项和估时；复用仍有效的结果。
最终候选只做一次集中独立审查。

本轮已重读全局、父项目及当前工作树规则，并同步两名活动子代理。下一步只保留上述6项所需工作；先复用直接相关实现和有效证据，只修可复现的验收阻断。无限时长与修复次数继续有效，不扩大范围。不另开全仓审计、重构或设计阶段，不删除已有工作。

[结构化清单](2026-10-07-goal-delivery-ledger.json) · [平台进度](2026-09-27-react-platform-progress.md) · [执行顺序](2026-09-29-factor-research-completion-sequence.md)

旧过程记录完整保留：[本次更新前的清单](../../data/verification/goal-progress-root/remaining-gates-revalidation-20261009-01/prior-delivery-ledger.md)。
