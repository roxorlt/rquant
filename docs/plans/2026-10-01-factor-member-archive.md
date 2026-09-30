# 因子历史成员归档与文件驱动检验

**目标：** 把已归一化的逐日证券/指数成员事实封存为可复读、可校验的真实文件，按日接入已验收的流式公式、收益、统计和衰减执行器，为后续 v2 持久任务提供小型引用。

**架构：** 每日一个有界规范文件，小 manifest 绑定日程、选择、截止时刻、计算代码范围及全部日文件。归档负责文件事实与摘要，reader 负责单日消费与完成证据；执行包装器复用现有 runner。运行参数、成员归档、原始价格来源与结果计算通过严格类型合同连接。

**技术：** Python 3.11+、Pydantic、现有 canonical JSON / private file 原语；不新增数据库或调度框架。

## 分级、基线与职责

- 普通任务：新增领域内不可变输入文件和执行包装器，改变本地研究行为；不修改权威任务账本、租约、权限、共享来源版本或生产数据。
- 冻结代码基线：`08120d93da4d7f0eb038c88865f40672f6f19e32`，`cdx/20260929-factor-source-integration`，clean。实现 worktree 从本计划提交创建，并记录实际 branch / HEAD / clean。
- 当前产品为 native Codex，父任务 `/root`；既有 native implementer 编码、测试与修复，既有独立 reviewer 只在最终候选集中审查一次。普通任务最多一次定向修复与复核，遵循全局 AGENTS。
- 指定新 worktree 内离线开发；不访问网络、`.env`、真实凭据或生产环境，不继续委派。主 checkout 及此前来源准备候选保持冻结。

## 写集与入口

1. `src/rquant/factor/member_archive.py`：严格日输入、manifest、归档引用模型；`publish_factor_member_archive`、`load_factor_member_archive`。只从实际归一化日文件实读导入，读取前后检查文件身份并重新计算内容摘要。
2. `src/rquant/factor/member_stream.py`：`open_factor_member_stream` 与 `run_factor_stream_research_from_members`；返回现有研究/衰减结果、实际成员引用与完成回执的严格包装模型。
3. `tests/unit/test_factor_member_archive.py`：归档封存、规范摘要、拒绝、私有文件访问与自有临时资源清理。
4. `tests/unit/test_factor_member_stream.py`：自然完整消费、日事实绑定、结果对照、尾部失败及资源释放。

不为便于接线修改旧 v1 jobSpec、ledger、worker、full/display artifact 或原 runner 的公开合同。本片验收后才进入显式 v2 持久任务/结果接线；仓库必要的清单与进度文件由 root 集成时更新。

## 必须保持的口径

- 四种选择保持 `all / hs300 / zz1000 / gem`。调用既有 `select_factor_universe`，保留未知 ST、缺板块、缺完整当日指数成员的明确拒绝；不能从缺行补出“未上市”，不能用最新名单回填历史。
- 最多 7,000 条单日证券事实、最多 1,024 个计算交易日，使用现有常量。日期严格有序且唯一；日程包括预热和末尾计算日。日文件与 manifest 设明确字节上限，先检查边界再解析。
- `as_of` 是全局研究截止时刻，覆盖实际归档观察时间。输入保持 `historical_retrospective`；不把今天获取的历史文件改写成过去 09:25 实际采集。特征决策时间仍沿现有回溯假设。
- 日文件保存实际归一化证券事实、完整清单、当日成员及其来源标签/真实观察时刻。调用方填写的来源 SHA 不能作为覆盖、真实性或内容证明；导入从文件内容实算摘要，保留原始来源声明供追溯。
- 所有日批使用的单一 security/index 来源摘要必须由对应实际日 payload 摘要序列生成，且只按需要的源组件聚合；不能把派生 archive SHA 写回日 payload 形成自引用。manifest 再绑定日文件、聚合来源身份与参数。
- 每日完整证券清单必须落在计算代码范围内；日期、选择、全局 cutoff、证券/指数来源身份与 runner 的公式请求精确匹配。计算 scope 是请求的上界，不凭其宣称全市场来源覆盖。

## 文件发布与消费

- 复用 `strict_json`、`private_fs.rename_noreplace_at`，以及已被 display 使用的 `result_artifact` 私有目录/文件身份、写入与临时文件清理助手。不创建通用文件框架、不改变这些共享原语的合同。
- 路径由受信运行时注入；引用只含规范文件名、内容摘要及必要元数据，不把任意路径、SQL 或 Python 塞进未来任务规格。校验 private owned root、普通单链接文件、规范内容和实际字节数；拒绝符号链接、越界与损坏文件。
- 按日写入/读取，不缓存整段证券矩阵。manifest 仅保存有界日期引用与计数；source digest 聚合与历史链只保存小型标量元数据。
- 单日完成及 yield 不能标记整段成功。reader 必须消费精确日程、检查缺日/额外日/尾部错误，完成前核验 manifest 与已读文件身份，且与已有 adapter / formula / statistics / decay 的完成合同对应。
- 异常、取消、提前关闭均不产生完成回执；FD、迭代器、执行 session 与自有临时文件须清理。已经发布的不可变共享日文件可保留供重试，不能删除其他任务文件；不完整导入不发布最终 manifest 成功。
- wrapper 消费实际归档 reader，并复用 `run_factor_stream_research_with_decay`；输出绑定成员引用、实际完整消费证据与研究结果规范摘要，不能以调用方 Iterable 或摘要承诺替代真实文件输入。

## 实施与聚焦验收

1. 先用实际临时规范日文件建立红测：缺模块/入口红测及有意义的损坏/未知事实拒绝；保存准确命令、环境、exit 和日志。
2. 完成日文件封存与 manifest 发布：排序后摘要稳定；日内容、来源标签/观察时刻变化能改变相应身份；缺文件、非规范/超字节、被替换或损坏输入拒绝，临时资源清理成立。
3. 完成按日 reader：四池、动态池与合法空池；完整日程/范围/cutoff/来源身份绑定；缺日、额外日、取消和读后换代不返回整段成功。
4. 完成文件驱动 wrapper：实际文件→reader→现有 raw/formula/statistics/decay，小型已知样本与旧入口黄金结果一致；同一执行代、一次消费、自然尾部、完成摘要和清理核对。
5. 做必要的边界与轻量资源验证：单日上限不截断，manifest 上限拒绝；多日小横截面逐批对象释放、只保留当前日/有界元数据，自有 FD 与执行副本清理。复用已有效的既有 7,000×16 日 runner 规模证据，不为了形式再跑旧规模或全仓。
6. 改动区 Ruff / format / diff 检查；冻结 clean candidate 与原始证据，交原独立 reviewer 一次集中终审。审查后只修本次阻断 finding，按既定上限复核。

实际测试使用 `/Users/roxor/brain/30-projects/rQuant/.venv/bin/python`（3.12.13），明确 `PYTHONPATH=<指定新 worktree>/src`、`RQUANT_DISABLE_DOTENV=1`、假 token、空通知 key 与独立临时目录。执行命令和测试节点由实现者随最终写集记录；skip / deselect / 环境失败不能计为通过。集成只追加新测试节点并运行仓库实际要求的两项清单门禁，不自动执行全量回归。

## 完成边界

本片证明实际文件内容可封存、复读、精确绑定并驱动一次研究。真实提供方历史归档、市场覆盖、指数有效成员语义及实际采集 PIT 仍须采集/核验；内容散列不证明这些事实。本片不开放网页运行入口、不改生产或停用 Streamlit。后续依次接 v2 持久任务/结果封存、可信准入与原型运行交互，再实现每日因子跟踪；完整平台目标继续保持未完成。
