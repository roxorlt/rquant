# Deploy Log

> 每次部署到 82.156.0.68 时追加一条。日期 + tag + 备注 + 回滚命令。
> 最新在最上面。

---

## v0.30.0 Release A 上线前置条件（尚未部署，非部署记录）

**状态**：`cc/workload-isolation-continuation` 分支的 R07 / signal-family 工作已开 PR，等待
Codex 最终验收。**尚未 merge、尚未打 tag、尚未部署**，云服务器 82.156.0.68（lighthouse 用户）
上没有发生任何变更。本节记录的是这次发布**之前**必须逐条满足的条件，不是一条部署记录；
真正部署后再按本文件的既有格式追加 `## YYYY-MM-DD · v0.30.0 · 标题`。

1. **合版方式只能是 "Create a merge commit"**。R07 冻结 baseline `45d0b57c` 不在
   `origin/main` 的祖先链上，squash 或 rebase merge 会让 main 上任何 commit 永远过不了
   `verify_wire` 的 ancestry 检查，Release B 也就永远拿不到部署证据。merge 之后立刻核对
   `git merge-base --is-ancestor 45d0b57c origin/main` 返回 0。
2. **第一次真实 push-to-main run 之后**核对 WP1-SPEC-06 / SPEC-12：evidence 里的
   `job.check_run_id` 与 artifact 内部路径必须与真实 GitHub API 返回一致（本地只用 fake
   transport 验证过）。
3. **服务器 `.env` 增加 `RQUANT_GITHUB_EVIDENCE_TOKEN`**。这属于生产密钥变更，需要用户单独
   明确授权；Release A 本身不消费证据，这个 token 是 Release B 才需要的。
4. **Release A 之后的下一次部署只能是 Release B**：`deployment_mode=enforced`，并且
   `bootstrap_predecessor` 精确声明 Release A 的 commit 与 tree SHA。中间不允许插入其他
   部署目标。
5. **云端只读核对**：对每一个 live generation 核对
   `sha256(full-manifest.json) == slot.full_manifest_hash`。只读操作，走
   `open_readonly_store()` / 只读副本，不碰主库写锁。
6. **云端 child 访问实验**：以真实 `lighthouse` 身份对 `0715` 的 child workspace 做
   `O_RDONLY | O_DIRECTORY` 打开，并确认 `id -g lighthouse != 0`——工作区的 group 位是
   `--x`，子进程一旦落进 group 类就会丢掉读权限（验证器现在会直接拒绝这种身份配对）。
7. **root verifier 必须从 root-owned 树运行**。当前入口脚本
   `scripts/signal-family-root-verifier.py` 会把自己所在 checkout 的 `src` 插进 `sys.path`；
   若从 `/home/lighthouse/rquant/` 以 root 运行，root 就会执行 lighthouse 可写的代码。
   policy / harness / store 的安装是**单独授权的事务**，不能借受控发布器绕过。
8. **R07 证据缓存命中也需要网络与 token**：缓存命中不再跳过 GitHub run 身份核验，部署器仍会
   用 `RQUANT_GITHUB_EVIDENCE_TOKEN` 查一次 workflow runs 并要求与缓存条目自称的
   `workflow_run_id` / `run_attempt` 完全一致，因此**离线部署不可行**；GitHub 不可达时结果是
   blocked，不降级放行。缓存目录及其全部祖先必须由部署身份（lighthouse）拥有、无 group/other
   写位、无 symlink，否则同样 blocked。
9. **规格 errata 未决**：family taxonomy 单元素域、bundle/overlay identity 语义、
   producer/consumer id 域、profile-service-manifests 文档绑定、`strategy-router` /
   `strategy-shadow` 五个 surface 的向量语义、WP5 Q1–Q4、wire schema 在 3.11/3.12 的可见性、
   退休门的交易日数字，全部等 Codex 裁决；在此之前真实 harness 不产生五对 `READY`，
   Phase C activation 也不成立。

**回滚**：本分支没有产生任何生产变更，因此没有回滚基线。PR 未合并前直接关闭 PR 即可；
已合并但未部署时，生产仍停在上一次部署的 commit，无需任何动作。

---

## 2026-08-04 · v0.28.3 · 爆量历史搜索与触发日趋势标记

**状态**：PR #151 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag
`v0.28.3` 精确指向 `e4e303b0a4c05d2a4deefbee502718053672fe6f`。受控发布器从
`a637bb62b9efe8c2b9c915466ec086e6f0ba912a` 快进发布，结果为 `deployed`；无 schema、
systemd、nginx 或密钥变更。

**部署内容**：爆量记录支持跨交易日按标的搜索全部触发记录；从当天记录可打开当日附近的
趋势图，并在图上标记每一次触发时点。发布前修复 2026-08-04 监控池实际 15 只标的的
production `minute_bar` 缺口，并刷新只读副本。

**发布与验收**：修复后 preflight 为 `ok=5 warn=0 fail=0 skip=0`；生产健康端点返回
HTTP 200。真实旧历史样本如缺少 `minute_bar`，趋势图会如实显示“暂不可用”；生产真实历史
趋势尚未完成全量数据验收，不将该部分表述为已全绿。

**回滚基线**：`a637bb62b9efe8c2b9c915466ec086e6f0ba912a`。如需执行受控代码回滚，使用
`bash scripts/deploy-production.sh --target a637bb62b9efe8c2b9c915466ec086e6f0ba912a`；
不得盲目拉取 main 或直接覆盖生产数据。

---

## 2026-08-04 · v0.27.2 · 爆量近五日推送次数收尾记录

**状态**：PR #143 的 CI run `30512484263`（Python 3.11/3.12）双绿后 squash merge；
merge SHA 为 `1ec9bac2fa86c9fab3625be980923e8831f7804b`，annotated tag `v0.27.2`
精确指向该 SHA。**本版本未单独部署**：受控发布自动化执行前，生产已经快进至
`v0.28.1`，其 HEAD `a637bb62b9efe8c2b9c915466ec086e6f0ba912a` 包含该 merge 的功能；
此处仅记录后继版本中的生产收尾验收，不将其表述为 v0.27.2 部署成功。

**生产只读验收**：生产 HEAD `a637bb6` 包含目标 SHA，tracked worktree clean；
`rquant-surge-watch` 当日正常退出。由于截至 8 月 4 日的近五交易日没有足够跨日样本，
按时间倒序回推 2 个交易日至窗口 `2026-07-27..2026-07-31`：`300673.SZ` 在 7 月
29 日和 30 日出现，近五日计数为 2，但不进入 7 月 31 日的 `pushed_today`；7 月 31 日
最新真实记录 `300063.SZ` 经离线正文渲染包含“近5日推送次数：1”。验收只调用纯历史
加载与渲染路径，`rquant.notify` 未加载，未发送 Push。

**变更边界**：无 schema、systemd、生产数据、nginx 或密钥变更。

**回滚**：只可对 PR #143 创建 revert PR，合并后打新的 SemVer tag 并向前发布；禁止
直接回退生产 tag 或将本收尾记录当作已完成的独立部署。

---

## 2026-07-30 · v0.28.1 · 全景页冷启动兜底与爆量历史回看

**状态**：PR #148 经 CI 全绿后 squash merge；annotated tag `v0.28.1` 指向
`a637bb62b9efe8c2b9c915466ec086e6f0ba912a`。19:49 通过
`scripts/deploy-production.sh --target v0.28.1` 从 v0.28.0 快进发布，部署器返回
`deployed`，五个长驻服务重启后 active。无 schema、systemd、nginx 或密钥变更。

**背景（当日事故排查）**：v0.28.0 收盘后部署重启暴露既有冷启动缺口——poller
内存快照被清、收盘后 surge feed 停更超 120s、东财/新浪对云端 IP 间歇性拒绝，
页面卡在等首拉全空（连爆量记录 tab 都被 rerun 等待挡住）。本版修复：所有活路由
失败且 slot 为空时，从自家 `panorama_live` drop（优先，含原始 as_of）或陈旧 surge
feed 恢复最后一份快照，`age_seconds` 按数据真实时间回算，⚠️ 陈旧标注如实触发。

**新增**：爆量记录 tab 日期选择器（默认今天，可回看云端留存的历史 events，当前约
15 天）。

**部署插曲**：当晚 19:00-19:30 三次部署被 preflight 正确拦下并自动回滚（只读副本
陈旧超 12 分钟阈值——副本同步 timer 每日 17:30 后停到次日 09:00，叠加 461 日历史
回补批任务持写锁）。确认副本同步脚本与活跃写者本就并行安全（盘中 monitor 持锁时
每 5 分钟照常同步）后，手动跑 `scripts/sync-readonly-replica.sh` 刷新副本，preflight
通过后发布成功。回补批任务全程未受影响。

**验证**：Playwright 带网关 cookie 实测——页面出数（快照路由新浪、数据 0 秒前）、
爆量记录 tab 日期选择器渲染正常（当日 54 条记录）、console 0 errors。冷启动兜底
路径本次未触发（新浪路由恰好可用），由 26 条 poller 单测覆盖，待下次全路由失败时
实战验证。runtime_config 动态口径页脚将于次日 09:25 surge-watch 首启后生效。

**回滚**：`bash scripts/deploy-production.sh --target v0.28.0`（自动回滚基线
`3b9656056452e12393fbb4f86e4cb23c793a725b`）。

---

## 2026-07-30 · v0.28.0 · 全景页爆量图表与脉搏异动

**状态**：PR #142 经 Python 3.11/3.12 CI 全绿后 squash merge（期间与 #143/#144 合并
解冲突，仅版本号一处，保留 0.28.0）；annotated tag `v0.28.0` 精确指向
`3b9656056452e12393fbb4f86e4cb23c793a725b`。15:12 在交易保护窗口外通过
`scripts/deploy-production.sh --target v0.28.0` 从 `v0.27.1`
（`f7c3105e3043d873185674460a6a4358a2599956`，含 #138-#144 的 main 快进）发布，
部署器返回 `deployed`；无 schema、systemd、nginx 或密钥变更。

**内容**：① 爆量记录 tab 行选择联动个股图表，分时/5日图橙色虚线标注每日首次爆量
确认时刻；② 脉搏历史由 surge-watch 每分钟落 `surge_live/pulse-*.jsonl`（新模块
`pulse_watch.py`），📈 浮层改四张分面小图；③ 四类脉搏异动（涨停潮/炸板潮/跌停潮/
涨跌占比突变，10 分钟滑窗 + 30 分钟冷却）触发页面提示条 + PushDeer `pulse_alert`
场景（仅 PushDeer）；④ surge-watch 启动落 `runtime_config.json`，爆量记录页脚动态
显示检测口径；⑤ 分时/5日量柱按分钟涨跌 tick-rule 近似红绿上色。

**配置变更**：部署后云端 `.env` 追加 `RQUANT_SURGE_BOARDS=all`（爆量检测范围
从创业+科创扩至全市场，次日 09:25 surge-watch 启动生效）。排查结论：东百集团
600693.SH 等主板肉眼爆量此前不进台账的根因即检测范围默认仅 gem/star。

**验证**：合并树全量单测 2424 passed（8 条为 backup/replica 本地环境性 pre-existing
失败）；Playwright e2e 10 项 checklist 全过（含点行出图见标记与量柱色）；部署后
五个长驻服务重启均 active，28080 网关健康 200、未登录 302 跳转正常，服务日志无异常。

**回滚**：纯代码 + 单行 .env 变更，无数据写入。回滚命令
`bash scripts/deploy-production.sh --target v0.27.1`（自动回滚基线
`f7c3105e3043d873185674460a6a4358a2599956`）；如需同时撤销检测范围全开，删除云端
`.env` 中 `RQUANT_SURGE_BOARDS=all` 一行即可（次日生效）。

---

## 2026-07-27 · v0.27.1 · 完整历史迁云与爆量事件分钟线解耦

**状态**：PR #137 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag
`v0.27.1` 精确指向 `f7c3105e3043d873185674460a6a4358a2599956`。22:24 在交易保护
窗口外通过 `scripts/deploy-production.sh --target v0.27.1` 从 `v0.27.0`
（`5bb641ab23efa9595100070ff77282e18c14d170`）快进发布，部署器返回 `deployed`；没有
schema、systemd、nginx 或密钥变更。

**历史迁移与新口径**：先把 Mac 主库制作成不可变 zstd 归档并在云端校验源文件 SHA-256，
再按月提交补入 `daily_bar` 4,750,817 行（2020-08-24 至 2024-08-30）和
`minute_bar` 19,399,449 行（2025-03-28 至 2026-07-27）。随后按 12 份爆量事件文件补齐
156 个缺口 code-day、37,596 根分钟线；最终 161/161 个 `confirmed` 或 `unbuyable`
事件 code-day 都有一条日线和规范的 241 根分钟线。`research-ingest` 自本版本起把当日
爆量事件代码并入盘前 Pool 1/2 分钟采集集合，后续留存不再依赖 Mac 本地 monitor。

**验证与备份**：完整本地归档与云端主库再次反连接，日线和分钟线剩余缺口均为 0；云端
主库与只读副本分别通过 161/161 事件覆盖验收。最终主库 7,553,429,504 字节、52 张表，
只读副本已刷新；压缩备份 2,314,254,589 字节，`verified=true`、源延迟 0，`gzip -t`
通过，SHA-256 为 `ffc2aa3def63a65b9e801866b600aa8c0c7f7d2709aae27c0039e1d87e23efda`。
生产包版本为 `0.27.1`，tracked 工作区干净，发布审计无错误；五个活跃长驻服务重启后
均为 active，脉搏、午间报告、研究增量、副本同步和备份 timer 均为 enabled/active。

**本地清理**：云端归档、主库、副本和最终备份全部验收后，删除 Mac 的
5,336,477,696 字节主库及 2,055,209,122 字节上传暂存压缩包；本地 `data/` 由约 5GB
降至 76MB。相关本地 monitor、旧 daily、云同步、脉搏和午间报告 LaunchAgent 保持
disabled，避免重新生成生产数据或重复推送。

**回滚**：本次含生产数据增补，禁止回滚或覆盖云端数据库。若新的事件并集读取导致
`research-ingest` fail closed，先保留当日事件 JSONL 和研究发布证据，再创建 revert PR、
打更高 SemVer tag 并通过受控发布器向前发布；代码发布器自动回滚基线为
`5bb641ab23efa9595100070ff77282e18c14d170`，不得手工 reset 生产仓库。

---

## 2026-07-27 · v0.26.9 · 爆量与脉搏 Push 移动端格式优化

**状态**：爆量格式 PR #133（CI run `30232003418`）与脉搏格式 PR #134（CI run
`30233029001`）均经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag `v0.26.9`
精确指向 `c7b38f0d6177ac35fc87de1c2c58ec51e5629241`，其父提交是包含爆量格式的
`182f0b590313368d306f07a1f2bec18750150370`。15:10-15:11 在交易保护窗口外使用
`scripts/deploy-production.sh --target v0.26.9` 从 `v0.26.7`
（`9bb5235a8a2fd1d4d874a2c71858e99acb58f9fe`）快进发布，部署器返回 `deployed`；
没有 schema、systemd、生产数据或密钥变更。

**变更内容**：爆量 Push 改为每个标的一组，题材、涨幅/涨停空间、累计比/累计额和分钟方向
逐行展示；30 分钟脉搏改为“市场温度 / 新晋涨停 / 题材热度 / 放量异动”四个 Markdown
分节，股票与题材逐项展示。判定口径、去重、数量上限、通知路由和调度时间均未改变。

**验证与生产验收**：

- 本地实际 launchd 运行 worktree 用提交 `089cbe1` 仅覆盖脉搏渲染器与对应测试；47 项
  midday/notify 聚焦测试、Ruff 和差异检查通过。根 `.venv` 已确认导入该 worktree，
  `morning-pulse` 与 `midday-report` 均保持已加载、收盘后不运行、上次退出码 0。
- 生产 HEAD、tag 与包版本分别为上述精确 SHA、`v0.26.9`、`0.26.9`，工作树干净；纯
  `render_pulse` 样例输出四个分节。发布器重启的 canvas、dashboard、nl-screen、
  panorama-auth 和 panorama 五个服务均为 `active`。
- monitor 与 surge-watch 收盘后保持 `inactive`，两个 timer 均等待下一交易日 09:25；
  未盘后补跑监控或发送测试 Push。

**回滚**：本版本只有通知文本渲染变化。若移动端展示异常，创建针对 PR #133/#134 的
revert PR，合并后打更高 SemVer tag，并通过受控部署器向前发布；禁止生产机直接回退旧 tag。

---

## 2026-07-24 · v0.26.7 · Growth 固定回放停牌证据绑定修复

**状态**：PR #131 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag
`v0.26.7` 精确指向 `9bb5235a8a2fd1d4d874a2c71858e99acb58f9fe`。07:38 在交易保护
窗口前完成 dry-run 和正式发布，生产 tag、HEAD 与目标 SHA 精确匹配；发布器从
`v0.26.6` 的 `92c9308cafdf9a24271239d7490d6437471ba01a` 快进部署，没有 schema、systemd
或生产数据迁移。

**修复内容**：Growth Stage 1 执行依赖由 `stage1-v1` 升级为 `stage1-v2`。snapshot
builder 在生产源库中使用完整历史日线、分钟线、停牌事件与覆盖版本，先物化小型
`stock_suspend_session_evidence`，再把它绑定进不可变 execution snapshot。正式回放不再
访问未声明的 `stock_suspend_event`，也不会因按回测日期裁掉历史冲突而把未知停牌误判为
整日停牌；无法还原旧 `as_of` 覆盖版本时明确 fail closed。

**验证与生产验收**：

- 本地聚焦测试 66 项、全量测试 2,319 项通过；ruff、锁文件和差异检查通过。独立语义
  审查重放历史冲突、旧版本 fail-closed、空工件与 binding 身份反例后结论为通过。
- 发布前 dry-run 只包含 9 个 Growth 快照契约、实现、测试和版本文件，目标 SHA 精确；
  正式发布状态为 `deployed`。发布后两次 preflight 均为
  `ok=5 warn=0 fail=0 skip=0`，28 个 unit 全部 verify。
- 发布后备份 07:38:56 开始、07:42:06 成功结束，`Result=success`、
  `ExecMainStatus=0`；随后主动刷新只读副本成功，第二次 preflight 显示副本年龄 0 分钟、
  主副本工件延迟 0 分钟。
- `daily_bar` 最新为 2026-07-23、1,667,446 行；`minute_bar` 最新为
  2026-07-23 14:59、47,549,142 行。daily、monitor、surge-watch、replica 与 backup
  timers 均为 `enabled/active`，monitor 与 surge-watch 下一次触发为当日 09:25。

**Growth Stage 1 后续**：使用独立临时状态库完成了 `v0.26.7` 只读 planner，未写生产
manifest。范围为 2026-04-01 至 2026-07-09，资格记录 22,879 条；baseline 覆盖率
99.9829%，entry/exit 覆盖率 99.9146%；剩余 132 个任务、预计 136,406 行。planner
实耗 1,989.97 秒，若开盘前再向生产状态库重算并继续 snapshot、审计和固定回放，存在跨越
09:10 硬截止的风险，因此正式写入延至交易保护窗口后执行，不将临时 manifest
`2c9bd7b023316c11f40cf8768e2de9e9d9f53d81abc3764ae47a24ac1b9ae58e` 冒充生产证据。

**回滚**：本版本没有 schema 或业务数据写入。若发现 Growth snapshot 语义异常，创建
revert PR、合并后打更高 SemVer tag 并向前发布；禁止生产机直接回退旧 tag 或修改已发布
binding。紧急情况下只停止新的 Stage 1 研究任务，不停止 monitor、daily 和数据采集链路。

---

## 2026-07-23 · v0.26.4 · 成长板 Stage 1 规划内存止损

**补录说明**：本条补录 PR #128 已核验的历史发布与生产审计事实，不是新的部署或重复执行部署。

**状态**：PR #127 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag
`v0.26.4` 精确指向 `b68c37619a90a049b2170866a3e5e86f710857d7`。04:12 在交易保护
窗口外通过受控发布器从 `v0.26.3` 部署，未修改 schema 或生产业务数据。

**修复内容**：

- 成长板开盘结构分类按目标日稳定分批，资格解析与覆盖核对复用同一份结构事实，不再重复
  执行全范围分类。
- `backfill-plan` 的独立 DuckDB 连接默认限制为 2048 MB、2 线程，并使用命令级临时
  spill 目录；异常退出后自动清理。
- 前一交易日 MA5/10/20/60 完整时，候选解析只读取当日收盘；仅为缺少任一均线的代码
  回退读取 60 个交易日日线。混合样本测试锁定 fallback 不扫描完整均线代码。

**验证**：

- 本地全量 2,306 项测试全部通过；受限沙箱内先通过 2,299 项，另外 7 项端口绑定与
  `ps` 权限测试在放宽对应本机权限后通过。Ruff 与差异检查通过。
- GitHub Actions `test (3.11)`、`test (3.12)` 分别用时 6 分 44 秒、6 分 34 秒并通过。
- 修复前成长板 planner 单进程约 5.9 GiB 后被内核 OOM。生产只读隔离基准运行
  43 分 32 秒、峰值 1,901,112 KiB，未新增 OOM；随后按用户指令以 SIGTERM 终止，
  未输出最终 planner JSON，也未写入生产 manifest。该结果只证明内存止损有效，不代表
  Stage 1 或完整耗时验收通过。
- 部署后版本为 `v0.26.4` 且 HEAD 精确匹配 tag；preflight `ok=5 warn=0 fail=0 skip=0`，
  28 个 systemd unit 验证通过。主库与只读副本代际差为 0 分钟；日线最新
  `2026-07-22`、分钟最新 `2026-07-22 14:59`。Dashboard、NL Screen、Panorama 正常
  running；monitor/surge-watch 在盘前保持 inactive，六个关键 timers 全部 active。
- 按“不要再启动远端长任务”的明确要求，本次未手工启动备份或 planner；backup 与
  replica-sync timer 的下一次计划触发均为 2026-07-23 09:00。

**剩余门禁**：需要在另行允许的资源窗口完成新快速路径下的 growth `backfill-plan`，并以
新 manifest 依次通过 repair、snapshot、data audit 和 formal replay，取得
`comparable` 结果。完成前不得宣称成长板 Stage 1 已验收。

**回滚**：本版本无 schema 和业务数据变更。部署失败由受控发布器自动回滚；成功发布后的
回退必须对 `b68c376` 创建 revert PR、合并为新的 main 提交、打新 SemVer tag 后向前发布，
禁止在生产机使用 `git reset` 或直接覆盖 DuckDB。

---

## 2026-07-21 · v0.25.4 · 爆量累计器跨日冻结修复

**状态**：PR #119 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag
`v0.25.4` 精确指向 `0909fa3135c6f6ce42c9ced05040e1c47f6cc730`。16:26-16:27 在
交易保护窗口外完成 dry-run 和正式发布，生产 tag、HEAD 与包版本均精确匹配。

**事故与修复**：09:25 启动时，`rt_min` 在当日首根形成前返回上一交易日 15:00 末根；
原累计器只比较时分，导致今日全部分钟被判为时间回退。2026-07-21 生产服务虽然完整运行
09:25-15:02，247 次全市场请求全部成功，但创业/科创 2,010 只股票的 241 点累计额全部
整日零变化；确认层仅在 09:31-09:34 拉取 6 只候选，之后没有新候选。新累计器绑定交易日，
非当日分钟不再写入累计或分钟锚点；同分钟去重、乱序保护与同日重启 seed 保持不变。

**验证**：

- 回归测试先在旧实现得到预期红灯，再由修复转绿；`test_surge_watch.py` 87 项通过，
  surge/CLI 聚焦测试 90 项通过，ruff、锁文件和差异检查通过。
- 241 分钟合成会话产生 241 个不同累计值，首分钟 100、收盘 24,100；云端生产包以
  “昨日 15:00 → 今日 09:30 → 09:31”重放得到 `[0, 100, 160]`，退出码 0。
- 生产 preflight 为 `ok=5 warn=0 fail=0 skip=0`；monitor 与 surge-watch 保持收盘后的
  `inactive/dead`，未盘后补跑。两个 timer 均为 `enabled/active/waiting`，下一次触发为
  2026-07-22 09:25；daily、replica 与 backup timers 正常。

**影响与回滚**：本版本没有 schema 或业务数据迁移。2026-07-21 的 4 条爆量事件来自冻结
粗筛，不能用于评价正常策略效果，保留原记录作事故证据，不改写历史。若明日累计序列仍不
增长，应先停止 `rquant-surge-watch.timer/service` 并保留 snapshot、series、events 和
journal；代码回退必须创建 revert PR、打更高 SemVer tag 后向前发布，禁止生产机直接
`git reset`。

---

## 2026-07-21 · v0.25.3 · 爆量方向与内外盘确认修复

**状态**：PR #117 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag
`v0.25.3` 精确指向 `f85b8d8cddb83b0fc65e48a31eb50f693f635049`。15:19 在交易保护
窗口外完成 dry-run 和正式发布，生产 tag、HEAD 与包版本分别为 `v0.25.3`、上述精确 SHA
和 `0.25.3`。

**修复内容**：

- surge-watch 在发送前重新确认当前涨跌方向，要求精确分钟覆盖决策时点、当前一分钟收益为正，
  并以逐分钟 tick-rule 近似确认外盘主动买量大于内盘主动卖量；当前分钟数据缺失时延迟判断，
  不用后续分钟补看当时信号。
- 科创/创业放量历史回放修正了内外盘门槛和评分方向：由错误的 `内盘/外盘 > 1` 改为
  `内盘/外盘 < 1`，同时保留旧 CLI 参数作为兼容别名。既有相关回测结论已在分析文档标记为
  失效，等待用修正口径重跑，未把本次方向修复解释为收益已被证明。
- 严格按信号分钟重放 2026-07-21 上午 4 条实际 Push：300901.SZ、301007.SZ 因当分钟
  下跌或内盘占优被拒绝；300889.SZ、300203.SZ 在各自决策分钟仍满足方向条件。没有使用
  信号后的分钟判断信号当时是否成立。

**基线对齐与发布**：生产应用原停在 `v0.25.2`，PR #115 的 backup unit 已于 7 月 20 日
单独安装而 Git 基线未推进。发布器因此按设计拒绝跨越受保护文件。现场复核云端 unit、
`v0.25.3` 仓库文件的 SHA-256 均为
`9a8bb5c92a479bccb076d992d8e2d478b2aff9a6f7c37595c8d35d6cae764003`，
`systemd-analyze verify` 退出码 0、`TimeoutStartUSec=10min`。随后在部署锁内只快进到
PR #116 的精确 SHA `752bf66eae08a2faaac7c1823f7d766348b0c9fb`；该步只对齐已验收
unit、测试与文档，不改变运行时代码，并写入 `baseline_adopted` 审计。标准发布器再从该
基线发布 `v0.25.3`，变更清单不含受保护路径。

**生产验收**：

- preflight 为 `ok=5 warn=0 fail=0 skip=0`，28 个 unit 全部 verify；dashboard、panorama
  等发布前活跃服务完成白名单重启，monitor 与 surge-watch 正常保持收盘后的
  `inactive/dead`，没有盘后强制补跑。
- `rquant-surge-watch.timer` 与 `rquant-monitor.timer` 均为 `enabled/active/waiting`，下一次
  触发为 2026-07-22 09:25；daily、research-ingest、replica、backup 等共 11 个 timer
  均已恢复调度。
- 主库与只读副本摘要完全一致：`daily_bar=1,650,869`、
  `stock_status_daily=1,061,544`、`adj_factor=2,469,013`、`screen_result=856`、
  `minute_bar=46,993,701`，分钟最新为 2026-07-21 14:59，schema migration 为 v10。
- 发布后备份成功原子更新 `backup/latest.duckdb.gz`，`gzip -t` 退出码 0；主动副本同步
  `Result=success`、`ExecMainStatus=0`。验收期间短暂停止 backup timer 以消除连续触发
  竞态，未终止运行中的备份，检查后已恢复为 `enabled/active`。

**回滚**：本版本没有 schema 或业务数据迁移。发现方向过滤异常时必须创建 revert PR，
合并为新的 main 提交并打更高 SemVer tag 后向前发布；受控发布器禁止直接退回旧 tag，
也不得在生产机 `git reset`。紧急止住爆量 Push 时可在保留 monitor 的前提下先停止
`rquant-surge-watch.timer` 与当前 service，保留日志和事件作审计，再发布向前修复。

---

## 2026-07-20 · v0.25.2 + PR #115 · 研究增量 candidate 与备份修复收尾

**状态**：生产应用继续冻结在 tag `v0.25.2`、精确 SHA
`f743fc46ece7c2677fd1bbbd6bdef47418ebf53b`，没有为基础设施修复重启应用服务。备份修复
PR #115 经 Python 3.11/3.12 CI 全绿后 squash merge，精确 SHA 为
`e4d14f06502c745101059f05382e401fd2dedf3b`。18:02 只从该 SHA 提取
`rquant-backup.service`，云端 `systemd-analyze verify` 通过且文件 SHA-256 为
`9a8bb5c92a479bccb076d992d8e2d478b2aff9a6f7c37595c8d35d6cae764003`，安装后
`TimeoutStartUSec=10min`。

**研究日增量首次 candidate**：

- 09:25:06 的不可变 watchlist snapshot 绑定应用 SHA `f743fc4`，共 4 只
  （pool1=2、pool2=2），与 monitor 运行清单一致；当天没有盘后补造或回填开盘前证据。
- 17:00 daily 成功，主动刷新只读副本后 readiness 返回 `ready`、`issues=[]`。首次手工
  ingest 退出码 0、状态 `candidate`：分钟 4/4 标的完整，覆盖率和观测精度均为 100%；
  竞价 5,522/5,524，覆盖率 99.9638%，观测精度 100%；authority、catalog、只读 catalog
  和 lake 全部验收通过。
- `rquant-research-ingest.timer` 随后启用。18:10 首次定时运行于 12 秒内成功退出，
  分钟与竞价分区均为 `unchanged`、`issues=[]`。最终 authority 仍为 `candidate`，
  `stable_trading_days=1`、`observation_count=6`，catalog 与只读 catalog 哈希一致；
  `eligible_for_promotion=false`，继续累计 10 个交易日证据，不提前晋级。

**备份修复与清理**：

- 生产 DuckDB 已增至 5,204,881,408 字节。旧 unit 的 `TimeoutStartSec=120` 会在复制、
  压缩和校验即将完成时终止任务。新 unit 恢复 timer 后因 `Persistent=true` 补触发一次，
  18:02:55 开始、18:06:07 成功，实测 192 秒，`Result=success`、
  `ExecMainStatus=0`。产物来自只读副本、源延迟 0、52 张表，压缩后
  1,529,612,162 字节，`gzip -t` 通过；timer 为 `enabled/active/waiting`，下一次触发
  2026-07-21 09:00。
- 删除前逐个核对 18 个 `.latest.duckdb.<pid>[.gz]` 私有代际文件：路径全部匹配固定格式，
  9 个 PID 均不存在，最新有效备份完整，备份服务 inactive。随后按文件数量和总字节数双门
  删除逻辑大小合计 57,077,510,144 字节；云盘可用空间由约 52 GiB 增至 63 GiB，实际
  回收约 11 GiB，说明这些临时文件的逻辑大小不等于独占物理块。

**最终验收**：

- 从精确合并 SHA `e4d14f0` 的临时 worktree 运行 preflight：
  `ok=5 warn=0 fail=0 skip=0`，28 个 unit 全部 verify，通过后临时 worktree 已删除。
- 主库与只读副本摘要完全一致：`daily_bar=1,650,869`、
  `daily_state=1,650,869`、`adj_factor=2,469,013`、
  `stock_status_daily=1,061,544`、`screen_result=856`、
  `minute_bar=46,992,269`；前五表最新日期均为 2026-07-20，分钟最新为
  2026-07-20 14:59。
- 修复分支本地全量 `2264 passed`，ruff、shell 语法和 `git diff --check` 通过；
  独立审查无 P0/P1。PR #115 的 Python 3.11/3.12 CI 分别通过。

**影响与回滚**：个人平台没有外部用户操作，管理员无需手工介入。若研究增量变为
`degraded`、catalog 哈希失配或 service 非 0，应立即
`sudo systemctl disable --now rquant-research-ingest.timer` 并保留 observation、catalog
和 lake 作审计证据，禁止补造 09:30 前 snapshot。若备份超过 10 分钟、完整性失败或新 unit
异常，应先停止 `rquant-backup.timer`，再用
`/tmp/rquant-backup-rollback-20260720T180254.service` 恢复旧 unit 并
`daemon-reload`；旧 unit 的 120 秒上限已知不适合当前库体积，因此回滚后 timer 必须保持
inactive，改用受控的 replica 直接备份，直到新的向前修复上线。

---

## 2026-07-17 · v0.21.1 · 云端研究日增量首次上线

**状态**：应用代码 PR #97 与 preflight 热修 PR #98 经 Python 3.11/3.12 CI 全绿后
squash merge；annotated tag `v0.21.1` 精确指向
`530bb8c489fb481da9a934220813c5ec02a65909`。15:19-15:20 由受控发布器从 `v0.20.2`
快进部署，状态 `deployed`。基础设施 PR #99 随后合并为精确 SHA
`39341f8145caa33c7355ca01983de9f65ab9f883`；生产 `main` 仅按白名单快进这 5 个文件，
安装 `rquant-research-ingest.service/.timer` 后保持 timer `disabled/inactive`。

**上线内容**：日终 runner 只在当日 daily 成功、主动刷新的只读副本包含完整日线后运行；
默认日期使用上海时区，普通增量和历史恢复都强制 observation 连续。分钟、竞价、catalog
和只读 catalog 先写隔离事务，完整审计后再原子发布。runner 固定交易日，最多尝试 4 次；
`degraded` 和开关关闭不重试。`auction_bar` 仍由研究 authority 验收，不再误列为没有每日
writer 的生产 DuckDB freshness 必选表。

**验证与生产验收**：

- 本地组合分支 `1981 passed`，核心质量检查和 `git diff --check` 通过；PR #97、#98、#99
  的 Python 3.11/3.12 CI 均通过。
- 云端原样 `systemd-analyze calendar` 将触发式规范为工作日 `18:10:00`，连续 5 次迭代
  正确；两个 unit `systemd-analyze verify` 退出码为 0。安装后 28 个 unit 全部通过
  preflight，timer 明确为 `disabled/inactive`。
- 生产代码 tag、包版本和应用 SHA 分别为 `v0.21.1`、`0.21.1`、`530bb8c`；安装基础设施后
  生产 `main` 为 `39341f8`。部署只重启当时 active 的 5 个 Web 服务，monitor 与
  surge-watch 保持正常收盘退出；备份与只读副本同步均为 `status=0/SUCCESS`。
- 17:00 daily 于 17:02 成功，写入 `daily_bar=5,522`、`stock_status_daily=5,522`，
  副本主动刷新后 readiness 返回 `ready`、`issues=[]`。

**首次 observation**：17:11 手工运行写入 2026-07-17 分钟 4,329 行（9/9 标的完整，
覆盖率 100%）和竞价 5,523 行（预期 5,522，覆盖率 99.9819%），catalog 与只读 catalog
哈希一致。结果按设计为 `degraded`、退出码 2，唯一问题是
`watchlist_snapshot_missing`：当天 09:25 monitor 启动时研究开关尚未启用，不能在盘后
伪造 09:30 前不可变清单。开关现保持 `true`，让下个交易日 monitor 留下真实清单；timer
继续禁用，待下个交易日收盘后手工得到 `candidate` 且退出码 0 才能启用。

**回滚**：发现异常时先执行
`sudo systemctl disable --now rquant-research-ingest.timer`，并将
`RESEARCH_CLOUD_INGEST_ENABLED=false`。已发布的 degraded observation、Parquet 和 catalog
必须保留作审计证据，不得手工删除或改写。代码或 unit 撤回必须创建 revert PR、合并后向前
发布新 SemVer / 精确基础设施 SHA；禁止生产机非快进回退。

---

## 2026-07-17 · v0.20.2 · 研究提交纯净度门修复

**状态**：PR #95 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag `v0.20.2`
精确指向 `0c1755e15b2f1a78f09ef18171010d0cf32e4f1f`。09:05 在交易保护窗口前由受控发布器从
`v0.20.1` 快进部署，状态 `deployed`。研究采集开关继续关闭，没有新增 systemd timer，
没有写生产 DuckDB 或研究 lake。

**修复内容**：根目录 `backup/` 明确作为云端定时恢复快照目录加入 `.gitignore`，避免受控
运行时工件让研究提交探测误报 `-dirty`；其他未提交或未跟踪文件仍会使可信度门 fail closed。
同时补齐 `v0.20.0` 和 `v0.20.1` 的生产部署审计记录。

**验证与生产验收**：

- 新回归测试先复现 `backup/snapshot.duckdb.gz` 导致 `-dirty`，修复后通过；本地聚焦测试
  `11 passed`、全量测试 `1958 passed`，核心质量检查与 `git diff --check` 通过。
- 部署后 tag、HEAD 和包版本均为 `v0.20.2` / `0c1755e` / `0.20.2`；包含未跟踪文件的
  `git status` 为空，`detect_code_commit()` 返回精确 40 位 SHA
  `0c1755e15b2f1a78f09ef18171010d0cf32e4f1f`。
- 原失败日期的 `research-ingest --date 2026-07-16 --dry-run` 返回 `status=planned`，结果内
  `code_commit` 为同一精确 SHA、`issues=[]`，且没有发布分区或调用网络补拉。
- 09:06 preflight 为 `ok=5 warn=0 fail=0 skip=0`，主副本工件延迟 0 分钟；五个前台服务
  `active/running`、`Result=success`、`NRestarts=0`。09:00 盘前检查已完成，monitor 与
  surge-watch 保持 inactive 等待 09:25 timer，原有 10 个 timers 全部正常。

**保留门**：本版本只消除正式研究增量的提交纯净度阻断。生产开关仍为 false/missing；安装
研究日增量 systemd 调度、打开开关并开始 10 个交易日 observation，仍属于独立基础设施与
生产研究数据写入变更，必须另行明确授权。

**回滚**：本版本没有 schema 或数据写入。部署失败由受控发布器自动回滚；成功后的撤回必须
对 `0c1755e` 创建 revert PR、合并并打新 SemVer tag 后向前发布，禁止生产机非快进回退旧 tag。

---

## 2026-07-17 · v0.20.1 · 研究日增量日期类型热修

**状态**：PR #94 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag `v0.20.1`
精确指向 `dc486566e356178596c7d917f0bf8fb42c38b055`。08:50 由受控发布器从
`v0.20.0` 快进部署，状态 `deployed`。研究采集开关继续保持关闭，没有安装或启用新的
systemd timer，也没有写生产 DuckDB 或研究 lake。

**修复内容**：研究 Parquet 返回的 `datetime.date` 与运营 DuckDB 返回的 pandas
`Timestamp` 在合并前统一规范化，再进入主键分组和排序；保留同主键最新业务值与最早
`created_at` 的既有语义。新增函数级测试和真实 Parquet + DuckDB dry-run 全链路回归测试。

**验证与生产验收**：

- 本地研究采集测试 `25 passed`、全量测试 `1957 passed`，核心质量检查与
  `git diff --check` 通过；GitHub Actions Python 3.11/3.12 分别通过。
- 部署后 tag、HEAD 和包版本均为 `v0.20.1` / `dc48656` / `0.20.1`；preflight 为
  `ok=5 warn=0 fail=0 skip=0`，无 DuckDB 锁，五个前台服务均为 `active/running`、
  `Result=success`、`NRestarts=0`，原有 10 个 timers 均有下一次触发。
- 对原失败日期重新执行 `research-ingest --date 2026-07-16 --dry-run` 成功返回
  `status=planned`：分钟计划 3,561 行、竞价计划 5,523 行，没有网络补拉或文件发布，日期类型
  异常未再出现。`research-authority-status` 仍为 `bootstrap_candidate`，bootstrap catalog
  哈希一致，尚无日增量 observation，符合开关未启用时的预期。

**启用前门槛**：部署目录存在长期未跟踪的 `backup/`，导致提交探测器在包含未跟踪文件时
返回 `dc48656-dirty`；tracked worktree 实际干净，tag 与 HEAD 准确。dry-run 允许该标记，
正式采集会 fail closed。该目录误判由后续 `v0.20.2` 修复；研究日增量仍须完成独立基础设施
授权与发布后才能启用。

**回滚**：本版本没有 schema 或数据写入。部署失败由受控发布器自动回滚；成功后的撤回必须
对 `dc48656` 创建 revert PR、合并并打新 SemVer tag 后向前发布，禁止生产机非快进退回旧 tag。

---

## 2026-07-17 · v0.20.0 · 云端研究日增量候选代码上线

**状态**：PR #93 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag `v0.20.0`
精确指向 `03d04c8acf8d9c3fc377432b4977d768506d87e9`。08:33-08:34 由受控发布器从
`v0.19.0` 快进部署，状态 `deployed`。本次只上线候选链路代码，生产开关
`RESEARCH_CLOUD_INGEST_ENABLED` 保持关闭，systemd 配置未发布。

**部署内容**：新增盘前不可变清单、日终分钟补齐、全市场竞价增量、隔离 lake/catalog 发布
事务、完整覆盖率审计、连续 observation 哈希链、只读权威状态检查和 10 个交易日晋级门。
正式增量与存量迁移共用发布锁，任一分区或证据失败都会回滚；生产 DuckDB 不进入写路径。

**验证与生产验收**：本地全量 `1956 passed`，PR 双版本 CI 通过；部署后 preflight 5/5、
五个前台服务和 10 个既有 timers 正常。`research-authority-status` 返回
`bootstrap_candidate`、bootstrap catalog 哈希一致、`stable_trading_days=0`，没有提前提升
研究权威。首次对 `2026-07-16` 执行只读 dry-run 暴露 Parquet 日期与 DuckDB 时间戳混合比较
异常；由于是 dry-run 且开关关闭，没有产生数据写入，随后由 `v0.20.1` 修复并复验。

**回滚**：本版本未启用调度且未改数据。部署失败由受控发布器自动回滚；成功后的撤回必须
对 `03d04c8` 创建 revert PR、合并并打新 SemVer tag 后向前发布。

---

## 2026-07-17 · v0.19.0 · 首次研究数据迁云候选发布

**状态**：用户明确授权首次迁云后，05:58-06:23 完成 Mac 冻结快照、迁移包、云端 staging
上传、candidate 发布和独立验收。snapshot ID 为
`research-20260716T215935Z-4e713ead`，全链路绑定代码
`4e713eada6596228f81f455a12fde3cca1111b30`。本次发布的是**研究权威候选**，没有切换现有
Dashboard/Lab/回测消费者，也没有删除 Mac 数据。

**迁移证据**：

- WAL-free 恢复快照 5,157,957,632 字节、51 张表，SHA-256
  `c5863c8e73606b84632eae336282df74315a1d816f5d084ac2f3c05f5a5cc6a2`；绑定 37 个
  Strategy Lab 文件，artifact inventory hash 为
  `990f0af8a675fe7627be88fc6aed9620e826f5ea41bc9cb99c12b3ad20332393`。
- 迁移包 1,392 个内容文件、317,323,241 字节、670 个分区、21,065,728 行；固定样本
  `200/200` 匹配，所有物理主键重复数为 0。bundle manifest SHA-256 为
  `db276d943b8810439c63dcd7e611eb21c5823db40955c8d2c0e57d25a4ac12d0`。
- 分钟线为 316 个分区、19,114,853 行，覆盖 `2025-03-28..2026-07-16`；集合竞价为
  354 个分区、1,950,875 行，覆盖 `2025-01-16..2026-07-16`。云端逐项核对 manifest 与
  version Parquet 数量均为 `316/316` 和 `354/354`。
- 7 张辅助研究表全部发布：`monitor_event=2434`、`data_quality_issue=9`，其余五张当前为
  0 行；物理 data/manifest 均为 7 个，Lab artifact 为 37 个。

**发布过程与安全门**：

- 原计划 15:12 发布；用户在 06:19 明确要求盘前立即执行后，先取消下午 timer，再使用
  “08:15 后拒绝启动 + CLI 30 分钟硬超时 + systemd 31 分 40 秒外层上限”执行，确保最迟
  08:45 结束，与 09:15 交易保护窗口保留 30 分钟。实际发布于 06:21:13 开始，06:23:28
  完成。
- 发布前 monitor inactive，远端空间门通过；发布只写独立 `data/lake/`、
  `data/research.duckdb`、`data/research_artifacts/` 和 candidate 标记。生产
  `data/rquant.duckdb` 发布前后 SHA-256 均为
  `53a76b354c838d6345aeadb345ad90573601b29219b4dd61b6b3bf712c73d73b`。
- candidate 的 catalog SHA-256 为
  `7700f28cc25aa6486d14391cb262cfa7bb9c3721963d61cb2e11cd55adce8b43`，与实际
  `research.duckdb` 一致。发布后再次执行 verify 通过，幂等重跑返回 `unchanged`。

**生产验收**：最终 preflight 为 `ok=5 warn=0 fail=0 skip=0`，无 DuckDB 读写锁；
dashboard active，monitor/surge-watch 按盘前日程保持 inactive，原有 10 个 rQuant timers
均有下一次触发。立即发布 transient service 为 `Result=success`、`ExecMainStatus=0`，下午
timer 已取消。

**保留与后续门**：candidate 要求 Mac 主库、恢复快照、迁移包和 staging 全部继续保留。
完成云端每日分钟/竞价/模拟盘增量、消费者候选验证入口、异机备份和至少 10 个交易日观察前，
不得删除本地研究库或把 candidate 提升为唯一权威。

**回滚**：生产 DuckDB 未变化，现有消费者尚未切换，因此不需要业务数据回滚。若候选后续
验收失败，应停止晋级并保留快照、staging、candidate 和日志作为证据，禁止手工覆盖生产库或
删除本地原始研究数据。

---

## 2026-07-17 · v0.19.0 · 研究数据迁云工具上线

**状态**：PR #90 经 Python 3.11/3.12 CI 全绿后 squash merge；annotated tag `v0.19.0`
精确指向 `4e713eada6596228f81f455a12fde3cca1111b30`。01:03 在交易保护窗口外由受控发布器从
`v0.17.3` 快进部署，状态 `deployed`。本次只上线代码与操作手册，**没有上传、发布或切换
研究数据权威，也没有写生产 DuckDB**。

**部署内容**：

- 一并上线 v0.18.0 的研究湖导出契约：分钟/竞价按交易日导出校验过的不可变 Parquet，独立
  `research.duckdb` 保存 manifest、覆盖度和替换审计。
- 新增 `research-migration snapshot/prepare/verify/publish`：从同一个 WAL-free 恢复快照
  打包分钟、竞价、7 张辅助研究表和 Strategy Lab artifact，保存 PIT 截止、主键、聚合、
  固定样本与文件/内容哈希证据。
- 新增 `scripts/migrate-research-to-cloud.sh` 与中文操作手册，支持本地准备、checksum rsync
  续传、云端重验和 candidate-last 发布。中断恢复会校验 publish state、分区 export lock、
  inode 和 symlink 边界；生产 `rquant.duckdb` 不进入迁移写路径。

**发布与验收**：

- dry-run 识别前序 SHA `06c4eb0bc3a35a4749212b5b1c1e8960bde8d288`、目标 SHA
  `4e713ea`，changed files 不含 `deploy/systemd/`、nginx、sudoers 或生产数据修复路径。
- 发布器只重启部署前 active 的 canvas、dashboard、NL screen、panorama-auth、panorama；
  monitor 和 surge-watch 保持按日程 `inactive/dead`。五个 UI 服务均为 `active/running`、
  `Result=success`、`NRestarts=0`，10 个 rQuant timers 均有下一次触发。
- 最终 preflight 为 `ok=5 warn=0 fail=0 skip=0`：主副本工件延迟 0 分钟，日线最新
  `2026-07-16`，分钟最新 `2026-07-16 15:00`（87,709 行），竞价最新 `2026-07-16`
  （11,047 行），无 DuckDB 读写锁；Dashboard 健康端点返回 `ok`，新 CLI help 正常。
- 本地最终为 1,922 项全量测试和 134 项聚焦测试通过；独立审查未发现剩余 Critical、High、
  Medium 问题。GitHub Actions Python 3.11/3.12 分别通过。

**后续数据门**：真实迁移属于单独的生产数据写入变更，仍需明确授权后按操作手册执行。完成
云端逐表/逐分区验收、每日增量验证和至少 10 个交易日观察前，本地研究主库不得删除。

**回滚**：本版本没有 schema 或生产业务数据变更。部署中失败由受控发布器自动回滚；成功后
如需撤回，必须对 `4e713ea` 创建 revert PR、合并并打新 SemVer tag 后向前发布，禁止在生产
机向旧 tag 非快进回退或直接覆盖 DuckDB。

---

## 2026-07-16 · v0.17.3 · 通知事故治理与研究云化计划

**状态**：基础设施 PR #85 先行 squash merge 为
`03cb96468ef8983f4ea88f17b47704069ea36158`；通知代码与研究/Lab 计划 PR #86 squash merge
并以 annotated tag `v0.17.3` 精确指向
`06c4eb0bc3a35a4749212b5b1c1e8960bde8d288`。21:29-21:47 在交易保护窗口外完成两段发布。

**部署内容**：

- systemd monitor 自动重启限制为 30 分钟最多 3 次；Mac monitor LaunchAgent 显式设置
  `NOTIFY_ENABLED=false`，本地只保留研究分钟采集。
- 错误与运维告警使用 60 秒 pending 租约，至少一个通道成功后才进入 30 分钟冷却；文件锁
  状态为跨进程权威，SQLite 保存可查询镜像并作为降级。两种状态存储均不可用时 fail closed，
  不退回无状态重复发送。
- monitor/surge-watch 进程异常只交给 systemd `OnFailure`；watchdog 复用事故键，并在服务
  连续稳定 5 分钟后才关闭事故，避免短暂拉起重新打开 Push 风暴窗口。
- 新增研究数据云化、服务迁移和 Strategy Lab 六项改造实施计划；本次未迁移或删除研究主库。

**发布与验收**：

- 基础设施先在腾讯云 `/tmp` 通过 `systemd-analyze verify`，安装前后均复验；实际参数为
  `StartLimitIntervalUSec=30min`、`StartLimitBurst=3`、`RestartUSec=30s`。
- v0.17.3 dry-run 明确以 `03cb964` 为前序，changed files 不含 `deploy/systemd/` 等受保护
  路径。实际发布由 transient `rquant-v0173-code-rollout2.service` 托管，结果
  `success`、`ExecMainStatus=0`；只重启发布前 active 的五个 UI 服务，monitor/surge 保持
  inactive。
- 无 Push 的事故门 smoke 通过：claim、complete、冷却抑制、clear 和重新 claim 全部符合
  预期。发布前本地 160 项聚焦测试通过，GitHub Actions Python 3.11/3.12 全量 CI 通过。
- 副本同步与备份均 `Result=success`、`ExecMainStatus=0`；只读副本文件年龄归零。最终
  preflight 为 `ok=5 warn=0 fail=0 skip=0`，分钟最新 `2026-07-16 15:00`、竞价最新
  `2026-07-16`。
- 云端 tag、HEAD 和包版本均为 v0.17.3；dashboard/panorama active，monitor/surge
  inactive，10 个 rQuant timers 均有下一次触发。Mac 主运行时也已快进到相同 SHA，editable
  包重新绑定主 checkout，版本为 0.17.3。

**运行环境说明**：发布期间 sshd 多次返回 `Exceeded MaxStartups`，属于未认证连接队列限流，
不是 rQuant 故障。改用 systemd transient oneshot 后发布不再依赖 SSH 会话；未修改 sshd
配置。

**回滚**：本版本无 schema 和业务数据变更。部署过程中失败由受控发布器自动回滚；成功发布
后的回退必须对 `06c4eb0` 创建 revert PR、合并为新的 main 提交、打新 SemVer tag 后向前发布。
受控发布器会拒绝向旧 tag 非快进倒退，禁止在生产机使用 `git reset` 或直接覆盖 DuckDB。

---

## 2026-07-16 · v0.17.2 · Stage 1 生产数据修复验收

**状态**：PR #82、#83 已依次 squash merge；`v0.17.2` 精确指向
`aa3d4e378d2867303681a7a553bba752f6744a07`。16:12-16:17 在交易保护窗口外通过受控发布器
部署，Stage 1 生产数据 P0 已清零，调度已恢复。

**部署与修复内容**：

- 上线 v0.17.1 的历史状态对账、PIT 竞价修复和备份/副本原子发布加固，再以 v0.17.2
  识别 Tushare `namechange.change_reason=退市整理期`。
- 13 个退市整理期股票日继续 fail closed，不伪装为普通非 ST，也不进入策略；审计从真正的
  unknown/conflict 中分离，作为 `stock-status-intentional-exclusion` P2 证据保留。
- Stage 1 审计规则升级为 `stage1-v3`；真正缺失、未知、冲突和非法状态仍保持 P0。

**生产验收**：

- `data-audit`：区间 `2026-04-01..2026-07-15`，审计 ID
  `62485722f2daa4591189f88ac3d65db327ae9cef4d437f638ea9ce19cee55782`，
  `finding_count=4`、`p0_count=0`、状态 `completed`。
- `stock_status_daily`：资格分母与持久化均为 385,183，missing/unknown/conflict/invalid 均为 0，
  主动安全排除 13；schema migration 保持 v9。
- 主库与 `rquant_ro.duckdb` 摘要完全一致：日线最新 `2026-07-15`、1,628,806 行；
  状态覆盖、审计凭证、13 条排除和 migration 版本均一致。
- preflight 为 `ok=5 warn=0 fail=0 skip=0`；分钟最新 `2026-07-16 15:00`，竞价最新
  `2026-07-16`，停复牌覆盖最新 `2026-07-15`。dashboard、NL screen 等长驻服务完成重启，
  daily/monitor/surge-watch 按日程保持 inactive，全部 rQuant timers 已恢复下一次触发。
- 修复前恢复点：
  `/home/lighthouse/rquant/backup/v0.17.2-pre-apply-20260716T081302Z.duckdb.gz`；
  修复后恢复点：
  `/home/lighthouse/rquant/backup/v0.17.2-post-repair-20260716T081750Z.duckdb.gz`。

**剩余研究门**：生产数据 P0 清零不等于策略可晋级。全市场历史分钟缺口、策略 manifest 的
B/S 与基准覆盖率、不可变计算快照仍需完成；N 字、集合竞价和科创/创业放量继续保持
`exploratory`。

**回滚**：代码回滚使用
`bash scripts/deploy-production.sh --target v0.17.1`。v0.17.2 未新增 schema，13 条安全排除可
保留；只有确认必须回滚数据时，才在停止全部写服务后使用上述 pre-apply 恢复点，禁止运行期间
直接覆盖 DuckDB。

---

## 2026-07-15 · v0.15.0 · 阶段 1 PR-B PIT 质量守卫

**状态**：PR #78 已 squash merge 为
`22618768aaf6bf507eaeb7ed4c8c42813b19fe4b`。权威交易日历于 7 月 14 日完成初始化，
代码于 7 月 15 日 08:37 部署；云端与本地核验完成后恢复盘中调度。

**部署内容**：

- 历史证券名称/ST 状态、上市/重新上市边界改为 nullable、PIT 且 fail closed。
- 日线/分钟语义审计、分钟时间戳语义归一、复权价格可见性守卫与质量问题落库。
- 涨停池修复采用权威交易日历、稳定 plan id、CAS 重算和调用方事务所有权保护。
- 竞价与科创/创业策略回放必须使用匹配的历史状态，禁止回退当前证券快照。

**生产初始化与验证**：

- 首次部署在 preflight smoke 阶段因 `trade_calendar` 缺少 `2026-07-13` anchor 自动回滚到
  `v0.14.0`；根因是目标版本的 fail-closed 筛选先于部署后的日历 bootstrap 执行。
- 使用同一 `v0.15.0` 临时 worktree 执行幂等 bootstrap，Tushare 返回并验证
  `2020-01-01..2026-12-31` 共 2,557 个自然日、1,697 个交易日；随后正式发布成功。
- 发布后云端 preflight 为 `ok=5 warn=0 fail=0 skip=0`；主库和只读副本日历范围、行数、
  交易日计数完全一致。
- 09:05 重新生成 252M HTTP 生产快照，本地 `cloud_backup.duckdb`、主库和只读副本均完成
  合并并核验为 2,557/1,697；本地 preflight 为 `ok=3 warn=0 fail=0 skip=2`。
- 同时恢复多个 timer 时，monitor 首次启动与一次性备份任务争夺 DuckDB 写锁；backup 与
  replica 成功结束后 monitor 按 `RestartSec=30s` 自动恢复，`NRestarts=1`、10 只 watchlist
  进入盘前阶段。后续恢复顺序应先跑完一次性写任务，再启动 monitor/surge-watch。

**回滚基线**：`bb6141982d65f5ed78ed59c24c6c694d11cbd0c1`（`v0.14.0`）。
代码和依赖仍使用受控发布器自动回滚；已写入的交易日历和新增 schema 向后兼容，代码回滚时
可保留，不得在服务运行期间整文件覆盖 DuckDB。

---

## 2026-07-14 · v0.14.0 · 阶段 1 PR-A 数据可信底座

**状态**：PR #76 已 squash merge 为
`bb6141982d65f5ed78ed59c24c6c694d11cbd0c1`，01:01-01:05 部署并完成生产迁移。

**部署内容**：

- 新增 checksum 固定、事务执行的 DuckDB migration v1-v3；项目版本更新为 `0.14.0`。
- 新增数据集快照、覆盖率、质量问题、PIT 数据契约与权威交易日历基础能力。
- 研究同步改为跨表原子事务；只读副本发布、回滚和提交结果按真实状态报告。
- 此版本不修改策略触发、买卖规则或历史业务数据，不执行历史清理。

**生产迁移**：

- 迁移前在无写锁窗口生成 `backup/latest.duckdb.gz`：主库 264,515,584 bytes，
  压缩快照 110,463,165 bytes，完成时间 `2026-07-14 01:03:31 +08:00`。
- 创建 `schema_migration`、`dataset_snapshot`、`dataset_coverage`、
  `data_quality_issue`、`trade_calendar`；账本精确记录 v1-v3 及固定 checksum。
- 迁移后原表行数保持：`daily_bar=1,617,757`、`screen_result=848`、
  `monitor_event=2,337`；`trade_calendar` 初始为 0 行，留给后续权威日历回补。
- 原子刷新 `rquant_ro.duckdb`，主库与只读副本均为 264,515,584 bytes。

**验证**：

- 本地最终 HEAD：`1276 passed in 31.40s`；GitHub Actions Python 3.11/3.12 均通过。
- 发布器状态为 `deployed`，仅重启部署前 active 的 canvas、dashboard、nl-screen、
  panorama-auth 和 panorama；monitor、surge-watch 保持 inactive。
- 部署后 preflight：`ok=5 warn=0 fail=0 skip=0`，smoke screen 命中 8。
- 8501/8502/8504/8506 四个 Streamlit 健康端点均返回 `ok`。

**回滚基线**：`e3b48c0b358c4fd98748f4a57bb142c900294b4c`。代码/依赖使用受控发布器
自动回滚；新增空表为向后兼容 schema，代码回滚时可保留。如需文件级恢复，使用本次迁移前
`backup/latest.duckdb.gz`，不得在服务运行时直接覆盖主库。

---

## 2026-07-13 · v0.13.2 · 受控自动发布

**状态**：已于 15:42-15:45 部署到腾讯云，commit
`e3b48c0b358c4fd98748f4a57bb142c900294b4c`。

**部署内容**：

- 精确 tag/SHA、main 归属与快进校验；tracked 脏文件和并发部署拒绝。
- diff 自动计算服务重启；工作日 09:15-15:10 有重启需求时自动延期。
- 仅允许 7 个 rQuant 长驻服务走 `sudo -n systemctl restart`；基础设施变更自动拒绝。
- 依赖/preflight/服务健康失败自动回滚；审计写入 `logs/production-deploy.jsonl`。
- 包含尚未上云的 `v0.13.1` preflight 只读副本热修复。

**首次引导**：

1. SSH 22 端口恢复后，使用已授权密钥登录 `lighthouse@82.156.0.68`。
2. 从 `77f6ebf` 精确快进到 `v0.13.2`，执行 `uv sync --frozen`。
3. `visudo` 校验通过后，安装
   `/etc/sudoers.d/rquant-production-deploy`（root:root，0440）并复验授权。
4. 以后的日常发布全部调用
   `scripts/deploy-production.sh --target <exact-ref>`。

**验证**：

- 环境包版本为 `0.13.2`，tracked 工作区干净，仅保留 untracked `backup/`。
- 服务重启前后两次 preflight 均为 `ok=5 warn=0 fail=0 skip=0`。
- 只重启发布前 active 的 canvas、dashboard、nl-screen、panorama-auth 和 panorama；
  已按日程退出的 monitor 和 surge-watch 保持 inactive。
- Dashboard `127.0.0.1:8501/_stcore/health` 返回 `ok`。
- 受控入口复核返回 `already_current`，JSONL 审计时间为
  `2026-07-13T15:45:10+08:00`。

**回滚基线**：`77f6ebfb7782521e5c58ffc2e9226e20af9ac96c`；使用受控发布器的自动
回滚链路，不手工改生产数据。

---

## 2026-07-13 · v0.13.1 · preflight 只读副本热修复

**状态**：PR #73 已合并为 `347d57d`，annotated tag `v0.13.1` 已推送；修复已随
`v0.13.2` 于 2026-07-13 一并上云。

**候选内容**：

- `preflight` 的数据新鲜度与 smoke 筛选优先读取只读副本，避免盘中撞 monitor 主库写锁。
- `lsof` 只有 `mem` 等未分类 FD 时改报“无法判断”，不再误报 monitor 未运行。
- 修正 Stage 0 新增 CI 的上下文作用域，使 Python 3.11/3.12 矩阵能实际创建 job。
- 不改数据库 schema、systemd unit、策略逻辑或生产数据。

**部署后验证**：

1. `rquant.__version__` 输出 `0.13.1`。
2. monitor 运行期间执行 `.venv/bin/rquant preflight`，不再出现 DuckDB conflicting lock。
3. `duckdb_lock_detail` 可以预警未分类 FD，但不应因此令 preflight 失败。

**回滚命令**：`git checkout 77f6ebf && /home/lighthouse/.local/bin/uv sync --frozen`

---

## 2026-07-13 · v0.13.0 · 研究可信度阶段 0

**状态**：已部署到腾讯云，commit `77f6ebf`。

**部署内容**：

- Strategy Lab 研究记录增加四级可信度 manifest；旧记录自动降级为探索性。
- 页面增加 N 字、科创/创业、集合竞价三项当前可信度警示。
- 新增研究基线、中文总路线图和 GitHub Actions CI。
- 版本元数据和 README 对齐到当前活跃项目状态。

**验证**：

- `uv sync --frozen` 完成，包版本由 `0.1.0` 更新到 `0.13.0`。
- `git rev-parse --short HEAD` 为 `77f6ebf`，`rquant.__version__` 为 `0.13.0`。
- 26 个 systemd unit 验证通过，9 个生产 unit 状态正常，monitor 盘中 active/running。
- preflight 的数据新鲜度与 smoke 检查因直连主库撞 monitor 写锁而失败；部署本身正常，
  该问题由上方 `v0.13.1` 热修复处理。

**回滚命令**：`git checkout 20eadf9 && /home/lighthouse/.local/bin/uv sync --frozen`

---

## 2026-05-06 · v0.12.1 · hotfix：nl-screen 只读 DuckDB

**背景**：节后首日 09:30 开盘 monitor 启动失败，38 次 crash-loop + 持续 OnFailure
PushDeer 告警。根因：`rquant-nl-screen.service`（自 4/30 部署起常驻）持有
`/home/lighthouse/rquant/data/rquant.duckdb` 的写锁（PID 2597296），monitor 拿
不到锁就退出。

**部署内容**：

- `DuckDBStore.__init__` 新增 `read_only: bool=False` 参数；read_only=True 时跳过
  `_init_schema()` 的 DDL
- `dashboard/nl_screen.py` 改用 `DuckDBStore(settings.duckdb_path, read_only=True)`
  打开 DB（NL 选股是纯查询场景，与 `dashboard/app.py` 同模式）
- 不动 systemd unit、nginx、依赖

**入口**：无变化（NL screen 仍在 8502 / `/nl/`，monitor 仍 systemd 调度）

**验证**：
- 本地 smoke：read-only 开 DB 不锁、能读、写被拒 ✓
- 云端 hotfix 流程：先停 nl-screen 释放锁 → reset-failed monitor → 起 monitor（active running）→ pull fix 分支 → restart nl-screen ✓
- monitor PID 716834 自 09:49:58 起稳定，nl-screen restart 后 monitor PID 不变 ✓
- ⏳ 待验：浏览器跑 NL query 时 monitor 不被踢（read-only + 写锁理论上不冲突，等真请求覆盖）

**回滚命令**：

```bash
# 回到 v0.12.0
cd /home/lighthouse/rquant
git checkout v0.12.0
sudo systemctl restart rquant-nl-screen.service
# ⚠️ 回滚后会重现 nl-screen 占写锁问题，monitor 与 nl-screen 不能同时跑。
#   要么停 monitor 让 nl-screen 用，要么停 nl-screen 让 monitor 用。
```

**预防**：未来任何想跟 monitor 共存的 DB 消费者（dashboard / nl-screen / 新增 Streamlit / 临时 CLI 查询）必须用 `read_only=True` 打开 DuckDB。

---

## 2026-04-30 · v0.12.0 · Week 7 NL 选股

**部署内容**：

- 新 systemd 服务 `rquant-nl-screen.service`（端口 8502，独立 Streamlit）
- nginx 增加 `/nl/` 反代 → 8502，与 `/dashboard/` 共用 `.rquant-backup.htpasswd`
- `.env` 加入 `DEEPSEEK_API_KEY` / `DEEPSEEK_BASE_URL` / `DEEPSEEK_MODEL`
- `openai>=1.0` 依赖（实际安装 2.33.0）

**入口**：
- 浏览器 http://82.156.0.68:8081/nl/（用 `.rquant-backup.htpasswd` 凭据）
- 监控看板继续走 8501 / `/dashboard/`，无影响

**验证**：
- 内网 `127.0.0.1:8502/_stcore/health` ✓
- 外网 nginx /nl/ 反代 + auth ✓
- 浏览器 NL 流程跑通（解析 + Stage Cards + 运行 + 结果）

**已知遗留**：
- 前端 UX 与最终目标差距较大，下一迭代优化（Week 7.5 真画布会处理一部分）
- `/nl/` 与 `/dashboard/` 共用 htpasswd，未来想给协作者单独开放 NL 时
  按 `deploy/nl-screen.md` "未来：单独开放 NL" 一节切独立 htpasswd

**回滚命令**：

```bash
# 1. 停 systemd 服务
sudo systemctl stop rquant-nl-screen.service
sudo systemctl disable rquant-nl-screen.service
sudo rm /etc/systemd/system/rquant-nl-screen.service
sudo systemctl daemon-reload

# 2. nginx 摘掉 /nl/ location（手编辑 /www/server/panel/vhost/nginx/rquant-backup.conf 删 location /nl/ 块）
sudo nginx -t && sudo systemctl reload nginx

# 3. 代码回滚（main 上拉前一个 tag）
cd /home/lighthouse/rquant
sudo -u lighthouse git checkout v0.11.3
# 或回到 v0.12.0 上一个 commit：
# sudo -u lighthouse git reset --hard v0.11.3
sudo -u lighthouse /home/lighthouse/.local/bin/uv sync

# 4. .env 删除 DEEPSEEK_* 三行（手工编辑或 sed）
sudo -u lighthouse sed -i '/^DEEPSEEK_/d' /home/lighthouse/rquant/.env
sudo -u lighthouse sed -i '/^# ===== LLM (Week 7/d' /home/lighthouse/rquant/.env
```

监控看板（`rquant-dashboard.service`）不受回滚影响，继续运行。
