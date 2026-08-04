# 受管任务生命周期

## 目的与适用范围

rQuant 的日常交付由 Codex 受管：用户只参与产品需求、方案取舍、用户可见行为的业务验收、
高风险变更的明确授权，以及对无法恢复的业务工作是否放弃的决定。用户不需要操作 Git、
worktree、测试、PR、CI、合并、tag、部署、回滚、验证或清理。

Codex 必须负责一个任务从创建到关闭的完整生命周期，包括创建隔离工作区、实现、测试、
审查、PR、CI 跟进、合并、tag、受控部署、健康检查和符合本规范的回收。高风险基础设施、
生产数据写入或修复、密钥轮换，以及其他项目指令列出的高风险操作，仍须在执行前取得用户
单独明确授权。

本文件是任务生命周期的唯一详细规范；`AGENTS.md`、`CLAUDE.md` 与
`docs/production-release.md` 必须引用并遵守它。

## 工具落地前的人工协议

在自动生命周期工具落地前，本节的人工协议是受管任务的唯一事实来源。当前受管的 private
GitHub origin 中，每个会改变仓库内容的受管任务都必须有一个 GitHub PR（可先为 draft）。PR
正文或专用 lifecycle comment 必须随状态更新并记录：任务状态、仓库、PR、分支、head SHA、
base SHA、worktree、owner thread/tool。未来若迁移到 Gitea，必须先为该 remote 明确定义并验证
等价的、可保留的不可变 review/merge evidence adapter，才可启用受管合并与回收；在 adapter
就绪前，相关任务必须 `blocked` 或 `quarantined`，不得以提交祖先推测或其他猜测替代证据。
跨会话任务或在创建正式 PR 前被 `blocked` 的任务，Codex 必须在安全且技术可行时于结束会话前
推送分支并创建或更新 draft PR，保留上述记录。

每个任务还有本地镜像注册表
`<git-common-dir>/rquant-lifecycle/tasks/<task-id>.json`。必须在同一目录先写临时文件再原子 rename，
其字段至少包括 `task_id`、`status`、`resume_state`、`owner_tool`、`owner_thread`、`branch`、
`worktree`、`base_sha`、`pr`、`evidence` 和各项时间戳。PR 可用时是耐久的远端事实来源，本地
JSON 是其镜像和无 PR 时的临时事实来源。

任务 owner 必须以原子 `mkdir` 获取
`<git-common-dir>/rquant-lifecycle/leases/<task-id>.lock/`，并在目录内写入 owner、thread、process、
branch、时间戳和 heartbeat 元数据。owner 在每次仓库变更前必须刷新 heartbeat；其他 owner 不得
对持租约任务进行任何变更。租约年龄本身不构成接管理由。接管前只能做只读核验，确认原 owner /
thread / process 已不活跃、保留现有工作，并在本地注册表及（如存在）PR 同时记录原 owner、新
owner、接管原因和时间戳。任务 `closed` 时或发生明确 handoff 时必须释放租约；`blocked` 与
`quarantined` 必须有明确 handoff，禁止静默接管。

只有讨论且未改变仓库内容的工作不进入受管交付生命周期，无须 PR 或交付回执。发生仓库改动后，
即使必须立刻从 `active` 进入 `quarantined`，也必须先写入临时 pre-PR 隔离记录：任务、仓库、
分支、当前 head/base SHA、worktree、owner、原因、时间戳，以及「PR 证据元组暂不可用」的原因。
Codex 必须在安全且技术可行时推送并创建或更新 draft PR，将本地 JSON 的隔离记录立即迁入 PR
作为事实来源。若外部故障使此事无法完成，任务保持 `quarantined` 并须在该任务 JSON 的
`evidence` 中形成一份本地隔离回执，其中 PR number/URL、`merged_at` 与 `merge_commit_sha`
标记为 N/A 并写明原因；该回执不能用于 `closed` 或清理。恢复发生在同一会话时必须立即迁入
draft PR；否则下一位负责该任务的会话必须在首次安全机会完成迁入。`quarantined` 在耐久 PR 技术
可用前不得清理。

准备进入 `closed` 时，Codex 必须先将含不可变证据与计划清理的 provisional receipt 成功发布到
该 PR，并原子更新任务 JSON；清理完成后再将终态交付回执发布到 PR，且只有发布成功后才可将
状态设为 `closed`；并在对用户的交接消息中回显同一回执摘要。任务进入 `quarantined` 且已有
draft PR 时，必须立即尝试将隔离终态回执发布到该 draft PR；若 PR 发布失败，适用后文规定的本地
隔离回执与原子状态转换顺序。人工收集证据、作出清理决定和写入回执均不得绕过后文门禁。

## 命名与隔离

每个新任务必须先执行 `git fetch`，并立即核验最新的 `origin/main`，再从该精确引用创建独立
分支和 worktree。不得在本地 `main` 上开发功能；`main` 只用于集成、发布和必要的只读检查。

| 创建工具 | 分支格式 | 示例 |
| --- | --- | --- |
| Codex | `cdx/YYYYMMDD-<kind>-<topic>` | `cdx/20260804-fix-duckdb-lock` |
| Claude Code | `cc/YYYYMMDD-<kind>-<topic>` | `cc/20260804-feat-vp-engine` |

`<kind>` 使用 `feat`、`fix`、`docs`、`refactor`、`test`、`chore` 或 `deploy`。提交信息仍使用
Conventional Commits，例如 `feat:` 或 `fix:`；分支前缀只用于标明创建工具和任务归属。

Codex 或 Claude 可控制位置的新 worktree 必须创建在 `.worktrees/` 下，且目录名只使用字母、
数字和连字符，例如 `.worktrees/cdx-20260804-fix-duckdb-lock`；目录名不得包含斜杠。外部
harness 控制的位置和既有 legacy worktree 不受此位置规则约束，但仍受本规范的状态、保护、
审计与回收门禁约束。外部 harness worktree 只能在全部回收门禁通过、任务 ownership 可证明，
且使用 harness/tool 支持的非强制 remove 或 close 操作时才可安全回收；不得手工删除其目录。若
不具备受支持的移除机制或无法证明 ownership，任务必须 `quarantined`，不得进入 `closed`。历史
分支、历史 worktree 和既有非规范命名均视为 legacy，不要求重命名，也不得仅因不符合新命名而删除。

## 切换范围

legacy 分支或 worktree 指本策略任务开始前已经存在的对象；本策略任务本身是首个受管任务，
不属于 legacy。本策略合并后，所有从最新 `origin/main` 新建的任务都必须受管并遵循本文件。
legacy 对象本轮不重命名、不移动、不重置、不删除，但必须在后续审计时被识别并受到保护。

## 状态与责任

每个受管任务只能处于下列状态之一：

| 状态 | 含义与下一步 |
| --- | --- |
| `active` | 正在实现、测试或审查；Codex 持有任务租约并维护 worktree。 |
| `blocked` | 因外部依赖、CI、授权或业务决定无法继续；保留现场并说明阻塞原因。 |
| `ready` | 验证完成，已满足 PR 与业务验收要求，等待合并或发布窗口。 |
| `merged` | 精确对应的 PR 已合入 `main`，但 tag、交付验证或回收尚未全部完成。 |
| `deployed` | 已完成受控部署和健康检查，等待安全回收证据。 |
| `closed` | 所有交付与回收证据完整，远端分支、该任务 worktree 和本地分支已安全回收。 |
| `quarantined` | 工作区 dirty、存在未知内容、证据不完整或回收失败；冻结并保留，等待 Codex 调查或用户作业务决定。 |

Codex 对技术状态转换、测试结果和回收证据负责。改变用户可见行为的任务，未经用户业务验收
不得进入 `ready` 或合并；纯技术、文档和流程任务在技术测试与 CI 门禁通过后可由 Codex 自动
推进。若工作无法恢复，Codex 应说明业务影响并请用户决定继续还是放弃，而不是要求用户判断
Git 分支能否删除。

进入 `quarantined` 时必须先在任务 JSON 原子记录进入前的状态为 `resume_state`；恢复只按该字段
和下表执行，不得凭当前会话猜测或跳跃状态。

正常流转与恢复路径如下；除表中记录部署 N/A 的路径外，不存在自动跳过交付验证或回收门禁的
终态捷径。

| 当前状态 | 允许的下一状态 | 前提 |
| --- | --- | --- |
| `active` | `ready`、`blocked`、`quarantined` | 完成验证；如改变用户可见行为，已获用户业务验收，才可进入 `ready`。发生不安全或证据异常可立即隔离，但须遵循 pre-PR 隔离记录规则。 |
| `ready` | `merged`、`blocked`、`quarantined` | PR 合并进入 `merged`；其余须记录原因。 |
| `merged` | `deployed`、`closed`、`quarantined` | 需要生产部署时进入 `deployed`；仅已记录部署 N/A 及理由的任务可直达 `closed`。 |
| `deployed` | `closed`、`quarantined` | 受控部署与健康检查成功后，且全部回收门禁通过，才可关闭。 |
| `blocked` | `active` | 阻塞原因已记录且已解除。 |
| `quarantined` | `resume_state` 或更早的安全状态 | 只能回到 JSON 中的 `resume_state` 或其更早安全状态，并重走正常验收、`ready` 与 CI 门禁；pre-merge 隔离不得跳至 `merged` 或 `deployed`，post-merge 隔离先恢复为 `merged`。 |

## 集成锁与受保护 checkout

合并、tag、部署和清理必须串行，并通过原子 `mkdir` 获取 Git common dir 的
`<git-common-dir>/rquant-integration.lock/`。锁目录内必须记录 `owner`、`task`、`branch`、
`timestamp` 和 heartbeat，成功创建后立即重新核验锁仍归自己所有；已存在的锁一律阻塞操作。
stale lock 仅可在只读核验原 owner / thread / process 已不活跃，并记录原 owner、新 owner、原因
和时间戳后接管，不得按年龄或盲目删除。持锁操作必须在 finally 路径释放集成锁。

以下对象一律受保护：主本地 checkout、`main`、runtime worktree、deploy worktree、legacy
worktree，以及带保护标记的对象。主本地 checkout 是仓库的常规本地入口 checkout；runtime
worktree 是正被本地或生产常驻服务使用的 checkout；deploy worktree 是受控发布器实际使用的
checkout。保护标记或人工注册表必须可追溯地记录对象路径、类型、owner、用途和最后核验时间；
没有该证据时按受保护对象处理。

本策略阶段不定义也不授权对 dirty、diverged 或归属不明的本地 `main` 进行破坏性 reconciliation。
这类状态必须由单独、可审计的 remediation task 处理，先保留并归属所有提交与本地文件，再形成
clean/reconciled 证据。在此之前，合并、tag、部署与回收一律阻塞；禁止盲目 `pull`、`reset`、
`clean`、覆盖或删除来取得表面 clean 状态。

## 合并、发布与回收门禁

PR 必须使用受管分支，并在可合并、所需 CI 全绿后 squash merge。由于 squash 会改变提交祖先
关系，是否可以清理不得只凭 `git merge-base --is-ancestor` 判断。不可变 PR 证据元组必须精确为
`{repository, PR number/URL, base=main, merged_at, head.sha, merge_commit_sha}`；本地任务分支的
HEAD 必须等于 `head.sha`，并且在新鲜 `git fetch` 后 `merge_commit_sha` 必须位于
`origin/main`。

一个任务只有同时满足以下证据，才可进入 `closed`。需要生产部署的任务从 `deployed` 进入
`closed`；不需要生产部署的任务在记录「部署不适用（N/A）」及其理由后，可从 `merged` 直接
进入 `closed`：

1. 已记录完整不可变 PR 证据元组，且其对应当前任务；当前 GitHub origin 自动删除远端 head branch
   后，保留的 PR 元数据仍是充分证据，不要求另有删除事件记录。Gitea remote 未先具备本文件要求的
   evidence adapter 时，不得进入此门禁或关闭。
2. 任务分支在 PR 后没有新的提交；本地引用与已核验的 `head.sha` 一致。
3. 任务 worktree 不存在 tracked 或 untracked 改动，除非它们属于该任务开始前已声明的可丢弃
   allowlist；每个任务的 allowlist 只可包含可重复生成的产物，并须在移除前记录且完成相应处理。
   任何未知 ignored 文件都必须进入 `quarantined`，不得作为 clean 的例外。
4. 没有其他活动任务持有该 worktree、分支或任务租约；回收操作持有仓库级集成锁。
5. 发布分类的 tag、部署与健康检查要求已满足，或已记录适用的 N/A 理由。
6. 目标不是任何受保护 checkout 或引用，且没有其他保护标记。

准备进入 `closed` 时必须按以下顺序执行：先准备不可变 PR 与清理证据，将 immutable
PR/head/merge evidence 和计划清理成功发布为 PR 的 provisional receipt，并原子更新任务 JSON；
在该步骤成功前不得开始清理。随后才从受保护的集成上下文中执行清理。清理后 Codex 必须成功将
final receipt 发布到 PR，再原子更新任务 JSON；只有 final receipt 发布成功后才可将状态设为
`closed` 并释放租约。清理时，Codex 在持有集成锁时回收该任务的远端功能分支、以正常方式移除该任务
worktree（不得使用 force）。外部 harness worktree 必须改用其受支持的非强制 remove 或 close 操作，
绝不手工删除目录；若该操作不可用或 ownership 无法证明，立即停止并按后文隔离回执顺序进入
`quarantined`。随后必须
重新核验精确的本地 branch、其 HEAD 与已移除 worktree 的身份：仅当它们仍与已核验的受管任务完全匹配时，才明确允许执行
`git branch -D <exact-verified-branch>`。`-D` 绝不得用于 legacy 或未知分支。任何部分清理失败或
final receipt 发布失败都必须依次执行：

1. 立即停止并保留所有尚未移除的 artifacts。
2. 在任务 JSON 中形成并原子持久化本地 quarantine receipt，记录已完成和失败的清理结果以及 PR
   发布失败。
3. 将状态 `quarantined` 作为该本地记录的一部分原子写入，使终态本地回执先于或与隔离状态同时存在。
4. 之后重试向 PR 发布 quarantine/final receipt；PR 发布失败不得阻塞这项安全状态转换，但任务不得
   `closed`。

不得声称已移除的 artifacts 被保留，也不得以破坏性方式重建它们。

当前尚未提供自动回收工具或脚本。工具落地前，Codex 必须人工收集上述证据、作出回收或
`quarantined` 决定并记录交付回执；人工执行不豁免任何门禁。

## 发布分类

每个受管任务必须在合并前于 PR lifecycle record 中选择且仅选择一个发布分类；发布分类只按是否
影响产品、runtime 或生产部署判定，与 Conventional Commit 的 kind 无关。不得因 kind 而省略或
推断分类。

| 发布分类 | 覆盖的变更 | 必需交付 |
| --- | --- | --- |
| 发布类 | 任何改变产品可见行为、runtime 行为、打包产物或生产部署结果的变更。 | 创建指向合并后 `origin/main` 的 annotated SemVer tag，完成受控部署并通过健康检查。 |
| 非发布类 | 不改变产品、runtime、打包产物或生产部署结果的代码、文档或流程变更。 | 在 PR lifecycle record 和终态交付回执记录 tag、部署、健康检查均为 N/A 及具体理由。 |

`feat`、`fix`、`docs`、`test`、`refactor`、`chore` 和 `deploy` 均可能属于任一类，必须按实际影响
记录；尤其 `test`、`refactor`、`chore` 不可默认任一类。不得将发布类要求以笼统的“维护”理由
标记为 N/A。因此从 `merged` 进入 `closed` 对每个任务均有确定的 tag、部署和健康检查结论。

普通发布只能通过 `scripts/deploy-production.sh --target <exact-tag>` 执行，Codex 负责云端验证和
相关健康检查。高风险基础设施变更（如 systemd、nginx、frp、sudoers、生产数据修复或密钥轮换）
不是普通发布：必须另有用户明确授权的高风险 runbook，并使用精确 tag；Codex 仍负责执行相关
云端验证和健康检查。两类流程均不得以本地猜测替代 cloud 验证。

## 交付回执

每个任务在进入 `closed` 或 `quarantined` 前，Codex 必须形成一份可追溯的终态交付回执。`closed`
回执必须包含完整不可变 PR 证据元组；`quarantined` 回执如已有 draft PR，必须包含 draft PR 身份
`{repository, PR number/URL, base=main, head.sha, draft=true}`，并明确 `merged_at` 与
`merge_commit_sha` 不可用的原因。若安全或技术条件使 draft PR 暂不可建，本地隔离回执可将
PR number/URL、`merged_at` 与 `merge_commit_sha` 标记为 N/A 并写明原因；它只能维持
`quarantined`，不能关闭或清理，且必须在 draft PR 可建立后转入 PR。部分清理或 PR 回执发布失败时，
原子持久化到任务 JSON、并同时写入 `quarantined` 状态的本地 quarantine receipt 满足此处的终态
回执要求；之后仍必须重试发布到 PR。回执至少记录：

- 任务标识、分支和 worktree 路径；
- `closed` 的完整不可变 PR 证据元组 `{repository, PR number/URL, base=main, merged_at, head.sha, merge_commit_sha}`，或上述 `quarantined` draft 身份／本地 N/A 记录与不可用原因；
- 本地测试与 CI 的结果或证据链接；
- annotated tag，或其 N/A 理由；
- 受控部署与健康检查结果，或部署 N/A 及其理由；
- 远端分支、本地分支和 worktree 的清理结果，或进入 `quarantined` 的具体原因；
- 记录时间戳。

回执是关闭判定的一部分，不得在缺少回执时将任务标记为 `closed`。当前 GitHub origin 自动删除
head branch 的情形，回执中的完整 PR 证据元组即为身份依据；未来 Gitea 必须使用已定义的 evidence
adapter，不得以 ancestry 猜测代替。

## 异常与存量保护

证据不完整、worktree dirty、存在未推送提交或文件归属不明时，Codex 必须使用
`quarantined`，先调查、补充提交或迁移确认后的成果。对于清理操作或未核验状态，禁止通过以下
方式绕过证据要求：

- `git reset --hard`（唯一的受限例外是既有、已获授权的 `scripts/deploy-production.sh` 内部回滚）
- `git clean`
- `git worktree remove --force`
- 按分支或 worktree 年龄自动删除
- 删除未知内容、legacy 分支或 legacy worktree

本阶段仅建立流程和规范：**不授权清理现有存量分支或 worktree**，不创建会执行清理的脚本，
也不对现有存量进行重命名、重置、删除或迁移。存量须在后续独立审计中逐项分类，并按本规范
的证据要求安全处理。
