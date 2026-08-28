# rQuant 工作负载解耦后续开发交接

**交接日期：** 2026-08-21
**来源工具：** OpenAI Codex
**目标工具：** Claude Code，由用户手工启动和管理
**交接类型：** 代码实现接力；Codex 保留最终独立验收职责
**生产状态：** 本次交接不授权部署、生产写入、systemd 变更或 root 策略安装

## 1. 精确交接基准

- 仓库：`/Users/roxor/brain/30-projects/rQuant`
- 来源 worktree：`/Users/roxor/brain/30-projects/rQuant/.worktrees/workload-isolation-final`
- 来源分支：`cdx/workload-isolation-final`
- 精确代码基准提交：`5c93d4162f013c6eb6c6312ad8facbb371ff00f3`
- 交接文档提交：由 Codex 最终交接消息提供；该提交必须以精确代码基准为直接父提交，且只包含本文件
- 基准提交信息：`feat(ci): produce R07 dual-python gate evidence`
- 交接时来源 worktree：clean
- 来源分支相对其远端跟踪分支：ahead 86

Claude Code 不得直接写来源 worktree 或继续使用 `cdx/` 分支。应从 Codex 最终交接消息给出的精确 handoff commit 创建独立分支和 worktree，并验证其直接父提交是上述代码基准，例如：

```bash
cd /Users/roxor/brain/30-projects/rQuant
HANDOFF_DOC_SHA='<由 Codex 最终交接消息提供的 40-hex>'
git worktree add \
  -b cc/workload-isolation-continuation \
  .worktrees/workload-isolation-cc \
  "${HANDOFF_DOC_SHA}"
test "$(git rev-parse "${HANDOFF_DOC_SHA}^")" = \
  '5c93d4162f013c6eb6c6312ad8facbb371ff00f3'
```

开始写入前必须确认：HEAD 精确匹配、worktree clean、分支为 `cc/` 前缀。不得覆盖、回退或混入其他 worktree 的修改。

## 2. 总目标

完成《工作负载解耦与故障隔离架构设计》中的七条流水线和六项附加能力，完成端到端测试、双 Python CI、PR 合并、受控发布和真实交易日 shadow 验收；只有所有门槛通过后才能声明完成或退役旧链路。

主要规范：

- `docs/architecture/2026-07-22-workload-isolation-design.md`
- `docs/architecture/production-interpreter-authority.md`
- `AGENTS.md`

必须保留的边界：不做实盘自动下单、Tick/Level2、高频；不得引入 Kafka、PostgreSQL、Kubernetes 等新基础设施；生产 DuckDB 继续遵守单写者约束。

## 3. 已完成并冻结

以下内容已有实现和本地专项证据，不应重新设计或无关重构：

1. 持久 Lab Job Center、worker、checkpoint、暂停续跑和 ETA。
2. 统一分钟网关、额度租约、revision、stale/degraded 和独立 cursor。
3. PIT 盘中特征服务及历史回放一致性基础。
4. 独立策略 runner、冻结 StrategySpec、独立状态和崩溃重放幂等。
5. signal bus、通知 outbox、paper consumer/broker、T+1 和下一分钟成交模型。
6. serving 不可变代际、只读页面和资源准入。
7. 慢变参考数据、schema/definition/experiment registry、保留恢复、凭据能力封装。
8. R07 Phase A differential no-activation gate tranche A。

R07 tranche A 的正式审查结论：

- `SPEC-APPROVED-R07-TYPESTATE-IMPL`
- `QUALITY-APPROVED-R07-DR-A`
- Q1-Q4 全部关闭。
- 固定 full-suite manifest 在进入 CI 证据任务前为 11,772 nodeids。
- B01-B19、candidate diff gate、static root/forbidden-definition gate 已通过。

关键提交链末端：

```text
9823ee28 fix(r07): isolate differential gate evidence verification
e4a249df fix(r07): keep probe facade imports inert
cce9ea76 test(r07): harden cold-start probe evidence
5c93d416 feat(ci): produce R07 dual-python gate evidence
```

## 4. 已编码但尚未独立验收

提交 `5c93d4162f013c6eb6c6312ad8facbb371ff00f3` 实现了 R07 tranche B 的 CI 证据生产部分，但尚未经过后续独立 SPEC 审查和代码质量审查，也没有 GitHub 云端 Python 3.11/3.12 真实运行证据。

涉及：

- `.github/workflows/ci.yml`
- `scripts/r07_ci_evidence.py`
- `tests/unit/test_r07_ci_evidence.py`
- `src/rquant/signal_family_differential_gate.py`
- `tests/fixtures/r07_differential_gate/policy-v1.json`
- full-suite manifest

实现者报告的本地结果只能作为待复核证据：新增测试 35 项、受影响回归 72 项、Python 3.12 本地 gate summary 20/20、manifest 更新至 11,807。接手后第一步不是继续堆功能，而是独立核对该提交是否严格符合 `CI Evidence And Deployment Gate` 规范，特别检查：

1. 三个静态 job ID 是否精确，是否避免 matrix 改写 job identity。
2. PR、tag、manual、非 main、错误 SHA 是否绝不生成可部署 evidence。
3. 两个 Python summary 是否绑定同一 workflow/run/attempt/commit/tree，且零 skip/deselect。
4. canonical JSON、严格字段、额外字段、tamper 和 cross-binding 是否 fail closed。
5. artifact 名称、内部路径、90 天 retention 和 action 完整 SHA pin 是否精确。
6. policy allowlist 是否只覆盖经审查的完整 diff，没有通过扩大 allowlist 掩盖越界。
7. manifest 从 11,772 到 11,807 的 35 个新增 nodeid 是否全部真实、唯一、稳定。

发现问题时由同一实现者修复；完成后保留可供 Codex 独立重跑的命令和证据。

## 5. 尚未完成的关键路径

必须按依赖顺序推进；不要并行修改同一发布或 authority 边界。

### WP1：冻结 CI 证据生产

- 完成上节独立 SPEC/质量复核和必要修复。
- 本地不能宣称 Python 3.11/3.12 GitHub job 已通过。
- 后续 PR 的真实 `push main` run 才能产生部署证据。

### WP2：Release A/B 部署证据门

实现 `production-interpreter-authority.md` 中的两阶段 bootstrap：

- 固定 GitHub workflow/artifact 下载器。
- 服务器 evidence cache 的原子写入和只读校验。
- symlink、非普通文件、文件名/commit/tree/channel/digest mismatch 全部阻断。
- target policy 从 Git object 读取，证据验证必须发生在 checkout、服务停止或生产变更之前。
- Release A 仅允许一次 `disabled_for_bootstrap`；下一目标必须是声明精确 predecessor 的 `enforced` Release B。
- Release B 以后所有 forward/rollback target 都必须 enforced 且有精确证据。
- checkout 前失败保持现有 checkout/services 不变；checkout 后失败走现有精确回滚。
- 扩展部署审计，不能把失败降级为 warning。

预期核心测试：`tests/unit/test_production_deploy.py` 的 R07 bootstrap、错误 channel/SHA/tree/run/artifact/cache、禁用策略和 pre-checkout 不变性矩阵。

### WP3：Phase B successor registry 与 staged overlay

严格实现四个冻结 schema：

- `SuccessorChannelV1`
- `SuccessorBundleV1`
- `OverlayDeclarationV1`
- `OverlayBundleV1`

要求：严格原生类型、无额外/重复字段、canonical bytes、精确 hash preimage、排序和去重；payload model 必须解析为真实、manifest 覆盖的 current-family class。不得把 current 语义覆盖到 v2，也不得让 absent/partial overlay 进入 READY。

预期核心测试：`tests/unit/test_signal_family_successor_registry_reset.py`。

### WP4：Phase C root verifier、receipt 与 readiness

这是剩余风险最高的工作包：

- 固定 root-owned release policy。
- OS 分离的 root verifier 与非特权 generation child。
- 严格、限长、canonical IPC；子进程不可发现 verifier/store authority。
- exact callable-object allowlist、service binding、manifest source hash。
- root-owned append-only receipt/audit store。
- deployment lock 二次验证，authority 变化时阻断。
- 五个 pair receipt 的精确集合、epoch/CAS、expiry/rollback/revoke。
- 状态只允许未就绪或 READY；不得引入 ATTESTING/ACTIVATED。
- 审计不得包含 payload、环境、凭据、原始异常文本。

预期核心测试：

- `tests/integration/test_signal_family_verification_reset.py`
- `tests/integration/test_signal_family_root_verifier_isolation.py`
- `tests/integration/test_signal_family_root_policy_anchor.py`
- `tests/unit/test_signal_family_readiness_reset.py`
- `tests/unit/test_signal_family_verification_audit.py`

注意：安装或更新 `/etc/rquant/signal-family-verifier-policy-v1.json` 是单独的 root 基础设施事务，必须先取得用户明确授权。代码实现和离线测试本身不构成安装授权。

### WP5：spool ticket、崩溃恢复与 SQLite generation authority

- 完成未决的 ticket/claim/finalize 状态机。
- 覆盖 crash、orphan、retry、duplicate、conflict、concurrency 和 generation return。
- 所有持久化前后不变量、fsync/atomic pointer、SQLite generation ownership 必须可验证。
- 不得借此实现或激活生产 v3 writer；未来 v3 publication primitive 仍需单独授权。

> **本轮撤回（Codex round-2 order 2026-08-25，裁决 1）**
>
> 上面 WP5 小节的原文保留不改，但其中两项要求已由 Codex 在第二轮工单里**撤回**，不再是本次
> 交付的验收条件：
>
> 1. 「完成未决的 ticket/claim/finalize 状态机」——该状态机在规格与代码里都没有锚点，属于本
>    交接文档单方面引入的新要求。
> 2. 「SQLite generation ownership」——同样没有规格/代码锚点。
>
> **保留有效的部分**：既有的 job/shard claim-finalize 语义与既有的 immutable generation 语义
> 一字不改，继续按原样验收；崩溃、orphan、retry、duplicate、conflict、concurrency 覆盖要求
> 保留；fsync/atomic pointer 的可验证性要求保留；「不得实现或激活生产 v3 writer」保留。
>
> **再引入的门槛**：若将来确实需要新的 ticket/claim/finalize 状态机或 SQLite generation
> ownership 语义，必须另开 ADR 并单独获得授权，不能借这份交接文档的历史措辞引入。
>
> WP5 的实际交付已按 `wp5-rulings.md` R1 执行（两项均未实现），因此本条撤回**不产生任何代码
> 变更**，只是把交接文档与实际交付和 Codex 裁决对齐。

### WP6：最终回归、合版和上线

- 精确更新 full-suite manifest。
- Python 3.11/3.12 全量 CI 绿；不得把 skip/deselect 表述为通过。
- 独立最终 SPEC 和代码质量 sweep，关闭阻断范围内 P0-P2。
- 更新 `CHANGELOG.md`、相关 README/docs 和 `DEPLOY.md`。
- PR 全绿后 squash merge，tag 指向精确 merge SHA。
- 只通过 `scripts/deploy-production.sh --target <exact-tag-or-full-sha>` 受控部署。
- 云端备份、副本、preflight、systemd/slice、服务和 timer 验收。
- 新旧实时链路至少完成真实交易日 shadow 对账并达到退休门；未达标时保留旧链路。

## 6. 不得越过的停止条件

遇到以下情况必须停下并报告，不得自行绕过：

1. 需要修改 systemd/nginx/frp/sudoers、生产数据库或生产密钥。
2. 需要安装或更新 root-owned verifier policy。
3. 需要启用 current-family writer、切流、迁移 cursor 或删除旧链路。
4. 交易日 09:15-15:10 需要生产部署、服务重启或生产写入。
5. 两轮局部修补后仍有同一阻断 finding，必须架构复盘。
6. 工作树出现来源不明修改，或基准/分支/实际执行身份不能确认。

## 7. 测试与审查纪律

- 每个工作包先红测后实现。
- 先 SPEC 审查，SPEC 通过后再做代码质量审查。
- 审查先整体 sweep，再给稳定 finding ledger；不要每轮只冒出一个孤立问题。
- P0/P1 阻断；P2 只在冻结 threat model/生产路径阻断范围内阻断。
- 同一实现者修复，同一审查者复核；最多两轮局部修补。
- 测试按风险分层复用：新红测、受影响模块、冻结高风险路径、最终全量。
- 被 skip/deselect 或环境缺口阻止的测试必须明确列为缺口。
- 不得访问 `.env`、真实凭据或生产环境完成普通编码测试。

## 8. 交回 Codex 的验收回执

Claude Code 完成后，请把以下内容原样交给 Codex；信息不全时 Codex 不开始最终验收：

```text
HANDOFF_RETURN_V1
source_base_sha=5c93d4162f013c6eb6c6312ad8facbb371ff00f3
cc_branch=cc/workload-isolation-continuation
cc_head_sha=<40-hex>
worktree_clean=true|false
commit_list=<按时间顺序列出 SHA 和 message>
changed_files=<完整文件清单或可审计 diff 边界>
completed_work_packages=<WP1..WP6>
spec_review_results=<稳定批准标识或 finding ledger>
quality_review_results=<稳定批准标识或 finding ledger>
tests_py311=<命令、passed/failed/skipped/deselected>
tests_py312=<命令、passed/failed/skipped/deselected>
full_suite_manifest=<case 数、shard 数、aggregate digest>
github_pr=<URL/编号，若有>
github_ci=<run URL/ID 和各 job 结论，若有>
production_changes=none|<逐项列出并附用户授权>
open_findings=<无则 none>
rollback_or_cleanup_notes=<内容>
```

同时提供：

- `git status --short --branch`
- `git log --oneline 5c93d416..HEAD`
- `git diff --stat 5c93d416..HEAD`
- 所有实际运行测试的原始摘要
- 所有未运行、skip、deselect 或环境受限项

Codex 将从精确 base/head 独立审查，不接受“已经测试过”作为替代证据，也不会默认信任实现者自述。

## 9. 可直接交给 Claude Code 的启动提示

```text
这是用户自有 rQuant 仓库中的授权软件开发和可靠性工作。请先阅读：
/Users/roxor/brain/30-projects/rQuant/.worktrees/workload-isolation-final/docs/handoffs/2026-08-21-workload-isolation-cc-handoff.md

严格从 Codex 最终交接消息给出的 handoff commit 创建 cc/workload-isolation-continuation 独立分支和独立 worktree，并验证该提交的直接父提交精确等于 5c93d4162f013c6eb6c6312ad8facbb371ff00f3；不得写 cdx/workload-isolation-final，不得混入其他 worktree 修改。按文档 WP1 到 WP6 的依赖顺序完成后续开发、TDD、SPEC 审查、质量审查和本地/CI 验证。未经我另行明确授权，不得部署、修改生产数据、systemd/nginx/frp/sudoers、安装 root policy、启用 v3 writer、切流或删除旧链路。

完成后不要只给文字总结；必须按 HANDOFF_RETURN_V1 返回精确 base/head、提交清单、完整 diff 边界、测试统计、CI run、未运行项、open findings 和生产变更。Codex 将对交付结果做独立最终验收。
```
