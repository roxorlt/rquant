# rQuant 受控自动发布

## 目标

日常交付由 Codex 端到端负责：创建和跟进 PR、测试与 CI、合并、tag、适用时的腾讯云发布、
验证、既有部署器的回滚处理，以及当前受管任务的安全回收。用户不需要操作 Git、worktree、
SSH、服务器、PR、CI、tag、部署、回滚或清理；只负责用户可见行为的业务验收，以及对高风险
变更作出明确授权。

[`docs/engineering/task-lifecycle.md`](engineering/task-lifecycle.md) 是受管任务状态、不可变 PR
证据、集成锁、发布分类、回收门禁和终态回执的唯一详细规范；本文件只说明生产发布如何落入
该闭环。每次合并、tag、部署和当前任务回收必须由同一 owner 持有该规范定义的集成锁串行执行。
本地 `main` dirty、与 `origin/main` diverged，或其归属不明时，集成一律阻塞，必须先取得
clean/reconciled 证据，禁止盲目 `pull`、`reset` 或清理。

自动化不是任意生产权限：普通部署器只能部署 `origin/main` 中的精确 SemVer tag 或完整 commit，
只能重启固定的 rQuant 服务，不能修改 systemd/nginx、写生产数据库或执行任意 sudo。

## 合并验收与发布分类

每个受管 PR 必须在合并前的 lifecycle record 中按**实际影响**选择且仅选择一个分类；不得根据
分支名、工具前缀或 Conventional Commit kind 推断。用户可见行为变更必须先获得用户业务验收，
才可由 Codex 合并。纯技术、文档和流程维护在本地验证、适用测试和 CI 门禁通过后可由 Codex
自动合并。基础设施、生产数据写入或修复、密钥轮换及其他高风险变更仍须用户单独明确授权。

| 分类 | 判定 | 合并后的必需结论 |
| --- | --- | --- |
| 发布类 | 改变产品可见行为、runtime 行为、打包产物或生产部署结果。 | 在精确 `origin/main` squash merge SHA 创建 annotated SemVer tag，完成受控部署并取得健康检查证据。 |
| 非发布类 | 不改变产品、runtime、打包产物或生产部署结果。 | lifecycle record 和终态回执均记录 tag、部署、健康检查为 N/A 及具体理由；不进行应用版本升级或生产部署。 |

高风险基础设施变更不是普通发布。它必须使用经用户明确授权的独立高风险 runbook，并以精确
tag 为目标；不得把 `scripts/deploy-production.sh` 当作该 runbook 的替代或绕过。Codex 仍负责
cloud 端验证与健康检查，并把证据写入 lifecycle record 和终态回执。

## 发布链路

1. Codex 在持有集成锁的受保护集成上下文中，确认本地 `main` clean/reconciled、PR 可合并、
   适用本地验证完成，且 Python 3.11/3.12 CI 全绿。用户可见变更还必须有用户业务验收记录。
2. Codex squash merge PR，并记录不可变 PR 证据元组
   `{repository, PR number/URL, base=main, merged_at, head.sha, merge_commit_sha}`。新鲜
   `git fetch` 后，必须确认 `merge_commit_sha` 位于 `origin/main`。
3. 发布类任务：Codex 在该精确 `origin/main` merge SHA 创建并推送 annotated SemVer tag，随后
   执行受控部署。非发布类任务：Codex 在 lifecycle record 中记录 tag、部署和健康检查均为
   N/A 与原因，不升级应用版本，也不部署。
4. 普通发布类任务在腾讯云执行：

   ```bash
   cd /home/lighthouse/rquant
   bash scripts/deploy-production.sh --target v0.13.2
   ```

5. 部署器依次执行：部署锁、tracked 工作区检查、`git fetch`、target/main 归属与快进检查、
   diff 风险分类、`git merge --ff-only <exact-sha>`、`uv sync --frozen`、preflight、
   白名单服务重启、第二次 preflight、JSONL 审计。
6. Codex 收集并记录部署和健康检查结果。更新依赖、preflight 或服务健康检查失败时，部署器按
   既有设计自动 `git reset --hard` 回上一 commit、
   恢复锁定依赖并重启已经切到新代码的服务。
7. 所有适用的合并后门禁成功后，Codex 仍在集成锁内按照生命周期规范关闭当前任务：核验精确
   PR/head/merge 证据、任务分支没有 PR 后新提交、当前受管 worktree 没有 tracked、untracked
   或未知 ignored 改动、且没有其他 owner 或任务租约使用这些对象。必须先成功向 PR 发布含上述
   不可变证据及计划清理的 provisional receipt，并原子更新 lifecycle record；仅在这些证据完整且
   provisional receipt 成功后，才可
   删除仍存在的远端任务分支、正常移除 worktree（不得 force）、重新核验精确本地分支及其 HEAD
   后，才可按规范执行 `git branch -D <exact-verified-branch>` 删除该本地分支，并执行
   `git fetch --prune`；该删除绝不适用于 legacy 或未知分支。
8. 清理完成后，Codex 向 PR 发布终态交付回执、原子更新 lifecycle record，并将任务标为
   `closed`；回执必须包括精确 PR 证据、测试/CI、tag 或 N/A 理由、部署/健康检查或 N/A 理由
   以及远端分支、本地分支和 worktree 的清理结果。详细顺序与本地分支删除的精确核验规则以
   [`task-lifecycle.md`](engineering/task-lifecycle.md#合并发布与回收门禁) 为准。

若部署、健康检查或清理前证据失败，Codex 不得开始清理，必须保留当前任务全部 artifacts；部署器
在其适用范围内按既有设计回滚，任务保持 `merged` 或进入 `quarantined`，不得声明 `closed`。若
发生部分清理或 final receipt 发布失败，Codex 必须立即停止，保留所有尚未移除的 artifacts，并在
本地 lifecycle record 标为 `quarantined`、记录已完成和失败的清理结果，随后重试 PR 的 final/
quarantine receipt；不得声称已移除的 artifacts 被保留或以破坏性方式重建它们，且不得标为
`closed`。此流程仅适用于当前受管任务，**不授权处理任何既有 legacy 分支或 worktree**。

## 自动拒绝

- target 是 `main`、`origin/main`、短 SHA 或包含 shell 字符，而不是 SemVer tag/完整 SHA。
- target 不属于 `origin/main`，或不是当前生产 commit 的快进后继。
- tracked 工作区存在未提交改动；`backup/` 等 untracked 文件不阻断。
- diff 包含 `deploy/systemd/`、`deploy/nginx/`、`deploy/frp/`、`deploy/sudoers/`。
- 工作日 09:15-15:10 的发布需要重启任何长驻服务。
- 另一个部署进程已持有 `logs/production-deploy.lock`。
- `sudo -n`、依赖同步、preflight 或服务健康检查失败。

部署器没有 `--force` / `--emergency` 绕过参数。高风险基础设施和生产数据操作必须另开
受控变更，并取得用户明确授权。

## 一次性安装

首次需要恢复可用的受控 SSH，并由 root 安装最小 sudoers 白名单：

```bash
cd /home/lighthouse/rquant
sudo visudo -cf deploy/sudoers/rquant-production-deploy
sudo install -o root -g root -m 0440 \
  deploy/sudoers/rquant-production-deploy \
  /etc/sudoers.d/rquant-production-deploy
sudo visudo -cf /etc/sudoers.d/rquant-production-deploy
sudo -n -l /usr/bin/systemctl restart rquant-dashboard.service
```

最后一条只检查白名单授权，不会重启服务。正式安装后，Codex 仅通过
`scripts/deploy-production.sh --target <exact-ref>` 部署。

## 预演与审计

预演会 fetch 和计算计划，但不 checkout、不更新依赖、不重启服务：

```bash
bash scripts/deploy-production.sh --target v0.13.2 --dry-run
```

退出码：`0` 成功/无需更新，`2` 策略拒绝，`75` 交易时段延期，`1` 部署或回滚失败。
审计记录位于 `/home/lighthouse/rquant/logs/production-deploy.jsonl`。

## 旧脚本边界

`scripts/deploy.sh` 保留给 systemd unit 等人工基础设施部署。它会执行交互式 sudo，且不具备
精确 target、交易时段保护和自动回滚，因此不得用于 Codex 无人值守发布。
